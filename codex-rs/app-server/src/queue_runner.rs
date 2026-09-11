//! Queue runner: loads unloaded threads and drives the durable queue scheduler.
//!
//! On app-server startup the runner recovers expired leases from previous
//! processes and spawns the background scheduler task. The scheduler
//! periodically discovers due head items across all threads, claims them
//! atomically, and dispatches them. When a thread is not yet loaded the
//! runner resumes it from durable history before dispatch.
//!
//! On graceful shutdown the runner stops claiming new jobs so a replacement
//! process can take over without duplicate dispatch.

use std::sync::Arc;

use codex_core::CodexThread;
use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_login::AuthManager;
use codex_protocol::ThreadId;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_queue_extension::QueuedItemService;
use codex_queue_extension::ThreadLoader;
use codex_rollout::InitialHistory;
use codex_rollout::ResumedHistory;
use codex_thread_store::LoadThreadHistoryParams as StoreLoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams as StoreReadThreadParams;
use codex_thread_store::StoredThread;
use codex_thread_store::ThreadStore;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// App-server queue runner that owns the scheduler lifecycle.
///
/// Created during [`MessageProcessor`] initialization and dropped on
/// shutdown. The runner holds a strong reference to the
/// [`QueuedItemService`] so the scheduler task is not cancelled early.
pub struct QueueRunner {
    service: Arc<QueuedItemService>,
    scheduler_handle: Mutex<Option<JoinHandle<()>>>,
}

impl QueueRunner {
    /// Creates a runner, recovers expired leases, and spawns the scheduler.
    ///
    /// The `thread_loader` is installed on the service before the scheduler
    /// starts so due items for unloaded threads can be dispatched.
    pub fn start(service: Arc<QueuedItemService>, loader: Arc<AppServerThreadLoader>) -> Self {
        // Install the loader on the shared service so the scheduler can
        // resume unloaded threads.  Unlike `with_thread_loader`, this does
        // not create a separate service instance.
        if let Err(loader) = service.set_thread_loader(loader) {
            tracing::warn!("thread loader was already installed; ignoring duplicate");
            let _ = loader;
        }

        // Recover expired leases from a previous process crash in a background
        // task so the app-server starts without blocking on lease recovery.
        let recovery_service = Arc::clone(&service);
        tokio::spawn(async move {
            match recovery_service.recover_leases().await {
                Ok(recovered) => {
                    if recovered > 0 {
                        tracing::info!(recovered, "queue runner recovered expired leases");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "queue runner failed to recover expired leases");
                }
            }
        });

        let scheduler_handle = service.clone().spawn_scheduler();
        tracing::info!("queue runner started scheduler");

        Self {
            service,
            scheduler_handle: Mutex::new(Some(scheduler_handle)),
        }
    }

    /// Stops claiming new jobs and waits for the scheduler task to exit.
    ///
    /// Already-claimed items continue to be dispatched: successful dispatches
    /// are acknowledged, failures release the lease, and any remaining leased
    /// items expire naturally and are recovered by the replacement process
    /// on startup via [`QueuedItemService::recover_leases`].
    ///
    /// After this call the scheduler will not claim new work.
    pub async fn shutdown(&self) {
        self.service.stop_claiming();
        let handle = self.scheduler_handle.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        tracing::info!("queue runner stopped scheduler");
    }
}

impl Drop for QueueRunner {
    fn drop(&mut self) {
        self.service.stop_claiming();
        if let Some(handle) = self
            .scheduler_handle
            .try_lock()
            .ok()
            .and_then(|mut h| h.take())
        {
            handle.abort();
        }
    }
}

/// App-server implementation of [`ThreadLoader`] that resumes unloaded
/// threads from durable history.
pub struct AppServerThreadLoader {
    thread_manager: Arc<ThreadManager>,
    thread_store: Arc<dyn ThreadStore>,
    config: Arc<Config>,
    auth_manager: Arc<AuthManager>,
}

impl AppServerThreadLoader {
    pub fn new(
        thread_manager: Arc<ThreadManager>,
        thread_store: Arc<dyn ThreadStore>,
        config: Arc<Config>,
        auth_manager: Arc<AuthManager>,
    ) -> Self {
        Self {
            thread_manager,
            thread_store,
            config,
            auth_manager,
        }
    }

    /// Loads the initial history for a stored thread.
    async fn load_initial_history(
        &self,
        stored_thread: &StoredThread,
    ) -> Result<InitialHistory, String> {
        if matches!(stored_thread.history_mode, ThreadHistoryMode::Paginated) {
            let model_context = self
                .thread_store
                .load_latest_model_context(StoreLoadThreadHistoryParams {
                    thread_id: stored_thread.thread_id,
                    include_archived: true,
                })
                .await
                .map_err(|err| format!("failed to load paginated history: {err}"))?;
            return Ok(InitialHistory::Resumed(ResumedHistory {
                conversation_id: model_context.thread_id,
                history: Arc::new(model_context.items),
                rollout_path: stored_thread.rollout_path.clone(),
            }));
        }

        // Non-paginated: re-read with history included.
        let mut stored = self
            .thread_store
            .read_thread(StoreReadThreadParams {
                thread_id: stored_thread.thread_id,
                include_archived: true,
                include_history: true,
            })
            .await
            .map_err(|err| format!("failed to read thread history: {err}"))?;
        let history = stored.history.take().map(|h| h.items).ok_or_else(|| {
            format!(
                "thread {} did not include persisted history",
                stored_thread.thread_id
            )
        })?;
        Ok(InitialHistory::Resumed(ResumedHistory {
            conversation_id: stored_thread.thread_id,
            history: Arc::new(history),
            rollout_path: stored.rollout_path.clone(),
        }))
    }
}

impl ThreadLoader for AppServerThreadLoader {
    fn load_thread(
        &self,
        thread_id: ThreadId,
    ) -> futures::future::BoxFuture<'static, Result<Arc<CodexThread>, String>> {
        let thread_manager = self.thread_manager.clone();
        let thread_store = self.thread_store.clone();
        let config = self.config.clone();
        let auth_manager = self.auth_manager.clone();
        Box::pin(async move {
            // If the thread is already loaded, return it directly.
            if let Ok(thread) = thread_manager.get_thread(thread_id).await {
                return Ok(thread);
            }

            // Read the stored thread metadata.
            let stored_thread = thread_store
                .read_thread(StoreReadThreadParams {
                    thread_id,
                    include_archived: true,
                    include_history: false,
                })
                .await
                .map_err(|err| format!("failed to read stored thread {thread_id}: {err}"))?;

            // Reject multi-agent-v2 children; they must be loaded through their parent.
            if stored_thread.parent_thread_id.is_some() {
                return Err(format!(
                    "cannot load multi-agent-v2 child {thread_id} directly; resume the parent first"
                ));
            }

            // Load the initial history.
            let loader = AppServerThreadLoader {
                thread_manager: thread_manager.clone(),
                thread_store: thread_store.clone(),
                config: config.clone(),
                auth_manager: auth_manager.clone(),
            };
            let history = loader.load_initial_history(&stored_thread).await?;

            // Resume the thread.
            let new_thread = thread_manager
                .resume_thread_with_history(
                    config.as_ref().clone(),
                    history,
                    auth_manager,
                    /*parent_trace*/ None,
                    Default::default(),
                )
                .await
                .map_err(|err| format!("failed to resume thread {thread_id}: {err}"))?;

            Ok(new_thread.thread)
        })
    }
}
