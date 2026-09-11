//! Lease-based scheduler for the durable FIFO queue.
//!
//! Extends [`QueuedItemService`] with availability-aware dispatch:
//! - [`QueuedItemService::schedule_due_items`] discovers due head items
//!   across all threads, claims them atomically, and dispatches them.
//! - [`QueuedItemService::recover_leases`] resets expired leases on
//!   startup so crashed workers' items are re-dispatched.
//!
//! The scheduler preserves strict FIFO per thread: an unavailable head
//! blocks later items because `list_due_heads` only returns the head
//! (smallest `queue_order`) for each thread.

use codex_core::StartIfIdleSubmission;
use codex_core::TurnInput;
use codex_core::TurnInputRequest;
use codex_core::TurnStartOptions;
use codex_protocol::ThreadId;
use codex_protocol::protocol::AgentStatus;
use codex_thread_store::QueuedItemState;
use codex_thread_store::RescheduleParams;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::payload::decode_payload;
use crate::service::QueuedItemService;

/// Default lease duration: 60 seconds.
const DEFAULT_LEASE_DURATION_MS: i64 = 60_000;

/// Maximum number of due heads to process in one scheduling cycle.
const SCHEDULE_BATCH_LIMIT: usize = 32;

/// Interval between scheduler wake-ups when no due items are pending.
const SCHEDULER_POLL_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(/*secs*/ 5);

impl QueuedItemService {
    /// Returns this worker's unique ID for lease ownership.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Resets all expired leases back to pending state.
    ///
    /// Should be called on startup to recover items that were being
    /// dispatched when a previous process crashed.
    pub async fn recover_leases(&self) -> Result<u64, crate::QueueServiceError> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        Ok(self.queue.recover_expired_leases(now_ms).await?)
    }

    /// Spawns a background task that periodically discovers due items and
    /// dispatches them.
    ///
    /// The task runs until [`stop_claiming`] is called or the service is
    /// dropped. Returns a handle that can be awaited to ensure the task has
    /// exited.
    pub fn spawn_scheduler(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SCHEDULER_POLL_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Skip the immediate first tick so the app-server has time to
            // fully initialize before the first scheduling cycle.
            interval.tick().await;
            loop {
                interval.tick().await;
                if !self
                    .claiming_enabled
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    tracing::info!("queue scheduler stopping (claiming disabled)");
                    return;
                }
                match self.schedule_due_items().await {
                    Ok(dispatched) if dispatched > 0 => {
                        tracing::debug!(dispatched, "queue scheduler dispatched items");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "queue scheduler cycle failed");
                    }
                }
            }
        })
    }

    /// Stops the scheduler from claiming new jobs.
    ///
    /// After this call, the background scheduler task will exit on its next
    /// tick. Already-claimed items continue to be dispatched.
    pub fn stop_claiming(&self) {
        self.claiming_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns whether this worker is still allowed to claim new jobs.
    pub fn is_claiming_enabled(&self) -> bool {
        self.claiming_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Discovers due head items across all threads, claims them, and
    /// dispatches them to their respective threads.
    ///
    /// Returns the number of items successfully dispatched.
    ///
    /// Strict FIFO is preserved: only the head item (smallest `queue_order`)
    /// per thread is considered, and a blocked head prevents later items
    /// from being dispatched.
    pub async fn schedule_due_items(&self) -> Result<usize, crate::QueueServiceError> {
        if !self.is_claiming_enabled() {
            return Ok(0);
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let due_heads = self
            .queue
            .list_due_heads(now_ms, SCHEDULE_BATCH_LIMIT)
            .await?;

        let mut dispatched = 0;
        for head in due_heads {
            let claimed = self
                .queue
                .claim_lease(
                    head.id.clone(),
                    self.worker_id.clone(),
                    now_ms,
                    DEFAULT_LEASE_DURATION_MS,
                )
                .await?;

            let Some(record) = claimed else {
                continue;
            };

            let input = match decode_payload(&record.payload) {
                Ok(crate::payload::QueuePayload::V1(envelope)) => match envelope.job {
                    crate::payload::QueueJobKind::UserMessage { input } => input,
                    crate::payload::QueueJobKind::SuspendedTreeRecovery(manifest) => {
                        // Dispatch the suspended-tree recovery job.  The
                        // lease is held during recovery; if this process
                        // crashes the lease expires and a replacement
                        // process reconciles idempotently.
                        match self.dispatch_suspended_tree_recovery(manifest).await {
                            RecoveryOutcome::Started => {
                                let _ = self
                                    .queue
                                    .acknowledge(record.id.clone(), self.worker_id.clone())
                                    .await;
                                dispatched += 1;
                            }
                            RecoveryOutcome::NotIdle => {
                                let _ = self
                                    .queue
                                    .release_lease(
                                        record.id.clone(),
                                        self.worker_id.clone(),
                                        now_ms,
                                    )
                                    .await;
                            }
                            RecoveryOutcome::Failed(error) => {
                                tracing::warn!(
                                    item_id = %record.id,
                                    %error,
                                    "failed to dispatch suspended-tree recovery, releasing lease"
                                );
                                let _ = self
                                    .queue
                                    .release_lease(
                                        record.id.clone(),
                                        self.worker_id.clone(),
                                        now_ms,
                                    )
                                    .await;
                            }
                        }
                        continue;
                    }
                },
                Ok(crate::payload::QueuePayload::V0(input)) => input,
                Err(error) => {
                    tracing::warn!(item_id = %record.id, %error, "discarding invalid queued item");
                    let _ = self
                        .queue
                        .acknowledge(record.id.clone(), self.worker_id.clone())
                        .await;
                    continue;
                }
            };

            if !matches!(input, TurnInput::UserInput { .. }) {
                tracing::warn!(item_id = %record.id, "discarding non-user queued input");
                let _ = self
                    .queue
                    .acknowledge(record.id.clone(), self.worker_id.clone())
                    .await;
                continue;
            }

            let thread_id = head.thread_id;
            let item_id = record.id.clone();

            match self.dispatch_claimed(thread_id, input).await {
                DispatchOutcome::Started => {
                    let _ = self
                        .queue
                        .acknowledge(item_id.clone(), self.worker_id.clone())
                        .await;
                    dispatched += 1;
                }
                DispatchOutcome::NotIdle => {
                    let _ = self
                        .queue
                        .release_lease(item_id.clone(), self.worker_id.clone(), now_ms)
                        .await;
                }
                DispatchOutcome::Failed(error) => {
                    tracing::warn!(
                        %thread_id,
                        %item_id,
                        %error,
                        "failed to dispatch queued item, releasing lease"
                    );
                    let _ = self
                        .queue
                        .release_lease(item_id.clone(), self.worker_id.clone(), now_ms)
                        .await;
                }
            }
        }

        Ok(dispatched)
    }

    /// Attempts to dispatch a claimed item to its thread.
    ///
    /// If the thread is not currently loaded and a [`ThreadLoader`] is
    /// configured, the loader is invoked to resume the thread from durable
    /// history before dispatching.
    async fn dispatch_claimed(&self, thread_id: ThreadId, input: TurnInput) -> DispatchOutcome {
        let Some(manager) = self.thread_manager.upgrade() else {
            return DispatchOutcome::NotIdle;
        };
        let thread = match manager.get_thread(thread_id).await {
            Ok(thread) => thread,
            Err(_) => {
                // Thread is not loaded; try to load it via the loader.
                let Some(loader) = self.thread_loader.get() else {
                    return DispatchOutcome::NotIdle;
                };
                match loader.load_thread(thread_id).await {
                    Ok(thread) => thread,
                    Err(error) => {
                        tracing::warn!(
                            %thread_id,
                            %error,
                            "failed to load thread for queue dispatch"
                        );
                        return DispatchOutcome::Failed(error);
                    }
                }
            }
        };
        if matches!(
            thread.agent_status().await,
            AgentStatus::Running | AgentStatus::Interrupted
        ) {
            return DispatchOutcome::NotIdle;
        }

        match thread
            .start_turn_if_idle(TurnInputRequest::new(input).on_start(TurnStartOptions {
                turn_trigger: Some("queue".to_string()),
                ..Default::default()
            }))
            .await
        {
            Ok(StartIfIdleSubmission::Started { .. }) => DispatchOutcome::Started,
            Ok(StartIfIdleSubmission::NotSubmitted { .. }) => DispatchOutcome::NotIdle,
            Err(error) => DispatchOutcome::Failed(error.to_string()),
        }
    }

    /// Reschedules a leased item as blocked with a future `not_before_ms`.
    ///
    /// Used when a usage-limit reset time is known and the item should not
    /// be retried until after the reset.
    pub async fn reschedule_blocked(
        &self,
        item_id: &str,
        not_before_ms: i64,
        block_reason: Option<String>,
        last_error: Option<String>,
    ) -> Result<bool, crate::QueueServiceError> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let result = self
            .queue
            .reschedule(
                item_id.to_string(),
                self.worker_id.clone(),
                now_ms,
                RescheduleParams {
                    not_before_ms,
                    state: QueuedItemState::Blocked,
                    block_reason,
                    last_error,
                },
            )
            .await?;
        Ok(result.is_some())
    }
}

enum DispatchOutcome {
    Started,
    NotIdle,
    Failed(String),
}

/// Outcome of dispatching a suspended-tree recovery job.
enum RecoveryOutcome {
    /// Recovery was started successfully.
    Started,
    /// The root thread is not idle; retry later.
    NotIdle,
    /// Recovery failed; release the lease for retry.
    Failed(String),
}

impl QueuedItemService {
    /// Dispatches a suspended-tree recovery job.
    ///
    /// This is the lease-fenced recovery path (PR 9-4).  The lease is
    /// already held by the caller; this method:
    ///
    /// 1. Loads the root thread via the configured [`ThreadLoader`] if it
    ///    is not already loaded.
    /// 2. Checks that the root thread is idle (not already running).
    /// 3. Builds a [`TreeRecoveryRequest`] from the manifest and calls
    ///    [`CodexThread::recover_tree`].
    /// 4. Returns the outcome so the caller can acknowledge or release
    ///    the lease.
    ///
    /// If this process crashes during recovery, the lease expires and a
    /// replacement process reconciles idempotently.  Stale workers cannot
    /// continue writing because the lease is no longer valid.
    async fn dispatch_suspended_tree_recovery(
        &self,
        manifest: crate::payload::SuspendedTreeRecoveryManifest,
    ) -> RecoveryOutcome {
        let Some(manager) = self.thread_manager.upgrade() else {
            return RecoveryOutcome::NotIdle;
        };

        // Load the root thread if it is not already loaded.
        let thread = match manager.get_thread(manifest.root_thread_id).await {
            Ok(thread) => thread,
            Err(_) => {
                let Some(loader) = self.thread_loader.get() else {
                    return RecoveryOutcome::NotIdle;
                };
                match loader.load_thread(manifest.root_thread_id).await {
                    Ok(thread) => thread,
                    Err(error) => {
                        tracing::warn!(
                            thread_id = %manifest.root_thread_id,
                            %error,
                            "failed to load root thread for suspended-tree recovery"
                        );
                        return RecoveryOutcome::Failed(error);
                    }
                }
            }
        };

        // Refuse recovery if the root thread is already running.  This
        // prevents duplicate dispatch and ensures idempotency.
        if matches!(
            thread.agent_status().await,
            AgentStatus::Running | AgentStatus::Interrupted
        ) {
            return RecoveryOutcome::NotIdle;
        }

        // Refuse automatic recovery when the manifest says effects are
        // ambiguous.  The user must manually intervene.
        if matches!(
            manifest.replay_safety,
            crate::payload::ReplaySafetySummary::AttentionRequired { .. }
        ) {
            return RecoveryOutcome::Failed(
                "suspended-tree recovery refused: ambiguous side effects".to_string(),
            );
        }

        // Build the tree recovery request from the manifest.
        let members = manifest
            .members
            .iter()
            .map(|m| codex_protocol::turn_input::TreeMemberRecoveryRequest {
                thread_id: m.thread_id,
                turn_id: m.turn_id.clone(),
                parent_thread_id: m.parent_thread_id,
                parent_turn_id: m.parent_turn_id.clone(),
                root_turn_id: m.root_turn_id.clone(),
                agent_path: m.agent_path.clone(),
            })
            .collect();
        let request = codex_protocol::turn_input::TreeRecoveryRequest {
            root_turn_id: manifest.root_turn_id.clone(),
            thread_settings: Default::default(),
            trace: None,
            cyber_access_program: None,
            members,
        };

        match thread.recover_tree(request).await {
            Ok(StartIfIdleSubmission::Started { .. }) => RecoveryOutcome::Started,
            Ok(StartIfIdleSubmission::NotSubmitted { .. }) => RecoveryOutcome::NotIdle,
            Err(error) => RecoveryOutcome::Failed(error.to_string()),
        }
    }
}
