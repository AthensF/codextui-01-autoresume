use std::sync::Arc;

use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::ThreadUsageLimitActionParams;
use codex_app_server_protocol::ThreadUsageLimitActionResponse;
use codex_app_server_protocol::ThreadUsageLimitReadParams;
use codex_app_server_protocol::ThreadUsageLimitReadResponse;
use codex_app_server_protocol::UsageLimitAction;
use codex_app_server_protocol::UsageLimitActionResult;
use codex_app_server_protocol::UsageLimitClassification;
use codex_app_server_protocol::UsageLimitManualWaitReason;
use codex_app_server_protocol::UsageLimitSnapshot;
use codex_app_server_protocol::UsageLimitWaitPhase;
use codex_core::ThreadManager;
use codex_protocol::ThreadId;
use codex_usage_limits::WaitAction;

use crate::error_code::invalid_request;

pub(crate) struct UsageLimitRequestProcessor {
    thread_manager: Arc<ThreadManager>,
    queue_store: Option<Arc<dyn codex_thread_store::QueueStore>>,
}

impl UsageLimitRequestProcessor {
    pub(crate) fn new(
        thread_manager: Arc<ThreadManager>,
        queue_store: Option<Arc<dyn codex_thread_store::QueueStore>>,
    ) -> Self {
        Self {
            thread_manager,
            queue_store,
        }
    }

    pub(crate) async fn read(
        &self,
        params: ThreadUsageLimitReadParams,
    ) -> Result<ThreadUsageLimitReadResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|_| invalid_request("invalid thread_id"))?;
        // First try the loaded thread's in-memory coordinator.
        if let Ok(thread) = self.thread_manager.get_thread(thread_id).await {
            let snapshot = thread.usage_limit_snapshot().await;
            return Ok(ThreadUsageLimitReadResponse {
                snapshot: snapshot.map(map_snapshot),
            });
        }
        // Thread is not loaded; try to hydrate from durable recovery manifests.
        if let Some(queue_store) = &self.queue_store {
            if let Some(snapshot) = hydrate_snapshot_from_queue(queue_store, thread_id).await {
                return Ok(ThreadUsageLimitReadResponse {
                    snapshot: Some(snapshot),
                });
            }
        }
        Err(invalid_request("thread not found"))
    }

    pub(crate) async fn action(
        &self,
        params: ThreadUsageLimitActionParams,
    ) -> Result<ThreadUsageLimitActionResponse, JSONRPCErrorError> {
        let thread_id = ThreadId::from_string(&params.thread_id)
            .map_err(|_| invalid_request("invalid thread_id"))?;
        let thread = self
            .thread_manager
            .get_thread(thread_id)
            .await
            .map_err(|_| invalid_request("thread not found"))?;
        let action = match params.action {
            UsageLimitAction::RetryNow => WaitAction::RetryNow,
            UsageLimitAction::SetAutoResume { enabled } => WaitAction::SetAutoResume { enabled },
            UsageLimitAction::Cancel => WaitAction::Cancel,
        };
        let result = thread
            .usage_limit_apply_action(params.generation, action)
            .await;
        Ok(ThreadUsageLimitActionResponse {
            result: match result {
                codex_usage_limits::ActionResult::Applied => UsageLimitActionResult::Applied,
                codex_usage_limits::ActionResult::StaleGeneration => {
                    UsageLimitActionResult::StaleGeneration
                }
            },
        })
    }
}

/// Maps a `codex_usage_limits::WaitSnapshot` to the protocol DTO
/// `UsageLimitSnapshot`.
pub(crate) fn map_snapshot(s: codex_usage_limits::WaitSnapshot) -> UsageLimitSnapshot {
    UsageLimitSnapshot {
        work_id: s.work_id,
        generation: s.generation,
        trigger_thread_id: s.trigger_thread_id,
        trigger_turn_id: s.trigger_turn_id,
        phase: map_phase(s.phase),
        classification: map_classification(s.classification),
        limit_id: s.limit_id,
        reset_at: s.reset_at,
        next_check_at: s.next_check_at,
        auto_resume_enabled: s.auto_resume_enabled,
        automatic_resumes_used: s.automatic_resumes_used,
        max_auto_resumes: s.max_auto_resumes,
        availability_checks_used: s.availability_checks_used,
        max_availability_checks: s.max_availability_checks,
        unknown_reset_wait_started_at: s.unknown_reset_wait_started_at,
        unknown_reset_max_wait_seconds: s.unknown_reset_max_wait_seconds,
        manual_wait_reason: s.manual_wait_reason.map(map_manual_wait_reason),
        parked_thread_ids: s.parked_thread_ids,
        expected_participants: s.expected_participants,
        leader_thread_id: s.leader_thread_id,
        manual_retry_requested: s.manual_retry_requested,
    }
}

fn map_phase(p: codex_usage_limits::WaitPhase) -> UsageLimitWaitPhase {
    match p {
        codex_usage_limits::WaitPhase::Queued => UsageLimitWaitPhase::Queued,
        codex_usage_limits::WaitPhase::Suspending => UsageLimitWaitPhase::Suspending,
        codex_usage_limits::WaitPhase::Suspended => UsageLimitWaitPhase::Suspended,
        codex_usage_limits::WaitPhase::Revalidating => UsageLimitWaitPhase::Revalidating,
        codex_usage_limits::WaitPhase::Resuming => UsageLimitWaitPhase::Resuming,
        codex_usage_limits::WaitPhase::ManualWait => UsageLimitWaitPhase::ManualWait,
        codex_usage_limits::WaitPhase::AttentionRequired => UsageLimitWaitPhase::AttentionRequired,
        codex_usage_limits::WaitPhase::Cancelled => UsageLimitWaitPhase::Cancelled,
    }
}

fn map_classification(c: codex_usage_limits::LimitClassification) -> UsageLimitClassification {
    match c {
        codex_usage_limits::LimitClassification::UsageWindow => {
            UsageLimitClassification::UsageWindow
        }
        codex_usage_limits::LimitClassification::TransientRateLimit => {
            UsageLimitClassification::TransientRateLimit
        }
        codex_usage_limits::LimitClassification::GenericTooManyRequests => {
            UsageLimitClassification::GenericTooManyRequests
        }
    }
}

fn map_manual_wait_reason(r: codex_usage_limits::ManualWaitReason) -> UsageLimitManualWaitReason {
    match r {
        codex_usage_limits::ManualWaitReason::AutoResumeDisabled => {
            UsageLimitManualWaitReason::AutoResumeDisabled
        }
        codex_usage_limits::ManualWaitReason::AutoResumeLimitReached => {
            UsageLimitManualWaitReason::AutoResumeLimitReached
        }
        codex_usage_limits::ManualWaitReason::AvailabilityCheckLimitReached => {
            UsageLimitManualWaitReason::AvailabilityCheckLimitReached
        }
        codex_usage_limits::ManualWaitReason::WaitCeilingReached => {
            UsageLimitManualWaitReason::WaitCeilingReached
        }
    }
}

/// Hydrates a usage-limit snapshot for an unloaded thread from durable
/// recovery manifests in the queue store.
///
/// When a thread is not loaded in memory, its in-memory coordinator is
/// gone.  However, if the thread was suspended for durable recovery, the
/// `SuspendedTreeRecoveryManifest` in the queue carries a serialized
/// `WaitSnapshot`.  This function finds the most recent manifest for the
/// thread and deserialises the snapshot.
async fn hydrate_snapshot_from_queue(
    queue_store: &Arc<dyn codex_thread_store::QueueStore>,
    thread_id: ThreadId,
) -> Option<UsageLimitSnapshot> {
    let items = queue_store.list_page(thread_id, 0, 50).await.ok()?;
    for item in items {
        if let Ok(Some(manifest)) =
            codex_queue_extension::decode_suspended_tree_recovery(&item.payload)
        {
            if let Some(snapshot_value) = &manifest.usage_limit_snapshot {
                if let Ok(snapshot) = serde_json::from_value::<codex_usage_limits::WaitSnapshot>(
                    snapshot_value.clone(),
                ) {
                    return Some(map_snapshot(snapshot));
                }
            }
        }
    }
    None
}
