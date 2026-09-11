//! Recovery orchestration for usage-limit encounters during turn execution.
//!
//! This module isolates the logic for:
//! - Classifying a `CodexErr` as a recoverable usage-limit error.
//! - Converting `UsageLimitRecoveryHint` to a coordinator `LimitEncounter`.
//! - Coordinating the wait/resume cycle through `UsageLimitCoordinator`.
//! - Determining continuation safety from `TurnExecutionProgress`.
//!
//! The outer agent loop (`run_sampling_request`) calls into this module
//! instead of returning the error immediately when a usage limit is hit.

use super::turn_execution_progress::ContinuationSafety;
use super::turn_execution_progress::TurnExecutionProgress;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::UsageLimitRecoveryHint;
use codex_protocol::error::UsageLimitRecoveryKind;
use codex_usage_limits::LimitClassification;
use codex_usage_limits::LimitEncounter;
use codex_usage_limits::ResumeProbePermit;
use codex_usage_limits::UsageLimitCoordinator;
use codex_usage_limits::WaitCancelled;
use codex_usage_limits::WaitDisposition;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Outcome of attempting recovery for a usage-limit error.
#[derive(Debug)]
pub(crate) enum RecoveryOutcome {
    /// Recovery succeeded; the caller should retry the model request.
    /// The `ResumeProbePermit` indicates whether this caller is the
    /// elected probe leader or a gate-open follower.
    Resume(ResumeProbePermit),
    /// The error is not a recoverable usage-limit error.
    NotRecoverable,
    /// The wait was cancelled by the user or system.
    Cancelled,
}

/// Classifies a `CodexErr` as a recoverable usage-limit error and returns
/// the recovery hint if present.
pub(crate) fn extract_recovery_hint(err: &CodexErr) -> Option<&UsageLimitRecoveryHint> {
    err.usage_limit_recovery_hint()
}

/// Converts a `UsageLimitRecoveryHint` to a coordinator `LimitEncounter`,
/// choosing the disposition based on the current execution progress.
pub(crate) fn hint_to_encounter(
    hint: &UsageLimitRecoveryHint,
    progress: &TurnExecutionProgress,
) -> LimitEncounter {
    let classification = match hint.kind {
        UsageLimitRecoveryKind::UsageWindow => LimitClassification::UsageWindow,
        UsageLimitRecoveryKind::TransientRateLimit => LimitClassification::TransientRateLimit,
        UsageLimitRecoveryKind::GenericTooManyRequests => {
            LimitClassification::GenericTooManyRequests
        }
    };
    // Classify the turn's continuation safety to choose the disposition.
    // Untouched turns can be safely queued — there is nothing to preserve.
    // All other states (SafeToSuspend, EffectsObserved, AttentionRequired)
    // require suspension to preserve completed work and avoid replay.
    let disposition = match progress.continuation_safety() {
        ContinuationSafety::Untouched => WaitDisposition::Queued,
        ContinuationSafety::SafeToSuspend
        | ContinuationSafety::EffectsObserved
        | ContinuationSafety::AttentionRequired => WaitDisposition::Suspended,
    };
    LimitEncounter {
        classification,
        disposition,
        reset_at: hint.resets_at,
        retry_after: hint.retry_after,
        limit_id: hint.limit_id.clone(),
    }
}

/// Orchestrates the full recovery cycle for a usage-limit error.
///
/// This function:
/// 1. Extracts the recovery hint from the error.
/// 2. Converts it to a `LimitEncounter` using execution progress.
/// 3. Reports the encounter to the coordinator.
/// 4. Registers the calling thread as parked.
/// 5. Waits for the gate to open or for leadership election.
/// 6. Returns the permit or an error outcome.
///
/// The caller is responsible for:
/// - Retrying the model request if `RecoveryOutcome::Resume` is returned.
/// - Propagating the original error if `RecoveryOutcome::NotRecoverable`
///   or `RecoveryOutcome::Cancelled` is returned.
/// - Calling `coordinator.mark_probe_admitted` or
///   `coordinator.mark_probe_limited` after the retry, if this caller is
///   the elected leader.
/// - Calling `coordinator.finish_work` when the turn completes.
pub(crate) async fn handle_usage_limit_encounter(
    coordinator: &Arc<UsageLimitCoordinator>,
    work_id: String,
    thread_id: String,
    turn_id: String,
    err: &CodexErr,
    progress: &TurnExecutionProgress,
    expected_participants: Vec<String>,
    cancellation_token: CancellationToken,
) -> RecoveryOutcome {
    let Some(hint) = extract_recovery_hint(err) else {
        return RecoveryOutcome::NotRecoverable;
    };
    let encounter = hint_to_encounter(hint, progress);
    let generation = coordinator
        .encounter_limit(
            work_id.clone(),
            thread_id.clone(),
            turn_id.clone(),
            encounter,
        )
        .await;
    // Register all known live tree members as expected participants so
    // the coordinator does not elect a probe until they have all parked
    // or been removed via genuine completion.
    if !expected_participants.is_empty() {
        let _ = coordinator
            .register_expected_participants(generation, expected_participants)
            .await;
    }
    // Register as parked so the coordinator knows we are waiting.
    let _ = coordinator
        .register_parked_agent(generation, thread_id.clone())
        .await;
    match coordinator
        .wait_for_open_or_probe(generation, thread_id, cancellation_token)
        .await
    {
        Ok(permit) => RecoveryOutcome::Resume(permit),
        Err(WaitCancelled) => RecoveryOutcome::Cancelled,
    }
}

/// Returns `true` if the error is a usage-limit error that should be
/// handled by the recovery orchestration rather than propagated.
pub(crate) fn is_recoverable_usage_limit(err: &CodexErr) -> bool {
    matches!(err.details(), CodexErrorDetails::UsageLimitReached(_))
        && err.usage_limit_recovery_hint().is_some()
}

/// Derives the causal work ID for a turn from `root_turn_id`, falling back
/// to the current turn ID for legacy/non-tree turns.
///
/// The work ID must be stable across retries and descendants of the same
/// causal root turn so that the coordinator preserves automatic-resume
/// accounting across wait generations.
pub(crate) fn derive_work_id(root_turn_id: Option<String>, turn_id: &str) -> String {
    root_turn_id.unwrap_or_else(|| turn_id.to_string())
}
