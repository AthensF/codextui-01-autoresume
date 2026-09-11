//! Tests for the usage-limit recovery orchestration module.
//!
//! These tests verify the pure logic of error classification, hint-to-encounter
//! conversion, and execution-progress-based disposition selection, without
//! requiring a full session/model-provider integration.

use super::turn_execution_progress::ContinuationSafety;
use super::turn_execution_progress::DrainReport;
use super::turn_execution_progress::TurnExecutionProgress;
use super::usage_limit_recovery::derive_work_id;
use super::usage_limit_recovery::extract_recovery_hint;
use super::usage_limit_recovery::hint_to_encounter;
use super::usage_limit_recovery::is_recoverable_usage_limit;
use chrono::Duration as ChronoDuration;
use chrono::Utc;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::UsageLimitReachedError;
use codex_protocol::error::UsageLimitRecoveryHint;
use codex_protocol::error::UsageLimitRecoveryKind;
use codex_usage_limits::LimitClassification;
use codex_usage_limits::WaitDisposition;

/// Builds a `CodexErr` with a `UsageLimitReached` detail and a recovery hint.
fn usage_limit_err(
    kind: UsageLimitRecoveryKind,
    resets_at: Option<chrono::DateTime<Utc>>,
) -> CodexErr {
    let mut err = CodexErr::new(CodexErrorDetails::UsageLimitReached(
        UsageLimitReachedError {
            plan_type: None,
            resets_at,
            rate_limits: None,
            promo_message: None,
            rate_limit_reached_type: None,
        },
    ));
    let mut hint = UsageLimitRecoveryHint::new(kind);
    hint.resets_at = resets_at;
    err = err.with_usage_limit_recovery_hint(hint);
    err
}

#[test]
fn is_recoverable_with_hint() {
    let err = usage_limit_err(UsageLimitRecoveryKind::UsageWindow, None);
    assert!(is_recoverable_usage_limit(&err));
}

#[test]
fn is_not_recoverable_without_hint() {
    let err = CodexErr::new(CodexErrorDetails::UsageLimitReached(
        UsageLimitReachedError {
            plan_type: None,
            resets_at: None,
            rate_limits: None,
            promo_message: None,
            rate_limit_reached_type: None,
        },
    ));
    assert!(!is_recoverable_usage_limit(&err));
}

#[test]
fn is_not_recoverable_for_other_errors() {
    let err = CodexErr::new(CodexErrorDetails::TurnAborted);
    assert!(!is_recoverable_usage_limit(&err));
}

#[test]
fn extract_hint_returns_some_when_present() {
    let err = usage_limit_err(UsageLimitRecoveryKind::UsageWindow, None);
    let hint = extract_recovery_hint(&err);
    assert!(hint.is_some());
    assert_eq!(hint.unwrap().kind, UsageLimitRecoveryKind::UsageWindow);
}

#[test]
fn extract_hint_returns_none_when_absent() {
    let err = CodexErr::new(CodexErrorDetails::TurnAborted);
    assert!(extract_recovery_hint(&err).is_none());
}

#[test]
fn hint_to_encounter_queued_when_pristine() {
    let progress = TurnExecutionProgress::new();
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.disposition, WaitDisposition::Queued);
    assert_eq!(encounter.classification, LimitClassification::UsageWindow);
}

#[test]
fn hint_to_encounter_suspended_when_model_output_received() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.disposition, WaitDisposition::Suspended);
}

#[test]
fn hint_to_encounter_suspended_when_tool_dispatched() {
    let progress = TurnExecutionProgress::new();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.disposition, WaitDisposition::Suspended);
}

#[test]
fn hint_to_encounter_suspended_when_hook_effect_applied() {
    let progress = TurnExecutionProgress::new();
    progress.record_hook_effect_applied();
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.disposition, WaitDisposition::Suspended);
}

#[test]
fn hint_to_encounter_preserves_reset_at() {
    let progress = TurnExecutionProgress::new();
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    let mut hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    hint.resets_at = Some(reset_at);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.reset_at, Some(reset_at));
}

#[test]
fn hint_to_encounter_preserves_limit_id() {
    let progress = TurnExecutionProgress::new();
    let mut hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
    hint.limit_id = Some("codex".to_string());
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(encounter.limit_id, Some("codex".to_string()));
}

#[test]
fn hint_to_encounter_classifies_transient_rate_limit() {
    let progress = TurnExecutionProgress::new();
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::TransientRateLimit);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(
        encounter.classification,
        LimitClassification::TransientRateLimit
    );
}

#[test]
fn hint_to_encounter_classifies_generic_429() {
    let progress = TurnExecutionProgress::new();
    let hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::GenericTooManyRequests);
    let encounter = hint_to_encounter(&hint, &progress);
    assert_eq!(
        encounter.classification,
        LimitClassification::GenericTooManyRequests
    );
}

// ---------------------------------------------------------------------------
// TurnExecutionProgress tests
// ---------------------------------------------------------------------------

#[test]
fn progress_starts_pristine() {
    let progress = TurnExecutionProgress::new();
    assert!(progress.is_pristine());
    assert!(!progress.has_side_effects());
    assert!(!progress.side_effects_ambiguous());
}

#[test]
fn progress_model_output_makes_non_pristine() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    assert!(!progress.is_pristine());
    assert!(!progress.has_side_effects());
    assert!(progress.safe_to_suspend_at_model_boundary());
}

#[test]
fn progress_tool_dispatched_creates_side_effects() {
    let progress = TurnExecutionProgress::new();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    assert!(!progress.is_pristine());
    assert!(progress.has_side_effects());
    assert!(progress.side_effects_ambiguous());
}

#[test]
fn progress_tool_completed_resolves_ambiguity() {
    let progress = TurnExecutionProgress::new();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_completed();
    assert!(progress.has_side_effects());
    assert!(!progress.side_effects_ambiguous());
}

#[test]
fn progress_hook_effect_creates_side_effects() {
    let progress = TurnExecutionProgress::new();
    progress.record_hook_effect_applied();
    assert!(!progress.is_pristine());
    assert!(progress.has_side_effects());
}

#[test]
fn progress_model_request_count_increments() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_request_dispatched();
    progress.record_model_request_dispatched();
    let snap = progress.snapshot();
    assert_eq!(snap.model_request_count, 2);
    assert!(snap.model_request_dispatched);
}

#[test]
fn progress_tool_dispatch_count_increments() {
    let progress = TurnExecutionProgress::new();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    let snap = progress.snapshot();
    assert_eq!(snap.tool_dispatch_count, 3);
}

#[test]
fn progress_compaction_tracking() {
    let progress = TurnExecutionProgress::new();
    progress.record_compaction_started();
    let snap = progress.snapshot();
    assert!(snap.compaction_started);
    assert!(!snap.compaction_completed);
    progress.record_compaction_completed();
    let snap = progress.snapshot();
    assert!(snap.compaction_completed);
}

#[test]
fn progress_safe_to_suspend_with_only_model_output() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    assert!(progress.safe_to_suspend_at_model_boundary());
    assert!(!progress.has_side_effects());
}

#[test]
fn progress_not_safe_to_suspend_with_tools() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    assert!(!progress.safe_to_suspend_at_model_boundary());
    assert!(progress.has_side_effects());
}

// ---------------------------------------------------------------------------
// ContinuationSafety classification tests
// ---------------------------------------------------------------------------

#[test]
fn continuation_safety_untouched_at_start() {
    let progress = TurnExecutionProgress::new();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::Untouched
    );
}

#[test]
fn continuation_safety_safe_to_suspend_with_only_model_output() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::SafeToSuspend
    );
}

#[test]
fn continuation_safety_effects_observed_after_tool_completed() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_completed();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::EffectsObserved
    );
}

#[test]
fn continuation_safety_attention_required_for_incomplete_tools() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    // Tool dispatched but not completed → ambiguous
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::AttentionRequired
    );
}

#[test]
fn continuation_safety_attention_required_for_missing_tool_output() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_completed();
    progress.record_missing_tool_output();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::AttentionRequired
    );
}

#[test]
fn continuation_safety_effects_observed_with_hook_effect() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_hook_effect_applied();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::EffectsObserved
    );
}

#[test]
fn continuation_safety_effects_observed_with_compaction() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_compaction_started();
    progress.record_compaction_completed();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::EffectsObserved
    );
}

// ---------------------------------------------------------------------------
// DrainReport tests
// ---------------------------------------------------------------------------

#[test]
fn drain_report_default_is_not_ambiguous() {
    let report = DrainReport::default();
    assert!(!report.is_ambiguous());
}

#[test]
fn drain_report_ambiguous_when_failed_without_output() {
    let report = DrainReport {
        completed: 2,
        failed_without_output: 1,
        admitted: 3,
    };
    assert!(report.is_ambiguous());
}

#[test]
fn drain_report_not_ambiguous_when_all_completed() {
    let report = DrainReport {
        completed: 3,
        failed_without_output: 0,
        admitted: 3,
    };
    assert!(!report.is_ambiguous());
}

#[test]
fn record_drain_report_marks_missing_output_on_failure() {
    let progress = TurnExecutionProgress::new();
    progress.record_model_output_received();
    progress.record_tool_dispatched(&codex_protocol::ToolName::plain("exec_command"));
    progress.record_tool_completed();
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::EffectsObserved
    );
    let report = DrainReport {
        completed: 1,
        failed_without_output: 1,
        admitted: 2,
    };
    progress.record_drain_report(&report);
    assert_eq!(
        progress.continuation_safety(),
        ContinuationSafety::AttentionRequired
    );
}

// ---------------------------------------------------------------------------
// derive_work_id tests
// ---------------------------------------------------------------------------

#[test]
fn derive_work_id_uses_root_turn_id_when_present() {
    let work_id = derive_work_id(Some("root-turn-123".to_string()), "child-turn-456");
    assert_eq!(work_id, "root-turn-123");
}

#[test]
fn derive_work_id_falls_back_to_turn_id_when_no_root() {
    let work_id = derive_work_id(None, "turn-789");
    assert_eq!(work_id, "turn-789");
}
