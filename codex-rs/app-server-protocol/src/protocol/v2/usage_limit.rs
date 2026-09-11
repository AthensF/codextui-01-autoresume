//! Experimental usage-limit status and controls for coordinated
//! usage-limit recovery.
//!
//! These types mirror the coordinator snapshot from
//! `codex-usage-limits` without exposing Tokio or core types. All
//! methods are experimental and gated by `experimental_api` negotiation.

use crate::JsonSchema;
use crate::TS;
use serde::Deserialize;
use serde::Serialize;

/// Phase of a usage-limit wait.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum UsageLimitWaitPhase {
    /// Work is queued before any execution effect.
    Queued,
    /// Work is transitioning to suspended after effects were observed.
    Suspending,
    /// Work is suspended after effects were observed.
    Suspended,
    /// A leader probe is in flight to revalidate capacity.
    Revalidating,
    /// Capacity was admitted; parked agents are resuming.
    Resuming,
    /// Automatic probing is exhausted; manual retry is required.
    ManualWait,
    /// A non-recoverable issue requires user attention.
    AttentionRequired,
    /// The wait was cancelled.
    Cancelled,
}

/// Classification of a waitable usage-limit encounter.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum UsageLimitClassification {
    /// Account usage-window exhaustion with an authoritative reset timestamp.
    UsageWindow,
    /// Transient response-stream rate limit.
    TransientRateLimit,
    /// Generic HTTP 429 with no structured reset metadata.
    GenericTooManyRequests,
}

/// Why automatic probing stopped and manual retry is required.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum UsageLimitManualWaitReason {
    /// `auto_resume` is disabled.
    AutoResumeDisabled,
    /// `max_auto_resumes` has been reached.
    AutoResumeLimitReached,
    /// `max_availability_checks` has been reached.
    AvailabilityCheckLimitReached,
    /// `unknown_reset_max_wait_seconds` wall-clock ceiling reached.
    WaitCeilingReached,
}

/// Immutable snapshot of the coordinator state for a single wait generation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct UsageLimitSnapshot {
    /// The causal work ID (root turn ID).
    pub work_id: String,
    /// Monotonic generation; stale callbacks compare against this.
    pub generation: u64,
    /// The thread that triggered the wait.
    pub trigger_thread_id: String,
    /// The turn that triggered the wait.
    pub trigger_turn_id: String,
    /// Current phase.
    pub phase: UsageLimitWaitPhase,
    /// Classification of the original encounter.
    pub classification: UsageLimitClassification,
    /// Limit identifier, if known.
    pub limit_id: Option<String>,
    /// Known reset timestamp (Unix seconds), if available.
    pub reset_at: Option<i64>,
    /// Next scheduled check time (Unix seconds), if any.
    pub next_check_at: Option<i64>,
    /// Whether automatic resume is enabled for this generation.
    pub auto_resume_enabled: bool,
    /// Number of automatic resumes consumed so far.
    pub automatic_resumes_used: u32,
    /// Maximum automatic resumes allowed.
    pub max_auto_resumes: u32,
    /// Number of unknown-reset probes issued so far.
    pub availability_checks_used: u32,
    /// Maximum unknown-reset probes allowed.
    pub max_availability_checks: u32,
    /// When the first unknown-reset probe was issued (Unix seconds), if any.
    pub unknown_reset_wait_started_at: Option<i64>,
    /// Maximum unknown-reset wait in seconds.
    pub unknown_reset_max_wait_seconds: u64,
    /// Why manual wait was entered, if applicable.
    pub manual_wait_reason: Option<UsageLimitManualWaitReason>,
    /// Thread IDs registered as parked.
    pub parked_thread_ids: Vec<String>,
    /// Thread IDs expected to park before a probe is elected.
    pub expected_participants: Vec<String>,
    /// Thread ID of the elected revalidation leader, if any.
    pub leader_thread_id: Option<String>,
    /// Whether a manual retry has been requested and is pending election.
    pub manual_retry_requested: bool,
}

/// Request payload for `thread/usageLimit/read`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadUsageLimitReadParams {
    pub thread_id: String,
}

/// Response payload for `thread/usageLimit/read`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadUsageLimitReadResponse {
    /// Current coordinator snapshot, or `null` if no wait is active.
    pub snapshot: Option<UsageLimitSnapshot>,
}

/// Action variant for `thread/usageLimit/action`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum UsageLimitAction {
    /// Manually trigger a revalidation probe now.
    RetryNow,
    /// Enable or disable automatic resume for the current generation.
    SetAutoResume { enabled: bool },
    /// Cancel the entire coordinated tree wait.
    Cancel,
}

/// Request payload for `thread/usageLimit/action`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadUsageLimitActionParams {
    pub thread_id: String,
    /// The generation to apply the action to. Stale generations are rejected.
    pub generation: u64,
    /// The action to apply.
    pub action: UsageLimitAction,
}

/// Result of applying a usage-limit action.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub enum UsageLimitActionResult {
    /// The action was applied successfully.
    Applied,
    /// The generation is stale; the action was ignored.
    StaleGeneration,
}

/// Response payload for `thread/usageLimit/action`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadUsageLimitActionResponse {
    pub result: UsageLimitActionResult,
}

/// Notification payload for `thread/usageLimit/updated`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export_to = "v2/")]
pub struct ThreadUsageLimitUpdatedNotification {
    pub thread_id: String,
    /// Current coordinator snapshot, or `null` if the wait cleared.
    pub snapshot: Option<UsageLimitSnapshot>,
}
