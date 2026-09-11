//! State machine types for usage-limit waits.
//!
//! The state machine is:
//! ```text
//! Idle
//!   └─ submission / active work
//!        ├─ Running
//!        │    └─ waitable 429 + effects observed
//!        │          └─ Suspending
//!        │                └─ Suspended
//!        └─ waitable 429 + no effects observed
//!             └─ Queued
//!
//! Queued | Suspended
//!   ├─ timer/manual retry ─> Revalidating
//!   │                         ├─ admitted ─> Resuming ─> Running
//!   │                         ├─ still 429 ─> Queued/Suspended (new deadline/backoff)
//!   │                         └─ non-recoverable ─> AttentionRequired
//!   ├─ disable auto-resume ─> ManualWait
//!   ├─ auto-resume/check cap ─> ManualWait
//!   ├─ ambiguous side effect ─> AttentionRequired
//!   └─ cancel ─> Cancelled
//! ```

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::time::Duration;

/// Classification of a waitable usage-limit encounter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LimitClassification {
    /// Account usage-window exhaustion with an authoritative reset timestamp.
    UsageWindow,
    /// Transient response-stream rate limit.
    TransientRateLimit,
    /// Generic HTTP 429 with no structured reset metadata.
    GenericTooManyRequests,
}

/// Why automatic probing stopped and the work now requires manual retry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManualWaitReason {
    /// `auto_resume` is disabled.
    AutoResumeDisabled,
    /// `max_auto_resumes` has been reached.
    AutoResumeLimitReached,
    /// `max_availability_checks` has been reached.
    AvailabilityCheckLimitReached,
    /// `unknown_reset_max_wait_seconds` wall-clock ceiling reached.
    WaitCeilingReached,
}

/// Phase of a usage-limit wait.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitPhase {
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
    /// A non-recoverable issue (ambiguous side effect, auth/entitlement/model
    /// failure) requires user attention.
    AttentionRequired,
    /// The wait was cancelled.
    Cancelled,
}

impl WaitPhase {
    /// Returns `true` if the phase is terminal for this generation.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WaitPhase::ManualWait | WaitPhase::AttentionRequired | WaitPhase::Cancelled
        )
    }

    /// Returns `true` if the phase represents an active wait (not running or
    /// terminal).
    pub fn is_waiting(&self) -> bool {
        matches!(
            self,
            WaitPhase::Queued
                | WaitPhase::Suspending
                | WaitPhase::Suspended
                | WaitPhase::Revalidating
                | WaitPhase::ManualWait
                | WaitPhase::AttentionRequired
        )
    }
}

/// Execution disposition of a waitable encounter, determined by the caller
/// based on whether execution effects have already been observed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitDisposition {
    /// No execution effects have occurred; the work can be retried from scratch.
    Queued,
    /// Execution effects have occurred; the work must be continued from
    /// recorded history rather than replayed.
    Suspended,
}

/// A waitable usage-limit encounter reported to the coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitEncounter {
    pub classification: LimitClassification,
    /// Execution disposition: queued (no effects) or suspended (effects observed).
    pub disposition: WaitDisposition,
    /// Known reset timestamp, if available.
    pub reset_at: Option<DateTime<Utc>>,
    /// `Retry-After` delay, if available.
    pub retry_after: Option<Duration>,
    /// Limit identifier, if known.
    pub limit_id: Option<String>,
}

/// Origin of a revalidation probe permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeOrigin {
    /// Automatically scheduled by the coordinator.
    Automatic,
    /// Manually requested by the user.
    Manual,
}

/// User-facing action applied to the current wait generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WaitAction {
    /// Manually trigger a revalidation probe now.
    RetryNow,
    /// Enable or disable automatic resume for the current generation.
    SetAutoResume { enabled: bool },
    /// Cancel the entire coordinated tree wait.
    Cancel,
}

/// Immutable snapshot of the coordinator state for a single wait generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitSnapshot {
    /// The causal work ID (root turn ID).
    pub work_id: String,
    /// Monotonic generation; stale callbacks compare against this.
    pub generation: u64,
    /// The thread that triggered the wait.
    pub trigger_thread_id: String,
    /// The turn that triggered the wait.
    pub trigger_turn_id: String,
    /// Current phase.
    pub phase: WaitPhase,
    /// Classification of the original encounter.
    pub classification: LimitClassification,
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
    pub manual_wait_reason: Option<ManualWaitReason>,
    /// Thread IDs registered as parked.
    pub parked_thread_ids: Vec<String>,
    /// Thread IDs expected to park before a probe is elected.
    pub expected_participants: Vec<String>,
    /// Thread ID of the elected revalidation leader, if any.
    pub leader_thread_id: Option<String>,
    /// Whether a manual retry has been requested and is pending election.
    pub manual_retry_requested: bool,
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
