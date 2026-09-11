//! Generic usage-limit wait coordinator for the Codex agent tree.
//!
//! This crate owns the pure state machine, scheduling math, leader election,
//! and Tokio notification/timer integration for usage-limit queueing and
//! auto-resume. It is intentionally independent of `codex-core` thread types
//! and uses opaque string IDs so it can be reused by the TUI, app-server,
//! and future durable phases.
//!
//! See the implementation plan at
//! `/Users/athens/.devin/plans/plan-df12cd0ec00a8fa0.md` for the full
//! roadmap.

mod coordinator;
mod machine;
mod policy;
mod state;

pub use coordinator::ResumeProbePermit;
pub use coordinator::UsageLimitCoordinator;
pub use coordinator::WaitCancelled;
pub use machine::ActionResult;
pub use machine::ElectionResult;
pub use policy::DeterministicJitter;
pub use policy::UsageLimitPolicy;
pub use state::LimitClassification;
pub use state::LimitEncounter;
pub use state::ManualWaitReason;
pub use state::ProbeOrigin;
pub use state::WaitAction;
pub use state::WaitDisposition;
pub use state::WaitPhase;
pub use state::WaitSnapshot;

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
