//! Usage-limit coordinator integration for the agent tree.
//!
//! This module bridges the generic `codex-usage-limits` coordinator with
//! `codex-core` configuration types. The coordinator is owned by
//! `AgentControl` and shared across the entire agent tree for a root
//! session, enabling tree-wide usage-limit wait coordination.

use codex_config::types::UsageLimitsConfig;
use codex_usage_limits::UsageLimitCoordinator;
use codex_usage_limits::UsageLimitPolicy;
use std::sync::Arc;
use std::time::Duration;

/// Converts the resolved `UsageLimitsConfig` into a `UsageLimitPolicy`
/// for the coordinator.
pub(crate) fn policy_from_config(config: &UsageLimitsConfig) -> UsageLimitPolicy {
    UsageLimitPolicy {
        queue_when_limited: config.queue_when_limited,
        auto_resume: config.auto_resume,
        max_auto_resumes: config.max_auto_resumes,
        max_availability_checks: config.max_availability_checks,
        unknown_reset_base_delay: Duration::from_secs(config.unknown_reset_base_delay_seconds),
        unknown_reset_max_interval: Duration::from_secs(config.unknown_reset_max_interval_seconds),
        unknown_reset_max_wait: Duration::from_secs(config.unknown_reset_max_wait_seconds),
    }
}

/// Creates a new coordinator from the resolved config.
pub(crate) fn coordinator_from_config(config: &UsageLimitsConfig) -> Arc<UsageLimitCoordinator> {
    let policy = policy_from_config(config);
    UsageLimitCoordinator::new(policy, /* jitter_seed */ 0)
}

/// Creates a default coordinator for tests and default `AgentControl`.
pub(crate) fn default_coordinator() -> Arc<UsageLimitCoordinator> {
    UsageLimitCoordinator::new(UsageLimitPolicy::default(), /* jitter_seed */ 0)
}
