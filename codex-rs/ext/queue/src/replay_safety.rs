//! Replay-safety audit for suspended-tree hand-off.
//!
//! This module documents the explicit replay-safety audit for every
//! tool/runtime category and provides the classification logic used to
//! determine whether automatic durable recovery is safe.
//!
//! ## Audit categories
//!
//! | Category | Replay-safe? | Reason |
//! |----------|-------------|--------|
//! | Completed shell commands | Yes (if flushed) | Output is in history and `executed_tool_calls` |
//! | File edits / apply-patch | Yes (if flushed) | Effect is captured in history items |
//! | MCP tool calls | Yes (if flushed) | Output is in history |
//! | Dynamic tool calls | Yes (if flushed) | Output is in history |
//! | Pending approvals | No | Interactive state cannot be serialised |
//! | Interactive input (elicitation) | No | Local-only handle, not durable |
//! | Background terminals | No | Pseudo-terminal state is not serialisable |
//! | Unverifiable remote effects | No | Cannot prove idempotency |
//!
//! The audit is enforced by [`classify_replay_safety`], which is called
//! during tree suspension to populate [`ReplaySafetySummary`] in the
//! durable recovery manifest.

use crate::payload::ReplayEffectCategories;
use crate::payload::ReplaySafetySummary;

/// Categories of tool/runtime effects audited for replay safety.
///
/// Each variant maps to a rule in the replay-safety audit.  The audit
/// determines whether completed effects are safely captured in durable
/// history (and thus can be skipped on recovery) or whether pending
/// effects block automatic recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplaySafetyCategory {
    /// Completed shell-command output.  Safe when the output is flushed
    /// to history and `executed_tool_calls` before hand-off.
    CompletedShellCommand,
    /// File edit / apply-patch.  Safe when the edit item is flushed to
    /// history before hand-off.
    CompletedFileEdit,
    /// MCP tool call.  Safe when the output is flushed to history.
    CompletedMcpTool,
    /// Dynamic tool call.  Safe when the output is flushed to history.
    CompletedDynamicTool,
    /// Pending approval request.  Not safe — interactive state cannot
    /// be serialised across process restarts.
    PendingApproval,
    /// Interactive input (elicitation).  Not safe — local-only handle.
    InteractiveInput,
    /// Background terminal session.  Not safe — pseudo-terminal state
    /// is not serialisable.
    BackgroundTerminal,
    /// Unverifiable remote effect (e.g. webhook, external API call).
    /// Not safe — cannot prove idempotency.
    UnverifiableRemoteEffect,
}

/// Result of classifying a single replay-safety category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CategorySafety {
    /// The category is safe for automatic recovery.
    Safe,
    /// The category blocks automatic recovery.
    BlocksRecovery,
}

/// Classifies a single replay-safety category.
pub(crate) fn classify_category(category: ReplaySafetyCategory) -> CategorySafety {
    match category {
        ReplaySafetyCategory::CompletedShellCommand
        | ReplaySafetyCategory::CompletedFileEdit
        | ReplaySafetyCategory::CompletedMcpTool
        | ReplaySafetyCategory::CompletedDynamicTool => CategorySafety::Safe,
        ReplaySafetyCategory::PendingApproval
        | ReplaySafetyCategory::InteractiveInput
        | ReplaySafetyCategory::BackgroundTerminal
        | ReplaySafetyCategory::UnverifiableRemoteEffect => CategorySafety::BlocksRecovery,
    }
}

/// Classifies the overall replay safety for a suspended tree.
///
/// Returns [`ReplaySafetySummary::Safe`] when all audited categories are
/// safe, or [`ReplaySafetySummary::AttentionRequired`] when any category
/// blocks automatic recovery.
pub(crate) fn classify_replay_safety(
    blocking_categories: &[ReplaySafetyCategory],
    verified_tool_calls: usize,
) -> ReplaySafetySummary {
    let blocking: Vec<ReplaySafetyCategory> = blocking_categories
        .iter()
        .copied()
        .filter(|c| matches!(classify_category(*c), CategorySafety::BlocksRecovery))
        .collect();
    if blocking.is_empty() {
        ReplaySafetySummary::Safe {
            verified_tool_calls,
            categories: ReplayEffectCategories::default(),
        }
    } else {
        let reason = format!(
            "blocking categories: {}",
            blocking
                .iter()
                .map(|c| format!("{c:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        ReplaySafetySummary::AttentionRequired { reason }
    }
}

#[cfg(test)]
#[path = "replay_safety_tests.rs"]
mod tests;
