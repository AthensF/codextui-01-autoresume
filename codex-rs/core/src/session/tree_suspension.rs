//! Sealed tree suspension safety checks.
//!
//! When an agent tree is suspended for usage-limit hand-off, every live
//! member must be at a safe model boundary with all model-visible tool
//! outputs flushed.  This module provides the safety checks that determine
//! whether a tree can be safely suspended and later recovered without
//! replaying destructive side effects.
//!
//! The flow is:
//!
//! 1. [`seal_and_check_tree_safety`] seals the tree admission barrier
//!    (preventing new spawns/turns) and collects the continuation safety of
//!    every live member.
//! 2. If any member has ambiguous side effects, the result is
//!    [`TreeSuspensionSafety::AttentionRequired`] and automatic recovery is
//!    refused.
//! 3. Otherwise the result is [`TreeSuspensionSafety::Safe`] with the
//!    ordered list of member thread IDs (children before parents) for
//!    child-first flush and shutdown.

use super::turn_execution_progress::ContinuationSafety;
use crate::agent::control::AgentControl;
use codex_protocol::ThreadId;
use codex_protocol::error::Result as CodexResult;

/// Per-member safety state collected during tree suspension.
#[derive(Clone, Debug)]
pub(crate) struct MemberSafetyState {
    pub thread_id: ThreadId,
    pub continuation_safety: ContinuationSafety,
}

/// Result of checking whether a tree can be safely suspended.
#[derive(Clone, Debug)]
pub(crate) enum TreeSuspensionSafety {
    /// All members are at safe model boundaries.  The members are ordered
    /// children-first so the caller can flush and shut down in dependency
    /// order.
    Safe { members: Vec<MemberSafetyState> },
    /// One or more members have ambiguous side effects.  Automatic
    /// recovery is refused; the user must be alerted.
    AttentionRequired {
        ambiguous_members: Vec<ThreadId>,
        reason: String,
    },
}

/// Seals the tree admission barrier and checks every live member's
/// continuation safety.
///
/// The barrier is sealed before collecting member states so no new spawns
/// or turn triggers can slip through while the safety picture is being
/// assembled.  The barrier remains sealed after this call returns; the
/// caller is responsible for unsealing it if suspension is aborted.
///
/// Members are returned in children-first order (descendants before root)
/// so the caller can flush and shut down writers in dependency order.
pub(crate) async fn seal_and_check_tree_safety(
    agent_control: &AgentControl,
    root_thread_id: ThreadId,
) -> CodexResult<TreeSuspensionSafety> {
    // Seal the tree admission barrier to prevent new spawns/turns while
    // we are assessing suspension safety.
    agent_control.tree_barrier().seal();

    // Collect all live member thread IDs (root first, then descendants).
    let mut thread_ids = agent_control
        .list_live_agent_subtree_thread_ids(root_thread_id)
        .await?;

    // Reverse to children-first order for flush/shutdown ordering.
    thread_ids.reverse();

    let mut members = Vec::with_capacity(thread_ids.len());
    let mut ambiguous_members = Vec::new();

    for thread_id in &thread_ids {
        let safety = agent_control
            .get_continuation_safety(thread_id.clone())
            .await;
        if matches!(safety, ContinuationSafety::AttentionRequired) {
            ambiguous_members.push(thread_id.clone());
        }
        members.push(MemberSafetyState {
            thread_id: thread_id.clone(),
            continuation_safety: safety,
        });
    }

    if !ambiguous_members.is_empty() {
        return Ok(TreeSuspensionSafety::AttentionRequired {
            ambiguous_members,
            reason: "one or more tree members have ambiguous side effects".to_string(),
        });
    }

    Ok(TreeSuspensionSafety::Safe { members })
}

/// Builds a human-readable reason for an attention-required outcome.
pub(crate) fn attention_required_reason(ambiguous: &[ThreadId]) -> String {
    format!(
        "ambiguous side effects in tree members: {}",
        ambiguous
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
#[path = "tree_suspension_tests.rs"]
mod tests;
