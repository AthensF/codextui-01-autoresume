use super::handlers;
use super::session::Session;
use super::tree_suspension::MemberSafetyState;
use super::turn_execution_progress::ContinuationSafety;
use super::turn_execution_progress::TurnExecutionProgressSnapshot;
use crate::agent::control::AgentControl;
use crate::state::TaskKind;
use codex_extension_api::SuspendedMemberInfo;
use codex_extension_api::SuspendedReplaySafety;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::SuspendTurnOutcome;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub(super) async fn suspend_turn_and_shutdown(
    session: &Arc<Session>,
    submission_id: String,
    not_before_ms: i64,
    members: &[SuspendedMemberInfo],
) -> CodexResult<SuspendTurnOutcome> {
    let progress_snapshot = {
        let active = session.active_turn.lock().await;
        let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
        // Check the root turn's continuation safety before suspending.
        // Ambiguous side effects must never be auto-recovered.
        let safety = task.turn_context.execution_progress.continuation_safety();
        if matches!(safety, ContinuationSafety::AttentionRequired) {
            return Ok(SuspendTurnOutcome::AttentionRequired {
                reason: "root turn has ambiguous side effects".to_string(),
            });
        }
        task.turn_context.execution_progress.snapshot()
    };

    // This is a snapshot of currently loaded descendants, not a spawn-admission seal.
    // Previously closed descendants and concurrent future spawns remain best effort.
    if session
        .services
        .agent_control
        .list_live_agent_subtree_thread_ids(session.thread_id)
        .await?
        .len()
        > 1
    {
        return Ok(SuspendTurnOutcome::HasLiveDescendants);
    }

    let live_thread = session
        .live_thread_for_persistence("suspend an unfinished root turn")
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
    // Flush before canceling execution so a persistence failure leaves the original turn running.
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush before root turn suspension failed: {error}"))
    })?;

    // The flush can yield while the active turn completes or changes. Recheck its
    // kind under the same lock used to remove it.
    let mut turn = {
        let mut active = session.active_turn.lock().await;
        let Some(active_turn) = active.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        let Some(task) = active_turn.task.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
        active.take().ok_or_else(|| {
            CodexErr::Fatal("accepted root turn suspension had no running turn".to_string())
        })?
    };

    let task = turn.task.take().ok_or_else(|| {
        CodexErr::Fatal("accepted root turn suspension had no running task".to_string())
    })?;
    let turn_id = task.turn_context.sub_id.clone();
    // Normal shutdown records a terminal turn event, preventing another worker from
    // recovering this turn under its original ID. Cancel the task without that event.
    task.cancellation_token.cancel();
    task.turn_context
        .turn_metadata_state
        .cancel_git_enrichment_task();
    let mut task_handle = task.handle.detach();
    match tokio::time::timeout(
        Duration::from_millis(crate::tasks::GRACEFULL_INTERRUPTION_TIMEOUT_MS),
        &mut task_handle,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(thread_id = %session.thread_id, %error, "suspended turn task exited abnormally");
        }
        Err(_) => {
            warn!(
                thread_id = %session.thread_id,
                "suspended turn task did not stop gracefully; aborting it"
            );
            task_handle.abort();
            let _ = task_handle.await;
        }
    }
    // Pending accepted input and interactive waiters live only in this process. Handoff
    // intentionally drops that state; persisting or replaying it needs a separate protocol.
    session.input_queue.clear_pending(&turn).await;

    // Stop all producers before flushing their final history and closing its writer.
    // If either persistence step fails, do not report success: the current worker
    // retains ownership until worker-failure recovery can take responsibility.
    handlers::shutdown_session_runtime(session).await;
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush after root turn suspension failed: {error}"))
    })?;
    live_thread.shutdown().await.map_err(|error| {
        CodexErr::Fatal(format!("close suspended root turn writer failed: {error}"))
    })?;
    // Announce thread shutdown only after its writer closes so a replacement worker
    // cannot write the same thread concurrently.
    handlers::emit_thread_stop_lifecycle(session.as_ref()).await;
    session
        .deliver_event_raw(Event {
            id: submission_id,
            msg: EventMsg::ShutdownComplete,
        })
        .await;
    // Compute the coordinator generation and replay-safety classification from
    // durable state captured before cancellation.  The coordinator snapshot is
    // read after shutdown so the generation reflects the wait that triggered
    // the suspension; if no wait is active (e.g. graceful shutdown without a
    // usage limit), generation 0 is used.
    let coordinator_generation = session
        .services
        .agent_control
        .usage_limit_coordinator()
        .snapshot()
        .await
        .map(|s| s.generation)
        .unwrap_or_default();
    let replay_safety = continuation_safety_to_replay_safety(&progress_snapshot);
    // Notify extensions (e.g. the queue extension) so a recovery manifest
    // is enqueued for durable hand-off.
    session
        .emit_turn_suspended_lifecycle(
            &session.thread_id.to_string(),
            &turn_id,
            not_before_ms,
            members,
            coordinator_generation,
            &replay_safety,
        )
        .await;
    Ok(SuspendTurnOutcome::Suspended { turn_id })
}

/// Suspends an entire agent tree child-first.
///
/// This is the internal tree manifest operation that generalises
/// [`suspend_turn_and_shutdown`] for multi-agent trees.  It:
///
/// 1. Seals the tree admission barrier to prevent new spawns/turns.
/// 2. Checks every live member's continuation safety.
/// 3. Returns [`SuspendTurnOutcome::AttentionRequired`] if any member has
///    ambiguous side effects.
/// 4. Flushes and shuts down all descendants child-first.
/// 5. Then suspends the root via the existing single-root path.
///
/// The existing [`suspend_turn_and_shutdown`] is preserved for
/// compatibility; this function is called only when tree-level suspension
/// is required.
pub(super) async fn suspend_tree_and_shutdown(
    session: &Arc<Session>,
    submission_id: String,
    not_before_ms: i64,
) -> CodexResult<SuspendTurnOutcome> {
    // Seal the tree barrier and check all members' continuation safety.
    let safety = super::tree_suspension::seal_and_check_tree_safety(
        &session.services.agent_control,
        session.thread_id,
    )
    .await?;

    let members = match safety {
        super::tree_suspension::TreeSuspensionSafety::Safe { members } => members,
        super::tree_suspension::TreeSuspensionSafety::AttentionRequired {
            ambiguous_members,
            reason: _,
        } => {
            let reason = super::tree_suspension::attention_required_reason(&ambiguous_members);
            // Unseal the barrier so the session can continue accepting spawns.
            session.services.agent_control.tree_barrier().unseal();
            return Ok(SuspendTurnOutcome::AttentionRequired { reason });
        }
    };

    // Collect durable member info before shutting down descendants so the
    // recovery manifest captures real parent/child relationships and turn IDs.
    let suspended_members =
        collect_suspended_members(&session.services.agent_control, &members, session.thread_id)
            .await;

    // Flush and shut down descendants child-first (members are already
    // ordered children-first by seal_and_check_tree_safety).
    let root_thread_id = session.thread_id;
    for member in &members {
        if member.thread_id == root_thread_id {
            continue;
        }
        let Some(state) = session.services.agent_control.upgrade_manager_state() else {
            continue;
        };
        let Ok(thread) = state.get_thread(member.thread_id.clone()).await else {
            continue;
        };
        // Flush the descendant's history before shutting it down.
        if let Err(error) = thread.flush_rollout().await {
            tracing::warn!(
                thread_id = %member.thread_id,
                %error,
                "failed to flush descendant during tree suspension"
            );
        }
        // Shut down the descendant's session.
        if let Err(error) = thread.shutdown_and_wait().await {
            tracing::warn!(
                thread_id = %member.thread_id,
                %error,
                "failed to shut down descendant during tree suspension"
            );
        }
        // Remove the descendant from the live thread map so the root's
        // `list_live_agent_subtree_thread_ids` check no longer sees it.
        state.remove_thread(&member.thread_id).await;
    }

    // Now suspend the root via the existing single-root path, passing the
    // pre-collected member info so the recovery manifest is complete.
    suspend_turn_and_shutdown(session, submission_id, not_before_ms, &suspended_members).await
}

/// Converts a `TurnExecutionProgressSnapshot` into a `SuspendedReplaySafety`
/// classification suitable for the durable recovery manifest.
///
/// `AttentionRequired` is already rejected earlier in the suspension path,
/// so this helper only classifies the remaining safe states.  The
/// `verified_tool_calls` count is derived from the progress snapshot: when
/// all dispatched tools completed without missing output, the dispatch
/// count is the verified count.
fn continuation_safety_to_replay_safety(
    snapshot: &TurnExecutionProgressSnapshot,
) -> SuspendedReplaySafety {
    if snapshot.missing_tool_output {
        return SuspendedReplaySafety::AttentionRequired {
            reason: "root turn has missing tool output".to_string(),
        };
    }
    let verified_tool_calls = if snapshot.tool_completed {
        snapshot.tool_dispatch_count as usize
    } else {
        0
    };
    SuspendedReplaySafety::Safe {
        verified_tool_calls,
    }
}

/// Collects `SuspendedMemberInfo` for each non-root member of a suspended tree.
///
/// Members are enriched with their active turn ID, parent thread/turn IDs,
/// root turn ID, and agent path from the live thread state.  Members whose
/// threads are not currently loaded are skipped; the caller is responsible
/// for handling their recovery separately.
async fn collect_suspended_members(
    agent_control: &AgentControl,
    members: &[MemberSafetyState],
    root_thread_id: ThreadId,
) -> Vec<SuspendedMemberInfo> {
    let mut info = Vec::new();
    for member in members {
        if member.thread_id == root_thread_id {
            continue;
        }
        let Some(state) = agent_control.upgrade_manager_state() else {
            continue;
        };
        let Ok(thread) = state.get_thread(member.thread_id.clone()).await else {
            tracing::warn!(
                thread_id = %member.thread_id,
                "skipping unloaded tree member during manifest collection"
            );
            continue;
        };
        // Extract the active turn ID and root turn ID from the member's
        // active turn context.
        let (turn_id, root_turn_id, parent_turn_id) = {
            let active = thread.session.active_turn.lock().await;
            let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
                continue;
            };
            (
                task.turn_context.sub_id.clone(),
                task.turn_context
                    .turn_metadata_state
                    .root_turn_id()
                    .unwrap_or_default(),
                task.turn_context
                    .turn_metadata_state
                    .parent_turn_id()
                    .unwrap_or_default(),
            )
        };
        let parent_thread_id = thread
            .session_source
            .parent_thread_id()
            .map(|id| id.to_string())
            .unwrap_or_default();
        let agent_path = thread
            .session_source
            .get_agent_path()
            .map(|p| p.to_string())
            .unwrap_or_default();
        info.push(SuspendedMemberInfo {
            thread_id: member.thread_id.to_string(),
            turn_id,
            parent_thread_id,
            parent_turn_id,
            root_turn_id,
            agent_path,
        });
    }
    info
}
