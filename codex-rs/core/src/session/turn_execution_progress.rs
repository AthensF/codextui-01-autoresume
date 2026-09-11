//! Monotonic execution-progress tracking for safe usage-limit suspension.
//!
//! `TurnExecutionProgress` records what has happened during a turn so that
//! when a usage-limit 429 arrives mid-execution, the recovery orchestration
//! can decide whether it is safe to suspend and later resume the same
//! persisted turn context without replaying destructive work.
//!
//! All mutations are monotonic — flags only flip from `false` to `true` and
//! counters only increase. This ensures that stale callbacks or repeated
//! encounters cannot regress the safety picture.

use std::sync::Mutex;

/// Continuation safety classification for a turn at a model-request boundary.
///
/// This is the primary input to the recovery orchestration's disposition
/// decision. It distinguishes turns that have not yet touched the model from
/// turns that have produced durable, model-visible effects, and from turns
/// whose effects are ambiguous and must not be auto-resumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContinuationSafety {
    /// No substantive model output or side effects have occurred.
    ///
    /// The prompt has not been sent (or the response produced nothing
    /// durable). The turn can be safely queued — there is nothing to
    /// preserve and no risk of replay.
    Untouched,

    /// Substantive model output has been received and all side effects
    /// (if any) have completed with model-visible output.
    ///
    /// Suspension is safe: the turn can resume from the model boundary
    /// without replaying anything. Completed tool calls, hook effects,
    /// and compaction are durable and present in persisted history.
    SafeToSuspend,

    /// Side effects have occurred and are durable, but the turn has
    /// accumulated enough state that it should be suspended (not queued)
    /// to preserve completed work.
    ///
    /// This is the normal "mid-execution" state: tools have run, output
    /// is recorded, and the turn must not be restarted from scratch.
    EffectsObserved,

    /// Side effects are ambiguous — tools were dispatched but did not
    /// all produce model-visible output, or a drain identified futures
    /// that failed without a durable result.
    ///
    /// Automatic continuation is refused; the user must be alerted.
    AttentionRequired,
}

/// Report returned by `drain_in_flight` describing what was drained.
///
/// This allows the recovery orchestration to detect unsafe continuation
/// conditions: if any in-flight tool future failed without producing a
/// durable, model-visible output, the turn cannot be safely resumed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DrainReport {
    /// Number of tool futures that completed successfully with output.
    pub completed: u32,
    /// Number of tool futures that failed without a durable output.
    pub failed_without_output: u32,
    /// Number of tool futures that were still admitted (dispatched).
    pub admitted: u32,
}

impl DrainReport {
    /// Returns `true` if the drain is ambiguous — at least one future
    /// failed without producing a durable output.
    pub(crate) fn is_ambiguous(&self) -> bool {
        self.failed_without_output > 0
    }
}

/// Snapshot of the current execution progress for a turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TurnExecutionProgressSnapshot {
    /// At least one model request has been dispatched for this turn.
    pub model_request_dispatched: bool,
    /// At least one substantive model response item has been received
    /// (reasoning, text, or tool call) — not merely `Created` or headers.
    pub model_output_received: bool,
    /// At least one tool has been admitted (dispatched to the tool runner).
    pub tool_dispatched: bool,
    /// At least one tool has completed execution with model-visible output.
    pub tool_completed: bool,
    /// At least one completed hook effect has been applied that mutates
    /// context or world state (not merely preview/start events).
    pub hook_effect_applied: bool,
    /// A compaction request has been started for this turn.
    pub compaction_started: bool,
    /// A compaction request has completed.
    pub compaction_completed: bool,
    /// A tool was dispatched but did not produce a model-visible output
    /// (failed during drain or was cancelled mid-execution).
    pub missing_tool_output: bool,
    /// Number of model requests dispatched so far in this turn.
    pub model_request_count: u32,
    /// Number of tools dispatched so far in this turn.
    pub tool_dispatch_count: u32,
    /// Per-category counts of completed tool calls.
    pub completed_categories: CompletedToolCategories,
}

/// Per-category counts of completed tool calls with model-visible output.
///
/// Used by the replay-safety audit to produce a per-effect breakdown
/// for the durable recovery manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompletedToolCategories {
    /// Completed `exec_command` / shell tool calls.
    pub shell_commands: u32,
    /// Completed `apply_patch` / file-edit tool calls.
    pub file_edits: u32,
    /// Completed MCP tool calls (non-default namespace).
    pub mcp_calls: u32,
    /// Completed dynamic tool calls (default namespace, not exec/apply_patch).
    pub dynamic_tool_calls: u32,
}

impl CompletedToolCategories {
    /// Returns the total across all categories.
    pub(crate) fn total(&self) -> u32 {
        self.shell_commands + self.file_edits + self.mcp_calls + self.dynamic_tool_calls
    }
}

/// Monotonic execution-progress tracker for a single turn.
///
/// Owned by `TurnContext`. All updates are monotonic: flags only flip from
/// `false` to `true` and counters only increase. A `Snapshot` can be taken
/// at any point to capture the current safety picture.
#[derive(Debug, Default)]
pub(crate) struct TurnExecutionProgress {
    inner: Mutex<TurnExecutionProgressSnapshot>,
}

impl TurnExecutionProgress {
    /// Creates a new, empty progress tracker.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records that a model request has been dispatched.
    pub(crate) fn record_model_request_dispatched(&self) {
        let mut s = self.lock();
        s.model_request_dispatched = true;
        s.model_request_count = s.model_request_count.saturating_add(1);
    }

    /// Records that substantive model output (reasoning, text, or tool call)
    /// has been received. This is marked at `OutputItemDone` or equivalent,
    /// not at `Created` or header/metadata events.
    pub(crate) fn record_model_output_received(&self) {
        let mut s = self.lock();
        s.model_output_received = true;
    }

    /// Records that a tool has been admitted (dispatched to the tool runner)
    /// after the parallel-execution gate.  The tool name is used to classify
    /// the dispatch into a replay-safety category.
    pub(crate) fn record_tool_dispatched(&self, tool_name: &codex_protocol::ToolName) {
        let mut s = self.lock();
        s.tool_dispatched = true;
        s.tool_dispatch_count = s.tool_dispatch_count.saturating_add(1);
        if tool_name.is_default_namespace() {
            match tool_name.name.as_str() {
                "exec_command" | "write_stdin" => {
                    s.completed_categories.shell_commands =
                        s.completed_categories.shell_commands.saturating_add(1);
                }
                "apply_patch" => {
                    s.completed_categories.file_edits =
                        s.completed_categories.file_edits.saturating_add(1);
                }
                _ => {
                    s.completed_categories.dynamic_tool_calls =
                        s.completed_categories.dynamic_tool_calls.saturating_add(1);
                }
            }
        } else {
            s.completed_categories.mcp_calls = s.completed_categories.mcp_calls.saturating_add(1);
        }
    }

    /// Records that a tool has completed execution with model-visible output.
    pub(crate) fn record_tool_completed(&self) {
        let mut s = self.lock();
        s.tool_completed = true;
    }

    /// Records that a completed hook effect has been applied that mutates
    /// context or world state.
    pub(crate) fn record_hook_effect_applied(&self) {
        let mut s = self.lock();
        s.hook_effect_applied = true;
    }

    /// Records that a compaction request has started.
    pub(crate) fn record_compaction_started(&self) {
        let mut s = self.lock();
        s.compaction_started = true;
    }

    /// Records that a compaction request has completed.
    pub(crate) fn record_compaction_completed(&self) {
        let mut s = self.lock();
        s.compaction_completed = true;
    }

    /// Records that a tool was dispatched but did not produce a
    /// model-visible output (failed during drain or was cancelled).
    pub(crate) fn record_missing_tool_output(&self) {
        let mut s = self.lock();
        s.missing_tool_output = true;
    }

    /// Records the result of a `drain_in_flight` operation.
    pub(crate) fn record_drain_report(&self, report: &DrainReport) {
        let mut s = self.lock();
        if report.failed_without_output > 0 {
            s.missing_tool_output = true;
        }
    }

    /// Takes a snapshot of the current progress.
    pub(crate) fn snapshot(&self) -> TurnExecutionProgressSnapshot {
        self.lock().clone()
    }

    /// Classifies the current continuation safety for recovery decisions.
    pub(crate) fn continuation_safety(&self) -> ContinuationSafety {
        let s = self.lock();
        if s.missing_tool_output {
            return ContinuationSafety::AttentionRequired;
        }
        let has_side_effects =
            s.tool_dispatched || s.tool_completed || s.hook_effect_applied || s.compaction_started;
        if !s.model_output_received && !has_side_effects {
            // Nothing has happened yet — safe to queue.
            ContinuationSafety::Untouched
        } else if !has_side_effects {
            // Model output received but no tools/hooks/compaction — safe to
            // suspend at the model boundary.
            ContinuationSafety::SafeToSuspend
        } else if s.tool_dispatched && !s.tool_completed {
            // Tools dispatched but not all completed — ambiguous.
            ContinuationSafety::AttentionRequired
        } else {
            // Side effects occurred and are complete/durable.
            ContinuationSafety::EffectsObserved
        }
    }

    /// Returns `true` if no model output or tool effects have occurred yet,
    /// meaning the turn can be safely queued (not suspended) because there
    /// is nothing to preserve.
    pub(crate) fn is_pristine(&self) -> bool {
        matches!(self.continuation_safety(), ContinuationSafety::Untouched)
    }

    /// Returns `true` if model output has been received but no tools have
    /// been dispatched, meaning suspension is safe — the turn can resume
    /// from the model boundary without replaying anything.
    pub(crate) fn safe_to_suspend_at_model_boundary(&self) -> bool {
        matches!(
            self.continuation_safety(),
            ContinuationSafety::SafeToSuspend
        )
    }

    /// Returns `true` if tools have been dispatched or hook effects applied,
    /// meaning side effects may have occurred. Suspension is still possible
    /// but the recovery orchestration must wait for the tool drain boundary
    /// and must not replay completed tools.
    pub(crate) fn has_side_effects(&self) -> bool {
        let s = self.lock();
        s.tool_dispatched || s.tool_completed || s.hook_effect_applied
    }

    /// Returns `true` if side effects are ambiguous — tools were dispatched
    /// but not all have completed, or a drain identified missing output.
    pub(crate) fn side_effects_ambiguous(&self) -> bool {
        matches!(
            self.continuation_safety(),
            ContinuationSafety::AttentionRequired
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TurnExecutionProgressSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
