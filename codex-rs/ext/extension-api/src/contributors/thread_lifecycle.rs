use std::sync::Arc;

use crate::ExtensionData;
use crate::ExtensionMetrics;
use codex_mcp::McpResourceClient;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::TurnEnvironmentSelection;

/// Trusted, host-resolved billing attribution for a thread.
///
/// Extensions may forward this value to first-party APIs. It is seeded by Core
/// after resolving persisted and host-provided originator state, rather than
/// from model- or tool-controlled input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadOriginator(pub String);

/// Input supplied when the host starts a runtime for a thread.
pub struct ThreadStartInput<'a, C> {
    /// Host configuration visible at thread start.
    pub config: &'a C,
    /// Source that created the session for this thread.
    pub session_source: &'a SessionSource,
    /// Whether persistent thread-scoped state is available for this thread.
    pub persistent_thread_state_available: bool,
    /// Execution environments selected for this thread.
    pub environments: &'a [TurnEnvironmentSelection],
    /// MCP resource access supplied by the host for this session.
    pub mcp_resource_client: Option<Arc<McpResourceClient>>,
    /// Session-attributed metrics supplied by the host.
    pub extension_metrics: Option<Arc<dyn ExtensionMetrics>>,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}

/// Input supplied after the host has registered a fully initialized thread.
pub struct ThreadReadyInput<'a, C> {
    /// Host configuration visible after thread registration.
    pub config: &'a C,
    /// Source that created the session for this thread.
    pub session_source: &'a SessionSource,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}

/// Input supplied when the host resumes an existing thread.
pub struct ThreadResumeInput<'a> {
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}

/// Why a thread has no immediately pending work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadIdleCause {
    /// The previous turn completed and automatic follow-up work can run.
    Completed,
    /// The user interrupted the previous turn.
    Interrupted,
    /// The previous turn ended with a terminal error.
    Failed,
}

/// Input supplied when the host has no immediately pending thread work.
pub struct ThreadIdleInput<'a> {
    /// Why the thread became idle.
    pub cause: ThreadIdleCause,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}

/// Input supplied when the host stops a thread runtime.
pub struct ThreadStopInput<'a> {
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}

/// One member of a suspended agent tree, excluding the root.
///
/// Core populates this from the live agent subtree before emitting
/// [`TurnSuspendedInput`].  The queue extension converts it to its
/// internal `SuspendedTreeMember` representation when building the
/// durable recovery manifest.
#[derive(Clone, Debug)]
pub struct SuspendedMemberInfo {
    /// The member's thread ID.
    pub thread_id: String,
    /// The turn ID to resume on this member.
    pub turn_id: String,
    /// Parent thread ID in the agent tree.
    pub parent_thread_id: String,
    /// Parent turn ID that spawned this member.
    pub parent_turn_id: String,
    /// Root turn ID shared by the entire tree.
    pub root_turn_id: String,
    /// Agent path from root to this member (e.g. `"root.child"`).
    pub agent_path: String,
    /// Per-member replay-safety classification derived from the member's
    /// own `TurnExecutionProgress`.
    pub replay_safety: SuspendedReplaySafety,
}

/// Per-category counts of completed, model-visible tool effects.
///
/// These categories let the recovery manifest distinguish flushed shell
/// commands from file edits, MCP calls, and dynamic tool calls so the
/// scheduler and the user can audit what will be replayed.
#[derive(Clone, Debug, Default)]
pub struct ReplayEffectCategories {
    /// Completed `exec_command` / shell tool calls flushed to history.
    pub shell_commands: usize,
    /// Completed `apply_patch` / file-edit tool calls flushed to history.
    pub file_edits: usize,
    /// Completed MCP tool calls flushed to history.
    pub mcp_calls: usize,
    /// Completed dynamic tool calls flushed to history.
    pub dynamic_tool_calls: usize,
}

impl ReplayEffectCategories {
    /// Returns the total across all categories.
    pub fn total(&self) -> usize {
        self.shell_commands + self.file_edits + self.mcp_calls + self.dynamic_tool_calls
    }
}

/// Replay-safety classification for a suspended tree.
///
/// Core derives this from the `TurnExecutionProgress` of the suspended
/// turn.  The queue extension converts it to its internal
/// `ReplaySafetySummary` when building the durable recovery manifest.
#[derive(Clone, Debug)]
pub enum SuspendedReplaySafety {
    /// All tool outputs are flushed to history.  Automatic recovery is safe.
    Safe {
        /// Number of completed tool calls verified in history.
        verified_tool_calls: usize,
        /// Per-category breakdown of the verified tool calls.
        categories: ReplayEffectCategories,
    },
    /// One or more tool/hook outcomes are ambiguous.  Automatic recovery
    /// is blocked; the user must manually intervene.
    AttentionRequired {
        /// Human-readable description of the ambiguous effects.
        reason: String,
    },
}

/// Input supplied when the host suspends an unfinished turn for durable
/// usage-limit hand-off.
///
/// The queue extension uses this callback to enqueue a
/// `SuspendedTreeRecoveryManifest` so a replacement process can recover
/// the suspended turn after the usage-limit reset window elapses.
pub struct TurnSuspendedInput<'a> {
    /// The thread whose turn was suspended.
    pub thread_id: &'a str,
    /// The turn ID to resume on recovery.
    pub turn_id: &'a str,
    /// Epoch-millis deadline after which the recovery job becomes due.
    ///
    /// Typically the usage-limit reset time.  A value of zero means
    /// "due immediately".
    pub not_before_ms: i64,
    /// Live member threads to recover after the root, in parent-first
    /// order.  Excludes the root thread itself.
    pub members: &'a [SuspendedMemberInfo],
    /// Coordinator generation at the time of suspension, used to fence
    /// stale callbacks from the previous process.
    pub coordinator_generation: u64,
    /// Replay-safety classification of the suspended tree.
    pub replay_safety: &'a SuspendedReplaySafety,
    /// Serialized usage-limit coordinator snapshot at the time of
    /// suspension, if a wait was active.  A replacement process
    /// deserialises this to rehydrate the coordinator so unloaded
    /// threads can report their usage-limit status without a live
    /// encounter.
    pub usage_limit_snapshot: Option<&'a serde_json::Value>,
    /// Store scoped to the host session runtime.
    pub session_store: &'a ExtensionData,
    /// Store scoped to this thread runtime.
    pub thread_store: &'a ExtensionData,
}
