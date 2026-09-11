//! Versioned, backward-compatible queue payload envelope.
//!
//! Legacy rows (written before this module existed) store raw `TurnInput`
//! JSON directly in `queued_items.payload_json`.  Those rows are decoded as
//! version 0 (`QueuePayload::V0`) by [`decode_payload`].
//!
//! New rows wrap the input in a [`QueueEnvelope`] that carries an explicit
//! schema version and job kind.  Today the supported job kinds are
//! [`QueueJobKind::UserMessage`] and [`QueueJobKind::SuspendedTreeRecovery`].

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use codex_core::TurnInput;
use codex_protocol::ThreadId;

/// Current envelope schema version produced by this code.
pub const CURRENT_PAYLOAD_VERSION: u32 = 1;

/// Discriminates queue job kinds inside the versioned envelope.
///
/// `UserMessage` is the ordinary queued user input.  `SuspendedTreeRecovery`
/// is a durable recovery job written when an active agent tree is suspended
/// for usage-limit hand-off; it is never disguised as a fresh user message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum QueueJobKind {
    /// Ordinary user message waiting to start on its thread.
    UserMessage { input: TurnInput },
    /// Durable recovery job for a suspended agent tree.
    ///
    /// Carries the root/member thread-turn relationships, coordinator
    /// generation, scheduling deadline, and replay-safety summary needed
    /// to reconstruct the tree and continue from persisted history.
    SuspendedTreeRecovery(SuspendedTreeRecoveryManifest),
}

/// Versioned queue payload envelope.
///
/// Stored as JSON in `queued_items.payload_json`.  Legacy rows that predate
/// the envelope are decoded as [`QueuePayload::V0`] by [`decode_payload`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "version")]
pub enum QueuePayload {
    /// Version 0 — raw `TurnInput` JSON written before the envelope existed.
    ///
    /// This variant is only produced by [`decode_payload`]; new rows always
    /// use [`QueuePayload::V1`].
    V0(TurnInput),
    /// Version 1 — explicit envelope with job kind.
    V1(QueueEnvelope),
}

/// Inner envelope carried by [`QueuePayload::V1`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueEnvelope {
    pub job: QueueJobKind,
}

/// Manifest for recovering a suspended agent tree.
///
/// Captures the durable state needed to reconstruct an active multi-agent
/// tree after a process crash or graceful suspension.  The root thread is
/// recovered first, then descendants in parent-first order, preserving
/// original turn IDs and replay-safety metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuspendedTreeRecoveryManifest {
    /// The root thread whose unfinished turn triggered suspension.
    pub root_thread_id: ThreadId,
    /// The turn ID to resume on the root thread.
    pub root_turn_id: String,
    /// The original user input that started the root turn, if available.
    ///
    /// Used to reconstruct the turn context without replaying history.
    pub original_input: Option<TurnInput>,
    /// Member threads to recover after the root, in dependency order.
    ///
    /// Parents appear before children so recovery can load parent-first.
    pub members: Vec<SuspendedTreeMember>,
    /// Coordinator generation at the time of suspension.
    ///
    /// Used to fence stale callbacks from the previous process.
    pub coordinator_generation: u64,
    /// Scheduling deadline in unix milliseconds.
    ///
    /// The recovery job becomes due at this time.  Stored redundantly
    /// with the queue row's `not_before_ms` so the manifest is
    /// self-contained for recovery auditing.
    pub not_before_ms: i64,
    /// Replay-safety classification of the suspended tree.
    pub replay_safety: ReplaySafetySummary,
    /// Serialized usage-limit coordinator snapshot at the time of
    /// suspension, if a wait was active.  A replacement process
    /// deserialises this to rehydrate the coordinator.
    pub usage_limit_snapshot: Option<Value>,
}

/// A member thread in a suspended agent tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuspendedTreeMember {
    /// The member's thread ID.
    pub thread_id: ThreadId,
    /// The turn ID to resume on this member.
    pub turn_id: String,
    /// Parent thread ID in the agent tree.
    pub parent_thread_id: ThreadId,
    /// Parent turn ID that spawned this member.
    pub parent_turn_id: String,
    /// Root turn ID shared by the entire tree.
    pub root_turn_id: String,
    /// Agent path from root to this member (e.g. `"root.child"`).
    pub agent_path: String,
    /// Per-member replay-safety classification.
    pub replay_safety: ReplaySafetySummary,
}

/// Per-category counts of completed, model-visible tool effects.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

/// Replay-safety classification for a suspended tree.
///
/// Determines whether automatic recovery is safe or requires manual
/// intervention.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum ReplaySafetySummary {
    /// All tool outputs are flushed to history and `executed_tool_calls`.
    /// Automatic recovery is safe.
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

impl QueuePayload {
    /// Encodes a user-message job at the current version.
    pub fn user_message(input: TurnInput) -> Result<String, serde_json::Error> {
        let payload = QueuePayload::V1(QueueEnvelope {
            job: QueueJobKind::UserMessage { input },
        });
        serde_json::to_string(&payload)
    }

    /// Encodes a suspended-tree recovery job at the current version.
    pub fn suspended_tree_recovery(
        manifest: SuspendedTreeRecoveryManifest,
    ) -> Result<String, serde_json::Error> {
        let payload = QueuePayload::V1(QueueEnvelope {
            job: QueueJobKind::SuspendedTreeRecovery(manifest),
        });
        serde_json::to_string(&payload)
    }

    /// Returns the inner `TurnInput` for user-message jobs, or `None` for
    /// other job kinds (e.g. suspended-tree recovery).
    pub fn into_turn_input(self) -> Option<TurnInput> {
        match self {
            QueuePayload::V0(input) => Some(input),
            QueuePayload::V1(envelope) => match envelope.job {
                QueueJobKind::UserMessage { input } => Some(input),
                QueueJobKind::SuspendedTreeRecovery(_) => None,
            },
        }
    }

    /// Returns the suspended-tree recovery manifest if this payload carries
    /// one, otherwise `None`.
    pub fn into_suspended_tree_recovery(self) -> Option<SuspendedTreeRecoveryManifest> {
        match self {
            QueuePayload::V0(_) => None,
            QueuePayload::V1(envelope) => match envelope.job {
                QueueJobKind::UserMessage { .. } => None,
                QueueJobKind::SuspendedTreeRecovery(manifest) => Some(manifest),
            },
        }
    }
}

/// Decodes a raw `payload_json` column value into a [`QueuePayload`].
///
/// Legacy rows store raw `TurnInput` JSON (no `version` tag).  When the value
/// does not contain a `"version"` field it is treated as version 0.
/// Otherwise it is decoded as the versioned envelope.
pub fn decode_payload(payload_json: &str) -> Result<QueuePayload, serde_json::Error> {
    let value: Value = serde_json::from_str(payload_json)?;
    if value.get("version").is_some() {
        return serde_json::from_value(value).map(QueuePayload::V1);
    }
    let input: TurnInput = serde_json::from_value(value)?;
    Ok(QueuePayload::V0(input))
}

/// Decodes a payload and extracts the inner `TurnInput`.
///
/// Returns `None` when the payload is valid JSON but does not contain a
/// user-message job (e.g. suspended-tree recovery jobs do not carry a
/// `TurnInput`).
pub fn decode_turn_input(payload_json: &str) -> Result<Option<TurnInput>, serde_json::Error> {
    decode_payload(payload_json).map(QueuePayload::into_turn_input)
}

/// Decodes a payload and extracts the suspended-tree recovery manifest.
///
/// Returns `None` when the payload does not carry a recovery job.
pub fn decode_suspended_tree_recovery(
    payload_json: &str,
) -> Result<Option<SuspendedTreeRecoveryManifest>, serde_json::Error> {
    decode_payload(payload_json).map(QueuePayload::into_suspended_tree_recovery)
}

#[cfg(test)]
#[path = "payload_tests.rs"]
mod tests;
