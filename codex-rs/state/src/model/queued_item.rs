use anyhow::Result;
use codex_protocol::ThreadId;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

/// Scheduling state of a queued item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedItemState {
    /// Item is waiting to be dispatched (possibly with a future `not_before_ms`).
    Pending,
    /// Item is blocked by a usage limit or other transient condition.
    Blocked,
    /// Item is currently leased by a worker for dispatch.
    Leased,
    /// Item has been completed and acknowledged.
    Done,
}

impl QueuedItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Blocked => "blocked",
            Self::Leased => "leased",
            Self::Done => "done",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "blocked" => Self::Blocked,
            "leased" => Self::Leased,
            "done" => Self::Done,
            _ => Self::Pending,
        }
    }
}

/// One durable, ordered user submission for a thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedUserSubmissionRecord {
    pub id: String,
    pub thread_id: ThreadId,
    pub payload: String,
    /// Earliest wall-clock time (epoch ms) the item may be dispatched.
    pub not_before_ms: i64,
    /// Current scheduling state.
    pub state: QueuedItemState,
    /// Short string describing why the item is blocked, if any.
    pub block_reason: Option<String>,
    /// Number of failed dispatch attempts.
    pub attempt_count: i64,
    /// Worker ID that currently owns the item, if leased.
    pub lease_owner: Option<String>,
    /// When the current lease expires (epoch ms), or 0 if no lease.
    pub lease_expiry_ms: i64,
    /// Bounded error message from the most recent failure.
    pub last_error: Option<String>,
}

impl QueuedUserSubmissionRecord {
    pub(crate) fn try_from_row(row: &SqliteRow) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            thread_id: ThreadId::try_from(row.try_get::<String, _>("thread_id")?)?,
            payload: row.try_get("payload_json")?,
            not_before_ms: row.try_get("not_before_ms")?,
            state: QueuedItemState::from_str(row.try_get("state")?),
            block_reason: row.try_get("block_reason")?,
            attempt_count: row.try_get("attempt_count")?,
            lease_owner: row.try_get("lease_owner")?,
            lease_expiry_ms: row.try_get("lease_expiry_ms")?,
            last_error: row.try_get("last_error")?,
        })
    }
}
