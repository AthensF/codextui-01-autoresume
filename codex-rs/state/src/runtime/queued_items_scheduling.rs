//! Atomic scheduling operations for the durable queue.
//!
//! These methods extend [`SqliteQueueStore`] with lease-based dispatch:
//! - [`SqliteQueueStore::list_due_heads`] finds pending/blocked head items
//!   whose `not_before_ms` has passed and that are not currently leased.
//! - [`SqliteQueueStore::claim_lease`] atomically transitions an item to the
//!   leased state under a worker ID.
//! - [`SqliteQueueStore::renew_lease`] extends an existing lease.
//! - [`SqliteQueueStore::acknowledge`] deletes a leased item after successful
//!   dispatch.
//! - [`SqliteQueueStore::reschedule`] moves a leased item back to pending or
//!   blocked with a new `not_before_ms`.
//! - [`SqliteQueueStore::release_lease`] releases a lease without rescheduling.
//! - [`SqliteQueueStore::recover_expired_leases`] resets expired leases back
//!   to pending.

use super::*;
use crate::QueuedItemState;
use crate::QueuedUserSubmissionRecord;

/// A due head item discovered by [`SqliteQueueStore::list_due_heads`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueHead {
    pub id: String,
    pub thread_id: ThreadId,
    pub payload: String,
    pub state: QueuedItemState,
    pub attempt_count: i64,
}

/// Parameters for rescheduling a leased item.
#[derive(Debug, Clone)]
pub struct RescheduleParams {
    pub not_before_ms: i64,
    pub state: QueuedItemState,
    pub block_reason: Option<String>,
    pub last_error: Option<String>,
}

impl SqliteQueueStore {
    /// List head items (smallest `queue_order` per thread) that are due and
    /// not currently leased.
    ///
    /// Returns at most `limit` items across all threads, ordered by
    /// `not_before_ms` then `queue_order`.
    pub async fn list_due_heads(&self, now_ms: i64, limit: usize) -> anyhow::Result<Vec<DueHead>> {
        let rows = sqlx::query(
            "SELECT qi.id, qi.thread_id, qi.payload_json, qi.state, qi.attempt_count
             FROM queued_items qi
             WHERE qi.queue_order = (
                 SELECT MIN(qi2.queue_order)
                 FROM queued_items qi2
                 WHERE qi2.thread_id = qi.thread_id
                   AND qi2.state IN ('pending', 'blocked')
             )
             AND qi.state IN ('pending', 'blocked')
             AND qi.not_before_ms <= ?
             AND (qi.lease_expiry_ms = 0 OR qi.lease_expiry_ms < ?)
             ORDER BY qi.not_before_ms, qi.queue_order
             LIMIT ?",
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(i64::try_from(limit)?)
        .fetch_all(self.pool.as_ref())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(DueHead {
                    id: row.try_get("id")?,
                    thread_id: ThreadId::try_from(row.try_get::<String, _>("thread_id")?)?,
                    payload: row.try_get("payload_json")?,
                    state: QueuedItemState::from_str(row.try_get("state")?),
                    attempt_count: row.try_get("attempt_count")?,
                })
            })
            .collect()
    }

    /// Atomically claim a pending/blocked item for dispatch.
    ///
    /// Returns the full record if the claim succeeded, or `None` if another
    /// worker already claimed it or the item is no longer in a claimable
    /// state.
    pub async fn claim_lease(
        &self,
        item_id: &str,
        worker_id: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
        let expiry_ms = now_ms.saturating_add(lease_duration_ms);
        let row = sqlx::query(
            "UPDATE queued_items
             SET state = 'leased',
                 lease_owner = ?,
                 lease_expiry_ms = ?,
                 updated_at_ms = ?
             WHERE id = ?
               AND state IN ('pending', 'blocked')
               AND (lease_expiry_ms = 0 OR lease_expiry_ms < ?)
             RETURNING id, thread_id, payload_json, not_before_ms, state, block_reason, attempt_count, lease_owner, lease_expiry_ms, last_error",
        )
        .bind(worker_id)
        .bind(expiry_ms)
        .bind(now_ms)
        .bind(item_id)
        .bind(now_ms)
        .fetch_optional(self.pool.as_ref())
        .await?;
        row.as_ref()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .transpose()
    }

    /// Renew an existing lease, extending its expiry.
    ///
    /// Returns `true` if the renewal succeeded, `false` if the item is not
    /// currently leased by `worker_id`.
    pub async fn renew_lease(
        &self,
        item_id: &str,
        worker_id: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> anyhow::Result<bool> {
        let expiry_ms = now_ms.saturating_add(lease_duration_ms);
        Ok(sqlx::query(
            "UPDATE queued_items
             SET lease_expiry_ms = ?, updated_at_ms = ?
             WHERE id = ? AND lease_owner = ? AND state = 'leased'",
        )
        .bind(expiry_ms)
        .bind(now_ms)
        .bind(item_id)
        .bind(worker_id)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected()
            > 0)
    }

    /// Acknowledge a leased item: delete it after successful dispatch.
    ///
    /// Returns `true` if the item was deleted, `false` if it was not leased
    /// by `worker_id`.
    pub async fn acknowledge(&self, item_id: &str, worker_id: &str) -> anyhow::Result<bool> {
        Ok(sqlx::query(
            "DELETE FROM queued_items
             WHERE id = ? AND lease_owner = ? AND state = 'leased'",
        )
        .bind(item_id)
        .bind(worker_id)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected()
            > 0)
    }

    /// Reschedule a leased item back to pending or blocked with new
    /// scheduling metadata.
    ///
    /// Increments `attempt_count` if `params.state` is `Blocked`.
    /// Returns the updated record, or `None` if the item was not leased by
    /// `worker_id`.
    pub async fn reschedule(
        &self,
        item_id: &str,
        worker_id: &str,
        now_ms: i64,
        params: RescheduleParams,
    ) -> anyhow::Result<Option<QueuedUserSubmissionRecord>> {
        let row = if params.state == QueuedItemState::Blocked {
            sqlx::query(
                "UPDATE queued_items
                 SET state = ?,
                     not_before_ms = ?,
                     block_reason = ?,
                     last_error = ?,
                     lease_owner = NULL,
                     lease_expiry_ms = 0,
                     attempt_count = attempt_count + 1,
                     updated_at_ms = ?
                 WHERE id = ? AND lease_owner = ? AND state = 'leased'
                 RETURNING id, thread_id, payload_json, not_before_ms, state, block_reason, attempt_count, lease_owner, lease_expiry_ms, last_error",
            )
            .bind(params.state.as_str())
            .bind(params.not_before_ms)
            .bind(params.block_reason)
            .bind(params.last_error)
            .bind(now_ms)
            .bind(item_id)
            .bind(worker_id)
            .fetch_optional(self.pool.as_ref())
            .await?
        } else {
            sqlx::query(
                "UPDATE queued_items
                 SET state = ?,
                     not_before_ms = ?,
                     block_reason = ?,
                     last_error = ?,
                     lease_owner = NULL,
                     lease_expiry_ms = 0,
                     updated_at_ms = ?
                 WHERE id = ? AND lease_owner = ? AND state = 'leased'
                 RETURNING id, thread_id, payload_json, not_before_ms, state, block_reason, attempt_count, lease_owner, lease_expiry_ms, last_error",
            )
            .bind(params.state.as_str())
            .bind(params.not_before_ms)
            .bind(params.block_reason)
            .bind(params.last_error)
            .bind(now_ms)
            .bind(item_id)
            .bind(worker_id)
            .fetch_optional(self.pool.as_ref())
            .await?
        };
        row.as_ref()
            .map(QueuedUserSubmissionRecord::try_from_row)
            .transpose()
    }

    /// Release a lease without rescheduling, returning the item to its
    /// previous pending state.
    ///
    /// Returns `true` if the lease was released, `false` if the item was not
    /// leased by `worker_id`.
    pub async fn release_lease(
        &self,
        item_id: &str,
        worker_id: &str,
        now_ms: i64,
    ) -> anyhow::Result<bool> {
        Ok(sqlx::query(
            "UPDATE queued_items
             SET state = 'pending',
                 lease_owner = NULL,
                 lease_expiry_ms = 0,
                 updated_at_ms = ?
             WHERE id = ? AND lease_owner = ? AND state = 'leased'",
        )
        .bind(now_ms)
        .bind(item_id)
        .bind(worker_id)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected()
            > 0)
    }

    /// Reset all expired leases back to pending state.
    ///
    /// Returns the number of leases recovered.
    pub async fn recover_expired_leases(&self, now_ms: i64) -> anyhow::Result<u64> {
        Ok(sqlx::query(
            "UPDATE queued_items
             SET state = 'pending',
                 lease_owner = NULL,
                 lease_expiry_ms = 0,
                 updated_at_ms = ?
             WHERE state = 'leased' AND lease_expiry_ms > 0 AND lease_expiry_ms < ?",
        )
        .bind(now_ms)
        .bind(now_ms)
        .execute(self.pool.as_ref())
        .await?
        .rows_affected())
    }
}
