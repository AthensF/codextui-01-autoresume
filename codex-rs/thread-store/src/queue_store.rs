use std::fmt::Display;
use std::future::Future;

use codex_protocol::ThreadId;
use codex_rollout::StateDbHandle;
use codex_state::DueHead;
use codex_state::QueuedUserSubmissionRecord;
use codex_state::RescheduleParams;
use codex_state::SqliteQueueStore;

use crate::MAX_QUEUE_ITEMS;
use crate::ThreadStoreError;
use crate::ThreadStoreFuture;

/// Storage-neutral persistence for ordered, thread-scoped user messages.
pub trait QueueStore: Send + Sync {
    /// Return a stable revision that changes when another connection updates the queue.
    fn change_version(&self) -> ThreadStoreFuture<'_, i64>;

    /// Return changed, loaded thread IDs and their durable revisions after `revision`.
    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>>;

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord>;

    /// Enqueue a payload with an explicit `not_before_ms` scheduling deadline.
    fn enqueue_with_not_before(
        &self,
        thread_id: ThreadId,
        payload: String,
        not_before_ms: i64,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord>;

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>>;

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>>;

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool>;

    /// Atomically replace queue order with every current item ID exactly once.
    ///
    /// Returns [`ThreadStoreError::InvalidRequest`] when `item_ids` is not a
    /// permutation of the complete queue.
    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()>;

    // --- Scheduling operations ---

    /// List head items across all threads that are due for dispatch.
    fn list_due_heads(&self, now_ms: i64, limit: usize) -> ThreadStoreFuture<'_, Vec<DueHead>>;

    /// Atomically claim a pending/blocked item for dispatch.
    fn claim_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>>;

    /// Renew an existing lease.
    fn renew_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> ThreadStoreFuture<'_, bool>;

    /// Acknowledge a leased item (delete after successful dispatch).
    fn acknowledge(&self, item_id: String, worker_id: String) -> ThreadStoreFuture<'_, bool>;

    /// Reschedule a leased item with new scheduling metadata.
    fn reschedule(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        params: RescheduleParams,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>>;

    /// Release a lease, returning the item to pending state.
    fn release_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
    ) -> ThreadStoreFuture<'_, bool>;

    /// Reset all expired leases back to pending.
    fn recover_expired_leases(&self, now_ms: i64) -> ThreadStoreFuture<'_, u64>;
}

/// Adapts the local state runtime to the shared queue-storage interface.
#[derive(Clone)]
pub struct LocalQueueStore {
    state_db: StateDbHandle,
}

impl LocalQueueStore {
    pub fn new(state_db: StateDbHandle) -> Self {
        Self { state_db }
    }

    fn queue(&self) -> &SqliteQueueStore {
        self.state_db.thread_queue()
    }
}

fn queue_future<'a, T, E>(
    future: impl Future<Output = Result<T, E>> + Send + 'a,
) -> ThreadStoreFuture<'a, T>
where
    T: Send + 'a,
    E: Display + Send + 'a,
{
    Box::pin(async move {
        future.await.map_err(|error| ThreadStoreError::Internal {
            message: format!("queue storage failed: {error}"),
        })
    })
}

impl QueueStore for LocalQueueStore {
    fn change_version(&self) -> ThreadStoreFuture<'_, i64> {
        queue_future(self.queue().change_version())
    }

    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>> {
        queue_future(self.queue().changes_since(revision, thread_ids))
    }

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(async move {
            self.queue()
                .enqueue(thread_id, &payload)
                .await
                .map_err(|error| match error.downcast_ref::<sqlx::Error>() {
                    Some(sqlx::Error::RowNotFound) => ThreadStoreError::InvalidRequest {
                        message: format!(
                            "queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"
                        ),
                    },
                    _ => ThreadStoreError::Internal {
                        message: format!("queue storage failed: {error}"),
                    },
                })
        })
    }

    fn enqueue_with_not_before(
        &self,
        thread_id: ThreadId,
        payload: String,
        not_before_ms: i64,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(async move {
            self.queue()
                .enqueue_with_not_before(thread_id, &payload, not_before_ms)
                .await
                .map_err(|error| match error.downcast_ref::<sqlx::Error>() {
                    Some(sqlx::Error::RowNotFound) => ThreadStoreError::InvalidRequest {
                        message: format!(
                            "queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"
                        ),
                    },
                    _ => ThreadStoreError::Internal {
                        message: format!("queue storage failed: {error}"),
                    },
                })
        })
    }

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>> {
        queue_future(self.queue().list_page(thread_id, offset, limit))
    }

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        queue_future(async move { self.queue().update(thread_id, &item_id, &payload).await })
    }

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool> {
        queue_future(async move { self.queue().delete(thread_id, &item_id).await })
    }

    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.queue()
                .reorder(thread_id, &item_ids)
                .await
                .map_err(|error| match error.downcast_ref::<std::io::Error>() {
                    Some(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
                        ThreadStoreError::InvalidRequest {
                            message: error.to_string(),
                        }
                    }
                    _ => ThreadStoreError::Internal {
                        message: format!("queue storage failed: {error}"),
                    },
                })
        })
    }

    fn list_due_heads(&self, now_ms: i64, limit: usize) -> ThreadStoreFuture<'_, Vec<DueHead>> {
        queue_future(self.queue().list_due_heads(now_ms, limit))
    }

    fn claim_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            self.queue()
                .claim_lease(&item_id, &worker_id, now_ms, lease_duration_ms)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("queue storage failed: {error}"),
                })
        })
    }

    fn renew_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            self.queue()
                .renew_lease(&item_id, &worker_id, now_ms, lease_duration_ms)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("queue storage failed: {error}"),
                })
        })
    }

    fn acknowledge(&self, item_id: String, worker_id: String) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            self.queue()
                .acknowledge(&item_id, &worker_id)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("queue storage failed: {error}"),
                })
        })
    }

    fn reschedule(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
        params: RescheduleParams,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            self.queue()
                .reschedule(&item_id, &worker_id, now_ms, params)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("queue storage failed: {error}"),
                })
        })
    }

    fn release_lease(
        &self,
        item_id: String,
        worker_id: String,
        now_ms: i64,
    ) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            self.queue()
                .release_lease(&item_id, &worker_id, now_ms)
                .await
                .map_err(|error| ThreadStoreError::Internal {
                    message: format!("queue storage failed: {error}"),
                })
        })
    }

    fn recover_expired_leases(&self, now_ms: i64) -> ThreadStoreFuture<'_, u64> {
        queue_future(self.queue().recover_expired_leases(now_ms))
    }
}
