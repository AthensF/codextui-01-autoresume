use super::*;
use crate::QueuedItemState;
use crate::migrations::QUEUE_MIGRATOR;
use crate::runtime::test_support::test_thread_metadata;
use crate::runtime::test_support::unique_temp_dir;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::migrate::Migrator;
use std::borrow::Cow;

async fn runtime_with_thread() -> (Arc<StateRuntime>, ThreadId) {
    let home = unique_temp_dir();
    let runtime = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("state runtime");
    let thread_id = ThreadId::new();
    let metadata = test_thread_metadata(home.as_path(), thread_id, home.clone());
    runtime.upsert_thread(&metadata).await.unwrap();
    (runtime, thread_id)
}

#[tokio::test]
async fn competing_runtimes_preserve_fifo_queue_order() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();
    let queue = runtime.thread_queue();
    let other_queue = other.thread_queue();
    let (first, second) = tokio::join!(
        queue.enqueue(thread_id, r#"{"first":true}"#),
        other_queue.enqueue(thread_id, r#"{"second":true}"#),
    );
    let mut expected = vec![first.unwrap(), second.unwrap()];
    expected.sort_by(|first, second| first.id.cmp(&second.id));
    let mut actual = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 2)
        .await
        .unwrap();
    actual.sort_by(|first, second| first.id.cmp(&second.id));
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn migrating_existing_queue_backfills_thread_revisions() {
    let home = unique_temp_dir();
    tokio::fs::create_dir_all(&home).await.unwrap();
    let sqlite = crate::SqliteConfig::new_for_testing(home.as_path().abs());
    let queue_path = sqlite.queue_db_path();
    let old_queue_migrator = Migrator {
        migrations: Cow::Owned(vec![QUEUE_MIGRATOR.migrations[0].clone()]),
        ignore_missing: false,
        locking: true,
        no_tx: false,
        table_name: QUEUE_MIGRATOR.table_name.clone(),
        create_schemas: QUEUE_MIGRATOR.create_schemas.clone(),
    };
    let pool = sqlite.open_read_write_pool(&queue_path).await.unwrap();
    old_queue_migrator.run(&pool).await.unwrap();

    let thread_id = ThreadId::new();
    let queued = QueuedUserSubmissionRecord {
        id: Uuid::now_v7().to_string(),
        thread_id,
        payload: r#"{"existing":true}"#.to_string(),
        not_before_ms: 0,
        state: QueuedItemState::Pending,
        block_reason: None,
        attempt_count: 0,
        lease_owner: None,
        lease_expiry_ms: 0,
        last_error: None,
    };
    sqlx::query(
        "INSERT INTO queued_items
         (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms)
         VALUES (?, ?, ?, 0, 0, 0)",
    )
    .bind(&queued.id)
    .bind(thread_id.to_string())
    .bind(&queued.payload)
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let runtime = StateRuntime::init(sqlite, "test-provider".to_string())
        .await
        .unwrap();
    let queue = runtime.thread_queue();
    assert_eq!(
        vec![(thread_id, 1)],
        queue
            .changes_since(/*revision*/ 0, &[thread_id])
            .await
            .unwrap()
    );
    assert_eq!(
        vec![queued],
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn queue_revisions_identify_changed_threads_after_updates_and_deletions() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"first":true}"#).await.unwrap();
    let first_revision = queue
        .changes_since(/*revision*/ 0, &[thread_id])
        .await
        .unwrap()[0]
        .1;
    queue
        .update(thread_id, &first.id, r#"{"updated":true}"#)
        .await
        .unwrap();
    let updated_revision = queue
        .changes_since(first_revision, &[thread_id])
        .await
        .unwrap()[0]
        .1;
    let other_thread_id = ThreadId::new();
    queue
        .enqueue(other_thread_id, r#"{"other":true}"#)
        .await
        .unwrap();
    let newly_loaded_changes = queue
        .changes_since(/*revision*/ 0, &[other_thread_id])
        .await
        .unwrap();
    assert_eq!(
        vec![(thread_id, updated_revision), newly_loaded_changes[0]],
        queue
            .changes_since(first_revision, &[thread_id, other_thread_id])
            .await
            .unwrap()
    );
    assert!(queue.delete(thread_id, &first.id).await.unwrap());
    assert!(
        queue
            .changes_since(updated_revision, &[thread_id])
            .await
            .unwrap()
            .iter()
            .any(|(changed_thread, _)| *changed_thread == thread_id)
    );
}

#[tokio::test]
async fn fifo_dispatch_preserves_edits_reordering_and_pagination() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let second = queue.enqueue(thread_id, r#"{"n":2}"#).await.unwrap();
    let third = queue.enqueue(thread_id, r#"{"n":3}"#).await.unwrap();

    let updated = queue
        .update(thread_id, &first.id, r#"{"n":"edited"}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, updated.id);
    let error = queue
        .reorder(thread_id, std::slice::from_ref(&first.id))
        .await
        .unwrap_err();
    assert_eq!(
        std::io::ErrorKind::InvalidInput,
        error.downcast_ref::<std::io::Error>().unwrap().kind()
    );

    let ordered_ids = vec![third.id, first.id, second.id];
    queue.reorder(thread_id, &ordered_ids).await.unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 3)
        .await
        .unwrap();
    let page = queue
        .list_page(thread_id, /*offset*/ 1, /*limit*/ 1)
        .await
        .unwrap();
    assert_eq!(vec![items[1].clone()], page);
    assert_eq!(r#"{"n":"edited"}"#, items[1].payload);

    for item in items {
        assert!(queue.delete(thread_id, &item.id).await.unwrap());
    }
    assert!(
        queue
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn queue_operations_cannot_mutate_another_threads_messages() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let other_thread_id = ThreadId::new();
    let other = queue.enqueue(other_thread_id, r#"{"n":2}"#).await.unwrap();
    let other_id = &other.id;

    assert_eq!(
        None,
        queue
            .update(thread_id, other_id, r#"{"n":3}"#)
            .await
            .unwrap()
    );
    assert!(!queue.delete(thread_id, other_id).await.unwrap());
    assert!(
        queue
            .reorder(thread_id, std::slice::from_ref(other_id))
            .await
            .is_err()
    );
    let (items, other_items) = tokio::join!(
        queue.list_page(thread_id, /*offset*/ 0, /*limit*/ 1),
        queue.list_page(other_thread_id, /*offset*/ 0, /*limit*/ 1),
    );
    assert_eq!(
        (vec![first], vec![other]),
        (items.unwrap(), other_items.unwrap())
    );
}

#[tokio::test]
async fn deleting_a_thread_removes_its_queue() {
    let (runtime, thread_id) = runtime_with_thread().await;
    runtime
        .thread_queue()
        .enqueue(thread_id, r#"{"n":1}"#)
        .await
        .unwrap();

    assert_eq!(1, runtime.delete_thread(thread_id).await.unwrap());
    assert!(
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn concurrent_inserts_enforce_the_queue_limit() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let other = StateRuntime::init(runtime.sqlite().clone(), "test-provider".to_string())
        .await
        .unwrap();

    for _ in 0..MAX_QUEUE_ITEMS - 1 {
        runtime
            .thread_queue()
            .enqueue(thread_id, r#"{"n":1}"#)
            .await
            .unwrap();
    }
    let (first, second) = tokio::join!(
        runtime.thread_queue().enqueue(thread_id, r#"{"n":2}"#),
        other.thread_queue().enqueue(thread_id, r#"{"n":3}"#),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        MAX_QUEUE_ITEMS,
        runtime
            .thread_queue()
            .list_page(thread_id, /*offset*/ 0, /*limit*/ MAX_QUEUE_ITEMS)
            .await
            .unwrap()
            .len()
    );
}

#[tokio::test]
async fn list_due_heads_returns_pending_items_in_fifo_order() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let first = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    let second = queue.enqueue(thread_id, r#"{"n":2}"#).await.unwrap();

    let due = queue
        .list_due_heads(/*now_ms*/ 0, /*limit*/ 10)
        .await
        .unwrap();
    assert_eq!(1, due.len());
    assert_eq!(first.id, due[0].id);

    // Acknowledge the first item, then the second becomes due.
    queue
        .claim_lease(
            &first.id, /*worker_id*/ "w1", /*now_ms*/ 0,
            /*lease_duration_ms*/ 10_000,
        )
        .await
        .unwrap();
    queue
        .acknowledge(&first.id, /*worker_id*/ "w1")
        .await
        .unwrap();

    let due = queue
        .list_due_heads(/*now_ms*/ 0, /*limit*/ 10)
        .await
        .unwrap();
    assert_eq!(1, due.len());
    assert_eq!(second.id, due[0].id);
}

#[tokio::test]
async fn list_due_heads_respects_not_before_ms() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let future = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();
    queue
        .claim_lease(
            &future.id, /*worker_id*/ "w1", /*now_ms*/ 0,
            /*lease_duration_ms*/ 10_000,
        )
        .await
        .unwrap();
    queue
        .reschedule(
            &future.id,
            /*worker_id*/ "w1",
            /*now_ms*/ 0,
            RescheduleParams {
                not_before_ms: 5_000,
                state: QueuedItemState::Pending,
                block_reason: None,
                last_error: None,
            },
        )
        .await
        .unwrap();

    // Not due yet.
    let due = queue
        .list_due_heads(/*now_ms*/ 1_000, /*limit*/ 10)
        .await
        .unwrap();
    assert!(due.is_empty());

    // Becomes due after not_before_ms.
    let due = queue
        .list_due_heads(/*now_ms*/ 5_000, /*limit*/ 10)
        .await
        .unwrap();
    assert_eq!(1, due.len());
    assert_eq!(future.id, due[0].id);
}

#[tokio::test]
async fn claim_lease_is_atomic_and_prevents_double_claim() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    // Run claims sequentially to ensure deterministic ordering.
    let first = queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 10_000,
        )
        .await;
    let second = queue
        .claim_lease(
            &item.id, "worker-b", /*now_ms*/ 0, /*lease_duration_ms*/ 10_000,
        )
        .await;
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(first.unwrap().is_some());
    assert!(second.unwrap().is_none());
}

#[tokio::test]
async fn recover_expired_leases_resets_to_pending() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 1_000,
        )
        .await
        .unwrap();

    // Before expiry, the item is leased and not due.
    let due = queue
        .list_due_heads(/*now_ms*/ 500, /*limit*/ 10)
        .await
        .unwrap();
    assert!(due.is_empty());

    // After expiry, recovery resets it to pending.
    let recovered = queue
        .recover_expired_leases(/*now_ms*/ 2_000)
        .await
        .unwrap();
    assert_eq!(1, recovered);

    let due = queue
        .list_due_heads(/*now_ms*/ 2_000, /*limit*/ 10)
        .await
        .unwrap();
    assert_eq!(1, due.len());
    assert_eq!(item.id, due[0].id);
}

#[tokio::test]
async fn reschedule_preserves_attempt_count_and_block_reason() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 10_000,
        )
        .await
        .unwrap();

    queue
        .reschedule(
            &item.id,
            "worker-a",
            /*now_ms*/ 0,
            RescheduleParams {
                not_before_ms: 5_000,
                state: QueuedItemState::Blocked,
                block_reason: Some("usage-limit".to_string()),
                last_error: Some("429".to_string()),
            },
        )
        .await
        .unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    assert_eq!(QueuedItemState::Blocked, items[0].state);
    assert_eq!(Some("usage-limit".to_string()), items[0].block_reason);
    assert_eq!(Some("429".to_string()), items[0].last_error);
    assert_eq!(5_000, items[0].not_before_ms);
}

#[tokio::test]
async fn acknowledge_deletes_item_after_successful_dispatch() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 10_000,
        )
        .await
        .unwrap();
    queue.acknowledge(&item.id, "worker-a").await.unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    assert!(items.is_empty());
}

#[tokio::test]
async fn release_lease_returns_item_to_pending_without_rescheduling() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 10_000,
        )
        .await
        .unwrap();
    queue
        .release_lease(&item.id, "worker-a", /*now_ms*/ 0)
        .await
        .unwrap();

    let items = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await
        .unwrap();
    assert_eq!(1, items.len());
    assert_eq!(QueuedItemState::Pending, items[0].state);
}

#[tokio::test]
async fn renew_lease_extends_lease_expiry() {
    let (runtime, thread_id) = runtime_with_thread().await;
    let queue = runtime.thread_queue();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#).await.unwrap();

    queue
        .claim_lease(
            &item.id, "worker-a", /*now_ms*/ 0, /*lease_duration_ms*/ 1_000,
        )
        .await
        .unwrap();

    // Renew the lease before it expires.
    queue
        .renew_lease(
            &item.id, "worker-a", /*now_ms*/ 500, /*lease_duration_ms*/ 20_000,
        )
        .await
        .unwrap();

    // The item should not be recovered because the lease was renewed.
    let recovered = queue
        .recover_expired_leases(/*now_ms*/ 5_000)
        .await
        .unwrap();
    assert_eq!(0, recovered);
}
