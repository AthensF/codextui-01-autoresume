use super::*;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn seal_and_unseal() {
    let barrier = TreeAdmissionBarrier::new();
    assert!(!barrier.is_sealed());
    assert!(barrier.seal(), "first seal should return true");
    assert!(barrier.is_sealed());
    assert!(
        !barrier.seal(),
        "second seal should return false (already sealed)"
    );
    barrier.unseal();
    assert!(!barrier.is_sealed());
}

#[tokio::test(start_paused = true)]
async fn acquire_returns_immediately_when_unsealed() {
    let barrier = TreeAdmissionBarrier::new();
    let cancel = CancellationToken::new();
    // Should return immediately since the barrier is not sealed.
    tokio::time::timeout(Duration::from_millis(10), barrier.acquire(cancel))
        .await
        .expect("should not block when unsealed");
}

#[tokio::test(start_paused = true)]
async fn acquire_blocks_until_unsealed() {
    let barrier = TreeAdmissionBarrier::new();
    let cancel = CancellationToken::new();
    barrier.seal();

    // Should block.
    let result =
        tokio::time::timeout(Duration::from_millis(10), barrier.acquire(cancel.clone())).await;
    assert!(result.is_err(), "should block while sealed");

    // Unseal should release the waiter.
    let barrier_clone = barrier.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        barrier_clone.unseal();
    });

    tokio::time::timeout(Duration::from_secs(1), barrier.acquire(cancel))
        .await
        .expect("should return after unseal");

    handle.await.expect("spawned task should complete");
}

#[tokio::test(start_paused = true)]
async fn acquire_returns_on_cancellation() {
    let barrier = TreeAdmissionBarrier::new();
    let cancel = CancellationToken::new();
    barrier.seal();

    let cancel_clone = cancel.clone();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_clone.cancel();
    });

    tokio::time::timeout(Duration::from_secs(1), barrier.acquire(cancel))
        .await
        .expect("should return on cancellation");

    handle.await.expect("spawned task should complete");
    // Barrier is still sealed — cancellation just unblocks the waiter.
    assert!(barrier.is_sealed());
}
