//! Tree admission barrier for usage-limit coordination.
//!
//! When any agent in a root/child tree encounters a waitable usage limit,
//! the barrier is sealed. While sealed:
//!
//! - New child spawns block until the barrier reopens.
//! - New turn triggers (send_input, inter-agent communication) block.
//! - Each model-request boundary checks the barrier and parks if sealed.
//!
//! The barrier is unsealed when the coordinator gate opens (capacity admitted)
//! or the wait is cancelled. This prevents new work from slipping through
//! while the tree is suspending, and ensures all members coordinate on the
//! same generation before a probe is elected.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Tree-wide admission barrier shared by the root and all spawned children.
///
/// Owned by `AgentControl` as an `Arc` so every clone of the control handle
/// shares the same barrier.
pub(crate) struct TreeAdmissionBarrier {
    /// `true` while the tree is sealed for usage-limit recovery.
    sealed: AtomicBool,
    /// Notified when the barrier transitions from sealed → unsealed.
    gate_notify: Notify,
}

impl TreeAdmissionBarrier {
    /// Creates a new unsealed barrier.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            sealed: AtomicBool::new(false),
            gate_notify: Notify::new(),
        })
    }

    /// Atomically seals the barrier. Returns `true` if this call sealed it
    /// (i.e. it was previously unsealed).
    pub(crate) fn seal(&self) -> bool {
        self.sealed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Unseals the barrier and wakes all waiters. Called when the
    /// coordinator gate opens or the wait is cancelled.
    pub(crate) fn unseal(&self) {
        self.sealed.store(false, Ordering::Release);
        self.gate_notify.notify_waiters();
    }

    /// Returns `true` if the barrier is currently sealed.
    pub(crate) fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }

    /// Waits until the barrier is unsealed, or the cancellation token fires.
    ///
    /// Spawn/turn-trigger boundaries call this before proceeding to ensure
    /// no new work slips through while the tree is suspending.
    pub(crate) async fn acquire(&self, cancellation: CancellationToken) {
        loop {
            if !self.is_sealed() {
                return;
            }
            tokio::select! {
                _ = self.gate_notify.notified() => {
                    // Barrier may have been unsealed; loop to re-check.
                    continue;
                }
                _ = cancellation.cancelled() => {
                    return;
                }
            }
        }
    }
}

impl Default for TreeAdmissionBarrier {
    fn default() -> Self {
        Self {
            sealed: AtomicBool::new(false),
            gate_notify: Notify::new(),
        }
    }
}

#[cfg(test)]
#[path = "tree_barrier_tests.rs"]
mod tests;
