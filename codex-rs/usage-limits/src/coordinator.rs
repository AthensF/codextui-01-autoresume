//! Tree-wide usage-limit wait coordinator (async wrapper).
//!
//! Owns the `Machine` (pure state), a `watch` channel for snapshot
//! publication, `Notify` for gate notifications, and deadline-based
//! timer waiting. All state transitions are delegated to `machine.rs`;
//! this module handles only locking, publication, cancellation, and
//! `tokio::time::sleep_until` integration.

use crate::machine::ActionResult;
use crate::machine::ElectionResult;
use crate::machine::Machine;
use crate::policy::UsageLimitPolicy;
use crate::state::LimitEncounter;
use crate::state::ProbeOrigin;
use crate::state::WaitAction;
use crate::state::WaitSnapshot;
use chrono::Utc;
use serde_json;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Permit issued to the elected revalidation leader for one probe,
/// or to a non-leader when the gate is already open.
#[derive(Debug)]
pub enum ResumeProbePermit {
    /// This agent was elected leader and should issue one real model request.
    Leader {
        generation: u64,
        origin: ProbeOrigin,
    },
    /// The gate was already opened by another leader; this agent may resume.
    GateOpen { generation: u64 },
}

impl ResumeProbePermit {
    /// Returns the generation this permit belongs to.
    pub fn generation(&self) -> u64 {
        match self {
            ResumeProbePermit::Leader { generation, .. }
            | ResumeProbePermit::GateOpen { generation } => *generation,
        }
    }

    /// Returns `true` if this permit is a leader permit.
    pub fn is_leader(&self) -> bool {
        matches!(self, ResumeProbePermit::Leader { .. })
    }

    /// Returns the probe origin, if this is a leader permit.
    pub fn origin(&self) -> Option<ProbeOrigin> {
        match self {
            ResumeProbePermit::Leader { origin, .. } => Some(*origin),
            ResumeProbePermit::GateOpen { .. } => None,
        }
    }
}

/// Error returned when a wait is cancelled or the generation is stale.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WaitCancelled;

/// Tree-wide usage-limit wait coordinator.
///
/// Owns at most one active wait generation at a time. All operations are
/// generation-guarded so stale callbacks become no-ops.
pub struct UsageLimitCoordinator {
    machine: Mutex<Machine>,
    snapshot_tx: watch::Sender<Option<WaitSnapshot>>,
    /// Notify parked agents when the gate opens, a probe is scheduled,
    /// or a manual retry is requested.
    gate_notify: Notify,
    policy: UsageLimitPolicy,
}

impl UsageLimitCoordinator {
    /// Creates a new coordinator with the given policy.
    pub fn new(policy: UsageLimitPolicy, _jitter_seed: u64) -> Arc<Self> {
        let (snapshot_tx, _) = watch::channel(None);
        Arc::new(Self {
            machine: Mutex::new(Machine::new(policy.clone())),
            snapshot_tx,
            gate_notify: Notify::new(),
            policy,
        })
    }

    /// Subscribes to coordinator state changes.
    pub fn subscribe(&self) -> watch::Receiver<Option<WaitSnapshot>> {
        self.snapshot_tx.subscribe()
    }

    /// Returns the current snapshot, if any.
    pub async fn snapshot(&self) -> Option<WaitSnapshot> {
        let machine = self.machine.lock().await;
        machine.snapshot()
    }

    /// Restores the coordinator state from a serialized JSON snapshot.
    ///
    /// This is used by a replacement process to rehydrate the coordinator
    /// after a restart so unloaded threads can report their usage-limit
    /// status without a live encounter.
    pub async fn restore_snapshot(
        &self,
        snapshot: &serde_json::Value,
    ) -> Result<(), serde_json::Error> {
        let wait_snapshot: WaitSnapshot = serde_json::from_value(snapshot.clone())?;
        let mut machine = self.machine.lock().await;
        machine.restore_snapshot(&wait_snapshot);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
        Ok(())
    }

    /// Returns the resolved policy.
    pub fn policy(&self) -> &UsageLimitPolicy {
        &self.policy
    }

    /// Reports a waitable usage-limit encounter.
    ///
    /// If a wait is already active for the same work ID, the encounter
    /// updates the existing generation rather than opening a duplicate.
    /// If no wait is active, a new generation is started.
    ///
    /// Returns the generation of the active wait.
    pub async fn encounter_limit(
        &self,
        work_id: String,
        trigger_thread_id: String,
        trigger_turn_id: String,
        encounter: LimitEncounter,
    ) -> u64 {
        let now = Instant::now();
        let now_wall = Utc::now();
        let mut machine = self.machine.lock().await;
        let generation = machine.encounter_limit(
            work_id,
            trigger_thread_id,
            trigger_turn_id,
            encounter,
            now,
            now_wall,
        );
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
        generation
    }

    /// Registers an agent as parked for the current generation.
    ///
    /// Returns `None` if no wait is active. The agent should await
    /// `wait_for_open_or_probe` afterwards.
    pub async fn register_parked_agent(&self, generation: u64, thread_id: String) -> Option<()> {
        let mut machine = self.machine.lock().await;
        let result = machine.register_parked(generation, thread_id);
        let snapshot = machine.snapshot();
        drop(machine);
        if result.is_some() {
            let _ = self.snapshot_tx.send(snapshot);
        }
        result
    }

    /// Registers a set of expected participant thread IDs for the current
    /// generation. Called by the tree admission barrier when sealing the
    /// tree so the coordinator knows which agents must park before a probe
    /// is elected.
    pub async fn register_expected_participants(
        &self,
        generation: u64,
        thread_ids: Vec<String>,
    ) -> Option<()> {
        let mut machine = self.machine.lock().await;
        let result = machine.register_expected_participants(generation, thread_ids);
        let snapshot = machine.snapshot();
        drop(machine);
        if result.is_some() {
            let _ = self.snapshot_tx.send(snapshot);
            self.gate_notify.notify_waiters();
        }
        result
    }

    /// Removes a participant from both the expected and parked sets for the
    /// current generation. Called when a thread genuinely completes while
    /// the barrier is closing.
    pub async fn remove_participant(&self, generation: u64, thread_id: &str) -> Option<()> {
        let mut machine = self.machine.lock().await;
        let result = machine.remove_participant(generation, thread_id);
        let snapshot = machine.snapshot();
        drop(machine);
        if result.is_some() {
            let _ = self.snapshot_tx.send(snapshot);
            self.gate_notify.notify_waiters();
        }
        result
    }

    /// Waits for the gate to open (capacity admitted) or for this agent to be
    /// elected as the revalidation leader.
    ///
    /// Returns:
    /// - `Ok(ResumeProbePermit::Leader { .. })` if this agent was elected leader.
    /// - `Ok(ResumeProbePermit::GateOpen { .. })` if capacity was admitted by another leader.
    /// - `Err(WaitCancelled)` if the wait was cancelled or the generation is stale.
    pub async fn wait_for_open_or_probe(
        self: &Arc<Self>,
        generation: u64,
        thread_id: String,
        cancellation: CancellationToken,
    ) -> Result<ResumeProbePermit, WaitCancelled> {
        loop {
            // Compute the deadline and attempt election under the lock.
            let deadline = {
                let (result, snapshot) = {
                    let mut machine = self.machine.lock().await;
                    let now = Instant::now();
                    let result = machine.try_elect_leader(generation, thread_id.clone(), now);
                    let snapshot = machine.snapshot();
                    (result, snapshot)
                };
                let _ = self.snapshot_tx.send(snapshot);

                match &result {
                    ElectionResult::Elected { .. } | ElectionResult::GateOpen { .. } => {
                        return Ok(election_result_to_permit(result));
                    }
                    ElectionResult::Cancelled => return Err(WaitCancelled),
                    ElectionResult::Terminal | ElectionResult::NotDue => {
                        // Get the deadline for the timer wait.
                        {
                            let machine = self.machine.lock().await;
                            machine.active.as_ref().and_then(|g| g.next_check_deadline)
                        }
                    }
                }
            };

            // Wait for either:
            // 1. The deadline timer to fire (automatic probe becomes due)
            // 2. A gate notification (manual retry, cancellation, or gate open)
            // 3. Cancellation
            tokio::select! {
                _ = self.gate_notify.notified() => {
                    // Loop back to re-check state and attempt election.
                    continue;
                }
                _ = async {
                    if let Some(deadline) = deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        // No deadline (terminal state); wait indefinitely for
                        // a notification or cancellation.
                        std::future::pending::<()>().await;
                    }
                } => {
                    // Deadline fired; loop back to attempt election.
                    continue;
                }
                _ = cancellation.cancelled() => {
                    return Err(WaitCancelled);
                }
            }
        }
    }

    /// Marks the leader's probe as admitted — capacity is available.
    ///
    /// Opens the gate for all parked agents and transitions to `Resuming`.
    /// For automatic probes, consumes one automatic resume allowance.
    pub async fn mark_probe_admitted(&self, generation: u64, origin: ProbeOrigin) {
        let mut machine = self.machine.lock().await;
        machine.mark_probe_admitted(generation, origin);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
    }

    /// Marks the leader's probe as still limited — capacity is not available.
    ///
    /// Updates the reset/backoff metadata and transitions back to
    /// `Queued`/`Suspended`.
    pub async fn mark_probe_limited(&self, generation: u64, encounter: LimitEncounter) {
        let now = Instant::now();
        let now_wall = Utc::now();
        let mut machine = self.machine.lock().await;
        machine.mark_probe_limited(generation, encounter, now, now_wall);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
    }

    /// Marks the leader's probe as failed with a non-recoverable reason.
    ///
    /// Transitions to `AttentionRequired`.
    pub async fn mark_probe_failed(&self, generation: u64, reason: String) {
        let mut machine = self.machine.lock().await;
        machine.mark_probe_failed(generation, reason);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
    }

    /// Applies a user action to the current wait generation.
    pub async fn apply_action(&self, generation: u64, action: WaitAction) -> ActionResult {
        let now = Instant::now();
        let now_wall = Utc::now();
        let mut machine = self.machine.lock().await;
        let result = machine.apply_action(generation, action, now, now_wall);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
        result
    }

    /// Finishes work for the given work ID, clearing the wait state and
    /// per-work accounting.
    pub async fn finish_work(&self, work_id: &str) {
        let mut machine = self.machine.lock().await;
        machine.finish_work(work_id);
        let snapshot = machine.snapshot();
        drop(machine);
        let _ = self.snapshot_tx.send(snapshot);
        self.gate_notify.notify_waiters();
    }
}

fn election_result_to_permit(result: ElectionResult) -> ResumeProbePermit {
    match result {
        ElectionResult::Elected { generation, origin } => {
            ResumeProbePermit::Leader { generation, origin }
        }
        ElectionResult::GateOpen { generation } => ResumeProbePermit::GateOpen { generation },
        _ => unreachable!("election_result_to_permit called with non-permit result"),
    }
}
