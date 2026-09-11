//! Pure state machine for usage-limit wait coordination.
//!
//! This module contains all synchronous state transitions with no async,
//! locking, or timer dependencies. Time-dependent decisions receive
//! `now: Instant` as a parameter so paused Tokio tests are authoritative.
//! The coordinator (`coordinator.rs`) wraps this machine with locking,
//! watch publication, cancellation, and deadline waiting.

use crate::policy::DeterministicJitter;
use crate::policy::UsageLimitPolicy;
use crate::state::LimitClassification;
use crate::state::LimitEncounter;
use crate::state::ManualWaitReason;
use crate::state::ProbeOrigin;
use crate::state::WaitAction;
use crate::state::WaitDisposition;
use crate::state::WaitPhase;
use crate::state::WaitSnapshot;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;

/// Result of applying a user action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionResult {
    /// The action was applied successfully.
    Applied,
    /// The generation is stale; the action was ignored.
    StaleGeneration,
}

/// Result of a leader election attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElectionResult {
    /// This agent was elected leader and should issue a probe.
    Elected {
        generation: u64,
        origin: ProbeOrigin,
    },
    /// The gate is open; capacity was already admitted.
    GateOpen { generation: u64 },
    /// The wait was cancelled or the generation is stale.
    Cancelled,
    /// No probe is due yet; continue waiting.
    NotDue,
    /// The phase is terminal (ManualWait/AttentionRequired); no automatic
    /// probe will be issued.
    Terminal,
}

/// Internal pure state for a single wait generation.
pub struct WaitGeneration {
    pub work_id: String,
    pub generation: u64,
    pub trigger_thread_id: String,
    pub trigger_turn_id: String,
    pub phase: WaitPhase,
    pub classification: LimitClassification,
    pub disposition: WaitDisposition,
    pub limit_id: Option<String>,
    pub reset_at: Option<DateTime<Utc>>,
    pub next_check_at: Option<DateTime<Utc>>,
    pub next_check_deadline: Option<Instant>,
    pub auto_resume_enabled: bool,
    pub availability_checks_used: u32,
    pub max_availability_checks: u32,
    /// Monotonic timestamp when the first unknown-reset probe was issued.
    pub unknown_reset_wait_started: Option<Instant>,
    /// Wall-clock timestamp when the wait started, for snapshots.
    pub unknown_reset_wait_started_wall: Option<DateTime<Utc>>,
    pub unknown_reset_max_wait: Duration,
    pub manual_wait_reason: Option<ManualWaitReason>,
    pub parked_thread_ids: HashSet<String>,
    pub leader_thread_id: Option<String>,
    /// Whether a manual retry has been requested since the last probe.
    pub manual_retry_requested: bool,
    /// Thread IDs expected to park before a probe is elected.
    /// Populated by the tree admission barrier when sealing the tree.
    /// A thread is removed from this set when it parks or genuinely
    /// completes. The leader is not elected until this set is empty
    /// (unless a manual retry is requested).
    pub expected_participants: HashSet<String>,
}

/// Per-work accounting that survives across wait generations until
/// `finish_work(work_id)` is called.
pub struct WorkRecord {
    pub automatic_resumes_used: u32,
    pub max_auto_resumes: u32,
}

/// Pure state machine — no async, no locking, no timers.
pub struct Machine {
    pub active: Option<WaitGeneration>,
    pub work_records: HashMap<String, WorkRecord>,
    pub next_generation: u64,
    pub policy: UsageLimitPolicy,
}

impl Machine {
    pub fn new(policy: UsageLimitPolicy) -> Self {
        Self {
            active: None,
            work_records: HashMap::new(),
            next_generation: 1,
            policy,
        }
    }

    /// Returns the current snapshot, if any.
    pub fn snapshot(&self) -> Option<WaitSnapshot> {
        self.active
            .as_ref()
            .map(|g| generation_to_snapshot(g, &self.work_records))
    }

    /// Restores the machine state from a serialized snapshot.
    ///
    /// This is used by a replacement process to rehydrate the coordinator
    /// after a restart.  Monotonic timestamps (`next_check_deadline`,
    /// `unknown_reset_wait_started`) cannot be restored across processes
    /// and are left as `None`; the wall-clock fields are restored so the
    /// snapshot reports the correct phase, reset time, and counters.
    pub fn restore_snapshot(&mut self, snapshot: &WaitSnapshot) {
        let generation = WaitGeneration {
            work_id: snapshot.work_id.clone(),
            generation: snapshot.generation,
            trigger_thread_id: snapshot.trigger_thread_id.clone(),
            trigger_turn_id: snapshot.trigger_turn_id.clone(),
            phase: snapshot.phase.clone(),
            classification: snapshot.classification.clone(),
            disposition: WaitDisposition::Suspended,
            limit_id: snapshot.limit_id.clone(),
            reset_at: snapshot
                .reset_at
                .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default()),
            next_check_at: snapshot
                .next_check_at
                .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default()),
            next_check_deadline: None,
            auto_resume_enabled: snapshot.auto_resume_enabled,
            availability_checks_used: snapshot.availability_checks_used,
            max_availability_checks: snapshot.max_availability_checks,
            unknown_reset_wait_started: None,
            unknown_reset_wait_started_wall: snapshot
                .unknown_reset_wait_started_at
                .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or_default()),
            unknown_reset_max_wait: Duration::from_secs(snapshot.unknown_reset_max_wait_seconds),
            manual_wait_reason: snapshot.manual_wait_reason.clone(),
            parked_thread_ids: snapshot.parked_thread_ids.iter().cloned().collect(),
            leader_thread_id: snapshot.leader_thread_id.clone(),
            manual_retry_requested: snapshot.manual_retry_requested,
            expected_participants: snapshot.expected_participants.iter().cloned().collect(),
        };
        self.next_generation = snapshot.generation.max(self.next_generation) + 1;
        // Ensure the work record has the correct counters.
        let record = self
            .work_records
            .entry(snapshot.work_id.clone())
            .or_insert_with(|| WorkRecord {
                automatic_resumes_used: 0,
                max_auto_resumes: snapshot.max_auto_resumes,
            });
        record.automatic_resumes_used = snapshot.automatic_resumes_used;
        record.max_auto_resumes = snapshot.max_auto_resumes;
        self.active = Some(generation);
    }

    /// Reports a waitable usage-limit encounter.
    ///
    /// If a wait is already active for the same work ID, the encounter
    /// updates the existing generation. Otherwise a new generation is started.
    ///
    /// Returns the generation of the active wait.
    pub fn encounter_limit(
        &mut self,
        work_id: String,
        trigger_thread_id: String,
        trigger_turn_id: String,
        encounter: LimitEncounter,
        now: Instant,
        now_wall: DateTime<Utc>,
    ) -> u64 {
        // If there is an active wait for the same work in a waiting phase,
        // update the existing generation (coalescing repeated 429s).
        // If the phase is Resuming (gate already opened) or Cancelled, this
        // is a new waiting boundary — clear the active wait and start a new
        // generation, preserving the per-work record.
        if self
            .active
            .as_ref()
            .is_some_and(|g| g.work_id == work_id && g.phase.is_waiting())
            && let Some(wait_gen) = self.active.as_mut()
        {
            update_encounter(wait_gen, &encounter, &self.policy, now, now_wall);
            return wait_gen.generation;
        }

        let generation = self.next_generation;
        self.next_generation += 1;

        let is_unknown_reset = encounter.reset_at.is_none();
        let (wait_started, wait_started_wall) = if is_unknown_reset {
            (Some(now), Some(now_wall))
        } else {
            (None, None)
        };

        // Ensure a work record exists for per-work resume accounting.
        let max_auto_resumes = self.policy.max_auto_resumes;
        self.work_records
            .entry(work_id.clone())
            .or_insert_with(|| WorkRecord {
                automatic_resumes_used: 0,
                max_auto_resumes,
            });

        let mut wait_gen = WaitGeneration {
            work_id,
            generation,
            trigger_thread_id,
            trigger_turn_id,
            phase: match encounter.disposition {
                WaitDisposition::Queued => WaitPhase::Queued,
                WaitDisposition::Suspended => WaitPhase::Suspended,
            },
            classification: encounter.classification.clone(),
            disposition: encounter.disposition.clone(),
            limit_id: encounter.limit_id.clone(),
            reset_at: encounter.reset_at,
            next_check_at: None,
            next_check_deadline: None,
            auto_resume_enabled: self.policy.auto_resume,
            availability_checks_used: 0,
            max_availability_checks: self.policy.max_availability_checks,
            unknown_reset_wait_started: wait_started,
            unknown_reset_wait_started_wall: wait_started_wall,
            unknown_reset_max_wait: self.policy.unknown_reset_max_wait,
            manual_wait_reason: None,
            parked_thread_ids: HashSet::new(),
            leader_thread_id: None,
            manual_retry_requested: false,
            expected_participants: HashSet::new(),
        };
        schedule_next_check(&mut wait_gen, &self.policy, now, now_wall);
        // If per-work auto-resume is already exhausted, immediately enter
        // ManualWait so no automatic probe is issued.
        if let Some(record) = self.work_records.get(&wait_gen.work_id)
            && record.automatic_resumes_used >= record.max_auto_resumes
            && wait_gen.auto_resume_enabled
        {
            wait_gen.phase = WaitPhase::ManualWait;
            wait_gen.manual_wait_reason = Some(ManualWaitReason::AutoResumeLimitReached);
            wait_gen.next_check_at = None;
            wait_gen.next_check_deadline = None;
        }
        self.active = Some(wait_gen);
        generation
    }

    /// Registers an agent as parked for the current generation.
    pub fn register_parked(&mut self, generation: u64, thread_id: String) -> Option<()> {
        let wait_gen = self.active.as_mut()?;
        if wait_gen.generation != generation {
            return None;
        }
        wait_gen.parked_thread_ids.insert(thread_id);
        Some(())
    }

    /// Registers a set of expected participant thread IDs for the current
    /// generation. These are live tree members that should park before a
    /// probe is elected. The tree admission barrier calls this when sealing.
    pub fn register_expected_participants(
        &mut self,
        generation: u64,
        thread_ids: Vec<String>,
    ) -> Option<()> {
        let wait_gen = self.active.as_mut()?;
        if wait_gen.generation != generation {
            return None;
        }
        for thread_id in thread_ids {
            wait_gen.expected_participants.insert(thread_id);
        }
        Some(())
    }

    /// Removes a participant from both the expected and parked sets for the
    /// current generation. Called when a thread genuinely completes while
    /// the barrier is closing, so it does not block probe election.
    pub fn remove_participant(&mut self, generation: u64, thread_id: &str) -> Option<()> {
        let wait_gen = self.active.as_mut()?;
        if wait_gen.generation != generation {
            return None;
        }
        wait_gen.expected_participants.remove(thread_id);
        wait_gen.parked_thread_ids.remove(thread_id);
        Some(())
    }

    /// Returns `true` if all expected participants have parked or been
    /// removed, meaning the tree is fully assembled and a probe can be
    /// safely elected.
    fn all_expected_parked(wait_gen: &WaitGeneration) -> bool {
        wait_gen
            .expected_participants
            .iter()
            .all(|id| wait_gen.parked_thread_ids.contains(id))
    }

    /// Attempts to elect a leader or detect gate-open/cancelled state.
    pub fn try_elect_leader(
        &mut self,
        generation: u64,
        thread_id: String,
        now: Instant,
    ) -> ElectionResult {
        let wait_gen = match self.active.as_mut() {
            Some(g) => g,
            None => return ElectionResult::Cancelled,
        };
        if wait_gen.generation != generation {
            return ElectionResult::Cancelled;
        }
        match wait_gen.phase {
            WaitPhase::Resuming => ElectionResult::GateOpen { generation },
            WaitPhase::Cancelled => ElectionResult::Cancelled,
            WaitPhase::ManualWait | WaitPhase::AttentionRequired => {
                // Manual retry can grant exactly one immediate probe.
                if wait_gen.manual_retry_requested && wait_gen.leader_thread_id.is_none() {
                    wait_gen.leader_thread_id = Some(thread_id);
                    wait_gen.phase = WaitPhase::Revalidating;
                    wait_gen.manual_retry_requested = false;
                    ElectionResult::Elected {
                        generation,
                        origin: ProbeOrigin::Manual,
                    }
                } else {
                    ElectionResult::Terminal
                }
            }
            _ => {
                // Do not elect a probe until all expected tree participants
                // have parked or been removed. This ensures the tree is
                // fully assembled before consuming a probe allowance.
                if !Self::all_expected_parked(wait_gen) {
                    return ElectionResult::NotDue;
                }
                // Try to elect as leader if no leader exists and a probe is due.
                let auto_resumes_used = self
                    .work_records
                    .get(&wait_gen.work_id)
                    .map(|r| r.automatic_resumes_used)
                    .unwrap_or(0);
                let max_auto_resumes = self
                    .work_records
                    .get(&wait_gen.work_id)
                    .map(|r| r.max_auto_resumes)
                    .unwrap_or(0);
                if wait_gen.leader_thread_id.is_none()
                    && should_probe(wait_gen, auto_resumes_used, max_auto_resumes, now)
                {
                    wait_gen.leader_thread_id = Some(thread_id);
                    wait_gen.phase = WaitPhase::Revalidating;
                    ElectionResult::Elected {
                        generation,
                        origin: ProbeOrigin::Automatic,
                    }
                } else {
                    ElectionResult::NotDue
                }
            }
        }
    }

    /// Marks the leader's probe as admitted — capacity is available.
    ///
    /// Increments `automatic_resumes_used` for the work (only for automatic
    /// probes), opens the gate for all parked agents, and transitions to
    /// `Resuming`.
    pub fn mark_probe_admitted(&mut self, generation: u64, origin: ProbeOrigin) {
        let wait_gen = match self.active.as_mut() {
            Some(g) if g.generation == generation => g,
            _ => return,
        };
        wait_gen.phase = WaitPhase::Resuming;
        wait_gen.leader_thread_id = None;
        wait_gen.next_check_at = None;
        wait_gen.next_check_deadline = None;
        wait_gen.manual_retry_requested = false;
        // Consume one automatic allowance only for automatic probes.
        if origin == ProbeOrigin::Automatic
            && let Some(record) = self.work_records.get_mut(&wait_gen.work_id)
        {
            record.automatic_resumes_used = record.automatic_resumes_used.saturating_add(1);
        }
    }

    /// Marks the leader's probe as still limited — capacity is not available.
    ///
    /// Increments the unknown-reset probe count, updates reset/backoff
    /// metadata, transitions back to Queued/Suspended, and checks ceilings.
    pub fn mark_probe_limited(
        &mut self,
        generation: u64,
        encounter: LimitEncounter,
        now: Instant,
        now_wall: DateTime<Utc>,
    ) {
        let wait_gen = match self.active.as_mut() {
            Some(g) if g.generation == generation => g,
            _ => return,
        };
        // Increment probe count for unknown-reset.
        if wait_gen.reset_at.is_none() {
            wait_gen.availability_checks_used = wait_gen.availability_checks_used.saturating_add(1);
        }
        wait_gen.leader_thread_id = None;
        wait_gen.manual_retry_requested = false;
        update_encounter(wait_gen, &encounter, &self.policy, now, now_wall);
        // Transition back to queued/suspended based on disposition.
        if wait_gen.phase == WaitPhase::Revalidating {
            wait_gen.phase = match wait_gen.disposition {
                WaitDisposition::Queued => WaitPhase::Queued,
                WaitDisposition::Suspended => WaitPhase::Suspended,
            };
        }
        check_ceilings(wait_gen, now);
    }

    /// Marks the leader's probe as failed with a non-recoverable reason.
    pub fn mark_probe_failed(&mut self, generation: u64, _reason: String) {
        let wait_gen = match self.active.as_mut() {
            Some(g) if g.generation == generation => g,
            _ => return,
        };
        wait_gen.phase = WaitPhase::AttentionRequired;
        wait_gen.leader_thread_id = None;
        wait_gen.next_check_at = None;
        wait_gen.next_check_deadline = None;
    }

    /// Applies a user action to the current wait generation.
    pub fn apply_action(
        &mut self,
        generation: u64,
        action: WaitAction,
        now: Instant,
        now_wall: DateTime<Utc>,
    ) -> ActionResult {
        let wait_gen = match self.active.as_mut() {
            Some(g) if g.generation == generation => g,
            _ => return ActionResult::StaleGeneration,
        };
        match action {
            WaitAction::RetryNow => {
                // Grant exactly one immediate manual probe.
                wait_gen.manual_retry_requested = true;
                wait_gen.manual_wait_reason = None;
                // Transition out of ManualWait back to waiting so the election
                // can proceed.
                if wait_gen.phase == WaitPhase::ManualWait {
                    wait_gen.phase = match wait_gen.disposition {
                        WaitDisposition::Queued => WaitPhase::Queued,
                        WaitDisposition::Suspended => WaitPhase::Suspended,
                    };
                }
                ActionResult::Applied
            }
            WaitAction::SetAutoResume { enabled } => {
                wait_gen.auto_resume_enabled = enabled;
                if !enabled {
                    if wait_gen.phase.is_waiting() && wait_gen.phase != WaitPhase::ManualWait {
                        wait_gen.phase = WaitPhase::ManualWait;
                        wait_gen.manual_wait_reason = Some(ManualWaitReason::AutoResumeDisabled);
                        wait_gen.next_check_at = None;
                        wait_gen.next_check_deadline = None;
                    }
                } else if wait_gen.phase == WaitPhase::ManualWait
                    && wait_gen.manual_wait_reason == Some(ManualWaitReason::AutoResumeDisabled)
                {
                    wait_gen.phase = match wait_gen.disposition {
                        WaitDisposition::Queued => WaitPhase::Queued,
                        WaitDisposition::Suspended => WaitPhase::Suspended,
                    };
                    wait_gen.manual_wait_reason = None;
                    schedule_next_check(wait_gen, &self.policy, now, now_wall);
                }
                ActionResult::Applied
            }
            WaitAction::Cancel => {
                wait_gen.phase = WaitPhase::Cancelled;
                wait_gen.next_check_at = None;
                wait_gen.next_check_deadline = None;
                ActionResult::Applied
            }
        }
    }

    /// Finishes work for the given work ID, clearing the wait state and
    /// per-work accounting.
    pub fn finish_work(&mut self, work_id: &str) {
        if let Some(wait_gen) = self.active.as_ref()
            && wait_gen.work_id == work_id
        {
            self.active = None;
        }
        self.work_records.remove(work_id);
    }
}

/// Updates an existing wait generation with new encounter metadata.
fn update_encounter(
    wait_gen: &mut WaitGeneration,
    encounter: &LimitEncounter,
    policy: &UsageLimitPolicy,
    now: Instant,
    now_wall: DateTime<Utc>,
) {
    // A later reliable reset can replace an unknown backoff with a known-reset schedule.
    if encounter.reset_at.is_some() && wait_gen.reset_at.is_none() {
        wait_gen.reset_at = encounter.reset_at;
        wait_gen.classification = encounter.classification.clone();
        wait_gen.unknown_reset_wait_started = None;
        wait_gen.unknown_reset_wait_started_wall = None;
    } else if let Some(new_reset) = encounter.reset_at {
        // A later blocking reset can move the deadline later.
        if let Some(existing) = wait_gen.reset_at {
            if new_reset > existing {
                wait_gen.reset_at = Some(new_reset);
            }
        } else {
            wait_gen.reset_at = Some(new_reset);
        }
    }
    if encounter.limit_id.is_some() {
        wait_gen.limit_id = encounter.limit_id.clone();
    }
    // Update disposition if the new encounter has more effects.
    if encounter.disposition == WaitDisposition::Suspended
        && wait_gen.disposition == WaitDisposition::Queued
    {
        wait_gen.disposition = WaitDisposition::Suspended;
        if wait_gen.phase == WaitPhase::Queued {
            wait_gen.phase = WaitPhase::Suspended;
        }
    }
    schedule_next_check(wait_gen, policy, now, now_wall);
}

/// Returns `true` if an automatic probe should be issued now.
fn should_probe(
    wait_gen: &WaitGeneration,
    auto_resumes_used: u32,
    max_auto_resumes: u32,
    now: Instant,
) -> bool {
    if !wait_gen.auto_resume_enabled {
        return false;
    }
    if auto_resumes_used >= max_auto_resumes {
        return false;
    }
    // Check unknown-reset ceilings.
    if wait_gen.reset_at.is_none() {
        if wait_gen.availability_checks_used >= wait_gen.max_availability_checks {
            return false;
        }
        if let Some(started) = wait_gen.unknown_reset_wait_started
            && now >= started + wait_gen.unknown_reset_max_wait
        {
            return false;
        }
    }
    // Check if next_check_deadline is due.
    if let Some(deadline) = wait_gen.next_check_deadline {
        return now >= deadline;
    }
    false
}

/// Schedules the next check time based on the current reset/backoff state.
fn schedule_next_check(
    wait_gen: &mut WaitGeneration,
    policy: &UsageLimitPolicy,
    now: Instant,
    now_wall: DateTime<Utc>,
) {
    // Derive jitter from work_id hash + generation + probe ordinal.
    let jitter_seed = jitter_seed(
        &wait_gen.work_id,
        wait_gen.generation,
        wait_gen.availability_checks_used,
    );
    let jitter = DeterministicJitter::new(jitter_seed);

    if let Some(reset_at) = wait_gen.reset_at {
        // Known reset: reset + grace + jitter.
        let next_wall = policy.known_reset_next_check(reset_at, jitter);
        wait_gen.next_check_at = Some(next_wall);
        // Convert wall-clock target to monotonic deadline.
        let delta = next_wall.signed_duration_since(now_wall);
        if delta.num_milliseconds() <= 0 {
            wait_gen.next_check_deadline = Some(now);
        } else {
            wait_gen.next_check_deadline =
                Some(now + Duration::from_millis(delta.num_milliseconds() as u64));
        }
    } else {
        // Unknown reset: exponential backoff.
        let probe_number = wait_gen.availability_checks_used.saturating_add(1);
        let delay = policy.unknown_reset_probe_delay(probe_number, jitter);
        wait_gen.next_check_at =
            Some(now_wall + chrono::Duration::from_std(delay).unwrap_or_default());
        wait_gen.next_check_deadline = Some(now + delay);
        if wait_gen.unknown_reset_wait_started.is_none() {
            wait_gen.unknown_reset_wait_started = Some(now);
            wait_gen.unknown_reset_wait_started_wall = Some(now_wall);
        }
    }
}

/// Checks unknown-reset ceilings and transitions to ManualWait if needed.
fn check_ceilings(wait_gen: &mut WaitGeneration, now: Instant) {
    if wait_gen.reset_at.is_some() {
        return;
    }
    // Probe count ceiling.
    if wait_gen.availability_checks_used >= wait_gen.max_availability_checks {
        wait_gen.phase = WaitPhase::ManualWait;
        wait_gen.manual_wait_reason = Some(ManualWaitReason::AvailabilityCheckLimitReached);
        wait_gen.next_check_at = None;
        wait_gen.next_check_deadline = None;
        return;
    }
    // Total-wait ceiling (monotonic).
    if let Some(started) = wait_gen.unknown_reset_wait_started
        && now >= started + wait_gen.unknown_reset_max_wait
    {
        wait_gen.phase = WaitPhase::ManualWait;
        wait_gen.manual_wait_reason = Some(ManualWaitReason::WaitCeilingReached);
        wait_gen.next_check_at = None;
        wait_gen.next_check_deadline = None;
    }
}

/// Converts a `WaitGeneration` to a `WaitSnapshot`, sorting parked IDs
/// for deterministic deep equality.
fn generation_to_snapshot(
    wait_gen: &WaitGeneration,
    work_records: &HashMap<String, WorkRecord>,
) -> WaitSnapshot {
    let (automatic_resumes_used, max_auto_resumes) = work_records
        .get(&wait_gen.work_id)
        .map(|r| (r.automatic_resumes_used, r.max_auto_resumes))
        .unwrap_or((0, wait_gen.max_availability_checks));

    let mut parked: Vec<String> = wait_gen.parked_thread_ids.iter().cloned().collect();
    parked.sort();

    let mut expected: Vec<String> = wait_gen.expected_participants.iter().cloned().collect();
    expected.sort();

    WaitSnapshot {
        work_id: wait_gen.work_id.clone(),
        generation: wait_gen.generation,
        trigger_thread_id: wait_gen.trigger_thread_id.clone(),
        trigger_turn_id: wait_gen.trigger_turn_id.clone(),
        phase: wait_gen.phase.clone(),
        classification: wait_gen.classification.clone(),
        limit_id: wait_gen.limit_id.clone(),
        reset_at: wait_gen.reset_at.map(|dt| dt.timestamp()),
        next_check_at: wait_gen.next_check_at.map(|dt| dt.timestamp()),
        auto_resume_enabled: wait_gen.auto_resume_enabled,
        automatic_resumes_used,
        max_auto_resumes,
        availability_checks_used: wait_gen.availability_checks_used,
        max_availability_checks: wait_gen.max_availability_checks,
        unknown_reset_wait_started_at: wait_gen
            .unknown_reset_wait_started_wall
            .map(|dt| dt.timestamp()),
        unknown_reset_max_wait_seconds: wait_gen.unknown_reset_max_wait.as_secs(),
        manual_wait_reason: wait_gen.manual_wait_reason.clone(),
        parked_thread_ids: parked,
        expected_participants: expected,
        leader_thread_id: wait_gen.leader_thread_id.clone(),
        manual_retry_requested: wait_gen.manual_retry_requested,
    }
}

/// Derives a stable jitter seed from the work ID, generation, and probe ordinal.
fn jitter_seed(work_id: &str, generation: u64, probe_ordinal: u32) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for byte in work_id.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= generation;
    hash = hash.wrapping_mul(0x100000001b3);
    hash ^= probe_ordinal as u64;
    hash = hash.wrapping_mul(0x100000001b3);
    hash
}
