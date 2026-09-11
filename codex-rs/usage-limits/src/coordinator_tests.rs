use super::*;
use crate::state::LimitClassification;
use crate::state::ManualWaitReason;
use crate::state::ProbeOrigin;
use crate::state::WaitDisposition;
use crate::state::WaitPhase;
use chrono::DateTime;
use chrono::Duration as ChronoDuration;
use chrono::Utc;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn policy() -> UsageLimitPolicy {
    UsageLimitPolicy::default()
}

fn known_encounter(reset_at: Option<DateTime<Utc>>) -> LimitEncounter {
    LimitEncounter {
        classification: LimitClassification::UsageWindow,
        disposition: WaitDisposition::Queued,
        reset_at,
        retry_after: None,
        limit_id: Some("codex".to_string()),
    }
}

fn unknown_encounter() -> LimitEncounter {
    LimitEncounter {
        classification: LimitClassification::GenericTooManyRequests,
        disposition: WaitDisposition::Queued,
        reset_at: None,
        retry_after: None,
        limit_id: None,
    }
}

fn suspended_encounter(reset_at: Option<DateTime<Utc>>) -> LimitEncounter {
    LimitEncounter {
        classification: LimitClassification::UsageWindow,
        disposition: WaitDisposition::Suspended,
        reset_at,
        retry_after: None,
        limit_id: Some("codex".to_string()),
    }
}

#[tokio::test(start_paused = true)]
async fn encounter_limit_starts_queued_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    assert_eq!(gen_val, 1);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Queued);
    assert_eq!(snap.work_id, "work-1");
    assert_eq!(snap.generation, 1);
    assert!(snap.reset_at.is_some());
    assert!(snap.next_check_at.is_some());
    assert_eq!(snap.automatic_resumes_used, 0);
    assert_eq!(snap.max_auto_resumes, 1);
}

#[tokio::test(start_paused = true)]
async fn suspended_encounter_starts_suspended_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            suspended_encounter(Some(reset_at)),
        )
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Suspended);
}

#[tokio::test(start_paused = true)]
async fn repeated_encounter_updates_existing_generation() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    let gen1 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    // A later reset pushes the deadline later.
    let later_reset = reset_at + ChronoDuration::minutes(30);
    let gen2 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-2".to_string(),
            "turn-2".to_string(),
            known_encounter(Some(later_reset)),
        )
        .await;
    assert_eq!(
        gen1, gen2,
        "repeated encounter should not open new generation"
    );
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.reset_at, Some(later_reset.timestamp()));
}

#[tokio::test(start_paused = true)]
async fn unknown_reset_replaces_with_known_reset() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert!(snap.reset_at.is_none());
    assert!(snap.unknown_reset_wait_started_at.is_some());
    // Now provide a known reset.
    let reset_at = Utc::now() + ChronoDuration::minutes(30);
    let gen2 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    assert_eq!(gen_val, gen2);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.reset_at, Some(reset_at.timestamp()));
    assert!(snap.unknown_reset_wait_started_at.is_none());
}

#[tokio::test(start_paused = true)]
async fn finish_work_clears_state_and_work_record() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    assert!(coord.snapshot().await.is_some());
    coord.finish_work("work-1").await;
    assert!(coord.snapshot().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn cancel_transitions_to_cancelled() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    let result = coord.apply_action(gen_val, WaitAction::Cancel).await;
    assert_eq!(result, ActionResult::Applied);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Cancelled);
}

#[tokio::test(start_paused = true)]
async fn stale_generation_action_rejected() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let result = coord.apply_action(999, WaitAction::Cancel).await;
    assert_eq!(result, ActionResult::StaleGeneration);
}

#[tokio::test(start_paused = true)]
async fn disable_auto_resume_transitions_to_manual_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    let result = coord
        .apply_action(gen_val, WaitAction::SetAutoResume { enabled: false })
        .await;
    assert_eq!(result, ActionResult::Applied);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AutoResumeDisabled)
    );
}

#[tokio::test(start_paused = true)]
async fn reenable_auto_resume_exits_manual_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .apply_action(gen_val, WaitAction::SetAutoResume { enabled: false })
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    coord
        .apply_action(gen_val, WaitAction::SetAutoResume { enabled: true })
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Queued);
    assert!(snap.manual_wait_reason.is_none());
}

#[tokio::test(start_paused = true)]
async fn mark_probe_admitted_increments_resume_count_for_automatic() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Automatic)
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Resuming);
    assert_eq!(snap.automatic_resumes_used, 1);
}

#[tokio::test(start_paused = true)]
async fn mark_probe_admitted_does_not_increment_for_manual() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Manual)
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Resuming);
    assert_eq!(snap.automatic_resumes_used, 0);
}

#[tokio::test(start_paused = true)]
async fn mark_probe_limited_increments_unknown_reset_checks() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;
    coord.mark_probe_limited(gen_val, unknown_encounter()).await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.availability_checks_used, 1);
}

#[tokio::test(start_paused = true)]
async fn unknown_reset_six_probes_transitions_to_manual_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;
    for _ in 0..6 {
        coord.mark_probe_limited(gen_val, unknown_encounter()).await;
    }
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AvailabilityCheckLimitReached)
    );
}

#[tokio::test(start_paused = true)]
async fn mark_probe_failed_transitions_to_attention_required() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .mark_probe_failed(gen_val, "model unavailable".to_string())
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::AttentionRequired);
}

#[tokio::test(start_paused = true)]
async fn register_parked_agent_tracks_thread_ids_sorted() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-3".to_string())
        .await;
    coord
        .register_parked_agent(gen_val, "thread-2".to_string())
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    // Parked IDs should be sorted for deterministic deep equality.
    assert_eq!(
        snap.parked_thread_ids,
        vec!["thread-2".to_string(), "thread-3".to_string()]
    );
}

#[tokio::test(start_paused = true)]
async fn register_parked_agent_stale_generation_returns_none() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let result = coord
        .register_parked_agent(999, "thread-1".to_string())
        .await;
    assert!(result.is_none());
}

#[tokio::test(start_paused = true)]
async fn subscribe_receives_snapshots() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let rx = coord.subscribe();
    // Initial value is None.
    assert!(rx.borrow().is_none());
    coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    // The watch should have been updated.
    let snap = rx.borrow().clone().expect("snapshot");
    assert_eq!(snap.work_id, "work-1");
}

#[tokio::test(start_paused = true)]
async fn manual_retry_transitions_out_of_manual_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;
    // Exhaust probes to enter ManualWait.
    for _ in 0..6 {
        coord.mark_probe_limited(gen_val, unknown_encounter()).await;
    }
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    // Manual retry should transition back to waiting.
    let result = coord.apply_action(gen_val, WaitAction::RetryNow).await;
    assert_eq!(result, ActionResult::Applied);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_ne!(snap.phase, WaitPhase::ManualWait);
}

// ---------------------------------------------------------------------------
// Timer wake and leader election tests (paused Tokio time is authoritative).
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn timer_wakes_waiter_at_known_reset_deadline() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    // Set a reset 10 seconds in the future (wall-clock).
    let reset_at = Utc::now() + ChronoDuration::seconds(10);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;

    let coord_clone = coord.clone();
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    // Spawn a waiter that should be woken by the timer, not by a notification.
    let handle = tokio::spawn(async move {
        coord_clone
            .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel_clone)
            .await
    });

    // Advance time past the reset + grace + jitter (max ~15s).
    tokio::time::advance(Duration::from_secs(20)).await;
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("waiter should complete")
        .expect("waiter should not be cancelled");
    assert!(result.is_ok());
    let permit = result.unwrap();
    assert!(permit.is_leader());
    assert_eq!(permit.origin(), Some(ProbeOrigin::Automatic));
}

#[tokio::test(start_paused = true)]
async fn timer_wakes_waiter_for_unknown_reset_backoff() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;

    let coord_clone = coord.clone();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(async move {
        coord_clone
            .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
            .await
    });

    // Advance time past the first unknown-reset probe delay (base = 60s).
    tokio::time::advance(Duration::from_secs(120)).await;
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("waiter should complete")
        .expect("waiter should not be cancelled");
    assert!(result.is_ok());
    let permit = result.unwrap();
    assert!(permit.is_leader());
}

#[tokio::test(start_paused = true)]
async fn cancellation_cancels_wait() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;

    // Cancel before calling wait_for_open_or_probe.
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = coord
        .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
        .await;
    assert!(result.is_err());
}

#[tokio::test(start_paused = true)]
async fn gate_open_after_admission() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(10);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;

    // Register two parked agents.
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;
    coord
        .register_parked_agent(gen_val, "thread-2".to_string())
        .await;

    // Advance time so the probe is due.
    tokio::time::advance(Duration::from_secs(20)).await;

    // Thread-1 should be elected leader.
    let coord_clone = coord.clone();
    let cancel1 = CancellationToken::new();
    let handle1 = tokio::spawn(async move {
        coord_clone
            .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel1)
            .await
    });
    let permit1 = tokio::time::timeout(Duration::from_secs(5), handle1)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(permit1.is_leader());

    // Mark the probe as admitted.
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Automatic)
        .await;

    // Thread-2 should get GateOpen.
    let coord_clone2 = coord.clone();
    let cancel2 = CancellationToken::new();
    let handle2 = tokio::spawn(async move {
        coord_clone2
            .wait_for_open_or_probe(gen_val, "thread-2".to_string(), cancel2)
            .await
    });
    let permit2 = tokio::time::timeout(Duration::from_secs(5), handle2)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!permit2.is_leader());
    assert_eq!(permit2.generation(), gen_val);
}

#[tokio::test(start_paused = true)]
async fn manual_retry_grants_one_probe_even_when_auto_exhausted() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;

    // Consume the one allowed auto-resume.
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Automatic)
        .await;
    assert_eq!(coord.snapshot().await.unwrap().automatic_resumes_used, 1);

    // Encounter a new limit for the same work after admission.
    // This crosses a new waiting boundary, creating a new generation
    // while preserving the per-work resume count.
    let gen2 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;

    let snap = coord.snapshot().await.expect("snapshot");
    // Per-work resume count survived across generations.
    assert_eq!(snap.automatic_resumes_used, 1);
    assert_eq!(snap.max_auto_resumes, 1);
    // Auto-resume is exhausted, so the phase should be ManualWait.
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AutoResumeLimitReached)
    );

    // Manual retry should still grant a probe.
    let result = coord.apply_action(gen2, WaitAction::RetryNow).await;
    assert_eq!(result, ActionResult::Applied);
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::Queued);
    assert!(snap.manual_retry_requested);
}

#[tokio::test(start_paused = true)]
async fn manual_retry_does_not_reset_auto_resume_counter() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            unknown_encounter(),
        )
        .await;
    // Exhaust probes to enter ManualWait.
    for _ in 0..6 {
        coord.mark_probe_limited(gen_val, unknown_encounter()).await;
    }
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);

    // Manual retry.
    coord.apply_action(gen_val, WaitAction::RetryNow).await;
    // Mark the manual probe as admitted.
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Manual)
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    // Manual probe should not consume auto-resume allowance.
    assert_eq!(snap.automatic_resumes_used, 0);

    // If the probe fails and we re-encounter, we should be back in ManualWait
    // without the counter being reset.
    coord.mark_probe_limited(gen_val, unknown_encounter()).await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AvailabilityCheckLimitReached)
    );
}

#[tokio::test(start_paused = true)]
async fn auto_resume_disabled_preserves_visible_deadline() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;

    // Disable auto-resume.
    coord
        .apply_action(gen_val, WaitAction::SetAutoResume { enabled: false })
        .await;

    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AutoResumeDisabled)
    );
    // The reset deadline should still be visible in the snapshot.
    assert!(snap.reset_at.is_some());
}

#[tokio::test(start_paused = true)]
async fn second_wait_for_same_work_preserves_resume_count() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);

    // First wait: consume the auto-resume.
    let gen1 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    coord
        .mark_probe_admitted(gen1, ProbeOrigin::Automatic)
        .await;
    assert_eq!(coord.snapshot().await.unwrap().automatic_resumes_used, 1);

    // Encounter a new limit for the same work after admission.
    // This crosses a new waiting boundary, creating a new generation
    // while preserving the per-work resume count.
    let gen2 = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at + ChronoDuration::hours(1))),
        )
        .await;
    assert_ne!(gen1, gen2);
    let snap = coord.snapshot().await.expect("snapshot");
    // Per-work resume count survived.
    assert_eq!(snap.automatic_resumes_used, 1);
    assert_eq!(snap.max_auto_resumes, 1);
    assert_eq!(snap.phase, WaitPhase::ManualWait);
    assert_eq!(
        snap.manual_wait_reason,
        Some(ManualWaitReason::AutoResumeLimitReached)
    );
}

// ---------------------------------------------------------------------------
// Expected-participant tracking tests
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn register_expected_participants_tracks_thread_ids_sorted() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-3".to_string(), "thread-2".to_string()],
        )
        .await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(
        snap.expected_participants,
        vec!["thread-2".to_string(), "thread-3".to_string()]
    );
}

#[tokio::test(start_paused = true)]
async fn register_expected_participants_stale_generation_returns_none() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let result = coord
        .register_expected_participants(999, vec!["thread-1".to_string()])
        .await;
    assert!(result.is_none());
}

#[tokio::test(start_paused = true)]
async fn remove_participant_clears_from_expected_and_parked() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(Utc::now() + ChronoDuration::hours(1))),
        )
        .await;
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-1".to_string(), "thread-2".to_string()],
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;
    // thread-1 genuinely completes — remove it from both sets.
    coord.remove_participant(gen_val, "thread-1").await;
    let snap = coord.snapshot().await.expect("snapshot");
    assert_eq!(snap.expected_participants, vec!["thread-2".to_string()]);
    assert!(snap.parked_thread_ids.is_empty());
}

#[tokio::test(start_paused = true)]
async fn probe_not_elected_until_all_expected_parked() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    // Two threads expected; only one has parked.
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-1".to_string(), "thread-2".to_string()],
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;

    // Advance past the reset deadline.
    tokio::time::advance(Duration::from_secs(2)).await;

    // Election should return NotDue because thread-2 has not parked.
    let cancel = CancellationToken::new();
    let permit = tokio::time::timeout(
        Duration::from_millis(100),
        coord.wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel),
    )
    .await;
    assert!(
        permit.is_err(),
        "should not elect while expected participant is not parked"
    );
}

#[tokio::test(start_paused = true)]
async fn probe_elected_after_all_expected_parked() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    // Two threads expected; both park.
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-1".to_string(), "thread-2".to_string()],
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;
    coord
        .register_parked_agent(gen_val, "thread-2".to_string())
        .await;

    // Advance past the reset deadline.
    tokio::time::advance(Duration::from_secs(2)).await;

    let cancel = CancellationToken::new();
    let permit = coord
        .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
        .await
        .expect("should elect when all expected parked");
    assert!(permit.is_leader());
}

#[tokio::test(start_paused = true)]
async fn probe_elected_after_expected_removed_via_completion() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    // Two threads expected; thread-2 completes and is removed.
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-1".to_string(), "thread-2".to_string()],
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;
    coord.remove_participant(gen_val, "thread-2").await;

    // Advance past the reset deadline.
    tokio::time::advance(Duration::from_secs(2)).await;

    let cancel = CancellationToken::new();
    let permit = coord
        .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
        .await
        .expect("should elect when remaining expected all parked");
    assert!(permit.is_leader());
}

#[tokio::test(start_paused = true)]
async fn no_expected_participants_allows_probe_immediately() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(1);
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    // No expected participants registered — backward-compatible single-agent case.
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;

    // Advance past the reset deadline.
    tokio::time::advance(Duration::from_secs(2)).await;

    let cancel = CancellationToken::new();
    let permit = coord
        .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
        .await
        .expect("should elect when no expected participants");
    assert!(permit.is_leader());
}

// ---------------------------------------------------------------------------
// Full tree coordination lifecycle test
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn tree_coordination_lifecycle_seal_park_probe_resume() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::seconds(1);

    // 1. Thread-1 (root) encounters a waitable limit.
    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;

    // 2. Register all live tree members as expected participants.
    coord
        .register_expected_participants(
            gen_val,
            vec![
                "thread-1".to_string(),
                "thread-2".to_string(),
                "thread-3".to_string(),
            ],
        )
        .await;

    // 3. Thread-1 (triggering) parks immediately.
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;

    // 4. Advance past reset deadline; probe should NOT be elected because
    //    thread-2 and thread-3 have not parked yet.
    tokio::time::advance(Duration::from_secs(2)).await;
    let cancel1 = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        coord.wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel1),
    )
    .await;
    assert!(
        result.is_err(),
        "should not elect while thread-2 and thread-3 not parked"
    );

    // 5. Thread-2 parks.
    coord
        .register_parked_agent(gen_val, "thread-2".to_string())
        .await;
    let cancel2 = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        coord.wait_for_open_or_probe(gen_val, "thread-2".to_string(), cancel2),
    )
    .await;
    assert!(
        result.is_err(),
        "should not elect while thread-3 not parked"
    );

    // 6. Thread-3 genuinely completes — remove it from expected and parked.
    coord.remove_participant(gen_val, "thread-3").await;

    // 7. Now all remaining expected participants are parked. Probe should
    //    be elected.
    let cancel3 = CancellationToken::new();
    let permit = coord
        .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel3)
        .await
        .expect("should elect when all remaining expected parked");
    assert!(permit.is_leader());

    // 8. Leader probe succeeds — mark admitted, gate opens.
    coord
        .mark_probe_admitted(gen_val, ProbeOrigin::Automatic)
        .await;

    // 9. Thread-2 should see the gate open.
    let cancel4 = CancellationToken::new();
    let permit2 = coord
        .wait_for_open_or_probe(gen_val, "thread-2".to_string(), cancel4)
        .await
        .expect("gate should be open for thread-2");
    assert!(!permit2.is_leader());
    assert_eq!(permit2.generation(), gen_val);
}

#[tokio::test(start_paused = true)]
async fn tree_coordination_cancel_unblocks_all_waiters() {
    let coord = UsageLimitCoordinator::new(policy(), 0);
    let reset_at = Utc::now() + ChronoDuration::hours(1);

    let gen_val = coord
        .encounter_limit(
            "work-1".to_string(),
            "thread-1".to_string(),
            "turn-1".to_string(),
            known_encounter(Some(reset_at)),
        )
        .await;
    coord
        .register_expected_participants(
            gen_val,
            vec!["thread-1".to_string(), "thread-2".to_string()],
        )
        .await;
    coord
        .register_parked_agent(gen_val, "thread-1".to_string())
        .await;
    coord
        .register_parked_agent(gen_val, "thread-2".to_string())
        .await;

    // Spawn a waiter that should be unblocked by cancellation.
    let coord_clone = coord.clone();
    let handle = tokio::spawn(async move {
        let cancel = CancellationToken::new();

        coord_clone
            .wait_for_open_or_probe(gen_val, "thread-1".to_string(), cancel)
            .await
    });

    // Cancel the wait.
    coord.apply_action(gen_val, WaitAction::Cancel).await;

    // The waiter should return with WaitCancelled.
    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("waiter should be unblocked by cancel")
        .expect("task should not panic");
    assert!(result.is_err(), "should return WaitCancelled after cancel");
}
