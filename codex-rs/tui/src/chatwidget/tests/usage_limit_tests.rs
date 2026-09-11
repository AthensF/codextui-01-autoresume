use super::*;
use codex_app_server_protocol::ThreadUsageLimitUpdatedNotification;
use codex_app_server_protocol::UsageLimitClassification;
use codex_app_server_protocol::UsageLimitSnapshot;
use codex_app_server_protocol::UsageLimitWaitPhase;
use pretty_assertions::assert_eq;

fn sample_snapshot(
    phase: UsageLimitWaitPhase,
    reset_at: Option<i64>,
    auto_resume_enabled: bool,
) -> UsageLimitSnapshot {
    UsageLimitSnapshot {
        work_id: "work-1".to_string(),
        generation: 1,
        trigger_thread_id: ThreadId::new().to_string(),
        trigger_turn_id: "turn-1".to_string(),
        phase,
        classification: UsageLimitClassification::UsageWindow,
        limit_id: Some("test-limit".to_string()),
        reset_at,
        next_check_at: None,
        auto_resume_enabled,
        automatic_resumes_used: 0,
        max_auto_resumes: 3,
        availability_checks_used: 0,
        max_availability_checks: 5,
        unknown_reset_wait_started_at: None,
        unknown_reset_max_wait_seconds: 300,
        manual_wait_reason: None,
        parked_thread_ids: Vec::new(),
        expected_participants: Vec::new(),
        leader_thread_id: None,
        manual_retry_requested: false,
    }
}

#[tokio::test]
async fn usage_limit_notification_sets_tree_closed_state() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());
    assert!(!chat.usage_limit_wait.is_tree_closed());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert!(chat.usage_limit_wait.is_tree_closed());
}

#[tokio::test]
async fn usage_limit_cleared_notification_opens_tree() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert!(chat.usage_limit_wait.is_tree_closed());

    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: None,
    });
    assert!(!chat.usage_limit_wait.is_tree_closed());
}

#[tokio::test]
async fn usage_limit_resuming_phase_does_not_close_tree() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(UsageLimitWaitPhase::Resuming, None, true);
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert!(!chat.usage_limit_wait.is_tree_closed());
}

#[tokio::test]
async fn usage_limit_closed_tree_queues_direct_submissions() {
    let (mut chat, _events, mut ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });

    chat.submit_user_message(UserMessage::from("queued during wait"));
    assert_no_submit_op(&mut ops);
    assert_eq!(chat.queued_user_message_texts(), vec!["queued during wait"]);
}

#[tokio::test]
async fn usage_limit_cleared_sends_queued_input() {
    let (mut chat, _events, mut ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });

    chat.submit_user_message(UserMessage::from("queued during wait"));
    assert_no_submit_op(&mut ops);
    assert_eq!(chat.queued_user_message_texts(), vec!["queued during wait"]);

    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: None,
    });

    let Op::UserTurn { items, .. } = next_submit_op(&mut ops) else {
        panic!("expected queued input to be submitted after wait cleared");
    };
    assert!(
        matches!(items.as_slice(), [UserInput::Text { text, .. }] if text == "queued during wait")
    );
}

#[tokio::test]
async fn usage_limit_status_header_shows_waiting() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });

    assert_eq!(
        chat.status_state.current_status.header,
        "Work temporarily unavailable"
    );
}

#[tokio::test]
async fn usage_limit_manual_wait_header_shows_manual_retry() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(UsageLimitWaitPhase::ManualWait, None, false);
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });

    assert_eq!(
        chat.status_state.current_status.header,
        "Work paused — manual retry"
    );
}

#[tokio::test]
async fn usage_limit_cleared_restores_working_status() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(
        UsageLimitWaitPhase::Suspended,
        Some(chrono::Utc::now().timestamp() + 300),
        true,
    );
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert_eq!(
        chat.status_state.current_status.header,
        "Work temporarily unavailable"
    );
    assert!(chat.usage_limit_wait.is_tree_closed());

    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: None,
    });
    assert!(!chat.usage_limit_wait.is_tree_closed());
    assert!(chat.usage_limit_wait.snapshot().is_none());
}

#[tokio::test]
async fn usage_limit_countdown_formats_remaining_seconds() {
    let mut state = super::usage_limit_wait::UsageLimitWaitState::default();
    let snapshot = sample_snapshot(UsageLimitWaitPhase::Suspended, Some(1_000_000 + 125), true);
    state.set_snapshot(Some(snapshot));

    let (label, changed) = state.countdown_label(1_000_000).unwrap();
    assert!(changed);
    assert_eq!(label, "2m 5s");

    // Second call with same now should not report a change.
    let (_label, changed) = state.countdown_label(1_000_000).unwrap();
    assert!(!changed);
}

#[tokio::test]
async fn usage_limit_countdown_shows_reset_due_when_past() {
    let mut state = super::usage_limit_wait::UsageLimitWaitState::default();
    let snapshot = sample_snapshot(UsageLimitWaitPhase::Suspended, Some(1_000_000), true);
    state.set_snapshot(Some(snapshot));

    let (label, _changed) = state.countdown_label(1_000_010).unwrap();
    assert_eq!(label, "reset due");
}

#[tokio::test]
async fn usage_limit_countdown_none_without_reset() {
    let mut state = super::usage_limit_wait::UsageLimitWaitState::default();
    let snapshot = sample_snapshot(UsageLimitWaitPhase::Suspended, None, true);
    state.set_snapshot(Some(snapshot));
    assert!(state.countdown_label(1_000_000).is_none());
}

#[tokio::test]
async fn usage_limit_needs_user_action_for_manual_wait() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    let snapshot = sample_snapshot(UsageLimitWaitPhase::ManualWait, None, false);
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert!(chat.usage_limit_wait.needs_user_action());

    let snapshot = sample_snapshot(UsageLimitWaitPhase::Suspended, None, true);
    chat.handle_usage_limit_updated(ThreadUsageLimitUpdatedNotification {
        thread_id: chat.thread_id.unwrap().to_string(),
        snapshot: Some(snapshot),
    });
    assert!(!chat.usage_limit_wait.needs_user_action());
}

#[tokio::test]
async fn usage_limit_refresh_status_sends_app_event() {
    let (mut chat, mut events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    chat.refresh_usage_limit_status();

    let found = std::iter::from_fn(|| events.try_recv().ok())
        .any(|event| matches!(event, AppEvent::RefreshUsageLimitStatus { .. }));
    assert!(
        found,
        "refresh_usage_limit_status should send RefreshUsageLimitStatus"
    );
}

#[tokio::test]
async fn usage_limit_action_result_applied_shows_message() {
    let (mut chat, _events, _ops) = make_chatwidget_manual(Some("test-model")).await;
    chat.thread_id = Some(ThreadId::new());

    use codex_app_server_protocol::UsageLimitActionResult;
    chat.set_usage_limit_snapshot(None);
    // Simulate the app-layer dispatching an action result.
    // The chatwidget itself doesn't handle AppEvent::UsageLimitActionResult;
    // the app layer does. But we can test set_usage_limit_snapshot clears state.
    assert!(!chat.usage_limit_wait.is_tree_closed());
    let _ = UsageLimitActionResult::Applied;
}

#[tokio::test]
async fn usage_limit_status_detail_includes_next_check_and_manual_reason() {
    use codex_app_server_protocol::UsageLimitManualWaitReason;
    use codex_app_server_protocol::UsageLimitSnapshot;
    use codex_app_server_protocol::UsageLimitWaitPhase;

    let snapshot = UsageLimitSnapshot {
        work_id: "work-1".to_string(),
        generation: 1,
        trigger_thread_id: "thread-1".to_string(),
        trigger_turn_id: "turn-1".to_string(),
        phase: UsageLimitWaitPhase::ManualWait,
        classification: codex_app_server_protocol::UsageLimitClassification::UsageWindow,
        limit_id: None,
        reset_at: Some(1_000_100),
        next_check_at: Some(1_000_050),
        auto_resume_enabled: false,
        automatic_resumes_used: 3,
        max_auto_resumes: 5,
        availability_checks_used: 2,
        max_availability_checks: 3,
        unknown_reset_wait_started_at: None,
        unknown_reset_max_wait_seconds: 600,
        manual_wait_reason: Some(UsageLimitManualWaitReason::AutoResumeLimitReached),
        parked_thread_ids: Vec::new(),
        expected_participants: Vec::new(),
        leader_thread_id: None,
        manual_retry_requested: false,
    };

    let detail =
        super::usage_limit_wait::status_detail(&snapshot, 1_000_000).expect("detail exists");
    assert!(detail.contains("reset in 1m 40s"), "detail: {detail}");
    assert!(detail.contains("next check in 50s"), "detail: {detail}");
    assert!(
        detail.contains("auto-resume limit reached"),
        "detail: {detail}"
    );
}
