use super::*;

#[test]
fn completed_categories_are_safe() {
    assert_eq!(
        classify_category(ReplaySafetyCategory::CompletedShellCommand),
        CategorySafety::Safe
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::CompletedFileEdit),
        CategorySafety::Safe
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::CompletedMcpTool),
        CategorySafety::Safe
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::CompletedDynamicTool),
        CategorySafety::Safe
    );
}

#[test]
fn pending_categories_block_recovery() {
    assert_eq!(
        classify_category(ReplaySafetyCategory::PendingApproval),
        CategorySafety::BlocksRecovery
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::InteractiveInput),
        CategorySafety::BlocksRecovery
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::BackgroundTerminal),
        CategorySafety::BlocksRecovery
    );
    assert_eq!(
        classify_category(ReplaySafetyCategory::UnverifiableRemoteEffect),
        CategorySafety::BlocksRecovery
    );
}

#[test]
fn classify_replay_safety_safe_when_no_blocking() {
    let result = classify_replay_safety(&[], 5);
    assert_eq!(
        result,
        ReplaySafetySummary::Safe {
            verified_tool_calls: 5
        }
    );
}

#[test]
fn classify_replay_safety_attention_required_when_blocking() {
    let result = classify_replay_safety(
        &[
            ReplaySafetyCategory::PendingApproval,
            ReplaySafetyCategory::CompletedShellCommand,
        ],
        3,
    );
    match result {
        ReplaySafetySummary::AttentionRequired { reason } => {
            assert!(reason.contains("PendingApproval"));
            // Completed categories should not appear in the blocking list.
            assert!(!reason.contains("CompletedShellCommand"));
        }
        ReplaySafetySummary::Safe { .. } => panic!("expected AttentionRequired"),
    }
}

#[test]
fn classify_replay_safety_multiple_blocking() {
    let result = classify_replay_safety(
        &[
            ReplaySafetyCategory::PendingApproval,
            ReplaySafetyCategory::BackgroundTerminal,
        ],
        0,
    );
    match result {
        ReplaySafetySummary::AttentionRequired { reason } => {
            assert!(reason.contains("PendingApproval"));
            assert!(reason.contains("BackgroundTerminal"));
        }
        ReplaySafetySummary::Safe { .. } => panic!("expected AttentionRequired"),
    }
}
