use super::*;

#[test]
fn attention_required_reason_formats_thread_ids() {
    let reason = attention_required_reason(&[ThreadId::new(), ThreadId::new()]);
    assert!(reason.starts_with("ambiguous side effects in tree members:"));
    // Two thread IDs should appear, separated by a comma.
    assert_eq!(reason.matches(',').count(), 1);
}

#[test]
fn tree_suspension_safety_safe_variant_carries_members() {
    let members = vec![
        MemberSafetyState {
            thread_id: ThreadId::new(),
            continuation_safety: ContinuationSafety::Untouched,
        },
        MemberSafetyState {
            thread_id: ThreadId::new(),
            continuation_safety: ContinuationSafety::SafeToSuspend,
        },
    ];
    let safety = TreeSuspensionSafety::Safe { members };
    match safety {
        TreeSuspensionSafety::Safe { members } => {
            assert_eq!(2, members.len());
        }
        TreeSuspensionSafety::AttentionRequired { .. } => {
            panic!("expected Safe");
        }
    }
}

#[test]
fn tree_suspension_safety_attention_required_carries_reason() {
    let thread_id = ThreadId::new();
    let safety = TreeSuspensionSafety::AttentionRequired {
        ambiguous_members: vec![thread_id.clone()],
        reason: "test reason".to_string(),
    };
    match safety {
        TreeSuspensionSafety::AttentionRequired {
            ambiguous_members,
            reason,
        } => {
            assert_eq!(1, ambiguous_members.len());
            assert_eq!("test reason", reason);
        }
        TreeSuspensionSafety::Safe { .. } => {
            panic!("expected AttentionRequired");
        }
    }
}
