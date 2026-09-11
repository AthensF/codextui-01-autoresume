use super::*;
use pretty_assertions::assert_eq;

#[test]
fn wait_phase_is_terminal() {
    assert!(WaitPhase::ManualWait.is_terminal());
    assert!(WaitPhase::AttentionRequired.is_terminal());
    assert!(WaitPhase::Cancelled.is_terminal());
    assert!(!WaitPhase::Queued.is_terminal());
    assert!(!WaitPhase::Suspended.is_terminal());
    assert!(!WaitPhase::Revalidating.is_terminal());
    assert!(!WaitPhase::Resuming.is_terminal());
}

#[test]
fn wait_phase_is_waiting() {
    assert!(WaitPhase::Queued.is_waiting());
    assert!(WaitPhase::Suspending.is_waiting());
    assert!(WaitPhase::Suspended.is_waiting());
    assert!(WaitPhase::Revalidating.is_waiting());
    assert!(WaitPhase::ManualWait.is_waiting());
    assert!(WaitPhase::AttentionRequired.is_waiting());
    assert!(!WaitPhase::Resuming.is_waiting());
    assert!(!WaitPhase::Cancelled.is_waiting());
}

#[test]
fn limit_classification_roundtrips() {
    for c in [
        LimitClassification::UsageWindow,
        LimitClassification::TransientRateLimit,
        LimitClassification::GenericTooManyRequests,
    ] {
        let json = serde_json::to_string(&c).unwrap();
        let back: LimitClassification = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}

#[test]
fn manual_wait_reason_roundtrips() {
    for r in [
        ManualWaitReason::AutoResumeDisabled,
        ManualWaitReason::AutoResumeLimitReached,
        ManualWaitReason::AvailabilityCheckLimitReached,
        ManualWaitReason::WaitCeilingReached,
    ] {
        let json = serde_json::to_string(&r).unwrap();
        let back: ManualWaitReason = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
