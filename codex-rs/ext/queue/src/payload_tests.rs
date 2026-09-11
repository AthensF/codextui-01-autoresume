use super::*;
use codex_core::TurnInput;
use codex_protocol::ThreadId;
use codex_protocol::user_input::UserInput;
use pretty_assertions::assert_eq;

fn user_text(text: &str) -> TurnInput {
    TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: Some("client-1".to_string()),
    }
}

#[test]
fn encodes_and_decodes_v1_envelope() {
    let input = user_text("hello");
    let json = QueuePayload::user_message(input.clone()).unwrap();
    let decoded = decode_payload(&json).unwrap();
    assert_eq!(Some(input), decoded.into_turn_input());
}

#[test]
fn decodes_legacy_raw_turn_input_as_v0() {
    let input = user_text("legacy");
    let raw = serde_json::to_string(&input).unwrap();
    let decoded = decode_payload(&raw).unwrap();
    assert_eq!(QueuePayload::V0(input.clone()), decoded);
    assert_eq!(Some(input), decoded.into_turn_input());
}

#[test]
fn decode_turn_input_returns_none_for_suspended_tree_recovery() {
    let manifest = SuspendedTreeRecoveryManifest {
        root_thread_id: ThreadId::new(),
        root_turn_id: "turn-1".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 1,
        not_before_ms: 0,
        replay_safety: ReplaySafetySummary::Safe {
            verified_tool_calls: 0,
            categories: ReplayEffectCategories::default(),
        },
    };
    let json = QueuePayload::suspended_tree_recovery(manifest).unwrap();
    // decode_turn_input returns None because this is not a user message.
    assert_eq!(None, decode_turn_input(&json).unwrap());
}

#[test]
fn decode_suspended_tree_recovery_round_trips() {
    let root_thread_id = ThreadId::new();
    let child_thread_id = ThreadId::new();
    let manifest = SuspendedTreeRecoveryManifest {
        root_thread_id,
        root_turn_id: "root-turn".to_string(),
        original_input: Some(user_text("original")),
        members: vec![SuspendedTreeMember {
            thread_id: child_thread_id,
            turn_id: "child-turn".to_string(),
            parent_thread_id: root_thread_id,
            parent_turn_id: "root-turn".to_string(),
            root_turn_id: "root-turn".to_string(),
            agent_path: "root.child".to_string(),
            replay_safety: ReplaySafetySummary::Safe {
                verified_tool_calls: 1,
                categories: ReplayEffectCategories::default(),
            },
        }],
        coordinator_generation: 3,
        not_before_ms: 1_700_000_000_000,
        replay_safety: ReplaySafetySummary::Safe {
            verified_tool_calls: 2,
            categories: ReplayEffectCategories::default(),
        },
    };
    let json = QueuePayload::suspended_tree_recovery(manifest.clone()).unwrap();
    let decoded = decode_suspended_tree_recovery(&json).unwrap();
    assert_eq!(Some(manifest), decoded);
}

#[test]
fn decode_suspended_tree_recovery_preserves_attention_required() {
    let manifest = SuspendedTreeRecoveryManifest {
        root_thread_id: ThreadId::new(),
        root_turn_id: "turn-1".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 0,
        not_before_ms: 0,
        replay_safety: ReplaySafetySummary::AttentionRequired {
            reason: "pending approval".to_string(),
        },
    };
    let json = QueuePayload::suspended_tree_recovery(manifest.clone()).unwrap();
    let decoded = decode_suspended_tree_recovery(&json).unwrap();
    assert_eq!(Some(manifest), decoded);
}

#[test]
fn unknown_job_kind_still_rejected() {
    // A completely unknown job kind must still fail to decode.
    let json = r#"{"version":"V1","job":{"kind":"unknownFutureKind","data":"x"}}"#;
    assert!(decode_payload(json).is_err());
}

#[test]
fn round_trips_through_string_column() {
    let input = user_text("round-trip");
    let json = QueuePayload::user_message(input.clone()).unwrap();
    let back = decode_turn_input(&json).unwrap();
    assert_eq!(Some(input), back);
}
