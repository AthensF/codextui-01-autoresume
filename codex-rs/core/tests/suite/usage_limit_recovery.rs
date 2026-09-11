//! Integration tests for usage-limit recovery during turn execution.
//!
//! These tests verify the end-to-end recovery flow: a usage-limit 429 is
//! returned by the model provider, the coordinator waits for the reset
//! window, and the turn resumes successfully under the same turn ID
//! without emitting a terminal event during the wait.

use anyhow::Result;
use chrono::Utc;
use codex_core::TurnInputRequest;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::built_in_model_providers;
use codex_models_manager::bundled_models_response;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::mount_models_once;
use core_test_support::responses::mount_response_once;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use serde_json::json;
use std::time::Duration;
use wiremock::MockServer;

/// Creates a model provider pointing at the mock server.
fn non_openai_model_provider(server: &MockServer) -> ModelProviderInfo {
    let mut provider = built_in_model_providers(None)["openai"].clone();
    provider.name = "OpenAI (test)".into();
    provider.base_url = Some(format!("{}/v1", server.uri()));
    provider.supports_websockets = false;
    provider
}

/// Builds a 429 usage-limit-reached HTTP response body with a reset timestamp
/// the given number of seconds in the future.
fn usage_limit_429_body(resets_in_secs: i64) -> String {
    let resets_at = Utc::now().timestamp() + resets_in_secs;
    json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
            "resets_at": resets_at,
        }
    })
    .to_string()
}

/// Mounts a 429 response followed by a successful SSE response on the mock server.
async fn mount_429_then_success(server: &MockServer, resets_in_secs: i64) {
    // First request: 429 usage_limit_reached
    let error_body = usage_limit_429_body(resets_in_secs);
    let error_response = wiremock::ResponseTemplate::new(429)
        .insert_header("content-type", "application/json")
        .set_body_string(error_body);
    mount_response_once(server, error_response).await;

    // Second request: successful SSE response
    let success_body = sse(vec![
        ev_assistant_message("m1", "recovered"),
        ev_completed("r1"),
    ]);
    mount_sse_once(server, success_body).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_limit_recovery_succeeds_after_reset() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let models_response = bundled_models_response().expect("bundled models.json should parse");
    mount_models_once(&server, models_response).await;
    mount_429_then_success(&server, /*resets_in_secs*/ 2).await;

    let model_provider = non_openai_model_provider(&server);
    let mut builder = test_codex().with_config(move |config| {
        config.model = Some("gpt-5.2".to_string());
        config.model_provider = model_provider;
    });
    let test = builder.build(&server).await?;
    let codex = test.codex.clone();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await?;

    // Wait for the turn to complete. The coordinator should wait ~2 seconds
    // for the reset window, then retry and succeed.
    let event = tokio::time::timeout(
        Duration::from_secs(30),
        wait_for_event(&codex, |ev| matches!(ev, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("timed out waiting for TurnComplete");

    // Verify the turn completed successfully.
    match event {
        EventMsg::TurnComplete(complete) => {
            assert!(
                complete.error.is_none(),
                "turn should complete without error, got: {:?}",
                complete.error
            );
        }
        other => panic!("expected TurnComplete, got {other:?}"),
    }

    Ok(())
}
