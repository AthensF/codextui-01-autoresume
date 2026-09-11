use std::time::Duration;

use anyhow::Result;
use app_test_support::TestAppServer;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadUsageLimitActionParams;
use codex_app_server_protocol::ThreadUsageLimitActionResponse;
use codex_app_server_protocol::ThreadUsageLimitReadParams;
use codex_app_server_protocol::ThreadUsageLimitReadResponse;
use codex_app_server_protocol::UsageLimitAction;
use codex_app_server_protocol::UsageLimitActionResult;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

const READ_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn usage_limit_read_requires_experimental_api_capability() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build()
        .await?;

    app.initialize_with_capabilities(
        ClientInfo {
            name: "usage-limit-gate".to_string(),
            title: None,
            version: "0.1.0".to_string(),
        },
        Some(InitializeCapabilities {
            experimental_api: false,
            ..Default::default()
        }),
    )
    .await?;

    // Use a dummy thread_id — the experimental gate should reject
    // the request before the thread is looked up.
    let request_id = app
        .send_raw_request(
            "thread/usageLimit/read",
            Some(json!({ "threadId": "dummy" })),
        )
        .await?;
    let error: JSONRPCError = timeout(
        READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert!(
        error.error.message.contains("experimental"),
        "expected experimental error, got: {}",
        error.error.message
    );
    Ok(())
}

#[tokio::test]
async fn usage_limit_action_requires_experimental_api_capability() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .without_auto_env()
        .build()
        .await?;

    app.initialize_with_capabilities(
        ClientInfo {
            name: "usage-limit-action-gate".to_string(),
            title: None,
            version: "0.1.0".to_string(),
        },
        Some(InitializeCapabilities {
            experimental_api: false,
            ..Default::default()
        }),
    )
    .await?;

    let request_id = app
        .send_raw_request(
            "thread/usageLimit/action",
            Some(json!({
                "threadId": "dummy",
                "generation": 0,
                "action": "retryNow"
            })),
        )
        .await?;
    let error: JSONRPCError = timeout(
        READ_TIMEOUT,
        app.read_stream_until_error_message(RequestId::Integer(request_id)),
    )
    .await??;
    assert!(
        error.error.message.contains("experimental"),
        "expected experimental error, got: {}",
        error.error.message
    );
    Ok(())
}

#[tokio::test]
async fn usage_limit_read_returns_null_when_no_wait_active() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build()
        .await?;

    app.initialize_with_capabilities(
        ClientInfo {
            name: "usage-limit-read".to_string(),
            title: None,
            version: "0.1.0".to_string(),
        },
        Some(InitializeCapabilities {
            experimental_api: true,
            ..Default::default()
        }),
    )
    .await?;

    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let request_id = app
        .send_raw_request(
            "thread/usageLimit/read",
            Some(serde_json::to_value(ThreadUsageLimitReadParams {
                thread_id: thread.id.clone(),
            })?),
        )
        .await?;
    let response: ThreadUsageLimitReadResponse =
        timeout(READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(
        response,
        ThreadUsageLimitReadResponse { snapshot: None },
        "expected null snapshot when no wait is active"
    );
    Ok(())
}

#[tokio::test]
async fn usage_limit_action_with_stale_generation_returns_stale() -> Result<()> {
    let codex_home = TempDir::new()?;
    let mut app = TestAppServer::builder()
        .with_codex_home(codex_home.path())
        .build()
        .await?;

    app.initialize_with_capabilities(
        ClientInfo {
            name: "usage-limit-action".to_string(),
            title: None,
            version: "0.1.0".to_string(),
        },
        Some(InitializeCapabilities {
            experimental_api: true,
            ..Default::default()
        }),
    )
    .await?;

    let thread = app.start_thread(ThreadStartParams::default()).await?.thread;
    let request_id = app
        .send_raw_request(
            "thread/usageLimit/action",
            Some(serde_json::to_value(ThreadUsageLimitActionParams {
                thread_id: thread.id,
                generation: 999,
                action: UsageLimitAction::RetryNow,
            })?),
        )
        .await?;
    let response: ThreadUsageLimitActionResponse =
        timeout(READ_TIMEOUT, app.read_response(request_id)).await??;
    assert_eq!(
        response.result,
        UsageLimitActionResult::StaleGeneration,
        "expected stale generation for non-existent generation"
    );
    Ok(())
}
