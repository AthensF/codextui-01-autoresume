use super::*;
use base64::Engine;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::RateLimitReachedType;
use pretty_assertions::assert_eq;

#[test]
fn map_api_error_maps_server_overloaded() {
    let err = map_api_error(ApiError::ServerOverloaded);
    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_preserves_retry_delay() {
    let retry_delay = std::time::Duration::from_secs(17);
    // `Retryable` stream errors continue to use the legacy `retry_delay` field.
    let err = map_api_error(ApiError::Retryable {
        message: "retry later".to_string(),
        delay: Some(retry_delay),
    });
    assert_eq!(
        (
            err.to_codex_protocol_error(),
            err.retry_delay(),
            err.is_retryable(),
            err.http_status_code_value(),
            err.to_string(),
        ),
        (
            CodexErrorInfo::Other,
            Some(retry_delay),
            true,
            None,
            "stream disconnected before completion: retry later".to_string(),
        )
    );
}

#[test]
fn map_api_error_preserves_rate_limit_delay_in_both_retry_delay_and_recovery_hint() {
    let retry_delay = std::time::Duration::from_secs(17);
    let err = map_api_error(ApiError::RateLimitExceeded {
        message: "retry later".to_string(),
        delay: Some(retry_delay),
        resets_at: None,
    });
    assert_eq!(
        err.to_codex_protocol_error(),
        CodexErrorInfo::RateLimitExceeded
    );
    // The legacy `retry_delay` field must remain populated so the existing
    // short retry loop continues to work alongside the coordinator hint.
    assert_eq!(err.retry_delay(), Some(retry_delay));
    assert!(err.is_retryable());
    assert_eq!(err.http_status_code_value(), None);
    assert_eq!(err.to_string(), "rate limit exceeded: retry later");
    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert_eq!(hint.kind, UsageLimitRecoveryKind::TransientRateLimit);
    assert_eq!(hint.retry_after, Some(retry_delay));
}

#[test]
fn map_api_error_maps_server_overloaded_from_503_body() {
    let body = serde_json::json!({
        "error": {
            "code": "server_is_overloaded"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::SERVICE_UNAVAILABLE,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    assert!(matches!(err.details(), CodexErrorDetails::ServerOverloaded));
}

#[test]
fn map_api_error_maps_cloudflare_blocked_response_to_user_message() {
    let mut headers = HeaderMap::new();
    headers.insert(CF_RAY_HEADER, http::HeaderValue::from_static("ray-id"));
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::FORBIDDEN,
        url: Some("http://example.com/blocked".to_string()),
        headers: Some(headers),
        body: Some(
            "<html><body>Cloudflare error: Sorry, you have been blocked</body></html>".to_string(),
        ),
    }));

    let CodexErrorDetails::UnexpectedStatus(err) = err.details() else {
        panic!("expected CodexErrorDetails::UnexpectedStatus, got {err:?}");
    };
    assert_eq!(
        err.user_message.as_deref(),
        Some(
            "Access blocked by Cloudflare. This usually happens when connecting from a restricted region (status 403 Forbidden)"
        )
    );
    assert_eq!(
        err.to_string(),
        "Access blocked by Cloudflare. This usually happens when connecting from a restricted region (status 403 Forbidden), url: http://example.com/blocked, cf-ray: ray-id"
    );
}

#[test]
fn map_api_error_maps_cyber_policy_from_400_body() {
    let body = serde_json::json!({
        "error": {
            "message": "This request has been flagged for potentially high-risk cyber activity.",
            "type": "invalid_request",
            "param": null,
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(
        message,
        "This request has been flagged for potentially high-risk cyber activity."
    );
}

#[test]
fn map_api_error_maps_wrapped_websocket_cyber_policy_from_400_body() {
    let body = serde_json::json!({
        "type": "error",
        "status": 400,
        "error": {
            "message": "This websocket request was flagged.",
            "type": "invalid_request",
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("ws://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(message, "This websocket request was flagged.");
}

#[test]
fn map_api_error_uses_cyber_policy_fallback_for_missing_message() {
    let body = serde_json::json!({
        "error": {
            "code": "cyber_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::CyberPolicy { message } = err.details() else {
        panic!("expected CodexErrorDetails::CyberPolicy, got {err:?}");
    };
    assert_eq!(
        message,
        "This request has been flagged for possible cybersecurity risk."
    );
}

#[test]
fn map_api_error_maps_misalignment_policy_violation_from_400_body() {
    assert_misalignment_policy_violation_from_http_body(http::StatusCode::BAD_REQUEST);
}

#[test]
fn map_api_error_maps_misalignment_policy_violation_from_403_body() {
    assert_misalignment_policy_violation_from_http_body(http::StatusCode::FORBIDDEN);
}

fn assert_misalignment_policy_violation_from_http_body(status: http::StatusCode) {
    let body = serde_json::json!({
        "error": {
            "message": "This request violated the misalignment policy.",
            "type": "invalid_request_error",
            "code": "misalignment_policy_violation"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::MisalignmentPolicyViolation {
        message,
        misalignment,
    } = err.details()
    else {
        panic!("expected CodexErrorDetails::MisalignmentPolicyViolation, got {err:?}");
    };
    assert_eq!(message, "This request violated the misalignment policy.");
    assert_eq!(misalignment, &None);
    assert!(!err.is_retryable());
}

#[test]
fn map_api_error_preserves_misalignment_details_from_403_body() {
    let body = serde_json::json!({
        "error": {
            "message": "This request violated the misalignment policy.",
            "code": "misalignment_policy_violation",
            "misalignment": {
                "error_type": "unauthorized_data_transfer",
                "detailed_explanation": "The agent attempted an external transfer.",
                "steer": { "message": "Do not transfer the user's files." }
            }
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::FORBIDDEN,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::MisalignmentPolicyViolation {
        message,
        misalignment,
    } = err.details()
    else {
        panic!("expected CodexErrorDetails::MisalignmentPolicyViolation, got {err:?}");
    };
    assert_eq!(message, "This request violated the misalignment policy.");
    assert_eq!(
        misalignment,
        &Some(MisalignmentErrorDetails {
            error_type: Some("unauthorized_data_transfer".to_string()),
            detailed_explanation: Some("The agent attempted an external transfer.".to_string()),
            steer: Some(codex_protocol::protocol::MisalignmentSteer {
                message: "Do not transfer the user's files.".to_string(),
            }),
        })
    );
    assert!(!err.is_retryable());
}

#[test]
fn map_api_error_preserves_misalignment_details_from_wrapped_websocket_error() {
    let body = serde_json::json!({
        "type": "error",
        "status": 403,
        "error": {
            "message": "This websocket request violated the misalignment policy.",
            "code": "misalignment_policy_violation",
            "misalignment": {
                "error_type": "future_safety_category",
                "detailed_explanation": "The agent attempted an external transfer.",
                "steer": { "message": "Do not transfer the user's files." }
            }
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::FORBIDDEN,
        url: Some("ws://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body),
    }));

    let CodexErrorDetails::MisalignmentPolicyViolation {
        message,
        misalignment,
    } = err.details()
    else {
        panic!("expected CodexErrorDetails::MisalignmentPolicyViolation, got {err:?}");
    };
    assert_eq!(
        message,
        "This websocket request violated the misalignment policy."
    );
    assert_eq!(
        misalignment,
        &Some(MisalignmentErrorDetails {
            error_type: Some("future_safety_category".to_string()),
            detailed_explanation: Some("The agent attempted an external transfer.".to_string()),
            steer: Some(codex_protocol::protocol::MisalignmentSteer {
                message: "Do not transfer the user's files.".to_string(),
            }),
        })
    );
    assert!(!err.is_retryable());
}

#[test]
fn map_api_error_keeps_unknown_400_errors_generic() {
    let body = serde_json::json!({
        "error": {
            "message": "Some other bad request.",
            "code": "some_other_policy"
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::BAD_REQUEST,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: None,
        body: Some(body.clone()),
    }));

    let CodexErrorDetails::InvalidRequest(message) = err.details() else {
        panic!("expected CodexErrorDetails::InvalidRequest, got {err:?}");
    };
    assert_eq!(message, &body);
}

#[test]
fn map_api_error_maps_usage_limit_limit_name_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACTIVE_LIMIT_HEADER,
        http::HeaderValue::from_static("codex_other"),
    );
    headers.insert(
        "x-codex-other-limit-name",
        http::HeaderValue::from_static("codex_other"),
    );
    let body = serde_json::json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: Some(headers),
        body: Some(body),
    }));

    let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
        panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
    };
    assert_eq!(
        usage_limit
            .rate_limits
            .as_ref()
            .and_then(|snapshot| snapshot.limit_name.as_deref()),
        Some("codex_other")
    );
}

#[test]
fn map_api_error_does_not_fallback_limit_name_to_limit_id() {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACTIVE_LIMIT_HEADER,
        http::HeaderValue::from_static("codex_other"),
    );
    let body = serde_json::json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: Some(headers),
        body: Some(body),
    }));

    let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
        panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
    };
    assert_eq!(
        usage_limit
            .rate_limits
            .as_ref()
            .and_then(|snapshot| snapshot.limit_name.as_deref()),
        None
    );
}

#[test]
fn map_api_error_copies_rate_limit_reached_type_to_usage_limit_snapshot() {
    for (active_limit, expected_limit_id) in [(None, "codex"), (Some("codex_other"), "codex_other")]
    {
        let mut headers = HeaderMap::new();
        if let Some(active_limit) = active_limit {
            headers.insert(
                ACTIVE_LIMIT_HEADER,
                http::HeaderValue::from_static(active_limit),
            );
        }
        for (name, value) in [
            ("x-codex-credits-has-credits", "true"),
            ("x-codex-credits-unlimited", "false"),
            ("x-codex-credits-balance", ""),
            (
                "x-codex-rate-limit-reached-type",
                "workspace_member_usage_limit_reached",
            ),
        ] {
            headers.insert(name, http::HeaderValue::from_static(value));
        }
        let body = serde_json::json!({
            "error": {
                "type": "usage_limit_reached",
                "plan_type": "pro",
            }
        })
        .to_string();

        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::TOO_MANY_REQUESTS,
            url: Some("http://example.com/v1/responses".to_string()),
            headers: Some(headers),
            body: Some(body),
        }));

        let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
            panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
        };
        assert_eq!(
            usage_limit.rate_limit_reached_type,
            Some(RateLimitReachedType::WorkspaceMemberUsageLimitReached)
        );
        let snapshot = usage_limit
            .rate_limits
            .as_ref()
            .expect("usage limit snapshot");
        assert_eq!(snapshot.limit_id.as_deref(), Some(expected_limit_id));
        assert_eq!(
            snapshot.rate_limit_reached_type,
            Some(RateLimitReachedType::WorkspaceMemberUsageLimitReached)
        );
        assert_eq!(
            snapshot.credits.as_ref().map(|credits| (
                credits.has_credits,
                credits.unlimited,
                credits.balance.as_deref()
            )),
            Some((true, false, None))
        );
    }
}

#[test]
fn map_api_error_ignores_unparseable_rate_limit_reached_type_headers() {
    let values = [
        http::HeaderValue::from_static("future_rate_limit_reached_type"),
        http::HeaderValue::from_bytes(&[0xff]).expect("valid opaque header value"),
    ];

    for value in values {
        let mut headers = HeaderMap::new();
        headers.insert("x-codex-rate-limit-reached-type", value);
        let body = serde_json::json!({
            "error": {
                "type": "usage_limit_reached",
                "plan_type": "pro",
            }
        })
        .to_string();
        let err = map_api_error(ApiError::Transport(TransportError::Http {
            status: http::StatusCode::TOO_MANY_REQUESTS,
            url: Some("http://example.com/v1/responses".to_string()),
            headers: Some(headers),
            body: Some(body),
        }));

        let CodexErrorDetails::UsageLimitReached(usage_limit) = err.details() else {
            panic!("expected CodexErrorDetails::UsageLimitReached, got {err:?}");
        };
        assert_eq!(usage_limit.rate_limit_reached_type, None);
    }
}

#[test]
fn map_api_error_extracts_identity_auth_details_from_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(REQUEST_ID_HEADER, http::HeaderValue::from_static("req-401"));
    headers.insert(CF_RAY_HEADER, http::HeaderValue::from_static("ray-401"));
    headers.insert(
        X_OPENAI_AUTHORIZATION_ERROR_HEADER,
        http::HeaderValue::from_static("missing_authorization_header"),
    );
    let x_error_json =
        base64::engine::general_purpose::STANDARD.encode(r#"{"error":{"code":"token_expired"}}"#);
    headers.insert(
        X_ERROR_JSON_HEADER,
        http::HeaderValue::from_str(&x_error_json).expect("valid x-error-json header"),
    );

    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::UNAUTHORIZED,
        url: Some("https://chatgpt.com/backend-api/codex/models".to_string()),
        headers: Some(headers),
        body: Some(r#"{"detail":"Unauthorized"}"#.to_string()),
    }));

    let CodexErrorDetails::UnexpectedStatus(err) = err.details() else {
        panic!("expected CodexErrorDetails::UnexpectedStatus, got {err:?}");
    };
    assert_eq!(err.request_id.as_deref(), Some("req-401"));
    assert_eq!(err.cf_ray.as_deref(), Some("ray-401"));
    assert_eq!(
        err.identity_authorization_error.as_deref(),
        Some("missing_authorization_header")
    );
    assert_eq!(err.identity_error_code.as_deref(), Some("token_expired"));
}

#[test]
fn map_api_error_attaches_recovery_hint_to_usage_limit_reached() {
    let mut headers = HeaderMap::new();
    headers.insert(ACTIVE_LIMIT_HEADER, http::HeaderValue::from_static("codex"));
    let future_reset = (Utc::now() + chrono::Duration::hours(1)).timestamp();
    let body = serde_json::json!({
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
            "resets_at": future_reset,
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: Some("http://example.com/v1/responses".to_string()),
        headers: Some(headers),
        body: Some(body),
    }));

    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert_eq!(hint.kind, UsageLimitRecoveryKind::UsageWindow);
    assert_eq!(hint.http_status_code, Some(429));
    assert!(hint.resets_at.is_some());
    assert_eq!(hint.limit_id.as_deref(), Some("codex"));
}

#[test]
fn map_api_error_attaches_recovery_hint_to_generic_429_with_retry_after_seconds() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", http::HeaderValue::from_static("120"));
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers: Some(headers),
        body: Some("unrecognized 429 body".to_string()),
    }));

    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert_eq!(hint.kind, UsageLimitRecoveryKind::GenericTooManyRequests);
    assert_eq!(hint.http_status_code, Some(429));
    assert_eq!(hint.retry_after, Some(Duration::from_secs(120)));
}

#[test]
fn map_api_error_attaches_recovery_hint_to_rate_limit_exceeded_stream_error() {
    let future_reset = (Utc::now() + chrono::Duration::hours(1)).timestamp();
    let err = map_api_error(ApiError::RateLimitExceeded {
        message: "rate limit reached".to_string(),
        delay: Some(Duration::from_secs(30)),
        resets_at: Some(future_reset),
    });

    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert_eq!(hint.kind, UsageLimitRecoveryKind::TransientRateLimit);
    assert_eq!(hint.retry_after, Some(Duration::from_secs(30)));
    assert!(hint.resets_at.is_some());
}

#[test]
fn parse_retry_after_value_handles_delta_seconds() {
    let value = http::HeaderValue::from_static("300");
    assert_eq!(
        parse_retry_after_value(&value),
        Some(Duration::from_secs(300))
    );
}

#[test]
fn parse_retry_after_value_handles_http_date() {
    let future = Utc::now() + chrono::Duration::hours(1);
    let date_str = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    let value = http::HeaderValue::from_str(&date_str).unwrap();
    let duration = parse_retry_after_value(&value).expect("should parse future date");
    // Should be roughly 1 hour, allow for a small skew.
    assert!(duration.as_secs() > 3500 && duration.as_secs() < 3700);
}

#[test]
fn parse_retry_after_value_returns_none_for_past_date() {
    let past = Utc::now() - chrono::Duration::hours(1);
    let date_str = past.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    let value = http::HeaderValue::from_str(&date_str).unwrap();
    assert_eq!(parse_retry_after_value(&value), None);
}

#[test]
fn parse_retry_after_value_returns_none_for_malformed() {
    let value = http::HeaderValue::from_static("not-a-date-or-number");
    assert_eq!(parse_retry_after_value(&value), None);
}

#[test]
fn parse_retry_after_value_returns_none_for_zero_seconds() {
    let value = http::HeaderValue::from_static("0");
    assert_eq!(parse_retry_after_value(&value), None);
}

// ---------------------------------------------------------------------------
// Authoritative reset selection for `usage_limit_reached` responses.
// ---------------------------------------------------------------------------

fn usage_limit_reached_body(resets_at: Option<i64>) -> String {
    let mut error = serde_json::json!({
        "type": "usage_limit_reached",
        "plan_type": "pro",
    });
    if let Some(ts) = resets_at {
        error["resets_at"] = serde_json::json!(ts);
    }
    serde_json::json!({ "error": error }).to_string()
}

fn map_usage_limit_response(headers: Option<HeaderMap>, body: String) -> CodexErr {
    map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers,
        body: Some(body),
    }))
}

#[test]
fn authoritative_reset_prefers_body_resets_at_when_in_future() {
    let future = (Utc::now() + chrono::Duration::hours(2)).timestamp();
    let err = map_usage_limit_response(None, usage_limit_reached_body(Some(future)));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    let reset = hint.resets_at.expect("future reset");
    assert_eq!(reset.timestamp(), future);
}

#[test]
fn authoritative_reset_rejects_past_body_resets_at() {
    let past = (Utc::now() - chrono::Duration::hours(2)).timestamp();
    let err = map_usage_limit_response(None, usage_limit_reached_body(Some(past)));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    assert!(hint.resets_at.is_none(), "past reset should be rejected");
}

#[test]
fn authoritative_reset_uses_exhausted_window_when_body_resets_at_absent() {
    let future = (Utc::now() + chrono::Duration::hours(3)).timestamp();
    let mut headers = HeaderMap::new();
    headers.insert(ACTIVE_LIMIT_HEADER, http::HeaderValue::from_static("codex"));
    headers.insert(
        "x-codex-primary-used-percent",
        http::HeaderValue::from_static("100"),
    );
    headers.insert(
        "x-codex-primary-reset-at",
        http::HeaderValue::from_str(&future.to_string()).unwrap(),
    );

    let err = map_usage_limit_response(Some(headers), usage_limit_reached_body(None));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    let reset = hint.resets_at.expect("reset from exhausted window");
    assert_eq!(reset.timestamp(), future);
}

#[test]
fn authoritative_reset_ignores_non_exhausted_window() {
    let future = (Utc::now() + chrono::Duration::hours(3)).timestamp();
    let mut headers = HeaderMap::new();
    headers.insert(ACTIVE_LIMIT_HEADER, http::HeaderValue::from_static("codex"));
    headers.insert(
        "x-codex-primary-used-percent",
        http::HeaderValue::from_static("80"),
    );
    headers.insert(
        "x-codex-primary-reset-at",
        http::HeaderValue::from_str(&future.to_string()).unwrap(),
    );

    let err = map_usage_limit_response(Some(headers), usage_limit_reached_body(None));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    assert!(
        hint.resets_at.is_none(),
        "non-exhausted window should not provide reset"
    );
}

#[test]
fn authoritative_reset_picks_latest_among_body_and_exhausted_windows() {
    let body_future = (Utc::now() + chrono::Duration::hours(1)).timestamp();
    let window_future = (Utc::now() + chrono::Duration::hours(5)).timestamp();
    let mut headers = HeaderMap::new();
    headers.insert(ACTIVE_LIMIT_HEADER, http::HeaderValue::from_static("codex"));
    headers.insert(
        "x-codex-primary-used-percent",
        http::HeaderValue::from_static("100"),
    );
    headers.insert(
        "x-codex-primary-reset-at",
        http::HeaderValue::from_str(&window_future.to_string()).unwrap(),
    );
    headers.insert(
        "x-codex-secondary-used-percent",
        http::HeaderValue::from_static("100"),
    );
    headers.insert(
        "x-codex-secondary-reset-at",
        http::HeaderValue::from_str(&body_future.to_string()).unwrap(),
    );

    let err = map_usage_limit_response(Some(headers), usage_limit_reached_body(Some(body_future)));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    let reset = hint.resets_at.expect("reset");
    assert_eq!(
        reset.timestamp(),
        window_future,
        "should pick the latest future reset"
    );
}

#[test]
fn authoritative_reset_rejects_past_exhausted_window() {
    let past = (Utc::now() - chrono::Duration::hours(1)).timestamp();
    let mut headers = HeaderMap::new();
    headers.insert(ACTIVE_LIMIT_HEADER, http::HeaderValue::from_static("codex"));
    headers.insert(
        "x-codex-primary-used-percent",
        http::HeaderValue::from_static("100"),
    );
    headers.insert(
        "x-codex-primary-reset-at",
        http::HeaderValue::from_str(&past.to_string()).unwrap(),
    );

    let err = map_usage_limit_response(Some(headers), usage_limit_reached_body(None));
    let hint = err.usage_limit_recovery_hint().expect("hint");
    assert!(
        hint.resets_at.is_none(),
        "past window reset should be rejected"
    );
}

// ---------------------------------------------------------------------------
// Intervention-only classification for entitlement/quota errors.
// ---------------------------------------------------------------------------

#[test]
fn usage_not_included_remains_intervention_only_with_no_hint() {
    let body = serde_json::json!({
        "error": {
            "type": "usage_not_included",
        }
    })
    .to_string();
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers: None,
        body: Some(body),
    }));
    assert!(matches!(err.details(), CodexErrorDetails::UsageNotIncluded));
    assert!(err.usage_limit_recovery_hint().is_none());
    assert!(!err.is_retryable());
}

#[test]
fn quota_exceeded_remains_intervention_only_with_no_hint() {
    let err = map_api_error(ApiError::QuotaExceeded);
    assert!(matches!(err.details(), CodexErrorDetails::QuotaExceeded));
    assert!(err.usage_limit_recovery_hint().is_none());
    assert!(!err.is_retryable());
}

// ---------------------------------------------------------------------------
// Generic 429 with Retry-After edge cases.
// ---------------------------------------------------------------------------

#[test]
fn generic_429_with_zero_retry_after_treated_as_unknown() {
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", http::HeaderValue::from_static("0"));
    let err = map_api_error(ApiError::Transport(TransportError::Http {
        status: http::StatusCode::TOO_MANY_REQUESTS,
        url: None,
        headers: Some(headers),
        body: Some("unrecognized 429 body".to_string()),
    }));

    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert_eq!(hint.kind, UsageLimitRecoveryKind::GenericTooManyRequests);
    assert!(
        hint.retry_after.is_none(),
        "zero Retry-After should be rejected"
    );
}

#[test]
fn rate_limit_exceeded_with_zero_delay_treated_as_unknown() {
    let err = map_api_error(ApiError::RateLimitExceeded {
        message: "retry now".to_string(),
        delay: Some(Duration::from_secs(0)),
        resets_at: None,
    });
    assert_eq!(
        err.retry_delay(),
        None,
        "zero delay should not populate retry_delay"
    );
    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert!(
        hint.retry_after.is_none(),
        "zero delay should not populate retry_after"
    );
}

#[test]
fn rate_limit_exceeded_with_past_resets_at_treated_as_unknown() {
    let past = (Utc::now() - chrono::Duration::hours(1)).timestamp();
    let err = map_api_error(ApiError::RateLimitExceeded {
        message: "rate limit reached".to_string(),
        delay: Some(Duration::from_secs(30)),
        resets_at: Some(past),
    });
    let hint = err.usage_limit_recovery_hint().expect("hint should be set");
    assert!(
        hint.resets_at.is_none(),
        "past resets_at should be rejected"
    );
    // The delay should still be preserved for the short retry loop.
    assert_eq!(err.retry_delay(), Some(Duration::from_secs(30)));
    assert_eq!(hint.retry_after, Some(Duration::from_secs(30)));
}
