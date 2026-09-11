use crate::TransportError;
use crate::error::ApiError;
use crate::rate_limits::parse_promo_message;
use crate::rate_limits::parse_rate_limit_for_limit;
use crate::rate_limits::parse_rate_limit_reached_type;
use base64::Engine;
use chrono::DateTime;
use chrono::Utc;
use codex_protocol::auth::PlanType;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::ConnectionFailedError;
use codex_protocol::error::RetryLimitReachedError;
use codex_protocol::error::UnexpectedResponseError;
use codex_protocol::error::UsageLimitReachedError;
use codex_protocol::error::UsageLimitRecoveryHint;
use codex_protocol::error::UsageLimitRecoveryKind;
use codex_protocol::protocol::MisalignmentErrorDetails;
use codex_protocol::protocol::RateLimitSnapshot;
use http::HeaderMap;
use http::HeaderValue;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

pub fn map_api_error(err: ApiError) -> CodexErr {
    match err {
        ApiError::ContextWindowExceeded => CodexErr::ContextWindowExceeded,
        ApiError::QuotaExceeded => CodexErr::QuotaExceeded,
        ApiError::UsageNotIncluded => CodexErr::UsageNotIncluded,
        ApiError::Retryable { message, delay } => {
            let error = CodexErr::Stream(message);
            match delay {
                Some(delay) => error.with_retry_delay(delay),
                None => error,
            }
        }
        ApiError::RateLimitExceeded {
            message,
            delay,
            resets_at,
        } => {
            let now = Utc::now();
            let mut hint = UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::TransientRateLimit);
            // Preserve the legacy `retry_delay` field for the existing short retry loop
            // alongside the coordinator recovery hint. Zero-duration advice is treated
            // as unknown so the coordinator does not schedule an immediate request.
            let valid_delay = delay.filter(|d| !d.is_zero());
            hint.retry_after = valid_delay;
            hint.resets_at = resets_at
                .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
                .filter(|dt| *dt > now);
            let mut err = CodexErr::new(CodexErrorDetails::RateLimitExceeded(message))
                .with_usage_limit_recovery_hint(hint);
            if let Some(d) = valid_delay {
                err = err.with_retry_delay(d);
            }
            err
        }
        ApiError::Stream(msg) => CodexErr::Stream(msg),
        ApiError::ServerOverloaded => CodexErr::ServerOverloaded,
        ApiError::Api { status, message } => {
            let user_message = api_error_user_message(status, &message);
            CodexErr::UnexpectedStatus(UnexpectedResponseError {
                status,
                body: message,
                user_message,
                url: None,
                cf_ray: None,
                request_id: None,
                identity_authorization_error: None,
                identity_error_code: None,
            })
        }
        ApiError::InvalidRequest { message } => CodexErr::InvalidRequest(message),
        ApiError::CyberPolicy { message } => {
            CodexErr::new(CodexErrorDetails::CyberPolicy { message })
        }
        ApiError::MisalignmentPolicyViolation {
            message,
            misalignment,
        } => CodexErr::new(CodexErrorDetails::MisalignmentPolicyViolation {
            message,
            misalignment,
        }),
        ApiError::Transport(transport) => match transport {
            TransportError::Http {
                status,
                url,
                headers,
                body,
            } => {
                let body_text = body.unwrap_or_default();

                if status == http::StatusCode::SERVICE_UNAVAILABLE
                    && let Ok(value) = serde_json::from_str::<serde_json::Value>(&body_text)
                    && matches!(
                        value
                            .get("error")
                            .and_then(|error| error.get("code"))
                            .and_then(serde_json::Value::as_str),
                        Some("server_is_overloaded" | "slow_down")
                    )
                {
                    return CodexErr::ServerOverloaded;
                }

                if (status == http::StatusCode::BAD_REQUEST
                    || status == http::StatusCode::FORBIDDEN)
                    && let Ok(parsed) = serde_json::from_str::<Value>(&body_text)
                    && let Some(error) = parsed.get("error")
                    && error.get("code").and_then(Value::as_str)
                        == Some(MISALIGNMENT_POLICY_VIOLATION_ERROR_CODE)
                {
                    let message = error
                        .get("message")
                        .and_then(Value::as_str)
                        .filter(|message| !message.trim().is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| {
                            MISALIGNMENT_POLICY_VIOLATION_FALLBACK_MESSAGE.to_string()
                        });
                    return CodexErr::new(CodexErrorDetails::MisalignmentPolicyViolation {
                        message,
                        misalignment: error.get("misalignment").cloned().and_then(|details| {
                            serde_json::from_value::<MisalignmentErrorDetails>(details).ok()
                        }),
                    });
                }

                if status == http::StatusCode::BAD_REQUEST {
                    if let Ok(parsed) = serde_json::from_str::<Value>(&body_text)
                        && let Some(error) = parsed.get("error")
                        && error.get("code").and_then(Value::as_str)
                            == Some(CYBER_POLICY_ERROR_CODE)
                    {
                        let message = error
                            .get("message")
                            .and_then(Value::as_str)
                            .filter(|message| !message.trim().is_empty())
                            .map(str::to_string)
                            .unwrap_or_else(|| CYBER_POLICY_FALLBACK_MESSAGE.to_string());
                        CodexErr::new(CodexErrorDetails::CyberPolicy { message })
                    } else if body_text
                        .contains("The image data you provided does not represent a valid image")
                    {
                        CodexErr::InvalidImageRequest()
                    } else {
                        CodexErr::InvalidRequest(body_text)
                    }
                } else if status == http::StatusCode::INTERNAL_SERVER_ERROR {
                    CodexErr::InternalServerError
                } else if status == http::StatusCode::TOO_MANY_REQUESTS {
                    if let Ok(err) = serde_json::from_str::<UsageErrorResponse>(&body_text) {
                        if err.error.error_type.as_deref() == Some("usage_limit_reached") {
                            let limit_id = extract_header(headers.as_ref(), ACTIVE_LIMIT_HEADER);
                            let promo_message = headers.as_ref().and_then(parse_promo_message);
                            let rate_limit_reached_type =
                                headers.as_ref().and_then(parse_rate_limit_reached_type);
                            let rate_limits = headers
                                .as_ref()
                                .and_then(|map| {
                                    parse_rate_limit_for_limit(map, limit_id.as_deref())
                                })
                                .map(|mut snapshot| {
                                    snapshot.rate_limit_reached_type = rate_limit_reached_type;
                                    snapshot
                                });
                            // Choose the latest future blocking reset from both the
                            // explicit body timestamp and all exhausted
                            // (`used_percent >= 100`) rate-limit windows. Past or
                            // invalid timestamps are rejected so the coordinator
                            // treats them as unknown rather than scheduling an
                            // immediate request.
                            let authoritative_reset =
                                authoritative_resets_at(err.error.resets_at, rate_limits.as_ref());
                            let mut hint =
                                UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::UsageWindow);
                            hint.http_status_code = Some(status.as_u16());
                            hint.resets_at = authoritative_reset;
                            hint.rate_limits = rate_limits.clone().map(Box::new);
                            hint.limit_id = limit_id;
                            return CodexErr::UsageLimitReached(UsageLimitReachedError {
                                plan_type: err.error.plan_type,
                                resets_at: authoritative_reset,
                                rate_limits: rate_limits.map(Box::new),
                                promo_message,
                                rate_limit_reached_type,
                            })
                            .with_usage_limit_recovery_hint(hint);
                        } else if err.error.error_type.as_deref() == Some("usage_not_included") {
                            // Entitlement/quota errors are intervention-only; no
                            // recovery hint is attached so the coordinator never
                            // schedules an automatic wait for them.
                            return CodexErr::UsageNotIncluded;
                        }
                    }

                    let retry_after = parse_retry_after_header(headers.as_ref());
                    let mut hint =
                        UsageLimitRecoveryHint::new(UsageLimitRecoveryKind::GenericTooManyRequests);
                    hint.http_status_code = Some(status.as_u16());
                    hint.retry_after = retry_after;
                    CodexErr::RetryLimit(RetryLimitReachedError {
                        status,
                        request_id: extract_request_tracking_id(headers.as_ref()),
                    })
                    .with_usage_limit_recovery_hint(hint)
                } else {
                    CodexErr::UnexpectedStatus(UnexpectedResponseError {
                        status,
                        user_message: api_error_user_message(status, &body_text),
                        body: body_text,
                        url,
                        cf_ray: extract_header(headers.as_ref(), CF_RAY_HEADER),
                        request_id: extract_request_id(headers.as_ref()),
                        identity_authorization_error: extract_header(
                            headers.as_ref(),
                            X_OPENAI_AUTHORIZATION_ERROR_HEADER,
                        ),
                        identity_error_code: extract_x_error_json_code(headers.as_ref()),
                    })
                }
            }
            TransportError::RetryLimit => CodexErr::RetryLimit(RetryLimitReachedError {
                status: http::StatusCode::INTERNAL_SERVER_ERROR,
                request_id: None,
            }),
            TransportError::Timeout => CodexErr::RequestTimeout,
            TransportError::Connection(source) => {
                CodexErr::ConnectionFailed(ConnectionFailedError { source })
            }
            TransportError::Network(msg) | TransportError::Build(msg) => CodexErr::Stream(msg),
        },
        ApiError::RateLimit(msg) => CodexErr::Stream(msg),
    }
}

const ACTIVE_LIMIT_HEADER: &str = "x-codex-active-limit";
const REQUEST_ID_HEADER: &str = "x-request-id";
const OAI_REQUEST_ID_HEADER: &str = "x-oai-request-id";
const CF_RAY_HEADER: &str = "cf-ray";
const X_OPENAI_AUTHORIZATION_ERROR_HEADER: &str = "x-openai-authorization-error";
const X_ERROR_JSON_HEADER: &str = "x-error-json";
const CYBER_POLICY_ERROR_CODE: &str = "cyber_policy";
const CYBER_POLICY_FALLBACK_MESSAGE: &str =
    "This request has been flagged for possible cybersecurity risk.";
const MISALIGNMENT_POLICY_VIOLATION_ERROR_CODE: &str = "misalignment_policy_violation";
const MISALIGNMENT_POLICY_VIOLATION_FALLBACK_MESSAGE: &str =
    "This request was blocked due to a misalignment policy violation.";
const CLOUDFLARE_BLOCKED_MESSAGE: &str =
    "Access blocked by Cloudflare. This usually happens when connecting from a restricted region";

#[cfg(test)]
#[path = "api_bridge_tests.rs"]
mod tests;

fn extract_request_tracking_id(headers: Option<&HeaderMap>) -> Option<String> {
    extract_request_id(headers).or_else(|| extract_header(headers, CF_RAY_HEADER))
}

fn api_error_user_message(status: http::StatusCode, body: &str) -> Option<String> {
    if status == http::StatusCode::FORBIDDEN
        && body.contains("Cloudflare")
        && body.contains("blocked")
    {
        Some(format!("{CLOUDFLARE_BLOCKED_MESSAGE} (status {status})"))
    } else {
        None
    }
}

fn extract_request_id(headers: Option<&HeaderMap>) -> Option<String> {
    extract_header(headers, REQUEST_ID_HEADER)
        .or_else(|| extract_header(headers, OAI_REQUEST_ID_HEADER))
}

fn extract_header(headers: Option<&HeaderMap>, name: &str) -> Option<String> {
    headers.and_then(|map| {
        map.get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    })
}

/// Parse a standard `Retry-After` header into a [`Duration`].
///
/// Supports both delta-seconds (`Retry-After: 120`) and HTTP-date
/// (`Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`) forms. Returns `None` for
/// malformed or past dates so callers treat them as unknown rather than
/// immediately hammering the endpoint.
fn parse_retry_after_header(headers: Option<&HeaderMap>) -> Option<Duration> {
    let value = headers?.get("retry-after")?;
    parse_retry_after_value(value)
}

fn parse_retry_after_value(value: &HeaderValue) -> Option<Duration> {
    let text = value.to_str().ok()?;
    // Try delta-seconds first (most common). A zero-duration `Retry-After: 0`
    // is treated as unknown so the coordinator does not schedule an immediate
    // request based on meaningless advice.
    if let Ok(seconds) = text.trim().parse::<u64>() {
        return (seconds > 0).then_some(Duration::from_secs(seconds));
    }
    // Fall back to HTTP-date per RFC 7231.
    let date = chrono::DateTime::parse_from_rfc2822(text.trim()).ok()?;
    let now = Utc::now();
    if date > now {
        let duration = (date.with_timezone(&Utc) - now).to_std().ok()?;
        (!duration.is_zero()).then_some(duration)
    } else {
        None
    }
}

/// Choose the latest future blocking reset from the explicit body `resets_at`
/// timestamp and all exhausted (`used_percent >= 100`) rate-limit windows.
///
/// Past, invalid, or absent timestamps are rejected so the coordinator treats
/// them as unknown rather than scheduling an immediate request.
fn authoritative_resets_at(
    body_resets_at: Option<i64>,
    rate_limits: Option<&RateLimitSnapshot>,
) -> Option<DateTime<Utc>> {
    let now = Utc::now();
    let mut candidates: Vec<DateTime<Utc>> = Vec::new();

    if let Some(seconds) = body_resets_at
        && let Some(dt) = DateTime::<Utc>::from_timestamp(seconds, 0)
        && dt > now
    {
        candidates.push(dt);
    }

    if let Some(snapshot) = rate_limits {
        for window in snapshot
            .primary
            .as_ref()
            .into_iter()
            .chain(snapshot.secondary.as_ref())
        {
            if window.used_percent >= 100.0
                && let Some(seconds) = window.resets_at
                && let Some(dt) = DateTime::<Utc>::from_timestamp(seconds, 0)
                && dt > now
            {
                candidates.push(dt);
            }
        }
    }

    candidates.into_iter().max()
}

fn extract_x_error_json_code(headers: Option<&HeaderMap>) -> Option<String> {
    let encoded = extract_header(headers, X_ERROR_JSON_HEADER)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let parsed = serde_json::from_slice::<Value>(&decoded).ok()?;
    parsed
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[derive(Debug, Deserialize)]
struct UsageErrorResponse {
    error: UsageErrorBody,
}

#[derive(Debug, Deserialize)]
struct UsageErrorBody {
    #[serde(rename = "type")]
    error_type: Option<String>,
    plan_type: Option<PlanType>,
    resets_at: Option<i64>,
}
