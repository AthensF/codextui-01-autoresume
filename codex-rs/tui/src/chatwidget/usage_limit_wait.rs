//! Usage-limit wait state, countdown rendering, and live controls.
//!
//! Consumes `thread/usageLimit/updated` notifications and exposes a compact
//! status surface, `/usage-limit` popup, and action dispatch for retry-now,
//! auto-resume toggle, and cancel.

use super::*;

use codex_app_server_protocol::UsageLimitAction;
use codex_app_server_protocol::UsageLimitManualWaitReason;
use codex_app_server_protocol::UsageLimitSnapshot;
use codex_app_server_protocol::UsageLimitWaitPhase;

const USAGE_LIMIT_MENU_VIEW_ID: &str = "usage-limit-menu";

/// State for the active usage-limit wait, if any.
#[derive(Debug, Default, Clone)]
pub(super) struct UsageLimitWaitState {
    /// Current coordinator snapshot, or `None` when no wait is active.
    snapshot: Option<UsageLimitSnapshot>,
    /// Last countdown label rendered, to avoid redundant redraws.
    last_countdown_label: Option<String>,
}

impl UsageLimitWaitState {
    /// Returns `true` when the tree is sealed and new prompts should not
    /// auto-send.
    pub(super) fn is_tree_closed(&self) -> bool {
        self.snapshot.as_ref().is_some_and(|s| {
            !matches!(
                s.phase,
                UsageLimitWaitPhase::Resuming | UsageLimitWaitPhase::Cancelled
            )
        })
    }

    /// Returns `true` when the wait is in manual-wait or attention-required
    /// and user action is needed.
    pub(super) fn needs_user_action(&self) -> bool {
        self.snapshot.as_ref().is_some_and(|s| {
            matches!(
                s.phase,
                UsageLimitWaitPhase::ManualWait | UsageLimitWaitPhase::AttentionRequired
            )
        })
    }

    /// Returns the current snapshot, if any.
    pub(super) fn snapshot(&self) -> Option<&UsageLimitSnapshot> {
        self.snapshot.as_ref()
    }

    /// Update the snapshot from a notification or read response.
    pub(super) fn set_snapshot(&mut self, snapshot: Option<UsageLimitSnapshot>) {
        self.snapshot = snapshot;
        self.last_countdown_label = None;
    }

    /// Compute the countdown label for display, or `None` if no reset is
    /// known. Returns the label and whether it changed since the last call.
    pub(super) fn countdown_label(&mut self, now: i64) -> Option<(String, bool)> {
        let snapshot = self.snapshot.as_ref()?;
        let reset_at = snapshot.reset_at?;
        let remaining = reset_at - now;
        let label = if remaining <= 0 {
            "reset due".to_string()
        } else {
            format_remaining(remaining)
        };
        let changed = self.last_countdown_label.as_deref() != Some(&label);
        if changed {
            self.last_countdown_label = Some(label.clone());
        }
        Some((label, changed))
    }
}

fn format_remaining(seconds: i64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    }
}

/// Human-readable phase label for status display.
pub(super) fn phase_label(phase: &UsageLimitWaitPhase) -> &'static str {
    match phase {
        UsageLimitWaitPhase::Queued => "Queued",
        UsageLimitWaitPhase::Suspending => "Suspending",
        UsageLimitWaitPhase::Suspended => "Waiting for reset",
        UsageLimitWaitPhase::Revalidating => "Checking availability",
        UsageLimitWaitPhase::Resuming => "Resuming",
        UsageLimitWaitPhase::ManualWait => "Manual retry needed",
        UsageLimitWaitPhase::AttentionRequired => "Attention required",
        UsageLimitWaitPhase::Cancelled => "Cancelled",
    }
}

/// Human-readable manual-wait reason label.
pub(super) fn manual_wait_reason_label(reason: &UsageLimitManualWaitReason) -> &'static str {
    match reason {
        UsageLimitManualWaitReason::AutoResumeDisabled => "auto-resume is disabled",
        UsageLimitManualWaitReason::AutoResumeLimitReached => "auto-resume limit reached",
        UsageLimitManualWaitReason::AvailabilityCheckLimitReached => {
            "availability check limit reached"
        }
        UsageLimitManualWaitReason::WaitCeilingReached => "wait ceiling reached",
    }
}

/// Status header for the bottom status surface.
pub(super) fn status_header(snapshot: &UsageLimitSnapshot) -> String {
    match snapshot.phase {
        UsageLimitWaitPhase::ManualWait => "Work paused — manual retry".to_string(),
        UsageLimitWaitPhase::AttentionRequired => "Work paused — attention required".to_string(),
        _ => "Work temporarily unavailable".to_string(),
    }
}

/// Compact one-line status detail for the bottom status surface.
pub(super) fn status_detail(snapshot: &UsageLimitSnapshot, now: i64) -> Option<String> {
    let phase = phase_label(&snapshot.phase);
    let mut parts = vec![phase.to_string()];
    if let Some(reset_at) = snapshot.reset_at {
        let remaining = reset_at - now;
        if remaining > 0 {
            parts.push(format!("reset in {}", format_remaining(remaining)));
        } else {
            parts.push("reset due".to_string());
        }
    }
    if snapshot.auto_resume_enabled && snapshot.phase != UsageLimitWaitPhase::ManualWait {
        parts.push("auto-resume on".to_string());
    }
    Some(parts.join(" · "))
}

impl ChatWidget {
    /// Handle a `thread/usageLimit/updated` notification.
    pub(crate) fn handle_usage_limit_updated(
        &mut self,
        notification: codex_app_server_protocol::ThreadUsageLimitUpdatedNotification,
    ) {
        self.set_usage_limit_snapshot(notification.snapshot);
    }

    /// Apply a usage-limit status read result from the app layer.
    pub(crate) fn set_usage_limit_snapshot(&mut self, snapshot: Option<UsageLimitSnapshot>) {
        let was_closed = self.usage_limit_wait.is_tree_closed();
        self.usage_limit_wait.set_snapshot(snapshot);
        self.refresh_usage_limit_status_indicator();
        if was_closed && !self.usage_limit_wait.is_tree_closed() {
            self.maybe_send_next_queued_input();
        }
        self.request_redraw();
    }

    /// Update the bottom status indicator to reflect the current usage-limit
    /// wait state, or restore the previous status when no wait is active.
    pub(super) fn refresh_usage_limit_status_indicator(&mut self) {
        let now = chrono::Utc::now().timestamp();
        if let Some(snapshot) = self.usage_limit_wait.snapshot() {
            let header = status_header(snapshot);
            let details = status_detail(snapshot, now);
            self.set_status(
                header,
                details,
                StatusDetailsCapitalization::Preserve,
                STATUS_DETAILS_DEFAULT_MAX_LINES,
            );
        } else {
            self.restore_reasoning_status_header();
        }
    }

    /// Read the current usage-limit status from the app-server after thread
    /// start/resume/switch. Delegates to the app layer via `AppEvent`.
    pub(crate) fn refresh_usage_limit_status(&mut self) {
        let Some(thread_id) = self.thread_id() else {
            return;
        };
        self.app_event_tx
            .send(AppEvent::RefreshUsageLimitStatus { thread_id });
    }

    /// Open the `/usage-limit` popup menu.
    pub(super) fn open_usage_limit_menu(&mut self) {
        let Some(snapshot) = self.usage_limit_wait.snapshot() else {
            self.add_error_message("No active usage-limit wait.".to_string());
            return;
        };
        let items = usage_limit_menu_items(snapshot);
        self.bottom_pane.show_selection_view(SelectionViewParams {
            view_id: Some(USAGE_LIMIT_MENU_VIEW_ID),
            title: Some("Usage Limit".to_string()),
            items,
            ..Default::default()
        });
        self.request_redraw();
    }
}

fn usage_limit_menu_items(snapshot: &UsageLimitSnapshot) -> Vec<SelectionItem> {
    let generation = snapshot.generation;
    let thread_id = ThreadId::from_string(&snapshot.trigger_thread_id).unwrap_or_default();
    let auto_resume_enabled = snapshot.auto_resume_enabled;
    let mut items = Vec::new();
    items.push(SelectionItem {
        name: "Retry now".to_string(),
        description: Some("Trigger a revalidation probe immediately.".to_string()),
        dismiss_on_select: true,
        actions: vec![Box::new(move |tx| {
            tx.send(AppEvent::SendUsageLimitAction {
                thread_id,
                generation,
                action: UsageLimitAction::RetryNow,
            });
        })],
        ..Default::default()
    });
    let toggle_thread_id = thread_id;
    items.push(SelectionItem {
        name: if auto_resume_enabled {
            "Disable auto-resume".to_string()
        } else {
            "Enable auto-resume".to_string()
        },
        description: Some("Toggle automatic resume when capacity returns.".to_string()),
        dismiss_on_select: true,
        actions: vec![Box::new(move |tx| {
            tx.send(AppEvent::SendUsageLimitAction {
                thread_id: toggle_thread_id,
                generation,
                action: UsageLimitAction::SetAutoResume {
                    enabled: !auto_resume_enabled,
                },
            });
        })],
        ..Default::default()
    });
    let cancel_thread_id = thread_id;
    items.push(SelectionItem {
        name: "Cancel wait".to_string(),
        description: Some("Cancel the coordinated tree wait.".to_string()),
        dismiss_on_select: true,
        actions: vec![Box::new(move |tx| {
            tx.send(AppEvent::SendUsageLimitAction {
                thread_id: cancel_thread_id,
                generation,
                action: UsageLimitAction::Cancel,
            });
        })],
        ..Default::default()
    });
    items
}
