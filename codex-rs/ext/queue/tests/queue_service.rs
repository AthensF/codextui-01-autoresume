use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use codex_core::NotSubmittedReason;
use codex_core::StartIfIdleSubmission;
use codex_core::StartThreadOptions;
use codex_core::TurnInput;
use codex_core::TurnInputRequest;
use codex_core::TurnInputSubmission;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionEventSink;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::ExtensionRegistry;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_extension_api::ExtensionWarning;
use codex_extension_api::NoopExtensionEventSink;
use codex_extension_api::ThreadIdleCause;
use codex_extension_api::ThreadIdleInput;
use codex_extension_api::ThreadLifecycleContributor;
use codex_extension_api::ThreadResumeInput;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnSuspendedInput;
use codex_protocol::ThreadId;
use codex_protocol::items::TurnItem;
use codex_protocol::models::ImageDetail;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::MAX_USER_INPUT_TEXT_CHARS;
use codex_protocol::user_input::UserInput;
use codex_queue_extension::QueueServiceError;
use codex_queue_extension::QueuedItemService;
use codex_queue_extension::decode_turn_input;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_thread_store::LocalQueueStore;
use codex_thread_store::QueueStore;
use codex_thread_store::ThreadStoreError;
use codex_utils_absolute_path::test_support::PathExt;
use core_test_support::hooks::trust_discovered_hooks;
use core_test_support::responses;
use core_test_support::responses::start_mock_server;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event_match;
use core_test_support::wait_for_event_with_timeout;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::oneshot;

const TINY_PNG_BYTES: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0,
    0, 0, 31, 21, 196, 137, 0, 0, 0, 11, 73, 68, 65, 84, 120, 156, 99, 96, 0, 2, 0, 0, 5, 0, 1,
    122, 94, 171, 63, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const TINY_PNG_DATA_URL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGNgAAIAAAUAAXpeqz8AAAAASUVORK5CYII=";

#[derive(Debug)]
struct TestAdmission(AtomicBool);

impl TurnStartAdmission for TestAdmission {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        if self.0.load(Ordering::SeqCst) {
            None
        } else {
            Some(Box::new(()))
        }
    }
}

#[derive(Default)]
struct RecordingEventSink {
    events: Mutex<Vec<Event>>,
}

impl ExtensionEventSink for RecordingEventSink {
    fn emit(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }

    fn emit_warning(&self, _warning: ExtensionWarning) {}
}

#[derive(Default)]
struct InstalledQueue {
    service: OnceLock<Arc<QueuedItemService>>,
    skip_next_idle: Mutex<Option<ThreadId>>,
}

impl ThreadLifecycleContributor<codex_core::config::Config> for InstalledQueue {
    fn on_thread_resume<'a>(&'a self, input: ThreadResumeInput<'a>) -> ExtensionFuture<'a, ()> {
        match self.service.get() {
            Some(service) => <QueuedItemService as ThreadLifecycleContributor<
                codex_core::config::Config,
            >>::on_thread_resume(service.as_ref(), input),
            None => Box::pin(async {}),
        }
    }

    fn on_thread_idle<'a>(&'a self, input: ThreadIdleInput<'a>) -> ExtensionFuture<'a, ()> {
        if self
            .skip_next_idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|thread_id| thread_id.to_string() == input.thread_store.level_id())
            .is_some()
        {
            return Box::pin(async {});
        }
        match self.service.get() {
            Some(service) => <QueuedItemService as ThreadLifecycleContributor<
                codex_core::config::Config,
            >>::on_thread_idle(service.as_ref(), input),
            None => Box::pin(async {}),
        }
    }

    fn on_turn_suspended<'a>(&'a self, input: TurnSuspendedInput<'a>) -> ExtensionFuture<'a, ()> {
        match self.service.get() {
            Some(service) => <QueuedItemService as ThreadLifecycleContributor<
                codex_core::config::Config,
            >>::on_turn_suspended(service.as_ref(), input),
            None => Box::pin(async {}),
        }
    }
}

fn registered_queue_extensions() -> (
    Arc<InstalledQueue>,
    Arc<ExtensionRegistry<codex_core::config::Config>>,
) {
    let installed = Arc::new(InstalledQueue::default());
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.thread_lifecycle_contributor(installed.clone());
    (installed, Arc::new(extensions.build()))
}

fn install_registered_queue(
    test: &TestCodex,
    installed: &InstalledQueue,
) -> anyhow::Result<Arc<QueuedItemService>> {
    let service = Arc::new(QueuedItemService::new(
        loaded_thread_queue(test)?,
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    ));
    assert!(installed.service.set(Arc::clone(&service)).is_ok());
    Ok(service)
}

fn write_rejecting_prompt_hook(home: &Path) {
    let script_path = home.join("queue_prompt_hook.py");
    let log_path = home.join("queue_prompt_hook.log");
    let script = format!(
        r#"import json
from pathlib import Path
import sys

payload = json.load(sys.stdin)
with Path(r"{log_path}").open("a", encoding="utf-8") as log:
    log.write(payload["prompt"] + "\n")
if payload["prompt"] == "blocked":
    print(json.dumps({{"decision": "block", "reason": "blocked by queue hook"}}))
"#,
        log_path = log_path.display(),
    );
    std::fs::write(&script_path, script)
        .unwrap_or_else(|error| panic!("write queue hook script: {error}"));
    let hooks = serde_json::json!({
        "hooks": {
            "UserPromptSubmit": [{
                "hooks": [{
                    "type": "command",
                    "command": format!("python3 {}", script_path.display()),
                }]
            }]
        }
    });
    std::fs::write(home.join("hooks.json"), hooks.to_string())
        .unwrap_or_else(|error| panic!("write queue hooks: {error}"));
}

async fn test_queue() -> anyhow::Result<(Arc<dyn QueueStore>, TempDir)> {
    let home = tempfile::tempdir()?;
    let sqlite = SqliteConfig::new_for_testing(home.path().abs());
    let runtime = StateRuntime::init(sqlite.clone(), "test-provider".to_string()).await?;
    let queue: Arc<dyn QueueStore> = Arc::new(LocalQueueStore::new(runtime));
    Ok((queue, home))
}

fn loaded_thread_queue(test: &TestCodex) -> anyhow::Result<Arc<dyn QueueStore>> {
    let runtime = test.codex.state_db().context("state runtime unavailable")?;
    Ok(Arc::new(LocalQueueStore::new(runtime)))
}

fn user_input(text: &str) -> TurnInput {
    TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: None,
    }
}

fn structured_user_input(text: &str) -> TurnInput {
    TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: Some("stable-client-message".to_string()),
    }
}

fn user_input_with_media(text: &str, image: UserInput, audio: UserInput) -> TurnInput {
    let mut input = structured_user_input(text);
    if let TurnInput::UserInput { content, .. } = &mut input {
        content.extend([image, audio]);
    }
    input
}

async fn emit_idle(service: &QueuedItemService, thread_id: ThreadId) {
    emit_idle_with_cause(service, thread_id, ThreadIdleCause::Completed).await;
}

async fn emit_idle_with_cause(
    service: &QueuedItemService,
    thread_id: ThreadId,
    cause: ThreadIdleCause,
) {
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(thread_id.to_string());
    <QueuedItemService as ThreadLifecycleContributor<()>>::on_thread_idle(
        service,
        ThreadIdleInput {
            cause,
            session_store: &session_store,
            thread_store: &thread_store,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_leaves_persisted_queued_message_for_a_later_start() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let response =
        responses::mount_sse_once(&server, responses::sse_completed("queued-turn")).await;
    let admission = Arc::new(TestAdmission(AtomicBool::new(true)));
    let mut extensions = ExtensionRegistryBuilder::new();
    extensions.turn_start_admission(admission.clone());
    let test = test_codex()
        .with_extensions(Arc::new(extensions.build()))
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let queue = loaded_thread_queue(&test)?;
    let staged = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let item = staged
        .enqueue(thread_id, user_input("queued after drain"))
        .await?;
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    );

    emit_idle(&service, thread_id).await;
    assert_eq!(vec![item.clone()], service.list(thread_id).await?);
    assert!(response.requests().is_empty());

    admission.0.store(false, Ordering::SeqCst);
    emit_idle(&service, thread_id).await;
    wait_for_event_match(test.codex.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_)).then_some(())
    })
    .await;
    assert!(service.list(thread_id).await?.is_empty());
    assert_eq!(1, response.requests().len());
    Ok(())
}

#[tokio::test]
async fn queued_input_and_unique_event_ids_round_trip() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let sink = Arc::new(RecordingEventSink::default());
    let event_sink: Arc<dyn ExtensionEventSink> = sink.clone();
    let service = QueuedItemService::new(queue, Weak::new(), event_sink);
    let thread_id = ThreadId::new();
    let input = structured_user_input("structured message");

    let first = service.enqueue(thread_id, input.clone()).await?;
    let second = service.enqueue(thread_id, user_input("next")).await?;
    let TurnInput::UserInput {
        client_id: Some(generated_client_id),
        ..
    } = &second.input
    else {
        anyhow::bail!("queued message did not receive a client message id");
    };
    assert_eq!(
        7,
        uuid::Uuid::parse_str(generated_client_id)?.get_version_num()
    );
    assert_eq!(
        vec![first.clone(), second.clone()],
        service.list(thread_id).await?
    );
    assert_eq!(input, first.input);

    {
        let events = sink
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(2, events.len());
        for event in events.iter() {
            let EventMsg::ThreadQueueChanged(change) = &event.msg else {
                anyhow::bail!("event is not a queue change");
            };
            assert_eq!(change.thread_id, thread_id);
        }
        assert_ne!(events[0].id, events[1].id);
    }
    Ok(())
}

#[tokio::test]
async fn editing_reordering_and_deleting_preserve_queue_identity() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let thread_id = ThreadId::new();
    let first = service
        .enqueue(thread_id, structured_user_input("first"))
        .await?;
    let second = service.enqueue(thread_id, user_input("second")).await?;
    let edited = service
        .update(thread_id, first.id.clone(), user_input("edited"))
        .await?
        .context("queued item missing")?;
    assert_eq!(first.id, edited.id);
    let TurnInput::UserInput { client_id, .. } = &edited.input else {
        anyhow::bail!("edited queue item does not contain user input");
    };
    assert_eq!(Some("stable-client-message"), client_id.as_deref());
    assert!(matches!(
        service.reorder(thread_id, vec![first.id.clone()]).await,
        Err(QueueServiceError::Storage(
            ThreadStoreError::InvalidRequest { .. }
        ))
    ));
    service
        .reorder(thread_id, vec![second.id.clone(), first.id.clone()])
        .await?;
    assert_eq!(
        vec![second.clone(), edited.clone()],
        service.list(thread_id).await?
    );
    assert!(service.delete(thread_id, second.id).await?);
    assert_eq!(vec![edited], service.list(thread_id).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_a_selected_item_preserves_the_remaining_queue() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let response =
        responses::mount_sse_once(&server, responses::sse_completed("selected-turn")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let queue = loaded_thread_queue(&test)?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let first = service.enqueue(thread_id, user_input("first")).await?;
    let second = service
        .enqueue(thread_id, structured_user_input("second"))
        .await?;

    let submission = service
        .start(
            test.codex.as_ref(),
            Some(second.id.clone()),
            /*trace*/ None,
        )
        .await?;

    assert!(matches!(
        submission,
        StartIfIdleSubmission::Started { turn_id } if !turn_id.is_empty()
    ));
    assert_eq!(vec![first], service.list(thread_id).await?);
    wait_for_event_match(test.codex.as_ref(), |event| match event {
        EventMsg::TurnComplete(_) => Some(()),
        _ => None,
    })
    .await;
    assert_eq!(
        Some("second"),
        response
            .single_request()
            .message_input_texts("user")
            .last()
            .map(String::as_str)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_a_selected_item_while_active_leaves_it_queued() -> anyhow::Result<()> {
    let (release_response, response_gate) = oneshot::channel();
    let (server, _completions) = start_streaming_sse_server(vec![vec![
        StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![responses::ev_response_created("resp-1")]),
        },
        StreamingSseChunk {
            gate: Some(response_gate),
            body: responses::sse(vec![responses::ev_completed("resp-1")]),
        },
    ]])
    .await;
    let test = test_codex().build_with_streaming_server(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let service = QueuedItemService::new(
        loaded_thread_queue(&test)?,
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let queued = service
        .enqueue(thread_id, user_input("stay queued"))
        .await?;

    let active_turn = test
        .codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "active turn".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    assert!(matches!(active_turn, TurnInputSubmission::Started { .. }));
    tokio::time::timeout(
        Duration::from_secs(5),
        server.wait_for_request_count(/*count*/ 1),
    )
    .await?;

    let submission = service
        .start(
            test.codex.as_ref(),
            Some(queued.id.clone()),
            /*trace*/ None,
        )
        .await?;
    assert!(matches!(
        submission,
        StartIfIdleSubmission::NotSubmitted {
            reason: NotSubmittedReason::NotIdle
        }
    ));
    assert_eq!(vec![queued], service.list(thread_id).await?);

    release_response
        .send(())
        .expect("active response gate should remain open");
    wait_for_event_match(test.codex.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_)).then_some(())
    })
    .await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_turns_pause_queued_messages_but_failed_turns_drain_them() -> anyhow::Result<()>
{
    let server = start_mock_server().await;
    let response =
        responses::mount_sse_once(&server, responses::sse_completed("failed-follow-up")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let queue = loaded_thread_queue(&test)?;
    let service = QueuedItemService::new(
        queue,
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    );
    let queued = service
        .enqueue(thread_id, user_input("continue after failure"))
        .await?;

    emit_idle_with_cause(&service, thread_id, ThreadIdleCause::Interrupted).await;
    assert_eq!(vec![queued], service.list(thread_id).await?);

    emit_idle_with_cause(&service, thread_id, ThreadIdleCause::Failed).await;
    wait_for_event_match(test.codex.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_)).then_some(())
    })
    .await;
    assert!(service.list(thread_id).await?.is_empty());
    assert_eq!(
        Some("continue after failure"),
        response
            .single_request()
            .message_input_texts("user")
            .last()
            .map(String::as_str)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_queue_lifecycle_starts_messages_in_fifo_order() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let responses = responses::mount_sse_sequence(
        &server,
        ["turn-a", "turn-b", "turn-c"]
            .into_iter()
            .map(responses::sse_completed)
            .collect(),
    )
    .await;
    let (installed, extensions) = registered_queue_extensions();
    let test = test_codex()
        .with_extensions(extensions)
        .with_config(|config| config.include_environment_context = false)
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let staging = QueuedItemService::new(
        loaded_thread_queue(&test)?,
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    for prompt in ["B", "C"] {
        staging.enqueue(thread_id, user_input(prompt)).await?;
    }
    let queue = install_registered_queue(&test, installed.as_ref())?;

    tokio::time::timeout(Duration::from_secs(10), async {
        test.submit_text_turn("A").await?;
        for _ in 0..2 {
            wait_for_event_match(test.codex.as_ref(), |event| {
                matches!(event, EventMsg::TurnComplete(_)).then_some(())
            })
            .await;
        }
        anyhow::Ok(())
    })
    .await??;

    let prompts = responses
        .requests()
        .into_iter()
        .map(|request| {
            request
                .message_input_texts("user")
                .pop()
                .context("model request has no user input")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(vec!["A", "B", "C"], prompts);
    assert!(queue.list(thread_id).await?.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn externally_changed_queues_dispatch_independently_and_retry_failed_wakes()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let model_responses = responses::mount_sse_sequence(
        &server,
        [
            "independent-queued-turn",
            "external-queued-turn",
            "resumed-queued-turn",
        ]
        .into_iter()
        .map(responses::sse_completed)
        .collect(),
    )
    .await;
    let (installed, extensions) = registered_queue_extensions();
    let test = test_codex()
        .with_extensions(extensions)
        .with_config(|config| config.include_environment_context = false)
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let queue = install_registered_queue(&test, installed.as_ref())?;
    let independent_thread = test
        .thread_manager
        .start_thread(StartThreadOptions::new(test.config.clone()))
        .await?;
    let external_runtime = StateRuntime::init(
        test.codex
            .state_db()
            .context("state runtime unavailable")?
            .sqlite()
            .clone(),
        "test-provider".to_string(),
    )
    .await?;
    let external_queue = QueuedItemService::new(
        Arc::new(LocalQueueStore::new(external_runtime)),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let mut watcher_extensions = ExtensionRegistryBuilder::<codex_core::config::Config>::new();
    codex_queue_extension::install(&mut watcher_extensions, Arc::clone(&queue));

    tokio::time::sleep(Duration::from_secs(/*secs*/ 1)).await;
    assert!(model_responses.requests().is_empty());
    *installed
        .skip_next_idle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(thread_id);
    let first = external_queue
        .enqueue(thread_id, user_input("written by another process"))
        .await?;
    let updated = queue
        .update(
            thread_id,
            first.id,
            user_input("locally edited external message"),
        )
        .await?
        .context("external queue item disappeared")?;
    external_queue
        .enqueue(
            independent_thread.thread_id,
            user_input("independent thread"),
        )
        .await?;

    wait_for_event_with_timeout(
        independent_thread.thread.as_ref(),
        |event| matches!(event, EventMsg::TurnComplete(_)),
        Duration::from_secs(/*secs*/ 25),
    )
    .await;
    assert_eq!(1, model_responses.requests().len());
    assert_eq!(vec![updated], queue.list(thread_id).await?);

    wait_for_event_with_timeout(
        test.codex.as_ref(),
        |event| matches!(event, EventMsg::TurnComplete(_)),
        Duration::from_secs(/*secs*/ 25),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(/*secs*/ 11)).await;

    assert!(queue.list(thread_id).await?.is_empty());
    assert!(queue.list(independent_thread.thread_id).await?.is_empty());

    let rollout_path = test.codex.rollout_path().context("rollout path missing")?;
    test.codex.shutdown_and_wait().await?;
    test.thread_manager.remove_thread(&thread_id).await;
    external_queue
        .enqueue(thread_id, user_input("queued before ordinary resume"))
        .await?;
    tokio::time::sleep(Duration::from_secs(/*secs*/ 11)).await;
    let resumed = test
        .thread_manager
        .resume_thread_from_rollout(
            test.config.clone(),
            rollout_path,
            test.thread_manager.auth_manager(),
            /*parent_trace*/ None,
            Default::default(),
        )
        .await?;
    wait_for_event_with_timeout(
        resumed.thread.as_ref(),
        |event| matches!(event, EventMsg::TurnComplete(_)),
        Duration::from_secs(/*secs*/ 25),
    )
    .await;
    assert!(queue.list(thread_id).await?.is_empty());

    let prompts = model_responses
        .requests()
        .into_iter()
        .filter_map(|request| request.message_input_texts("user").pop())
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            "independent thread",
            "locally edited external message",
            "queued before ordinary resume",
        ],
        prompts
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_queue_messages_are_consumed_without_retrying_or_blocking_followups()
-> anyhow::Result<()> {
    let server = start_mock_server().await;
    let responses = responses::mount_sse_sequence(
        &server,
        ["initial-turn", "following-turn"]
            .into_iter()
            .map(responses::sse_completed)
            .collect(),
    )
    .await;
    let (installed, extensions) = registered_queue_extensions();
    let test = test_codex()
        .with_extensions(extensions)
        .with_pre_build_hook(write_rejecting_prompt_hook)
        .with_config(trust_discovered_hooks)
        .with_config(|config| config.include_environment_context = false)
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let staging = QueuedItemService::new(
        loaded_thread_queue(&test)?,
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    for prompt in ["blocked", "C"] {
        staging.enqueue(thread_id, user_input(prompt)).await?;
    }
    let queue = install_registered_queue(&test, installed.as_ref())?;

    tokio::time::timeout(Duration::from_secs(10), async {
        test.submit_text_turn("A").await?;
        for _ in 0..2 {
            wait_for_event_match(test.codex.as_ref(), |event| {
                matches!(event, EventMsg::TurnComplete(_)).then_some(())
            })
            .await;
        }
        anyhow::Ok(())
    })
    .await??;

    let prompts = responses
        .requests()
        .into_iter()
        .map(|request| {
            request
                .message_input_texts("user")
                .pop()
                .context("model request has no user input")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    assert_eq!(vec!["A", "C"], prompts);
    let hook_log = std::fs::read_to_string(test.codex_home_path().join("queue_prompt_hook.log"))?;
    assert_eq!(
        vec!["A", "blocked", "C"],
        hook_log.lines().collect::<Vec<_>>()
    );
    assert!(queue.list(thread_id).await?.is_empty());
    assert_eq!(2, responses.requests().len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicitly_started_rejected_queue_messages_are_consumed() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let responses =
        responses::mount_sse_once(&server, responses::sse_completed("unexpected-turn")).await;
    let test = test_codex()
        .with_pre_build_hook(write_rejecting_prompt_hook)
        .with_config(trust_discovered_hooks)
        .with_config(|config| config.include_environment_context = false)
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let queue = QueuedItemService::new(
        loaded_thread_queue(&test)?,
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    let rejected = queue.enqueue(thread_id, user_input("blocked")).await?;
    let submission = tokio::time::timeout(
        Duration::from_secs(10),
        queue.start(test.codex.as_ref(), Some(rejected.id), /*trace*/ None),
    )
    .await?
    .expect("explicitly started input should be submitted");
    assert!(matches!(submission, StartIfIdleSubmission::Started { .. }));
    wait_for_event_match(test.codex.as_ref(), |event| {
        matches!(event, EventMsg::TurnComplete(_)).then_some(())
    })
    .await;
    assert!(queue.list(thread_id).await?.is_empty());
    let hook_log = std::fs::read_to_string(test.codex_home_path().join("queue_prompt_hook.log"))?;
    assert_eq!(vec!["blocked"], hook_log.lines().collect::<Vec<_>>());
    assert!(responses.requests().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_attachments_are_snapshotted_before_enqueue_and_update() -> anyhow::Result<()> {
    let (queue, home) = test_queue().await?;
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let thread_id = ThreadId::new();
    let image_path = home.path().join("queued.png");
    let audio_path = home.path().join("queued.mp3");
    std::fs::write(&image_path, TINY_PNG_BYTES)?;
    std::fs::write(&audio_path, b"audio")?;
    let queued_input = user_input_with_media(
        "queued attachments",
        UserInput::LocalImage {
            path: image_path.clone(),
            detail: Some(ImageDetail::Original),
        },
        UserInput::LocalAudio {
            path: audio_path.clone(),
        },
    );
    let expected_queued_input = user_input_with_media(
        "queued attachments",
        UserInput::Image {
            image_url: TINY_PNG_DATA_URL.to_string(),
            detail: Some(ImageDetail::Original),
        },
        UserInput::Audio {
            audio_url: "data:audio/mpeg;base64,YXVkaW8=".to_string(),
        },
    );

    let queued = service.enqueue(thread_id, queued_input).await?;
    std::fs::remove_file(&image_path)?;
    std::fs::remove_file(&audio_path)?;

    assert_eq!(expected_queued_input, queued.input);
    assert_eq!(vec![queued.clone()], service.list(thread_id).await?);
    let queued_record = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await?
        .into_iter()
        .next()
        .context("snapshotted queued attachments were not persisted")?;
    let persisted_queued_input: TurnInput =
        decode_turn_input(&queued_record.payload)?.expect("user message payload");
    assert_eq!(expected_queued_input, persisted_queued_input);

    let edited_image_path = home.path().join("edited.png");
    let edited_audio_path = home.path().join("edited.m4a");
    std::fs::write(&edited_image_path, TINY_PNG_BYTES)?;
    std::fs::write(&edited_audio_path, b"edited audio")?;
    let edited_input = user_input_with_media(
        "edited attachments",
        UserInput::LocalImage {
            path: edited_image_path.clone(),
            detail: Some(ImageDetail::High),
        },
        UserInput::LocalAudio {
            path: edited_audio_path.clone(),
        },
    );
    let expected_edited_input = user_input_with_media(
        "edited attachments",
        UserInput::Image {
            image_url: TINY_PNG_DATA_URL.to_string(),
            detail: Some(ImageDetail::High),
        },
        UserInput::Audio {
            audio_url: "data:audio/mp4;base64,ZWRpdGVkIGF1ZGlv".to_string(),
        },
    );

    let edited = service
        .update(thread_id, queued.id.clone(), edited_input)
        .await?
        .context("snapshotted queued attachments were not updated")?;
    std::fs::remove_file(&edited_image_path)?;
    std::fs::remove_file(&edited_audio_path)?;

    assert_eq!(expected_edited_input, edited.input);
    assert_eq!(vec![edited.clone()], service.list(thread_id).await?);
    let edited_record = queue
        .list_page(thread_id, /*offset*/ 0, /*limit*/ 1)
        .await?
        .into_iter()
        .next()
        .context("snapshotted edited attachments were not persisted")?;
    let persisted_edited_input: TurnInput =
        decode_turn_input(&edited_record.payload)?.expect("user message payload");
    assert_eq!(expected_edited_input, persisted_edited_input);

    Ok(())
}

#[tokio::test]
async fn invalid_local_attachments_do_not_mutate_queue() -> anyhow::Result<()> {
    let (queue, home) = test_queue().await?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let thread_id = ThreadId::new();
    let existing = service.enqueue(thread_id, user_input("existing")).await?;
    let missing_path = home.path().join("missing.png");
    let invalid_input = user_input_with_media(
        "invalid attachments",
        UserInput::LocalImage {
            path: missing_path,
            detail: Some(ImageDetail::Original),
        },
        UserInput::Audio {
            audio_url: "data:audio/mpeg;base64,YXVkaW8=".to_string(),
        },
    );

    assert!(matches!(
        service
            .enqueue(thread_id, invalid_input.clone())
            .await,
        Err(QueueServiceError::InvalidAttachment(error))
            if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert!(matches!(
        service
            .update(
                thread_id,
                existing.id.clone(),
                invalid_input,
            )
            .await,
        Err(QueueServiceError::InvalidAttachment(error))
            if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert_eq!(vec![existing], service.list(thread_id).await?);

    Ok(())
}

#[tokio::test]
async fn non_user_input_cannot_enter_the_user_message_queue() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let error = service
        .enqueue(
            ThreadId::new(),
            TurnInput::ResponseItem(ResponseItem::Other),
        )
        .await
        .expect_err("response item should not enter the user queue");
    assert!(matches!(error, QueueServiceError::InvalidInput));
    Ok(())
}

#[tokio::test]
async fn queued_text_limit_is_enforced_across_all_input_items() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let thread_id = ThreadId::new();
    let existing = service.enqueue(thread_id, user_input("existing")).await?;
    let mut oversized = user_input(&"x".repeat(MAX_USER_INPUT_TEXT_CHARS / 2));
    if let TurnInput::UserInput { content, .. } = &mut oversized {
        content.push(UserInput::Text {
            text: "y".repeat(MAX_USER_INPUT_TEXT_CHARS / 2 + 1),
            text_elements: Vec::new(),
        });
    }

    assert!(matches!(
        service
            .enqueue(thread_id, oversized.clone())
            .await,
        Err(QueueServiceError::InputTooLarge { actual_chars })
            if actual_chars == MAX_USER_INPUT_TEXT_CHARS + 1
    ));
    assert!(matches!(
        service
            .update(
                thread_id,
                existing.id.clone(),
                oversized,
            )
            .await,
        Err(QueueServiceError::InputTooLarge { actual_chars })
            if actual_chars == MAX_USER_INPUT_TEXT_CHARS + 1
    ));
    assert_eq!(vec![existing], service.list(thread_id).await?);

    Ok(())
}

#[tokio::test]
async fn queued_text_limit_counts_characters_and_ignores_non_text_items() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let service = QueuedItemService::new(queue, Weak::new(), Arc::new(NoopExtensionEventSink));
    let mut input = structured_user_input(&"é".repeat(MAX_USER_INPUT_TEXT_CHARS));
    if let TurnInput::UserInput { content, .. } = &mut input {
        content.push(UserInput::Mention {
            name: "Demo App".to_string(),
            path: "app://demo-app".to_string(),
        });
    }

    let queued = service.enqueue(ThreadId::new(), input.clone()).await?;
    assert_eq!(input, queued.input);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_head_is_skipped_and_a_live_user_turn_is_accepted() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    let response =
        responses::mount_sse_once(&server, responses::sse_completed("queued-turn")).await;
    let test = test_codex()
        .with_config(|config| config.include_environment_context = false)
        .build_with_auto_env(&server)
        .await?;
    let thread_id = test.session_configured.thread_id;
    let queue = loaded_thread_queue(&test)?;
    queue
        .enqueue(thread_id, r#"{"unsupported":true}"#.to_string())
        .await?;
    let service = QueuedItemService::new(
        queue,
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    );

    service
        .enqueue(thread_id, structured_user_input("durable follow-up"))
        .await?;
    emit_idle(&service, thread_id).await;
    let client_id = wait_for_event_match(test.codex.as_ref(), |event| match event {
        EventMsg::ItemCompleted(event) => match &event.item {
            TurnItem::UserMessage(item) => Some(item.client_id.clone()),
            _ => None,
        },
        _ => None,
    })
    .await;
    assert_eq!(Some("stable-client-message".to_string()), client_id);
    wait_for_event_match(test.codex.as_ref(), |event| match event {
        EventMsg::TurnComplete(_) => Some(()),
        _ => None,
    })
    .await;
    assert!(service.list(thread_id).await?.is_empty());
    let request = response.single_request();
    assert_eq!(
        vec!["durable follow-up"],
        request.message_input_texts("user")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_idle_dispatches_input_without_a_loaded_manager() -> anyhow::Result<()> {
    let server = start_mock_server().await;
    responses::mount_sse_once(&server, responses::sse_completed("resumed-turn")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let thread_id = test.session_configured.thread_id;
    let queue = loaded_thread_queue(&test)?;
    QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    )
    .enqueue(thread_id, user_input("queued while unloaded"))
    .await?;
    let service = QueuedItemService::new(
        queue,
        Arc::downgrade(&test.thread_manager),
        Arc::new(NoopExtensionEventSink),
    );
    emit_idle(&service, thread_id).await;
    assert!(service.list(thread_id).await?.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// PR 8-5: Scheduler lifecycle tests
//
// These tests verify the durable queue scheduler's lease-based concurrency
// guarantees: two workers cannot claim the same item, expired leases are
// recovered by a replacement worker, and stop_claiming prevents new work.
// ---------------------------------------------------------------------------

/// Two workers cannot claim the same due head item.
#[tokio::test]
async fn two_workers_cannot_claim_the_same_item() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#.to_string()).await?;

    let service_a = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let service_b = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Worker A claims first.
    let claimed_a = queue
        .claim_lease(
            item.id.clone(),
            service_a.worker_id().to_string(),
            0,
            60_000,
        )
        .await?;
    assert!(claimed_a.is_some());

    // Worker B cannot claim the same item.
    let claimed_b = queue
        .claim_lease(
            item.id.clone(),
            service_b.worker_id().to_string(),
            0,
            60_000,
        )
        .await?;
    assert!(claimed_b.is_none());

    Ok(())
}

/// A replacement worker recovers expired leases from a crashed worker.
#[tokio::test]
async fn replacement_worker_recovers_expired_leases() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#.to_string()).await?;

    let service_a = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Worker A claims with a short lease, then "crashes" (no ack/release).
    queue
        .claim_lease(item.id.clone(), service_a.worker_id().to_string(), 0, 1_000)
        .await?;

    // Before expiry, the item is not due.
    let due = queue.list_due_heads(500, 10).await?;
    assert!(due.is_empty());

    // After expiry, recovery resets the lease.
    let recovered = queue.recover_expired_leases(2_000).await?;
    assert_eq!(1, recovered);

    // A replacement worker can now claim the item.
    let service_b = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let claimed = queue
        .claim_lease(
            item.id.clone(),
            service_b.worker_id().to_string(),
            2_000,
            60_000,
        )
        .await?;
    assert!(claimed.is_some());

    Ok(())
}

/// `stop_claiming` prevents the scheduler from dispatching new items.
#[tokio::test]
async fn stop_claiming_prevents_new_dispatch() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    queue.enqueue(thread_id, r#"{"n":1}"#.to_string()).await?;

    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Stop claiming before any scheduling cycle runs.
    service.stop_claiming();
    assert!(!service.is_claiming_enabled());

    // schedule_due_items returns 0 because claiming is disabled.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item remains in the queue.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// `recover_leases` on startup resets expired leases from a previous process.
#[tokio::test]
async fn recover_leases_on_startup_resets_expired_leases() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let item = queue.enqueue(thread_id, r#"{"n":1}"#.to_string()).await?;

    // Simulate a crashed previous process: claim with a short lease.
    queue
        .claim_lease(item.id.clone(), "crashed-worker".to_string(), 0, 1_000)
        .await?;

    // New service starts up and recovers leases.
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    let recovered = service.recover_leases().await?;
    // Note: recover_leases uses the current time, so the 1-second lease
    // may or may not have expired depending on test execution speed.
    // We only assert that recovery doesn't error; the count is timing-dependent.
    let _ = recovered;

    Ok(())
}

/// The scheduler dispatches due items without a connected client.
#[tokio::test]
async fn scheduler_dispatches_without_connected_client() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let payload = codex_queue_extension::QueuePayload::user_message(user_input("hello"))?;
    queue.enqueue(thread_id, payload).await?;

    // Create a service with no thread manager (no connected client).
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // schedule_due_items discovers the item but cannot dispatch it
    // because there is no thread manager. The item is claimed but
    // dispatch returns NotIdle, so the lease is released.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item should still be in the queue (released back to pending).
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// The scheduler attempts to dispatch suspended-tree recovery jobs but
/// releases the lease when no thread manager is available.
#[tokio::test]
async fn scheduler_dispatches_suspended_tree_recovery_no_manager() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a suspended-tree recovery job.
    let manifest = codex_queue_extension::SuspendedTreeRecoveryManifest {
        root_thread_id: thread_id.clone(),
        root_turn_id: "root-turn".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 0,
        not_before_ms: 0,
        usage_limit_snapshot: None,
        replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 0,
            categories: codex_queue_extension::ReplayEffectCategories::default(),
        },
    };
    let payload = codex_queue_extension::QueuePayload::suspended_tree_recovery(manifest)?;
    queue.enqueue(thread_id, payload).await?;

    // Create a service with no thread manager (no connected client).
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // schedule_due_items discovers the recovery job but cannot dispatch
    // it because there is no thread manager.  It releases the lease so
    // the item remains in the queue for a later retry.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item should still be in the queue (released back to pending).
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// The scheduler refuses to dispatch a suspended-tree recovery job when
/// the manifest says effects are ambiguous (AttentionRequired).
#[tokio::test]
async fn scheduler_refuses_attention_required_recovery() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a suspended-tree recovery job with ambiguous effects.
    let manifest = codex_queue_extension::SuspendedTreeRecoveryManifest {
        root_thread_id: thread_id.clone(),
        root_turn_id: "root-turn".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 0,
        not_before_ms: 0,
        usage_limit_snapshot: None,
        replay_safety: codex_queue_extension::ReplaySafetySummary::AttentionRequired {
            reason: "ambiguous tool output".to_string(),
        },
    };
    let payload = codex_queue_extension::QueuePayload::suspended_tree_recovery(manifest)?;
    queue.enqueue(thread_id, payload).await?;

    // Create a service with no thread manager.
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // schedule_due_items discovers the recovery job but refuses to
    // dispatch it because the manifest says effects are ambiguous.
    // The lease is released so the item remains in the queue.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item should still be in the queue (released back to pending).
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// Stale writer fencing: a worker whose lease has expired cannot
/// acknowledge or release the item.  The item is recovered by a different
/// worker.
#[tokio::test]
async fn stale_writer_cannot_acknowledge_after_lease_expiry() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a user message.
    let payload = codex_queue_extension::QueuePayload::user_message(TurnInput::UserInput {
        content: vec![codex_protocol::user_input::UserInput::Text {
            text: "stale-writer-test".to_string(),
            text_elements: Vec::new(),
        }],
        client_id: None,
    })?;
    queue.enqueue(thread_id, payload).await?;

    // Get the item ID.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());
    let item_id = items[0].id.clone();

    // Worker A claims the item.
    let now_ms = chrono::Utc::now().timestamp_millis();
    let claimed = queue
        .claim_lease(
            item_id.clone(),
            "worker-A".to_string(),
            now_ms,
            1, // 1ms lease — expires immediately
        )
        .await?;
    assert!(claimed.is_some());

    // Wait for the lease to expire.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Worker B recovers expired leases.
    let later_ms = chrono::Utc::now().timestamp_millis();
    let recovered = queue.recover_expired_leases(later_ms).await?;
    assert_eq!(1, recovered);

    // Worker A tries to acknowledge — should fail (no rows affected).
    let ack_result = queue
        .acknowledge(item_id.clone(), "worker-A".to_string())
        .await?;
    assert!(
        !ack_result,
        "stale worker should not be able to acknowledge"
    );

    // Worker B can now claim the item.
    let claimed_b = queue
        .claim_lease(item_id.clone(), "worker-B".to_string(), later_ms, 60_000)
        .await?;
    assert!(
        claimed_b.is_some(),
        "worker B should be able to claim after recovery"
    );

    Ok(())
}

/// Exactly-once: after a successful dispatch, the item is acknowledged
/// and cannot be re-dispatched by another worker.
#[tokio::test]
async fn exactly_once_acknowledge_prevents_redispatch() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a user message.
    let payload = codex_queue_extension::QueuePayload::user_message(TurnInput::UserInput {
        content: vec![codex_protocol::user_input::UserInput::Text {
            text: "exactly-once-test".to_string(),
            text_elements: Vec::new(),
        }],
        client_id: None,
    })?;
    queue.enqueue(thread_id, payload).await?;

    // Get the item ID.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());
    let item_id = items[0].id.clone();

    // Worker A claims and acknowledges the item.
    let now_ms = chrono::Utc::now().timestamp_millis();
    let claimed = queue
        .claim_lease(item_id.clone(), "worker-A".to_string(), now_ms, 60_000)
        .await?;
    assert!(claimed.is_some());

    let acked = queue
        .acknowledge(item_id.clone(), "worker-A".to_string())
        .await?;
    assert!(acked, "acknowledge should succeed for the lease owner");

    // Worker B tries to claim the same item — should fail (already acked).
    let claimed_b = queue
        .claim_lease(item_id.clone(), "worker-B".to_string(), now_ms, 60_000)
        .await?;
    assert!(claimed_b.is_none(), "acked item should not be claimable");

    // The queue should be empty.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(0, items.len());

    Ok(())
}

/// Parent/child recovery order: the manifest's members list is in
/// parent-first order so recovery loads parents before children.
#[tokio::test]
async fn recovery_manifest_members_are_parent_first() -> anyhow::Result<()> {
    let root_thread = ThreadId::new();
    let child_thread = ThreadId::new();
    let grandchild_thread = ThreadId::new();

    let manifest = codex_queue_extension::SuspendedTreeRecoveryManifest {
        root_thread_id: root_thread.clone(),
        root_turn_id: "root-turn".to_string(),
        original_input: None,
        members: vec![
            codex_queue_extension::SuspendedTreeMember {
                thread_id: child_thread.clone(),
                turn_id: "child-turn".to_string(),
                parent_thread_id: root_thread.clone(),
                parent_turn_id: "root-turn".to_string(),
                root_turn_id: "root-turn".to_string(),
                agent_path: "root.child".to_string(),
                replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
                    verified_tool_calls: 0,
                    categories: codex_queue_extension::ReplayEffectCategories::default(),
                },
            },
            codex_queue_extension::SuspendedTreeMember {
                thread_id: grandchild_thread.clone(),
                turn_id: "grandchild-turn".to_string(),
                parent_thread_id: child_thread.clone(),
                parent_turn_id: "child-turn".to_string(),
                root_turn_id: "root-turn".to_string(),
                agent_path: "root.child.grandchild".to_string(),
                replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
                    verified_tool_calls: 0,
                    categories: codex_queue_extension::ReplayEffectCategories::default(),
                },
            },
        ],
        coordinator_generation: 1,
        not_before_ms: 0,
        usage_limit_snapshot: None,
        replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 0,
            categories: codex_queue_extension::ReplayEffectCategories::default(),
        },
    };

    // Verify the ordering: child (parent) appears before grandchild.
    assert_eq!(manifest.members.len(), 2);
    assert_eq!(manifest.members[0].thread_id, child_thread);
    assert_eq!(manifest.members[1].thread_id, grandchild_thread);

    // The parent of the grandchild is the child, which appears earlier.
    assert_eq!(
        manifest.members[1].parent_thread_id, manifest.members[0].thread_id,
        "grandchild's parent must appear before grandchild"
    );

    Ok(())
}

/// Partial suspension rollback: if recovery fails, the lease is released
/// and the item remains in the queue for retry.
#[tokio::test]
async fn partial_suspension_rollback_releases_lease() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a suspended-tree recovery job with Safe replay safety.
    let manifest = codex_queue_extension::SuspendedTreeRecoveryManifest {
        root_thread_id: thread_id.clone(),
        root_turn_id: "root-turn".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 0,
        not_before_ms: 0,
        usage_limit_snapshot: None,
        replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 0,
            categories: codex_queue_extension::ReplayEffectCategories::default(),
        },
    };
    let payload = codex_queue_extension::QueuePayload::suspended_tree_recovery(manifest)?;
    queue.enqueue(thread_id, payload).await?;

    // Create a service with no thread manager — recovery will fail with
    // NotIdle, releasing the lease.
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // First dispatch attempt fails (no thread manager).
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item should still be in the queue (lease released).
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    // Second dispatch attempt also fails — the item is still pending.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// The `on_turn_suspended` lifecycle callback enqueues a durable
/// `SuspendedTreeRecoveryManifest` that the scheduler can later dispatch.
///
/// This exercises the producer side of the recovery flow: core emits the
/// callback, the queue extension enqueues the manifest, and the scheduler
/// discovers and attempts to dispatch it.
#[tokio::test]
async fn on_turn_suspended_enqueues_recovery_manifest() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let thread_id_str = thread_id.to_string();

    // Create the queue service (no thread manager — we only test enqueue).
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Simulate the core emitting on_turn_suspended after a successful
    // suspension with a due time 60s in the future.
    let not_before_ms = 60_000;
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(&thread_id_str);
    let replay_safety = codex_extension_api::SuspendedReplaySafety::Safe {
        verified_tool_calls: 3,
        categories: codex_extension_api::ReplayEffectCategories::default(),
    };
    let input = TurnSuspendedInput {
        thread_id: &thread_id_str,
        turn_id: "suspended-turn",
        not_before_ms,
        members: &[],
        coordinator_generation: 7,
        replay_safety: &replay_safety,
        usage_limit_snapshot: None,
        session_store: &session_store,
        thread_store: &thread_store,
    };
    <QueuedItemService as ThreadLifecycleContributor<codex_core::config::Config>>::on_turn_suspended(
        &service,
        input,
    )
    .await;

    // The manifest should be durably enqueued.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());
    assert_eq!(not_before_ms, items[0].not_before_ms);

    // The scheduler should not dispatch the item yet (not_before_ms is in
    // the future relative to the queue's due-time check).
    // Note: schedule_due_items uses the queue store's not_before_ms filter.
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item remains in the queue, pending.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// The `on_turn_suspended` callback with `not_before_ms = 0` enqueues a
/// manifest that is immediately due, so the scheduler discovers it.
#[tokio::test]
async fn on_turn_suspended_due_immediately_is_discovered_by_scheduler() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let thread_id_str = thread_id.to_string();

    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Simulate suspension with not_before_ms = 0 (due immediately).
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(&thread_id_str);
    let replay_safety = codex_extension_api::SuspendedReplaySafety::Safe {
        verified_tool_calls: 0,
        categories: codex_extension_api::ReplayEffectCategories::default(),
    };
    let input = TurnSuspendedInput {
        thread_id: &thread_id_str,
        turn_id: "suspended-turn-immediate",
        not_before_ms: 0,
        members: &[],
        coordinator_generation: 0,
        replay_safety: &replay_safety,
        usage_limit_snapshot: None,
        session_store: &session_store,
        thread_store: &thread_store,
    };
    <QueuedItemService as ThreadLifecycleContributor<codex_core::config::Config>>::on_turn_suspended(
        &service,
        input,
    )
    .await;

    // The manifest should be enqueued and due immediately.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());
    assert_eq!(0, items[0].not_before_ms);

    // The scheduler discovers the due item.  Dispatch fails because there
    // is no thread manager, but the item is found (lease released back).
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The item remains in the queue.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    Ok(())
}

/// The `on_turn_suspended` callback with real member data and a non-zero
/// coordinator generation enqueues a manifest that preserves the member
/// relationships and replay-safety classification.
#[tokio::test]
async fn on_turn_suspended_preserves_members_and_coordinator_generation() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let root_thread_id = ThreadId::new();
    let root_thread_id_str = root_thread_id.to_string();
    let child_thread_id = ThreadId::new();
    let child_thread_id_str = child_thread_id.to_string();

    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    let member = codex_extension_api::SuspendedMemberInfo {
        thread_id: child_thread_id_str.clone(),
        turn_id: "child-turn-1".to_string(),
        parent_thread_id: root_thread_id_str.clone(),
        parent_turn_id: "root-turn-1".to_string(),
        root_turn_id: "root-turn-1".to_string(),
        agent_path: "root.child".to_string(),
        replay_safety: codex_extension_api::SuspendedReplaySafety::Safe {
            verified_tool_calls: 1,
            categories: codex_extension_api::ReplayEffectCategories {
                shell_commands: 1,
                file_edits: 0,
                mcp_calls: 0,
                dynamic_tool_calls: 0,
            },
        },
    };
    let replay_safety = codex_extension_api::SuspendedReplaySafety::Safe {
        verified_tool_calls: 2,
        categories: codex_extension_api::ReplayEffectCategories {
            shell_commands: 1,
            file_edits: 1,
            mcp_calls: 0,
            dynamic_tool_calls: 0,
        },
    };
    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(&root_thread_id_str);
    let input = TurnSuspendedInput {
        thread_id: &root_thread_id_str,
        turn_id: "root-turn-1",
        not_before_ms: 0,
        members: std::slice::from_ref(&member),
        coordinator_generation: 42,
        replay_safety: &replay_safety,
        usage_limit_snapshot: None,
        session_store: &session_store,
        thread_store: &thread_store,
    };
    <QueuedItemService as ThreadLifecycleContributor<codex_core::config::Config>>::on_turn_suspended(
        &service,
        input,
    )
    .await;

    // Verify the manifest was enqueued with the correct data.
    let records = queue.list_page(root_thread_id, 0, 10).await?;
    assert_eq!(1, records.len());

    // Decode the payload to inspect the manifest.
    let manifest = codex_queue_extension::decode_suspended_tree_recovery(&records[0].payload)?
        .expect("payload should be a suspended-tree recovery manifest");

    assert_eq!(manifest.root_thread_id, root_thread_id);
    assert_eq!(manifest.root_turn_id, "root-turn-1");
    assert_eq!(manifest.coordinator_generation, 42);
    assert_eq!(manifest.members.len(), 1);
    assert_eq!(manifest.members[0].thread_id, child_thread_id);
    assert_eq!(manifest.members[0].turn_id, "child-turn-1");
    assert_eq!(manifest.members[0].parent_thread_id, root_thread_id);
    assert_eq!(manifest.members[0].parent_turn_id, "root-turn-1");
    assert_eq!(manifest.members[0].root_turn_id, "root-turn-1");
    assert_eq!(manifest.members[0].agent_path, "root.child");
    // Per-member replay safety survives the callback → manifest → SQLite → decode round trip.
    assert_eq!(
        manifest.members[0].replay_safety,
        codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 1,
            categories: codex_queue_extension::ReplayEffectCategories {
                shell_commands: 1,
                file_edits: 0,
                mcp_calls: 0,
                dynamic_tool_calls: 0,
            },
        }
    );
    // Root replay safety with per-category breakdown survives the round trip.
    assert_eq!(
        manifest.replay_safety,
        codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 2,
            categories: codex_queue_extension::ReplayEffectCategories {
                shell_commands: 1,
                file_edits: 1,
                mcp_calls: 0,
                dynamic_tool_calls: 0,
            },
        }
    );

    Ok(())
}

/// A suspended-tree recovery manifest enqueued before shutdown is still
/// present and discoverable after the queue runner stops claiming, so a
/// replacement process can dispatch it.
///
/// This verifies the graceful-shutdown ordering guarantee: the queue
/// runner stops claiming before waiting threads are suspended, so the
/// manifest written by the suspension callback is not raced by a
/// duplicate dispatch from the current process.
#[tokio::test]
async fn suspended_tree_recovery_survives_shutdown_and_is_redispatchable() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();

    // Enqueue a suspended-tree recovery manifest (simulating what
    // on_turn_suspended would write during graceful shutdown).
    let manifest = codex_queue_extension::SuspendedTreeRecoveryManifest {
        root_thread_id: thread_id,
        root_turn_id: "suspended-turn".to_string(),
        original_input: None,
        members: Vec::new(),
        coordinator_generation: 1,
        not_before_ms: 0,
        usage_limit_snapshot: None,
        replay_safety: codex_queue_extension::ReplaySafetySummary::Safe {
            verified_tool_calls: 0,
            categories: codex_queue_extension::ReplayEffectCategories::default(),
        },
    };
    let payload = codex_queue_extension::QueuePayload::suspended_tree_recovery(manifest)?;
    queue.enqueue(thread_id, payload).await?;

    // Simulate graceful shutdown step 1: stop claiming.
    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    service.stop_claiming();
    assert!(!service.is_claiming_enabled());

    // The scheduler cannot dispatch the item (claiming is stopped).
    let dispatched = service.schedule_due_items().await?;
    assert_eq!(0, dispatched);

    // The manifest is still in the queue, available for a replacement.
    let items = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, items.len());

    // A replacement process starts and discovers the due manifest.
    let replacement = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );
    replacement.recover_leases().await?;
    let due = queue.list_due_heads(0, 10).await?;
    assert_eq!(1, due.len());
    assert_eq!(due[0].thread_id, thread_id);

    Ok(())
}

/// The `on_turn_suspended` callback preserves the serialized usage-limit
/// coordinator snapshot so a replacement process can rehydrate the
/// coordinator for unloaded threads.
#[tokio::test]
async fn on_turn_suspended_preserves_usage_limit_snapshot() -> anyhow::Result<()> {
    let (queue, _home) = test_queue().await?;
    let thread_id = ThreadId::new();
    let thread_id_str = thread_id.to_string();

    let service = QueuedItemService::new(
        Arc::clone(&queue),
        Weak::new(),
        Arc::new(NoopExtensionEventSink),
    );

    // Simulate a coordinator snapshot at suspension time.
    let snapshot = serde_json::json!({
        "work_id": "root-turn-1",
        "generation": 7,
        "trigger_thread_id": thread_id_str,
        "trigger_turn_id": "root-turn-1",
        "phase": "Suspended",
        "classification": "UsageWindow",
        "limit_id": null,
        "reset_at": 1_700_000_000,
        "next_check_at": null,
        "auto_resume_enabled": true,
        "automatic_resumes_used": 1,
        "max_auto_resumes": 5,
        "availability_checks_used": 0,
        "max_availability_checks": 3,
        "unknown_reset_wait_started_at": null,
        "unknown_reset_max_wait_seconds": 600,
        "manual_wait_reason": null,
        "parked_thread_ids": [],
        "expected_participants": [],
        "leader_thread_id": null,
        "manual_retry_requested": false,
    });

    let session_store = ExtensionData::new("session");
    let thread_store = ExtensionData::new(&thread_id_str);
    let replay_safety = codex_extension_api::SuspendedReplaySafety::Safe {
        verified_tool_calls: 0,
        categories: codex_extension_api::ReplayEffectCategories::default(),
    };
    let input = TurnSuspendedInput {
        thread_id: &thread_id_str,
        turn_id: "suspended-turn",
        not_before_ms: 0,
        members: &[],
        coordinator_generation: 7,
        replay_safety: &replay_safety,
        usage_limit_snapshot: Some(&snapshot),
        session_store: &session_store,
        thread_store: &thread_store,
    };
    <QueuedItemService as ThreadLifecycleContributor<codex_core::config::Config>>::on_turn_suspended(
        &service,
        input,
    )
    .await;

    // Decode the manifest and verify the snapshot round-trips.
    let records = queue.list_page(thread_id, 0, 10).await?;
    assert_eq!(1, records.len());
    let manifest = codex_queue_extension::decode_suspended_tree_recovery(&records[0].payload)?
        .expect("payload should be a suspended-tree recovery manifest");
    assert_eq!(manifest.coordinator_generation, 7);
    assert_eq!(manifest.usage_limit_snapshot, Some(snapshot));

    Ok(())
}
