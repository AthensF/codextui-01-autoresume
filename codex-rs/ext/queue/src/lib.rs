//! Durable, storage-neutral user-message queue and idle dispatch.

use std::sync::Arc;

use codex_extension_api::ExtensionRegistryBuilder;

mod payload;
pub(crate) mod replay_safety;
mod scheduler;
mod service;

pub use payload::CURRENT_PAYLOAD_VERSION;
pub use payload::QueueEnvelope;
pub use payload::QueueJobKind;
pub use payload::QueuePayload;
pub use payload::ReplayEffectCategories;
pub use payload::ReplaySafetySummary;
pub use payload::SuspendedTreeMember;
pub use payload::SuspendedTreeRecoveryManifest;
pub use payload::decode_payload;
pub use payload::decode_suspended_tree_recovery;
pub use payload::decode_turn_input;
pub use service::QueueServiceError;
pub use service::QueuedItem;
pub use service::QueuedItemService;
pub use service::ThreadLoader;

/// Registers the caller-owned queue before lower-priority idle contributors.
pub fn install<C>(registry: &mut ExtensionRegistryBuilder<C>, service: Arc<QueuedItemService>)
where
    C: Send + Sync + 'static,
{
    let watcher = Arc::downgrade(&service);
    registry.thread_lifecycle_contributor(service);
    tokio::spawn(QueuedItemService::watch_external_messages(watcher));
}
