# codextui-01-autoresume

Fork of [openai/codex](https://github.com/openai/codex) with durable usage-limit recovery, TUI countdown controls, and replay-safe active-tree suspension.

## What this branch adds

Three commits on top of upstream `0d46c252b3`:

| Commit | Description |
|--------|-------------|
| `3b8cd403eb` | Durable usage-limit recovery with tree-level suspension |
| `8feed1adab` | Replay-safety auditing with per-member, per-effect data |
| `f7ac6610dc` | Durable snapshot hydration, TUI countdown, and shutdown tests |

### Features

- **Durable recovery**: Waitable usage-limit failures are treated as nonterminal. Suspended turns are persisted to SQLite and recovered across process restarts.
- **Tree-level suspension**: Entire agent trees (root + descendants) are suspended child-first with replay-safety classification.
- **Replay-safety auditing**: Each suspended member gets its own classification with per-effect categories (shell commands, file edits, MCP calls, dynamic tools).
- **Coordinator snapshot hydration**: The usage-limit coordinator snapshot is serialized at suspension time and deserialized on recovery, so unloaded threads can report their wait status without a live encounter.
- **TUI live countdown**: The bottom status bar updates the reset countdown on every frame tick, not just when a notification arrives. Surfaces `next_check_at`, `manual_wait_reason`, and auto-resume/check counters.
- **`/usage-limit` menu**: Shows phase, reset countdown, auto-resume/check counters, and manual-wait reason. Actions: Retry now, Toggle auto-resume, Cancel wait.
- **Graceful shutdown ordering**: Queue claiming stops before waiting threads are suspended, preventing duplicate dispatch across restart boundaries.

---

## Set up on another machine

### Prerequisites

- Rust toolchain (stable, with `cargo`)
- macOS or Linux
- ~5 GB free disk for `target/`

### Clone and build

```bash
git clone https://github.com/AthensF/codextui-01-autoresume.git
cd codextui-01-autoresume/codex-rs
cargo build --release
```

### Run the TUI

```bash
# From codex-rs/
cargo run --release -p codex-tui -- \
  --experimental-api \
  --config experimental_thread_store=local
```

Or set up a config file at `~/.codex/config.toml`:

```toml
[experimental_thread_store]
mode = "local"

[experimental_api]
enabled = true
```

Then just:

```bash
cargo run --release -p codex-tui
```

### Enable the features

The usage-limit recovery features require:

1. **`experimental_api`** — enables the `thread/usageLimit/*` RPCs and notifications.
2. **`experimental_thread_store = "local"`** — enables SQLite-backed durable queue storage (required for recovery manifests to persist across restarts).

Without both, the features are dormant and you'll see standard usage-limit behavior (hard failure).

### What you'll see

When your model provider returns a waitable usage-limit error (HTTP 429 with a reset time):

1. The TUI bottom status shows: `Work temporarily unavailable · Waiting for reset · reset in 5m 30s · auto-resume on`
2. The countdown updates live every second.
3. New prompts are queued durably (not sent until capacity returns).
4. Type `/usage-limit` to see full status and take actions.
5. On app-server restart, suspended turns are recovered automatically from SQLite.

### Run the tests

```bash
# Queue extension (durable queue, lease recovery, manifests)
cd codex-rs
RUST_MIN_STACK=16777216 cargo test -p codex-queue-extension

# Core usage-limit recovery
RUST_MIN_STACK=16777216 cargo test -p codex-core --lib usage_limit_recovery_tests

# TUI usage-limit UI
cargo test -p codex-tui --lib usage_limit

# App-server compile check
cargo check -p codex-app-server
```

> **Note:** Some integration tests overflow the default thread stack. Use `RUST_MIN_STACK=16777216` for the queue and core suites.

---

## Agent prompt

Paste this into a new agent session to continue work on this project:

```
You are continuing work on a Rust Codex TUI/app-server project at /Users/athens/codextui-01-autoresume (or wherever you cloned it).

This is a fork of openai/codex with durable usage-limit recovery. The repo has three commits of work on top of upstream 0d46c252b3:

1. 3b8cd403eb — Durable usage-limit recovery with tree-level suspension
2. 8feed1adab — Replay-safety auditing with per-member, per-effect data
3. f7ac6610dc — Durable snapshot hydration, TUI countdown, and shutdown tests

Key architecture:
- Usage-limit coordinator (codex-rs/usage-limits/) tracks wait state per agent tree
- Turn suspension (codex-rs/core/src/session/turn_suspension.rs) serializes the coordinator snapshot into a SuspendedTreeRecoveryManifest
- The queue extension (codex-rs/ext/queue/) persists manifests to SQLite and dispatches them when due
- The app-server (codex-rs/app-server/) runs a background scheduler that loads unloaded threads and recovers suspended turns
- The TUI (codex-rs/tui/) shows a live countdown and /usage-limit menu

The roadmap plan is at /Users/athens/.devin/plans/plan-df12cd0ec00a8fa0.md.

Remaining work that could be done:
- Rebase onto latest upstream main (conflicts expected in input_submission.rs, turn.rs, hook_runtime.rs, thread-store/lib.rs)
- End-to-end app-server integration test that starts a turn, kills the process mid-lease, restarts, and asserts exactly-once dispatch
- TUI server-side queue view (thread/queue/list is wired in the protocol but not yet surfaced in the UI)
- Snapshot hydration tests at the app-server integration level (currently tested at the queue extension level)

Before making changes:
- Read AGENTS.md in the repo root for Rust/codex-rs conventions
- Run cargo fmt --all after edits
- Use RUST_MIN_STACK=16777216 for queue and core integration tests
- The repo has two remotes: "mine" (your fork) and "origin" (upstream openai/codex)
- Do not push to origin; push to mine
```

---

## Repo layout

```
codextui-01-autoresume/
├── codex-rs/                    # Rust workspace
│   ├── core/                    # Codex Core (turn execution, suspension)
│   ├── app-server/              # App-server (queue runner, usage-limit processor)
│   ├── tui/                     # TUI (chatwidget, usage-limit wait UI)
│   ├── ext/
│   │   ├── extension-api/       # Extension API (TurnSuspendedInput, replay safety)
│   │   └── queue/               # Queue extension (manifests, scheduler, leases)
│   ├── usage-limits/            # Usage-limit coordinator state machine
│   ├── protocol/                # Protocol types (TreeRecoveryRequest, etc.)
│   ├── state/                   # SQLite state runtime + queue store
│   └── thread-store/            # Thread metadata + queue store trait
├── AGENTS.md                    # Repo conventions (read this first)
└── USAGE.md                     # This file
```
