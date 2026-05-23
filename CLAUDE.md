# Outbox Publisher (Rust)

## Project

**outbox-publisher-rs** — small client library that applications use to write domain events to a Postgres outbox table atomically with their business writes, plus helpers to verify HMAC-signed webhooks delivered by **outbox-dispatcher**. Paired with the dispatcher but with no source-level dependency in either direction: the only contract is the shared SQL schema.

Design document: `../TDDs/05-outbox-publisher-tdd.md`. The step-by-step build plan lives in §12 of that document — it is the source of truth for what to do next.

## Status

Phases 1–3 are implemented and merged on `main`:

- **Phase 1** — workspace, core types, `DomainEvent`/`Publisher` traits, `#[derive(DomainEvent)]` proc-macro.
- **Phase 2** — `SqlxPublisher` with `append`, `append_with_id`, `append_batch` (UNNEST single round-trip); testcontainers integration tests.
- **Phase 3** — `WebhookVerifier` with two-sided drift tolerance and redacting `Debug`, `WebhookEnvelope<E>`, constant-time HMAC via `Mac::verify_slice` + proptest single-byte-flip coverage, and the `axum` extractor (`OutboxWebhook<E>`, `WebhookRejection` with `400`/`401`/`422` mapping).

**Phase 4 (Distribution) is next** per TDD §12. Recommended order: `4.3 (CI) → 4.1 (examples) → 4.2 (docs) → 4.5 dry-run → 4.4 (cross-language interop, blocked on dispatcher v1.0.0 image) → 4.5 (real release)`.

## Workspace layout (target — established by Step 1.1)

```
outbox-publisher-rs/
├── Cargo.toml                       # workspace root
├── rust-toolchain.toml              # pinned to 1.88.0 in lockstep with outbox-dispatcher
├── crates/
│   ├── outbox-publisher/            # umbrella crate (published to crates.io)
│   │   ├── src/
│   │   │   ├── lib.rs               # re-exports per feature flags (sqlx, derive, axum)
│   │   │   ├── domain_event.rs      # DomainEvent trait
│   │   │   ├── event.rs             # EventContext, EventId
│   │   │   ├── publisher.rs         # Publisher trait with Tx<'a> GAT
│   │   │   ├── error.rs             # PublishError, VerifyError
│   │   │   └── webhook/
│   │   │       ├── mod.rs           # WebhookVerifier, WebhookEnvelope
│   │   │       └── signing.rs       # ported from outbox-dispatcher/.../signing.rs
│   │   └── Cargo.toml
│   ├── outbox-publisher-derive/     # proc-macro crate: #[derive(DomainEvent)]
│   │   ├── src/lib.rs
│   │   └── Cargo.toml
│   └── outbox-publisher-sqlx/       # SQLx Postgres adapter (SqlxPublisher)
│       ├── src/lib.rs
│       └── Cargo.toml
├── tests/
│   └── fixtures/
│       └── 0001_initial_schema.sql  # read-only copy of outbox-dispatcher/migrations/0001_*
├── examples/
│   ├── axum-handler.rs
│   ├── webhook-receiver.rs
│   └── batch-emit.rs
└── README.md
```

Most consumers depend only on `outbox-publisher = { version = "1", features = ["sqlx", "derive", "axum"] }`. The sub-crates are split so users on a different database driver can pull just the umbrella crate without the SQLx transitive deps.

## Mandatory After Every Code Change

Run in this order after **every** edit — fix all issues before moving on:

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If a `Cargo.toml` dependency was added or removed:

```bash
cargo sort --workspace
```

`Cargo.lock` is committed to keep CI builds reproducible. Update it with
`cargo update -p <crate>` for targeted bumps rather than a blanket `cargo update`.

If a sqlx query macro was added or changed:

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher cargo sqlx prepare --workspace
```

## Key source files (target)

| File                                              | Purpose                                                                    |
|---------------------------------------------------|----------------------------------------------------------------------------|
| `crates/outbox-publisher/src/domain_event.rs`     | `DomainEvent` trait                                                        |
| `crates/outbox-publisher/src/event.rs`            | `EventContext`, `EventId`                                                  |
| `crates/outbox-publisher/src/publisher.rs`        | `Publisher` trait with associated `Tx<'a>` GAT                             |
| `crates/outbox-publisher/src/error.rs`            | `PublishError`, `VerifyError`                                              |
| `crates/outbox-publisher/src/webhook/mod.rs`      | `WebhookVerifier`, `WebhookEnvelope<E>`, optional axum extractor           |
| `crates/outbox-publisher/src/webhook/signing.rs`  | HMAC-SHA256 ported from outbox-dispatcher (constant-time `verify`)         |
| `crates/outbox-publisher-derive/src/lib.rs`       | `#[derive(DomainEvent)]` proc-macro                                        |
| `crates/outbox-publisher-sqlx/src/lib.rs`         | `SqlxPublisher`; `append`, `append_with_id`, `append_batch`                |
| `tests/fixtures/0001_initial_schema.sql`          | Read-only fixture; testcontainers integration tests apply it to a fresh DB |

## Schema contract (read-only)

The publisher writes to `outbox_events` but **never** owns the schema. The dispatcher's migration is the single source of truth.

- `tests/fixtures/0001_initial_schema.sql` is a byte-for-byte copy of `outbox-dispatcher/migrations/0001_initial_schema.sql`. Do not edit it locally.
- Re-copy whenever the dispatcher bumps the schema. The cross-language interop test (Step 4.4 in TDD §12) is what catches drift in CI once the dispatcher's `v1.0.0` image is published.
- Production deployments rely on the dispatcher having migrated the database before the publisher writes its first row.

## sqlx offline mode

The `.sqlx/` directory contains cached query metadata and is checked into version control. Builds without `DATABASE_URL` use it automatically (`SQLX_OFFLINE=true`). Regenerate after any sqlx query macro change:

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher cargo sqlx prepare --workspace
```

## Integration tests

Tests in `crates/outbox-publisher-sqlx/tests/` use `testcontainers` to spin up a real ephemeral Postgres per test, apply the schema fixture, and exercise the publisher end-to-end. Docker must be running.

```bash
cargo test --test '*'
```

## Implementation phases

See `TDDs/05-outbox-publisher-tdd.md` §12 for the PR-sized step-by-step plan. Summary:

| Phase | Status   | Description                                                                       |
|-------|----------|-----------------------------------------------------------------------------------|
| 1     | DONE     | Workspace, core types, `DomainEvent` + `Publisher` traits, derive macro           |
| 2     | DONE     | `SqlxPublisher`; `append`, `append_with_id`, `append_batch`                       |
| 3     | DONE     | `WebhookVerifier`, `WebhookEnvelope`, constant-time verify, axum extractor        |
| 4     | TODO     | CI (4.3), examples (4.1), docs (4.2), crates.io publish (4.5); cross-language interop (4.4) blocked on dispatcher v1.0.0 |

## Key design notes

- **Schema is not owned here.** The publisher only INSERTs into `outbox_events`. No migrations, no DDL — the dispatcher owns those.
- **Atomicity through the caller's transaction.** `Publisher::append(&mut tx, ...)` takes the caller's `sqlx::Transaction`. The library never commits or rolls back; the application is responsible for the transaction lifecycle.
- **Port, don't depend.** `crates/outbox-publisher/src/webhook/signing.rs` is copied from `outbox-dispatcher/crates/http-callback/src/signing.rs`. The cross-language interop test (Step 4.4) catches drift. Resist creating an upstream dependency on `outbox-dispatcher-core` — that crate is not on crates.io (deferred to dispatcher v1.1) and pulling it would couple the publisher to the binary's release cadence.
- **HMAC body bytes**: feed the raw payload to HMAC via `mac.update(body)`. Never `String::from_utf8_lossy(body)` (mutates non-UTF-8 with U+FFFD) or `format!("{ts}.{body}")` (allocates the full payload). Stream `format!("{ts}.")` then `body`.
- **Constant-time verify**: use `Mac::verify_slice` or `subtle::ConstantTimeEq` on decoded digests — never `==` on hex strings. The dispatcher's `verify_rejects_single_byte_flip` test is mirrored on this side via proptest.
- **`Publisher` trait is generic over `Tx<'a>`** (associated GAT). Applications using a different database driver implement the trait themselves; the umbrella crate ships only the SQLx impl.
- **Caller-provided `event_id`** is a first-class API. `append_with_id` lets applications use deterministic UUIDs (e.g. v5) for cross-system idempotency. The default `append` generates UUID v4 internally. (Resolves TDD §10 Q2.)
- **No `unwrap()` / `expect()` in library code.** Library crates surface every error through `PublishError` / `VerifyError`. Examples and tests may use `expect()` freely.
- **Secrets never `Debug`-printed.** `WebhookVerifier` (and any future key-holding types) either custom-impl `Debug` to mask the secret bytes or do not derive `Debug` at all.
- **Cross-language consistency is load-bearing.** `WebhookEnvelope` field names, JSON serialisation, and the HMAC signing format must stay byte-identical to TDD 04 §6.1 and the Java library — they share dispatchers.

## Code Conventions

### Comments

- Use comments sparingly — only for complex or non-obvious logic; self-documenting code is preferred.

### Errors

- Use `thiserror` for all error types in library crates (`outbox-publisher`, `outbox-publisher-sqlx`).
- Examples may use `anyhow` with `.with_context(|| ...)` for brevity.

### Async / Tokio

- No blocking calls inside `async fn` — no sync file I/O, no `std::thread::sleep`, no blocking HTTP clients.
- Never hold a lock across an `.await` point.
- Use `tokio::sync` primitives in async code, not `std::sync::Mutex` / `RwLock` — unless the critical section is purely synchronous and never crosses an `.await` point, in which case `std::sync::Mutex` is correct and lighter.

### Database

- All queries use SQLx compile-time macros (`sqlx::query!`, `sqlx::query_as!`, `sqlx::query_scalar!`).
- The publisher takes the caller's `&mut Transaction` — it never opens its own transaction, never commits, never rolls back.

### Proc-macro (`outbox-publisher-derive`)

- Compile errors emit spans pointing at the offending source token (use `syn::Error::new_spanned`), not the `#[derive(...)]` line.
- The macro generates only one `impl DomainEvent` block — no additional impls, no `pub use`, no shadowing.
- Hygienic identifiers — no leaking macro-internal names into user scope.

### Logging

- Use `tracing` macros where logging is useful (mostly examples and the axum extractor): `debug!` for request details, `info!` for business events, `warn!` for verification failures.

### Testing

- Unit tests use hand-rolled mocks for the `Publisher` trait — `mockall`'s `#[automock]` does not support GAT-bearing traits (`type Tx<'a>`). See `tests/publisher_mock_test.rs` for the reference pattern.
- Integration tests use `testcontainers` Postgres with the read-only schema fixture.
- Cross-language interop tests (Phase 4.4) pull `ghcr.io/volodymyrd/outbox-dispatcher:1.0.0` and exercise publisher → dispatcher → receiver end-to-end.
- Target >90% coverage per module.
- Test both happy path AND all error branches.
