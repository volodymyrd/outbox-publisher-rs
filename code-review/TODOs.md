# Code Review — PR #3 follow-up TODOs

**Date:** 2026-05-23T09:29:05Z
**Last updated:** 2026-05-23T09:42:54Z
**Branch:** phase2
**Reviewed by:** Claude (review command)
**Scope:** Carry-forward TODOs from prior reviews + new findings from a fresh review of PR #3 (`crates/outbox-publisher-sqlx/src/lib.rs`, `crates/outbox-publisher-sqlx/tests/integration_test.rs`)

---

## Context

PR #3 status at the time of this review: `OPEN` / `MERGEABLE` / `mergeStateStatus = CLEAN` — ready to merge.

The two prior review logs — for PR #1 (Phase 1, 40 findings) and PR #2 of Phase 2 history (PR #3, 17 findings) — were audited and every row was marked DONE before being removed from the working tree. They remain in git history if a future reader needs the per-finding detail. No findings are being carried over from those files; the items below come solely from the fresh re-review of PR #3.

`cargo check --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` both pass cleanly at HEAD (`9143595`).

---

## Findings

### Finding 1 — `MissingCallbacks` and `empty batch` integration tests boot Postgres for paths that never touch the DB

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher-sqlx/tests/integration_test.rs:294-317`, `:320-339`, `:491-513`, `:516-549` |
| **Severity** | Low |
| **Category** | Testing |

**Problem**

Phase 2 Finding 17 moved the `Serialization` test to a `#[cfg(test)]` unit test in `lib.rs` because "it's the only test in the file that does not actually need a database". That invariant was broken by the simultaneously-landed `MissingCallbacks` tests — `SqlxPublisher::insert` and `insert_batch` both check `ctx.callbacks().is_empty()` and return `PublishError::MissingCallbacks` **before** issuing any SQL (lib.rs:121-123, lib.rs:173-177). Likewise, `insert_batch` returns `Ok(vec![])` immediately when `events.is_empty()` (lib.rs:169-171).

Four tests today boot a `testcontainers` Postgres image (~3–10 s startup + schema apply) for a code path that never opens a connection:

- `append_returns_missing_callbacks_error` (line 491)
- `append_batch_returns_missing_callbacks_error` (line 516)
- `append_with_id_returns_missing_callbacks_error` (line 294)
- `append_batch_empty_is_noop` (line 320) — only the post-hoc `COUNT(*)` assertion uses the DB, and that assertion is redundant once the function's early-return semantics are unit-tested

For `cargo test --test integration_test`, that is 4 of ~14 containers — roughly 30% of integration-suite wall time spent on no-op paths.

**Context** (representative example — surrounding code as it exists today)

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:294-317
#[tokio::test]
async fn append_with_id_returns_missing_callbacks_error() {
    let (pool, _container) = setup_db().await;            // boots Postgres, applies schema
    let publisher = SqlxPublisher::new();

    let event_id = EventId::from(Uuid::new_v4());
    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "no-cb@example.com".to_owned(),
    };
    let ctx = EventContext::default();                    // no callbacks

    let mut tx = pool.begin().await.expect("begin tx");   // unused: no SQL issued past this point
    let err = publisher
        .append_with_id(&mut tx, event_id, &event, &ctx)
        .await
        .expect_err("expected MissingCallbacks");
    tx.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::MissingCallbacks),
        "expected MissingCallbacks, got {err:?}",
    );
}
```

**Recommended fix**

Move all four tests into a `#[cfg(test)] mod tests` block at the bottom of `crates/outbox-publisher-sqlx/src/lib.rs`, alongside `serialization_error_converts_to_publish_error`. None of them need a real `Transaction`; they can exercise `SqlxPublisher` via the trait directly using a `tokio::test` runtime, since the early-return branches fire before the `tx` argument is used.

The cleanest shape is to bypass the `Publisher` trait (which requires a real `Tx<'a> = Transaction<'_, Postgres>`) and call the private `insert`/`insert_batch` directly with a transaction obtained from a single shared `lazy<PgPool>`, OR — simpler — factor a `fn validate_callbacks(ctx: &EventContext) -> Result<(), PublishError>` and `fn validate_batch(events: &[...]) -> Result<(), PublishError>` helper and unit-test those:

```rust
// crates/outbox-publisher-sqlx/src/lib.rs (new helpers)
fn validate_callbacks(ctx: &EventContext) -> Result<(), PublishError> {
    if ctx.callbacks().is_empty() {
        return Err(PublishError::MissingCallbacks);
    }
    Ok(())
}

// in `insert`:
validate_callbacks(ctx)?;

// in `insert_batch`:
for (_, ctx) in events {
    validate_callbacks(ctx)?;
}
```

```rust
// crates/outbox-publisher-sqlx/src/lib.rs (new tests, no container required)
#[cfg(test)]
mod tests {
    use super::*;

    // ... existing serialization_error_converts_to_publish_error ...

    #[test]
    fn validate_callbacks_rejects_empty() {
        let ctx = EventContext::default();
        let err = validate_callbacks(&ctx).expect_err("expected MissingCallbacks");
        assert!(matches!(err, PublishError::MissingCallbacks));
    }

    #[test]
    fn validate_callbacks_accepts_non_empty() {
        let ctx = EventContext::default()
            .with_callbacks(vec![serde_json::json!({"name": "n", "url": "u"})]);
        assert!(validate_callbacks(&ctx).is_ok());
    }
}
```

Delete the corresponding three integration tests (`append_returns_missing_callbacks_error`, `append_batch_returns_missing_callbacks_error`, `append_with_id_returns_missing_callbacks_error`).

`append_batch_empty_is_noop` is a slightly different case — it covers the public-API contract that an empty slice yields an empty `Vec<EventId>` and does not query the DB. The cheapest correct change is to keep it but drop the `setup_db()` call entirely; `insert_batch` short-circuits before any `tx` use, so a minimal fake (or a pool obtained via `OnceLock` shared with the rest of the suite) works:

```rust
// alternative: keep as integration test but share the pool via OnceLock so
// the cost is paid once per `cargo test` invocation, not per test.
```

If sharing the pool is out of scope, the simplest option is to move `append_batch_empty_is_noop` to a unit test that asserts `Ok(vec![])` for the `events.is_empty()` branch only — the absence of any DB write is implicit in not issuing SQL.

**Why this fix**

Re-establishes the invariant Phase 2 Finding 17 set: integration tests in this file own the "real Postgres" cost; everything else lives next to the code. Roughly a third of the integration-suite wall time goes to spinning up containers for assertions that don't depend on Postgres. The CLAUDE.md "test both happy path AND all error branches" rule is satisfied either way — moving the tests does not change coverage, only execution cost.

---

### Finding 2 — Stale `Finding N` section-header comments reference deleted review numbers

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher-sqlx/tests/integration_test.rs:353, 383, 460` |
| **Severity** | Low |
| **Category** | Idiom |

**Problem**

Three section-header comments in the integration-test file point at numbered findings from prior review files that have since been deleted from the working tree (kept only in git history). New readers cannot resolve what `Finding 5`, `Finding 6`, or `Finding 16` mean without spelunking through git — and the numbers will continue to bit-rot. Section headers should describe the test, not cite ephemeral review IDs.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:353
// ── Finding 6 — rollback test for append_with_id ─────────────────────────────

/// `append_with_id` rollback leaves no row in the table.
#[tokio::test]
async fn append_with_id_rollback_leaves_no_row() {
```

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:383
// ── Finding 5 — batch with mixed-NULL optional fields ─────────────────────────

/// `append_batch` correctly handles a mix of populated and absent optional fields.
#[tokio::test]
async fn append_batch_handles_mixed_null_optional_fields() {
```

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:460
// ── Finding 16 — batch vs individual column equivalence ──────────────────────

/// `append_batch` writes the same column values as N successive `append` calls.
#[tokio::test]
async fn append_batch_writes_same_columns_as_individual_appends() {
```

**Recommended fix**

Replace each `Finding N` reference with a descriptive section title — or drop the section banner entirely, since each test already has a doc-comment describing its scope.

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:353
// ── Rollback behaviour ───────────────────────────────────────────────────────

/// `append_with_id` rollback leaves no row in the table.
#[tokio::test]
async fn append_with_id_rollback_leaves_no_row() {
```

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:383
// ── Optional field handling ──────────────────────────────────────────────────

/// `append_batch` correctly handles a mix of populated and absent optional fields.
#[tokio::test]
async fn append_batch_handles_mixed_null_optional_fields() {
```

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:460
// ── Batch / individual equivalence ───────────────────────────────────────────

/// `append_batch` writes the same column values as N successive `append` calls.
#[tokio::test]
async fn append_batch_writes_same_columns_as_individual_appends() {
```

**Why this fix**

Comments should make sense to a reader who only has the current code — referencing review-file finding numbers couples the source to documents that are not under source control alongside the code. Per CLAUDE.md, comments are for non-obvious *why*; review-bookkeeping markers don't qualify.

---

### Finding 3 — `tx2.rollback().await.ok()` silently swallows rollback errors

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher-sqlx/tests/integration_test.rs:281` |
| **Severity** | Low |
| **Category** | Testing |

**Problem**

The duplicate-event-id test uses `.ok()` to discard the result of `tx2.rollback().await`, which drops both success and error outcomes. Every other rollback in this file uses `.expect("rollback")` (lines 204, 373, 607). The inconsistency is small but masks the case where the test transaction itself is unhealthy — and tests should be loud about cleanup failures, not silently move on.

The transaction here aborts because `append_with_id` returned an error, so PostgreSQL has already marked the connection's transaction state. A subsequent `ROLLBACK` is the legitimate way to clear that state and should succeed; if it doesn't, the test environment is broken and the test should fail.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher-sqlx/tests/integration_test.rs:276-287
    let mut tx2 = pool.begin().await.expect("begin tx2");
    let err = publisher
        .append_with_id(&mut tx2, event_id, &event2, &ctx)
        .await
        .expect_err("expected duplicate error");
    tx2.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::DuplicateEventId),
        "expected DuplicateEventId, got {err:?}",
    );
```

**Recommended fix**

```rust
    let mut tx2 = pool.begin().await.expect("begin tx2");
    let err = publisher
        .append_with_id(&mut tx2, event_id, &event2, &ctx)
        .await
        .expect_err("expected duplicate error");
    tx2.rollback().await.expect("rollback");

    assert!(
        matches!(err, outbox_publisher::error::PublishError::DuplicateEventId),
        "expected DuplicateEventId, got {err:?}",
    );
```

**Why this fix**

Matches the convention used by every other rollback in the file, and makes a real transaction-cleanup failure visible instead of silent. `.ok()` is the equivalent of writing "if this fails, do nothing" — almost never what a test wants.

---

## Summary

| # | Title | File:Line | Severity | Category | Status | Notes |
|---|-------|-----------|----------|----------|--------|-------|
| 1 | `MissingCallbacks` and empty-batch integration tests boot Postgres for no-DB code paths | `tests/integration_test.rs:294,320,491,516` | Low | Testing | DONE | Extracted `validate_callbacks` helper; added `validate_callbacks_rejects_empty` and `validate_callbacks_accepts_non_empty` unit tests in `lib.rs`; deleted the 3 `MissingCallbacks` integration tests. `append_batch_empty_is_noop` kept in integration suite — `pool.begin()` requires a real connection so it cannot be made DB-free without refactoring the `Transaction` type boundary. |
| 2 | Stale `Finding N` section-header comments reference deleted review numbers | `tests/integration_test.rs:353,383,460` | Low | Idiom | DONE | Replaced `Finding 6`, `Finding 5`, `Finding 16` banners with descriptive titles: `Rollback behaviour`, `Optional field handling`, `Batch / individual equivalence`. |
| 3 | `tx2.rollback().await.ok()` silently swallows rollback errors | `tests/integration_test.rs:281` | Low | Testing | DONE | Changed `.ok()` to `.expect("rollback")` to match every other rollback in the file. |

> **Instructions for the implementing LLM:**
> - Change `TODO` to `DONE` once a finding is fully addressed.
> - If a finding is intentionally skipped or cannot be applied, change `TODO` to `SKIPPED` and add a short reason in the **Notes** column.
> - Do not delete rows — the table is the authoritative implementation log.
