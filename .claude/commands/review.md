Review the code in $ARGUMENTS (or the most recently edited file if none specified) for quality, correctness, and production-readiness in this Rust + Tokio + sqlx (Postgres) outbox-publisher library codebase.

## Checklist

### Rust Idioms
- [ ] No `unwrap()` / `expect()` in library code — only allowed in examples, tests, or where an invariant is documented inline (e.g. `HmacSha256::new_from_slice` accepts any key length)
- [ ] No `clone()` calls that could be avoided with references or `Arc`
- [ ] Use `?` operator instead of manual `match` on `Result` / `Option`
- [ ] Prefer `if let` / `while let` over explicit `match` for single-arm patterns
- [ ] Iterators preferred over manual `for` loops with `push`
- [ ] No unnecessary `collect()` before immediately iterating
- [ ] Library errors use `thiserror`; examples may use `anyhow` with `.with_context(...)`
- [ ] `#[derive(Debug, Clone)]` only where semantically correct; never derive `Debug` on types holding secrets (`WebhookVerifier`'s `secret: Vec<u8>` masked or not derived)
- [ ] Prefer `From` / `Into` impls over explicit conversion functions
- [ ] Comments only for non-obvious logic — self-documenting code preferred (per CLAUDE.md)
- [ ] Public items in the umbrella crate carry rustdoc (`#![deny(missing_docs)]` once Phase 4.2 lands)

### Async / Tokio
- [ ] No blocking calls inside `async fn` (sync file I/O, `std::thread::sleep`, blocking HTTP clients)
- [ ] No locks held across `.await` points
- [ ] `tokio::sync` primitives in async code, not `std::sync::Mutex` / `RwLock`
- [ ] Async traits use `#[async_trait]` (object safety) unless GAT-only callers are intended

### Database (sqlx + Postgres, publisher path)
- [ ] All queries use sqlx compile-time macros (`sqlx::query!`, `query_scalar!`) — no raw string queries
- [ ] `append`, `append_with_id`, `append_batch` take the caller's `&mut sqlx::Transaction<'_, Postgres>` — never open their own transaction, never call `.commit()` or `.rollback()`
- [ ] `append_batch` uses a single round trip (`UNNEST`) — no per-row `INSERT` in a loop
- [ ] `.sqlx/` query cache regenerated after any query change (`cargo sqlx prepare --workspace`)
- [ ] The publisher writes only the columns documented in TDD 05 §2.1 — `id` and `created_at` are set by the database
- [ ] `event_id` UNIQUE-constraint violations on caller-provided IDs surface as a distinguishable `PublishError` variant, not a generic database error

### Schema fixture (`tests/fixtures/0001_initial_schema.sql`)
- [ ] File is a byte-identical copy of `outbox-dispatcher/migrations/0001_initial_schema.sql` — no local edits
- [ ] No `CREATE` / `ALTER` / `DROP` issued from publisher source code — the dispatcher owns DDL
- [ ] Any PR that bumps the dispatcher schema also re-syncs the fixture and notes it in the description

### Publisher trait
- [ ] `Publisher` stays usable across crate boundaries (`async-trait` is acceptable; the `Tx<'a>` GAT is the trade-off the TDD made for driver-agnosticism)
- [ ] No leaking of driver-specific types through the umbrella crate's public API
- [ ] Integration tests use `testcontainers` for a real ephemeral Postgres; unit tests mock the trait via `#[automock]`
- [ ] `append_with_id` overload exists alongside the UUID-v4 default (resolves TDD §10 Q2)

### Proc-macro hygiene (`outbox-publisher-derive`)
- [ ] Compile errors point at the offending source span via `syn::Error::new_spanned`, not the `#[derive]` line
- [ ] All documented negative cases reject with useful diagnostics: missing `kind`, missing `aggregate`, no `aggregate_id` field, multiple `aggregate_id` fields, non-`Uuid` `aggregate_id`, derive on enum or union
- [ ] Macro generates only one `impl DomainEvent` block — no extra traits, no `pub use`, no shadowed imports
- [ ] Hygienic identifiers — no leaking macro-internal names into user scope (`__outbox_*` internal idents if needed)
- [ ] `trybuild`-style compile-fail tests cover every negative path

### Webhook verification (`outbox-publisher/src/webhook/signing.rs`)
- [ ] HMAC fed the **raw body bytes** via `mac.update(body)` — never `String::from_utf8_lossy(body)` (mutates non-UTF-8 with U+FFFD) or `format!("{ts}.{body}")` (allocates the full payload). Stream `format!("{ts}.")` then `body`.
- [ ] Signature comparison uses `Mac::verify_slice(...)` or `subtle::ConstantTimeEq` on decoded digests — never `==` on hex strings
- [ ] Timestamp tolerance defaults to 5 minutes per TDD 04 §6.2; `with_tolerance` builder is the only path to override
- [ ] `VerifyError` variants cover: missing header, malformed header (no `t=`, no `v1=`, garbage hex), expired timestamp, invalid signature, body parse failure
- [ ] Secrets never appear in `Debug` output, error messages, or logs
- [ ] Property test (proptest) injects single-byte flips into a valid digest and asserts `verify` returns `Err(InvalidSignature)` — mirrors dispatcher's `verify_rejects_single_byte_flip`

### Cross-language consistency
- [ ] `WebhookEnvelope` field names match TDD 04 §6.1 exactly: `delivery_id`, `event_id`, `kind`, `callback_name`, `mode`, `aggregate_type`, `aggregate_id`, `payload`, `metadata`, `actor_id`, `correlation_id`, `causation_id`, `created_at`, `attempt`
- [ ] JSON serialisation matches the dispatcher: snake_case keys, ISO-8601 timestamps, UUIDs as strings
- [ ] HMAC signing format is `t=<unix_seconds>,v1=<lowercase_hex>` — identical to dispatcher and the Java library (TDD 05 §5)
- [ ] Any divergence in shape, naming, or serialisation must be flagged for Step 4.4 cross-language interop CI

### Testing
- [ ] Every public function has at least one test
- [ ] Error branches (missing header, expired timestamp, signature mismatch, payload parse failure, DB unique violation) tested
- [ ] Test names follow `<function>_<scenario>` convention
- [ ] Target >90% coverage per module (per CLAUDE.md)
- [ ] Integration tests do not hard-code ports or assume a particular Postgres image tag — `testcontainers` handles that

## Output Format

For each issue found:
1. **File:Line** — exact location
2. **Severity** — `Critical` / `High` / `Medium` / `Low`
3. **Category** — (Security | Correctness | Concurrency | Performance | Idiom | Proc-macro | Schema | Testing | Cross-language)
4. **Finding** — what the problem is
5. **Fix** — the idiomatic Rust solution with a code snippet

End with a summary table of findings by severity.

## Report File

After completing the review, **always** write a report file, even if there are no findings.

### Gather context first

Before writing, run:
```bash
git branch --show-current
date -u +"%Y-%m-%dT%H:%M:%SZ"
```

### File path

Create the `code-review/` directory at the workspace root if it does not exist, then determine the report path:

```
code-review/YYYY-MM-DD_<branch>_<target-slug>.md
```

- `YYYY-MM-DD` — today's UTC date
- `<branch>` — current git branch name with `/` replaced by `-`
- `<target-slug>` — the reviewed file or scope: base filename without extension, or `workspace` when reviewing the full workspace

Example: `code-review/2026-06-01_phase2-sqlx_publisher.md`

**Same-file rule:** Before writing, check whether a report file already exists for this branch + target-slug combination (the date in the filename may differ — glob `code-review/*_<branch>_<target-slug>.md`). If one exists, **append** new findings to that file rather than creating a new one:

1. Number new findings sequentially after the highest existing finding number.
2. Update the `**Last updated:**` header line (add it after `**Date:**` if absent).
3. Add new rows to the existing summary table.
4. Do **not** duplicate findings that are already present (matched by title or file:line).
5. Do **not** create a separate `-round2`, `-followup`, or dated-suffix file.

If no prior report exists, create the file with today's date as `YYYY-MM-DD`.

### Report structure

Use **exactly** this template:

```markdown
# Code Review — <target>

**Date:** <ISO-8601 UTC datetime>
**Branch:** <branch>
**Reviewed by:** Claude (review command)
**Scope:** <file path(s) or "full workspace">

---

## Findings

<!-- One section per finding. Omit section entirely if no findings. -->

### Finding <N> — <short title>

| Field | Value |
|-------|-------|
| **File:Line** | `path/to/file.rs:42` |
| **Severity** | Critical / High / Medium / Low |
| **Category** | Security / Correctness / Concurrency / Performance / Idiom / Proc-macro / Schema / Testing / Cross-language |

**Problem**

<One or two sentences describing what is wrong and why it matters.>

**Context** (surrounding code as it exists today)

```rust
// file.rs lines 38-48
<exact existing code excerpt — enough for an LLM to locate and understand the problem>
```

**Recommended fix**

```rust
<complete corrected replacement — not a diff, the full new form of the changed lines>
```

**Why this fix**

<One sentence explaining the Rust/project reasoning behind the recommendation.>

---

<!-- repeat for each finding -->

## Summary

| # | Title | File:Line | Severity | Category | Status | Notes |
|---|-------|-----------|----------|----------|--------|-------|
| 1 | <short title> | `path/file.rs:42` | Critical | Correctness | TODO | |
| 2 | <short title> | `path/file.rs:88` | High | Idiom | TODO | |

> **Instructions for the implementing LLM:**
> - Change `TODO` to `DONE` once a finding is fully addressed.
> - If a finding is intentionally skipped or cannot be applied, change `TODO` to `SKIPPED` and add a short reason in the **Notes** column.
> - Do not delete rows — the table is the authoritative implementation log.
```

### When there are no findings

Still write the file. Use an empty `## Findings` section with a note:

```markdown
## Findings

No issues found.
```

And a summary table with a single row:

```markdown
| # | Title | File:Line | Severity | Category | Status | Notes |
|---|-------|-----------|----------|----------|--------|-------|
| — | No findings | — | — | — | DONE | All checklist items passed |
```

## Mandatory Post-Change Steps

After applying **every** fix, run these commands in order and resolve all issues before finishing:

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If any `Cargo.toml` dependency was added or removed:
```bash
cargo sort --workspace
```

If any sqlx query macro was added or changed:
```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher cargo sqlx prepare --workspace
```

Do not report the module as done until all commands exit cleanly and all tests pass.
