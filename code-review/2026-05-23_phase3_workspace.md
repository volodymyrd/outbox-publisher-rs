# Code Review — PR #4 (Phase 3: webhook verification)

**Date:** 2026-05-23T10:47:15Z
**Branch:** phase3
**Reviewed by:** Claude (review command)
**Scope:** PR #4 — `crates/outbox-publisher/src/webhook/signing.rs`, `crates/outbox-publisher/src/webhook/mod.rs`, `crates/outbox-publisher/Cargo.toml`, workspace `Cargo.toml`, and integration-test touch-ups in `crates/outbox-publisher-sqlx/tests/integration_test.rs`.

---

## Findings

### Finding 1 — Non-hex `v1=` digest is reported as `InvalidSignature` instead of `MalformedHeader`

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:131-149`, `crates/outbox-publisher/src/webhook/signing.rs:33-45`, `crates/outbox-publisher/src/error.rs:25-26` |
| **Severity** | Medium |
| **Category** | Correctness |

**Problem**

`VerifyError::MalformedHeader`'s rustdoc explicitly promises three failure modes — *"missing `t=`, `v1=`, or non-hex digest"* (`error.rs:26`). The first two are honored; the third is not. When the `v1=` field is present but contains characters outside `[0-9a-f]`, `signing::verify` short-circuits on `hex::decode` and returns `false`, and `WebhookVerifier::verify` translates that into `VerifyError::InvalidSignature` (`mod.rs:147-149`).

This breaks the documented contract and matters for downstream behaviour: the axum extractor maps `MalformedHeader` to `400 Bad Request` (a client-fixable input error) and `InvalidSignature` to `401 Unauthorized` (an auth failure). A receiver that gets a request with `X-Outbox-Signature: t=123,v1=not-hex!!` today sees `401`, which would lead an integrator to chase a secret-mismatch ghost instead of fixing the malformed payload.

There is no test on the `WebhookVerifier::verify` path that asserts the variant for a non-hex `v1=` field — `verify_rejects_non_hex_digest` (signing.rs:201-204) only checks the low-level boolean.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:131-149
        // Reject if v1= field is missing before doing any HMAC work.
        if !signature_header
            .split(',')
            .any(|p| p.trim().starts_with("v1="))
        {
            return Err(VerifyError::MalformedHeader);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        if now.saturating_sub(ts) > self.tolerance.as_secs() {
            return Err(VerifyError::TimestampOutOfTolerance);
        }

        if !verify(&self.secret, ts, body, signature_header) {
            return Err(VerifyError::InvalidSignature);
        }
```

```rust
// crates/outbox-publisher/src/webhook/signing.rs:33-45
pub fn verify(secret: &[u8], timestamp_secs: u64, body: &[u8], header_value: &str) -> bool {
    let Some(hex_digest) = parse_v1_digest(header_value) else {
        return false;
    };
    let Ok(decoded) = hex::decode(hex_digest) else {
        return false;
    };

    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(format!("{timestamp_secs}.").as_bytes());
    mac.update(body);
    mac.verify_slice(&decoded).is_ok()
}
```

**Recommended fix**

Lift the hex-decode step into `WebhookVerifier::verify` so the two parse-failure cases stay together, and have `signing::verify` accept the already-decoded digest. Or, less invasively, expose the parse-and-decode step from `signing` and call it explicitly from the high-level verifier so a hex-decode failure can return `MalformedHeader`.

```rust
// crates/outbox-publisher/src/webhook/signing.rs — new helper, keep existing verify intact
pub(crate) fn parse_v1_decoded(header_value: &str) -> Option<Vec<u8>> {
    let hex_digest = parse_v1_digest(header_value)?;
    hex::decode(hex_digest).ok()
}
```

```rust
// crates/outbox-publisher/src/webhook/mod.rs — replace the v1=/verify section
let decoded = signing::parse_v1_decoded(signature_header).ok_or(VerifyError::MalformedHeader)?;

let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_secs())
    .unwrap_or(0);

if now.saturating_sub(ts) > self.tolerance.as_secs() {
    return Err(VerifyError::TimestampOutOfTolerance);
}

if !signing::verify_decoded(&self.secret, ts, body, &decoded) {
    return Err(VerifyError::InvalidSignature);
}
```

```rust
// crates/outbox-publisher/src/webhook/mod.rs — new test
#[test]
fn verify_rejects_non_hex_v1_as_malformed_header() {
    let verifier = WebhookVerifier::new(SECRET.to_vec());
    let header = format!("t={},v1=not-hex!!", now_ts());
    let err = verifier.verify(&header, b"body").unwrap_err();
    assert!(matches!(err, VerifyError::MalformedHeader), "{err:?}");
}
```

**Why this fix**

A non-hex digest is a structural problem with the header value, not an authentication failure — calling it `MalformedHeader` lines up with the documented contract and produces the correct HTTP status code in the axum extractor, which is what integrators rely on to diagnose the failure.

---

### Finding 2 — Future-dated timestamps pass the replay-window check

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:138-145`, `crates/outbox-publisher/src/webhook/signing.rs:52-64` |
| **Severity** | Medium |
| **Category** | Security |

**Problem**

The tolerance check is one-sided: `now.saturating_sub(ts) > self.tolerance.as_secs()` only fires when `now > ts + tolerance`. A header carrying a `t=` value in the future (`ts > now`) yields `now.saturating_sub(ts) == 0`, which is always within tolerance, so a future-dated signature is accepted indefinitely.

Concretely, an attacker who once captured a signed request that happened to carry a future `t=` (e.g. due to a transient dispatcher clock skew, or a deliberately backdated test signature leaked to a logfile) can replay it long past any reasonable replay window. The standard Stripe-style check is `(now - ts).abs() > tolerance` — both directions. The dispatcher's signing module has the same one-sided check (and was the source of this port), but the publisher is the library *receivers* depend on; defensively closing this direction here protects every consumer.

The `verify_header` helper in `signing.rs` has the same one-sided check (lines 56-62).

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:138-145
let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_secs())
    .unwrap_or(0);

if now.saturating_sub(ts) > self.tolerance.as_secs() {
    return Err(VerifyError::TimestampOutOfTolerance);
}
```

**Recommended fix**

```rust
// crates/outbox-publisher/src/webhook/mod.rs
let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_secs())
    .unwrap_or(0);

let tolerance_secs = self.tolerance.as_secs();
let drift = if now >= ts { now - ts } else { ts - now };
if drift > tolerance_secs {
    return Err(VerifyError::TimestampOutOfTolerance);
}
```

Mirror the same change in `signing::verify_header` (or, per Finding 5, remove `verify_header` entirely), and add a test:

```rust
#[test]
fn verify_rejects_future_timestamp_outside_tolerance() {
    let body = b"body";
    let future_ts = now_ts() + 3_600; // 1 hour in the future
    let header = sign(SECRET, future_ts, body);
    let verifier = WebhookVerifier::new(SECRET.to_vec())
        .with_tolerance(Duration::from_secs(300));
    let err = verifier.verify(&header, body).unwrap_err();
    assert!(matches!(err, VerifyError::TimestampOutOfTolerance), "{err:?}");
}
```

**Why this fix**

A two-sided tolerance window matches how every well-known webhook-signing library (Stripe, GitHub, Shopify) treats `t=`. It prevents indefinite replay of any signature whose timestamp happened to be future-dated when it was captured, without rejecting legitimate small clock drift.

---

### Finding 3 — Axum extractor and `WebhookRejection` are entirely untested

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:175-275` |
| **Severity** | Medium |
| **Category** | Testing |

**Problem**

The `axum_support` module is a public surface (gated behind `feature = "axum"`) that integrators will actually wire into request handlers, but the PR adds zero tests for it. `OutboxWebhook::from_request`, `WebhookRejection::into_response`, the status-code mapping for each `VerifyError` variant, and the body/header extraction paths are all uncovered. CLAUDE.md requires *"every public function has at least one test"* and the file-level checklist asks for tests on the rejection conversion (the `BAD_REQUEST` / `UNAUTHORIZED` / `UNPROCESSABLE_ENTITY` mapping in particular).

The mapping itself is also subtle — `MalformedHeader` returning `400` while `InvalidSignature` returns `401` is a real behavioural difference that Finding 1 hinges on. Without tests, future refactors can quietly regress it.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:228-242
    impl IntoResponse for WebhookRejection {
        fn into_response(self) -> Response {
            let status = match &self.0 {
                VerifyError::MissingHeader | VerifyError::MalformedHeader => {
                    StatusCode::BAD_REQUEST
                }
                VerifyError::TimestampOutOfTolerance | VerifyError::InvalidSignature => {
                    StatusCode::UNAUTHORIZED
                }
                VerifyError::BodyParse(_) => StatusCode::UNPROCESSABLE_ENTITY,
            };
            warn!(error = %self.0, "webhook verification failed");
            (status, self.0.to_string()).into_response()
        }
    }
```

**Recommended fix**

Add a `#[cfg(all(test, feature = "axum"))]` test module that drives a tiny Router end-to-end. Use `axum::body::Body` and `tower::ServiceExt::oneshot` (already in the transitive tree) to send synthetic requests and assert status codes:

```rust
// crates/outbox-publisher/src/webhook/mod.rs (new tests)
#[cfg(all(test, feature = "axum"))]
mod axum_tests {
    use super::axum_support::OutboxWebhook;
    use super::*;
    use axum::{
        body::Body, extract::FromRef, http::{Request, StatusCode}, routing::post, Router,
    };
    use serde::Deserialize;
    use signing::sign;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct AppState { verifier: WebhookVerifier }
    impl FromRef<AppState> for WebhookVerifier {
        fn from_ref(s: &AppState) -> Self { s.verifier.clone() }
    }

    #[derive(Deserialize)]
    struct Empty {}

    async fn handler(OutboxWebhook(_): OutboxWebhook<Empty>) -> StatusCode { StatusCode::OK }

    fn app(secret: &[u8]) -> Router {
        Router::new()
            .route("/hook", post(handler))
            .with_state(AppState { verifier: WebhookVerifier::new(secret.to_vec()) })
    }

    fn envelope_bytes() -> Vec<u8> {
        serde_json::to_vec(&make_envelope_json(serde_json::json!({}))).unwrap()
    }

    #[tokio::test]
    async fn extractor_accepts_valid_signature() {
        let body = envelope_bytes();
        let ts = now_ts();
        let header = sign(SECRET, ts, &body);
        let resp = app(SECRET).oneshot(
            Request::post("/hook")
                .header("x-outbox-signature", header)
                .body(Body::from(body)).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn extractor_returns_401_for_bad_signature() {
        let body = envelope_bytes();
        let ts = now_ts();
        let header = sign(b"wrong-secret", ts, &body);
        let resp = app(SECRET).oneshot(
            Request::post("/hook")
                .header("x-outbox-signature", header)
                .body(Body::from(body)).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn extractor_returns_400_for_missing_header() {
        let resp = app(SECRET).oneshot(
            Request::post("/hook").body(Body::from(envelope_bytes())).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
```

Add a dev-dep on `tower = "0.5"` (axum already depends on it; `ServiceExt::oneshot` requires the `util` feature).

**Why this fix**

Treating the extractor as part of the library's public contract means it earns tests like everything else. The status-code mapping is exactly the kind of subtle behaviour that quietly drifts in refactors — a one-shot integration test per branch is the cheapest possible insurance.

---

### Finding 4 — Body-extraction failure is laundered into a fake `serde_json::Error`

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:262-266` |
| **Severity** | Medium |
| **Category** | Correctness / Idiom |

**Problem**

When `Bytes::from_request` fails (request body too large, IO error, content-length mismatch, etc.) the extractor synthesises a `serde_json::Error` by calling `serde_json::from_str::<serde_json::Value>("").unwrap_err()` and wraps it as `VerifyError::BodyParse`. Three problems compound here:

1. **Library `unwrap_err()`.** CLAUDE.md forbids `unwrap()`/`expect()` in library code unless an invariant is inline-documented. This particular call is "safe" in that an empty string is never valid JSON, but the rule exists to keep panic-free guarantees easy to audit — papering it over with a magic empty-string trick defeats the audit.
2. **The error message lies.** A receiver that hits a `413 Payload Too Large` rejection will see the message `"EOF while parsing a value at line 1 column 0"`, which sends integrators looking for JSON-shape bugs in a body that was never read.
3. **The HTTP status is wrong.** `VerifyError::BodyParse` maps to `422 Unprocessable Entity` (line 237), but a `Bytes` extraction failure is a transport-layer concern that should typically be `400 Bad Request` (or in some axum versions, `413`/`500` depending on the inner rejection).

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:262-266
            let body = Bytes::from_request(req, state).await.map_err(|_| {
                WebhookRejection(VerifyError::BodyParse(
                    serde_json::from_str::<serde_json::Value>("").unwrap_err(),
                ))
            })?;
```

**Recommended fix**

Add a distinct `VerifyError` variant for body-read failures and a parallel `WebhookRejection` arm. Use the underlying axum rejection's `Display` so the integrator sees the real cause:

```rust
// crates/outbox-publisher/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    // ... existing variants ...

    /// The request body could not be read from the underlying transport.
    #[error("request body read failed: {0}")]
    BodyRead(String),
}
```

```rust
// crates/outbox-publisher/src/webhook/mod.rs (axum extractor)
let body = Bytes::from_request(req, state).await.map_err(|e| {
    WebhookRejection(VerifyError::BodyRead(e.to_string()))
})?;
```

```rust
// crates/outbox-publisher/src/webhook/mod.rs (status mapping)
let status = match &self.0 {
    VerifyError::MissingHeader | VerifyError::MalformedHeader => StatusCode::BAD_REQUEST,
    VerifyError::TimestampOutOfTolerance | VerifyError::InvalidSignature => {
        StatusCode::UNAUTHORIZED
    }
    VerifyError::BodyParse(_) => StatusCode::UNPROCESSABLE_ENTITY,
    VerifyError::BodyRead(_) => StatusCode::BAD_REQUEST,
};
```

**Why this fix**

Naming the failure for what it is removes both the `unwrap_err` and the misleading diagnostic. A receiver troubleshooting a stuck deploy gets *"request body read failed: failed to buffer body"* instead of *"EOF while parsing a value"* — that one-line difference often saves a long debugging session.

---

### Finding 5 — `signing::verify_header` is a redundant public API duplicating `WebhookVerifier::verify`

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/signing.rs:52-64` |
| **Severity** | Low |
| **Category** | Idiom |

**Problem**

The new module exposes two parallel public verification entry points:

- `signing::verify_header(secret, body, header, max_age) -> bool` — coarse boolean, no error variants.
- `WebhookVerifier::new(secret).with_tolerance(...).verify(header, body) -> Result<(), VerifyError>` — typed errors, the documented happy path.

`verify_header` is not used by `WebhookVerifier` (which calls the lower-level `verify` directly), nor by the axum extractor. Its only callers are tests in `signing.rs`. Exposing two equivalent APIs makes the library harder to learn and creates a "second contract" that must be kept in sync (this is exactly where the future-timestamp bug from Finding 2 would diverge first).

The dispatcher ships `verify_header` as `pub fn` because the dispatcher has no `WebhookVerifier` wrapper. The publisher does, and is the library that webhook receivers depend on — so the one-true-path through `WebhookVerifier::verify` is the right user-facing API.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/signing.rs:47-64
/// High-level verifier: parses `t=`, enforces the replay window, then
/// delegates to [`verify`] for the constant-time HMAC check.
///
/// Returns `true` only when the signature is valid **and** within the replay
/// window.
pub fn verify_header(secret: &[u8], body: &[u8], header_value: &str, max_age: Duration) -> bool {
    let Some(ts) = parse_t_field(header_value) else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now.saturating_sub(ts) > max_age.as_secs() {
        return false;
    }
    verify(secret, ts, body, header_value)
}
```

**Recommended fix**

Either demote it to `pub(crate)` (or `#[cfg(test)]`) since only the in-module tests exercise it, or remove it entirely and let `signing::verify` plus `WebhookVerifier::verify` be the two layers:

```rust
// crates/outbox-publisher/src/webhook/signing.rs — option A: scope down
#[cfg(test)]
pub(crate) fn verify_header(secret: &[u8], body: &[u8], header_value: &str, max_age: Duration) -> bool {
    // ... unchanged ...
}
```

Then move the four `verify_header_*` tests under `#[cfg(test)]` (they already are) and rewrite them against `WebhookVerifier` so the tested code is the one users actually call.

**Why this fix**

One canonical path through a security primitive is easier to audit, easier to teach, and harder to accidentally break asymmetrically. `WebhookVerifier::verify` is strictly more informative (typed errors) and the only path the docs and quick-start example reach for.

---

### Finding 6 — Inline `v1=` field-presence check duplicates `signing::parse_v1_digest`

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:131-136`, `crates/outbox-publisher/src/webhook/signing.rs:90-97` |
| **Severity** | Low |
| **Category** | Idiom |

**Problem**

`WebhookVerifier::verify` re-implements `signing::parse_v1_digest`'s `header.split(',').any(...)` loop inline to detect a missing `v1=` field. Two side-by-side parsers means two places to keep in sync — and Finding 1's fix is going to land right here, so consolidating now keeps the eventual diff small.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:130-136
        // Reject if v1= field is missing before doing any HMAC work.
        if !signature_header
            .split(',')
            .any(|p| p.trim().starts_with("v1="))
        {
            return Err(VerifyError::MalformedHeader);
        }
```

```rust
// crates/outbox-publisher/src/webhook/signing.rs:90-97
fn parse_v1_digest(header_value: &str) -> Option<&str> {
    for part in header_value.split(',') {
        if let Some(hex) = part.trim().strip_prefix("v1=") {
            return Some(hex);
        }
    }
    None
}
```

**Recommended fix**

Promote `parse_v1_digest` to `pub(crate)` and use it in `mod.rs`. If Finding 1 is applied, the parse-and-decode helper supersedes this entirely:

```rust
// crates/outbox-publisher/src/webhook/signing.rs
pub(crate) fn parse_v1_digest(header_value: &str) -> Option<&str> { /* unchanged */ }
```

```rust
// crates/outbox-publisher/src/webhook/mod.rs
use signing::{parse_t_field, parse_v1_digest, verify};

// inside WebhookVerifier::verify
if parse_v1_digest(signature_header).is_none() {
    return Err(VerifyError::MalformedHeader);
}
```

**Why this fix**

One authoritative parser per field; the high-level verifier composes the helpers rather than reinventing them.

---

### Finding 7 — Clock-before-epoch silently accepts any timestamp

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:138-145`, `crates/outbox-publisher/src/webhook/signing.rs:56-62` |
| **Severity** | Low |
| **Category** | Security |

**Problem**

If the system clock is before `UNIX_EPOCH` (e.g. an unconfigured embedded device, a broken NTP sync, a container with a corrupted RTC), `SystemTime::now().duration_since(UNIX_EPOCH)` returns `Err`. The current code maps that to `now = 0`, after which `now.saturating_sub(ts) == 0` for any positive `ts` — so every signature inside the replay window is accepted regardless of how old it is. Combined with Finding 2, the verifier becomes effectively a no-op replay-wise when the clock is wrong.

It is a degenerate scenario, but the fail-open posture is the wrong default for a security primitive. The straightforward fix is fail-closed: if we can't tell what time it is, we can't tell whether the timestamp is fresh.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:138-145
let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|d| d.as_secs())
    .unwrap_or(0);

if now.saturating_sub(ts) > self.tolerance.as_secs() {
    return Err(VerifyError::TimestampOutOfTolerance);
}
```

**Recommended fix**

Return `VerifyError::TimestampOutOfTolerance` (or a new `ClockError` variant) when `duration_since` fails:

```rust
// crates/outbox-publisher/src/webhook/mod.rs
let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map_err(|_| VerifyError::TimestampOutOfTolerance)?
    .as_secs();

let tolerance_secs = self.tolerance.as_secs();
let drift = if now >= ts { now - ts } else { ts - now };
if drift > tolerance_secs {
    return Err(VerifyError::TimestampOutOfTolerance);
}
```

If a dedicated variant is preferred, name it `VerifyError::ClockUnavailable` for clarity. Either way, fail closed.

**Why this fix**

Defaulting to "verification passes when we can't compute the freshness window" is exactly the wrong direction for a security check. A receiver with a broken clock should reject signatures, not blindly accept them.

---

### Finding 8 — `WebhookEnvelope` deserialization is not tested with populated optional fields

| Field | Value |
|-------|-------|
| **File:Line** | `crates/outbox-publisher/src/webhook/mod.rs:386-405` |
| **Severity** | Low |
| **Category** | Testing / Cross-language |

**Problem**

The only `WebhookEnvelope`-shaped assertion is `verify_and_parse_happy_path`, which checks two fields (`kind`, `payload.email`) and otherwise sends `actor_id`, `correlation_id`, `causation_id` as `null`. None of the populated-Option paths are exercised, and `delivery_id`, `attempt`, `created_at`, `metadata`, `callback_name`, `mode` are never read in any test.

For Phase 4.4's cross-language interop test this is fine — the Java/dispatcher side will provide real values — but until then the contract that *"field names match TDD 04 §6.1 exactly"* and *"snake_case keys, ISO-8601 timestamps, UUIDs as strings"* (the file-level checklist) is unverified locally. A typo or missing `#[serde(rename = "...")]` would slip through every test in this PR.

**Context** (surrounding code as it exists today)

```rust
// crates/outbox-publisher/src/webhook/mod.rs:392-405
    #[test]
    fn verify_and_parse_happy_path() {
        let payload = json!({ "user_id": Uuid::new_v4(), "email": "a@example.com" });
        let body = serde_json::to_vec(&make_envelope_json(payload.clone())).unwrap();
        let ts = now_ts();
        let header = sign(SECRET, ts, &body);
        let verifier = WebhookVerifier::new(SECRET.to_vec());

        let envelope: WebhookEnvelope<UserRegistered> =
            verifier.verify_and_parse(&header, &body).unwrap();

        assert_eq!(envelope.kind, "user.registered@v1");
        assert_eq!(envelope.payload.email, "a@example.com");
    }
```

**Recommended fix**

Extend `make_envelope_json` to allow optional-field overrides (or write a second helper), then add two tests asserting every field round-trips:

```rust
// crates/outbox-publisher/src/webhook/mod.rs (new test)
#[test]
fn verify_and_parse_populates_all_envelope_fields() {
    let payload = json!({ "user_id": Uuid::new_v4(), "email": "a@example.com" });
    let actor = Uuid::new_v4();
    let corr = Uuid::new_v4();
    let cause = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    let aggregate_id = Uuid::new_v4();
    let created_at = Utc::now();

    let mut env = make_envelope_json(payload);
    env["actor_id"] = json!(actor);
    env["correlation_id"] = json!(corr);
    env["causation_id"] = json!(cause);
    env["event_id"] = json!(event_id);
    env["aggregate_id"] = json!(aggregate_id);
    env["created_at"] = json!(created_at);
    env["metadata"] = json!({"source": "test"});
    env["delivery_id"] = json!(42_i64);
    env["attempt"] = json!(3_i32);

    let body = serde_json::to_vec(&env).unwrap();
    let ts = now_ts();
    let header = sign(SECRET, ts, &body);
    let verifier = WebhookVerifier::new(SECRET.to_vec());

    let e: WebhookEnvelope<UserRegistered> =
        verifier.verify_and_parse(&header, &body).unwrap();

    assert_eq!(e.delivery_id, 42);
    assert_eq!(e.event_id, event_id);
    assert_eq!(e.kind, "user.registered@v1");
    assert_eq!(e.callback_name, "welcome_email");
    assert_eq!(e.mode, "managed");
    assert_eq!(e.aggregate_type, "user");
    assert_eq!(e.aggregate_id, aggregate_id);
    assert_eq!(e.actor_id, Some(actor));
    assert_eq!(e.correlation_id, Some(corr));
    assert_eq!(e.causation_id, Some(cause));
    assert_eq!(e.attempt, 3);
    assert_eq!(e.metadata, json!({"source": "test"}));
    // chrono round-trips to the same instant (allow µs precision loss)
    assert!((e.created_at - created_at).num_milliseconds().abs() < 2);
}
```

**Why this fix**

The cross-language contract is the load-bearing surface of this library; locally-verifiable end-to-end coverage of every envelope field reduces the risk that Phase 4.4 is the first place a serialization typo is discovered.

---

## Summary

| # | Title | File:Line | Severity | Category | Status | Notes |
|---|-------|-----------|----------|----------|--------|-------|
| 1 | Non-hex `v1=` digest reported as `InvalidSignature` instead of `MalformedHeader` | `crates/outbox-publisher/src/webhook/mod.rs:131-149` | Medium | Correctness | TODO | |
| 2 | Future-dated timestamps pass the tolerance check (one-sided drift comparison) | `crates/outbox-publisher/src/webhook/mod.rs:138-145` | Medium | Security | TODO | |
| 3 | Axum extractor and `WebhookRejection` are entirely untested | `crates/outbox-publisher/src/webhook/mod.rs:175-275` | Medium | Testing | TODO | |
| 4 | Body-extraction failure laundered into fabricated `serde_json::Error` | `crates/outbox-publisher/src/webhook/mod.rs:262-266` | Medium | Correctness / Idiom | TODO | |
| 5 | `signing::verify_header` is a redundant public API duplicating `WebhookVerifier::verify` | `crates/outbox-publisher/src/webhook/signing.rs:52-64` | Low | Idiom | TODO | |
| 6 | Inline `v1=` parsing in `mod.rs` duplicates `signing::parse_v1_digest` | `crates/outbox-publisher/src/webhook/mod.rs:131-136` | Low | Idiom | TODO | |
| 7 | Clock-before-epoch silently accepts any timestamp (fail-open) | `crates/outbox-publisher/src/webhook/mod.rs:138-145` | Low | Security | TODO | |
| 8 | `WebhookEnvelope` deserialization not tested with populated optional fields | `crates/outbox-publisher/src/webhook/mod.rs:386-405` | Low | Testing / Cross-language | TODO | |

> **Instructions for the implementing LLM:**
> - Change `TODO` to `DONE` once a finding is fully addressed.
> - If a finding is intentionally skipped or cannot be applied, change `TODO` to `SKIPPED` and add a short reason in the **Notes** column.
> - Do not delete rows — the table is the authoritative implementation log.
