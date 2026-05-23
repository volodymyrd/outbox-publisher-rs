//! Webhook verification helpers for receivers of dispatcher-signed events.
//!
//! # Quick start
//!
//! ```no_run
//! use outbox_publisher::webhook::{WebhookVerifier, WebhookEnvelope};
//! use serde::Deserialize;
//! use uuid::Uuid;
//!
//! #[derive(Deserialize)]
//! struct UserRegistered { user_id: Uuid, email: String }
//!
//! # fn example(header: &str, body: &[u8]) -> Result<(), outbox_publisher::error::VerifyError> {
//! let verifier = WebhookVerifier::new("my-secret");
//! let envelope: WebhookEnvelope<UserRegistered> =
//!     verifier.verify_and_parse(header, body)?;
//! println!("user: {}", envelope.payload.email);
//! # Ok(())
//! # }
//! ```

pub mod signing;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize};
use uuid::Uuid;

use crate::error::VerifyError;
use signing::{parse_t_field, parse_v1_decoded, verify_decoded};

// ── WebhookEnvelope ───────────────────────────────────────────────────────────

/// The JSON body the dispatcher `POST`s to a webhook endpoint.
///
/// Field names and types are byte-identical to the dispatcher's `build_body`
/// (§6.1 of the dispatcher TDD). Generic over `E` — the event payload type
/// stored in [`payload`][WebhookEnvelope::payload].
#[derive(Debug, Clone, Deserialize)]
pub struct WebhookEnvelope<E> {
    /// Dispatcher-internal delivery row identifier.
    pub delivery_id: i64,
    /// Event identifier (UUID of the outbox row).
    pub event_id: Uuid,
    /// Stable, versioned event kind string (e.g. `"user.registered@v1"`).
    pub kind: String,
    /// Name of the callback target that received this delivery.
    pub callback_name: String,
    /// Delivery mode (`"managed"` or `"external"`).
    pub mode: String,
    /// Aggregate type (e.g. `"user"`).
    pub aggregate_type: String,
    /// Aggregate instance identifier.
    pub aggregate_id: Uuid,
    /// Typed event payload — deserialized from the `payload` JSON field.
    pub payload: E,
    /// Arbitrary structured metadata from the publisher's `EventContext`.
    pub metadata: serde_json::Value,
    /// Actor that triggered the event, if set.
    pub actor_id: Option<Uuid>,
    /// Correlation identifier, if set.
    pub correlation_id: Option<Uuid>,
    /// Causation identifier, if set.
    pub causation_id: Option<Uuid>,
    /// When the outbox row was created.
    pub created_at: DateTime<Utc>,
    /// 1-based delivery attempt counter.
    pub attempt: i32,
}

// ── WebhookVerifier ───────────────────────────────────────────────────────────

/// Verifies HMAC-SHA256 signatures on dispatcher-signed webhook requests.
///
/// Create once (e.g. as application state) and share across requests.
///
/// The secret is held in memory only for verification; it is never written
/// to logs or `Debug` output.
#[derive(Clone)]
pub struct WebhookVerifier {
    secret: Vec<u8>,
    tolerance: Duration,
}

impl std::fmt::Debug for WebhookVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookVerifier")
            .field("tolerance", &self.tolerance)
            .field("secret", &"[redacted]")
            .finish()
    }
}

impl WebhookVerifier {
    /// Create a verifier with a 5-minute replay-window tolerance.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            tolerance: Duration::from_secs(300),
        }
    }

    /// Override the replay-window tolerance (default 5 minutes).
    pub fn with_tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Verify the `X-Outbox-Signature` header against the raw request body.
    ///
    /// Returns `Ok(())` when:
    /// 1. The header contains both `t=` and `v1=` fields.
    /// 2. The timestamp is within the configured tolerance window (both past and future).
    /// 3. The HMAC-SHA256 digest matches in constant time.
    ///
    /// # Errors
    ///
    /// - [`VerifyError::MissingHeader`] — `signature_header` is empty.
    /// - [`VerifyError::MalformedHeader`] — missing `t=` or `v1=` field, or non-hex digest.
    /// - [`VerifyError::TimestampOutOfTolerance`] — timestamp is outside the replay window,
    ///   or the system clock is before the Unix epoch.
    /// - [`VerifyError::InvalidSignature`] — HMAC digest does not match.
    pub fn verify(&self, signature_header: &str, body: &[u8]) -> Result<(), VerifyError> {
        if signature_header.is_empty() {
            return Err(VerifyError::MissingHeader);
        }

        let ts = parse_t_field(signature_header).ok_or(VerifyError::MalformedHeader)?;

        // Parse and hex-decode the v1= digest; distinguish a missing/malformed field
        // from an HMAC mismatch so the caller gets the correct error variant.
        let decoded = parse_v1_decoded(signature_header).ok_or(VerifyError::MalformedHeader)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| VerifyError::TimestampOutOfTolerance)?
            .as_secs();

        // Two-sided drift: reject timestamps both too far in the past *and* the future.
        let tolerance_secs = self.tolerance.as_secs();
        if now.abs_diff(ts) > tolerance_secs {
            return Err(VerifyError::TimestampOutOfTolerance);
        }

        if !verify_decoded(&self.secret, ts, body, &decoded) {
            return Err(VerifyError::InvalidSignature);
        }

        Ok(())
    }

    /// Verify the signature and deserialize the body into a [`WebhookEnvelope<E>`].
    ///
    /// Combines [`verify`][Self::verify] with JSON deserialization in one step.
    ///
    /// # Errors
    ///
    /// All errors from [`verify`][Self::verify], plus
    /// [`VerifyError::BodyParse`] when the JSON cannot be deserialized.
    pub fn verify_and_parse<E: DeserializeOwned>(
        &self,
        signature_header: &str,
        body: &[u8],
    ) -> Result<WebhookEnvelope<E>, VerifyError> {
        self.verify(signature_header, body)?;
        let envelope: WebhookEnvelope<E> = serde_json::from_slice(body)?;
        Ok(envelope)
    }
}

// ── Axum extractor ────────────────────────────────────────────────────────────

#[cfg(feature = "axum")]
pub mod axum_support {
    use axum::{
        body::Bytes,
        extract::{FromRef, FromRequest, Request},
        http::StatusCode,
        response::{IntoResponse, Response},
    };
    use serde::de::DeserializeOwned;
    use tracing::warn;

    use super::{WebhookEnvelope, WebhookVerifier};
    use crate::error::VerifyError;

    /// Axum extractor that verifies the `X-Outbox-Signature` header and
    /// deserialises the request body into `WebhookEnvelope<E>`.
    ///
    /// Requires `WebhookVerifier` to be accessible via [`FromRef`] on the
    /// application state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use axum::{Router, routing::post, extract::State};
    /// use outbox_publisher::webhook::{WebhookVerifier, WebhookEnvelope};
    /// use outbox_publisher::webhook::axum_support::OutboxWebhook;
    /// use serde::Deserialize;
    /// use uuid::Uuid;
    ///
    /// #[derive(Clone)]
    /// struct AppState { verifier: WebhookVerifier }
    ///
    /// impl axum::extract::FromRef<AppState> for WebhookVerifier {
    ///     fn from_ref(state: &AppState) -> Self {
    ///         state.verifier.clone()
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct UserRegistered { email: String }
    ///
    /// async fn handle(
    ///     OutboxWebhook(env): OutboxWebhook<UserRegistered>,
    /// ) -> axum::http::StatusCode {
    ///     println!("got event for {}", env.payload.email);
    ///     axum::http::StatusCode::OK
    /// }
    /// ```
    pub struct OutboxWebhook<E>(pub WebhookEnvelope<E>);

    /// Rejection returned when signature verification or body parsing fails.
    pub struct WebhookRejection(pub(crate) VerifyError);

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
                VerifyError::BodyRead(_) => StatusCode::BAD_REQUEST,
            };
            warn!(error = %self.0, "webhook verification failed");
            (status, self.0.to_string()).into_response()
        }
    }

    impl<S, E> FromRequest<S> for OutboxWebhook<E>
    where
        S: Send + Sync,
        WebhookVerifier: FromRef<S>,
        E: DeserializeOwned,
    {
        type Rejection = WebhookRejection;

        async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
            let verifier = WebhookVerifier::from_ref(state);

            let signature = req
                .headers()
                .get("x-outbox-signature")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();

            let body = Bytes::from_request(req, state)
                .await
                .map_err(|e| WebhookRejection(VerifyError::BodyRead(e.to_string())))?;

            let envelope = verifier
                .verify_and_parse::<E>(&signature, &body)
                .map_err(WebhookRejection)?;

            Ok(OutboxWebhook(envelope))
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use signing::sign;

    const SECRET: &[u8] = b"test-secret-key-minimum-32-bytes!!";

    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn make_envelope_json(payload: serde_json::Value) -> serde_json::Value {
        json!({
            "delivery_id": 1_i64,
            "event_id": Uuid::new_v4(),
            "kind": "user.registered@v1",
            "callback_name": "welcome_email",
            "mode": "managed",
            "aggregate_type": "user",
            "aggregate_id": Uuid::new_v4(),
            "payload": payload,
            "metadata": {},
            "actor_id": null,
            "correlation_id": null,
            "causation_id": null,
            "created_at": Utc::now(),
            "attempt": 1_i32,
        })
    }

    // ── WebhookVerifier::verify ───────────────────────────────────────────────

    #[test]
    fn verify_accepts_valid_signature() {
        let body = b"{\"hello\":\"world\"}";
        let ts = now_ts();
        let header = sign(SECRET, ts, body);
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        assert!(verifier.verify(&header, body).is_ok());
    }

    #[test]
    fn verify_rejects_empty_header() {
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        let err = verifier.verify("", b"body").unwrap_err();
        assert!(matches!(err, VerifyError::MissingHeader), "{err:?}");
    }

    #[test]
    fn verify_rejects_missing_t_field() {
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        let err = verifier.verify("v1=deadbeef", b"body").unwrap_err();
        assert!(matches!(err, VerifyError::MalformedHeader), "{err:?}");
    }

    #[test]
    fn verify_rejects_missing_v1_field() {
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        let err = verifier.verify("t=12345", b"body").unwrap_err();
        assert!(matches!(err, VerifyError::MalformedHeader), "{err:?}");
    }

    // Finding 1: non-hex v1= must be MalformedHeader, not InvalidSignature.
    #[test]
    fn verify_rejects_non_hex_v1_as_malformed_header() {
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        let header = format!("t={},v1=not-hex!!", now_ts());
        let err = verifier.verify(&header, b"body").unwrap_err();
        assert!(matches!(err, VerifyError::MalformedHeader), "{err:?}");
    }

    #[test]
    fn verify_rejects_expired_timestamp() {
        let body = b"body";
        let old_ts = now_ts().saturating_sub(600);
        let header = sign(SECRET, old_ts, body);
        let verifier =
            WebhookVerifier::new(SECRET.to_vec()).with_tolerance(Duration::from_secs(300));
        let err = verifier.verify(&header, body).unwrap_err();
        assert!(
            matches!(err, VerifyError::TimestampOutOfTolerance),
            "{err:?}"
        );
    }

    // Finding 2: future-dated timestamps beyond tolerance must be rejected.
    #[test]
    fn verify_rejects_future_timestamp_outside_tolerance() {
        let body = b"body";
        let future_ts = now_ts() + 3_600; // 1 hour in the future
        let header = sign(SECRET, future_ts, body);
        let verifier =
            WebhookVerifier::new(SECRET.to_vec()).with_tolerance(Duration::from_secs(300));
        let err = verifier.verify(&header, body).unwrap_err();
        assert!(
            matches!(err, VerifyError::TimestampOutOfTolerance),
            "{err:?}"
        );
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let body = b"body";
        let ts = now_ts();
        let header = sign(SECRET, ts, body);
        let verifier = WebhookVerifier::new(b"different-secret".to_vec());
        let err = verifier.verify(&header, body).unwrap_err();
        assert!(matches!(err, VerifyError::InvalidSignature), "{err:?}");
    }

    #[test]
    fn verify_rejects_tampered_body() {
        let body = b"original";
        let ts = now_ts();
        let header = sign(SECRET, ts, body);
        let verifier = WebhookVerifier::new(SECRET.to_vec());
        let err = verifier.verify(&header, b"tampered").unwrap_err();
        assert!(matches!(err, VerifyError::InvalidSignature), "{err:?}");
    }

    // ── WebhookVerifier::verify_and_parse ─────────────────────────────────────

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct UserRegistered {
        user_id: Uuid,
        email: String,
    }

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

    // Finding 8: all envelope fields round-trip correctly.
    #[test]
    fn verify_and_parse_populates_all_envelope_fields() {
        let user_id = Uuid::new_v4();
        let payload = json!({ "user_id": user_id, "email": "b@example.com" });
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

        let e: WebhookEnvelope<UserRegistered> = verifier.verify_and_parse(&header, &body).unwrap();

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
        // Allow millisecond-level precision loss in chrono round-trip.
        assert!((e.created_at - created_at).num_milliseconds().abs() < 2);
    }

    #[test]
    fn verify_and_parse_returns_body_parse_on_bad_json() {
        let ts = now_ts();
        let body = b"not-json";
        let header = sign(SECRET, ts, body);
        let verifier = WebhookVerifier::new(SECRET.to_vec());

        let err = verifier
            .verify_and_parse::<UserRegistered>(&header, body)
            .unwrap_err();
        assert!(matches!(err, VerifyError::BodyParse(_)), "{err:?}");
    }

    // ── WebhookVerifier Debug ─────────────────────────────────────────────────

    #[test]
    fn debug_output_redacts_secret() {
        let verifier = WebhookVerifier::new(b"my-super-secret".to_vec());
        let debug = format!("{verifier:?}");
        assert!(
            debug.contains("[redacted]"),
            "secret must not appear: {debug}"
        );
        assert!(!debug.contains("my-super-secret"));
    }

    // ── with_tolerance builder ────────────────────────────────────────────────

    #[test]
    fn with_tolerance_overrides_default() {
        let body = b"body";
        // Timestamp 90 seconds old — valid within 2-minute window, rejected within 1-minute.
        let ts = now_ts().saturating_sub(90);
        let header = sign(SECRET, ts, body);

        let verifier_2min =
            WebhookVerifier::new(SECRET.to_vec()).with_tolerance(Duration::from_secs(120));
        assert!(verifier_2min.verify(&header, body).is_ok());

        let verifier_1min =
            WebhookVerifier::new(SECRET.to_vec()).with_tolerance(Duration::from_secs(60));
        let err = verifier_1min.verify(&header, body).unwrap_err();
        assert!(matches!(err, VerifyError::TimestampOutOfTolerance));
    }
}

// ── Axum extractor tests ──────────────────────────────────────────────────────

#[cfg(all(test, feature = "axum"))]
mod axum_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::{
        body::Body,
        extract::FromRef,
        http::{Request, StatusCode},
        routing::post,
        Router,
    };
    use serde::Deserialize;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::axum_support::OutboxWebhook;
    use super::*;
    use signing::sign;

    const SECRET: &[u8] = b"test-secret-key-minimum-32-bytes!!";

    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[derive(Clone)]
    struct AppState {
        verifier: WebhookVerifier,
    }

    impl FromRef<AppState> for WebhookVerifier {
        fn from_ref(s: &AppState) -> Self {
            s.verifier.clone()
        }
    }

    #[derive(Deserialize)]
    struct Empty {}

    async fn handler(OutboxWebhook(_): OutboxWebhook<Empty>) -> StatusCode {
        StatusCode::OK
    }

    fn app(secret: &[u8]) -> Router {
        Router::new()
            .route("/hook", post(handler))
            .with_state(AppState {
                verifier: WebhookVerifier::new(secret.to_vec()),
            })
    }

    fn envelope_bytes() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "delivery_id": 1_i64,
            "event_id": Uuid::new_v4(),
            "kind": "test.event@v1",
            "callback_name": "test",
            "mode": "managed",
            "aggregate_type": "test",
            "aggregate_id": Uuid::new_v4(),
            "payload": {},
            "metadata": {},
            "actor_id": null,
            "correlation_id": null,
            "causation_id": null,
            "created_at": chrono::Utc::now(),
            "attempt": 1_i32,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn extractor_accepts_valid_signature() {
        let body = envelope_bytes();
        let ts = now_ts();
        let header = sign(SECRET, ts, &body);
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .header("x-outbox-signature", header)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn extractor_returns_401_for_bad_signature() {
        let body = envelope_bytes();
        let ts = now_ts();
        let header = sign(b"wrong-secret", ts, &body);
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .header("x-outbox-signature", header)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn extractor_returns_400_for_missing_header() {
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .body(Body::from(envelope_bytes()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extractor_returns_400_for_malformed_header() {
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .header("x-outbox-signature", format!("t={},v1=not-hex!!", now_ts()))
                    .body(Body::from(envelope_bytes()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn extractor_returns_401_for_expired_timestamp() {
        let body = envelope_bytes();
        let old_ts = now_ts().saturating_sub(3_600);
        let header = sign(SECRET, old_ts, &body);
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .header("x-outbox-signature", header)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn extractor_returns_422_for_invalid_json_body() {
        let body = b"not-json".to_vec();
        let ts = now_ts();
        let header = sign(SECRET, ts, &body);
        let resp = app(SECRET)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hook")
                    .header("x-outbox-signature", header)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
