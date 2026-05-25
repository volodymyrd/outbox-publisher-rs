//! Webhook receiver example — verify an incoming dispatcher signature and
//! handle the typed event.
//!
//! Run against a local dispatcher:
//!
//! ```text
//! WEBHOOK_HMAC_SECRET=my-secret \
//!   cargo run --example webhook-receiver -p outbox-publisher-examples
//! ```
//!
//! The dispatcher should be configured to POST `user.registered@v1` events
//! to `http://localhost:4000/hooks/welcome-email`.

use anyhow::{Context, Result};
use axum::{extract::FromRef, http::StatusCode, routing::post, Router};
use outbox_publisher::{OutboxWebhook, WebhookVerifier};
use serde::Deserialize;
use uuid::Uuid;

// ── Event payload ─────────────────────────────────────────────────────────────

#[derive(Deserialize, Debug)]
struct UserRegistered {
    user_id: Uuid,
    email: String,
}

// ── Application state ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    verifier: WebhookVerifier,
}

impl FromRef<AppState> for WebhookVerifier {
    fn from_ref(state: &AppState) -> Self {
        state.verifier.clone()
    }
}

// ── Handler ───────────────────────────────────────────────────────────────────

async fn welcome_email(OutboxWebhook(env): OutboxWebhook<UserRegistered>) -> StatusCode {
    if env.attempt > 1 {
        tracing::info!(attempt = env.attempt, "retrying delivery");
    }

    // Idempotency key: env.event_id (UUID of the outbox row).
    tracing::info!(
        event_id = %env.event_id,
        user_id  = %env.payload.user_id,
        email    = %env.payload.email,
        attempt  = env.attempt,
        "sending welcome email",
    );

    // In a real application you would call your email service here and return
    // an appropriate status:
    //   2xx → success (dispatcher marks delivery done)
    //   4xx → permanent failure (dispatcher stops retrying)
    //   5xx → transient failure (dispatcher will retry)
    StatusCode::OK
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let secret = std::env::var("WEBHOOK_HMAC_SECRET")
        .context("WEBHOOK_HMAC_SECRET environment variable required")?;

    let state = AppState {
        verifier: WebhookVerifier::new(secret.into_bytes()),
    };

    let app = Router::new()
        .route("/hooks/welcome-email", post(welcome_email))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000")
        .await
        .context("failed to bind listener")?;

    tracing::info!("webhook receiver listening on http://0.0.0.0:4000");
    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}
