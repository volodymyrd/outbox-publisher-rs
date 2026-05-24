//! Axum handler example — publish an event atomically with a business write.
//!
//! Run against a local Postgres + dispatcher:
//!
//! ```text
//! DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher \
//!   cargo run --example axum-handler -p outbox-publisher-examples
//! ```
//!
//! Then POST a registration request:
//!
//! ```text
//! curl -s -X POST http://localhost:3000/users/register \
//!   -H 'Content-Type: application/json' \
//!   -d '{"email":"alice@example.com"}'
//! ```

use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use outbox_publisher::{event::EventContext, DomainEvent};
use outbox_publisher_sqlx::SqlxPublisher;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

// ── Event definition ──────────────────────────────────────────────────────────

#[derive(DomainEvent, Serialize, Clone)]
#[event(kind = "user.registered@v1", aggregate = "user")]
struct UserRegistered {
    #[event(aggregate_id)]
    user_id: Uuid,
    email: String,
}

// ── Application state ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    publisher: SqlxPublisher,
    webhook_url: Arc<str>,
}

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct RegisterRequest {
    email: String,
}

#[derive(Serialize)]
struct RegisterResponse {
    user_id: Uuid,
}

// ── Handler ───────────────────────────────────────────────────────────────────

async fn register(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, StatusCode> {
    use outbox_publisher::publisher::Publisher as _;

    let user_id = Uuid::new_v4();

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let event = UserRegistered {
        user_id,
        email: req.email.clone(),
    };

    let ctx = EventContext::default()
        .for_actor(user_id)
        .with_callbacks(vec![json!({
            "name": "welcome_email",
            "url": state.webhook_url,
        })]);

    state
        .publisher
        .append(&mut tx, &event, &ctx)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to append event");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    tx.commit()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tracing::info!(user_id = %user_id, email = %req.email, "user registered");

    Ok(Json(RegisterResponse { user_id }))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable required")?;

    let webhook_url = std::env::var("WEBHOOK_URL")
        .unwrap_or_else(|_| "http://localhost:4000/hooks/welcome-email".to_owned());

    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;

    let state = AppState {
        pool,
        publisher: SqlxPublisher::new(),
        webhook_url: Arc::from(webhook_url.as_str()),
    };

    let app = Router::new()
        .route("/users/register", post(register))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .context("failed to bind listener")?;

    tracing::info!("listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}
