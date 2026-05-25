//! Batch-emit example — publish multiple events in a single round-trip using
//! `append_batch`.
//!
//! Run against a local Postgres:
//!
//! ```text
//! DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher \
//!   cargo run --example batch-emit -p outbox-publisher-examples
//! ```

use anyhow::{Context, Result};
use outbox_publisher::{DomainEvent, EventContext, Publisher as _};
use outbox_publisher_sqlx::SqlxPublisher;
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

// ── Event definitions ─────────────────────────────────────────────────────────

#[derive(DomainEvent, Serialize, Clone)]
#[event(kind = "order.placed@v1", aggregate = "order")]
struct OrderPlaced {
    #[event(aggregate_id)]
    order_id: Uuid,
    customer_id: Uuid,
    total_cents: i64,
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
        .unwrap_or_else(|_| "http://localhost:4000/hooks/order-placed".to_owned());

    let pool = PgPool::connect(&database_url)
        .await
        .context("failed to connect to Postgres")?;

    let publisher = SqlxPublisher::new();

    // Build a batch of events — one per simulated order.
    let customer_id = Uuid::new_v4();
    let events: Vec<(OrderPlaced, EventContext)> = (1..=5_i64)
        .map(|i| {
            let order_id = Uuid::new_v4();
            let event = OrderPlaced {
                order_id,
                customer_id,
                total_cents: i * 1000,
            };
            let ctx = EventContext::default()
                .for_actor(customer_id)
                .with_callbacks(vec![json!({
                    "name": "order_confirmation",
                    "url": webhook_url,
                })]);
            (event, ctx)
        })
        .collect();

    let mut tx = pool.begin().await.context("failed to begin transaction")?;

    let ids = publisher
        .append_batch(&mut tx, &events)
        .await
        .context("append_batch failed")?;

    tx.commit().await.context("failed to commit transaction")?;

    tracing::info!(
        count = ids.len(),
        customer_id = %customer_id,
        "batch published",
    );

    for (i, id) in ids.iter().enumerate() {
        tracing::info!(index = i, event_id = %id, "event written");
    }

    Ok(())
}
