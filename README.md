# outbox-publisher-rs

Write domain events to a Postgres outbox table atomically with your business writes,
and verify HMAC-signed webhooks delivered by
[outbox-dispatcher](https://github.com/volodymyrd/outbox-dispatcher).

## Quick start

Add to `Cargo.toml`:

```toml
outbox-publisher = { version = "0.1", features = ["derive", "sqlx", "axum"] }
outbox-publisher-sqlx = { version = "0.1" }
```

### 1 — Define a typed event

```rust
use outbox_publisher::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(DomainEvent, Serialize, Deserialize, Clone)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: Uuid,
    pub email: String,
}
```

### 2 — Publish inside your transaction

```rust
use outbox_publisher::{event::EventContext, publisher::Publisher as _};
use outbox_publisher_sqlx::SqlxPublisher;
use serde_json::json;

let publisher = SqlxPublisher::new();
let mut tx = pool.begin().await?;

let event = UserRegistered { user_id: Uuid::new_v4(), email: "alice@example.com".into() };
let ctx = EventContext::default()
    .for_actor(event.user_id)
    .with_callbacks(vec![json!({
        "name": "welcome_email",
        "url": "https://your-app.example.com/hooks/welcome-email",
    })]);

publisher.append(&mut tx, &event, &ctx).await?;
tx.commit().await?;   // event and business writes commit together
```

### 3 — Receive the webhook (Axum)

```rust
use axum::{Router, http::StatusCode, routing::post};
use outbox_publisher::webhook::{WebhookVerifier, axum_support::OutboxWebhook};

async fn handle(OutboxWebhook(env): OutboxWebhook<UserRegistered>) -> StatusCode {
    println!("welcome email for {}", env.payload.email);
    StatusCode::OK
}

let verifier = WebhookVerifier::new(std::env::var("WEBHOOK_SECRET").unwrap());
let app = Router::new()
    .route("/hooks/welcome-email", post(handle))
    .with_state(/* AppState containing verifier */);
```

## Examples

Runnable examples are in the [`examples/`](examples/) directory:

| Example | Description |
|---|---|
| `axum-handler` | Register endpoint that publishes a `UserRegistered` event |
| `webhook-receiver` | Axum server that verifies and handles incoming webhooks |
| `batch-emit` | Publish multiple events in a single round-trip with `append_batch` |

Run with:

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5434/outbox_dispatcher \
  cargo run --example axum-handler -p outbox-publisher-examples
```

## Features

| Feature | Enables |
|---|---|
| `derive` | `#[derive(DomainEvent)]` proc-macro |
| `sqlx` | Re-exports for the SQLx adapter (use `outbox-publisher-sqlx` for the impl) |
| `axum` | `OutboxWebhook<E>` extractor and `WebhookRejection` |

## Contract

The publisher writes to `outbox_events` but never owns the schema.
The dispatcher's migration is the single source of truth. See
[the design document](../outbox/TDDs/05-outbox-publisher-tdd.md) for details on
the shared SQL schema, webhook signature format, and cross-language interoperability.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
