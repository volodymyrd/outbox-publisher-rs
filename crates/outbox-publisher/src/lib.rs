//! Write domain events to a Postgres outbox table atomically with your business
//! writes, and verify HMAC-signed webhooks delivered by the
//! [outbox-dispatcher](https://github.com/volodymyrd/outbox-dispatcher).
//!
//! # Quick start
//!
//! Add the dependency:
//!
//! ```toml
//! outbox-publisher = { version = "0.1", features = ["derive", "axum"] }
//! outbox-publisher-sqlx = { version = "0.1" }
//! ```
//!
//! Define an event, publish it, and receive the webhook:
//!
//! ```rust
//! use outbox_publisher::{DomainEvent, event::EventContext};
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//! use uuid::Uuid;
//!
//! // 1. Define a typed event.
//! #[derive(DomainEvent, Serialize, Deserialize, Clone)]
//! #[event(kind = "user.registered@v1", aggregate = "user")]
//! pub struct UserRegistered {
//!     #[event(aggregate_id)]
//!     pub user_id: Uuid,
//!     pub email: String,
//! }
//!
//! // 2. Publish inside your transaction (requires sqlx feature + SqlxPublisher):
//! //
//! //   state.publisher.append(&mut tx, &event, &ctx).await?;
//! //   tx.commit().await?;
//! //
//! // 3. Receive the webhook (requires axum feature):
//! //
//! //   async fn handler(OutboxWebhook(env): OutboxWebhook<UserRegistered>) -> StatusCode {
//! //       println!("got event for {}", env.payload.email);
//! //       StatusCode::OK
//! //   }
//! ```
//!
//! See the `examples/` directory for runnable end-to-end examples.

#![deny(missing_docs)]

/// The [`DomainEvent`] trait.
pub mod domain_event;
/// Error types: [`PublishError`] and [`VerifyError`].
pub mod error;
/// [`EventContext`] and [`EventId`].
pub mod event;
/// The [`Publisher`] trait.
pub mod publisher;
/// Webhook verification helpers: [`WebhookVerifier`] and [`WebhookEnvelope`].
pub mod webhook;

pub use domain_event::DomainEvent;
pub use error::{PublishError, VerifyError};
pub use event::{EventContext, EventId};
pub use publisher::Publisher;
pub use webhook::{WebhookEnvelope, WebhookVerifier};

#[cfg(feature = "derive")]
pub use outbox_publisher_derive::DomainEvent;
