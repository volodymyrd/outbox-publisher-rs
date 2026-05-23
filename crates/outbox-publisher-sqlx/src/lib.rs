//! SQLx Postgres adapter for `outbox-publisher`.
//!
//! Provides [`SqlxPublisher`], which implements [`Publisher`] with
//! `Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>`.
//!
//! # Usage
//!
//! ```no_run
//! use outbox_publisher_sqlx::SqlxPublisher;
//! use outbox_publisher::publisher::Publisher;
//! use outbox_publisher::event::EventContext;
//! use serde_json::json;
//! # use serde::Serialize;
//! # use outbox_publisher::domain_event::DomainEvent;
//! # use uuid::Uuid;
//! # #[derive(Serialize)]
//! # struct MyEvent { id: Uuid }
//! # impl DomainEvent for MyEvent {
//! #     fn kind() -> &'static str where Self: Sized { "my.event@v1" }
//! #     fn aggregate_type() -> &'static str where Self: Sized { "my" }
//! #     fn aggregate_id(&self) -> Uuid { self.id }
//! # }
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let publisher = SqlxPublisher::new();
//! let mut tx = pool.begin().await?;
//!
//! let event = MyEvent { id: Uuid::new_v4() };
//! let ctx = EventContext::default()
//!     .with_callbacks(vec![json!({"name": "notify", "url": "https://example.com/hook"})]);
//!
//! let event_id = publisher.append(&mut tx, &event, &ctx).await?;
//! tx.commit().await?;
//! # Ok(())
//! # }
//! ```
#![deny(missing_docs)]

use outbox_publisher::{
    domain_event::DomainEvent,
    error::PublishError,
    event::{EventContext, EventId},
    publisher::Publisher,
};
use serde::Serialize;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// SQLx Postgres implementation of [`Publisher`].
///
/// Writes events to `outbox_events` using the caller's transaction. Never
/// commits or rolls back — transaction lifecycle is the caller's responsibility.
///
/// The target schema is determined by the connection's `search_path`. To write
/// to a non-default schema, set `search_path` on the pool or connection before
/// passing the transaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct SqlxPublisher;

impl SqlxPublisher {
    /// Create a new publisher.
    pub fn new() -> Self {
        Self
    }
}

impl Publisher for SqlxPublisher {
    type Tx<'a> = Transaction<'a, Postgres>;

    async fn append<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        event: &'b E,
        ctx: &'b EventContext,
    ) -> Result<EventId, PublishError>
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        let event_id = Uuid::new_v4();
        self.insert(tx, event_id, event, ctx).await
    }

    async fn append_with_id<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        event_id: EventId,
        event: &'b E,
        ctx: &'b EventContext,
    ) -> Result<EventId, PublishError>
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        self.insert(tx, event_id.into(), event, ctx).await
    }

    async fn append_batch<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        events: &'b [(E, EventContext)],
    ) -> Result<Vec<EventId>, PublishError>
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        self.insert_batch(tx, events).await
    }
}

/// Returns `Err(PublishError::MissingCallbacks)` when `ctx` carries no callbacks.
fn validate_callbacks(ctx: &EventContext) -> Result<(), PublishError> {
    if ctx.callbacks().is_empty() {
        return Err(PublishError::MissingCallbacks);
    }
    Ok(())
}

impl SqlxPublisher {
    /// Core single-row INSERT shared by `append` and `append_with_id`.
    async fn insert<E>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event_id: Uuid,
        event: &E,
        ctx: &EventContext,
    ) -> Result<EventId, PublishError>
    where
        E: DomainEvent + Serialize,
    {
        validate_callbacks(ctx)?;

        let payload = serde_json::to_value(event)?;
        let metadata = serde_json::to_value(ctx.metadata())?;
        let callbacks = serde_json::to_value(ctx.callbacks())?;

        let result = sqlx::query!(
            r#"
            INSERT INTO outbox_events
                (event_id, kind, aggregate_type, aggregate_id,
                 payload, metadata, callbacks,
                 actor_id, correlation_id, causation_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            event_id,
            E::kind(),
            E::aggregate_type(),
            event.aggregate_id(),
            payload,
            metadata,
            callbacks,
            ctx.actor_id(),
            ctx.correlation_id(),
            ctx.causation_id(),
        )
        .execute(&mut **tx)
        .await;

        match result {
            Ok(_) => Ok(EventId::from(event_id)),
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                Err(PublishError::DuplicateEventId)
            }
            Err(e) => Err(PublishError::Database(Box::new(e))),
        }
    }

    /// Batch INSERT using UNNEST for a single round-trip.
    async fn insert_batch<E>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        events: &[(E, EventContext)],
    ) -> Result<Vec<EventId>, PublishError>
    where
        E: DomainEvent + Serialize,
    {
        if events.is_empty() {
            return Ok(vec![]);
        }

        for (_, ctx) in events {
            validate_callbacks(ctx)?;
        }

        let len = events.len();
        let mut event_ids: Vec<Uuid> = Vec::with_capacity(len);
        let mut kinds: Vec<&str> = Vec::with_capacity(len);
        let mut aggregate_types: Vec<&str> = Vec::with_capacity(len);
        let mut aggregate_ids: Vec<Uuid> = Vec::with_capacity(len);
        let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(len);
        let mut metadatas: Vec<serde_json::Value> = Vec::with_capacity(len);
        let mut callbacks_list: Vec<serde_json::Value> = Vec::with_capacity(len);
        let mut actor_ids: Vec<Option<Uuid>> = Vec::with_capacity(len);
        let mut correlation_ids: Vec<Option<Uuid>> = Vec::with_capacity(len);
        let mut causation_ids: Vec<Option<Uuid>> = Vec::with_capacity(len);

        for (event, ctx) in events {
            let event_id = Uuid::new_v4();
            let payload = serde_json::to_value(event)?;
            let metadata = serde_json::to_value(ctx.metadata())?;
            let callbacks = serde_json::to_value(ctx.callbacks())?;
            event_ids.push(event_id);
            kinds.push(E::kind());
            aggregate_types.push(E::aggregate_type());
            aggregate_ids.push(event.aggregate_id());
            payloads.push(payload);
            metadatas.push(metadata);
            callbacks_list.push(callbacks);
            actor_ids.push(ctx.actor_id());
            correlation_ids.push(ctx.correlation_id());
            causation_ids.push(ctx.causation_id());
        }

        let result = sqlx::query!(
            r#"
            INSERT INTO outbox_events
                (event_id, kind, aggregate_type, aggregate_id,
                 payload, metadata, callbacks,
                 actor_id, correlation_id, causation_id)
            SELECT * FROM UNNEST(
                $1::uuid[], $2::text[], $3::text[], $4::uuid[],
                $5::jsonb[], $6::jsonb[], $7::jsonb[],
                $8::uuid[], $9::uuid[], $10::uuid[]
            )
            "#,
            &event_ids as &[Uuid],
            &kinds as &[&str],
            &aggregate_types as &[&str],
            &aggregate_ids as &[Uuid],
            &payloads as &[serde_json::Value],
            &metadatas as &[serde_json::Value],
            &callbacks_list as &[serde_json::Value],
            &actor_ids as &[Option<Uuid>],
            &correlation_ids as &[Option<Uuid>],
            &causation_ids as &[Option<Uuid>],
        )
        .execute(&mut **tx)
        .await;

        match result {
            Ok(_) => Ok(event_ids.into_iter().map(EventId::from).collect()),
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                Err(PublishError::DuplicateEventId)
            }
            Err(e) => Err(PublishError::Database(Box::new(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `PublishError::Serialization` is produced when `serde_json::to_value` fails.
    ///
    /// This exercises the `From<serde_json::Error>` conversion that backs the `?`
    /// in `insert` — no database connection required.
    #[test]
    fn serialization_error_converts_to_publish_error() {
        use serde::ser::Error as _;

        struct Boom;

        impl serde::Serialize for Boom {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(S::Error::custom("boom"))
            }
        }

        let err = serde_json::to_value(Boom).unwrap_err();
        let publish_err = PublishError::from(err);
        assert!(
            matches!(publish_err, PublishError::Serialization(_)),
            "expected Serialization, got {publish_err:?}",
        );
    }

    #[test]
    fn validate_callbacks_rejects_empty() {
        let ctx = EventContext::default();
        let err = validate_callbacks(&ctx).expect_err("expected MissingCallbacks");
        assert!(
            matches!(err, PublishError::MissingCallbacks),
            "expected MissingCallbacks, got {err:?}",
        );
    }

    #[test]
    fn validate_callbacks_accepts_non_empty() {
        let ctx = EventContext::default()
            .with_callbacks(vec![serde_json::json!({"name": "n", "url": "u"})]);
        assert!(validate_callbacks(&ctx).is_ok());
    }
}
