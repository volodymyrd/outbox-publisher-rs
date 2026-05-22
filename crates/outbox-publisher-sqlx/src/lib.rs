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
/// # Schema
///
/// The target schema is `public` by default. Use [`SqlxPublisher::with_schema`]
/// to override.
pub struct SqlxPublisher {
    schema: String,
}

impl SqlxPublisher {
    /// Create a publisher that writes to the `public` schema.
    pub fn new() -> Self {
        Self {
            schema: "public".to_owned(),
        }
    }

    /// Override the Postgres schema (default: `"public"`).
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// The Postgres schema this publisher writes to.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

impl Default for SqlxPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl Publisher for SqlxPublisher {
    type Tx<'a> = Transaction<'a, Postgres>;

    fn append<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        event: &'b E,
        ctx: &'b EventContext,
    ) -> impl std::future::Future<Output = Result<EventId, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        let event_id = Uuid::new_v4();
        async move { self.insert(tx, event_id, event, ctx).await }
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
        let payload = serde_json::to_value(event)?;
        let metadata = serde_json::Value::Object(ctx.metadata().clone());
        let callbacks = serde_json::Value::Array(ctx.callbacks().to_vec());
        let table = format!("{}.outbox_events", self.schema);

        let result = sqlx::query(&format!(
            r#"
            INSERT INTO {table}
                (event_id, kind, aggregate_type, aggregate_id,
                 payload, metadata, callbacks,
                 actor_id, correlation_id, causation_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        ))
        .bind(event_id)
        .bind(E::kind())
        .bind(E::aggregate_type())
        .bind(event.aggregate_id())
        .bind(payload)
        .bind(metadata)
        .bind(callbacks)
        .bind(ctx.actor_id())
        .bind(ctx.correlation_id())
        .bind(ctx.causation_id())
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
            event_ids.push(event_id);
            kinds.push(E::kind());
            aggregate_types.push(E::aggregate_type());
            aggregate_ids.push(event.aggregate_id());
            payloads.push(payload);
            metadatas.push(serde_json::Value::Object(ctx.metadata().clone()));
            callbacks_list.push(serde_json::Value::Array(ctx.callbacks().to_vec()));
            actor_ids.push(ctx.actor_id());
            correlation_ids.push(ctx.correlation_id());
            causation_ids.push(ctx.causation_id());
        }

        let table = format!("{}.outbox_events", self.schema);

        let result = sqlx::query(&format!(
            r#"
            INSERT INTO {table}
                (event_id, kind, aggregate_type, aggregate_id,
                 payload, metadata, callbacks,
                 actor_id, correlation_id, causation_id)
            SELECT * FROM UNNEST(
                $1::uuid[], $2::text[], $3::text[], $4::uuid[],
                $5::jsonb[], $6::jsonb[], $7::jsonb[],
                $8::uuid[], $9::uuid[], $10::uuid[]
            )
            "#,
        ))
        .bind(&event_ids)
        .bind(&kinds)
        .bind(&aggregate_types)
        .bind(&aggregate_ids)
        .bind(&payloads)
        .bind(&metadatas)
        .bind(&callbacks_list)
        .bind(&actor_ids)
        .bind(&correlation_ids)
        .bind(&causation_ids)
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
