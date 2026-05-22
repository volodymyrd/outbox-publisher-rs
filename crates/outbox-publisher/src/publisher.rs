use std::future::Future;

use crate::{
    domain_event::DomainEvent,
    error::PublishError,
    event::{EventContext, EventId},
};
use serde::Serialize;

/// Writes domain events into the outbox table inside a caller-controlled transaction.
///
/// The trait is generic over `Tx<'a>` — an associated GAT that abstracts the
/// underlying database driver. Callers pass their own transaction handle; the
/// publisher **never** commits or rolls back — that remains the caller's
/// responsibility.
///
/// The `SqlxPublisher` in `outbox-publisher-sqlx` binds `Tx<'a>` to
/// `sqlx::Transaction<'a, sqlx::Postgres>`.
///
/// # Note on mocking
///
/// `mockall`'s `#[automock]` does not support GAT-bearing traits (`type Tx<'a>`).
/// Use a hand-rolled mock (see `tests/publisher_mock_test.rs`) when unit-testing
/// code that depends on `Publisher`.
pub trait Publisher: Send + Sync {
    /// The transaction type for the underlying database driver.
    type Tx<'a>: Send
    where
        Self: 'a;

    /// Append a single event to the outbox inside the given transaction.
    ///
    /// A UUID v4 `event_id` is generated internally. Use [`Publisher::append_with_id`] if
    /// you need a deterministic identifier.
    fn append<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        event: &'b E,
        ctx: &'b EventContext,
    ) -> impl Future<Output = Result<EventId, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b;

    /// Append a single event using a caller-supplied `event_id`.
    ///
    /// Useful for deterministic idempotency keys (e.g. UUID v5 derived from
    /// the input). On unique-constraint violation the call returns
    /// [`PublishError::DuplicateEventId`].
    fn append_with_id<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        event_id: EventId,
        event: &'b E,
        ctx: &'b EventContext,
    ) -> impl Future<Output = Result<EventId, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b;

    /// Append multiple events to the outbox in a single round-trip.
    fn append_batch<'a, 'b, E>(
        &'a self,
        tx: &'b mut Self::Tx<'a>,
        events: &'b [(E, EventContext)],
    ) -> impl Future<Output = Result<Vec<EventId>, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b;
}
