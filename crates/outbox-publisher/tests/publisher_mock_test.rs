use outbox_publisher::{
    domain_event::DomainEvent,
    error::PublishError,
    event::{EventContext, EventId},
    publisher::Publisher,
};
use serde::Serialize;
use std::future::Future;
use uuid::Uuid;

// A minimal domain event used only in these tests.
#[derive(Serialize)]
struct OrderPlaced {
    order_id: Uuid,
}

impl DomainEvent for OrderPlaced {
    fn kind() -> &'static str
    where
        Self: Sized,
    {
        "order.placed@v1"
    }
    fn aggregate_type() -> &'static str
    where
        Self: Sized,
    {
        "order"
    }
    fn aggregate_id(&self) -> Uuid {
        self.order_id
    }
}

// A hand-rolled mock that records calls — demonstrates the trait is usable
// across crate boundaries without a live database.
//
// std::sync::Mutex is intentional: the critical section (push/clone) is purely
// synchronous and never crosses an `.await` point, so the lighter std mutex is
// correct here (tokio::sync::Mutex would add unnecessary overhead).
struct RecordingPublisher {
    appended: std::sync::Mutex<Vec<String>>,
}

impl RecordingPublisher {
    fn new() -> Self {
        Self {
            appended: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn recorded(&self) -> Vec<String> {
        self.appended.lock().unwrap().clone()
    }
}

// A dummy transaction type — no real DB needed for unit tests.
pub struct NoopTx;

// The impl below uses the explicit `fn ... -> impl Future` form intentionally:
// it mirrors exactly what a real adapter (e.g. SqlxPublisher) must write, making
// this the canonical reference pattern for implementing the Publisher trait.
#[allow(clippy::manual_async_fn)]
impl Publisher for RecordingPublisher {
    type Tx<'a> = NoopTx;

    fn append<'a, 'b, E>(
        &'a self,
        _tx: &'b mut NoopTx,
        _event: &'b E,
        _ctx: &'b EventContext,
    ) -> impl Future<Output = Result<EventId, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        async move {
            self.appended.lock().unwrap().push(E::kind().to_owned());
            Ok(EventId::from(Uuid::new_v4()))
        }
    }

    fn append_with_id<'a, 'b, E>(
        &'a self,
        _tx: &'b mut NoopTx,
        event_id: EventId,
        _event: &'b E,
        _ctx: &'b EventContext,
    ) -> impl Future<Output = Result<EventId, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        async move {
            self.appended.lock().unwrap().push(E::kind().to_owned());
            Ok(event_id)
        }
    }

    fn append_batch<'a, 'b, E>(
        &'a self,
        _tx: &'b mut NoopTx,
        events: &'b [(E, EventContext)],
    ) -> impl Future<Output = Result<Vec<EventId>, PublishError>> + Send + 'b
    where
        E: DomainEvent + Serialize + Send + Sync,
        'a: 'b,
    {
        async move {
            let kind = E::kind().to_owned();
            let mut guard = self.appended.lock().unwrap();
            let ids = events
                .iter()
                .map(|_| {
                    guard.push(kind.clone());
                    EventId::from(Uuid::new_v4())
                })
                .collect();
            Ok(ids)
        }
    }
}

#[tokio::test]
async fn mock_publisher_append_records_event_kind() {
    let publisher = RecordingPublisher::new();
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };
    let ctx = EventContext::default();
    let mut tx = NoopTx;

    let result = publisher.append(&mut tx, &event, &ctx).await;
    assert!(result.is_ok());
    assert_eq!(publisher.recorded(), vec!["order.placed@v1"]);
}

#[tokio::test]
async fn mock_publisher_append_batch_records_all_events() {
    let publisher = RecordingPublisher::new();
    let events = vec![
        (
            OrderPlaced {
                order_id: Uuid::new_v4(),
            },
            EventContext::default(),
        ),
        (
            OrderPlaced {
                order_id: Uuid::new_v4(),
            },
            EventContext::default(),
        ),
    ];
    let mut tx = NoopTx;

    let result = publisher.append_batch(&mut tx, &events).await;
    assert!(result.is_ok());
    let ids = result.unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(
        publisher.recorded(),
        vec!["order.placed@v1", "order.placed@v1"]
    );
}

#[tokio::test]
async fn mock_publisher_append_with_id_preserves_event_id() {
    let publisher = RecordingPublisher::new();
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };
    let ctx = EventContext::default();
    let mut tx = NoopTx;
    let supplied_id = EventId::from(Uuid::new_v4());

    let result = publisher
        .append_with_id(&mut tx, supplied_id, &event, &ctx)
        .await;
    assert_eq!(result.unwrap(), supplied_id);
    assert_eq!(publisher.recorded(), vec!["order.placed@v1"]);
}

#[tokio::test]
async fn mock_publisher_append_returns_unique_event_ids() {
    let publisher = RecordingPublisher::new();
    let mut tx = NoopTx;
    let ctx = EventContext::default();

    let id1 = publisher
        .append(
            &mut tx,
            &OrderPlaced {
                order_id: Uuid::new_v4(),
            },
            &ctx,
        )
        .await
        .unwrap();
    let id2 = publisher
        .append(
            &mut tx,
            &OrderPlaced {
                order_id: Uuid::new_v4(),
            },
            &ctx,
        )
        .await
        .unwrap();

    assert_ne!(id1.into_uuid(), id2.into_uuid());
}

#[tokio::test]
async fn mock_publisher_does_not_record_until_awaited() {
    let publisher = RecordingPublisher::new();
    let event = OrderPlaced {
        order_id: Uuid::new_v4(),
    };
    let ctx = EventContext::default();
    let mut tx = NoopTx;

    // Construct the future but do not poll it.
    let _fut = publisher.append(&mut tx, &event, &ctx);
    assert!(publisher.recorded().is_empty());
}
