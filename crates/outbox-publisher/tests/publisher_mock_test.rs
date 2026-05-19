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

unsafe impl Send for NoopTx {}

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
        let kind = E::kind().to_owned();
        self.appended.lock().unwrap().push(kind);
        async { Ok(EventId::from_uuid(Uuid::new_v4())) }
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
        let count = events.len();
        {
            let kind = E::kind().to_owned();
            let mut guard = self.appended.lock().unwrap();
            for _ in 0..count {
                guard.push(kind.clone());
            }
        }
        async move {
            Ok((0..count)
                .map(|_| EventId::from_uuid(Uuid::new_v4()))
                .collect())
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
