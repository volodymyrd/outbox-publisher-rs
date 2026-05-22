//! Integration tests for `SqlxPublisher`.
//!
//! Each test spins up a fresh Postgres container via `testcontainers`, applies
//! the schema fixture, and exercises the publisher end-to-end against a real
//! database. Docker must be running locally.

use std::time::Duration;

use outbox_publisher::{
    event::{EventContext, EventId},
    publisher::Publisher,
};
use outbox_publisher_sqlx::SqlxPublisher;
use serde::Serialize;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

// ── Shared test helpers ───────────────────────────────────────────────────────

const SCHEMA_SQL: &str = include_str!("../../../tests/fixtures/0001_initial_schema.sql");

/// Spin up a fresh Postgres container and return a connected pool.
async fn setup_db() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("get_host");
    let port = container.get_host_port_ipv4(5432).await.expect("get_port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    sqlx::raw_sql(SCHEMA_SQL)
        .execute(&pool)
        .await
        .expect("apply schema fixture");

    (pool, container)
}

/// A minimal test event.
#[derive(Debug, Serialize)]
struct UserRegistered {
    user_id: Uuid,
    email: String,
}

impl outbox_publisher::domain_event::DomainEvent for UserRegistered {
    fn kind() -> &'static str
    where
        Self: Sized,
    {
        "user.registered@v1"
    }

    fn aggregate_type() -> &'static str
    where
        Self: Sized,
    {
        "user"
    }

    fn aggregate_id(&self) -> Uuid {
        self.user_id
    }
}

/// Default callback used in tests.
fn test_callback() -> serde_json::Value {
    json!({"name": "notify", "url": "https://example.com/hook"})
}

/// Default `EventContext` with one callback.
fn test_ctx() -> EventContext {
    EventContext::default().with_callbacks(vec![test_callback()])
}

// ── Step 2.3 — append (single row) ───────────────────────────────────────────

/// `append` inserts a row with all columns correctly populated.
#[tokio::test]
async fn append_inserts_all_columns() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let actor_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();
    let causation_id = Uuid::new_v4();
    let cb = test_callback();

    let ctx = EventContext::default()
        .for_actor(actor_id)
        .with_correlation(correlation_id)
        .with_causation(causation_id)
        .with_callbacks(vec![cb.clone()]);

    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "alice@example.com".to_owned(),
    };

    let mut tx = pool.begin().await.expect("begin tx");
    let event_id = publisher
        .append(&mut tx, &event, &ctx)
        .await
        .expect("append");
    tx.commit().await.expect("commit");

    let row = sqlx::query(
        "SELECT event_id, kind, aggregate_type, aggregate_id,
                payload, metadata, callbacks,
                actor_id, correlation_id, causation_id
         FROM outbox_events
         WHERE event_id = $1",
    )
    .bind(Uuid::from(event_id))
    .fetch_one(&pool)
    .await
    .expect("fetch row");

    assert_eq!(row.get::<Uuid, _>("event_id"), Uuid::from(event_id));
    assert_eq!(row.get::<String, _>("kind"), "user.registered@v1");
    assert_eq!(row.get::<String, _>("aggregate_type"), "user");
    assert_eq!(row.get::<Uuid, _>("aggregate_id"), event.user_id);

    let payload: serde_json::Value = row.get("payload");
    assert_eq!(payload["email"], "alice@example.com");

    let metadata: serde_json::Value = row.get("metadata");
    assert_eq!(metadata, json!({}));

    let callbacks: serde_json::Value = row.get("callbacks");
    assert_eq!(callbacks, json!([cb]));

    assert_eq!(row.get::<Option<Uuid>, _>("actor_id"), Some(actor_id));
    assert_eq!(
        row.get::<Option<Uuid>, _>("correlation_id"),
        Some(correlation_id)
    );
    assert_eq!(
        row.get::<Option<Uuid>, _>("causation_id"),
        Some(causation_id)
    );
}

/// `append` returns a unique `EventId` on each call.
#[tokio::test]
async fn append_returns_unique_event_ids() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let event1 = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "a@example.com".to_owned(),
    };
    let event2 = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "b@example.com".to_owned(),
    };
    let ctx = test_ctx();

    let mut tx = pool.begin().await.expect("begin tx");
    let id1 = publisher
        .append(&mut tx, &event1, &ctx)
        .await
        .expect("append 1");
    let id2 = publisher
        .append(&mut tx, &event2, &ctx)
        .await
        .expect("append 2");
    tx.commit().await.expect("commit");

    assert_ne!(id1, id2);
}

/// `append` rollback leaves no row in the table.
#[tokio::test]
async fn append_rollback_leaves_no_row() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "rollback@example.com".to_owned(),
    };
    let ctx = test_ctx();

    let mut tx = pool.begin().await.expect("begin tx");
    let event_id = publisher
        .append(&mut tx, &event, &ctx)
        .await
        .expect("append");
    tx.rollback().await.expect("rollback");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE event_id = $1")
        .bind(Uuid::from(event_id))
        .fetch_one(&pool)
        .await
        .expect("count");

    assert_eq!(count, 0);
}

// ── Step 2.4 — append_with_id ─────────────────────────────────────────────────

/// `append_with_id` inserts a row with the caller-supplied UUID.
#[tokio::test]
async fn append_with_id_uses_caller_uuid() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let deterministic_id = Uuid::new_v4();
    let event_id = EventId::from(deterministic_id);
    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "idempotent@example.com".to_owned(),
    };
    let ctx = test_ctx();

    let mut tx = pool.begin().await.expect("begin tx");
    let returned = publisher
        .append_with_id(&mut tx, event_id, &event, &ctx)
        .await
        .expect("append_with_id");
    tx.commit().await.expect("commit");

    assert_eq!(Uuid::from(returned), deterministic_id);

    let stored: Uuid = sqlx::query_scalar("SELECT event_id FROM outbox_events WHERE event_id = $1")
        .bind(deterministic_id)
        .fetch_one(&pool)
        .await
        .expect("fetch");

    assert_eq!(stored, deterministic_id);
}

/// `append_with_id` returns `PublishError::DuplicateEventId` on a conflict.
#[tokio::test]
async fn append_with_id_duplicate_returns_error() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let deterministic_id = Uuid::new_v4();
    let event_id = EventId::from(deterministic_id);
    let ctx = test_ctx();

    // First insert succeeds.
    let event1 = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "first@example.com".to_owned(),
    };
    let mut tx = pool.begin().await.expect("begin tx");
    publisher
        .append_with_id(&mut tx, event_id, &event1, &ctx)
        .await
        .expect("first insert");
    tx.commit().await.expect("commit");

    // Second insert with the same event_id must fail.
    let event2 = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "second@example.com".to_owned(),
    };
    let mut tx2 = pool.begin().await.expect("begin tx2");
    let err = publisher
        .append_with_id(&mut tx2, event_id, &event2, &ctx)
        .await
        .expect_err("expected duplicate error");
    tx2.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::DuplicateEventId),
        "expected DuplicateEventId, got {err:?}",
    );
}

// ── Step 2.5 — append_batch ───────────────────────────────────────────────────

/// `append_batch` on an empty slice returns an empty vec without touching the DB.
#[tokio::test]
async fn append_batch_empty_is_noop() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let mut tx = pool.begin().await.expect("begin tx");
    let ids = publisher
        .append_batch::<UserRegistered>(&mut tx, &[])
        .await
        .expect("batch empty");
    tx.commit().await.expect("commit");

    assert!(ids.is_empty());

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 0);
}

/// `append_batch` inserts N rows and returns N distinct IDs.
#[tokio::test]
async fn append_batch_inserts_all_rows() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let ctx = test_ctx();
    let events: Vec<(UserRegistered, EventContext)> = (0..5)
        .map(|i| {
            (
                UserRegistered {
                    user_id: Uuid::new_v4(),
                    email: format!("user{i}@example.com"),
                },
                ctx.clone(),
            )
        })
        .collect();

    let mut tx = pool.begin().await.expect("begin tx");
    let ids = publisher
        .append_batch(&mut tx, &events)
        .await
        .expect("batch");
    tx.commit().await.expect("commit");

    assert_eq!(ids.len(), 5);

    // All IDs must be distinct.
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 5);

    // All rows must be present in the DB.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 5);
}

// ── Finding 6 — rollback test for append_with_id ─────────────────────────────

/// `append_with_id` rollback leaves no row in the table.
#[tokio::test]
async fn append_with_id_rollback_leaves_no_row() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let event_id = EventId::from(Uuid::new_v4());
    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "rollback-with-id@example.com".to_owned(),
    };
    let ctx = test_ctx();

    let mut tx = pool.begin().await.expect("begin tx");
    publisher
        .append_with_id(&mut tx, event_id, &event, &ctx)
        .await
        .expect("append_with_id");
    tx.rollback().await.expect("rollback");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE event_id = $1")
        .bind(Uuid::from(event_id))
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 0);
}

// ── Finding 5 — batch with mixed-NULL optional fields ─────────────────────────

/// `append_batch` correctly handles a mix of populated and absent optional fields.
#[tokio::test]
async fn append_batch_handles_mixed_null_optional_fields() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let cb = test_callback();
    let actor = Uuid::new_v4();
    let corr = Uuid::new_v4();

    let events: Vec<(UserRegistered, EventContext)> = vec![
        (
            UserRegistered {
                user_id: Uuid::new_v4(),
                email: "all@example.com".into(),
            },
            EventContext::default()
                .for_actor(actor)
                .with_correlation(corr)
                .with_causation(Uuid::new_v4())
                .with_callbacks(vec![cb.clone()]),
        ),
        (
            UserRegistered {
                user_id: Uuid::new_v4(),
                email: "none@example.com".into(),
            },
            EventContext::default().with_callbacks(vec![cb.clone()]),
        ),
        (
            UserRegistered {
                user_id: Uuid::new_v4(),
                email: "partial@example.com".into(),
            },
            EventContext::default()
                .for_actor(actor)
                .with_callbacks(vec![cb.clone()]),
        ),
    ];

    let mut tx = pool.begin().await.expect("begin");
    let ids = publisher
        .append_batch(&mut tx, &events)
        .await
        .expect("batch");
    tx.commit().await.expect("commit");

    let id_uuids: Vec<Uuid> = ids.iter().copied().map(Uuid::from).collect();
    let rows = sqlx::query(
        "SELECT actor_id, correlation_id, causation_id
         FROM outbox_events WHERE event_id = ANY($1) ORDER BY id",
    )
    .bind(&id_uuids)
    .fetch_all(&pool)
    .await
    .expect("fetch");

    assert_eq!(rows.len(), 3);

    // Row 0: all fields set.
    assert_eq!(rows[0].get::<Option<Uuid>, _>("actor_id"), Some(actor));
    assert_eq!(rows[0].get::<Option<Uuid>, _>("correlation_id"), Some(corr));
    assert!(rows[0].get::<Option<Uuid>, _>("causation_id").is_some());

    // Row 1: none set.
    assert_eq!(rows[1].get::<Option<Uuid>, _>("actor_id"), None);
    assert_eq!(rows[1].get::<Option<Uuid>, _>("correlation_id"), None);
    assert_eq!(rows[1].get::<Option<Uuid>, _>("causation_id"), None);

    // Row 2: only actor set.
    assert_eq!(rows[2].get::<Option<Uuid>, _>("actor_id"), Some(actor));
    assert_eq!(rows[2].get::<Option<Uuid>, _>("correlation_id"), None);
    assert_eq!(rows[2].get::<Option<Uuid>, _>("causation_id"), None);
}

// ── Finding 3 — MissingCallbacks error ───────────────────────────────────────

/// `append` returns `MissingCallbacks` when `EventContext` has no callbacks.
#[tokio::test]
async fn append_returns_missing_callbacks_error() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let event = UserRegistered {
        user_id: Uuid::new_v4(),
        email: "no-callbacks@example.com".to_owned(),
    };
    let ctx = EventContext::default(); // no callbacks

    let mut tx = pool.begin().await.expect("begin tx");
    let err = publisher
        .append(&mut tx, &event, &ctx)
        .await
        .expect_err("expected MissingCallbacks");
    tx.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::MissingCallbacks),
        "expected MissingCallbacks, got {err:?}",
    );
}

/// `append_batch` returns `MissingCallbacks` when any context has no callbacks.
#[tokio::test]
async fn append_batch_returns_missing_callbacks_error() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let events = vec![
        (
            UserRegistered {
                user_id: Uuid::new_v4(),
                email: "ok@example.com".to_owned(),
            },
            test_ctx(),
        ),
        (
            UserRegistered {
                user_id: Uuid::new_v4(),
                email: "no-cb@example.com".to_owned(),
            },
            EventContext::default(), // no callbacks
        ),
    ];

    let mut tx = pool.begin().await.expect("begin tx");
    let err = publisher
        .append_batch(&mut tx, &events)
        .await
        .expect_err("expected MissingCallbacks");
    tx.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::MissingCallbacks),
        "expected MissingCallbacks, got {err:?}",
    );
}

// ── Finding 7 — Serialization error branch ───────────────────────────────────

/// `append` returns `Serialization` when the event's `Serialize` impl fails.
#[tokio::test]
async fn append_returns_serialization_error_for_failing_serialize() {
    use serde::ser::Error as _;

    struct Boom;

    impl serde::Serialize for Boom {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("boom"))
        }
    }

    impl outbox_publisher::domain_event::DomainEvent for Boom {
        fn kind() -> &'static str
        where
            Self: Sized,
        {
            "boom@v1"
        }

        fn aggregate_type() -> &'static str
        where
            Self: Sized,
        {
            "boom"
        }

        fn aggregate_id(&self) -> Uuid {
            Uuid::nil()
        }
    }

    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let mut tx = pool.begin().await.expect("begin tx");
    let err = publisher
        .append(&mut tx, &Boom, &test_ctx())
        .await
        .expect_err("expected serialization error");
    tx.rollback().await.ok();

    assert!(
        matches!(err, outbox_publisher::error::PublishError::Serialization(_)),
        "expected Serialization, got {err:?}",
    );
}

// ── Finding 2 — with_schema validation ───────────────────────────────────────

/// `with_schema` rejects invalid identifiers.
#[test]
fn with_schema_rejects_invalid_identifiers() {
    let invalid_cases = [
        "",
        "123bad",
        "has space",
        "has-dash",
        "has;semi",
        "\"quoted\"",
    ];
    for case in &invalid_cases {
        assert!(
            SqlxPublisher::new().with_schema(*case).is_err(),
            "expected Err for schema {case:?}"
        );
    }
}

/// `with_schema` accepts valid unquoted Postgres identifiers.
#[test]
fn with_schema_accepts_valid_identifiers() {
    let valid_cases = ["public", "myschema", "_private", "schema_1", "MySchema"];
    for case in &valid_cases {
        let publisher = SqlxPublisher::new()
            .with_schema(*case)
            .expect("valid identifier");
        assert_eq!(publisher.schema(), *case);
    }
}

/// `append_batch` and N successive `append` calls produce equivalent rows
/// (same columns, same count).
#[tokio::test]
async fn append_batch_equivalent_to_individual_appends() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let ctx = test_ctx();
    let user_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();

    // Insert via individual appends.
    let mut tx = pool.begin().await.expect("begin tx");
    for uid in &user_ids {
        let event = UserRegistered {
            user_id: *uid,
            email: format!("{uid}@example.com"),
        };
        publisher
            .append(&mut tx, &event, &ctx)
            .await
            .expect("append");
    }
    tx.commit().await.expect("commit individual");

    // Insert via batch.
    let batch: Vec<(UserRegistered, EventContext)> = user_ids
        .iter()
        .map(|uid| {
            (
                UserRegistered {
                    user_id: *uid,
                    email: format!("{uid}@example.com"),
                },
                ctx.clone(),
            )
        })
        .collect();

    let mut tx2 = pool.begin().await.expect("begin tx2");
    let batch_ids = publisher
        .append_batch(&mut tx2, &batch)
        .await
        .expect("batch");
    tx2.commit().await.expect("commit batch");

    // Both inserts produced the same number of rows.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 6); // 3 individual + 3 batch

    assert_eq!(batch_ids.len(), 3);
}

/// `append_batch` rollback leaves no rows.
#[tokio::test]
async fn append_batch_rollback_leaves_no_rows() {
    let (pool, _container) = setup_db().await;
    let publisher = SqlxPublisher::new();

    let ctx = test_ctx();
    let events: Vec<(UserRegistered, EventContext)> = (0..3)
        .map(|i| {
            (
                UserRegistered {
                    user_id: Uuid::new_v4(),
                    email: format!("rollback{i}@example.com"),
                },
                ctx.clone(),
            )
        })
        .collect();

    let mut tx = pool.begin().await.expect("begin tx");
    publisher
        .append_batch(&mut tx, &events)
        .await
        .expect("batch");
    tx.rollback().await.expect("rollback");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 0);
}
