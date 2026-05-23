use outbox_publisher::domain_event::DomainEvent;
use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: Uuid,
    pub email: String,
    pub signup_method: String,
}

#[test]
fn derive_kind() {
    assert_eq!(UserRegistered::kind(), "user.registered@v1");
}

#[test]
fn derive_aggregate_type() {
    assert_eq!(UserRegistered::aggregate_type(), "user");
}

#[test]
fn derive_aggregate_id() {
    let id = Uuid::new_v4();
    let ev = UserRegistered {
        user_id: id,
        email: "test@example.com".into(),
        signup_method: "email".into(),
    };
    assert_eq!(ev.aggregate_id(), id);
}

// Verify the generated impl doesn't require #[event] on the struct to be duplicated.
#[derive(DomainEvent)]
#[event(kind = "order.placed@v1")]
#[event(aggregate = "order")]
pub struct OrderPlaced {
    #[event(aggregate_id)]
    pub order_id: Uuid,
    pub total_cents: u64,
}

#[test]
fn derive_split_attrs() {
    assert_eq!(OrderPlaced::kind(), "order.placed@v1");
    assert_eq!(OrderPlaced::aggregate_type(), "order");
    let id = Uuid::new_v4();
    let ev = OrderPlaced {
        order_id: id,
        total_cents: 9900,
    };
    assert_eq!(ev.aggregate_id(), id);
}
