use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "", aggregate = "user")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: Uuid,
}

fn main() {}
