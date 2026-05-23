use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: Uuid,
}

fn main() {}
