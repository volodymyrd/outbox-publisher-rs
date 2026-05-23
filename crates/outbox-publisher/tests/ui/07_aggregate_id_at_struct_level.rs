use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user", aggregate_id)]
pub struct UserRegistered {
    pub user_id: Uuid,
}

fn main() {}
