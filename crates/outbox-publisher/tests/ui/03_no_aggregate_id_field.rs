use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub struct UserRegistered {
    pub user_id: Uuid,
    pub email: String,
}

fn main() {}
