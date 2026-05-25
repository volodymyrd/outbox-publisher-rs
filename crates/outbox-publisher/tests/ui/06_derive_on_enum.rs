use outbox_publisher_derive::DomainEvent;
use uuid::Uuid;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub enum UserEvent {
    Registered { user_id: Uuid },
}

fn main() {}
