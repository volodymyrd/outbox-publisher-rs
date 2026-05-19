use outbox_publisher_derive::DomainEvent;

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: String,
}

fn main() {}
