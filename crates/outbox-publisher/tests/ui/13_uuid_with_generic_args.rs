use outbox_publisher_derive::DomainEvent;

// A user-defined Uuid<T> shadow type (or a typo) must not pass the macro's
// type guard, otherwise the compile error lands on the generated impl rather
// than the field declaration.
pub struct Uuid<T>(std::marker::PhantomData<T>);

#[derive(DomainEvent)]
#[event(kind = "user.registered@v1", aggregate = "user")]
pub struct UserRegistered {
    #[event(aggregate_id)]
    pub user_id: Uuid<()>,
}

fn main() {}
