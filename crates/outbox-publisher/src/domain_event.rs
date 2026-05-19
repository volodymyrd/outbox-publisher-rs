use uuid::Uuid;

/// Describes a domain event that can be written to the outbox.
///
/// Implement this trait (or derive it with `#[derive(DomainEvent)]`) for every
/// event struct you want to publish.
///
/// # Example
///
/// ```
/// use outbox_publisher::domain_event::DomainEvent;
/// use uuid::Uuid;
///
/// struct UserRegistered {
///     user_id: Uuid,
///     email: String,
/// }
///
/// impl DomainEvent for UserRegistered {
///     fn kind() -> &'static str where Self: Sized {
///         "user.registered@v1"
///     }
///     fn aggregate_type() -> &'static str where Self: Sized {
///         "user"
///     }
///     fn aggregate_id(&self) -> Uuid {
///         self.user_id
///     }
/// }
///
/// let event = UserRegistered { user_id: Uuid::nil(), email: "a@b.com".into() };
/// assert_eq!(UserRegistered::kind(), "user.registered@v1");
/// assert_eq!(UserRegistered::aggregate_type(), "user");
/// assert_eq!(event.aggregate_id(), Uuid::nil());
/// ```
pub trait DomainEvent {
    /// A stable, versioned event-kind identifier (e.g. `"user.registered@v1"`).
    fn kind() -> &'static str
    where
        Self: Sized;

    /// The aggregate type that owns this event (e.g. `"user"`).
    fn aggregate_type() -> &'static str
    where
        Self: Sized;

    /// The identifier of the specific aggregate instance that emitted this event.
    fn aggregate_id(&self) -> Uuid;
}
