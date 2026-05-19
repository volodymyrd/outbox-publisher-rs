use uuid::Uuid;

/// Opaque identifier for a published outbox event.
///
/// Returned by [`crate::publisher::Publisher::append`] and useful for logging
/// or chaining `causation_id` on downstream events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(pub Uuid);

impl EventId {
    /// Wrap an existing [`Uuid`] as an `EventId`.
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Return the inner [`Uuid`].
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Request-level metadata carried into the outbox row alongside the event payload.
///
/// Build with the fluent builder methods; `EventContext::default()` produces an
/// instance with all optional fields set to `None` and `metadata` set to `{}`.
///
/// # Example
///
/// ```
/// use outbox_publisher::event::EventContext;
/// use uuid::Uuid;
///
/// let ctx = EventContext::default()
///     .for_actor(Uuid::new_v4())
///     .with_correlation(Uuid::new_v4());
/// assert!(ctx.actor_id.is_some());
/// assert!(ctx.correlation_id.is_some());
/// assert!(ctx.causation_id.is_none());
/// ```
#[derive(Debug, Clone)]
pub struct EventContext {
    /// The authenticated user or service that triggered the event.
    pub actor_id: Option<Uuid>,
    /// Groups related events that belong to the same logical request / saga.
    pub correlation_id: Option<Uuid>,
    /// The event or command that directly caused this event.
    pub causation_id: Option<Uuid>,
    /// Arbitrary structured metadata forwarded verbatim into the outbox row.
    pub metadata: serde_json::Value,
}

impl Default for EventContext {
    fn default() -> Self {
        Self {
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

impl EventContext {
    /// Set the actor (caller identity) for this event.
    pub fn for_actor(mut self, actor_id: Uuid) -> Self {
        self.actor_id = Some(actor_id);
        self
    }

    /// Set the correlation identifier.
    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Set the causation identifier.
    pub fn with_causation(mut self, causation_id: Uuid) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    /// Replace the metadata value (must be a JSON object or `null`).
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_id_round_trips_uuid() {
        let id = Uuid::new_v4();
        let event_id = EventId::from_uuid(id);
        assert_eq!(event_id.into_uuid(), id);
    }

    #[test]
    fn event_id_display_matches_uuid() {
        let id = Uuid::new_v4();
        let event_id = EventId::from_uuid(id);
        assert_eq!(event_id.to_string(), id.to_string());
    }

    #[test]
    fn event_context_default_has_empty_metadata() {
        let ctx = EventContext::default();
        assert!(ctx.actor_id.is_none());
        assert!(ctx.correlation_id.is_none());
        assert!(ctx.causation_id.is_none());
        assert_eq!(ctx.metadata, json!({}));
    }

    #[test]
    fn event_context_for_actor() {
        let actor = Uuid::new_v4();
        let ctx = EventContext::default().for_actor(actor);
        assert_eq!(ctx.actor_id, Some(actor));
    }

    #[test]
    fn event_context_with_correlation() {
        let corr = Uuid::new_v4();
        let ctx = EventContext::default().with_correlation(corr);
        assert_eq!(ctx.correlation_id, Some(corr));
    }

    #[test]
    fn event_context_with_causation() {
        let cause = Uuid::new_v4();
        let ctx = EventContext::default().with_causation(cause);
        assert_eq!(ctx.causation_id, Some(cause));
    }

    #[test]
    fn event_context_with_metadata() {
        let meta = json!({"key": "value"});
        let ctx = EventContext::default().with_metadata(meta.clone());
        assert_eq!(ctx.metadata, meta);
    }

    #[test]
    fn event_context_builder_chain() {
        let actor = Uuid::new_v4();
        let corr = Uuid::new_v4();
        let cause = Uuid::new_v4();
        let meta = json!({"source": "test"});

        let ctx = EventContext::default()
            .for_actor(actor)
            .with_correlation(corr)
            .with_causation(cause)
            .with_metadata(meta.clone());

        assert_eq!(ctx.actor_id, Some(actor));
        assert_eq!(ctx.correlation_id, Some(corr));
        assert_eq!(ctx.causation_id, Some(cause));
        assert_eq!(ctx.metadata, meta);
    }
}
