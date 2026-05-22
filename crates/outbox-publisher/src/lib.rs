pub mod domain_event;
pub mod error;
pub mod event;
pub mod publisher;

#[cfg(feature = "axum")]
pub mod webhook;

pub use domain_event::DomainEvent;
pub use error::{PublishError, VerifyError};
pub use event::{EventContext, EventId};
pub use publisher::Publisher;

#[cfg(feature = "derive")]
pub use outbox_publisher_derive::DomainEvent;
