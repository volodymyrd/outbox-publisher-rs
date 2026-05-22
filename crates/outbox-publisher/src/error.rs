/// Errors that can occur when publishing events to the outbox.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The caller-supplied `event_id` already exists in the outbox.
    #[error("duplicate event id")]
    DuplicateEventId,
    /// A database-level error occurred while inserting the outbox row.
    #[error("database error")]
    Database(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The event payload could not be serialized to JSON.
    #[error("payload serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Errors that can occur when verifying a dispatcher-signed webhook.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The `X-Outbox-Signature` header was absent.
    #[error("missing signature header")]
    MissingHeader,
    /// The header value could not be parsed (missing `t=`, `v1=`, or non-hex digest).
    #[error("malformed signature header")]
    MalformedHeader,
    /// The signed timestamp is outside the configured tolerance window.
    #[error("timestamp outside tolerance")]
    TimestampOutOfTolerance,
    /// The computed HMAC digest does not match the header value.
    #[error("invalid signature")]
    InvalidSignature,
    /// The webhook body could not be deserialized into the expected type.
    #[error("body parse failed: {0}")]
    BodyParse(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_error_duplicate_event_id_display() {
        assert_eq!(
            PublishError::DuplicateEventId.to_string(),
            "duplicate event id"
        );
    }

    #[test]
    fn publish_error_database_carries_source() {
        use std::error::Error;
        let cause: Box<dyn std::error::Error + Send + Sync> =
            Box::new(std::io::Error::other("connection refused"));
        let err = PublishError::Database(cause);
        assert_eq!(err.to_string(), "database error");
        assert_eq!(err.source().unwrap().to_string(), "connection refused",);
    }

    #[test]
    fn publish_error_serialization_display() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err = PublishError::Serialization(json_err);
        assert!(err.to_string().starts_with("payload serialization failed:"));
    }

    #[test]
    fn verify_error_display_variants() {
        assert_eq!(
            VerifyError::MissingHeader.to_string(),
            "missing signature header"
        );
        assert_eq!(
            VerifyError::MalformedHeader.to_string(),
            "malformed signature header"
        );
        assert_eq!(
            VerifyError::TimestampOutOfTolerance.to_string(),
            "timestamp outside tolerance"
        );
        assert_eq!(
            VerifyError::InvalidSignature.to_string(),
            "invalid signature"
        );
    }

    #[test]
    fn verify_error_body_parse_display() {
        let json_err = serde_json::from_str::<serde_json::Value>("bad").unwrap_err();
        let err = VerifyError::BodyParse(json_err);
        assert!(err.to_string().starts_with("body parse failed:"));
    }
}
