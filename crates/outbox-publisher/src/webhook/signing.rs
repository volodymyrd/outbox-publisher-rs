//! HMAC-SHA256 signature verification for dispatcher-signed webhooks.
//!
//! Ported from `outbox-dispatcher/crates/http-callback/src/signing.rs`.
//! The cross-language interop test (Step 4.4) is what catches any drift.
//!
//! # Format
//!
//! The dispatcher sets `X-Outbox-Signature: t=<unix_seconds>,v1=<lowercase_hex>`.
//! The signed payload is `"<timestamp>.<raw_body_bytes>"` fed to HMAC-SHA256.
//!
//! # Constant-time verification
//!
//! [`verify`] uses `hmac::Mac::verify_slice` — never `==` on hex strings — to
//! prevent timing side-channels.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verifies a `X-Outbox-Signature` header value in constant time.
///
/// Returns `true` only when the HMAC digest encoded in `header_value`
/// matches `HMAC-SHA256(secret, "<timestamp_secs>.<body>")`.
///
/// The caller is responsible for:
/// 1. Extracting `t=<unix_ts>` from `header_value` and supplying it as
///    `timestamp_secs`.  A mismatch between the two causes the computed HMAC
///    to differ from the header digest, so verification correctly fails.
/// 2. Enforcing a replay window (e.g. reject if `|now − timestamp_secs| > 300`).
pub fn verify(secret: &[u8], timestamp_secs: u64, body: &[u8], header_value: &str) -> bool {
    let Some(hex_digest) = parse_v1_digest(header_value) else {
        return false;
    };
    let Ok(decoded) = hex::decode(hex_digest) else {
        return false;
    };
    verify_decoded(secret, timestamp_secs, body, &decoded)
}

/// Like [`verify`] but accepts the already-decoded digest bytes.
///
/// Used by [`super::WebhookVerifier::verify`] after it has decoded the digest
/// itself (so a hex-decode failure can be distinguished from an HMAC mismatch).
pub(crate) fn verify_decoded(
    secret: &[u8],
    timestamp_secs: u64,
    body: &[u8],
    decoded_digest: &[u8],
) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(format!("{timestamp_secs}.").as_bytes());
    mac.update(body);
    mac.verify_slice(decoded_digest).is_ok()
}

/// Parses the `v1=` field from `header_value` and hex-decodes the digest.
///
/// Returns `None` if the field is absent or contains non-hex characters.
/// Used by [`super::WebhookVerifier::verify`] so a hex-decode failure can be
/// reported as [`crate::error::VerifyError::MalformedHeader`] rather than
/// [`crate::error::VerifyError::InvalidSignature`].
pub(crate) fn parse_v1_decoded(header_value: &str) -> Option<Vec<u8>> {
    let hex_digest = parse_v1_digest(header_value)?;
    hex::decode(hex_digest).ok()
}

/// Sign `body` with `secret` and `timestamp_secs`.
///
/// Produces `t=<unix_ts>,v1=<hex(HMAC-SHA256(secret, "<ts>.<body>"))>`.
///
/// This is the same computation the dispatcher uses. Kept here so tests can
/// produce reference signatures without depending on the dispatcher crate.
#[cfg(test)]
pub(crate) fn sign(secret: &[u8], timestamp_secs: u64, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(format!("{timestamp_secs}.").as_bytes());
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    format!("t={timestamp_secs},v1={}", hex::encode(digest))
}

pub(crate) fn parse_t_field(header_value: &str) -> Option<u64> {
    for part in header_value.split(',') {
        if let Some(ts) = part.trim().strip_prefix("t=") {
            return ts.parse().ok();
        }
    }
    None
}

pub(crate) fn parse_v1_digest(header_value: &str) -> Option<&str> {
    for part in header_value.split(',') {
        if let Some(hex) = part.trim().strip_prefix("v1=") {
            return Some(hex);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    const SECRET: &[u8] = b"super-secret-key-32-bytes-minimum!!";

    #[test]
    fn sign_and_verify_roundtrip() {
        let body = b"{\"hello\":\"world\"}";
        let ts = 1_714_229_400_u64;
        let header = sign(SECRET, ts, body);
        assert!(verify(SECRET, ts, body, &header));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let body = b"{\"hello\":\"world\"}";
        let ts = 1_714_229_400_u64;
        let header = sign(SECRET, ts, body);
        assert!(!verify(b"wrong-secret", ts, body, &header));
    }

    #[test]
    fn verify_rejects_single_byte_flip() {
        let body = b"{\"hello\":\"world\"}";
        let ts = 1_714_229_400_u64;
        let header = sign(SECRET, ts, body);

        let flipped = header.replacen('a', "b", 1);
        let flipped = if flipped == header {
            header.replacen('0', "1", 1)
        } else {
            flipped
        };

        assert!(!verify(SECRET, ts, body, &flipped));
    }

    #[test]
    fn verify_rejects_malformed_header_no_v1() {
        assert!(!verify(SECRET, 0, b"body", "t=0,garbage=abc"));
    }

    #[test]
    fn verify_rejects_non_hex_digest() {
        assert!(!verify(SECRET, 0, b"body", "t=0,v1=not-hex!!"));
    }

    #[test]
    fn parse_v1_decoded_returns_none_for_non_hex() {
        assert!(parse_v1_decoded("t=0,v1=not-hex!!").is_none());
    }

    #[test]
    fn parse_v1_decoded_returns_none_when_absent() {
        assert!(parse_v1_decoded("t=0,garbage=abc").is_none());
    }

    #[test]
    fn parse_v1_decoded_returns_bytes_for_valid_hex() {
        let body = b"hello";
        let ts = 1_714_229_400_u64;
        let header = sign(SECRET, ts, body);
        assert!(parse_v1_decoded(&header).is_some());
    }

    // ── verify_header (test-only helper, kept for coverage of legacy path) ────

    fn verify_header(secret: &[u8], body: &[u8], header_value: &str, max_age: Duration) -> bool {
        let Some(ts) = parse_t_field(header_value) else {
            return false;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let tolerance_secs = max_age.as_secs();
        if now.abs_diff(ts) > tolerance_secs {
            return false;
        }
        verify(secret, ts, body, header_value)
    }

    #[test]
    fn verify_header_roundtrip() {
        let body = b"{\"hello\":\"world\"}";
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = sign(SECRET, ts, body);
        assert!(verify_header(
            SECRET,
            body,
            &header,
            Duration::from_secs(300)
        ));
    }

    #[test]
    fn verify_header_rejects_old_signature() {
        let body = b"{\"hello\":\"world\"}";
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(3601);
        let header = sign(SECRET, ts, body);
        assert!(!verify_header(
            SECRET,
            body,
            &header,
            Duration::from_secs(300)
        ));
    }

    #[test]
    fn verify_header_rejects_wrong_secret() {
        let body = b"{\"hello\":\"world\"}";
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = sign(SECRET, ts, body);
        assert!(!verify_header(
            b"wrong-secret",
            body,
            &header,
            Duration::from_secs(300)
        ));
    }

    #[test]
    fn verify_header_rejects_missing_t_field() {
        assert!(!verify_header(
            SECRET,
            b"body",
            "v1=aabbcc",
            Duration::from_secs(300)
        ));
    }

    #[test]
    fn verify_header_rejects_future_timestamp() {
        let body = b"{\"hello\":\"world\"}";
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3601;
        let header = sign(SECRET, ts, body);
        assert!(!verify_header(
            SECRET,
            body,
            &header,
            Duration::from_secs(300)
        ));
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use proptest::prelude::*;

    const SECRET: &[u8] = b"super-secret-key-32-bytes-minimum!!";

    proptest! {
        /// A single-byte flip anywhere in the HMAC digest must cause verify to return false.
        ///
        /// Mirrors the dispatcher's `verify_rejects_single_byte_flip` test with
        /// ≥ 256 random inputs to exercise the constant-time comparison path.
        #[test]
        fn verify_rejects_any_single_byte_flip(
            body in prop::collection::vec(any::<u8>(), 1..256_usize),
            flip_char_from in prop::sample::select(vec!['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']),
            flip_char_to  in prop::sample::select(vec!['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']),
        ) {
            prop_assume!(flip_char_from != flip_char_to);

            let ts = 1_714_229_400_u64;
            let header = sign(SECRET, ts, &body);

            // Isolate the v1= hex digest portion and flip only within it,
            // so the t= timestamp field is not disturbed.
            let v1_prefix = "v1=";
            let v1_start = match header.find(v1_prefix) {
                Some(i) => i + v1_prefix.len(),
                None => panic!("sign() produced a header without v1=: {header}"),
            };
            let hex_part = &header[v1_start..];

            // Only attempt the flip if the target character exists in the digest.
            prop_assume!(hex_part.contains(flip_char_from));

            let flipped_hex = hex_part.replacen(flip_char_from, &flip_char_to.to_string(), 1);
            prop_assume!(flipped_hex != hex_part); // sanity: replacen actually changed something

            let flipped_header = format!("{}{flipped_hex}", &header[..v1_start]);

            prop_assert!(!verify(SECRET, ts, &body, &flipped_header));
        }
    }
}
