// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared helper for extracting a top-level string field from a JSON secret.
//!
//! Used by providers that support the `name(secret, field)` shape — `aws_secret`
//! for RDS-style credential blobs.

/// Extract a top-level string field from a JSON secret.
///
/// Returns the field's string content on success. On failure, returns a
/// reason string suitable for `SecretResolveError::ResolutionFailed`.
pub(super) fn extract_json_field(
    secret_string: &str,
    json_key: &str,
    secret_name: &str,
) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(secret_string).map_err(|e| {
        format!(
            "secret '{}' is not valid JSON; cannot extract field '{}': {}",
            secret_name, json_key, e
        )
    })?;

    let field = value
        .get(json_key)
        .ok_or_else(|| format!("secret '{}' has no field '{}'", secret_name, json_key))?;

    match field {
        serde_json::Value::String(s) => Ok(s.clone()),
        _ => Err(format!(
            "field '{}' in secret '{}' is not a string",
            json_key, secret_name
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn extract_json_field_returns_string_value() {
        let secret = r#"{"username":"alice","password":"s3cr3t"}"#;
        assert_eq!(
            extract_json_field(secret, "password", "rds-creds").unwrap(),
            "s3cr3t"
        );
        assert_eq!(
            extract_json_field(secret, "username", "rds-creds").unwrap(),
            "alice"
        );
    }

    #[mz_ore::test]
    fn extract_json_field_errors_on_missing_key() {
        let secret = r#"{"username":"alice"}"#;
        let err = extract_json_field(secret, "password", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("password"));
    }

    #[mz_ore::test]
    fn extract_json_field_errors_on_non_string_value() {
        let secret = r#"{"port":5432}"#;
        let err = extract_json_field(secret, "port", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("port"));
        assert!(err.contains("not a string"));
    }

    #[mz_ore::test]
    fn extract_json_field_errors_on_invalid_json() {
        let secret = "not json at all";
        let err = extract_json_field(secret, "password", "rds-creds").unwrap_err();
        assert!(err.contains("rds-creds"));
        assert!(err.contains("not valid JSON"));
    }
}
