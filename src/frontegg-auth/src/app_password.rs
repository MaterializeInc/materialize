// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

/// The prefix that identifies an app password as a Materialize password.
pub const PREFIX: &str = "mzp_";

/// A Materialize app password.
///
/// Somewhat unusually, the app password encodes both the client ID and secret
/// for the API key in use. Both the client ID and secret are UUIDs. The
/// password can have one of two formats:
///
///   * The URL-safe base64 encoding of the concatenated bytes of the UUIDs.
///
///     This format is a very compact representation (only 43 or 44 bytes)
///     that is safe to use in a connection string without escaping.
///
///   * The concatenated hex-encoding of the UUIDs, with any number of
///     special characters that are ignored.
///
///     This format allows for the UUIDs to be formatted with hyphens, or
///     not.
///
pub struct AppPassword {
    /// The client ID embedded in the app password.
    pub client_id: Uuid,
    /// The secret key embedded in the app password.
    pub secret_key: Uuid,
}

impl fmt::Display for AppPassword {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut buf = vec![];
        buf.extend(self.client_id.as_bytes());
        buf.extend(self.secret_key.as_bytes());
        let encoded = base64::encode_config(buf, base64::URL_SAFE_NO_PAD);
        f.write_str(PREFIX)?;
        f.write_str(&encoded)
    }
}

impl FromStr for AppPassword {
    type Err = AppPasswordParseError;

    fn from_str(password: &str) -> Result<AppPassword, AppPasswordParseError> {
        let password = password
            .strip_prefix(PREFIX)
            .ok_or(AppPasswordParseError)?;
        if password.len() == 43 || password.len() == 44 {
            // If it's exactly 43 or 44 bytes, assume we have base64-encoded
            // UUID bytes without or with padding, respectively.
            let buf = base64::decode_config(password, base64::URL_SAFE)
                .map_err(|_| AppPasswordParseError)?;
            let client_id =
                Uuid::from_slice(&buf[..16]).map_err(|_| AppPasswordParseError)?;
            let secret_key =
                Uuid::from_slice(&buf[16..]).map_err(|_| AppPasswordParseError)?;
            Ok(AppPassword {
                client_id,
                secret_key,
            })
        } else if password.len() >= 64 {
            // If it's more than 64 bytes, assume we have concatenated
            // hex-encoded UUIDs, possibly with some special characters mixed
            // in.
            let mut chars = password.chars().filter(|c| c.is_alphanumeric());
            let client_id = Uuid::parse_str(&chars.by_ref().take(32).collect::<String>())
                .map_err(|_| AppPasswordParseError)?;
            let secret_key = Uuid::parse_str(&chars.take(32).collect::<String>())
                .map_err(|_| AppPasswordParseError)?;
            Ok(AppPassword {
                client_id,
                secret_key,
            })
        } else {
            // Otherwise it's definitely not a password format we understand.
            Err(AppPasswordParseError)
        }
    }
}

/// An error while parsing an [`AppPassword`].
#[derive(Debug)]
pub struct AppPasswordParseError;

impl Error for AppPasswordParseError {}

impl fmt::Display for AppPasswordParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("invalid app password format")
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::AppPassword;

    #[test]
    fn test_app_password() {
        struct TestCase {
            input: &'static str,
            expected_output: &'static str,
            expected_client_id: Uuid,
            expected_secret_key: Uuid,
        }

        for tc in [
            TestCase {
                input: "mzp_7ce3c1e8ea854594ad5d785f17d1736f1947fdcef5404adb84a47347e5d30c9f",
                expected_output: "mzp_fOPB6OqFRZStXXhfF9FzbxlH_c71QErbhKRzR-XTDJ8",
                expected_client_id: "7ce3c1e8-ea85-4594-ad5d-785f17d1736f".parse().unwrap(),
                expected_secret_key: "1947fdce-f540-4adb-84a4-7347e5d30c9f".parse().unwrap(),
            },
            TestCase {
                input: "mzp_fOPB6OqFRZStXXhfF9FzbxlH_c71QErbhKRzR-XTDJ8",
                expected_output: "mzp_fOPB6OqFRZStXXhfF9FzbxlH_c71QErbhKRzR-XTDJ8",
                expected_client_id: "7ce3c1e8-ea85-4594-ad5d-785f17d1736f".parse().unwrap(),
                expected_secret_key: "1947fdce-f540-4adb-84a4-7347e5d30c9f".parse().unwrap(),
            },
            TestCase {
                input: "mzp_0445db36-5826-41af-84f6-e09402fc6171:a0c11434-07ba-426a-b83d-cc4f192325a3",
                expected_output: "mzp_BEXbNlgmQa-E9uCUAvxhcaDBFDQHukJquD3MTxkjJaM",
                expected_client_id: "0445db36-5826-41af-84f6-e09402fc6171".parse().unwrap(),
                expected_secret_key: "a0c11434-07ba-426a-b83d-cc4f192325a3".parse().unwrap(),
            },
        ] {
            let app_password: AppPassword = tc.input.parse().unwrap();
            assert_eq!(app_password.to_string(), tc.expected_output);
            assert_eq!(app_password.client_id, tc.expected_client_id);
            assert_eq!(app_password.secret_key, tc.expected_secret_key);
        }
    }
}
