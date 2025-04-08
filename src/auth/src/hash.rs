// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::num::NonZeroU32;

use base64::prelude::*;
use ring::digest::SHA256_OUTPUT_LEN;
use ring::hmac::{self, Key, HMAC_SHA256};
use ring::pbkdf2::{self, PBKDF2_HMAC_SHA256 as SHA256};
use ring::rand::SecureRandom;

/// The default iteration count as suggested by
/// https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
const DEFAULT_ITERATIONS: NonZeroU32 = NonZeroU32::new(600_000).unwrap();

/// The default salt size, not currently configurable
const DEFAULT_SALT_SIZE: usize = 32;

/// The options for hashing a password
pub struct HashOpts {
    /// The number of iterations to use for PBKDF2
    pub iterations: NonZeroU32,
    /// The salt to use for PBKDF2. It is up to the caller to
    /// ensure that however the salt is generated, it is cryptographically
    /// secure.
    pub salt: [u8; DEFAULT_SALT_SIZE],
}

pub struct PasswordHash {
    /// The salt used for hashing
    pub salt: [u8; DEFAULT_SALT_SIZE],
    /// The number of iterations used for hashing
    pub iterations: NonZeroU32,
    /// The hash of the password.
    /// This is the result of PBKDF2 with SHA256
    pub hash: [u8; SHA256_OUTPUT_LEN],
}

#[derive(Debug)]
pub enum VerifyError {
    MalformedHash,
    InvalidPassword,
}

/// Hashes a password using PBKDF2 with SHA256
/// and a random salt.
pub fn hash_password(password: &String) -> PasswordHash {
    let mut salt = [0u8; 32];
    ring::rand::SystemRandom::new().fill(&mut salt).unwrap();

    let hash = hash_password_inner(
        &HashOpts {
            iterations: DEFAULT_ITERATIONS,
            salt,
        },
        password.as_bytes(),
    );

    PasswordHash {
        salt,
        iterations: DEFAULT_ITERATIONS,
        hash,
    }
}

/// Hashes a password using PBKDF2 with SHA256
/// and the given options.
pub fn hash_password_with_opts(opts: &HashOpts, password: &String) -> PasswordHash {
    let hash = hash_password_inner(opts, password.as_bytes());

    PasswordHash {
        salt: opts.salt,
        iterations: opts.iterations,
        hash,
    }
}

/// Hashes a password using PBKDF2 with SHA256,
/// and returns it in the SCRAM-SHA-256 format.
/// The format is SCRAM-SHA-256$<iterations>:<salt>$<client_key>:<server_key>
pub fn scram256_hash(password: &String) -> String {
    let hashed_password = hash_password(password);
    scram256_hash_inner(hashed_password).to_string()
}

/// Verifies a password against a SCRAM-SHA-256 hash.
pub fn scram256_verify(password: &String, hashed_password: &str) -> Result<(), VerifyError> {
    let opts = scram256_parse_opts(hashed_password)?;
    let hashed = hash_password_with_opts(&opts, password);
    let scram = scram256_hash_inner(hashed);
    if *hashed_password == scram.to_string() {
        Ok(())
    } else {
        Err(VerifyError::InvalidPassword)
    }
}

/// Parses a SCRAM-SHA-256 hash and returns the options used to create it.
fn scram256_parse_opts(hashed_password: &str) -> Result<HashOpts, VerifyError> {
    let parts: Vec<&str> = hashed_password.split('$').collect();
    if parts.len() != 3 {
        return Err(VerifyError::MalformedHash);
    }
    let scheme = parts[0];
    if scheme != "SCRAM-SHA-256" {
        return Err(VerifyError::MalformedHash);
    }
    let auth_info = parts[1].split(':').collect::<Vec<&str>>();
    if auth_info.len() != 2 {
        return Err(VerifyError::MalformedHash);
    }
    let auth_value = parts[2].split(':').collect::<Vec<&str>>();
    if auth_value.len() != 2 {
        return Err(VerifyError::MalformedHash);
    }

    let iterations = auth_info[0]
        .parse::<u32>()
        .map_err(|_| VerifyError::MalformedHash)?;

    let salt = BASE64_STANDARD
        .decode(auth_info[1])
        .map_err(|_| VerifyError::MalformedHash)?;

    let salt = salt.try_into().map_err(|_| VerifyError::MalformedHash)?;

    Ok(HashOpts {
        iterations: NonZeroU32::new(iterations).ok_or(VerifyError::MalformedHash)?,
        salt,
    })
}

/// The SCRAM-SHA-256 hash
struct ScramSha256Hash {
    /// The number of iterations used for hashing
    iterations: NonZeroU32,
    /// The salt used for hashing
    salt: [u8; 32],
    /// The server key
    server_key: [u8; SHA256_OUTPUT_LEN],
    /// The client key
    client_key: [u8; SHA256_OUTPUT_LEN],
}

impl Display for ScramSha256Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SCRAM-SHA-256${}:{}${}:{}",
            self.iterations,
            BASE64_STANDARD.encode(&self.salt),
            BASE64_STANDARD.encode(&self.client_key),
            BASE64_STANDARD.encode(&self.server_key)
        )
    }
}

fn scram256_hash_inner(hashed_password: PasswordHash) -> ScramSha256Hash {
    let signing_key = Key::new(HMAC_SHA256, &hashed_password.hash);
    let client_key = hmac::sign(&signing_key, b"Client Key");
    let server_key = hmac::sign(&signing_key, b"Server Key");

    ScramSha256Hash {
        iterations: hashed_password.iterations,
        salt: hashed_password.salt,
        server_key: server_key.as_ref().try_into().unwrap(),
        client_key: client_key.as_ref().try_into().unwrap(),
    }
}

fn hash_password_inner(opts: &HashOpts, password: &[u8]) -> [u8; SHA256_OUTPUT_LEN] {
    let mut salted_password = [0u8; SHA256_OUTPUT_LEN];
    pbkdf2::derive(
        SHA256,
        opts.iterations,
        &opts.salt,
        password,
        &mut salted_password,
    );
    salted_password
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_hash_password() {
        let password = "password".to_string();
        let hashed_password = hash_password(&password);
        assert_eq!(hashed_password.iterations, DEFAULT_ITERATIONS);
        assert_eq!(hashed_password.salt.len(), DEFAULT_SALT_SIZE);
        assert_eq!(hashed_password.hash.len(), SHA256_OUTPUT_LEN);
    }

    #[mz_ore::test]
    fn test_scram256_hash() {
        let password = "password".to_string();
        let scram_hash = scram256_hash(&password);

        let res = scram256_verify(&password, &scram_hash);
        assert!(res.is_ok());
        let res = scram256_verify(&"wrong_password".to_string(), &scram_hash);
        assert!(res.is_err());
    }

    #[mz_ore::test]
    fn test_scram256_parse_opts() {
        let salt = "9bkIQQjQ7f1OwPsXZGC/YfIkbZsOMDXK0cxxvPBaSfM=";
        let hashed_password = format!("SCRAM-SHA-256$600000:{}$client-key:server-key", salt);
        let opts = scram256_parse_opts(&hashed_password);

        assert!(opts.is_ok());
        let opts = opts.unwrap();
        assert_eq!(opts.iterations, DEFAULT_ITERATIONS);
        assert_eq!(opts.salt.len(), DEFAULT_SALT_SIZE);
        let decoded_salt = BASE64_STANDARD.decode(salt).expect("Failed to decode salt");
        assert_eq!(opts.salt, decoded_salt.as_ref());
    }
}
