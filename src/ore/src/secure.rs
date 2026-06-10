// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utilities for handling sensitive data that must be zeroed from memory on drop.
//!
//! This module provides:
//!
//! - Re-exports of [`zeroize`] crate fundamentals ([`Zeroize`], [`ZeroizeOnDrop`],
//!   [`Zeroizing`]) so that downstream crates can depend on `mz-ore` alone.
//!
//! - [`SecureString`]: a `String` wrapper that is zeroed on drop and redacted
//!   in `Debug`/`Display` output. Use for passwords, tokens, and credentials.
//!
//! - [`SecureVec`]: a `Vec<u8>` wrapper that is zeroed on drop and redacted
//!   in `Debug`/`Display` output. Use for raw key material and secret bytes.
//!
//! # When to use
//!
//! Use these types whenever a value contains secret material (passwords, keys,
//! tokens, salts, nonces) that should not linger in process memory after use.
//!
//! # Examples
//!
//! ```
//! use mz_ore::secure::{SecureString, SecureVec, Zeroizing};
//!
//! // Wrap a password — zeroed on drop, redacted in logs
//! let password = SecureString::from("hunter2");
//! assert_eq!(password.unsecure(), "hunter2");
//! assert!(!format!("{:?}", password).contains("hunter2"));
//!
//! // Wrap raw key bytes
//! let key = SecureVec::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
//! assert_eq!(key.unsecure(), &[0xDE, 0xAD, 0xBE, 0xEF]);
//!
//! // Use Zeroizing<T> for temporary buffers
//! let buf = Zeroizing::new([0u8; 32]);
//! ```

use std::fmt;

pub use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// A `String` that is zeroed from memory on drop and redacted in
/// `Debug`/`Display` output.
///
/// `SecureString` intentionally does **not** implement `Clone` to prevent
/// untracked copies of sensitive data. Use [`unsecure`](SecureString::unsecure)
/// to access the inner value when needed, and pass by reference where possible.
#[derive(Zeroize, ZeroizeOnDrop, PartialEq, Eq)]
pub struct SecureString(String);

impl SecureString {
    /// Returns a reference to the inner string.
    ///
    /// Prefer passing `&SecureString` over calling this method, to keep the
    /// secret wrapped as long as possible.
    pub fn unsecure(&self) -> &str {
        &self.0
    }
}

impl From<String> for SecureString {
    fn from(s: String) -> Self {
        SecureString(s)
    }
}

impl From<&str> for SecureString {
    fn from(s: &str) -> Self {
        SecureString(s.to_string())
    }
}

impl fmt::Debug for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecureString(<redacted>)")
    }
}

impl fmt::Display for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// A `Vec<u8>` that is zeroed from memory on drop and redacted in
/// `Debug`/`Display` output.
///
/// `SecureVec` intentionally does **not** implement `Clone` to prevent
/// untracked copies of sensitive data. Use [`unsecure`](SecureVec::unsecure)
/// to access the inner bytes when needed.
#[derive(Zeroize, ZeroizeOnDrop, PartialEq, Eq)]
pub struct SecureVec(Vec<u8>);

impl SecureVec {
    /// Returns a reference to the inner byte slice.
    ///
    /// Prefer passing `&SecureVec` over calling this method, to keep the
    /// secret wrapped as long as possible.
    pub fn unsecure(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for SecureVec {
    fn from(v: Vec<u8>) -> Self {
        SecureVec(v)
    }
}

impl fmt::Debug for SecureVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecureVec(<redacted>)")
    }
}

impl fmt::Display for SecureVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[crate::test]
    fn secure_string_round_trip_and_redaction() {
        let s = SecureString::from("super-secret");
        assert_eq!(s.unsecure(), "super-secret");
        assert!(!format!("{:?}", s).contains("super-secret"));
        assert!(!format!("{}", s).contains("super-secret"));
    }

    #[crate::test]
    fn secure_vec_round_trip_and_redaction() {
        let v = SecureVec::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(v.unsecure(), &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert!(!format!("{:?}", v).contains("222")); // 0xDE = 222
    }

    #[crate::test]
    fn secure_string_from_str() {
        let s = SecureString::from("literal");
        assert_eq!(s.unsecure(), "literal");
    }

    #[crate::test]
    fn types_implement_zeroize_on_drop() {
        fn assert_zod<T: ZeroizeOnDrop>() {}
        assert_zod::<SecureString>();
        assert_zod::<SecureVec>();
    }
}
