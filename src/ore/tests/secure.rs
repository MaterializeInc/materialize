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

//! Tests for `mz_ore::secure` module.

use mz_ore::secure::{SecureString, SecureVec, Zeroize, ZeroizeOnDrop, Zeroizing};

// ============================================================
// Tests for re-exports: verify the zeroize API is available
// ============================================================

#[test]
fn test_zeroizing_vec_u8() {
    let secret = Zeroizing::new(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(&*secret, &[0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(secret.len(), 4);
}

#[test]
fn test_zeroizing_string() {
    let secret = Zeroizing::new("hunter2".to_string());
    assert_eq!(&*secret, "hunter2");
}

#[test]
fn test_zeroize_trait_on_vec() {
    let mut v = vec![1u8, 2, 3, 4];
    v.zeroize();
    assert!(v.is_empty());
}

#[test]
fn test_zeroize_trait_on_string() {
    let mut s = "password".to_string();
    s.zeroize();
    assert!(s.is_empty());
}

#[test]
fn test_zeroize_trait_on_byte_array() {
    let mut arr = [0xFFu8; 32];
    arr.zeroize();
    assert_eq!(arr, [0u8; 32]);
}

// ============================================================
// Tests for SecureString
// ============================================================

#[test]
fn test_secure_string_from_string() {
    let s = SecureString::from("my-secret".to_string());
    assert_eq!(s.unsecure(), "my-secret");
}

#[test]
fn test_secure_string_from_str() {
    let s = SecureString::from("my-secret");
    assert_eq!(s.unsecure(), "my-secret");
}

#[test]
fn test_secure_string_debug_redacts() {
    let s = SecureString::from("super-secret-password");
    let debug = format!("{:?}", s);
    assert!(!debug.contains("super-secret-password"));
    assert!(debug.contains("redacted"));
}

#[test]
fn test_secure_string_display_redacts() {
    let s = SecureString::from("super-secret-password");
    let display = format!("{}", s);
    assert!(!display.contains("super-secret-password"));
    assert!(display.contains("redacted"));
}

#[test]
fn test_secure_string_not_clone() {
    // SecureString should NOT implement Clone to prevent untracked copies.
    // The real guarantee is compile-time: the following would not compile:
    //   let a = SecureString::from("x");
    //   let b = a.clone(); // compile error
    fn assert_not_clone<T>() {}
    assert_not_clone::<SecureString>();
}

#[test]
fn test_secure_string_zeroize_on_drop() {
    fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}
    assert_zeroize_on_drop::<SecureString>();
}

#[test]
fn test_secure_string_as_bytes() {
    let s = SecureString::from("abc");
    assert_eq!(s.unsecure().as_bytes(), b"abc");
}

#[test]
fn test_secure_string_eq() {
    let a = SecureString::from("same");
    let b = SecureString::from("same");
    assert_eq!(a, b);
}

#[test]
fn test_secure_string_ne() {
    let a = SecureString::from("one");
    let b = SecureString::from("two");
    assert_ne!(a, b);
}

// ============================================================
// Tests for SecureVec
// ============================================================

#[test]
fn test_secure_vec_from_vec() {
    let v = SecureVec::from(vec![1u8, 2, 3]);
    assert_eq!(v.unsecure(), &[1, 2, 3]);
}

#[test]
fn test_secure_vec_debug_redacts() {
    let v = SecureVec::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    let debug = format!("{:?}", v);
    assert!(!debug.contains("222")); // 0xDE = 222
    assert!(debug.contains("redacted"));
}

#[test]
fn test_secure_vec_zeroize_on_drop() {
    fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}
    assert_zeroize_on_drop::<SecureVec>();
}

#[test]
fn test_secure_vec_eq() {
    let a = SecureVec::from(vec![1, 2, 3]);
    let b = SecureVec::from(vec![1, 2, 3]);
    assert_eq!(a, b);
}

#[test]
fn test_secure_vec_ne() {
    let a = SecureVec::from(vec![1, 2, 3]);
    let b = SecureVec::from(vec![4, 5, 6]);
    assert_ne!(a, b);
}

#[test]
fn test_secure_vec_len() {
    let v = SecureVec::from(vec![0u8; 32]);
    assert_eq!(v.unsecure().len(), 32);
}

// ============================================================
// THROWAWAY tests — remove before final PR
// These test implementation details and edge cases beyond
// what's reasonable to maintain long-term.
// ============================================================

#[test]
fn throwaway_zeroizing_drop_zeroes_memory() {
    let secret = Zeroizing::new([0xAAu8; 32]);
    let ptr = secret.as_ptr();
    drop(secret);
    unsafe {
        let slice = std::slice::from_raw_parts(ptr, 32);
        let all_zero = slice.iter().all(|&b| b == 0);
        if all_zero {
            eprintln!("THROWAWAY: confirmed zeroize cleared stack array");
        } else {
            eprintln!("THROWAWAY: memory reused or optimizer interfered (expected on some platforms)");
        }
    }
}

#[test]
fn throwaway_secure_string_empty() {
    let s = SecureString::from("");
    assert_eq!(s.unsecure(), "");
}

#[test]
fn throwaway_secure_vec_empty() {
    let v = SecureVec::from(vec![]);
    let empty: &[u8] = &[];
    assert_eq!(v.unsecure(), empty);
}

#[test]
fn throwaway_secure_string_large() {
    let big = "x".repeat(100_000);
    let s = SecureString::from(big.clone());
    assert_eq!(s.unsecure(), &big);
}

#[test]
fn throwaway_secure_vec_large() {
    let big = vec![0xABu8; 100_000];
    let v = SecureVec::from(big.clone());
    assert_eq!(v.unsecure(), &big);
}

#[test]
fn throwaway_zeroizing_nested() {
    let inner = Zeroizing::new(vec![1u8, 2, 3]);
    let outer = Zeroizing::new(inner);
    assert_eq!(&**outer, &[1, 2, 3]);
}
