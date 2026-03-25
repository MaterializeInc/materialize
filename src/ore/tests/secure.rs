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

use mz_ore::secure::{SecureString, SecureVec, ZeroizeOnDrop};

#[test]
fn secure_string_round_trip_and_redaction() {
    let s = SecureString::from("super-secret");
    assert_eq!(s.unsecure(), "super-secret");
    assert!(!format!("{:?}", s).contains("super-secret"));
    assert!(!format!("{}", s).contains("super-secret"));
}

#[test]
fn secure_vec_round_trip_and_redaction() {
    let v = SecureVec::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(v.unsecure(), &[0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(!format!("{:?}", v).contains("222")); // 0xDE = 222
}

#[test]
fn secure_string_from_str() {
    let s = SecureString::from("literal");
    assert_eq!(s.unsecure(), "literal");
}

#[test]
fn types_implement_zeroize_on_drop() {
    fn assert_zod<T: ZeroizeOnDrop>() {}
    assert_zod::<SecureString>();
    assert_zod::<SecureVec>();
}
