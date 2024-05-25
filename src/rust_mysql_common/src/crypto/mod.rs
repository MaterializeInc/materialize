// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use rand::rngs::OsRng;

pub mod der;
pub mod rsa;

/// Helper function to encrypt mysql password using a public key loaded from a server.
///
/// It will use OAEP padding, so MySql versions prior to 8.0.5 are not supported.
pub fn encrypt(pass: &[u8], key: &[u8]) -> Vec<u8> {
    let pub_key = self::rsa::PublicKey::from_pem(key);
    let pad = self::rsa::Pkcs1OaepPadding::new(OsRng);
    pub_key.encrypt_block(pass, pad)
}
