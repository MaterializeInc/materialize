// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use base64::{engine::general_purpose::STANDARD, Engine};
use num_bigint::BigUint;
use regex::bytes::Regex;
use std::mem::size_of;

/// Type of a der-encoded public key.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum PubKeyFileType {
    Pkcs1,
    Pkcs8,
}

/// Converts pem encoded RSA public key to der.
pub fn pem_to_der(pem: impl AsRef<[u8]>) -> (Vec<u8>, PubKeyFileType) {
    let pkcs1_re = Regex::new(
        "-----BEGIN RSA PUBLIC KEY-----\
         ([^-]*)\
         -----END RSA PUBLIC KEY-----",
    )
    .unwrap();
    let pkcs8_re = Regex::new(
        "-----BEGIN PUBLIC KEY-----\
         ([^-]*)\
         -----END PUBLIC KEY-----",
    )
    .unwrap();

    let (captures, key_file_type) = pkcs1_re
        .captures(pem.as_ref())
        .map(|captures| (captures, PubKeyFileType::Pkcs1))
        .unwrap_or_else(|| {
            pkcs8_re
                .captures(pem.as_ref())
                .map(|captures| (captures, PubKeyFileType::Pkcs8))
                .expect("valid PEM is mandatory here")
        });
    let pem_body = captures.get(1).unwrap().as_bytes();
    let pem_body = pem_body
        .iter()
        .filter(|x| !b" \n\t\r\x0b\x0c".contains(x))
        .cloned()
        .collect::<Vec<_>>();

    let der = STANDARD
        .decode(&*pem_body)
        .expect("valid base64 is mandatory here");

    (der, key_file_type)
}

fn big_uint_to_usize(x: BigUint) -> usize {
    let mut y = 0;
    let x_le = x.to_bytes_le();
    for (i, b) in x_le.into_iter().enumerate().take(size_of::<usize>()) {
        y += (b as usize) << (8 * i);
    }
    y
}

/// der bytes -> (len, rest of der bytes)
fn parse_len(der: &[u8]) -> (BigUint, &[u8]) {
    if der[0] & 0x80 > 0 {
        let len = (der[0] & (!0x80)) as usize;
        (BigUint::from_bytes_be(&der[1..=len]), &der[len + 1..])
    } else {
        (BigUint::from(der[0]), &der[1..])
    }
}

/// der bytes -> (sequence bytes, rest of der bytes)
pub fn parse_sequence(mut der: &[u8]) -> (&[u8], &[u8]) {
    assert_eq!(der[0], 0x30, "expecting SEQUENCE in primitive encoding");
    der = &der[1..];
    let (sequence_len, der) = parse_len(der);
    let sequence_len = big_uint_to_usize(sequence_len);
    (&der[..sequence_len], &der[sequence_len..])
}

/// der bytes -> (unused bits, bytes of bit string, rest of der bytes)
pub fn parse_bit_string(mut der: &[u8]) -> (u8, &[u8], &[u8]) {
    assert_eq!(der[0], 0x03, "expecting BIT STRING in primitive encoding");
    der = &der[1..];
    let (bit_string_len, der) = parse_len(der);
    let bit_string_len = big_uint_to_usize(bit_string_len);
    let unused_bits = der[0];
    (unused_bits, &der[1..bit_string_len], &der[bit_string_len..])
}

/// der bytes -> (uint, rest of der bytes)
pub fn parse_uint(mut der: &[u8]) -> (BigUint, &[u8]) {
    assert_eq!(der[0], 0x02, "expecting INTEGER");
    der = &der[1..];
    let (uint_len, der) = parse_len(der);
    let uint_len = big_uint_to_usize(uint_len);
    let out = BigUint::from_bytes_be(&der[..uint_len]);
    (out, &der[uint_len..])
}

/// Extracts modulus and exponent from pkcs1 der public key representation
pub fn parse_pub_key_pkcs1(der: &[u8]) -> (BigUint, BigUint) {
    let (pub_key_fields, _) = parse_sequence(der);
    let (modulus, pub_key_fields) = parse_uint(pub_key_fields);
    let (exponent, _) = parse_uint(pub_key_fields);
    (modulus, exponent)
}

/// Extracts modulus and exponent from pkcs8 der public key representation
pub fn parse_pub_key_pkcs8(der: &[u8]) -> (BigUint, BigUint) {
    let (seq_data, _) = parse_sequence(der);
    // ignore algorithm
    let (_, der) = parse_sequence(seq_data);
    let (unused_bits, pub_key, _) = parse_bit_string(der);
    assert_eq!(unused_bits, 0, "expecting no unused bits");
    parse_pub_key_pkcs1(pub_key)
}

/// Extracts modulus and exponent from specified der public key representation
pub fn parse_pub_key(der: &[u8], file_type: PubKeyFileType) -> (BigUint, BigUint) {
    match file_type {
        PubKeyFileType::Pkcs1 => parse_pub_key_pkcs1(der),
        PubKeyFileType::Pkcs8 => parse_pub_key_pkcs8(der),
    }
}

#[test]
fn test_pem_to_der() {
    const PEM_DATA: &[u8] = br"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxSKOcxiet8lLMn8ImyUE
bGGKob5EdRz/4wdiw12ED0GfKKTKhVnodFCfm1mdy7bKOX5QxL9skrvYodpW43eR
R5bfOzIgy1qIB8RYb6qOXRBw1oA4snBDqtUjDv/lbHLJN+IbzM4oU+e3Lt9rXyLX
VY289ewONPweXHqSCnTL91w+wkU1peIFV2QhZ+upUCdCtwOn5hnJPNgxtbklFoya
C8W3Z7Xx7He2QDJsEWAqX197efw0L6j8X8Tyd8Uwb7zUB1tfMGhHfm9EwejPAtzx
4GztQNtNMtGS2oGZLQBLV9hib4dDL92iiZeckg2LAf4GsJofLLR8mcHCRoqVbQJ1
YQIDAQAB
-----END PUBLIC KEY-----";

    let (_der, key_type) = pem_to_der(PEM_DATA);

    assert_eq!(key_type, PubKeyFileType::Pkcs8);
}
