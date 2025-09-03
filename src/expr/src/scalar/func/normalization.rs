// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unicode normalization support for string functions.

use uncased::UncasedStr;
use unicode_normalization::UnicodeNormalization;

use crate::EvalError;

/// A Unicode normalization form.
pub trait NormalizationForm {
    /// Applies this normalization form to the input string.
    fn normalize(&self, s: &str) -> String;
}

/// NFC (Canonical Decomposition, followed by Canonical Composition)
struct NfcForm;

impl NormalizationForm for NfcForm {
    fn normalize(&self, s: &str) -> String {
        s.nfc().collect()
    }
}

/// NFD (Canonical Decomposition)
struct NfdForm;

impl NormalizationForm for NfdForm {
    fn normalize(&self, s: &str) -> String {
        s.nfd().collect()
    }
}

/// NFKC (Compatibility Decomposition, followed by Canonical Composition)
struct NfkcForm;

impl NormalizationForm for NfkcForm {
    fn normalize(&self, s: &str) -> String {
        s.nfkc().collect()
    }
}

/// NFKD (Compatibility Decomposition)
struct NfkdForm;

impl NormalizationForm for NfkdForm {
    fn normalize(&self, s: &str) -> String {
        s.nfkd().collect()
    }
}

/// Looks up a normalization form by name.
///
/// Accepts the PostgreSQL-compatible form names: NFC, NFD, NFKC, NFKD.
/// The comparison is case-insensitive.
pub fn lookup_form(s: &str) -> Result<&'static dyn NormalizationForm, EvalError> {
    let s = UncasedStr::new(s);
    if s == "NFC" {
        Ok(&NfcForm)
    } else if s == "NFD" {
        Ok(&NfdForm)
    } else if s == "NFKC" {
        Ok(&NfkcForm)
    } else if s == "NFKD" {
        Ok(&NfkdForm)
    } else {
        Err(EvalError::InvalidParameterValue(
            format!(
                "invalid normalization form '{}': expected one of: NFC, NFD, NFKC, NFKD",
                s.as_str()
            )
            .into(),
        ))
    }
}
