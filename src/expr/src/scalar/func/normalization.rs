// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unicode normalization support for string functions.

use unicode_normalization::UnicodeNormalization;

use super::NormalizationForm;

impl NormalizationForm {
    /// Applies this normalization form to the input string.
    pub fn normalize(&self, s: &str) -> String {
        match self {
            NormalizationForm::Nfc => s.nfc().collect(),
            NormalizationForm::Nfd => s.nfd().collect(),
            NormalizationForm::Nfkc => s.nfkc().collect(),
            NormalizationForm::Nfkd => s.nfkd().collect(),
        }
    }
}
