// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use derivative::Derivative;

use mz_license_keys::ValidatedLicenseKey;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct LicenseKeyConfig {
    pub max_credit_consumption_rate: f64,
    pub allow_credit_consumption_override: bool,
}

impl LicenseKeyConfig {
    pub fn for_tests() -> Self {
        Self {
            allow_credit_consumption_override: true,
            ..Default::default()
        }
    }

    pub fn max_credit_consumption_rate(&self) -> Option<f64> {
        if self.allow_credit_consumption_override {
            None
        } else {
            Some(self.max_credit_consumption_rate)
        }
    }
}

impl From<ValidatedLicenseKey> for LicenseKeyConfig {
    fn from(value: ValidatedLicenseKey) -> Self {
        Self {
            max_credit_consumption_rate: value.max_credit_consumption_rate,
            allow_credit_consumption_override: value.allow_credit_consumption_override,
        }
    }
}

impl Default for LicenseKeyConfig {
    fn default() -> Self {
        Self {
            max_credit_consumption_rate: 24.0,
            allow_credit_consumption_override: false,
        }
    }
}
