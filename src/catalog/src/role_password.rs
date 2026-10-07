// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Password preparation for logical catalog operations, before candidate retries.

use std::num::NonZeroU32;

use mz_auth::password::Password;
use mz_sql::catalog::{PasswordAction, RoleAttributes, RoleAttributesRaw};

/// A SCRAM verifier with redacted debug output and zeroization on drop.
/// Cloning reuses the verifier, including its salt, without hashing again.
#[derive(Debug, Clone)]
pub struct PreparedPassword(Password);

impl PreparedPassword {
    /// Hash with the selected policy once per logical password operation.
    /// Retries must clone the result rather than call this constructor again.
    pub fn new(password: &Password, iterations: NonZeroU32) -> Self {
        Self(
            mz_auth::hash::scram256_hash(password, &iterations)
                .expect("password hash should be valid")
                .into(),
        )
    }

    pub(crate) fn into_verifier(self) -> String {
        // Only the durable authentication record receives an unredacted copy.
        self.0.as_str().to_owned()
    }
}

/// CREATE attributes whose password has already been prepared.
/// Construct once before retries, and reconstruct if replanning changes the input.
#[derive(Debug, Clone)]
pub struct PreparedRoleAttributes {
    pub attributes: RoleAttributes,
    pub(crate) password: Option<PreparedPassword>,
}

impl From<RoleAttributesRaw> for PreparedRoleAttributes {
    fn from(raw: RoleAttributesRaw) -> Self {
        let password = raw.password.as_ref().map(|password| {
            let iterations = raw.scram_iterations.unwrap_or_else(|| {
                mz_ore::soft_panic_or_log!(
                    "Hash iterations must be set if a password is provided."
                );
                // Missing policy must never weaken password storage.
                NonZeroU32::new(600_000).expect("known valid")
            });
            PreparedPassword::new(password, iterations)
        });
        Self {
            attributes: raw.into(),
            password,
        }
    }
}

/// A prepared ALTER action. NoChange must not touch the authentication record.
#[derive(Debug, Clone)]
pub enum PreparedPasswordAction {
    Set(PreparedPassword),
    Clear,
    NoChange,
}

impl From<PasswordAction> for PreparedPasswordAction {
    fn from(action: PasswordAction) -> Self {
        match action {
            PasswordAction::Set(config) => Self::Set(PreparedPassword::new(
                &config.password,
                config.scram_iterations,
            )),
            PasswordAction::Clear => Self::Clear,
            PasswordAction::NoChange => Self::NoChange,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Op;
    use mz_auth::hash::{scram256_parse_opts, scram256_verify};
    use mz_repr::role_id::RoleId;
    use mz_sql::catalog::RoleVars;

    #[mz_ore::test]
    fn preparation_preserves_policy_and_redacts_secrets() {
        let selected = NonZeroU32::new(4096).unwrap();
        let current = NonZeroU32::new(8192).unwrap();
        let mut raw = RoleAttributesRaw::new();
        raw.password = Some("create-secret".into());
        raw.scram_iterations = Some(selected);
        let create = Op::CreateRole {
            name: "role".into(),
            attributes: raw.clone().into(),
        };
        assert!(!format!("{create:?}").contains("create-secret"));
        let Op::CreateRole { attributes, .. } = create else {
            unreachable!()
        };
        let verifier = attributes.password.unwrap().into_verifier();
        assert_eq!(scram256_parse_opts(&verifier).unwrap().iterations, selected);
        scram256_verify(raw.password.as_ref().unwrap(), &verifier).unwrap();

        // Replanning with a different password or policy prepares new input.
        for (policy, expected) in [(Some(selected), selected), (None, current)] {
            raw.password = Some("alter-secret".into());
            raw.scram_iterations = policy;
            let alter = Op::alter_role(
                RoleId::User(1),
                "role".into(),
                raw.clone(),
                false,
                RoleVars::default(),
                current,
            );
            let debug = format!("{alter:?}");
            assert!(!debug.contains("alter-secret"));
            assert!(!debug.contains("SCRAM-SHA-256"));
            let Op::AlterRole {
                password: PreparedPasswordAction::Set(password),
                ..
            } = alter
            else {
                unreachable!()
            };
            let verifier = password.into_verifier();
            assert!(!debug.contains(&verifier));
            assert_eq!(scram256_parse_opts(&verifier).unwrap().iterations, expected);
            scram256_verify(&"alter-secret".into(), &verifier).unwrap();
            assert!(scram256_verify(&"create-secret".into(), &verifier).is_err());
        }
        let clear = Op::alter_role(
            RoleId::User(1),
            "role".into(),
            raw,
            true,
            RoleVars::default(),
            current,
        );
        assert!(matches!(
            clear,
            Op::AlterRole {
                password: PreparedPasswordAction::Clear,
                ..
            }
        ));
        let unchanged = Op::alter_role(
            RoleId::User(1),
            "role".into(),
            RoleAttributesRaw::new(),
            false,
            RoleVars::default(),
            current,
        );
        assert!(matches!(
            unchanged,
            Op::AlterRole {
                password: PreparedPasswordAction::NoChange,
                ..
            }
        ));
    }

    #[mz_ore::test]
    fn create_missing_policy_has_secure_fallback() {
        let mut raw = RoleAttributesRaw::new();
        raw.password = Some("secret".into());
        let prepared = std::panic::catch_unwind(|| PreparedRoleAttributes::from(raw));
        if mz_ore::assert::soft_assertions_enabled() {
            assert!(prepared.is_err());
        } else {
            let verifier = prepared.unwrap().password.unwrap().into_verifier();
            assert_eq!(
                scram256_parse_opts(&verifier).unwrap().iterations.get(),
                600_000
            );
            scram256_verify(&"secret".into(), &verifier).unwrap();
        }
    }
}
