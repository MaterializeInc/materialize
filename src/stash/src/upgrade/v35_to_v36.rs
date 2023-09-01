// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod v35 {
    include!(concat!(env!("OUT_DIR"), "/objects_v35.rs"));
}

pub mod v36 {
    include!(concat!(env!("OUT_DIR"), "/objects_v36.rs"));
}

wire_compatible!(v35::ServerConfigurationKey with v36::ServerConfigurationKey);
wire_compatible!(v35::ServerConfigurationValue with v36::ServerConfigurationValue);

const SYSTEM_CONFIGURATION_COLLECTION: TypedCollection<
    v35::ServerConfigurationKey,
    v35::ServerConfigurationValue,
> = TypedCollection::new("system_configuration");

const ENABLE_LD_RBAC_CHECKS_NAME: &str = "enable_ld_rbac_checks";
const ENABLE_RBAC_CHECKS_NAME: &str = "enable_rbac_checks";

/// Persist `false` for existing environments' RBAC flags, iff they're not already set.
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    SYSTEM_CONFIGURATION_COLLECTION
        .migrate_to(
            tx,
            |entries: &BTreeMap<v35::ServerConfigurationKey, v35::ServerConfigurationValue>| {
                let mut updates = Vec::with_capacity(entries.len());
                let mut found_ld_flag = false;
                let mut found_user_flag = false;

                for (key, value) in entries {
                    if key.name == ENABLE_LD_RBAC_CHECKS_NAME {
                        found_ld_flag = true;
                    } else if key.name == ENABLE_RBAC_CHECKS_NAME {
                        found_user_flag = true;
                    }

                    let new_key: v36::ServerConfigurationKey = WireCompatible::convert(key);
                    let new_value: v36::ServerConfigurationValue = WireCompatible::convert(value);
                    updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
                }

                if !found_ld_flag {
                    updates.push(MigrationAction::Insert(
                        v36::ServerConfigurationKey {
                            name: ENABLE_LD_RBAC_CHECKS_NAME.to_string(),
                        },
                        v36::ServerConfigurationValue {
                            value: false.to_string(),
                        },
                    ));
                }

                if !found_user_flag {
                    updates.push(MigrationAction::Insert(
                        v36::ServerConfigurationKey {
                            name: ENABLE_RBAC_CHECKS_NAME.to_string(),
                        },
                        v36::ServerConfigurationValue {
                            value: false.to_string(),
                        },
                    ));
                }

                updates
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use crate::DebugStashFactory;

    use super::*;

    const SYSTEM_CONFIGURATION_COLLECTION_V36: TypedCollection<
        v36::ServerConfigurationKey,
        v36::ServerConfigurationValue,
    > = TypedCollection::new("system_configuration");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_existing_flags() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        SYSTEM_CONFIGURATION_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        v35::ServerConfigurationKey {
                            name: ENABLE_LD_RBAC_CHECKS_NAME.to_string(),
                        },
                        v35::ServerConfigurationValue {
                            value: true.to_string(),
                        },
                    ),
                    (
                        v35::ServerConfigurationKey {
                            name: ENABLE_RBAC_CHECKS_NAME.to_string(),
                        },
                        v35::ServerConfigurationValue {
                            value: true.to_string(),
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut system_configs: Vec<_> = SYSTEM_CONFIGURATION_COLLECTION_V36
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key.name, value.value))
            .collect();
        system_configs.sort();

        assert_eq!(
            system_configs,
            vec![
                (ENABLE_LD_RBAC_CHECKS_NAME.to_string(), true.to_string()),
                (ENABLE_RBAC_CHECKS_NAME.to_string(), true.to_string()),
            ]
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_empty_flags() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        SYSTEM_CONFIGURATION_COLLECTION
            .insert_without_overwrite(&mut stash, vec![])
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let mut system_configs: Vec<_> = SYSTEM_CONFIGURATION_COLLECTION_V36
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key.name, value.value))
            .collect();
        system_configs.sort();

        assert_eq!(
            system_configs,
            vec![
                (ENABLE_LD_RBAC_CHECKS_NAME.to_string(), false.to_string()),
                (ENABLE_RBAC_CHECKS_NAME.to_string(), false.to_string()),
            ]
        );
    }
}
