// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::{wire_compatible, MigrationAction};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;
use std::string::ToString;

use crate::durable::upgrade::{objects_v44 as v44, objects_v45 as v45};

wire_compatible!(v44::ServerConfigurationKey with v45::ServerConfigurationKey);
wire_compatible!(v44::ServerConfigurationValue with v45::ServerConfigurationValue);
wire_compatible!(v44::ConfigKey with v45::ConfigKey);
wire_compatible!(v44::ConfigValue with v45::ConfigValue);

const SYSTEM_CONFIGURATION_COLLECTION: TypedCollection<
    v44::ServerConfigurationKey,
    v44::ServerConfigurationValue,
> = TypedCollection::new("system_configuration");
const CONFIG_COLLECTION: TypedCollection<v44::ConfigKey, v44::ConfigValue> =
    TypedCollection::new("config");

const SYSTEM_CONFIG_KEY: &str = "config_has_synced_once";
const CONFIG_KEY: &str = "system_config_synced";

/// Migrate LD sync flag from the System Configuration Collection to the Config Collection.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    let system_config_key = v44::ServerConfigurationKey {
        name: SYSTEM_CONFIG_KEY.to_string(),
    };
    let mut system_config_value = false;

    SYSTEM_CONFIGURATION_COLLECTION
        .migrate_to(tx, |entries| {
            let mut actions: Vec<
                MigrationAction<
                    v44::ServerConfigurationKey,
                    v45::ServerConfigurationKey,
                    v45::ServerConfigurationValue,
                >,
            > = Vec::new();
            let value: Option<&v44::ServerConfigurationValue> = entries.get(&system_config_key);
            if let Some(value) = value {
                system_config_value = &value.value == "on";
                actions.push(MigrationAction::Delete(system_config_key));
            }
            actions
        })
        .await?;

    let config_key = v45::ConfigKey {
        key: CONFIG_KEY.to_string(),
    };
    let config_value = v45::ConfigValue {
        value: if system_config_value { 1 } else { 0 },
    };

    CONFIG_COLLECTION
        .migrate_to(tx, |_| {
            vec![MigrationAction::Insert(config_key, config_value)]
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_stash::Stash;

    const SYSTEM_CONFIGURATION_COLLECTION_V45: TypedCollection<
        v45::ServerConfigurationKey,
        v45::ServerConfigurationValue,
    > = TypedCollection::new("system_configuration");
    const CONFIG_COLLECTION_V45: TypedCollection<v45::ConfigKey, v45::ConfigValue> =
        TypedCollection::new("config");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_on_migration() {
        Stash::with_debug_stash(|mut stash| async move {
            SYSTEM_CONFIGURATION_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [
                        (
                            v44::ServerConfigurationKey {
                                name: SYSTEM_CONFIG_KEY.to_string(),
                            },
                            v44::ServerConfigurationValue {
                                value: "on".to_string(),
                            },
                        ),
                        (
                            v44::ServerConfigurationKey {
                                name: "foo".to_string(),
                            },
                            v44::ServerConfigurationValue {
                                value: "bar".to_string(),
                            },
                        ),
                    ],
                )
                .await
                .expect("insert success");
            CONFIG_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [(
                        v44::ConfigKey {
                            key: "joe".to_string(),
                        },
                        v44::ConfigValue { value: 42 },
                    )],
                )
                .await
                .expect("insert success");

            // Run the migration.
            stash
                .with_transaction(|tx| {
                    Box::pin(async move {
                        upgrade(&tx).await?;
                        Ok(())
                    })
                })
                .await
                .expect("transaction failed");

            let system_configs: Vec<_> = SYSTEM_CONFIGURATION_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.name, value.value))
                .collect();
            assert_eq!(
                system_configs.as_slice(),
                [("foo".to_string(), "bar".to_string())]
            );

            let configs: Vec<_> = CONFIG_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.key, value.value))
                .collect();
            assert_eq!(
                configs.as_slice(),
                [
                    ("joe".to_string(), 42),
                    ("system_config_synced".to_string(), 1)
                ]
            )
        })
        .await
        .expect("stash failed");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_off_migration() {
        Stash::with_debug_stash(|mut stash| async move {
            SYSTEM_CONFIGURATION_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [
                        (
                            v44::ServerConfigurationKey {
                                name: SYSTEM_CONFIG_KEY.to_string(),
                            },
                            v44::ServerConfigurationValue {
                                value: "off".to_string(),
                            },
                        ),
                        (
                            v44::ServerConfigurationKey {
                                name: "foo".to_string(),
                            },
                            v44::ServerConfigurationValue {
                                value: "bar".to_string(),
                            },
                        ),
                    ],
                )
                .await
                .expect("insert success");
            CONFIG_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [(
                        v44::ConfigKey {
                            key: "joe".to_string(),
                        },
                        v44::ConfigValue { value: 42 },
                    )],
                )
                .await
                .expect("insert success");

            // Run the migration.
            stash
                .with_transaction(|tx| {
                    Box::pin(async move {
                        upgrade(&tx).await?;
                        Ok(())
                    })
                })
                .await
                .expect("transaction failed");

            let system_configs: Vec<_> = SYSTEM_CONFIGURATION_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.name, value.value))
                .collect();
            assert_eq!(
                system_configs.as_slice(),
                [("foo".to_string(), "bar".to_string())]
            );

            let configs: Vec<_> = CONFIG_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.key, value.value))
                .collect();
            assert_eq!(
                configs.as_slice(),
                [
                    ("joe".to_string(), 42),
                    ("system_config_synced".to_string(), 0)
                ]
            )
        })
        .await
        .expect("stash failed");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_missing_migration() {
        Stash::with_debug_stash(|mut stash| async move {
            SYSTEM_CONFIGURATION_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [(
                        v44::ServerConfigurationKey {
                            name: "foo".to_string(),
                        },
                        v44::ServerConfigurationValue {
                            value: "bar".to_string(),
                        },
                    )],
                )
                .await
                .expect("insert success");
            CONFIG_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    [(
                        v44::ConfigKey {
                            key: "joe".to_string(),
                        },
                        v44::ConfigValue { value: 42 },
                    )],
                )
                .await
                .expect("insert success");

            // Run the migration.
            stash
                .with_transaction(|tx| {
                    Box::pin(async move {
                        upgrade(&tx).await?;
                        Ok(())
                    })
                })
                .await
                .expect("transaction failed");

            let system_configs: Vec<_> = SYSTEM_CONFIGURATION_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.name, value.value))
                .collect();
            assert_eq!(
                system_configs.as_slice(),
                [("foo".to_string(), "bar".to_string())]
            );

            let configs: Vec<_> = CONFIG_COLLECTION_V45
                .peek_one(&mut stash)
                .await
                .expect("read v45")
                .into_iter()
                .map(|(key, value)| (key.key, value.value))
                .collect();
            assert_eq!(
                configs.as_slice(),
                [
                    ("joe".to_string(), 42),
                    ("system_config_synced".to_string(), 0)
                ]
            )
        })
        .await
        .expect("stash failed");
    }
}
