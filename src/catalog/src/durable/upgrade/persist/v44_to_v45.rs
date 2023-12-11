// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::string::ToString;

use crate::durable::upgrade::persist::MigrationAction;
use crate::durable::upgrade::{objects_v44 as v44, objects_v45 as v45};

const SYSTEM_CONFIG_KEY: &str = "config_has_synced_once";
const CONFIG_KEY: &str = "system_config_synced";

/// Migrate LD sync flag from the System Configuration Collection to the Config Collection.
pub fn upgrade(
    snapshot: Vec<v44::StateUpdateKind>,
) -> Vec<MigrationAction<v44::StateUpdateKind, v45::StateUpdateKind>> {
    let mut migrations = Vec::new();

    // Get current value of system variable, if one exists.
    let system_config = snapshot.iter().find_map(|update| {
        let v44::state_update_kind::Kind::ServerConfiguration(server_conf) =
            update.kind.as_ref().expect("missing field")
        else {
            return None;
        };

        let key = server_conf.key.as_ref().expect("missing field");
        if key.name == SYSTEM_CONFIG_KEY {
            Some((
                key.clone(),
                server_conf.value.clone().expect("missing field"),
            ))
        } else {
            None
        }
    });
    let system_config_value = system_config
        .as_ref()
        .map(|(_key, value)| value.value == "on")
        .unwrap_or(false);

    // Delete system variable, if it exists.
    if let Some((key, value)) = system_config {
        let deletion = v44::StateUpdateKind {
            kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                v44::state_update_kind::ServerConfiguration {
                    key: Some(key),
                    value: Some(value),
                },
            )),
        };
        migrations.push(MigrationAction::Delete(deletion));
    }

    // Insert config.
    let config_key = v45::ConfigKey {
        key: CONFIG_KEY.to_string(),
    };
    let config_value = v45::ConfigValue {
        value: if system_config_value { 1 } else { 0 },
    };
    let insertion = v45::StateUpdateKind {
        kind: Some(v45::state_update_kind::Kind::Config(
            v45::state_update_kind::Config {
                key: Some(config_key),
                value: Some(config_value),
            },
        )),
    };
    migrations.push(MigrationAction::Insert(insertion));

    migrations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_on_migration() {
        let snapshot = vec![
            v44::StateUpdateKind {
                kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                    v44::state_update_kind::ServerConfiguration {
                        key: Some(v44::ServerConfigurationKey {
                            name: SYSTEM_CONFIG_KEY.to_string(),
                        }),
                        value: Some(v44::ServerConfigurationValue {
                            value: "on".to_string(),
                        }),
                    },
                )),
            },
            v44::StateUpdateKind {
                kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                    v44::state_update_kind::ServerConfiguration {
                        key: Some(v44::ServerConfigurationKey {
                            name: "foo".to_string(),
                        }),
                        value: Some(v44::ServerConfigurationValue {
                            value: "bar".to_string(),
                        }),
                    },
                )),
            },
        ];

        let actions = upgrade(snapshot);

        insta::assert_debug_snapshot!("sync_on", actions);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_off_migration() {
        let snapshot = vec![
            v44::StateUpdateKind {
                kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                    v44::state_update_kind::ServerConfiguration {
                        key: Some(v44::ServerConfigurationKey {
                            name: SYSTEM_CONFIG_KEY.to_string(),
                        }),
                        value: Some(v44::ServerConfigurationValue {
                            value: "off".to_string(),
                        }),
                    },
                )),
            },
            v44::StateUpdateKind {
                kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                    v44::state_update_kind::ServerConfiguration {
                        key: Some(v44::ServerConfigurationKey {
                            name: "foo".to_string(),
                        }),
                        value: Some(v44::ServerConfigurationValue {
                            value: "bar".to_string(),
                        }),
                    },
                )),
            },
        ];

        let actions = upgrade(snapshot);

        insta::assert_debug_snapshot!("sync_off", actions);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_ld_sync_missing_migration() {
        let snapshot = vec![v44::StateUpdateKind {
            kind: Some(v44::state_update_kind::Kind::ServerConfiguration(
                v44::state_update_kind::ServerConfiguration {
                    key: Some(v44::ServerConfigurationKey {
                        name: "foo".to_string(),
                    }),
                    value: Some(v44::ServerConfigurationValue {
                        value: "bar".to_string(),
                    }),
                },
            )),
        }];

        let actions = upgrade(snapshot);

        insta::assert_debug_snapshot!("sync_missing", actions);
    }
}
