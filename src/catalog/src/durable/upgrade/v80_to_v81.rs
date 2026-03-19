// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::{JsonCompatible, json_compatible};
use crate::durable::upgrade::objects_v80 as v80;
use crate::durable::upgrade::objects_v81 as v81;
use mz_catalog_protos::objects_v80::{ClusterVariant, ManagedCluster};
use mz_repr::adt::regex::Regex;

json_compatible!(v80::RoleKey with v81::RoleKey);
json_compatible!(v80::RoleMembership with v81::RoleMembership);
json_compatible!(v80::RoleVars with v81::RoleVars);

/// Migrates the catalog role attribute `auto_provision_source` to the new `AutoProvisionSource` enum.
///
/// For cloud environments (heuristic: mz_system cluster replication factor > 0), all existing
/// roles are assumed to have been provisioned via Frontegg. For self-managed environments, the
/// field is left as `None` (the JSON default handles this without explicit update actions).
pub fn upgrade(
    snapshot: Vec<v80::StateUpdateKind>,
) -> Vec<MigrationAction<v80::StateUpdateKind, v81::StateUpdateKind>> {
    // This is a heuristic to determine if the environment is a Materialize Cloud environment
    // and not a self-managed environment. This heuristic works because by default,
    // self managed environments have an mz_system cluster with a replication factor of 0.
    // This was to reduce the hardware requirements for self managed environments. However in
    // Materialize cloud, we always set the replication factor to 1.
    let is_cloud = snapshot.iter().any(|update| match update {
        v80::StateUpdateKind::Cluster(cluster) if cluster.value.name == "mz_system" => {
            if let ClusterVariant::Managed(ManagedCluster {
                replication_factor, ..
            }) = cluster.value.config.variant
            {
                replication_factor > 0
            } else {
                false
            }
        }
        _ => false,
    });

    if !is_cloud {
        // Self-managed: auto_provision_source defaults to None
        return Vec::new();
    }

    let mut migrations = Vec::new();
    for update in snapshot {
        let v80::StateUpdateKind::Role(role) = update else {
            continue;
        };

        // This is a heuristic to determine if the role was auto-provisioned via Frontegg.
        // This works for the vast majority of cases in production. Roles that users
        // log in to come from Frontegg and therefore *must* be valid email
        // addresses, while roles that are created via `CREATE ROLE` (e.g.,
        // `admin`, `prod_app`) almost certainly are not named to look like email
        // addresses.
        let email_regex_heuristic = Regex::new(r".+@.+\..+", true).expect("valid regex");
        let (login, auto_provision_source) =
            if email_regex_heuristic.is_match(&role.value.name.clone()) {
                // Set login to true to differentiate users from other roles.
                (Some(true), Some(v81::AutoProvisionSource::Frontegg))
            } else {
                (Some(false), None)
            };

        let new_role = v81::StateUpdateKind::Role(v81::Role {
            key: JsonCompatible::convert(&role.key),
            value: v81::RoleValue {
                name: role.value.name.clone(),
                attributes: v81::RoleAttributes {
                    inherit: role.value.attributes.inherit,
                    superuser: role.value.attributes.superuser,
                    login,
                    auto_provision_source,
                },
                membership: JsonCompatible::convert(&role.value.membership),
                vars: JsonCompatible::convert(&role.value.vars),
                oid: role.value.oid,
            },
        });

        let old_role = v80::StateUpdateKind::Role(role);
        migrations.push(MigrationAction::Update(old_role, new_role));
    }
    migrations
}

#[cfg(test)]
mod tests {
    use super::upgrade;
    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::objects_v80 as v80;
    use crate::durable::upgrade::objects_v81 as v81;

    fn make_mz_system_cluster(replication_factor: u32) -> v80::StateUpdateKind {
        v80::StateUpdateKind::Cluster(v80::Cluster {
            key: v80::ClusterKey {
                id: v80::ClusterId::System(1),
            },
            value: v80::ClusterValue {
                name: "mz_system".to_string(),
                owner_id: v80::RoleId::System(1),
                privileges: vec![],
                config: v80::ClusterConfig {
                    workload_class: None,
                    variant: v80::ClusterVariant::Managed(v80::ManagedCluster {
                        size: "1".to_string(),
                        replication_factor,
                        availability_zones: vec![],
                        logging: v80::ReplicaLogging {
                            log_logging: false,
                            interval: None,
                        },
                        optimizer_feature_overrides: vec![],
                        schedule: v80::ClusterSchedule::Manual,
                    }),
                },
            },
        })
    }

    fn make_role(id: u64, name: &str) -> v80::StateUpdateKind {
        v80::StateUpdateKind::Role(v80::Role {
            key: v80::RoleKey {
                id: v80::RoleId::User(id),
            },
            value: v80::RoleValue {
                name: name.to_string(),
                attributes: v80::RoleAttributes {
                    inherit: true,
                    superuser: None,
                    login: None,
                },
                membership: v80::RoleMembership { map: vec![] },
                vars: v80::RoleVars { entries: vec![] },
                oid: id.try_into().expect("id fits into u32"),
            },
        })
    }

    #[mz_ore::test]
    fn test_self_managed_returns_no_migrations() {
        // We make mz_system cluster with replication factor 0 as heuristic to determine
        // if the environment is cloud or not.
        let snapshot = vec![make_mz_system_cluster(0), make_role(1, "user@example.com")];
        let migrations = upgrade(snapshot);
        assert!(migrations.is_empty());
    }

    #[mz_ore::test]
    fn test_cloud_mixed_roles() {
        // Roles that look like email addresses should have autoprovisionsource = 'frontegg'.
        let snapshot = vec![
            make_mz_system_cluster(1),
            make_role(1, "user@example.com"),
            make_role(2, "manually_created_role"),
        ];
        let migrations = upgrade(snapshot);
        assert_eq!(migrations.len(), 2);

        let MigrationAction::Update(_, user_role_action) = &migrations[0] else {
            panic!("Expected action for user role");
        };
        let v81::StateUpdateKind::Role(user_role) = user_role_action else {
            panic!();
        };
        assert_eq!(
            user_role.value.attributes.auto_provision_source,
            Some(v81::AutoProvisionSource::Frontegg)
        );
        assert_eq!(user_role.value.attributes.login, Some(true));

        let MigrationAction::Update(_, manually_created_role_action) = &migrations[1] else {
            panic!();
        };
        let v81::StateUpdateKind::Role(manually_created_role) = manually_created_role_action else {
            panic!();
        };
        assert_eq!(
            manually_created_role.value.attributes.auto_provision_source,
            None
        );

        assert_eq!(manually_created_role.value.attributes.login, Some(false));
    }

    #[mz_ore::test]
    fn test_non_role_updates_ignored() {
        // Should ignore Cloud environments with no roles
        let snapshot = vec![make_mz_system_cluster(1)];
        let migrations = upgrade(snapshot);
        assert!(migrations.is_empty());
    }
}
