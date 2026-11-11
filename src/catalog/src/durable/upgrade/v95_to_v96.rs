// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v95 as v95;
use crate::durable::upgrade::objects_v96 as v96;

crate::json_compatible!(v95::ClusterReplicaSizeKey with v96::ClusterReplicaSizeKey);

/// Adds the `allowed_workloads` field to cluster replica sizes, backfilling
/// it as `None`, which allows every workload class.
///
/// Opening the catalog then updates each builtin size whose allocation in
/// `--cluster-replica-sizes` differs, so a restricted size gets its classes on
/// the first boot after the upgrade.
///
/// NOTE: The explicit rewrite matters even though serde would default the
/// missing field to `None` on read. A later update to a size retracts the
/// record by writing its v96 encoding at diff -1, and without the backfill
/// that retraction doesn't match the stored record.
pub fn upgrade(
    snapshot: Vec<v95::StateUpdateKind>,
) -> Vec<MigrationAction<v95::StateUpdateKind, v96::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update {
            v95::StateUpdateKind::ClusterReplicaSize(old) => {
                let new = migrate_size(old.clone());
                Some(MigrationAction::Update(
                    v95::StateUpdateKind::ClusterReplicaSize(old),
                    v96::StateUpdateKind::ClusterReplicaSize(new),
                ))
            }
            _ => None,
        })
        .collect()
}

fn migrate_size(old: v95::ClusterReplicaSize) -> v96::ClusterReplicaSize {
    let v95::ClusterReplicaSize { key, value } = old;
    let v95::ClusterReplicaSizeValue {
        name,
        memory_limit,
        cpu_limit,
        cpu_request,
        disk_limit,
        scale,
        workers,
        credits_per_hour,
        cpu_exclusive,
        is_cc,
        family,
        swap_enabled,
        disabled,
        selectors,
    } = value;
    v96::ClusterReplicaSize {
        key: JsonCompatible::convert(&key),
        value: v96::ClusterReplicaSizeValue {
            name,
            memory_limit,
            cpu_limit,
            cpu_request,
            disk_limit,
            scale,
            workers,
            credits_per_hour,
            cpu_exclusive,
            is_cc,
            family,
            swap_enabled,
            disabled,
            selectors,
            allowed_workloads: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::durable::upgrade::MigrationAction;
    use crate::durable::upgrade::v95_to_v96::upgrade;
    use crate::durable::upgrade::{objects_v95 as v95, objects_v96 as v96};

    #[mz_ore::test]
    fn backfills_allowed_workloads() {
        let size = v95::ClusterReplicaSize {
            key: v95::ClusterReplicaSizeKey {
                id: v95::ClusterReplicaSizeId::System(1),
            },
            value: v95::ClusterReplicaSizeValue {
                name: "scale=1,workers=1".into(),
                memory_limit: Some(1 << 30),
                cpu_limit: None,
                cpu_request: None,
                disk_limit: None,
                scale: 1,
                workers: 1,
                credits_per_hour: "1".into(),
                cpu_exclusive: false,
                is_cc: true,
                family: Some("legacy".into()),
                swap_enabled: false,
                disabled: false,
                selectors: BTreeMap::new(),
            },
        };
        let id_alloc = v95::StateUpdateKind::IdAlloc(v95::IdAlloc {
            key: v95::IdAllocKey {
                name: "user_role".into(),
            },
            value: v95::IdAllocValue { next_id: 1 },
        });

        let actions = upgrade(vec![
            v95::StateUpdateKind::ClusterReplicaSize(size.clone()),
            id_alloc,
        ]);

        let [MigrationAction::Update(old, new)] = actions.as_slice() else {
            panic!("expected only the size to be rewritten, got {actions:?}");
        };
        assert_eq!(old, &v95::StateUpdateKind::ClusterReplicaSize(size));
        let v96::StateUpdateKind::ClusterReplicaSize(new) = new else {
            panic!("expected a size, got {new:?}");
        };
        assert_eq!(new.key.id, v96::ClusterReplicaSizeId::System(1));
        assert_eq!(new.value.name, "scale=1,workers=1");
        assert_eq!(new.value.family.as_deref(), Some("legacy"));
        assert_eq!(new.value.allowed_workloads, None);
    }
}
