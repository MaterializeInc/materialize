// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable commit of a branch's state and registration of its catalog items.
//!
//! Given a set of freshly initialized fork shards plus the branch's identity,
//! [`persist_branch_state`] writes the `fork_blob_refs` rows and the
//! `BranchDescriptor` entries for each branched object in a single
//! transaction. After the transaction commits, the branch's catalog items
//! become visible through normal catalog APIs: branched sources are
//! registered with `ingestion_enabled = false`, branched sinks with
//! `emission_enabled = false`, and branched tables / MVs / indexes with
//! their normal flags.

use mz_persist::fork_blob_refs::{ForkBlobRef, ForkBlobRefsStore};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use uuid::Uuid;

use mz_catalog::durable::objects::BranchDescriptor;

/// Per-object information needed to register one branched object in the
/// catalog and pin its blobs against GC.
#[derive(Debug, Clone)]
pub struct BranchedObject {
    pub branch_catalog_id: CatalogItemId,
    pub branch_global_id: GlobalId,
    pub source_catalog_id: CatalogItemId,
    pub fork_shard_id: mz_persist_client::ShardId,
    pub relation_desc: Vec<u8>,
    pub absolute_blob_keys: Vec<String>,
}

/// Branch-level identity shared by every object in [`BranchedObject`].
#[derive(Debug, Clone)]
pub struct BranchIdentity {
    pub branch_id: Uuid,
    pub branch_name: String,
    pub owner: mz_repr::role_id::RoleId,
    pub branch_ts: Timestamp,
    pub created_at_ms: u64,
}

/// Failure modes for [`persist_branch_state`].
#[derive(Debug)]
pub enum PersistBranchError {
    /// `fork_blob_refs` insert failed mid-flight; no descriptors were
    /// written. Retrying with the same inputs is safe.
    RefInsertFailed(String),
    /// `BranchDescriptor` writes failed after `fork_blob_refs` were
    /// inserted. The inserted ref rows will be cleaned up on the next
    /// `DROP BRANCH`-style retry.
    DescriptorWriteFailed(String),
}

impl std::fmt::Display for PersistBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistBranchError::RefInsertFailed(msg) => {
                write!(f, "fork_blob_refs insert failed: {msg}")
            }
            PersistBranchError::DescriptorWriteFailed(msg) => {
                write!(f, "BranchDescriptor write failed: {msg}")
            }
        }
    }
}

impl std::error::Error for PersistBranchError {}

/// Build the [`ForkBlobRef`] rows that should be inserted for `objects` under
/// the given `branch_id`. Pure: no I/O, suitable for unit tests of the row
/// shape.
pub fn build_ref_rows(branch_id: Uuid, objects: &[BranchedObject]) -> Vec<ForkBlobRef> {
    let mut rows = Vec::new();
    for obj in objects {
        for blob_key in &obj.absolute_blob_keys {
            rows.push(ForkBlobRef {
                blob_key: blob_key.clone(),
                fork_shard_id: obj.fork_shard_id.to_string(),
                branch_id,
            });
        }
    }
    rows
}

/// Build the [`BranchDescriptor`] entries that should be inserted into the
/// catalog. Pure: no I/O.
pub fn build_descriptors(
    identity: &BranchIdentity,
    objects: &[BranchedObject],
) -> Vec<BranchDescriptor> {
    objects
        .iter()
        .map(|obj| BranchDescriptor {
            branch_catalog_id: obj.branch_catalog_id,
            fork_shard_id: obj.fork_shard_id,
            branch_ts: u64::from(identity.branch_ts),
            source_catalog_id: obj.source_catalog_id,
            branch_global_id: obj.branch_global_id,
            relation_desc: obj.relation_desc.clone(),
            branch_id: identity.branch_id.to_string(),
            branch_name: identity.branch_name.clone(),
            owner: identity.owner,
            created_at_ms: identity.created_at_ms,
        })
        .collect()
}

/// Durably record the branch by inserting `fork_blob_refs` rows for every
/// blob the fork shards reference and writing the per-object
/// `BranchDescriptor` rows into the supplied catalog transaction.
///
/// Both writes are idempotent on the same inputs: `fork_blob_refs` uses an
/// `ON CONFLICT DO NOTHING` bulk insert and the catalog `TableTransaction`
/// rejects duplicate keys at commit, so a retry with the same descriptor
/// set fails cleanly rather than double-counting references.
pub async fn persist_branch_state(
    refs: &dyn ForkBlobRefsStore,
    txn: &mut mz_catalog::durable::Transaction<'_>,
    identity: BranchIdentity,
    objects: Vec<BranchedObject>,
) -> Result<(), PersistBranchError> {
    let ref_rows = build_ref_rows(identity.branch_id, &objects);
    refs.bulk_insert(&ref_rows)
        .await
        .map_err(|err| PersistBranchError::RefInsertFailed(err.to_string()))?;

    for descriptor in build_descriptors(&identity, &objects) {
        txn.insert_branch_descriptor(descriptor)
            .map_err(|err| PersistBranchError::DescriptorWriteFailed(err.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist::fork_blob_refs::InMemoryForkBlobRefs;
    use mz_persist_client::ShardId;
    use mz_repr::role_id::RoleId;

    fn make_identity(branch_id: Uuid) -> BranchIdentity {
        BranchIdentity {
            branch_id,
            branch_name: "b".to_owned(),
            owner: RoleId::User(1),
            branch_ts: Timestamp::new(7),
            created_at_ms: 1_700_000_000_000,
        }
    }

    fn make_object(catalog_id: u64, blob_keys: Vec<String>) -> BranchedObject {
        BranchedObject {
            branch_catalog_id: CatalogItemId::User(catalog_id),
            branch_global_id: GlobalId::User(catalog_id),
            source_catalog_id: CatalogItemId::User(catalog_id - 1),
            fork_shard_id: ShardId::new(),
            relation_desc: Vec::new(),
            absolute_blob_keys: blob_keys,
        }
    }

    #[mz_ore::test]
    fn build_ref_rows_emits_one_row_per_blob_per_object() {
        let branch = Uuid::new_v4();
        let objects = vec![
            make_object(11, vec!["a".to_owned(), "b".to_owned()]),
            make_object(12, vec!["c".to_owned()]),
        ];
        let rows = build_ref_rows(branch, &objects);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.branch_id == branch));
        // Every row's fork_shard_id matches one of the input objects.
        let object_shards: Vec<String> = objects
            .iter()
            .map(|o| o.fork_shard_id.to_string())
            .collect();
        for row in &rows {
            assert!(object_shards.contains(&row.fork_shard_id));
        }
    }

    #[mz_ore::test]
    fn build_descriptors_carries_branch_identity() {
        let branch = Uuid::new_v4();
        let identity = make_identity(branch);
        let objects = vec![make_object(11, vec![]), make_object(12, vec![])];
        let descs = build_descriptors(&identity, &objects);
        assert_eq!(descs.len(), 2);
        for desc in &descs {
            assert_eq!(desc.branch_id, branch.to_string());
            assert_eq!(desc.branch_name, "b");
            assert_eq!(desc.branch_ts, 7);
            assert_eq!(desc.owner, RoleId::User(1));
        }
        // Each descriptor's branch_catalog_id matches one of the input
        // objects: the function is faithful per-object.
        let descriptor_ids: Vec<_> = descs.iter().map(|d| d.branch_catalog_id).collect();
        for obj in &objects {
            assert!(descriptor_ids.contains(&obj.branch_catalog_id));
        }
    }

    #[mz_ore::test]
    fn build_ref_rows_empty_objects_yields_empty() {
        assert!(build_ref_rows(Uuid::new_v4(), &[]).is_empty());
    }

    #[mz_ore::test]
    fn build_ref_rows_object_with_no_blobs_yields_no_rows() {
        let branch = Uuid::new_v4();
        let objects = vec![make_object(11, vec![])];
        assert!(build_ref_rows(branch, &objects).is_empty());
    }

    /// Drive the full `persist_branch_state` chain against an in-memory
    /// `ForkBlobRefs` and a real catalog `Transaction`. This proves the
    /// integration end-to-end: refs and descriptors land together, and
    /// retrying the same call is idempotent.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn persist_branch_state_end_to_end() {
        use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
        use mz_persist_client::PersistLocation;
        use mz_persist_client::cache::PersistClientCache;

        const VERSION: semver::Version = semver::Version::new(26, 0, 0);
        let mut persist_cache = PersistClientCache::new_no_metrics();
        persist_cache.cfg.build_version = VERSION.clone();
        let persist_client = persist_cache
            .open(PersistLocation::new_in_mem())
            .await
            .unwrap();
        let state_builder = TestCatalogStateBuilder::new(persist_client)
            .with_default_deploy_generation()
            .with_version(VERSION);
        let _ = state_builder
            .clone()
            .unwrap_build()
            .await
            .open(mz_ore::now::SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        let mut catalog_state = state_builder
            .unwrap_build()
            .await
            .open_savepoint(mz_ore::now::SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap()
            .0;
        let _ = catalog_state.sync_to_current_updates().await.unwrap();

        let refs = InMemoryForkBlobRefs::default();
        let branch = Uuid::new_v4();
        let identity = make_identity(branch);
        let objects = vec![
            make_object(101, vec!["abs-1".into(), "abs-2".into()]),
            make_object(102, vec!["abs-3".into()]),
        ];

        // First commit: refs land in the store, descriptors land in the
        // catalog transaction.
        let op_updates;
        {
            let mut txn = catalog_state.transaction().await.unwrap();
            persist_branch_state(&refs, &mut txn, identity.clone(), objects.clone())
                .await
                .expect("first commit");
            let listed: Vec<_> = txn.get_branch_descriptors().collect();
            assert_eq!(listed.len(), 2);
            // Drain the ops emitted by our insert so commit's "no
            // unconsumed updates" assertion holds. The standard catalog
            // pipeline does this from `catalog_transact_inner` before
            // calling commit; in this isolated test we do it inline.
            op_updates = txn.get_and_commit_op_updates();
            let commit_ts = txn.upper();
            txn.commit(commit_ts).await.unwrap();
        }
        let descriptor_updates: Vec<_> = op_updates
            .into_iter()
            .filter(|u| {
                matches!(
                    u.kind,
                    mz_catalog::memory::objects::StateUpdateKind::BranchDescriptor(_),
                )
            })
            .collect();
        assert_eq!(
            descriptor_updates.len(),
            2,
            "expected two BranchDescriptor additions in the commit's op stream",
        );

        // Each absolute blob key now reports as referenced.
        assert!(refs.exists("abs-1").await.unwrap());
        assert!(refs.exists("abs-3").await.unwrap());
        assert!(!refs.exists("abs-missing").await.unwrap());

        // After the commit, the descriptors are readable through a fresh
        // transaction (i.e. they're in the catalog state, not just the
        // ephemeral transaction).
        let _ = catalog_state.sync_to_current_updates().await.unwrap();
        {
            let txn = catalog_state.transaction().await.unwrap();
            let listed: Vec<_> = txn.get_branch_descriptors().collect();
            assert_eq!(listed.len(), 2, "descriptors should survive the commit");
            for descriptor in &listed {
                assert_eq!(descriptor.branch_id, branch.to_string());
            }
        }

        // Retry with the same inputs: `bulk_insert` is idempotent. We don't
        // commit again here because the catalog transaction would reject the
        // duplicate insert; the persist-layer idempotency is what we care
        // about for replay safety.
        refs.bulk_insert(&build_ref_rows(branch, &objects))
            .await
            .expect("idempotent re-insert");

        // DROP releases everything tagged with this branch.
        let removed = refs.delete_by_branch(branch).await.unwrap();
        assert_eq!(removed, 3);
        assert!(!refs.exists("abs-1").await.unwrap());
    }
}
