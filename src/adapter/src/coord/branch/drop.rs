// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `DROP BRANCH` orchestration.
//!
//! Tearing down a branch is a fixed sequence:
//!
//! 1. Delete every `fork_blob_refs` row tagged with the branch's id (one
//!    statement).
//! 2. Remove every `BranchDescriptor` belonging to the branch from the
//!    catalog.
//! 3. Drop the branch's catalog items from the in-memory catalog.
//! 4. Mark the fork shards for deletion so persist's GC reclaims them on
//!    the next cycle.
//!
//! Step 1 is the load-bearing one for blob lifecycle: it releases the only
//! references keeping the source's old blobs alive, so the source's next GC
//! pass can reclaim them.

use uuid::Uuid;

use mz_persist::fork_blob_refs::ForkBlobRefs;
use mz_repr::CatalogItemId;

/// What the caller asks `drop_branch` to do.
#[derive(Debug, Clone)]
pub struct DropBranchRequest {
    pub branch_id: Uuid,
    pub branch_name: String,
    pub if_exists: bool,
    pub branched_catalog_ids: Vec<CatalogItemId>,
    pub fork_shard_ids: Vec<mz_persist_client::ShardId>,
}

#[derive(Debug)]
pub enum DropBranchError {
    /// The branch name does not exist and the request was not `IF EXISTS`.
    NotFound { branch_name: String },
    /// `fork_blob_refs` delete failed.
    RefDeleteFailed(String),
    /// Catalog descriptor removal failed.
    CatalogRemoveFailed(String),
    /// Same plumbing gap as the create path: the actual catalog transaction
    /// + ForkBlobRefs handle aren't wired into this module yet.
    NotYetWired,
}

impl std::fmt::Display for DropBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DropBranchError::NotFound { branch_name } => {
                write!(f, "branch \"{branch_name}\" does not exist")
            }
            DropBranchError::RefDeleteFailed(msg) => {
                write!(f, "fork_blob_refs delete failed: {msg}")
            }
            DropBranchError::CatalogRemoveFailed(msg) => {
                write!(f, "BranchDescriptor removal failed: {msg}")
            }
            DropBranchError::NotYetWired => f.write_str(
                "drop_branch requires catalog Transaction + ForkBlobRefs plumbing",
            ),
        }
    }
}

impl std::error::Error for DropBranchError {}

/// `DROP BRANCH` orchestration. Currently a sketch: the function signature
/// is final, but the body is `NotYetWired` until the catalog `Transaction`
/// and `ForkBlobRefs` handles are plumbed onto the coordinator. The intent
/// is documented in the module-level doc.
#[allow(dead_code)]
pub async fn drop_branch(
    _refs: &ForkBlobRefs,
    _request: DropBranchRequest,
) -> Result<(), DropBranchError> {
    Err(DropBranchError::NotYetWired)
}

/// One row in the `SHOW BRANCHES` result. The exact rendering (formatter,
/// column order) lives in the `show` path; this struct is the data layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowBranchesRow {
    pub branch_name: String,
    pub owner: String,
    pub created_at_ms: u64,
}

/// One row in `SHOW BRANCH STATUS <name>`. Combines the descriptor with
/// runtime aggregates pulled from `fork_blob_refs` and compute status
/// views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowBranchStatusRow {
    pub branch_name: String,
    pub owner: String,
    pub created_at_ms: u64,
    /// Number of `fork_blob_refs` rows tagged with this branch. Lets the
    /// user gauge how many source blobs are being pinned.
    pub blob_ref_count: u64,
    /// Session cluster on which branched dataflows render. None until any
    /// branched dataflow has been installed.
    pub cluster: Option<String>,
}

/// Pure helper: given a slice of descriptors, project the columns shown by
/// `SHOW BRANCHES`. Used by the planner's row builder.
pub fn project_show_branches<I>(descriptors: I) -> Vec<ShowBranchesRow>
where
    I: IntoIterator<Item = (String, String, u64)>,
{
    let mut rows: Vec<_> = descriptors
        .into_iter()
        .map(|(branch_name, owner, created_at_ms)| ShowBranchesRow {
            branch_name,
            owner,
            created_at_ms,
        })
        .collect();
    rows.sort_by(|a, b| a.branch_name.cmp(&b.branch_name));
    rows.dedup();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn show_branches_dedupes_per_branch_name() {
        // The same branch shows up once per branched object in the
        // descriptor collection; `SHOW BRANCHES` should collapse to one row
        // per branch name.
        let descriptors = vec![
            ("nightly".to_owned(), "alice".to_owned(), 100),
            ("nightly".to_owned(), "alice".to_owned(), 100),
            ("experiment".to_owned(), "bob".to_owned(), 200),
        ];
        let rows = project_show_branches(descriptors);
        assert_eq!(rows.len(), 2);
        // Result is sorted by branch_name.
        assert_eq!(rows[0].branch_name, "experiment");
        assert_eq!(rows[1].branch_name, "nightly");
    }

    #[mz_ore::test]
    fn show_branches_empty_input_yields_empty() {
        assert!(project_show_branches(std::iter::empty()).is_empty());
    }

    #[mz_ore::test]
    fn drop_branch_returns_not_yet_wired_in_stub_mode() {
        let err = DropBranchError::NotYetWired;
        let rendered = format!("{err}");
        assert!(
            rendered.contains("plumbing"),
            "rendered message should mention plumbing: {rendered}",
        );
    }

    #[mz_ore::test]
    fn drop_branch_not_found_renders_branch_name() {
        let err = DropBranchError::NotFound {
            branch_name: "missing".to_owned(),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("missing"));
    }
}
