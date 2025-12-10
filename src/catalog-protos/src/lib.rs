// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! All types we durably persist in the Catalog.

pub mod audit_log;
pub mod objects;
pub mod objects_v67;
pub mod objects_v68;
pub mod objects_v69;
pub mod objects_v70;
pub mod objects_v71;
pub mod objects_v72;
pub mod objects_v73;
pub mod objects_v74;
pub mod objects_v75;
pub mod objects_v76;
pub mod objects_v77;
pub mod objects_v78;
pub mod serialization;

/// The current version of the `Catalog`.
///
/// We will initialize new `Catalog`s with this version, and migrate existing `Catalog`s to this
/// version. Whenever the `Catalog` changes, e.g. the types we serialize in the `Catalog`
/// change, we need to bump this version.
pub const CATALOG_VERSION: u64 = 78;

/// The minimum `Catalog` version number that we support migrating from.
///
/// After bumping this we can delete the old migrations.
pub const MIN_CATALOG_VERSION: u64 = 67;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use crate::{CATALOG_VERSION, MIN_CATALOG_VERSION};

    #[mz_ore::test]
    fn test_assert_snapshots_exist() {
        let src_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src"].iter().collect();

        // Get all the versioned object definition files.
        let mut filenames: BTreeSet<_> = fs::read_dir(src_dir)
            .expect("failed to read src dir")
            .map(|entry| entry.expect("failed to read dir entry").file_name())
            .map(|filename| filename.to_str().expect("utf8").to_string())
            .filter(|filename| filename.starts_with("objects_v"))
            .collect();

        // Assert snapshots exist for all of the versions we support.
        for version in MIN_CATALOG_VERSION..=CATALOG_VERSION {
            let filename = format!("objects_v{version}.rs");
            assert!(
                filenames.remove(&filename),
                "Missing snapshot for v{version}."
            );
        }

        // Common case. Check to make sure the user bumped the CATALOG_VERSION.
        if !filenames.is_empty()
            && filenames.remove(&format!("objects_v{}.proto", CATALOG_VERSION + 1))
        {
            panic!(
                "Found snapshot for v{}, please also bump `CATALOG_VERSION`.",
                CATALOG_VERSION + 1
            )
        }

        // Assert there aren't any extra snapshots.
        assert!(
            filenames.is_empty(),
            "Found snapshots for unsupported catalog versions {filenames:?}.\n\
             If you just increased `MIN_CATALOG_VERSION`, then please delete the old snapshots. \
             If you created a new snapshot, please bump `CATALOG_VERSION`."
        );
    }

    #[mz_ore::test]
    fn test_assert_current_snapshot() {
        let src_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src"].iter().collect();
        let current_rs = src_dir.join("objects.rs");
        let snapshot_rs = src_dir.join(format!("objects_v{CATALOG_VERSION}.rs"));

        let current = fs::read_to_string(current_rs).expect("read current");
        let snapshot = fs::read_to_string(snapshot_rs).expect("read snapshot");

        // Note: objects.rs and objects_v<CATALOG_VERSION>.rs should be exactly the same. The
        // reason being, when bumping the catalog to the next version, CATALOG_VERSION + 1, we need a
        // snapshot to migrate _from_, which should be a snapshot of how the types are today.
        // Hence why the two files should be exactly the same.
        similar_asserts::assert_eq!(current, snapshot);
    }
}
