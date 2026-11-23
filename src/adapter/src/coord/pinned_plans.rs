// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test-only storage for pinned plans, set by `--unsafe-pinned-plans-dir`.
//!
//! Each index and materialized view dataflow is pinned in a JSON file named
//! after its export's `GlobalId`. When the coordinator plans one of these
//! items, at creation or at boot, it runs the stored plan in place of the
//! freshly optimized one, and pins the fresh one when nothing is stored. A
//! test can thereby create items on one release and run their plans on the
//! next.
//!
//! The directory belongs to one environment, like its persist state. A file
//! left from another environment is instantiated against whatever item now
//! has its id.
// TODO(SQL-720): pins will be stored durably in the catalog. Then this module
// goes away and `Coordinator::pinned_or_fresh_plan` loads from the catalog.

use std::path::PathBuf;
use std::{fs, io};

use mz_compute_types::dataflows::LirDataflowDescription;
use mz_compute_types::plan::pinned::PinnedDataflow;
use mz_repr::GlobalId;
use tracing::debug;

use crate::coord::Coordinator;
use crate::optimize::pinned::CatalogInstantiationContext;

/// A directory of pinned plans keyed by `GlobalId`.
#[derive(Debug)]
pub(crate) struct PinnedPlanFiles {
    dir: PathBuf,
}

impl PinnedPlanFiles {
    pub(crate) fn new(dir: PathBuf) -> Self {
        PinnedPlanFiles { dir }
    }

    fn path(&self, id: GlobalId) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn load(&self, id: GlobalId) -> Option<PinnedDataflow> {
        let path = self.path(id);
        match fs::read(&path) {
            Ok(bytes) => Some(
                serde_json::from_slice(&bytes)
                    .unwrap_or_else(|e| panic!("parsing pinned plan {}: {e}", path.display())),
            ),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => panic!("reading pinned plan {}: {e}", path.display()),
        }
    }

    fn store(&self, id: GlobalId, pinned: &PinnedDataflow) {
        let path = self.path(id);
        let json = serde_json::to_vec_pretty(pinned).expect("pinned plans serialize");
        fs::create_dir_all(&self.dir)
            .and_then(|()| fs::write(&path, json))
            .unwrap_or_else(|e| panic!("writing pinned plan {}: {e}", path.display()));
    }
}

impl Coordinator {
    /// Returns the plan stored for `id` in place of `plan`, or pins `plan`
    /// and stores it when nothing is stored. Returns `plan` as it is when
    /// `--unsafe-pinned-plans-dir` is unset or `plan` cannot be pinned.
    ///
    /// `id` must name a catalog item already, since pinning and instantiation
    /// both look it up.
    ///
    /// Panics when a stored plan cannot be read or instantiated, or a pin
    /// cannot be written.
    pub(crate) fn pinned_or_fresh_plan(
        &self,
        id: GlobalId,
        plan: LirDataflowDescription,
    ) -> LirDataflowDescription {
        let Some(files) = &self.pinned_plans else {
            return plan;
        };

        // NOTE: Every failure past this point panics. Falling back to the
        // fresh plan would let a test that expects to run the stored plan
        // pass without running it.
        if let Some(pinned) = files.load(id) {
            let ctx = CatalogInstantiationContext {
                catalog: self.catalog().state(),
                transient_ids: &self.transient_id_gen,
            };
            return pinned
                .instantiate(&ctx, plan.debug_name)
                .unwrap_or_else(|e| panic!("instantiating pinned plan for {id}: {e}"));
        }

        match PinnedDataflow::pin(plan.clone(), self.catalog().state()) {
            Ok(pinned) => files.store(id, &pinned),
            // Dataflows over builtins or unstable dependencies are routine
            // and simply run unpinned.
            Err(e) => debug!("not pinning {id}: {e}"),
        }
        plan
    }
}
