// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Version requirements enrolled by catalog admission, not process liveness.

use std::collections::BTreeMap;

use mz_persist_client::cfg::code_can_write_data;
use semver::Version;

use crate::durable::DurableCatalogError;
use crate::durable::objects::DeploymentAdmission;

impl DeploymentAdmission {
    pub(super) fn initial(version: Version) -> Self {
        Self {
            members: BTreeMap::new(),
            persist_target: version,
        }
    }

    /// Enrolls a deployment, retiring fenced generations before choosing a target.
    /// `exclusive` additionally retires same-generation owners fenced by an epoch.
    /// The caller must commit the result against the snapshot used to derive it
    /// before authorizing Persist to use its target.
    pub(super) fn admit(
        &mut self,
        generation: u64,
        version: &Version,
        exclusive: bool,
        retire_before: Option<u64>,
    ) -> Result<(), DurableCatalogError> {
        let reject = |reason: String| DurableCatalogError::NotWritable(reason);
        if !code_can_write_data(version, &self.persist_target) {
            return Err(reject(format!(
                "deployment version {version} cannot write authorized Persist format {}",
                self.persist_target
            )));
        }
        if exclusive {
            self.members.clear();
        } else if self
            .members
            .get(&generation)
            .is_some_and(|existing| existing.cmp_precedence(version) != std::cmp::Ordering::Equal)
        {
            return Err(reject(format!(
                "deployment {generation} is already admitted with another version"
            )));
        }
        if let Some(active_generation) = retire_before {
            self.members
                .retain(|generation, _| *generation >= active_generation);
        }
        self.members.insert(generation, version.clone());
        let target = self
            .members
            .values()
            .min_by(|a, b| a.cmp_precedence(b))
            .expect("admitting a deployment leaves a nonempty membership");
        if target.cmp_precedence(&self.persist_target).is_lt()
            || self
                .members
                .values()
                .any(|version| !code_can_write_data(version, target))
        {
            return Err(reject(
                "deployment membership has no supported monotone Persist target".into(),
            ));
        }
        self.persist_target = target.clone();
        Ok(())
    }
}
