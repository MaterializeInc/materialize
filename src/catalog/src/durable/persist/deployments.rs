// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Version requirements enrolled by catalog admission, not process liveness.

use std::collections::{BTreeMap, BTreeSet};

use mz_persist_client::cfg::code_can_write_data;
use semver::Version;

use crate::durable::DurableCatalogError;
use crate::durable::objects::DeploymentAdmission;

impl DeploymentAdmission {
    pub(super) fn initial(version: Version) -> Self {
        Self {
            members: BTreeMap::new(),
            persist_target: version,
            retired: BTreeSet::new(),
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
        if self.retired.contains(&generation) {
            return Err(reject(format!("deployment {generation} has been retired")));
        }
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
        self.update_target()
    }

    /// Retires only `generation`, including an attempt that has not joined yet.
    /// The caller must commit against the same snapshot that supplied `active`.
    pub(super) fn retire(
        &mut self,
        generation: u64,
        active: u64,
    ) -> Result<(), DurableCatalogError> {
        let reject = |reason: String| DurableCatalogError::NotWritable(reason);
        if generation == active {
            return Err(reject("cannot retire the active deployment".into()));
        }
        if self.retired.contains(&generation) {
            return Ok(());
        }
        if generation < active {
            return Err(reject("only a prewarming deployment can be retired".into()));
        }
        self.members.remove(&generation);
        self.retired.insert(generation);
        self.update_target()
    }

    fn update_target(&mut self) -> Result<(), DurableCatalogError> {
        let target = self
            .members
            .values()
            .min_by(|a, b| a.cmp_precedence(b))
            .ok_or_else(|| {
                DurableCatalogError::NotWritable("deployment membership is empty".into())
            })?;
        if target.cmp_precedence(&self.persist_target).is_lt()
            || self
                .members
                .values()
                .any(|version| !code_can_write_data(version, target))
        {
            return Err(DurableCatalogError::NotWritable(
                "deployment membership has no supported monotone Persist target".into(),
            ));
        }
        self.persist_target = target.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn retirement_excludes_only_the_target_identity() {
        let version = Version::new(26, 47, 0);
        let mut admission = DeploymentAdmission::initial(version.clone());
        for generation in [1, 2, 3] {
            admission
                .admit(generation, &version, false, None)
                .expect("admit compatible participant");
        }
        let before = admission.clone();
        assert!(admission.retire(1, 1).is_err());
        assert_eq!(admission, before);

        admission.retire(2, 1).expect("retire prewarmer");
        assert_eq!(
            admission.members.keys().copied().collect::<Vec<_>>(),
            vec![1, 3]
        );
        let retired = admission.clone();
        admission.retire(2, 1).expect("retry retirement");
        assert_eq!(admission, retired);
        assert!(admission.admit(2, &version, false, None).is_err());
        assert!(admission.admit(2, &version, true, Some(2)).is_err());

        admission
            .retire(4, 1)
            .expect("cancel before first admission");
        assert!(admission.admit(4, &version, false, None).is_err());
        admission
            .admit(5, &version, false, None)
            .expect("fresh attempt may join");
    }
}
