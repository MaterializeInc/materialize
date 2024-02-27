// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal consistency checks that validate invariants of [`Coordinator`].

use super::Coordinator;
use crate::catalog::consistency::CatalogInconsistencies;
use mz_catalog::memory::objects::{CatalogItem, DataSourceDesc, Source};
use mz_ore::instrument;
use mz_repr::GlobalId;
use serde::Serialize;

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct CoordinatorInconsistencies {
    /// Inconsistencies found in the catalog.
    catalog_inconsistencies: Box<CatalogInconsistencies>,
    /// Inconsistencies found in read capabilities.
    read_capabilities: Vec<ReadCapabilitiesInconsistency>,
    /// Inconsistencies found with our map of active webhooks.
    active_webhooks: Vec<ActiveWebhookInconsistency>,
}

impl CoordinatorInconsistencies {
    pub fn is_empty(&self) -> bool {
        self.catalog_inconsistencies.is_empty()
            && self.read_capabilities.is_empty()
            && self.active_webhooks.is_empty()
    }
}

impl Coordinator {
    /// Checks the [`Coordinator`] to make sure we're internally consistent.
    #[instrument(name = "coord::check_consistency")]
    pub fn check_consistency(&self) -> Result<(), CoordinatorInconsistencies> {
        let mut inconsistencies = CoordinatorInconsistencies::default();

        if let Err(catalog_inconsistencies) = self.catalog().state().check_consistency() {
            inconsistencies.catalog_inconsistencies = catalog_inconsistencies;
        }

        if let Err(read_capabilities) = self.check_read_capabilities() {
            inconsistencies.read_capabilities = read_capabilities;
        }

        if let Err(active_webhooks) = self.check_active_webhooks() {
            inconsistencies.active_webhooks = active_webhooks;
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }

    /// # Invariants:
    ///
    /// * Read capabilities should reference known objects.
    ///
    fn check_read_capabilities(&self) -> Result<(), Vec<ReadCapabilitiesInconsistency>> {
        let mut read_capabilities_inconsistencies = Vec::new();
        for (gid, _) in &self.storage_read_capabilities {
            if self.catalog().try_get_entry(gid).is_none() {
                read_capabilities_inconsistencies
                    .push(ReadCapabilitiesInconsistency::Storage(gid.clone()));
            }
        }
        for (gid, _) in &self.compute_read_capabilities {
            if !gid.is_transient() && self.catalog().try_get_entry(gid).is_none() {
                read_capabilities_inconsistencies
                    .push(ReadCapabilitiesInconsistency::Compute(gid.clone()));
            }
        }

        if read_capabilities_inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(read_capabilities_inconsistencies)
        }
    }

    /// # Invariants
    ///
    /// * All [`GlobalId`]s in the `active_webhooks` map should reference known webhook sources.
    ///
    fn check_active_webhooks(&self) -> Result<(), Vec<ActiveWebhookInconsistency>> {
        let mut inconsistencies = vec![];
        for (id, _) in &self.active_webhooks {
            let is_webhook = self
                .catalog()
                .try_get_entry(id)
                .map(|entry| entry.item())
                .and_then(|item| {
                    let CatalogItem::Source(Source { data_source, .. }) = &item else {
                        return None;
                    };
                    Some(matches!(data_source, DataSourceDesc::Webhook { .. }))
                })
                .unwrap_or(false);
            if !is_webhook {
                inconsistencies.push(ActiveWebhookInconsistency::NonExistentWebhook(*id));
            }
        }

        if inconsistencies.is_empty() {
            Ok(())
        } else {
            Err(inconsistencies)
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
enum ReadCapabilitiesInconsistency {
    Storage(GlobalId),
    Compute(GlobalId),
}

#[derive(Debug, Serialize, PartialEq, Eq)]
enum ActiveWebhookInconsistency {
    NonExistentWebhook(GlobalId),
}
