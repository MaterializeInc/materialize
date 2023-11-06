// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::memory::objects::CollectionIdBundle;

use crate::coord::Coordinator;
use crate::session::Session;

impl Coordinator {
    /// Resolves the full name from the corresponding catalog entry for each item in `id_bundle`.
    /// If an item in the bundle does not exist in the catalog, it's not included in the result.
    pub fn resolve_collection_id_bundle_names(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
    ) -> Vec<String> {
        let mut names: Vec<_> = id_bundle
            .iter()
            // This could filter out an entry that has been replaced in another transaction.
            .filter_map(|id| self.catalog().try_get_entry(&id))
            .map(|item| {
                self.catalog()
                    .resolve_full_name(item.name(), Some(session.conn_id()))
                    .to_string()
            })
            .collect();
        names.sort();
        names
    }
}
