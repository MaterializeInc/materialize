// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::id_gen::AtomicIdGen;

// Re-export GlobalId and CatalogItemId from mz-catalog-types
pub use mz_catalog_types::{CatalogItemId, GlobalId};

// `GlobalId`s are serialized often, so it would be nice to try and keep them small. If this assert
// fails, then there isn't any correctness issues just potential performance issues.
static_assertions::assert_eq_size!(GlobalId, [u8; 16]);

#[derive(Debug)]
pub struct TransientIdGen(AtomicIdGen);

impl TransientIdGen {
    pub fn new() -> Self {
        let inner = AtomicIdGen::default();
        // Transient IDs start at 1, so throw away the 0 value.
        let _ = inner.allocate_id();
        Self(inner)
    }

    pub fn allocate_id(&self) -> (CatalogItemId, GlobalId) {
        let inner = self.0.allocate_id();
        (CatalogItemId::Transient(inner), GlobalId::Transient(inner))
    }
}
