// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog object data model.
//!
//! The data-model types and their current-version protobuf serialization live
//! in the `mz-catalog-types` crate and are re-exported here so that durable
//! code (and external consumers) keep using the `crate::durable::objects` path.

pub use mz_catalog_types::objects::*;

pub(crate) mod state_update;

/// Serialization between the durable object data model and protobuf.
pub mod serialization {
    pub use mz_catalog_types::objects::serialization::*;

    use super::state_update::StateUpdateKindJson;

    impl From<proto::StateUpdateKind> for StateUpdateKindJson {
        fn from(value: proto::StateUpdateKind) -> Self {
            StateUpdateKindJson::from_serde(value)
        }
    }

    impl TryFrom<StateUpdateKindJson> for proto::StateUpdateKind {
        type Error = String;

        fn try_from(value: StateUpdateKindJson) -> Result<Self, Self::Error> {
            value.try_to_serde::<Self>().map_err(|err| err.to_string())
        }
    }
}
