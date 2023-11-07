// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash_types::upgrade::{objects_v42 as v42, objects_v43 as v43};

use crate::upgrade::{wire_compatible, MigrationAction, WireCompatible};
use crate::{StashError, Transaction, TypedCollection};

wire_compatible!(v42::GlobalId with v43::GlobalId);
wire_compatible!(v42::TimestampAntichain with v43::TimestampAntichain);

pub static METADATA_EXPORT: TypedCollection<v42::GlobalId, v42::DurableExportMetadata> =
    TypedCollection::new("storage-export-metadata-u64");

/// Migrate sink as of.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    let convert_sink_as_of = |as_of: &v42::SinkAsOf| v43::SinkAsOf {
        frontier: Some(WireCompatible::convert(as_of.frontier.as_ref().unwrap())),
        strict: if as_of.strict { 1 } else { -1 },
    };
    let action = |(key, value): (&v42::GlobalId, &v42::DurableExportMetadata)| {
        let new_key: v42::GlobalId = WireCompatible::convert(key);
        let new_value = v43::DurableExportMetadata {
            initial_as_of: Some(convert_sink_as_of(value.initial_as_of.as_ref().unwrap())),
        };
        MigrationAction::Update(key.clone(), (new_key, new_value))
    };

    METADATA_EXPORT
        .migrate_to::<_, v43::DurableExportMetadata>(tx, |entries| {
            entries.iter().map(action).collect()
        })
        .await
}
