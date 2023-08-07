// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use mz_repr::{Datum, GlobalId, RelationDesc, Row, ScalarType, Diff};
use once_cell::sync::Lazy;

use crate::controller::{CollectionManager, IntrospectionType};

pub fn pack_status_row(
    collection_id: GlobalId,
    status_name: &str,
    error: Option<&str>,
    ts: u64,
    hint: Option<&str>,
) -> Row {
    let timestamp = NaiveDateTime::from_timestamp_opt(
        (ts / 1000)
            .try_into()
            .expect("timestamp seconds does not fit into i64"),
        (ts % 1000 * 1_000_000)
            .try_into()
            .expect("timestamp millis does not fit into a u32"),
    )
    .unwrap();
    let timestamp = Datum::TimestampTz(
        DateTime::from_utc(timestamp, Utc)
            .try_into()
            .expect("must fit"),
    );
    let collection_id = collection_id.to_string();
    let collection_id = Datum::String(&collection_id);
    let status = Datum::String(status_name);
    let error = error.into();

    let mut row = Row::default();
    let mut packer = row.packer();
    packer.extend([timestamp, collection_id, status, error]);

    match hint {
        Some(hint) => {
            let metadata = vec![("hint", Datum::String(hint))];
            packer.push_dict(metadata);
        }
        None => packer.push(Datum::Null),
    };
    row
}

pub static MZ_SINK_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("occurred_at", ScalarType::TimestampTz.nullable(false))
        .with_column("sink_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("occurred_at", ScalarType::TimestampTz.nullable(false))
        .with_column("source_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

/// Represents an entry in `mz_internal.mz_{source|sink}_status_history`
/// For brevity's sake, this is represented as a single type because currently
/// the two relation's have the same shape. If thta changes, we can bifurcate this
pub struct RawStatusUpdate<'a> {
    /// The ID of the collection
    pub collection_id: GlobalId,
    /// The name of the status update
    pub status_name: &'a str,
    /// A succinct error, if any
    pub error: Option<&'a str>,
    /// The timestamp of the status update
    pub ts: u64,
    /// Additional information regarding the update
    pub hint: Option<&'a str>,
}

/// A lightweight wrapper around [`CollectionManager`] that assists with
/// appending status updates to to `mz_internal.mz_{source|status}_history`
#[derive(Debug, Clone)]
pub struct CollectionStatusManager {
    /// Managed collection interface
    collection_manager: CollectionManager,
    /// A list of introspection IDs for managed collections
    introspection_ids: Arc<std::sync::Mutex<BTreeMap<IntrospectionType, GlobalId>>>,
}

impl CollectionStatusManager {
    pub fn new(
        collection_manager: CollectionManager,
        introspection_ids: Arc<std::sync::Mutex<BTreeMap<IntrospectionType, GlobalId>>>,
    ) -> Self {
        Self {
            collection_manager,
            introspection_ids,
        }
    }

    fn pack_status_updates(updates: Vec<RawStatusUpdate<'_>>) -> Vec<(Row, Diff)> {
        updates
            .into_iter()
            .map(
                |RawStatusUpdate {
                     collection_id,
                     status_name,
                     error,
                     ts,
                     hint,
                 }| {
                    let row = pack_status_row(collection_id, status_name, error, ts, hint);

                    (row, 1)
                },
            )
            .collect()
    }

    /// Appends updates for sources to the appropriate managed status collection
    ///
    /// # Panics
    ///
    /// Panics if the source status history collection is not registered in `introspection_ids`
    pub async fn append_source_updates(&self, updates: Vec<RawStatusUpdate<'_>>) {
        let source_status_history_id = *self
            .introspection_ids
            .lock()
            .expect("poisoned lock")
            .get(&IntrospectionType::SourceStatusHistory)
            .expect("source status history collection to be registered");

        self.collection_manager
            .append_to_collection(source_status_history_id, Self::pack_status_updates(updates))
            .await;
    }

    /// Appends updates for sinks to the appropriate managed status collection
    ///
    /// # Panics
    ///
    /// Panics if the sink status history collection is not registered in `introspection_ids`
    pub async fn append_sink_updates(&self, updates: Vec<RawStatusUpdate<'_>>) {
        let sink_status_history_id = *self
            .introspection_ids
            .lock()
            .expect("poisoned lock")
            .get(&IntrospectionType::SinkStatusHistory)
            .expect("source status history collection to be registered");

        self.collection_manager
            .append_to_collection(sink_status_history_id, Self::pack_status_updates(updates))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_row() {
        let error_message = "error message";
        let hint = "hint message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = pack_status_row(id, status, Some(error_message), 1000, Some(hint));

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));
        assert_eq!(
            row.iter()
                .nth(4)
                .unwrap()
                .unwrap_map()
                .iter()
                .collect::<Vec<_>>(),
            vec![("hint", Datum::String(hint))]
        );
    }

    #[mz_ore::test]
    fn test_row_without_hint() {
        let error_message = "error message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = pack_status_row(id, status, Some(error_message), 1000, None);

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));
        assert_eq!(row.iter().nth(4).unwrap(), Datum::Null);
    }

    #[mz_ore::test]
    fn test_row_without_error() {
        let id = GlobalId::User(1);
        let status = "dropped";
        let hint = "hint message";
        let row = pack_status_row(id, status, None, 1000, Some(hint));

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::Null);
        assert_eq!(
            row.iter()
                .nth(4)
                .unwrap()
                .unwrap_map()
                .iter()
                .collect::<Vec<_>>(),
            vec![("hint", Datum::String(hint))]
        );
    }
}
