// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_repr::{ColumnName, Datum, GlobalId, Row, TimestampManipulation};
use mz_storage_client::healthcheck;
use once_cell::sync::Lazy;
use timely::progress::Timestamp;

use crate::collection_mgmt::CollectionManager;
use crate::IntrospectionType;

pub use mz_storage_client::healthcheck::*;

/// Represents an entry in `mz_internal.mz_{source|sink}_status_history`
/// For brevity's sake, this is represented as a single type because currently
/// the two relation's have the same shape. If thta changes, we can bifurcate this
#[derive(Clone)]
pub struct RawStatusUpdate {
    /// The ID of the collection
    pub id: GlobalId,
    /// The timestamp of the status update
    pub ts: DateTime<Utc>,
    /// The name of the status update
    pub status_name: String,
    /// A succinct error, if any
    pub error: Option<String>,
    /// Hints about the error, if any
    pub hints: BTreeSet<String>,
    /// The full set of namespaced errors that consolidated into the `error`, if any
    pub namespaced_errors: BTreeMap<String, String>,
}

impl RawStatusUpdate {
    pub fn dropped_status(id: GlobalId, ts: DateTime<Utc>) -> RawStatusUpdate {
        RawStatusUpdate {
            id,
            ts,
            status_name: "dropped".to_string(),
            error: None,
            hints: Default::default(),
            namespaced_errors: Default::default(),
        }
    }

    fn produce_new_status(prev: &str, new: &str) -> bool {
        match (prev, new) {
            // TODO(guswynn): Ideally only `failed` sources should not be marked as paused.
            // Additionally, dropping a replica and then restarting environmentd will
            // fail this check. This will all be resolved in:
            // https://github.com/MaterializeInc/materialize/pull/23013
            (prev, new) if prev == "stalled" && new == "paused" => false,
            // Don't re-mark that object as paused.
            (prev, new) if prev == "paused" && new == "paused" => false,
            // De-duplication of other statuses is currently managed by the
            // `health_operator`.
            _ => true,
        }
    }
}

impl From<RawStatusUpdate> for Row {
    fn from(update: RawStatusUpdate) -> Self {
        let timestamp = Datum::TimestampTz(update.ts.try_into().expect("must fit"));
        let id = update.id.to_string();
        let id = Datum::String(&id);
        let status = Datum::String(&update.status_name);
        let error = update.error.as_deref().into();

        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([timestamp, id, status, error]);

        if !update.hints.is_empty() || !update.namespaced_errors.is_empty() {
            packer.push_dict_with(|dict_packer| {
                // `hint` and `namespaced` are ordered,
                // as well as the BTree's they each contain.
                if !update.hints.is_empty() {
                    dict_packer.push(Datum::String("hints"));
                    dict_packer.push_list(update.hints.iter().map(|s| Datum::String(s)));
                }
                if !update.namespaced_errors.is_empty() {
                    dict_packer.push(Datum::String("namespaced"));
                    dict_packer.push_dict(
                        update
                            .namespaced_errors
                            .iter()
                            .map(|(k, v)| (k.as_str(), Datum::String(v))),
                    );
                }
            });
        } else {
            packer.push(Datum::Null);
        }

        row
    }
}

/// A lightweight wrapper around [`CollectionManager`] that assists with
/// appending status updates to ntrospection statuses.
///
/// This struct is meant to contain the logic of providing envelopes over data
/// for introspection collections via [`IntrospectionStatusUpdate`]. If the
/// introspection data does not need any kind of envelope (i.e. it is truly
/// append only), you can instead use the collection manager directly.
#[derive(Debug, Clone)]
pub struct IntrospectionStatusManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    /// Managed collection interface
    collection_manager: CollectionManager<T>,
    /// A list of introspection IDs for managed collections
    introspection_ids: Arc<std::sync::Mutex<BTreeMap<IntrospectionType, GlobalId>>>,
    /// The previous value seen for each `GlobalId` of a specific type.
    previous_statuses: BTreeMap<GlobalId, Row>,
}

struct IntrospectionStatusUpdate {
    /// The column in the row that correlates to the `GlobalId`.
    pub global_id_idx: usize,
    /// The column in the row that correlates to the value used to determine
    /// whether or not to produce the data.
    ///
    /// All `IntrospectionStatusUpdate` impls currently rely on one column from
    /// the row to determine whether or not the value should be produced, so
    /// it's convenient to only need to track a single index.
    pub value_idx: usize,
    /// The comparison between previous and new values to determine whether or
    /// not to produce the new value.
    pub produce_new_value: fn(Datum, Datum) -> bool,
}

impl TryFrom<IntrospectionType> for &IntrospectionStatusUpdate {
    type Error = ();
    fn try_from(value: IntrospectionType) -> Result<&'static IntrospectionStatusUpdate, ()> {
        match value {
            IntrospectionType::SourceStatusHistory => Ok(&SOURCE_STATUS_UPDATES),
            IntrospectionType::SinkStatusHistory => Ok(&SINK_STATUS_UPDATES),
            _ => Err(()),
        }
    }
}

static SOURCE_STATUS_UPDATES: Lazy<IntrospectionStatusUpdate> = Lazy::new(|| {
    IntrospectionStatusUpdate {
        global_id_idx: healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("source_id"))
            .expect("schema has not changed")
            .0,

        value_idx: healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("status"))
            .expect("schema has not changed")
            .0,

        produce_new_value: |prev: Datum, new: Datum| -> bool {
            let new = new.unwrap_str();
            let prev = prev.unwrap_str();

            // The rows for SourceStatusUpdates are derived from RawStatusUpdate
            RawStatusUpdate::produce_new_status(prev, new)
        },
    }
});

static SINK_STATUS_UPDATES: Lazy<IntrospectionStatusUpdate> = Lazy::new(|| {
    IntrospectionStatusUpdate {
        global_id_idx: healthcheck::MZ_SINK_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("sink_id"))
            .expect("schema has not changed")
            .0,

        value_idx: healthcheck::MZ_SINK_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("status"))
            .expect("schema has not changed")
            .0,

        produce_new_value: |prev: Datum, new: Datum| -> bool {
            let new = new.unwrap_str();
            let prev = prev.unwrap_str();

            // The rows for SinkStatusUpdates are derived from RawStatusUpdate
            RawStatusUpdate::produce_new_status(prev, new)
        },
    }
});

impl<T> IntrospectionStatusManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation + From<EpochMillis>,
{
    pub fn new(
        collection_manager: CollectionManager<T>,
        introspection_ids: Arc<std::sync::Mutex<BTreeMap<IntrospectionType, GlobalId>>>,
    ) -> Self {
        Self {
            collection_manager,
            introspection_ids,
            previous_statuses: Default::default(),
        }
    }

    /// Seeds `IntrospectionStatusManager` with the last-seen statuses from
    /// collections without producing them. This allows continuity with the
    /// status collections across restarts.
    pub fn extend_previous_statuses<I>(&mut self, previous_statuses: I, type_: IntrospectionType)
    where
        I: IntoIterator<Item = Row>,
    {
        let _: &IntrospectionStatusUpdate = match type_.try_into() {
            Ok(type_) => type_,
            // Do not remember any previous values for types without
            // IntrospectionStatusUpdate.
            Err(()) => return,
        };

        for row in previous_statuses {
            // Examining whether or not we should produce the row performs the
            // book keeping of placing it in memory.
            let r = self.should_produce_row(type_, &row);
            mz_ore::soft_assert_no_log!(
                r,
                "extend_previous_statuses should not result in dropping any updates"
            );
        }
    }

    /// Appends updates for sources to the appropriate managed status
    /// collection.
    ///
    /// If `type_` is correlated to an [`IntrospectionStatusUpdate`]
    /// implementation, the rows will have envelope-like behavior applied.
    /// Otherwise, the rows will be treated as append-only.
    ///
    /// # Panics
    ///
    /// Panics if `type_` is not registered in `introspection_ids`
    pub async fn append_rows(&mut self, type_: IntrospectionType, updates: Vec<Row>) {
        let introspection_id = *self
            .introspection_ids
            .lock()
            .expect("poisoned lock")
            .get(&type_)
            .unwrap_or_else(|| panic!("{:?} status history collection to be registered", type_));

        let new: Vec<_> = updates
            .into_iter()
            .filter(|r| self.should_produce_row(type_, r))
            .map(|r| (r, 1))
            .collect();

        self.collection_manager
            .append_to_collection(introspection_id, new)
            .await;
    }

    /// Determines if the row should be produced to the collection or ignored
    /// based on the presence of the correlation between `IntrospectionType` and
    /// `IntrospectionStatusUpdate`.
    ///
    /// Also tracks the value of the last-produced value for the inferred
    /// `GlobalId` if appropriate.
    fn should_produce_row(&mut self, type_: IntrospectionType, new: &Row) -> bool {
        let type_: &IntrospectionStatusUpdate = match type_.try_into() {
            Ok(type_) => type_,
            // Treat all IntrospectionTypes without an IntrospectionStatusUpdate
            // impl as append-only
            Err(()) => return true,
        };

        let key = type_.global_id_idx;
        let this_id = new
            .iter()
            .nth(key)
            .expect("schema has not changed")
            .unwrap_str();
        let this_id = GlobalId::from_str(this_id).expect("GlobalIds must be parseable from datums");
        let new_val = new.iter().nth(type_.value_idx).expect("schema unchanged");

        let produce = match self.previous_statuses.get_mut(&this_id) {
            None => {
                let row = Row::pack_slice(&[new_val]);
                self.previous_statuses.insert(this_id, row);
                true
            }
            Some(prev) => {
                let prev_val = prev.iter().next().expect("prev value always one datum");
                let produce = (type_.produce_new_value)(prev_val, new_val);
                if produce {
                    let mut row = prev.packer();
                    row.push(new_val);
                }
                produce
            }
        };

        produce
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
        let row = Row::from(RawStatusUpdate {
            id,
            ts: chrono::offset::Utc::now(),
            status_name: status.to_string(),
            error: Some(error_message.to_string()),
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: Default::default(),
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let hint_datum = &details[0];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );
    }

    #[mz_ore::test]
    fn test_row_without_hint() {
        let error_message = "error message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = Row::from(RawStatusUpdate {
            id,
            ts: chrono::offset::Utc::now(),
            status_name: status.to_string(),
            error: Some(error_message.to_string()),
            hints: Default::default(),
            namespaced_errors: Default::default(),
        });

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
        let row = Row::from(RawStatusUpdate {
            id,
            ts: chrono::offset::Utc::now(),
            status_name: status.to_string(),
            error: None,
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: Default::default(),
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::Null);

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let hint_datum = &details[0];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );
    }

    #[mz_ore::test]
    fn test_row_with_namespaced() {
        let error_message = "error message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = Row::from(RawStatusUpdate {
            id,
            ts: chrono::offset::Utc::now(),
            status_name: status.to_string(),
            error: Some(error_message.to_string()),
            hints: Default::default(),
            namespaced_errors: BTreeMap::from([("thing".to_string(), "error".to_string())]),
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let ns_datum = &details[0];

        assert_eq!(ns_datum.0, "namespaced");
        assert_eq!(
            ns_datum.1.unwrap_map().iter().next().unwrap(),
            ("thing", Datum::String("error"))
        );
    }

    #[mz_ore::test]
    fn test_row_with_everything() {
        let error_message = "error message";
        let hint = "hint message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = Row::from(RawStatusUpdate {
            id,
            ts: chrono::offset::Utc::now(),
            status_name: status.to_string(),
            error: Some(error_message.to_string()),
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: BTreeMap::from([("thing".to_string(), "error".to_string())]),
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 2);
        // These are always sorted
        let hint_datum = &details[0];
        let ns_datum = &details[1];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );

        assert_eq!(ns_datum.0, "namespaced");
        assert_eq!(
            ns_datum.1.unwrap_map().iter().next().unwrap(),
            ("thing", Datum::String("error"))
        );
    }
}
