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
/// appending status updates to to `mz_internal.mz_{source|status}_history`
#[derive(Debug, Clone)]
pub struct CollectionStatusManager<T>
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

/// Provides [`IntrospectionStatusManager`] a means of generating specific types
/// of envelopes for data from each [`IntrospectionType`].
pub trait IntrospectionStatusUpdate {
    /// Provide this record's GlobalId, which is used as a key when determining
    /// whether or not to produce values.
    fn global_id(&self) -> usize;

    // Determine whether or not to produce or drop this value.
    fn produce_new(&self, prev: &Row, new: &Row) -> bool;
}

impl TryFrom<IntrospectionType> for &dyn IntrospectionStatusUpdate {
    type Error = ();
    fn try_from(value: IntrospectionType) -> Result<&'static dyn IntrospectionStatusUpdate, ()> {
        match value {
            IntrospectionType::SourceStatusHistory => Ok(&SourceStatusUpdates),
            IntrospectionType::SinkStatusHistory => Ok(&SinkStatusUpdates),
            _ => return Err(()),
        }
    }
}

struct SourceStatusUpdates;

impl IntrospectionStatusUpdate for SourceStatusUpdates {
    fn global_id(&self) -> usize {
        healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("source_id"))
            .expect("schema has not changed")
            .0
    }

    fn produce_new(&self, prev: &Row, new: &Row) -> bool {
        let status = healthcheck::MZ_SOURCE_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("status"))
            .expect("schema has not changed")
            .0;
        let new = new
            .iter()
            .nth(status)
            .expect("MZ_SOURCE_STATUS_HISTORY_DESC unchanged")
            .unwrap_str();
        let prev = prev
            .iter()
            .nth(status)
            .expect("MZ_SOURCE_STATUS_HISTORY_DESC unchanged")
            .unwrap_str();

        match (new, prev) {
            // TODO(guswynn): Ideally only `failed` sources should not be marked as paused.
            // Additionally, dropping a replica and then restarting environmentd will
            // fail this check. This will all be resolved in:
            // https://github.com/MaterializeInc/materialize/pull/23013
            (new, prev) if new == "paused" && prev == "stalled" => false,
            // Don't re-mark that object as paused.
            (new, prev) if new == "paused" && prev == "paused" => false,
            // De-duplication of other statuses is currently managed by the
            // `health_operator`.
            _ => true,
        }
    }
}
struct SinkStatusUpdates;

impl IntrospectionStatusUpdate for SinkStatusUpdates {
    fn global_id(&self) -> usize {
        healthcheck::MZ_SINK_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("sink_id"))
            .expect("schema has not changed")
            .0
    }

    fn produce_new(&self, prev: &Row, new: &Row) -> bool {
        let status = healthcheck::MZ_SINK_STATUS_HISTORY_DESC
            .get_by_name(&ColumnName::from("status"))
            .expect("schema has not changed")
            .0;
        let new = new
            .iter()
            .nth(status)
            .expect("MZ_SINK_STATUS_HISTORY_DESC unchanged")
            .unwrap_str();
        let prev = prev
            .iter()
            .nth(status)
            .expect("MZ_SINK_STATUS_HISTORY_DESC unchanged")
            .unwrap_str();

        match (new, prev) {
            // TODO(guswynn): Ideally only `failed` sources should not be marked as paused.
            // Additionally, dropping a replica and then restarting environmentd will
            // fail this check. This will all be resolved in:
            // https://github.com/MaterializeInc/materialize/pull/23013
            (new, prev) if new == "paused" && prev == "stalled" => false,
            // Don't re-mark that object as paused.
            (new, prev) if new == "paused" && prev == "paused" => false,
            // De-duplication of other statuses is currently managed by the
            // `health_operator`.
            _ => true,
        }
    }
}

impl<T> CollectionStatusManager<T>
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

    pub fn extend_previous_statuses<I>(&mut self, previous_statuses: I)
    where
        I: IntoIterator<Item = (GlobalId, Row)>,
    {
        self.previous_statuses.extend(previous_statuses)
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
    pub async fn append_rows(&mut self, updates: Vec<Row>, type_: IntrospectionType) {
        let source_status_history_id = *self
            .introspection_ids
            .lock()
            .expect("poisoned lock")
            .get(&type_)
            .unwrap_or_else(|| panic!("{:?} status history collection to be registered", type_));

        let new: Vec<_> = updates
            .into_iter()
            .filter(|r| self.assess_row(type_, r))
            .map(|r| (r, 1))
            .collect();

        self.collection_manager
            .append_to_collection(source_status_history_id, new)
            .await;
    }

    // Determines if the row should be produced to the collection or ignored
    // based on the presence of the correlation between `IntrospectionType` and
    // `IntrospectionStatusUpdate`.
    fn assess_row(&mut self, type_: IntrospectionType, new: &Row) -> bool {
        let type_: &dyn IntrospectionStatusUpdate = match type_.try_into() {
            Ok(type_) => type_,
            Err(()) => return true,
        };

        let key = type_.global_id();
        let this_id = new
            .iter()
            .nth(key)
            .expect("schema has not changed")
            .unwrap_str();
        let this_id = GlobalId::from_str(this_id).expect("GlobalIds must be parseable from datums");

        match self.previous_statuses.get_mut(&this_id) {
            None => {
                self.previous_statuses.insert(this_id, new.clone());
                true
            }
            Some(prev) => {
                let produce = type_.produce_new(prev, new);
                if produce {
                    *prev = new.clone()
                }
                produce
            }
        }
    }

    async fn drop_items(
        &mut self,
        ids: Vec<GlobalId>,
        ts: DateTime<Utc>,
        type_: IntrospectionType,
    ) {
        self.append_rows(
            ids.iter()
                .map(|id| {
                    Row::from(RawStatusUpdate {
                        id: *id,
                        ts,
                        status_name: "dropped".to_string(),
                        error: None,
                        hints: Default::default(),
                        namespaced_errors: Default::default(),
                    })
                })
                .collect(),
            type_,
        )
        .await;
        // Note that don't remove the statuses from `previous_statuses`, as they will never be
        // cleared from the history table
        //
        // TODO(guswynn): clean up dropped sources/sinks from status history tables, at some point.
    }

    /// Marks the sources as dropped.
    ///
    /// # Panics
    ///
    /// Panics if the source status history collection is not registered in `introspection_ids`
    pub async fn drop_sources(&mut self, ids: Vec<GlobalId>, ts: DateTime<Utc>) {
        self.drop_items(ids, ts, IntrospectionType::SourceStatusHistory)
            .await;
    }

    /// Marks the sinks as dropped.
    ///
    /// # Panics
    ///
    /// Panics if the source status history collection is not registered in `introspection_ids`
    pub async fn drop_sinks(&mut self, ids: Vec<GlobalId>, ts: DateTime<Utc>) {
        self.drop_items(ids, ts, IntrospectionType::SinkStatusHistory)
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
