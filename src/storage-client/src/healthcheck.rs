// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDateTime, Utc};
use mz_repr::{Datum, GlobalId, RelationDesc, Row, ScalarType};
use once_cell::sync::Lazy;

pub fn pack_status_row(
    collection_id: GlobalId,
    status_name: &str,
    error: Option<&str>,
    hints: &BTreeSet<String>,
    namespaced_errors: &BTreeMap<String, &String>,
    ts: u64,
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

    if !hints.is_empty() || !namespaced_errors.is_empty() {
        packer.push_dict_with(|dict_packer| {
            // `hint` and `namespaced` are ordered,
            // as well as the BTree's they each contain.
            if !hints.is_empty() {
                dict_packer.push(Datum::String("hints"));
                dict_packer.push_list(hints.iter().map(|s| Datum::String(s)));
            }
            if !namespaced_errors.is_empty() {
                dict_packer.push(Datum::String("namespaced"));
                dict_packer.push_dict(
                    namespaced_errors
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

pub static MZ_SINK_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("sink_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

// NOTE: Update the views `mz_statement_execution_history_redacted`
// and `mz_activity_log`, and `mz_activity_log_redacted` whenever this
// is updated, to include the new columns where appropriate.
//
// The `redacted` views should contain only those columns that should
// be queryable by support.
pub static MZ_PREPARED_STATEMENT_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column("session_id", ScalarType::Uuid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("sql", ScalarType::String.nullable(false))
        .with_column("redacted_sql", ScalarType::String.nullable(false))
        .with_column(
            "prepared_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
});

pub static MZ_SESSION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column(
            "connected_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "initial_application_name",
            ScalarType::String.nullable(false),
        )
        .with_column("authenticated_user", ScalarType::String.nullable(false))
});

// NOTE: Update the views `mz_statement_execution_history_redacted`
// and `mz_activity_log`, and `mz_activity_log_redacted` whenever this
// is updated, to include the new columns where appropriate.
//
// The `redacted` views should contain only those columns that should
// be queryable by support.
pub static MZ_STATEMENT_EXECUTION_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("id", ScalarType::Uuid.nullable(false))
        .with_column("prepared_statement_id", ScalarType::Uuid.nullable(false))
        .with_column("sample_rate", ScalarType::Float64.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(true))
        .with_column("application_name", ScalarType::String.nullable(false))
        .with_column("cluster_name", ScalarType::String.nullable(true))
        .with_column("transaction_isolation", ScalarType::String.nullable(false))
        // Note that this can't be a timestamp, as it might be u64::max,
        // which is out of range.
        .with_column("execution_timestamp", ScalarType::UInt64.nullable(true))
        .with_column("transaction_id", ScalarType::UInt64.nullable(false))
        .with_column(
            "params",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column(
            "began_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "finished_at",
            ScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("finished_status", ScalarType::String.nullable(true))
        .with_column("error_message", ScalarType::String.nullable(true))
        .with_column("rows_returned", ScalarType::Int64.nullable(true))
        .with_column("execution_strategy", ScalarType::String.nullable(true))
});

pub static MZ_SOURCE_STATUS_HISTORY_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "occurred_at",
            ScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("source_id", ScalarType::String.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("error", ScalarType::String.nullable(true))
        .with_column("details", ScalarType::Jsonb.nullable(true))
});

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_row() {
        let error_message = "error message";
        let hint = "hint message";
        let id = GlobalId::User(1);
        let status = "dropped";
        let row = pack_status_row(
            id,
            status,
            Some(error_message),
            &BTreeSet::from([hint.to_string()]),
            &Default::default(),
            1000,
        );

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
        let row = pack_status_row(
            id,
            status,
            Some(error_message),
            &Default::default(),
            &Default::default(),
            1000,
        );

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
        let row = pack_status_row(
            id,
            status,
            None,
            &BTreeSet::from([hint.to_string()]),
            &Default::default(),
            1000,
        );

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
        let row = pack_status_row(
            id,
            status,
            Some(error_message),
            &Default::default(),
            &BTreeMap::from([("thing".to_string(), &"error".to_string())]),
            1000,
        );

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
        let row = pack_status_row(
            id,
            status,
            Some(error_message),
            &BTreeSet::from([hint.to_string()]),
            &BTreeMap::from([("thing".to_string(), &"error".to_string())]),
            1000,
        );

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
