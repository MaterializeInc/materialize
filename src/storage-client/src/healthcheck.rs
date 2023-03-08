// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, NaiveDateTime, Utc};
use once_cell::sync::Lazy;

use mz_repr::{Datum, GlobalId, RelationDesc, Row, ScalarType};

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
    let hint: Datum = hint.into();
    let mut row = Row::pack_slice(&[timestamp, collection_id, status, error]);
    row.packer().push_dict_with(|row| {
        row.push(Datum::String("hint"));
        row.push(hint);
    });
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
