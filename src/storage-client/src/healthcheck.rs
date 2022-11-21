// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, NaiveDateTime, Utc};
use mz_repr::{Datum, GlobalId, Row};

pub fn pack_status_row(
    source_id: GlobalId,
    status_name: &str,
    error: Option<&str>,
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
    let source_id = source_id.to_string();
    let source_id = Datum::String(&source_id);
    let status = Datum::String(status_name);
    let error = error.as_deref().into();
    let metadata = Datum::Null;
    Row::pack_slice(&[timestamp, source_id, status, error, metadata])
}
