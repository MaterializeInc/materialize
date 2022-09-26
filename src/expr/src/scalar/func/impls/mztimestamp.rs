// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, NaiveDateTime, Utc};

use mz_ore::result::ResultExt;
use mz_repr::{adt::numeric::Numeric, strconv, Timestamp};

use crate::EvalError;

// Conversions to MzTimestamp, and a single conversion from MzTimestamp to
// String. In general we want to make MzTimestamp a more opaque type, so we
// easily support casting things to it but not from it.

sqlfunc!(
    #[sqlname = "mz_timestamp_to_text"]
    #[preserves_uniqueness = true]
    fn cast_mz_timestamp_to_string(a: Timestamp) -> String {
        let mut buf = String::new();
        strconv::format_mztimestamp(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "text_to_mz_timestamp"]
    fn cast_string_to_mz_timestamp(a: String) -> Result<Timestamp, EvalError> {
        strconv::parse_mztimestamp(&a).err_into()
    }
);

sqlfunc!(
    #[sqlname = "numeric_to_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_numeric_to_mz_timestamp(a: Numeric) -> Result<Timestamp, EvalError> {
        // The try_into will error if the conversion is lossy (out of range or fractional).
        a.try_into().map_err(|_| EvalError::MzTimestampOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "uint8_to_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_uint64_to_mz_timestamp(a: u64) -> Timestamp {
        a.into()
    }
);

sqlfunc!(
    #[sqlname = "uint4_to_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_uint32_to_mz_timestamp(a: u32) -> Timestamp {
        u64::from(a).into()
    }
);

sqlfunc!(
    #[sqlname = "bigint_to_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_int64_to_mz_timestamp(a: i64) -> Result<Timestamp, EvalError> {
        a.try_into().map_err(|_| EvalError::MzTimestampOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "integer_to_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn cast_int32_to_mz_timestamp(a: i32) -> Result<Timestamp, EvalError> {
        i64::from(a)
            .try_into()
            .map_err(|_| EvalError::MzTimestampOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "timestamp_tz_to_mz_timestamp"]
    fn cast_timestamp_tz_to_mz_timestamp(a: DateTime<Utc>) -> Result<Timestamp, EvalError> {
        a.timestamp_millis()
            .try_into()
            .map_err(|_| EvalError::MzTimestampOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "timestamp_to_mz_timestamp"]
    fn cast_timestamp_to_mz_timestamp(a: NaiveDateTime) -> Result<Timestamp, EvalError> {
        a.timestamp_millis()
            .try_into()
            .map_err(|_| EvalError::MzTimestampOutOfRange)
    }
);

sqlfunc!(
    #[sqlname = "step_mz_timestamp"]
    #[preserves_uniqueness = true]
    fn step_mz_timestamp(a: Timestamp) -> Result<Timestamp, EvalError> {
        a.checked_add(1).ok_or(EvalError::MzTimestampStepOverflow)
    }
);
