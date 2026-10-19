// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A human-readable JSON encoding for literal rows.
//!
//! A row is encoded as `{"$row": [datum, ...]}`, where each datum is `"Null"` or a
//! single-key object naming the datum kind, for example `{"Int64": 7}`. Only a small set of
//! datum kinds is supported. [`StableRow`](super::StableRow) serializes through this encoding
//! only inside [`with_readable_rows`], so the default wire format is unaffected.

use std::cell::Cell;

use dec::OrderedDecimal;
use serde_json::{Map, Value, json};

use crate::adt::numeric::{self, Numeric};
use crate::{Datum, Row, Timestamp};

const ROW_KEY: &str = "$row";

thread_local! {
    static READABLE_ROWS: Cell<bool> = const { Cell::new(false) };
}

/// Reports whether the current thread is inside [`with_readable_rows`].
pub(super) fn readable_rows_enabled() -> bool {
    READABLE_ROWS.with(Cell::get)
}

/// Runs `f` with readable row serialization enabled on the current thread.
///
/// While enabled, `StableRow` serializes as [`row_to_readable`] when that returns `Some`.
/// The previous setting is restored when `f` returns or unwinds.
pub fn with_readable_rows<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            READABLE_ROWS.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(READABLE_ROWS.with(|flag| flag.replace(true)));
    f()
}

/// Encodes `row` as `{"$row": [datum, ...]}`, or returns `None` if any datum is unsupported.
pub fn row_to_readable(row: &Row) -> Option<Value> {
    let datums = row
        .iter()
        .map(datum_to_readable)
        .collect::<Option<Vec<_>>>()?;
    Some(json!({ ROW_KEY: datums }))
}

fn datum_to_readable(datum: Datum<'_>) -> Option<Value> {
    Some(match datum {
        Datum::Null => json!("Null"),
        Datum::True => json!({ "Bool": true }),
        Datum::False => json!({ "Bool": false }),
        Datum::Int16(n) => json!({ "Int16": n }),
        Datum::Int32(n) => json!({ "Int32": n }),
        Datum::Int64(n) => json!({ "Int64": n }),
        Datum::UInt64(n) => json!({ "UInt64": n }),
        // JSON has no NaN or infinities.
        Datum::Float64(x) if x.is_finite() => json!({ "Float64": x.into_inner() }),
        Datum::Numeric(n) => json!({ "Numeric": n.0.to_string() }),
        Datum::String(s) => json!({ "String": s }),
        Datum::MzTimestamp(t) => json!({ "MzTimestamp": t.to_string() }),
        _ => return None,
    })
}

/// Decodes the encoding produced by [`row_to_readable`].
pub fn row_from_readable(value: &Value) -> Result<Row, String> {
    let obj = value
        .as_object()
        .filter(|obj| obj.len() == 1)
        .ok_or_else(|| format!("expected an object with the single key {ROW_KEY:?}"))?;
    let datums = obj
        .get(ROW_KEY)
        .ok_or_else(|| format!("expected the key {ROW_KEY:?}"))?
        .as_array()
        .ok_or_else(|| format!("{ROW_KEY:?} must be an array"))?;
    let mut row = Row::default();
    let mut packer = row.packer();
    for datum in datums {
        packer.push(datum_from_readable(datum)?);
    }
    Ok(row)
}

fn datum_from_readable(value: &Value) -> Result<Datum<'_>, String> {
    if value.as_str() == Some("Null") {
        return Ok(Datum::Null);
    }
    let (kind, inner) = single_entry(value)
        .ok_or_else(|| format!("expected \"Null\" or a single-key object, got {value}"))?;
    let bad = || format!("invalid {kind} datum: {inner}");
    Ok(match kind.as_str() {
        "Bool" => Datum::from(inner.as_bool().ok_or_else(bad)?),
        "Int16" => Datum::Int16(
            inner
                .as_i64()
                .and_then(|n| i16::try_from(n).ok())
                .ok_or_else(bad)?,
        ),
        "Int32" => Datum::Int32(
            inner
                .as_i64()
                .and_then(|n| i32::try_from(n).ok())
                .ok_or_else(bad)?,
        ),
        "Int64" => Datum::Int64(inner.as_i64().ok_or_else(bad)?),
        "UInt64" => Datum::UInt64(inner.as_u64().ok_or_else(bad)?),
        "Float64" => {
            let x = inner.as_f64().filter(|x| x.is_finite()).ok_or_else(bad)?;
            Datum::Float64(x.into())
        }
        "Numeric" => {
            let n: Numeric = numeric::cx_datum()
                .parse(inner.as_str().ok_or_else(bad)?)
                .map_err(|_| bad())?;
            Datum::Numeric(OrderedDecimal(n))
        }
        "String" => Datum::String(inner.as_str().ok_or_else(bad)?),
        "MzTimestamp" => {
            let ms = inner
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(bad)?;
            Datum::MzTimestamp(Timestamp::from(ms))
        }
        other => return Err(format!("unsupported datum kind {other:?}")),
    })
}

fn single_entry(value: &Value) -> Option<(&String, &Value)> {
    let obj: &Map<String, Value> = value.as_object()?;
    let mut entries = obj.iter();
    let entry = entries.next()?;
    entries.next().is_none().then_some(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::StableRow;

    fn parse_numeric(s: &str) -> Numeric {
        numeric::cx_datum().parse(s).expect("valid numeric")
    }

    #[mz_ore::test]
    fn readable_roundtrip() {
        let datums = [
            Datum::Null,
            Datum::True,
            Datum::False,
            Datum::Int16(-7),
            Datum::Int16(i16::MAX),
            Datum::Int32(i32::MIN),
            Datum::Int64(i64::MIN),
            Datum::UInt64(u64::MAX),
            Datum::Float64(1.5.into()),
            Datum::Float64((-0.0).into()),
            Datum::Numeric(OrderedDecimal(parse_numeric("1.50"))),
            Datum::Numeric(OrderedDecimal(parse_numeric("-1E+10"))),
            Datum::String(""),
            Datum::String("a \"quoted\" \u{1F600} string"),
            Datum::MzTimestamp(Timestamp::from(123u64)),
        ];
        for datum in datums {
            let row = Row::pack_slice(&[datum]);
            let json = row_to_readable(&row).expect("supported");
            assert_eq!(row_from_readable(&json), Ok(row), "datum {datum:?}");
        }
        let row = Row::pack_slice(&datums);
        let json = row_to_readable(&row).expect("supported");
        assert_eq!(row_from_readable(&json), Ok(row));
        assert_eq!(
            row_to_readable(&Row::pack_slice(&[Datum::Int64(7), Datum::Null])),
            Some(json!({"$row": [{"Int64": 7}, "Null"]})),
        );
    }

    #[mz_ore::test]
    fn readable_unsupported_is_none() {
        let date = crate::adt::date::Date::from_unix_epoch(0).expect("valid date");
        let row = Row::pack_slice(&[Datum::Int64(1), Datum::Date(date)]);
        assert_eq!(row_to_readable(&row), None);
        let row = Row::pack_slice(&[Datum::Float64(f64::NAN.into())]);
        assert_eq!(row_to_readable(&row), None);
    }

    #[mz_ore::test]
    fn readable_rejects_garbage() {
        for bad in [
            json!({"$row": [{"Int64": "x"}]}),
            json!({"rows": []}),
            json!({"$row": [], "extra": 1}),
            json!({"$row": 3}),
            json!({"$row": [{"Int16": 70000}]}),
            json!({"$row": [{"UInt64": -1}]}),
            json!({"$row": [{"Numeric": "abc"}]}),
            json!({"$row": [{"MzTimestamp": "x"}]}),
            json!({"$row": [{"Nope": 1}]}),
            json!({"$row": [{"Int64": 1, "Int32": 2}]}),
            json!({"$row": ["Int64"]}),
            json!([]),
        ] {
            assert!(row_from_readable(&bad).is_err(), "accepted {bad}");
        }
    }

    #[mz_ore::test]
    fn stable_row_serializes_readable_only_in_scope() {
        let row = Row::pack_slice(&[Datum::Int64(7), Datum::String("x")]);
        let stable = StableRow(row);

        let default = serde_json::to_value(&stable).expect("serializes");
        assert!(default.is_array(), "expected a byte array, got {default}");

        let readable = with_readable_rows(|| serde_json::to_value(&stable).expect("serializes"));
        assert_eq!(readable, json!({"$row": [{"Int64": 7}, {"String": "x"}]}));

        assert_eq!(serde_json::to_value(&stable).expect("serializes"), default);

        let unsupported = StableRow(Row::pack_slice(&[Datum::Interval(Default::default())]));
        let fallback =
            with_readable_rows(|| serde_json::to_value(&unsupported).expect("serializes"));
        assert!(fallback.is_array());
    }
}
