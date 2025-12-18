// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_avro::schema::{SchemaPiece, SchemaPieceOrNamed};

mod decode;
mod encode;
mod schema;

pub use crate::avro::decode::{Decoder, DiffPair};
pub use crate::avro::encode::{
    AvroEncoder, AvroSchemaGenerator, DocTarget, encode_datums_as_avro,
    encode_debezium_transaction_unchecked, get_debezium_transaction_schema,
};
pub use crate::avro::schema::{ConfluentAvroResolver, parse_schema, schema_to_relationdesc};

fn is_null(schema: &SchemaPieceOrNamed) -> bool {
    matches!(schema, SchemaPieceOrNamed::Piece(SchemaPiece::Null))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use mz_avro::types::{DecimalValue, Value};
    use mz_repr::adt::date::Date;
    use mz_repr::adt::numeric::{self, NumericMaxScale};
    use mz_repr::adt::timestamp::CheckedTimestamp;
    use mz_repr::{Datum, RelationDesc, SqlScalarType};
    use ordered_float::OrderedFloat;

    use super::*;

    #[mz_ore::test]
    fn record_without_fields() -> anyhow::Result<()> {
        let schema = r#"{
            "type": "record",
            "name": "test",
            "fields": []
        }"#;

        let desc = schema_to_relationdesc(parse_schema(schema)?)?;
        assert_eq!(desc.arity(), 0, "empty record produced rows");

        Ok(())
    }

    #[mz_ore::test]
    fn basic_record() -> anyhow::Result<()> {
        let schema = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                { "name": "f1", "type": "int" },
                { "name": "f2", "type": "string" }
            ]
        }"#;

        let desc = schema_to_relationdesc(parse_schema(schema)?)?;
        let expected_desc = RelationDesc::builder()
            .with_column("f1", SqlScalarType::Int32.nullable(false))
            .with_column("f2", SqlScalarType::String.nullable(false))
            .finish();

        assert_eq!(desc, expected_desc);
        Ok(())
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    /// Test that primitive Avro Schema types are allow Datums to be correctly
    /// serialized into Avro Values.
    ///
    /// Complete list of primitive types in test, also found in this
    /// documentation:
    /// https://avro.apache.org/docs/++version++/specification/#primitive-types
    fn test_diff_pair_to_avro_primitive_types() -> anyhow::Result<()> {
        use numeric::Numeric;
        // Data to be used later in assertions.
        let date = 365 * 50 + 20;
        let date_time = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2020, 1, 8).unwrap(),
            NaiveTime::from_hms_opt(1, 1, 1).unwrap(),
        );
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10];
        let string = String::from("test");

        // Simple transformations from primitive Avro Schema types
        // to Avro Values.
        let valid_pairings = vec![
            (SqlScalarType::Bool, Datum::True, Value::Boolean(true)),
            (SqlScalarType::Bool, Datum::False, Value::Boolean(false)),
            (SqlScalarType::Int32, Datum::Int32(1), Value::Int(1)),
            (SqlScalarType::Int64, Datum::Int64(1), Value::Long(1)),
            (
                SqlScalarType::Float32,
                Datum::Float32(OrderedFloat::from(1f32)),
                Value::Float(1f32),
            ),
            (
                SqlScalarType::Float64,
                Datum::Float64(OrderedFloat::from(1f64)),
                Value::Double(1f64),
            ),
            (
                SqlScalarType::Date,
                Datum::Date(Date::from_unix_epoch(date).unwrap()),
                Value::Date(date),
            ),
            (
                SqlScalarType::Timestamp { precision: None },
                Datum::Timestamp(CheckedTimestamp::from_timestamplike(date_time).unwrap()),
                Value::Timestamp(date_time),
            ),
            (
                SqlScalarType::TimestampTz { precision: None },
                Datum::TimestampTz(
                    CheckedTimestamp::from_timestamplike(DateTime::from_naive_utc_and_offset(
                        date_time, Utc,
                    ))
                    .unwrap(),
                ),
                Value::Timestamp(date_time),
            ),
            (
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::try_from(1_i64)?),
                },
                Datum::from(Numeric::from(1)),
                Value::Decimal(DecimalValue {
                    unscaled: bytes.clone(),
                    precision: 39,
                    scale: 1,
                }),
            ),
            (
                SqlScalarType::Numeric { max_scale: None },
                Datum::from(Numeric::from(1)),
                Value::Decimal(DecimalValue {
                    // equivalent to 1E39
                    unscaled: vec![
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 240, 80, 254, 147, 137,
                        67, 172, 196, 95, 101, 86, 128, 0, 0, 0, 0,
                    ],
                    precision: 81,
                    scale: 39,
                }),
            ),
            (
                SqlScalarType::Bytes,
                Datum::Bytes(&bytes),
                Value::Bytes(bytes.clone()),
            ),
            (
                SqlScalarType::String,
                Datum::String(&string),
                Value::String(string.clone()),
            ),
        ];
        for (typ, datum, expected) in valid_pairings {
            let desc = RelationDesc::builder()
                .with_column("column1", typ.nullable(false))
                .finish();
            let schema_generator =
                AvroSchemaGenerator::new(desc, false, Default::default(), "row", false, None, true)
                    .unwrap();
            let avro_value =
                encode_datums_as_avro(std::iter::once(datum), schema_generator.columns());
            assert_eq!(
                Value::Record(vec![("column1".into(), expected)]),
                avro_value
            );
        }

        Ok(())
    }
}
