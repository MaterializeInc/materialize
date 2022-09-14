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
pub mod envelope_cdc_v2;
mod schema;

pub use envelope_cdc_v2 as cdc_v2;

pub use self::decode::{Decoder, DiffPair};
pub use self::encode::{
    encode_datums_as_avro, encode_debezium_transaction_unchecked, get_debezium_transaction_schema,
    AvroEncoder, AvroSchemaGenerator,
};
pub use self::schema::{parse_schema, schema_to_relationdesc, ConfluentAvroResolver};

fn is_null(schema: &SchemaPieceOrNamed) -> bool {
    matches!(schema, SchemaPieceOrNamed::Piece(SchemaPiece::Null))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use ordered_float::OrderedFloat;

    use mz_avro::types::{DecimalValue, Value};
    use mz_repr::adt::numeric::{self, NumericMaxScale};
    use mz_repr::{Datum, RelationDesc, ScalarType};

    use super::*;

    #[test]
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

    #[test]
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
        let expected_desc = RelationDesc::empty()
            .with_column("f1", ScalarType::Int32.nullable(false))
            .with_column("f2", ScalarType::String.nullable(false));

        assert_eq!(desc, expected_desc);
        Ok(())
    }

    #[test]
    /// Test that primitive Avro Schema types are allow Datums to be correctly
    /// serialized into Avro Values.
    ///
    /// Complete list of primitive types in test, also found in this
    /// documentation:
    /// https://avro.apache.org/docs/current/spec.html#schemas
    fn test_diff_pair_to_avro_primitive_types() -> anyhow::Result<()> {
        use numeric::Numeric;
        // Data to be used later in assertions.
        let date = NaiveDate::from_ymd(2020, 1, 8);
        let date_time = NaiveDateTime::new(date, NaiveTime::from_hms(1, 1, 1));
        let bytes: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10];
        let string = String::from("test");

        // Simple transformations from primitive Avro Schema types
        // to Avro Values.
        let valid_pairings = vec![
            (ScalarType::Bool, Datum::True, Value::Boolean(true)),
            (ScalarType::Bool, Datum::False, Value::Boolean(false)),
            (ScalarType::Int32, Datum::Int32(1), Value::Int(1)),
            (ScalarType::Int64, Datum::Int64(1), Value::Long(1)),
            (
                ScalarType::Float32,
                Datum::Float32(OrderedFloat::from(1f32)),
                Value::Float(1f32),
            ),
            (
                ScalarType::Float64,
                Datum::Float64(OrderedFloat::from(1f64)),
                Value::Double(1f64),
            ),
            (ScalarType::Date, Datum::Date(date), Value::Date(date)),
            (
                ScalarType::Timestamp,
                Datum::Timestamp(date_time),
                Value::Timestamp(date_time),
            ),
            (
                ScalarType::TimestampTz,
                Datum::TimestampTz(DateTime::from_utc(date_time, Utc)),
                Value::Timestamp(date_time),
            ),
            (
                ScalarType::Numeric {
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
                ScalarType::Numeric { max_scale: None },
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
                ScalarType::Bytes,
                Datum::Bytes(&bytes),
                Value::Bytes(bytes.clone()),
            ),
            (
                ScalarType::String,
                Datum::String(&string),
                Value::String(string.clone()),
            ),
        ];
        for (typ, datum, expected) in valid_pairings {
            let desc = RelationDesc::empty().with_column("column1", typ.nullable(false));
            let schema_generator = AvroSchemaGenerator::new(None, None, None, desc, false);
            let avro_value =
                encode_datums_as_avro(std::iter::once(datum), schema_generator.value_columns());
            assert_eq!(
                Value::Record(vec![("column1".into(), expected)]),
                avro_value
            );
        }

        Ok(())
    }
}
