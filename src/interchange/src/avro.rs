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
mod envelope_debezium;
mod schema;

pub use envelope_cdc_v2 as cdc_v2;

pub use self::decode::{Decoder, DiffPair};
pub use self::encode::{
    encode_datums_as_avro, encode_debezium_transaction_unchecked, get_debezium_transaction_schema,
    AvroEncoder, AvroSchemaGenerator,
};
pub use self::envelope_debezium::DebeziumDeduplicationStrategy;
pub use self::schema::{
    parse_schema, validate_key_schema, validate_value_schema, ConfluentAvroResolver,
};

use self::decode::{AvroStringDecoder, OptionalRecordDecoder, RowWrapper};
use self::envelope_debezium::{AvroDebeziumDecoder, RowCoordinates};
use crate::json::build_row_schema_json;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EnvelopeType {
    None,
    Debezium,
    Upsert,
    CdcV2,
}

fn is_null(schema: &SchemaPieceOrNamed) -> bool {
    matches!(schema, SchemaPieceOrNamed::Piece(SchemaPiece::Null))
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use ordered_float::OrderedFloat;
    use serde::Deserialize;
    use std::fs::File;

    use mz_avro::types::{DecimalValue, Value};
    use repr::adt::numeric;
    use repr::{ColumnName, ColumnType, Datum, RelationDesc, ScalarType};

    use super::*;

    #[derive(Deserialize)]
    struct TestCase {
        name: String,
        input: serde_json::Value,
        expected: Vec<(ColumnName, ColumnType)>,
    }

    #[test]
    #[ignore] // TODO(benesch): update tests for diff envelope.
    fn test_schema_parsing() -> anyhow::Result<()> {
        let file = File::open("interchange/testdata/avro-schema.json")
            .context("opening test data file")?;
        let test_cases: Vec<TestCase> =
            serde_json::from_reader(file).context("parsing JSON test data")?;

        for tc in test_cases {
            // Stringifying the JSON we just parsed is rather silly, but it
            // avoids embedding JSON strings inside of JSON, which is hard on
            // the eyes.
            let schema = serde_json::to_string(&tc.input)?;
            let output = super::validate_value_schema(&schema, EnvelopeType::Debezium)?;
            assert_eq!(output, tc.expected, "failed test case name: {}", tc.name)
        }

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
                ScalarType::Numeric { scale: Some(1) },
                Datum::from(Numeric::from(1)),
                Value::Decimal(DecimalValue {
                    unscaled: bytes.clone(),
                    precision: 39,
                    scale: 1,
                }),
            ),
            (
                ScalarType::Numeric { scale: None },
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
            let desc = RelationDesc::empty().with_named_column("column1", typ.nullable(false));
            let schema_generator = AvroSchemaGenerator::new(None, desc, false);
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
