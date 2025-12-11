// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// We need to allow the std::collections::HashMap type since it is directly used as a type
// parameter to the arrow Field::with_metadata method.
#![allow(clippy::disallowed_types)]

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, builder::*};
use arrow::datatypes::{
    DECIMAL_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, Field, Schema,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_repr::adt::jsonb::JsonbRef;
use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};

const EXTENSION_PREFIX: &str = "materialize.v1.";

pub struct ArrowBuilder {
    columns: Vec<ArrowColumn>,
    /// A crude estimate of the size of the data in the builder
    /// based on the size of the rows added to it.
    row_size_bytes: usize,
    /// The original schema, if provided. Used to preserve metadata in to_record_batch().
    original_schema: Option<Arc<Schema>>,
}

/// Converts a RelationDesc to an Arrow Schema.
pub fn desc_to_schema(desc: &RelationDesc) -> Result<Schema, anyhow::Error> {
    let mut fields = vec![];
    let mut errs = vec![];
    let mut seen_names = BTreeMap::new();
    for (col_name, col_type) in desc.iter() {
        let mut col_name = col_name.to_string();
        // If we allow columns with the same name we encounter two issues:
        // 1. The arrow crate will accidentally reuse the same buffers for the columns
        // 2. Many parquet readers will error when trying to read the file metadata
        // Instead we append a number to the end of the column name for any duplicates.
        // TODO(roshan): We should document this when writing the copy-to-s3 MZ docs.
        seen_names
            .entry(col_name.clone())
            .and_modify(|e: &mut u32| {
                *e += 1;
                col_name += &e.to_string();
            })
            .or_insert(1);
        match scalar_to_arrow_datatype(&col_type.scalar_type) {
            Ok((data_type, extension_type_name)) => {
                fields.push(field_with_typename(
                    &col_name,
                    data_type,
                    col_type.nullable,
                    &extension_type_name,
                ));
            }
            Err(err) => errs.push(err.to_string()),
        }
    }
    if !errs.is_empty() {
        anyhow::bail!("Relation contains unimplemented arrow types: {:?}", errs);
    }
    Ok(Schema::new(fields))
}

impl ArrowBuilder {
    /// Helper to validate that a RelationDesc can be encoded into Arrow.
    pub fn validate_desc(desc: &RelationDesc) -> Result<(), anyhow::Error> {
        let mut errs = vec![];
        for (col_name, col_type) in desc.iter() {
            match scalar_to_arrow_datatype(&col_type.scalar_type) {
                Ok(_) => {}
                Err(_) => errs.push(format!("{}: {:?}", col_name, col_type.scalar_type)),
            }
        }
        if !errs.is_empty() {
            anyhow::bail!("Cannot encode the following columns/types: {:?}", errs);
        }
        Ok(())
    }

    /// Initializes a new ArrowBuilder with the schema of the provided RelationDesc.
    /// `item_capacity` is used to initialize the capacity of each column's builder which defines
    /// the number of values that can be appended to each column before reallocating.
    /// `data_capacity` is used to initialize the buffer size of the string and binary builders.
    /// Errors if the relation contains an unimplemented type.
    pub fn new(
        desc: &RelationDesc,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Result<Self, anyhow::Error> {
        let schema = desc_to_schema(desc)?;
        let mut columns = vec![];
        for field in schema.fields() {
            columns.push(ArrowColumn::new(
                field.name().clone(),
                field.is_nullable(),
                field.data_type().clone(),
                typename_from_field(field)?,
                item_capacity,
                data_capacity,
            )?);
        }
        Ok(Self {
            columns,
            row_size_bytes: 0,
            original_schema: None,
        })
    }

    /// Initializes a new ArrowBuilder with a pre-built Arrow Schema.
    /// This is useful when you need to preserve schema metadata (e.g., field IDs for Iceberg).
    /// `item_capacity` is used to initialize the capacity of each column's builder which defines
    /// the number of values that can be appended to each column before reallocating.
    /// `data_capacity` is used to initialize the buffer size of the string and binary builders.
    /// Errors if the schema contains an unimplemented type.
    pub fn new_with_schema(
        schema: Arc<Schema>,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Result<Self, anyhow::Error> {
        let mut columns = vec![];
        for field in schema.fields() {
            columns.push(ArrowColumn::new(
                field.name().clone(),
                field.is_nullable(),
                field.data_type().clone(),
                typename_from_field(field)?,
                item_capacity,
                data_capacity,
            )?);
        }
        Ok(Self {
            columns,
            row_size_bytes: 0,
            original_schema: Some(schema),
        })
    }

    /// Returns a copy of the schema of the ArrowBuilder.
    pub fn schema(&self) -> Schema {
        Schema::new(
            self.columns
                .iter()
                .map(Into::<Field>::into)
                .collect::<Vec<_>>(),
        )
    }

    /// Converts the ArrowBuilder into an arrow RecordBatch.
    pub fn to_record_batch(self) -> Result<RecordBatch, ArrowError> {
        let mut arrays = vec![];
        let mut fields: Vec<Field> = vec![];
        for mut col in self.columns.into_iter() {
            arrays.push(col.finish());
            fields.push((&col).into());
        }

        // If we have an original schema, use it to preserve metadata (e.g., field IDs)
        let schema = if let Some(original_schema) = self.original_schema {
            original_schema
        } else {
            Arc::new(Schema::new(fields))
        };

        RecordBatch::try_new(schema, arrays)
    }

    /// Appends a row to the builder.
    /// Errors if the row contains an unimplemented or out-of-range value.
    pub fn add_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        for (col, datum) in self.columns.iter_mut().zip_eq(row.iter()) {
            col.append_datum(datum)?;
        }
        self.row_size_bytes += row.byte_len();
        Ok(())
    }

    pub fn row_size_bytes(&self) -> usize {
        self.row_size_bytes
    }
}

/// Return the appropriate Arrow DataType for the given SqlScalarType, plus a string
/// that should be used as part of the Arrow 'Extension Type' name for fields using
/// this type: <https://arrow.apache.org/docs/format/Columnar.html#extension-types>
fn scalar_to_arrow_datatype(
    scalar_type: &SqlScalarType,
) -> Result<(DataType, String), anyhow::Error> {
    let (data_type, extension_name) = match scalar_type {
        SqlScalarType::Bool => (DataType::Boolean, "boolean"),
        SqlScalarType::Int16 => (DataType::Int16, "smallint"),
        SqlScalarType::Int32 => (DataType::Int32, "integer"),
        SqlScalarType::Int64 => (DataType::Int64, "bigint"),
        SqlScalarType::UInt16 => (DataType::UInt16, "uint2"),
        SqlScalarType::UInt32 => (DataType::UInt32, "uint4"),
        SqlScalarType::UInt64 => (DataType::UInt64, "uint8"),
        SqlScalarType::Float32 => (DataType::Float32, "real"),
        SqlScalarType::Float64 => (DataType::Float64, "double"),
        SqlScalarType::Date => (DataType::Date32, "date"),
        // The resolution of our time and timestamp types is microseconds, which is lucky
        // since the original parquet 'ConvertedType's support microsecond resolution but not
        // nanosecond resolution. The newer parquet 'LogicalType's support nanosecond resolution,
        // but many readers don't support them yet.
        SqlScalarType::Time => (
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            "time",
        ),
        SqlScalarType::Timestamp { .. } => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "timestamp",
        ),
        SqlScalarType::TimestampTz { .. } => (
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                // When appending values we always use UTC timestamps, and setting this to a non-empty
                // value allows readers to know that tz-aware timestamps can be compared directly.
                Some("+00:00".into()),
            ),
            "timestamptz",
        ),
        SqlScalarType::Bytes => (DataType::LargeBinary, "bytea"),
        SqlScalarType::Char { length } => {
            if length.map_or(false, |l| l.into_u32() < i32::MAX.unsigned_abs()) {
                (DataType::Utf8, "text")
            } else {
                (DataType::LargeUtf8, "text")
            }
        }
        SqlScalarType::VarChar { max_length } => {
            if max_length.map_or(false, |l| l.into_u32() < i32::MAX.unsigned_abs()) {
                (DataType::Utf8, "text")
            } else {
                (DataType::LargeUtf8, "text")
            }
        }
        SqlScalarType::String => (DataType::LargeUtf8, "text"),
        // Parquet does have a UUID 'Logical Type' in parquet format 2.4+, but there is no arrow
        // UUID type, so we match the format (a 16-byte fixed-length binary array) ourselves.
        SqlScalarType::Uuid => (DataType::FixedSizeBinary(16), "uuid"),
        // Parquet does have a JSON 'Logical Type' in parquet format 2.4+, but there is no arrow
        // JSON type, so for now we represent JSON as 'large' utf8-encoded strings.
        SqlScalarType::Jsonb => (DataType::LargeUtf8, "jsonb"),
        SqlScalarType::MzTimestamp => (DataType::UInt64, "mz_timestamp"),
        SqlScalarType::Numeric { max_scale } => {
            // Materialize allows 39 digits of precision for numeric values, but allows
            // arbitrary scales among those values. e.g. 1e38 and 1e-39 are both valid in
            // the same column. However, Arrow/Parquet only allows static declaration of both
            // the precision and the scale. To represent the full range of values of a numeric
            // column, we would need 78-digits to store all possible values. Arrow's Decimal256
            // type can only support 76 digits, so we are be unable to represent the entire range.

            // Instead of representing the full possible range, we instead try to represent most
            // values in the most-compatible way. We use a Decimal128 type which can handle 38
            // digits of precision and has more compatibility with other parquet readers than
            // Decimal256. We use Arrow's default scale of 10 if max-scale is not set. We will
            // error if we encounter a value that is too large to represent, and if that happens
            // a user can choose to cast the column to a string to represent the value.
            match max_scale {
                Some(scale) => {
                    let scale = i8::try_from(scale.into_u8()).expect("known <= 39");
                    if scale <= DECIMAL128_MAX_SCALE {
                        (
                            DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale),
                            "numeric",
                        )
                    } else {
                        anyhow::bail!("Numeric max scale {} out of range", scale)
                    }
                }
                None => (
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
                    "numeric",
                ),
            }
        }
        // arrow::datatypes::IntervalUnit::MonthDayNano is not yet implemented in the arrow parquet writer
        // https://github.com/apache/arrow-rs/blob/0d031cc8aa81296cb1bdfedea7a7cb4ec6aa54ea/parquet/src/arrow/arrow_writer/mod.rs#L859
        // SqlScalarType::Interval => DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
        SqlScalarType::Array(inner) => {
            // Postgres / MZ Arrays are weird, since they can be multi-dimensional but this is not
            // enforced in the type system, so can change per-value.
            // We use a struct type with two fields - one containing the array elements as a list
            // and the other containing the number of dimensions the array represents. Since arrays
            // are not allowed to be ragged, the number of elements in each dimension is the same.
            let (inner_type, inner_name) = scalar_to_arrow_datatype(inner)?;
            // TODO: Document these field names in our copy-to-s3 docs
            let inner_field = field_with_typename("item", inner_type, true, &inner_name);
            let list_field = Arc::new(field_with_typename(
                "items",
                DataType::List(inner_field.into()),
                false,
                "array_items",
            ));
            let dims_field = Arc::new(field_with_typename(
                "dimensions",
                DataType::UInt8,
                false,
                "array_dimensions",
            ));
            (DataType::Struct([list_field, dims_field].into()), "array")
        }
        SqlScalarType::List {
            element_type,
            custom_id: _,
        } => {
            let (inner_type, inner_name) = scalar_to_arrow_datatype(element_type)?;
            // TODO: Document these field names in our copy-to-s3 docs
            let field = field_with_typename("item", inner_type, true, &inner_name);
            (DataType::List(field.into()), "list")
        }
        SqlScalarType::Map {
            value_type,
            custom_id: _,
        } => {
            let (value_type, value_name) = scalar_to_arrow_datatype(value_type)?;
            // Arrow maps are represented as an 'entries' struct with 'keys' and 'values' fields.
            let field_names = MapFieldNames::default();
            let struct_type = DataType::Struct(
                vec![
                    Field::new(&field_names.key, DataType::Utf8, false),
                    field_with_typename(&field_names.value, value_type, true, &value_name),
                ]
                .into(),
            );
            (
                DataType::Map(
                    Field::new(&field_names.entry, struct_type, false).into(),
                    false,
                ),
                "map",
            )
        }
        _ => anyhow::bail!("{:?} unimplemented", scalar_type),
    };
    Ok((data_type, extension_name.to_lowercase()))
}

fn builder_for_datatype(
    data_type: &DataType,
    item_capacity: usize,
    data_capacity: usize,
) -> Result<ColBuilder, anyhow::Error> {
    let builder = match &data_type {
        DataType::Boolean => {
            ColBuilder::BooleanBuilder(BooleanBuilder::with_capacity(item_capacity))
        }
        DataType::Int16 => ColBuilder::Int16Builder(Int16Builder::with_capacity(item_capacity)),
        DataType::Int32 => ColBuilder::Int32Builder(Int32Builder::with_capacity(item_capacity)),
        DataType::Int64 => ColBuilder::Int64Builder(Int64Builder::with_capacity(item_capacity)),
        DataType::UInt8 => ColBuilder::UInt8Builder(UInt8Builder::with_capacity(item_capacity)),
        DataType::UInt16 => ColBuilder::UInt16Builder(UInt16Builder::with_capacity(item_capacity)),
        DataType::UInt32 => ColBuilder::UInt32Builder(UInt32Builder::with_capacity(item_capacity)),
        DataType::UInt64 => ColBuilder::UInt64Builder(UInt64Builder::with_capacity(item_capacity)),
        DataType::Float32 => {
            ColBuilder::Float32Builder(Float32Builder::with_capacity(item_capacity))
        }
        DataType::Float64 => {
            ColBuilder::Float64Builder(Float64Builder::with_capacity(item_capacity))
        }
        DataType::Date32 => ColBuilder::Date32Builder(Date32Builder::with_capacity(item_capacity)),
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            ColBuilder::Time64MicrosecondBuilder(Time64MicrosecondBuilder::with_capacity(
                item_capacity,
            ))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, timezone) => {
            ColBuilder::TimestampMicrosecondBuilder(
                TimestampMicrosecondBuilder::with_capacity(item_capacity)
                    .with_timezone_opt(timezone.clone()),
            )
        }
        DataType::LargeBinary => ColBuilder::LargeBinaryBuilder(LargeBinaryBuilder::with_capacity(
            item_capacity,
            data_capacity,
        )),
        DataType::FixedSizeBinary(byte_width) => ColBuilder::FixedSizeBinaryBuilder(
            FixedSizeBinaryBuilder::with_capacity(item_capacity, *byte_width),
        ),
        DataType::Utf8 => {
            ColBuilder::StringBuilder(StringBuilder::with_capacity(item_capacity, data_capacity))
        }
        DataType::LargeUtf8 => ColBuilder::LargeStringBuilder(LargeStringBuilder::with_capacity(
            item_capacity,
            data_capacity,
        )),
        DataType::Decimal128(precision, scale) => ColBuilder::Decimal128Builder(
            Decimal128Builder::with_capacity(item_capacity)
                .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::List(field) => {
            let inner_col_builder = ArrowColumn::new(
                field.name().clone(),
                field.is_nullable(),
                field.data_type().clone(),
                typename_from_field(field)?,
                item_capacity,
                data_capacity,
            )?;
            ColBuilder::ListBuilder(Box::new(
                ListBuilder::new(inner_col_builder).with_field(Arc::clone(field)),
            ))
        }
        DataType::Struct(fields) => {
            let mut field_builders: Vec<Box<dyn ArrayBuilder>> = vec![];
            for field in fields {
                let inner_col_builder = ArrowColumn::new(
                    field.name().clone(),
                    field.is_nullable(),
                    field.data_type().clone(),
                    typename_from_field(field)?,
                    item_capacity,
                    data_capacity,
                )?;
                field_builders.push(Box::new(inner_col_builder));
            }
            ColBuilder::StructBuilder(StructBuilder::new(fields.clone(), field_builders))
        }
        DataType::Map(entries_field, _sorted) => {
            let entries_field = entries_field.as_ref();
            if let DataType::Struct(fields) = entries_field.data_type() {
                if fields.len() != 2 {
                    anyhow::bail!(
                        "Expected map entries to have 2 fields, found {}",
                        fields.len()
                    )
                }
                let key_builder = StringBuilder::with_capacity(item_capacity, data_capacity);
                let value_field = &fields[1];
                let value_builder = ArrowColumn::new(
                    value_field.name().clone(),
                    value_field.is_nullable(),
                    value_field.data_type().clone(),
                    typename_from_field(value_field)?,
                    item_capacity,
                    data_capacity,
                )?;
                ColBuilder::MapBuilder(Box::new(
                    MapBuilder::with_capacity(
                        Some(MapFieldNames::default()),
                        key_builder,
                        value_builder,
                        item_capacity,
                    )
                    .with_values_field(Arc::clone(value_field)),
                ))
            } else {
                anyhow::bail!("Expected map entries to be a struct")
            }
        }
        _ => anyhow::bail!("{:?} unimplemented", data_type),
    };
    Ok(builder)
}

#[derive(Debug)]
struct ArrowColumn {
    field_name: String,
    nullable: bool,
    data_type: DataType,
    extension_type_name: String,
    inner: ColBuilder,
}

impl From<&ArrowColumn> for Field {
    fn from(col: &ArrowColumn) -> Self {
        field_with_typename(
            &col.field_name,
            col.data_type.clone(),
            col.nullable,
            &col.extension_type_name,
        )
    }
}

/// Create a Field and include the materialize 'type name' as an extension in the metadata.
fn field_with_typename(
    name: &str,
    data_type: DataType,
    nullable: bool,
    extension_type_name: &str,
) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_string(),
        format!("{}{}", EXTENSION_PREFIX, extension_type_name),
    )]))
}

/// Extract the materialize 'type name' from the metadata of a Field.
fn typename_from_field(field: &Field) -> Result<String, anyhow::Error> {
    let metadata = field.metadata();
    let extension_name = metadata
        .get("ARROW:extension:name")
        .ok_or_else(|| anyhow::anyhow!("Missing extension name in metadata"))?;
    if let Some(name) = extension_name.strip_prefix(EXTENSION_PREFIX) {
        Ok(name.to_string())
    } else {
        anyhow::bail!("Extension name {} does not match expected", extension_name,)
    }
}

impl ArrowColumn {
    fn new(
        field_name: String,
        nullable: bool,
        data_type: DataType,
        extension_type_name: String,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: builder_for_datatype(&data_type, item_capacity, data_capacity)?,
            field_name,
            nullable,
            data_type,
            extension_type_name,
        })
    }
}

macro_rules! make_col_builder {
    ($($x:ident), *) => {
        /// An enum wrapper for all arrow builder types that we support. Used to store
        /// a builder for each column and avoid dynamic dispatch and downcasting
        /// when appending data.
        #[derive(Debug)]
        enum ColBuilder {
            $(
                $x($x),
            )*
            /// ListBuilder & MapBuilder are handled separately than other builder types since they
            /// uses generic parameters for the inner types, and are boxed to avoid recursive
            /// type definitions.
            ListBuilder(Box<ListBuilder<ArrowColumn>>),
            MapBuilder(Box<MapBuilder<StringBuilder, ArrowColumn>>),
            /// StructBuilder is handled separately since its `append_null()` method must be
            /// overriden to both append nulls to all field builders and to append a null to
            /// the struct. It's unclear why `arrow-rs` implemented this differently than
            /// ListBuilder and MapBuilder.
            StructBuilder(StructBuilder),
        }

        impl ColBuilder {
            fn append_null(&mut self) {
                match self {
                    $(
                        ColBuilder::$x(builder) => builder.append_null(),
                    )*
                    ColBuilder::ListBuilder(builder) => builder.append_null(),
                    ColBuilder::MapBuilder(builder) => builder.append(false).unwrap(),
                    ColBuilder::StructBuilder(builder) => {
                        for i in 0..builder.num_fields() {
                            let field_builder: &mut ArrowColumn = builder.field_builder(i).unwrap();
                            field_builder.inner.append_null();
                        }
                        builder.append_null();
                    }
                }
            }
        }

        /// Implement the ArrayBuilder trait for ArrowColumn so that we can use an ArrowColumn as
        /// an inner-builder type in an [`arrow::array::builder::GenericListBuilder`]
        /// and an [`arrow::array::builder::StructBuilder`] and re-use our methods for appending
        /// data to the column.
        impl ArrayBuilder for ArrowColumn {
            fn len(&self) -> usize {
                match &self.inner {
                    $(
                        ColBuilder::$x(builder) => builder.len(),
                    )*
                    ColBuilder::ListBuilder(builder) => builder.len(),
                    ColBuilder::MapBuilder(builder) => builder.len(),
                    ColBuilder::StructBuilder(builder) => builder.len(),
                }
            }
            fn finish(&mut self) -> ArrayRef {
                match &mut self.inner {
                    $(
                        ColBuilder::$x(builder) => Arc::new(builder.finish()),
                    )*
                    ColBuilder::ListBuilder(builder) => Arc::new(builder.finish()),
                    ColBuilder::MapBuilder(builder) => Arc::new(builder.finish()),
                    ColBuilder::StructBuilder(builder) => Arc::new(builder.finish()),
                }
            }
            fn finish_cloned(&self) -> ArrayRef {
                match &self.inner {
                    $(
                        ColBuilder::$x(builder) => Arc::new(builder.finish_cloned()),
                    )*
                    ColBuilder::ListBuilder(builder) => Arc::new(builder.finish_cloned()),
                    ColBuilder::MapBuilder(builder) => Arc::new(builder.finish_cloned()),
                    ColBuilder::StructBuilder(builder) => Arc::new(builder.finish_cloned()),
                }
            }
            fn as_any(&self) -> &(dyn Any + 'static) {
                self
            }
            fn as_any_mut(&mut self) -> &mut (dyn Any + 'static) {
                self
            }
            fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
                self
            }
        }
    };
}

make_col_builder!(
    BooleanBuilder,
    Int16Builder,
    Int32Builder,
    Int64Builder,
    UInt8Builder,
    UInt16Builder,
    UInt32Builder,
    UInt64Builder,
    Float32Builder,
    Float64Builder,
    Date32Builder,
    Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
    LargeBinaryBuilder,
    FixedSizeBinaryBuilder,
    StringBuilder,
    LargeStringBuilder,
    Decimal128Builder
);

impl ArrowColumn {
    fn append_datum(&mut self, datum: Datum) -> Result<(), anyhow::Error> {
        match (&mut self.inner, datum) {
            (s, Datum::Null) => s.append_null(),
            (ColBuilder::BooleanBuilder(builder), Datum::False) => builder.append_value(false),
            (ColBuilder::BooleanBuilder(builder), Datum::True) => builder.append_value(true),
            (ColBuilder::Int16Builder(builder), Datum::Int16(i)) => builder.append_value(i),
            (ColBuilder::Int32Builder(builder), Datum::Int32(i)) => builder.append_value(i),
            (ColBuilder::Int64Builder(builder), Datum::Int64(i)) => builder.append_value(i),
            (ColBuilder::UInt8Builder(builder), Datum::UInt8(i)) => builder.append_value(i),
            (ColBuilder::UInt16Builder(builder), Datum::UInt16(i)) => builder.append_value(i),
            (ColBuilder::UInt32Builder(builder), Datum::UInt32(i)) => builder.append_value(i),
            (ColBuilder::UInt64Builder(builder), Datum::UInt64(i)) => builder.append_value(i),
            (ColBuilder::Float32Builder(builder), Datum::Float32(f)) => builder.append_value(*f),
            (ColBuilder::Float64Builder(builder), Datum::Float64(f)) => builder.append_value(*f),
            (ColBuilder::Date32Builder(builder), Datum::Date(d)) => {
                builder.append_value(d.unix_epoch_days())
            }
            (ColBuilder::Time64MicrosecondBuilder(builder), Datum::Time(t)) => {
                let micros_since_midnight = i64::cast_from(t.num_seconds_from_midnight())
                    * 1_000_000
                    + i64::cast_from(t.nanosecond().checked_div(1000).unwrap());
                builder.append_value(micros_since_midnight)
            }
            (ColBuilder::TimestampMicrosecondBuilder(builder), Datum::Timestamp(ts)) => {
                builder.append_value(ts.and_utc().timestamp_micros())
            }
            (ColBuilder::TimestampMicrosecondBuilder(builder), Datum::TimestampTz(ts)) => {
                builder.append_value(ts.timestamp_micros())
            }
            (ColBuilder::LargeBinaryBuilder(builder), Datum::Bytes(b)) => builder.append_value(b),
            (ColBuilder::FixedSizeBinaryBuilder(builder), Datum::Uuid(val)) => {
                builder.append_value(val.as_bytes())?
            }
            (ColBuilder::StringBuilder(builder), Datum::String(s)) => builder.append_value(s),
            (ColBuilder::LargeStringBuilder(builder), _) if self.extension_type_name == "jsonb" => {
                builder.append_value(JsonbRef::from_datum(datum).to_serde_json().to_string())
            }
            (ColBuilder::LargeStringBuilder(builder), Datum::String(s)) => builder.append_value(s),
            (ColBuilder::UInt64Builder(builder), Datum::MzTimestamp(ts)) => {
                builder.append_value(ts.into())
            }
            (ColBuilder::Decimal128Builder(builder), Datum::Numeric(mut dec)) => {
                if dec.0.is_special() {
                    anyhow::bail!("Cannot represent special numeric value {} in parquet", dec)
                }
                if let DataType::Decimal128(precision, scale) = self.data_type {
                    if dec.0.digits() > precision.into() {
                        anyhow::bail!(
                            "Decimal value {} out of range for column with precision {}",
                            dec,
                            precision
                        )
                    }

                    // Get the signed-coefficient represented as an i128, and the exponent such that
                    // the number should equal coefficient*10^exponent.
                    let coefficient: i128 = dec.0.coefficient()?;
                    let exponent = dec.0.exponent();

                    // Convert the value to use the scale of the column (add 0's to align the decimal
                    // point correctly). This is done by multiplying the coefficient by
                    // 10^(scale + exponent).
                    let scale_diff = i32::from(scale) + exponent;
                    // If the scale_diff is negative, we know there aren't enough digits in our
                    // column's scale to represent this value.
                    let scale_diff = u32::try_from(scale_diff).map_err(|_| {
                        anyhow::anyhow!(
                            "cannot represent decimal value {} in column with scale {}",
                            dec,
                            scale
                        )
                    })?;

                    let value = coefficient
                        .checked_mul(10_i128.pow(scale_diff))
                        .ok_or_else(|| {
                            anyhow::anyhow!("Decimal value {} out of range for parquet", dec)
                        })?;

                    builder.append_value(value)
                } else {
                    anyhow::bail!("Expected Decimal128 data type")
                }
            }
            (ColBuilder::StructBuilder(struct_builder), Datum::Array(arr)) => {
                // We've received an array datum which we know is represented as an Arrow struct
                // with two fields: the list of elements and the number of dimensions
                let list_builder: &mut ArrowColumn = struct_builder.field_builder(0).unwrap();
                if let ColBuilder::ListBuilder(list_builder) = &mut list_builder.inner {
                    let inner_builder = list_builder.values();
                    for datum in arr.elements().into_iter() {
                        inner_builder.append_datum(datum)?;
                    }
                    list_builder.append(true);
                } else {
                    anyhow::bail!(
                        "Expected ListBuilder for StructBuilder with Array datum: {:?}",
                        struct_builder
                    )
                }
                let dims_builder: &mut ArrowColumn = struct_builder.field_builder(1).unwrap();
                if let ColBuilder::UInt8Builder(dims_builder) = &mut dims_builder.inner {
                    dims_builder.append_value(arr.dims().ndims());
                } else {
                    anyhow::bail!(
                        "Expected UInt8Builder for StructBuilder with Array datum: {:?}",
                        struct_builder
                    )
                }
                struct_builder.append(true)
            }
            (ColBuilder::ListBuilder(list_builder), Datum::List(list)) => {
                let inner_builder = list_builder.values();
                for datum in list.into_iter() {
                    inner_builder.append_datum(datum)?;
                }
                list_builder.append(true)
            }
            (ColBuilder::MapBuilder(builder), Datum::Map(map)) => {
                for (key, value) in map.iter() {
                    builder.keys().append_value(key);
                    builder.values().append_datum(value)?;
                }
                builder.append(true).unwrap()
            }
            (builder, datum) => {
                anyhow::bail!("Datum {:?} does not match builder {:?}", datum, builder)
            }
        }
        Ok(())
    }
}
