// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt, io,
    sync::Arc,
};

use bitvec::{prelude::BitVec, slice::BitSlice};

use crate::{
    constants::{ColumnFlags, ColumnType},
    io::ParseBuf,
    misc::raw::int::*,
    packets::Column,
    proto::MyDeserialize,
    row::{new_row_raw, Row},
    value::Value,
};

use super::{
    events::{OptionalMetaExtractor, TableMapEvent},
    value::{BinlogValue, BinlogValueToValueError},
};

/// Binlog rows event row value options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[repr(u64)]
pub enum BinlogRowValueOptions {
    /// Store JSON updates in partial form
    PARTIAL_JSON_UPDATES = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown binlog version {}", _0)]
#[repr(transparent)]
pub struct UnknownBinlogRowValueOptions(pub u64);

impl From<UnknownBinlogRowValueOptions> for u64 {
    fn from(x: UnknownBinlogRowValueOptions) -> Self {
        x.0
    }
}

impl TryFrom<u64> for BinlogRowValueOptions {
    type Error = UnknownBinlogRowValueOptions;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PARTIAL_JSON_UPDATES),
            x => Err(UnknownBinlogRowValueOptions(x)),
        }
    }
}

/// Representation of a binlog row.
#[derive(Clone, PartialEq)]
pub struct BinlogRow {
    values: Vec<Option<BinlogValue<'static>>>,
    columns: Arc<[Column]>,
}

impl BinlogRow {
    pub fn new(values: Vec<Option<BinlogValue<'static>>>, columns: Arc<[Column]>) -> Self {
        Self { values, columns }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns columns of this row.
    pub fn columns_ref(&self) -> &[Column] {
        &self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Arc<[Column]> {
        self.columns.clone()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&BinlogValue> {
        self.values.get(index).and_then(|x| x.as_ref())
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take(&mut self, index: usize) -> Option<BinlogValue> {
        self.values.get_mut(index).and_then(|x| x.take())
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<BinlogValue<'static>> {
        self.values
            .into_iter()
            .map(|x| x.expect("Can't unwrap row if some of columns was taken"))
            .collect()
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: BinlogValue<'static>) {
        self.values[index] = Some(value);
    }
}

impl<'de> MyDeserialize<'de> for BinlogRow {
    const SIZE: Option<usize> = None;
    /// Content:
    ///
    /// * number of columns
    /// * column bitmap - bit is set if column is in the row
    /// * have shared image - `true` means, that this is a partial event
    ///   and this is an after image row. Therefore we need to parse a shared image
    /// * corresponding table map event
    type Ctx = (u64, &'de BitSlice<u8>, bool, &'de TableMapEvent<'de>);

    fn deserialize(
        (num_columns, cols, have_shared_image, table_info): Self::Ctx,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<Self> {
        let mut values: Vec<Option<BinlogValue<'static>>> = vec![];
        let mut columns = vec![];

        // read a shared image if needed (see WL#2955)
        let mut partial_cols = if have_shared_image {
            let value_options = *buf.parse::<RawInt<LenEnc>>(())?;
            if value_options & BinlogRowValueOptions::PARTIAL_JSON_UPDATES as u64 > 0 {
                let json_columns_count = table_info.json_column_count();
                let partial_columns_len = (json_columns_count + 7) / 8;
                let partial_columns: &[u8] = buf.parse(partial_columns_len)?;
                let partial_columns = BitSlice::<u8>::from_slice(partial_columns);
                Some(partial_columns.into_iter().take(json_columns_count))
            } else {
                None
            }
        } else {
            None
        };

        let num_bits = cols.count_ones();
        let bitmap_len = (num_bits + 7) / 8;
        let bitmap_buf: &[u8] = buf.parse(bitmap_len)?;
        let mut null_bitmap = BitVec::<u8>::from_slice(bitmap_buf);
        null_bitmap.truncate(num_bits);

        let mut image_idx = 0;

        let opt_meta_extractor = OptionalMetaExtractor::new(table_info.iter_optional_meta())?;

        let mut signedness_iterator = opt_meta_extractor.iter_signedness();
        let mut charset_iter = opt_meta_extractor.iter_charset();
        let mut enum_and_set_charset_iter = opt_meta_extractor.iter_enum_and_set_charset();
        let mut primary_key_iter = opt_meta_extractor.iter_primary_key();
        let mut column_name_iter = opt_meta_extractor.iter_column_name();

        for i in 0..(num_columns as usize) {
            // check if column is in columns list
            if cols.get(i).as_deref().copied().unwrap_or(false) {
                let column_type = table_info.get_column_type(i);

                // TableMapEvent must define column type for the current column.
                let column_type = match column_type {
                    Ok(Some(ty)) => ty,
                    Ok(None) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "No column type"))
                    }
                    Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                };

                let column_meta = table_info.get_column_metadata(i).unwrap_or(&[]);

                let is_partial = column_type == ColumnType::MYSQL_TYPE_JSON
                    && partial_cols
                        .as_mut()
                        .and_then(|bits| bits.next().as_deref().copied())
                        .unwrap_or(false);

                let is_unsigned = column_type
                    .is_numeric_type()
                    .then(|| signedness_iterator.next())
                    .flatten()
                    .unwrap_or_default();

                let charset = if column_type.is_character_type() {
                    charset_iter.next().transpose()?.unwrap_or_default()
                } else if column_type.is_enum_or_set_type() {
                    enum_and_set_charset_iter
                        .next()
                        .transpose()?
                        .unwrap_or_default()
                } else {
                    Default::default()
                };

                let column_name_raw = column_name_iter.next().transpose()?;
                let column_name = column_name_raw
                    .as_ref()
                    .map(|x| Cow::Borrowed(x.name_raw()))
                    .unwrap_or_else(|| {
                        // default column name is `@<i>` where i is a column offset in a table
                        Cow::Owned(format!("@{}", i).into())
                    });

                let mut column_flags = ColumnFlags::empty();

                if is_unsigned {
                    column_flags |= ColumnFlags::UNSIGNED_FLAG;
                }

                if primary_key_iter
                    .next_if(|next| next.is_err() || next.as_ref().ok() == Some(&(i as u64)))
                    .transpose()?
                    .is_some()
                {
                    column_flags |= ColumnFlags::PRI_KEY_FLAG;
                }

                let column = Column::new(column_type)
                    .with_schema(table_info.database_name_raw())
                    .with_table(table_info.table_name_raw())
                    .with_name(column_name.as_ref())
                    .with_flags(column_flags)
                    .with_schema(table_info.database_name_raw())
                    .with_org_table(table_info.table_name_raw())
                    .with_table(table_info.table_name_raw())
                    .with_character_set(charset);

                columns.push(column);

                // check if column is null
                if null_bitmap
                    .get(image_idx)
                    .as_deref()
                    .copied()
                    .unwrap_or(true)
                {
                    values.push(Some(BinlogValue::Value(Value::NULL)));
                } else {
                    let ctx = (column_type, column_meta, is_unsigned, is_partial);
                    values.push(Some(buf.parse::<BinlogValue>(ctx)?.into_owned()));
                }

                image_idx += 1;
            }
        }

        Ok(BinlogRow::new(values, columns.into_boxed_slice().into()))
    }
}

impl fmt::Debug for BinlogRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BinlogRow");
        for (val, column) in self.values.iter().zip(self.columns.iter()) {
            match *val {
                Some(ref val) => {
                    debug.field(column.name_str().as_ref(), val);
                }
                None => {
                    debug.field(column.name_str().as_ref(), &"<taken>");
                }
            }
        }
        debug.finish()
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Can't convert BinlogRow to Row at column offset {}: {}",
    column_offset,
    error
)]
pub struct BinlogRowToRowError {
    /// Column offset.
    pub column_offset: usize,
    /// Value conversion error.
    pub error: BinlogValueToValueError,
}

impl TryFrom<BinlogRow> for Row {
    type Error = BinlogRowToRowError;

    fn try_from(binlog_row: BinlogRow) -> Result<Self, Self::Error> {
        let mut values = Vec::with_capacity(binlog_row.values.len());
        for (column_offset, value) in binlog_row.values.into_iter().enumerate() {
            match value {
                Some(x) => {
                    values.push(Some(x.try_into().map_err(|error| BinlogRowToRowError {
                        column_offset,
                        error,
                    })?))
                }
                None => values.push(None),
            }
        }
        Ok(new_row_raw(values, binlog_row.columns))
    }
}
