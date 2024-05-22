// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{borrow::Cow, cmp::min, convert::TryFrom, io, iter::Peekable};

use bitvec::prelude::*;
use byteorder::ReadBytesExt;
use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType, OptionalMetadataFieldType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    constants::{ColumnType, GeometryType, UnknownColumnType},
    io::ParseBuf,
    misc::raw::{
        bytes::{BareBytes, EofBytes, LenEnc, U8Bytes},
        int::*,
        Either, RawBytes, RawConst, RawSeq, Skip,
    },
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum BadColumnType {
    #[error(transparent)]
    Unknown(#[from] UnknownColumnType),
    #[error("Unexpected column type: {}", _0)]
    Unexpected(u8),
}

/// Table map event.
///
/// In row-based mode, every row operation event is preceded by a Table_map_event which maps
/// a table definition to a number.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableMapEvent<'a> {
    // post-header
    /// The number that identifies the table.
    ///
    /// It's 6 bytes long, so valid range is [0, 1<<48).
    table_id: RawInt<LeU48>,
    /// Reserved for future use; currently always 0.
    flags: RawInt<LeU16>,

    // payload
    /// The name of the database in which the table resides.
    ///
    /// Length must be <= 64 bytes.
    database_name: RawBytes<'a, U8Bytes>,
    /// The database name is null-terminated even though it is preceded by the length.
    __null_1: Skip<1>,
    /// The name of the table.
    ///
    /// Length must be <= 64 bytes.
    table_name: RawBytes<'a, U8Bytes>,
    /// The table name is null-terminated even though it is preceded by the length.
    __null_2: Skip<1>,
    /// Number of columns in the table.
    columns_count: RawInt<LenEnc>,
    /// The type of each column in the table, listed from left to right.
    columns_type: RawSeq<'a, u8, ColumnType>,
    /// For each column from left to right, a chunk of data who's length and semantics depends
    /// on the type of the column.
    columns_metadata: RawBytes<'a, LenEnc>,
    /// For each column, a bit indicating whether data in the column can be NULL or not.
    ///
    /// The number of bytes needed for this is int((column_count + 7) / 8).
    /// The flag for the first column from the left is in the least-significant bit
    /// of the first byte, the second is in the second least significant bit of the first byte,
    /// the ninth is in the least significant bit of the second byte, and so on.
    null_bitmask: RawBytes<'a, BareBytes<0x2000000000000000>>,
    /// Optional metadata.
    optional_metadata: RawBytes<'a, EofBytes>,
}

impl<'a> TableMapEvent<'a> {
    /// Returns the table identifier.
    pub fn table_id(&self) -> u64 {
        self.table_id.0
    }

    /// Returns the number of columns
    pub fn columns_count(&self) -> u64 {
        self.columns_count.0
    }

    /// Ruturns a number of JSON columns.
    pub fn json_column_count(&self) -> usize {
        self.columns_type
            .0
            .iter()
            .filter(|x| **x == ColumnType::MYSQL_TYPE_JSON as u8)
            .count()
    }

    /// Returns null-bitmap for this table.
    ///
    /// For each column this null bitmap contains a bit indicating whether
    /// data in the column can be NULL or not.
    pub fn null_bitmask(&'a self) -> &'a BitSlice<u8> {
        let slice = BitSlice::from_slice(self.null_bitmask.as_bytes());
        &slice[..self.columns_count() as usize]
    }

    /// Returns raw database name value.
    pub fn database_name_raw(&'a self) -> &'a [u8] {
        self.database_name.as_bytes()
    }

    /// Returns database name as a string (lossy converted).
    pub fn database_name(&'a self) -> Cow<'a, str> {
        self.database_name.as_str()
    }

    /// Returns raw table name value.
    pub fn table_name_raw(&'a self) -> &'a [u8] {
        self.table_name.as_bytes()
    }

    /// Returns table name as a string (lossy converted).
    pub fn table_name(&'a self) -> Cow<'a, str> {
        self.table_name.as_str()
    }

    /// Returns raw type of the column as stored in the column_type field of the Table Map Event.
    ///
    /// `None` means that the column index is out of range.
    pub fn get_raw_column_type(
        &self,
        col_idx: usize,
    ) -> Result<Option<ColumnType>, UnknownColumnType> {
        self.columns_type.get(col_idx).map(|x| x.get()).transpose()
    }

    /// Returns a type of the given column.
    ///
    /// It'll read real column type out of the column
    /// metadata if column type is `MYSQL_TYPE_STRING`.
    ///
    /// Returns an error in case of unknown column type
    /// or unexpected real type for `MYSQL_TYPE_STRING`.
    ///
    /// `None` means that the column index is out of range.
    pub fn get_column_type(&self, col_idx: usize) -> Result<Option<ColumnType>, BadColumnType> {
        self.columns_type
            .get(col_idx)
            .map(|x| {
                x.get()
                    .map_err(BadColumnType::from)
                    .and_then(|column_type| self.get_real_type(col_idx, column_type))
            })
            .transpose()
    }

    /// Returns metadata for the given column.
    ///
    /// Returns `None` if column index is out of bounds or if offset couldn't be calculated
    /// (e.g. because of unknown column type between `0` and `col_idx`).
    pub fn get_column_metadata(&self, col_idx: usize) -> Option<&[u8]> {
        let mut offset = 0;
        for i in 0..=col_idx {
            let ty = self.columns_type.get(i)?.get().ok()?;
            let ptr = self.columns_metadata.as_bytes().get(offset..)?;
            let (metadata, len) = ty.get_metadata(ptr, false)?;
            if i == col_idx {
                return Some(metadata);
            } else {
                offset += len;
            }
        }
        None
    }

    pub fn iter_optional_meta(&'a self) -> OptionalMetadataIter<'a> {
        OptionalMetadataIter {
            columns: &self.columns_type,
            data: self.optional_metadata.as_bytes(),
        }
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> TableMapEvent<'static> {
        TableMapEvent {
            table_id: self.table_id,
            flags: self.flags,
            database_name: self.database_name.into_owned(),
            __null_1: self.__null_1,
            table_name: self.table_name.into_owned(),
            __null_2: self.__null_2,
            columns_count: self.columns_count,
            columns_type: self.columns_type.into_owned(),
            columns_metadata: self.columns_metadata.into_owned(),
            null_bitmask: self.null_bitmask.into_owned(),
            optional_metadata: self.optional_metadata.into_owned(),
        }
    }

    fn get_real_type(
        &self,
        col_idx: usize,
        column_type: ColumnType,
    ) -> Result<ColumnType, BadColumnType> {
        match column_type {
            ColumnType::MYSQL_TYPE_DATE => {
                // This type has not been used since before row-based replication,
                // so we can safely assume that it really is MYSQL_TYPE_NEWDATE.
                return Ok(ColumnType::MYSQL_TYPE_NEWDATE);
            }
            ColumnType::MYSQL_TYPE_STRING => {
                let mut real_type = column_type as u8;
                if let Some(metadata_bytes) = self.get_column_metadata(col_idx) {
                    let f1 = metadata_bytes[0];

                    if f1 != 0 {
                        real_type = f1 | 0x30;
                    }

                    match real_type {
                        247 => return Ok(ColumnType::MYSQL_TYPE_ENUM),
                        248 => return Ok(ColumnType::MYSQL_TYPE_SET),
                        254 => return Ok(ColumnType::MYSQL_TYPE_STRING),
                        x => {
                            // this event seems to be malformed
                            return Err(BadColumnType::Unexpected(x));
                        }
                    };
                }
            }
            _ => (),
        }

        Ok(column_type)
    }
}

impl<'de> MyDeserialize<'de> for TableMapEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let table_id = if 6 == ctx.fde.get_event_type_header_length(Self::EVENT_TYPE) {
            // old server
            let table_id: RawInt<LeU32> = buf.parse(())?;
            RawInt::new(table_id.0 as u64)
        } else {
            buf.parse(())?
        };

        let flags = buf.parse(())?;

        let database_name = buf.parse(())?;
        let __null_1 = buf.parse(())?;
        let table_name = buf.parse(())?;
        let __null_2 = buf.parse(())?;

        let columns_count: RawInt<LenEnc> = buf.parse(())?;
        let columns_type = buf.parse(columns_count.0 as usize)?;
        let columns_metadata = buf.parse(())?;
        let null_bitmask = buf.parse(((columns_count.0 + 7) / 8) as usize)?;
        let optional_metadata = buf.parse(())?;

        Ok(TableMapEvent {
            table_id,
            flags,
            database_name,
            __null_1,
            table_name,
            __null_2,
            columns_count,
            columns_type,
            columns_metadata,
            null_bitmask,
            optional_metadata,
        })
    }
}

impl MySerialize for TableMapEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.table_id.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.database_name.serialize(&mut *buf);
        self.__null_1.serialize(&mut *buf);
        self.table_name.serialize(&mut *buf);
        self.__null_2.serialize(&mut *buf);
        self.columns_count.serialize(&mut *buf);
        self.columns_type.serialize(&mut *buf);
        self.columns_metadata.serialize(&mut *buf);
        self.null_bitmask.serialize(&mut *buf);
        self.optional_metadata.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for TableMapEvent<'a> {
    const EVENT_TYPE: EventType = EventType::TABLE_MAP_EVENT;
}

impl<'a> BinlogStruct<'a> for TableMapEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(6);
        len += S(2);
        len += S(1);
        len += S(min(self.database_name.0.len(), u8::MAX as usize));
        len += S(1);
        len += S(1);
        len += S(min(self.table_name.0.len(), u8::MAX as usize));
        len += S(1);
        len += S(crate::misc::lenenc_int_len(self.columns_count()) as usize);
        len += S(self.columns_count() as usize);
        len += S(crate::misc::lenenc_str_len(self.columns_metadata.as_bytes()) as usize);
        len += S((self.columns_count() as usize + 8) / 7);
        len += S(self.optional_metadata.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}

/// Optional metadata field that contains charsets for columns.
///
/// - contains charsets for caracter columns if it's a [`OptionalMetadataField::DefaultCharset`];
/// – contains charsets for ENUM and SET columns if it's a
///   [`OptionalMetadataField::EnumAndSetDefaultCharset`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DefaultCharset<'a> {
    /// Default charset+collation id.
    default_charset: RawInt<LenEnc>,
    /// Maps column index to its charset+collation id for columns which charset isn't default.
    non_default: RawBytes<'a, EofBytes>,
}

impl<'a> DefaultCharset<'a> {
    /// Returns the default charset.
    pub fn default_charset(&self) -> u16 {
        self.default_charset.0 as u16
    }

    /// Iterates non-default charsets. Errors if data is malformed.
    ///
    /// It'll either enumerate charsets for character columns
    /// (for [`OptionalMetadataField::DefaultCharset`]) or for ENUM and SET columns
    /// (for [`OptionalMetadataField::EnumAndSetDefaultCharset`]).
    /// See [`ColumnType::is_character_type`] and [`ColumnType::is_enum_or_set_type`].
    ///
    /// The order is same to the order of [`TableMapEvent::get_column_type`] field.
    pub fn iter_non_default(&self) -> IterNonDefault<'_> {
        IterNonDefault {
            buf: ParseBuf(self.non_default.as_bytes()),
        }
    }
}

pub struct IterNonDefault<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterNonDefault<'a> {
    type Item = io::Result<NonDefaultCharset>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse(()) {
                Ok(x) => Some(Ok(x)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for DefaultCharset<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            default_charset: buf.parse(())?,
            non_default: buf.parse(())?,
        })
    }
}

impl MySerialize for DefaultCharset<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.default_charset.serialize(&mut *buf);
        self.non_default.serialize(&mut *buf);
    }
}

/// Contains `column_id -> charset+collation` mapping for columns with non-default charsets.
/// (see [`DefaultCharset::iter_non_default`]).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NonDefaultCharset {
    column_index: RawInt<LenEnc>,
    charset: RawInt<LenEnc>,
}

impl NonDefaultCharset {
    pub fn new(column_index: u64, charset: u16) -> Self {
        Self {
            column_index: RawInt::new(column_index),
            charset: RawInt::new(charset as u64),
        }
    }

    /// Returns the column index.
    pub fn column_index(&self) -> u64 {
        self.column_index.0
    }

    /// Returns the charset+collation.
    pub fn charset(&self) -> u16 {
        self.charset.0 as u16
    }
}

impl<'de> MyDeserialize<'de> for NonDefaultCharset {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            column_index: buf.parse(())?,
            charset: buf.parse(())?,
        })
    }
}

impl MySerialize for NonDefaultCharset {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.column_index.serialize(&mut *buf);
        self.charset.serialize(&mut *buf);
    }
}

/// Contains charset+collation for column.
///
/// - contains charsets for caracter columns if it's a [`OptionalMetadataField::ColumnCharset`];
/// – contains charsets for ENUM and SET columns if it's a
///   [`OptionalMetadataField::EnumAndSetColumnCharset`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnCharsets<'a> {
    charsets: RawBytes<'a, EofBytes>,
}

impl<'a> ColumnCharsets<'a> {
    /// Returns an iterator over charsets.
    ///
    /// It'll either enumerate charsets for character columns
    /// (for [`OptionalMetadataField::DefaultCharset`]) or for ENUM and SET columns
    /// (for [`OptionalMetadataField::EnumAndSetDefaultCharset`]).
    /// See [`ColumnType::is_character_type`] and [`ColumnType::is_enum_or_set_type`].
    ///
    /// The order is same to the order of [`TableMapEvent::get_column_type`] field.
    pub fn iter_charsets(&'a self) -> IterCharsets<'a> {
        IterCharsets {
            buf: ParseBuf(self.charsets.as_bytes()),
        }
    }
}

pub struct IterCharsets<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterCharsets<'a> {
    type Item = io::Result<u16>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse::<RawInt<LenEnc>>(()) {
                Ok(x) => Some(Ok(x.0 as u16)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for ColumnCharsets<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            charsets: buf.parse(())?,
        })
    }
}

impl MySerialize for ColumnCharsets<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.charsets.serialize(buf);
    }
}

/// Name of a column in [`ColumnNames`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnName<'a> {
    name: RawBytes<'a, LenEnc>,
}

impl<'a> ColumnName<'a> {
    /// Creates new column name.
    pub fn new(name: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            name: RawBytes::new(name),
        }
    }

    /// Returns the raw name.
    pub fn name_raw(&'a self) -> &'a [u8] {
        self.name.as_bytes()
    }

    /// Returns the name as a string (lossy converted).
    pub fn name(&'a self) -> Cow<'a, str> {
        self.name.as_str()
    }

    /// Converts self to a 'static version.
    pub fn into_owned(self) -> ColumnName<'static> {
        ColumnName {
            name: self.name.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for ColumnName<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            name: buf.parse(())?,
        })
    }
}

impl MySerialize for ColumnName<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.name.serialize(buf);
    }
}

/// Contains names of columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnNames<'a> {
    names: RawBytes<'a, EofBytes>,
}

impl<'a> ColumnNames<'a> {
    /// Returns an iterator over names of columns.
    pub fn iter_names(&self) -> IterNames<'_> {
        IterNames {
            buf: ParseBuf(self.names.as_bytes()),
        }
    }
}

pub struct IterNames<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterNames<'a> {
    type Item = io::Result<ColumnName<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse(()) {
                Ok(x) => Some(Ok(x)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for ColumnNames<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            names: buf.parse(())?,
        })
    }
}

impl MySerialize for ColumnNames<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.names.serialize(buf);
    }
}

/// Contains string values of SET columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetsStrValues<'a> {
    values: RawBytes<'a, EofBytes>,
}

impl<'a> SetsStrValues<'a> {
    /// Returns an iterator over SET columns string values.
    pub fn iter_values(&'a self) -> IterSetStrValues<'a> {
        IterSetStrValues {
            buf: ParseBuf(self.values.as_bytes()),
        }
    }
}

pub struct IterSetStrValues<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterSetStrValues<'a> {
    type Item = io::Result<SetStrValues<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse(()) {
                Ok(x) => Some(Ok(x)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for SetsStrValues<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            values: buf.parse(())?,
        })
    }
}

impl MySerialize for SetsStrValues<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.values.serialize(buf);
    }
}

/// String value for a SET column variant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetStrValue<'a> {
    value: RawBytes<'a, LenEnc>,
}

impl<'a> SetStrValue<'a> {
    /// Creates a new SET variant name.
    pub fn new(value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            value: RawBytes::new(value),
        }
    }

    /// Returns the raw value.
    pub fn value_raw(&'a self) -> &'a [u8] {
        self.value.as_bytes()
    }

    /// Returns the value as a string (lossy converted).
    pub fn value(&'a self) -> Cow<'a, str> {
        self.value.as_str()
    }
}

impl<'de> MyDeserialize<'de> for SetStrValue<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            value: buf.parse(())?,
        })
    }
}

impl MySerialize for SetStrValue<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.value.serialize(buf);
    }
}

/// Contains string values for SET column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SetStrValues<'a> {
    num_variants: RawInt<LenEnc>,
    values: Vec<SetStrValue<'a>>,
}

impl<'a> SetStrValues<'a> {
    /// Returns the number of variants in this SET column.
    pub fn num_variants(&self) -> u64 {
        self.num_variants.0
    }

    /// Returns an iterator over string values of SET variants.
    pub fn values(&'a self) -> &'a [SetStrValue<'a>] {
        self.values.as_ref()
    }
}

impl<'de> MyDeserialize<'de> for SetStrValues<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let num_variants: RawInt<LenEnc> = buf.parse(())?;
        let mut values = Vec::with_capacity(num_variants.0 as usize);
        for _ in 0..num_variants.0 {
            values.push(buf.parse(())?);
        }
        Ok(Self {
            num_variants,
            values,
        })
    }
}

impl MySerialize for SetStrValues<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.num_variants.serialize(&mut *buf);
        for value in &self.values {
            value.serialize(buf);
        }
    }
}

/// Contains string values of ENUM columns.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnumsStrValues<'a> {
    values: RawBytes<'a, EofBytes>,
}

impl<'a> EnumsStrValues<'a> {
    /// Returns an iterator over ENUM columns string values.
    pub fn iter_values(&'a self) -> IterEnumStrValues<'a> {
        IterEnumStrValues {
            buf: ParseBuf(self.values.as_bytes()),
        }
    }
}

pub struct IterEnumStrValues<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterEnumStrValues<'a> {
    type Item = io::Result<EnumStrValues<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse(()) {
                Ok(x) => Some(Ok(x)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for EnumsStrValues<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            values: buf.parse(())?,
        })
    }
}

impl MySerialize for EnumsStrValues<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.values.serialize(buf);
    }
}

/// String value for an ENUM column variant.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnumStrValue<'a> {
    value: RawBytes<'a, LenEnc>,
}

impl<'a> EnumStrValue<'a> {
    /// Creates a new ENUM variant name.
    pub fn new(value: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            value: RawBytes::new(value),
        }
    }

    /// Returns the raw value.
    pub fn value_raw(&'a self) -> &'a [u8] {
        self.value.as_bytes()
    }

    /// Returns the value as a string (lossy converted).
    pub fn value(&'a self) -> Cow<'a, str> {
        self.value.as_str()
    }
}

impl<'de> MyDeserialize<'de> for EnumStrValue<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            value: buf.parse(())?,
        })
    }
}

impl MySerialize for EnumStrValue<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.value.serialize(buf);
    }
}

/// Contains string values for ENUM column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnumStrValues<'a> {
    num_variants: RawInt<LenEnc>,
    values: Vec<EnumStrValue<'a>>,
}

impl<'a> EnumStrValues<'a> {
    /// Returns the number of variants in this ENUM column.
    pub fn num_variants(&self) -> u64 {
        self.num_variants.0
    }

    /// Returns an iterator over string values of ENUM variants.
    pub fn values(&'a self) -> &'a [EnumStrValue<'a>] {
        self.values.as_ref()
    }
}

impl<'de> MyDeserialize<'de> for EnumStrValues<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let num_variants: RawInt<LenEnc> = buf.parse(())?;
        let mut values = Vec::with_capacity(num_variants.0 as usize);
        for _ in 0..num_variants.0 {
            values.push(buf.parse(())?);
        }
        Ok(Self {
            num_variants,
            values,
        })
    }
}

impl MySerialize for EnumStrValues<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.num_variants.serialize(&mut *buf);
        for value in &self.values {
            value.serialize(buf);
        }
    }
}

/// Contains real types for every geometry column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeometryTypes<'a> {
    geometry_types: RawBytes<'a, EofBytes>,
}

impl<'a> GeometryTypes<'a> {
    /// Returns an iterator over real types.
    ///
    /// It will signal an error and stop iteration if field value is malformed.
    ///
    /// It'll continue iteration in case of unknown [`GeometryType`]. This error is indicated
    /// by the [`std::io::ErrorKind::InvalidData`] kind and
    /// [`crate::constants::UnknownGeometryType`] payload.
    pub fn iter_geometry_types(&'a self) -> IterGeometryTypes<'a> {
        IterGeometryTypes {
            buf: ParseBuf(self.geometry_types.as_bytes()),
        }
    }
}

pub struct IterGeometryTypes<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterGeometryTypes<'a> {
    type Item = io::Result<GeometryType>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse::<RawInt<LenEnc>>(()) {
                Ok(x) => Some(
                    GeometryType::try_from(x.0 as u8)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
                ),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for GeometryTypes<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            geometry_types: buf.parse(())?,
        })
    }
}

impl MySerialize for GeometryTypes<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.geometry_types.serialize(buf);
    }
}

/// Contains a sequence of PK column indexes where PK doesn't have a prefix.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SimplePrimaryKey<'a> {
    indexes: RawBytes<'a, EofBytes>,
}

impl<'a> SimplePrimaryKey<'a> {
    /// Returns an iterator over column indexes.
    pub fn iter_indexes(&'a self) -> IterIndexes<'a> {
        IterIndexes {
            buf: ParseBuf(self.indexes.as_bytes()),
        }
    }
}

pub struct IterIndexes<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterIndexes<'a> {
    type Item = io::Result<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse::<RawInt<LenEnc>>(()) {
                Ok(x) => Some(Ok(x.0)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for SimplePrimaryKey<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            indexes: buf.parse(())?,
        })
    }
}

impl MySerialize for SimplePrimaryKey<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.indexes.serialize(buf);
    }
}

/// Contains a sequence of primary keys with prefix.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PrimaryKeysWithPrefix<'a> {
    data: RawBytes<'a, EofBytes>,
}

impl<'a> PrimaryKeysWithPrefix<'a> {
    /// Returns an iterator over column indexes and corresponding prefix lengths.
    pub fn iter_keys(&'a self) -> IterKeys<'a> {
        IterKeys {
            buf: ParseBuf(self.data.as_bytes()),
        }
    }
}

pub struct IterKeys<'a> {
    buf: ParseBuf<'a>,
}

impl<'a> Iterator for IterKeys<'a> {
    type Item = io::Result<PrimaryKeyWithPrefix>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            None
        } else {
            match self.buf.parse(()) {
                Ok(x) => Some(Ok(x)),
                Err(e) => {
                    self.buf = ParseBuf(b"");
                    Some(Err(e))
                }
            }
        }
    }
}

impl<'de> MyDeserialize<'de> for PrimaryKeysWithPrefix<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            data: buf.parse(())?,
        })
    }
}

impl MySerialize for PrimaryKeysWithPrefix<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.data.serialize(buf);
    }
}

/// Info about primary key with a prefix (see [`PrimaryKeysWithPrefix`]).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PrimaryKeyWithPrefix {
    column_index: RawInt<LenEnc>,
    prefix_length: RawInt<LenEnc>,
}

impl PrimaryKeyWithPrefix {
    /// Creates a new PK with prefix.
    pub fn new(column_index: u64, prefix_length: u64) -> Self {
        Self {
            column_index: RawInt::new(column_index),
            prefix_length: RawInt::new(prefix_length),
        }
    }

    /// Returns the column index
    pub fn column_index(&self) -> u64 {
        self.column_index.0
    }

    /// Returns the prefix length. `0` means that the whole column value is used.
    pub fn prefix_length(&self) -> u64 {
        self.prefix_length.0
    }
}

impl<'de> MyDeserialize<'de> for PrimaryKeyWithPrefix {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            column_index: buf.parse(())?,
            prefix_length: buf.parse(())?,
        })
    }
}

impl MySerialize for PrimaryKeyWithPrefix {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.column_index.serialize(buf);
        self.prefix_length.serialize(buf);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OptionalMetadataField<'a> {
    /// See [`OptionalMetadataFieldType::SIGNEDNESS`].
    Signedness(
        /// Flags indicating unsignedness for every numeric column.
        &'a BitSlice<u8, Msb0>,
    ),
    /// See [`OptionalMetadataFieldType::DEFAULT_CHARSET`].
    DefaultCharset(DefaultCharset<'a>),
    /// See [`OptionalMetadataFieldType::COLUMN_CHARSET`].
    ColumnCharset(ColumnCharsets<'a>),
    /// See [`OptionalMetadataFieldType::COLUMN_NAME`].
    ColumnName(ColumnNames<'a>),
    /// See [`OptionalMetadataFieldType::SET_STR_VALUE`].
    SetStrValue(SetsStrValues<'a>),
    /// See [`OptionalMetadataFieldType::ENUM_STR_VALUE`].
    EnumStrValue(EnumsStrValues<'a>),
    /// See [`OptionalMetadataFieldType::GEOMETRY_TYPE`].
    GeometryType(GeometryTypes<'a>),
    /// See [`OptionalMetadataFieldType::SIMPLE_PRIMARY_KEY`].
    SimplePrimaryKey(SimplePrimaryKey<'a>),
    /// See [`OptionalMetadataFieldType::PRIMARY_KEY_WITH_PREFIX`].
    PrimaryKeyWithPrefix(PrimaryKeysWithPrefix<'a>),
    /// See [`OptionalMetadataFieldType::ENUM_AND_SET_DEFAULT_CHARSET`].
    EnumAndSetDefaultCharset(DefaultCharset<'a>),
    /// See [`OptionalMetadataFieldType::ENUM_AND_SET_COLUMN_CHARSET`].
    EnumAndSetColumnCharset(ColumnCharsets<'a>),
    /// See [`OptionalMetadataFieldType::COLUMN_VISIBILITY`].
    ColumnVisibility(
        /// Flags indicating visibility for every numeric column.
        &'a BitSlice<u8, Msb0>,
    ),
}

/// Iterator over fields of an optional metadata.
#[derive(Debug)]
pub struct OptionalMetadataIter<'a> {
    columns: &'a RawSeq<'a, u8, ColumnType>,
    data: &'a [u8],
}

impl<'a> OptionalMetadataIter<'a> {
    /// Reads type-length-value value.
    fn read_tlv(&mut self) -> io::Result<(RawConst<u8, OptionalMetadataFieldType>, &'a [u8])> {
        let t = self.data.read_u8()?;
        let l = self.data.read_u8()? as usize;
        let v = match self.data.get(..l) {
            Some(v) => v,
            None => {
                self.data = &[];
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "can't read tlv value",
                ));
            }
        };
        self.data = &self.data[l..];
        Ok((RawConst::new(t), v))
    }

    /// Returns next tlv, if any.
    fn next_tlv(
        &mut self,
    ) -> Option<io::Result<(RawConst<u8, OptionalMetadataFieldType>, &'a [u8])>> {
        if self.data.is_empty() {
            return None;
        }

        self.read_tlv().map(Some).transpose()
    }

    fn num_columns(&self) -> usize {
        self.columns.0.len()
    }

    fn count_columns(&self, f: fn(&ColumnType) -> bool) -> usize {
        self.columns
            .0
            .iter()
            .filter_map(|val| ColumnType::try_from(*val).ok())
            .filter(f)
            .count()
    }
}

impl<'a> Iterator for OptionalMetadataIter<'a> {
    type Item = io::Result<OptionalMetadataField<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        use OptionalMetadataFieldType::*;

        self.next_tlv()?
            .and_then(|(t, v)| {
                let mut v = ParseBuf(v);
                match t.get() {
                    Ok(t) => match t {
                        SIGNEDNESS => {
                            let num_numeric = self.count_columns(ColumnType::is_numeric_type);
                            let num_flags_bytes = (num_numeric + 7) / 8;
                            let flags: &[u8] = v.parse(num_flags_bytes)?;

                            if !v.is_empty() {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "bytes remaining on stream",
                                ));
                            }

                            let flags = BitSlice::from_slice(flags);
                            Ok(OptionalMetadataField::Signedness(&flags[..num_numeric]))
                        }
                        DEFAULT_CHARSET => Ok(OptionalMetadataField::DefaultCharset(v.parse(())?)),
                        COLUMN_CHARSET => Ok(OptionalMetadataField::ColumnCharset(v.parse(())?)),
                        COLUMN_NAME => Ok(OptionalMetadataField::ColumnName(v.parse(())?)),
                        SET_STR_VALUE => Ok(OptionalMetadataField::SetStrValue(v.parse(())?)),
                        ENUM_STR_VALUE => Ok(OptionalMetadataField::EnumStrValue(v.parse(())?)),
                        GEOMETRY_TYPE => Ok(OptionalMetadataField::GeometryType(v.parse(())?)),
                        SIMPLE_PRIMARY_KEY => {
                            Ok(OptionalMetadataField::SimplePrimaryKey(v.parse(())?))
                        }
                        PRIMARY_KEY_WITH_PREFIX => {
                            Ok(OptionalMetadataField::PrimaryKeyWithPrefix(v.parse(())?))
                        }
                        ENUM_AND_SET_DEFAULT_CHARSET => Ok(
                            OptionalMetadataField::EnumAndSetDefaultCharset(v.parse(())?),
                        ),
                        ENUM_AND_SET_COLUMN_CHARSET => {
                            Ok(OptionalMetadataField::EnumAndSetColumnCharset(v.parse(())?))
                        }
                        COLUMN_VISIBILITY => {
                            let num_columns = self.num_columns();
                            let num_flags_bytes = (num_columns + 7) / 8;
                            let flags: &[u8] = v.parse(num_flags_bytes)?;

                            if !v.is_empty() {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "bytes remaining on stream",
                                ));
                            }

                            let flags = BitSlice::from_slice(flags);
                            let flags = &flags[..num_columns];
                            Ok(OptionalMetadataField::ColumnVisibility(flags))
                        }
                    },
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Unknown optional metadata field type",
                    )),
                }
            })
            .map(Some)
            .transpose()
    }
}

/// Helper struct that extracts optional metadata for columns.
pub struct OptionalMetaExtractor<'a> {
    signedness: Option<&'a BitSlice<u8, Msb0>>,
    default_charset: Option<DefaultCharset<'a>>,
    column_charset: Option<ColumnCharsets<'a>>,
    column_name: Option<ColumnNames<'a>>,
    simple_primary_key: Option<SimplePrimaryKey<'a>>,
    primary_key_with_prefix: Option<PrimaryKeysWithPrefix<'a>>,
    enum_and_set_default_charset: Option<DefaultCharset<'a>>,
    enum_and_set_column_charset: Option<ColumnCharsets<'a>>,
}

impl<'a> OptionalMetaExtractor<'a> {
    pub fn new(iter_optional_meta: OptionalMetadataIter<'a>) -> io::Result<Self> {
        let mut this = Self {
            signedness: None,
            default_charset: None,
            column_charset: None,
            column_name: None,
            simple_primary_key: None,
            primary_key_with_prefix: None,
            enum_and_set_default_charset: None,
            enum_and_set_column_charset: None,
        };

        for field in iter_optional_meta {
            match field? {
                OptionalMetadataField::Signedness(x) => this.signedness = Some(x),
                OptionalMetadataField::DefaultCharset(x) => {
                    this.default_charset = Some(x);
                }
                OptionalMetadataField::ColumnCharset(x) => {
                    this.column_charset = Some(x);
                }
                OptionalMetadataField::ColumnName(x) => {
                    this.column_name = Some(x);
                }
                OptionalMetadataField::SetStrValue(_) => (),
                OptionalMetadataField::EnumStrValue(_) => (),
                OptionalMetadataField::GeometryType(_) => (),
                OptionalMetadataField::SimplePrimaryKey(x) => {
                    this.simple_primary_key = Some(x);
                }
                OptionalMetadataField::PrimaryKeyWithPrefix(x) => {
                    this.primary_key_with_prefix = Some(x);
                }
                OptionalMetadataField::EnumAndSetDefaultCharset(x) => {
                    this.enum_and_set_default_charset = Some(x);
                }
                OptionalMetadataField::EnumAndSetColumnCharset(x) => {
                    this.enum_and_set_column_charset = Some(x);
                }
                OptionalMetadataField::ColumnVisibility(_) => (),
            }
        }

        Ok(this)
    }

    /// For every numeric column (in order) emits signedness data (`true` means _unsigned_).
    ///
    /// Emits nothing if there are no signedness data in the optional metadata.
    pub fn iter_signedness(&'a self) -> impl Iterator<Item = bool> + 'a {
        self.signedness
            .as_ref()
            .map(|x| x.iter().by_vals())
            .into_iter()
            .flatten()
    }

    /// For every character column (in order) emits its character set.
    ///
    /// Will use either `DEFAULT_CHARSET` or `COLUMN_CHARSET` metadata.
    ///
    /// ### Warning
    ///
    /// This iterator is infinite if `DEFAULT_CHARSET` is used.
    ///
    /// Emits nothing if there are no charset data in the optional metadata.
    pub fn iter_charset(&'a self) -> impl Iterator<Item = Result<u16, io::Error>> + 'a {
        let default_charset = self.default_charset.as_ref().map(|x| x.default_charset());
        let non_default = self.default_charset.as_ref().map(|x| x.iter_non_default());
        let per_column = self.column_charset.as_ref().map(|x| x.iter_charsets());

        iter_charset_helper(default_charset, non_default, per_column)
    }

    /// For every ENUM and SET column (in order) emits its character set.
    ///
    /// Will use either `ENUM_AND_SET_DEFAULT_CHARSET` or `ENUM_AND_SET_COLUMN_CHARSET` metadata.
    ///
    /// ### Warning
    ///
    /// This iterator is infinite if `ENUM_AND_SET_DEFAULT_CHARSET` is used.
    ///
    /// Emits nothing if there are no charset data in the optional metadata.
    pub fn iter_enum_and_set_charset(
        &'a self,
    ) -> impl Iterator<Item = Result<u16, io::Error>> + 'a {
        let default_charset = self
            .enum_and_set_default_charset
            .as_ref()
            .map(|x| x.default_charset());
        let non_default = self
            .enum_and_set_default_charset
            .as_ref()
            .map(|x| x.iter_non_default());
        let per_column = self
            .enum_and_set_column_charset
            .as_ref()
            .map(|x| x.iter_charsets());

        iter_charset_helper(default_charset, non_default, per_column)
    }

    /// Iterates over column indexes for columns that are primary keys.
    ///
    /// Returns peekable iterator so that it is possible to observe next PK index.
    ///
    /// Emits nothing if there are no PK data in the optinal metadata.
    pub fn iter_primary_key(&'a self) -> Peekable<impl Iterator<Item = io::Result<u64>> + 'a> {
        let simple = self
            .simple_primary_key
            .as_ref()
            .map(|x| x.iter_indexes())
            .into_iter()
            .flatten();
        let prefixed = self
            .primary_key_with_prefix
            .as_ref()
            .map(|x| x.iter_keys().map(|x| x.map(|x| x.column_index())))
            .into_iter()
            .flatten();

        simple.chain(prefixed).peekable()
    }

    /// For every column (in order) emits its name
    ///
    /// Emits nothing if there are no column name data in the optional metadata.
    pub fn iter_column_name(&'a self) -> impl Iterator<Item = io::Result<ColumnName<'a>>> + 'a {
        self.column_name
            .as_ref()
            .map(|x| x.iter_names())
            .into_iter()
            .flatten()
    }
}

fn iter_charset_helper<'a>(
    default_charset: Option<u16>,
    iter_non_default: Option<IterNonDefault<'a>>,
    iter_charsets: Option<IterCharsets<'a>>,
) -> impl Iterator<Item = Result<u16, io::Error>> + 'a {
    let non_default = iter_non_default
        .into_iter()
        .flatten()
        .map(|x| x.map(Either::Left));
    let per_column = iter_charsets
        .into_iter()
        .flatten()
        .map(|x| x.map(Either::Right));

    let mut non_default = non_default.chain(per_column).peekable();
    let mut broken = false;
    let mut current = 0;
    std::iter::from_fn(move || {
        if broken {
            return None;
        }

        let result = match non_default.peek() {
            Some(x) => match x {
                Ok(x) => match x {
                    Either::Left(x) => {
                        if x.column_index() == current {
                            non_default
                                .next()
                                .map(|x| Ok(x.unwrap().unwrap_left().charset()))
                        } else {
                            default_charset.map(Ok)
                        }
                    }
                    Either::Right(x) => {
                        let x = *x;
                        non_default.next().map(|_| Ok(x))
                    }
                },
                Err(_) => {
                    broken = true;
                    non_default.next().map(|x| Err(x.unwrap_err()))
                }
            },
            None => default_charset.map(Ok),
        };

        current += 1;
        result
    })
}
