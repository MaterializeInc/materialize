// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{cmp::min, fmt, io};

use bitvec::prelude::*;
use bytes::BufMut;
use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType, RowsEventFlags},
        row::BinlogRow,
        BinlogCtx,
    },
    io::ParseBuf,
    misc::{
        raw::{
            bytes::{BareBytes, EofBytes},
            int::*,
            RawBytes, RawFlags,
        },
        unexpected_buf_eof,
    },
    proto::{MyDeserialize, MySerialize},
};

use super::{BinlogEventHeader, TableMapEvent};

/// Common base structure for all row-containing binary log events.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RowsEvent<'a> {
    /// An actual `EventType` of this wrapped object.
    event_type: EventType,
    /// Table identifier.
    ///
    /// If the table id is `0x00ffffff` it is a dummy event that should have
    /// the end of statement flag set that declares that all table maps can be freed.
    /// Otherwise it refers to a table defined by `TABLE_MAP_EVENT`.
    table_id: RawInt<LeU48>,
    /// Raw rows event flags (see `RowsEventFlags`).
    flags: RawFlags<RowsEventFlags, LeU16>,
    /// Raw extra data.
    ///
    /// Length is up to `u16::MAX - 2` bytes.
    extra_data: RawBytes<'a, BareBytes<{ u16::MAX as usize - 2 }>>,
    /// Number of columns.
    num_columns: RawInt<LenEnc>,
    /// For DELETE and UPDATE only. Bit-field indicating whether each column is used one bit
    /// per column.
    ///
    /// Will be empty for WRITE events.
    columns_before_image: Option<RawBytes<'a, BareBytes<0x2000000000000000>>>,
    /// For WRITE and UPDATE only. Bit-field indicating whether each column is used
    /// in the `UPDATE_ROWS_EVENT` and `WRITE_ROWS_EVENT` after-image; one bit per column.
    ///
    /// Will be empty for DELETE events.
    columns_after_image: Option<RawBytes<'a, BareBytes<0x2000000000000000>>>,
    /// A sequence of zero or more rows. The end is determined by the size of the event.
    ///
    /// Each row has the following format:
    ///
    /// *   A Bit-field indicating whether each field in the row is NULL. Only columns that
    ///     are "used" according to the second field in the variable data part are listed here.
    ///     If the second field in the variable data part has N one-bits, the amount of storage
    ///     required for this field is INT((N + 7) / 8) bytes.
    /// *   The row-image, containing values of all table fields. This only lists table fields
    ///     that are used (according to the second field of the variable data part) and non-NULL
    ///     (according to the previous field). In other words, the number of values listed here
    ///     is equal to the number of zero bits in the previous field. (not counting padding
    ///     bits in the last byte).
    rows_data: RawBytes<'a, EofBytes>,
}

impl<'a> RowsEvent<'a> {
    /// Returns an actual event type of this rows event.
    pub fn event_type(&self) -> EventType {
        self.event_type
    }

    /// Returns the number that identifies the table (see `TableMapEvent`).
    pub fn table_id(&self) -> u64 {
        self.table_id.0
    }

    /// Returns the number of columns in the table.
    pub fn num_columns(&self) -> u64 {
        self.num_columns.0
    }

    /// Returns columns in the before-image (only for DELETE and UPDATE).
    ///
    /// Each bit indicates whether corresponding column is used in the image.
    pub fn columns_before_image(&'a self) -> Option<&'a BitSlice<u8>> {
        match self.columns_before_image {
            Some(ref bytes) => {
                let slice = BitSlice::from_slice(bytes.as_bytes());
                Some(&slice[..self.num_columns() as usize])
            }
            None => None,
        }
    }

    /// Returns columns in the after-image (only for WRITE and UPDATE).
    ///
    /// Each bit indicates whether corresponding column is used in the image.
    pub fn columns_after_image(&'a self) -> Option<&'a BitSlice<u8>> {
        match self.columns_after_image {
            Some(ref bytes) => {
                let slice = BitSlice::from_slice(bytes.as_bytes());
                Some(&slice[..self.num_columns() as usize])
            }
            None => None,
        }
    }

    /// Returns raw rows data.
    pub fn rows_data(&'a self) -> &'a [u8] {
        self.rows_data.as_bytes()
    }

    /// Returns event flags (unknown bits are truncated).
    pub fn flags(&self) -> RowsEventFlags {
        self.flags.get()
    }

    /// Returns raw event flags (unknown bits are preserved).
    pub fn flags_raw(&self) -> u16 {
        self.flags.0
    }

    /// Returns length of this event in bytes.
    ///
    /// This function will be used in `BinlogStruct` implementations for derived events.
    pub fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(6); // table_id
        len += S(2); // flags
        len += S(2); // extra-data len
        len += S(min(self.extra_data.len(), u16::MAX as usize - 2)); // extra data
        len += S(crate::misc::lenenc_int_len(self.num_columns()) as usize); // number of columns
        let bitmap_len = (self.num_columns() as usize + 7) / 8;
        if self.columns_before_image.is_some() {
            len += S(bitmap_len); // columns present bitmap 1
        }
        if self.columns_after_image.is_some() {
            len += S(bitmap_len); // columns present bitmap 2
        }
        len += S(self.rows_data.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }

    /// Returns an iterator over event's rows given the corresponding `TableMapEvent`.
    pub fn rows<'b>(&'b self, table_map_event: &'b TableMapEvent<'b>) -> RowsEventRows<'b> {
        RowsEventRows {
            rows_event: self,
            table_map_event,
            rows_data: ParseBuf(self.rows_data.as_bytes()),
        }
    }

    pub fn into_owned(self) -> RowsEvent<'static> {
        RowsEvent {
            event_type: self.event_type,
            table_id: self.table_id,
            flags: self.flags,
            extra_data: self.extra_data.into_owned(),
            num_columns: self.num_columns,
            columns_before_image: self.columns_before_image.map(|x| x.into_owned()),
            columns_after_image: self.columns_after_image.map(|x| x.into_owned()),
            rows_data: self.rows_data.into_owned(),
        }
    }
}

/// Deserialization context for [`RowsEvent`].
pub struct RowsEventCtx<'a> {
    /// An actual event type.
    pub event_type: EventType,
    /// Additional context data.
    pub binlog_ctx: BinlogCtx<'a>,
}

impl<'de> MyDeserialize<'de> for RowsEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = RowsEventCtx<'de>;

    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let post_header_len = ctx
            .binlog_ctx
            .fde
            .get_event_type_header_length(ctx.event_type);

        let is_delete_event = ctx.event_type == EventType::DELETE_ROWS_EVENT
            || ctx.event_type == EventType::DELETE_ROWS_EVENT_V1;

        let is_update_event = ctx.event_type == EventType::UPDATE_ROWS_EVENT
            || ctx.event_type == EventType::UPDATE_ROWS_EVENT_V1
            || ctx.event_type == EventType::PARTIAL_UPDATE_ROWS_EVENT;

        let table_id = if post_header_len == 6 {
            // old server
            let value = buf.parse::<RawInt<LeU32>>(())?;
            RawInt::new(value.0 as u64)
        } else {
            buf.parse(())?
        };

        let flags = buf.parse(())?;

        let extra_data = if post_header_len
            == ctx
                .binlog_ctx
                .fde
                .get_event_type_header_length(EventType::WRITE_ROWS_EVENT)
        {
            // variable-length post header containing extra data
            let extra_data_len = buf.checked_eat_u16_le().ok_or_else(unexpected_buf_eof)? as usize;
            buf.parse(extra_data_len.saturating_sub(2))?
        } else {
            RawBytes::new(&[][..])
        };

        let num_columns: RawInt<LenEnc> = buf.parse(())?;
        let bitmap_len = (num_columns.0 as usize + 7) / 8;

        let mut columns_before_image = None;
        let mut columns_after_image = None;

        if is_update_event {
            columns_before_image = Some(buf.parse(bitmap_len)?);
            columns_after_image = Some(buf.parse(bitmap_len)?);
        } else if is_delete_event {
            columns_before_image = Some(buf.parse(bitmap_len)?);
        } else {
            columns_after_image = Some(buf.parse(bitmap_len)?);
        }

        let rows_data = buf.parse(())?;

        Ok(Self {
            event_type: ctx.event_type,
            table_id,
            flags,
            extra_data,
            num_columns,
            columns_before_image,
            columns_after_image,
            rows_data,
        })
    }
}

impl MySerialize for RowsEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.table_id.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);

        if self.event_type == EventType::WRITE_ROWS_EVENT
            || self.event_type == EventType::UPDATE_ROWS_EVENT
            || self.event_type == EventType::DELETE_ROWS_EVENT
            || self.event_type == EventType::PARTIAL_UPDATE_ROWS_EVENT
        {
            let len = min(self.extra_data.len().saturating_add(2), u16::MAX as usize) as u16;
            buf.put_u16_le(len);
            self.extra_data.serialize(&mut *buf);
        }

        self.num_columns.serialize(&mut *buf);

        if let Some(bitmap) = &self.columns_before_image {
            bitmap.serialize(&mut *buf);
        }
        if let Some(bitmap) = &self.columns_after_image {
            bitmap.serialize(&mut *buf);
        }

        self.rows_data.serialize(buf);
    }
}

/// Iterator over rows of a `RowsEvent`.
#[derive(Clone, Eq, PartialEq)]
pub struct RowsEventRows<'a> {
    rows_event: &'a RowsEvent<'a>,
    table_map_event: &'a TableMapEvent<'a>,
    rows_data: ParseBuf<'a>,
}

impl<'a> RowsEventRows<'a> {
    pub(crate) fn new(
        rows_event: &'a RowsEvent<'a>,
        table_map_event: &'a TableMapEvent<'a>,
        rows_data: ParseBuf<'a>,
    ) -> Self {
        Self {
            rows_event,
            table_map_event,
            rows_data,
        }
    }
}

impl<'a> Iterator for RowsEventRows<'a> {
    type Item = io::Result<(Option<BinlogRow>, Option<BinlogRow>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut row_before = None;
        let mut row_after = None;

        if self.rows_data.is_empty() {
            return None;
        }

        if let Some(cols) = self.rows_event.columns_before_image() {
            let ctx = (
                self.rows_event.num_columns(),
                cols,
                false,
                self.table_map_event,
            );
            row_before = match self.rows_data.parse(ctx) {
                Ok(row_before) => Some(row_before),
                Err(err) => return Some(Err(err)),
            };
        }

        if let Some(cols) = self.rows_event.columns_after_image() {
            let ctx = (
                self.rows_event.num_columns(),
                cols,
                self.rows_event.event_type == EventType::PARTIAL_UPDATE_ROWS_EVENT,
                self.table_map_event,
            );
            row_after = match self.rows_data.parse(ctx) {
                Ok(row_after) => Some(row_after),
                Err(err) => return Some(Err(err)),
            };
        }

        Some(Ok((row_before, row_after)))
    }
}

impl fmt::Debug for RowsEventRows<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}
