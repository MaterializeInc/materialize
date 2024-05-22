// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    borrow::Cow,
    cmp::min,
    io::{self},
};

use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{bytes::EofBytes, int::LeU32, RawBytes, RawInt},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

/// Begin load query event.
///
/// Used for LOAD DATA INFILE statements as of MySQL 5.0.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BeginLoadQueryEvent<'a> {
    file_id: RawInt<LeU32>,
    block_data: RawBytes<'a, EofBytes>,
}

impl<'a> BeginLoadQueryEvent<'a> {
    pub fn new(file_id: u32) -> Self {
        Self {
            file_id: RawInt::new(file_id),
            block_data: Default::default(),
        }
    }

    /// Sets the `file_id` value.
    pub fn with_file_id(mut self, file_id: u32) -> Self {
        self.file_id.0 = file_id;
        self
    }

    /// Sets the `block_data` value.
    pub fn with_block_data(mut self, block_data: impl Into<Cow<'a, [u8]>>) -> Self {
        self.block_data = RawBytes::new(block_data);
        self
    }

    /// Returns the `file_id` value.
    ///
    /// `file_id` is the ID of the file to begin load to.
    pub fn file_id(&self) -> u32 {
        self.file_id.0
    }

    /// Returns the `block_data` value.
    ///
    /// `block_data` is the data to load.
    pub fn block_data(&'a self) -> &'a [u8] {
        self.block_data.as_bytes()
    }

    pub fn into_owned(self) -> BeginLoadQueryEvent<'static> {
        BeginLoadQueryEvent {
            file_id: self.file_id,
            block_data: self.block_data.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for BeginLoadQueryEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            file_id: buf.parse(())?,
            block_data: buf.parse(())?,
        })
    }
}

impl MySerialize for BeginLoadQueryEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.file_id.serialize(&mut *buf);
        self.block_data.serialize(&mut *buf);
    }
}

impl<'a> BinlogStruct<'a> for BeginLoadQueryEvent<'a> {
    /// Returns length of this load event in bytes.
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(4);
        len += S(self.block_data.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}

impl<'a> BinlogEvent<'a> for BeginLoadQueryEvent<'a> {
    const EVENT_TYPE: EventType = EventType::BEGIN_LOAD_QUERY_EVENT;
}
