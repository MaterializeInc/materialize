// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{borrow::Cow, cmp::min, io};

use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{bytes::EofBytes, RawBytes, Skip},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

/// Query that caused the following `ROWS_EVENT`.
///
/// It is used to write the original query in the binlog file in case of row-based replication
/// when the session flag `binlog_rows_query_log_events` is set.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RowsQueryEvent<'a> {
    /// Length is ignored.
    length: Skip<1>,
    query: RawBytes<'a, EofBytes>,
}

impl<'a> RowsQueryEvent<'a> {
    /// Creates a new `RowsQueryEvent`.
    pub fn new(query: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            length: Default::default(),
            query: RawBytes::new(query),
        }
    }

    /// Returns the raw query.
    pub fn query_raw(&'a self) -> &'a [u8] {
        self.query.as_bytes()
    }

    /// Returns query as a string (lossy converted).
    pub fn query(&'a self) -> Cow<'a, str> {
        self.query.as_str()
    }

    pub fn into_owned(self) -> RowsQueryEvent<'static> {
        RowsQueryEvent {
            length: self.length,
            query: self.query.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for RowsQueryEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            length: buf.parse(())?,
            query: buf.parse(())?,
        })
    }
}

impl MySerialize for RowsQueryEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.length.serialize(&mut *buf);
        self.query.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for RowsQueryEvent<'a> {
    const EVENT_TYPE: EventType = EventType::ROWS_QUERY_EVENT;
}

impl<'a> BinlogStruct<'a> for RowsQueryEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(1);
        len += S(self.query.0.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}
