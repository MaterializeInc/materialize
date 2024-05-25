// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::io::{self};

use bytes::BufMut;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::unexpected_buf_eof,
    proto::{MyDeserialize, MySerialize},
};

/// Xid event.
///
/// Generated for a commit of a transaction that modifies one or more tables of an XA-capable
/// storage engine.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct XidEvent {
    pub xid: u64,
}

impl<'de> MyDeserialize<'de> for XidEvent {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let post_header_len = ctx.fde.get_event_type_header_length(Self::EVENT_TYPE);

        if !buf.checked_skip(post_header_len as usize) {
            return Err(unexpected_buf_eof());
        }

        let xid = buf.checked_eat_u64_le().ok_or_else(unexpected_buf_eof)?;

        Ok(Self { xid })
    }
}

impl MySerialize for XidEvent {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(self.xid);
    }
}

impl<'a> BinlogEvent<'a> for XidEvent {
    const EVENT_TYPE: EventType = EventType::XID_EVENT;
}

impl<'a> BinlogStruct<'a> for XidEvent {
    fn len(&self, _version: BinlogVersion) -> usize {
        8
    }
}
