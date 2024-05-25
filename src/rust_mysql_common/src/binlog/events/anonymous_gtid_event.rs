// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::io::{self};

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    proto::{MyDeserialize, MySerialize},
};

use super::GtidEvent;

/// Anonymous GTID event.
#[repr(transparent)]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AnonymousGtidEvent(pub GtidEvent);

impl<'de> MyDeserialize<'de> for AnonymousGtidEvent {
    const SIZE: Option<usize> = GtidEvent::SIZE;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        buf.parse_unchecked(ctx).map(Self)
    }
}

impl MySerialize for AnonymousGtidEvent {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf)
    }
}

impl<'a> BinlogStruct<'a> for AnonymousGtidEvent {
    fn len(&self, version: BinlogVersion) -> usize {
        self.0.len(version)
    }
}

impl<'a> BinlogEvent<'a> for AnonymousGtidEvent {
    const EVENT_TYPE: EventType = EventType::ANONYMOUS_GTID_EVENT;
}
