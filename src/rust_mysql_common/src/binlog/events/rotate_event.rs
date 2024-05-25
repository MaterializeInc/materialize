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
    misc::raw::{bytes::EofBytes, int::*, RawBytes},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

/// The rotate event is added to the binlog as last event
/// to tell the reader what binlog to request next.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RotateEvent<'a> {
    // post-header
    /// Only available if binlog version > 1 (zero otherwise).
    position: RawInt<LeU64>,

    // payload
    /// Name of the next binlog.
    name: RawBytes<'a, EofBytes>,
}

impl<'a> RotateEvent<'a> {
    pub fn new(position: u64, name: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            position: RawInt::new(position),
            name: RawBytes::new(name),
        }
    }

    /// Sets the `position` field value.
    pub fn with_position(mut self, position: u64) -> Self {
        self.position = RawInt::new(position);
        self
    }

    /// Sets the `name` field value.
    pub fn with_name(mut self, name: impl Into<Cow<'a, [u8]>>) -> Self {
        self.name = RawBytes::new(name);
        self
    }

    /// Returns the position within the binary log to rotate to.
    pub fn position(&self) -> u64 {
        self.position.0
    }

    /// Returns raw name of the binlog to rotate to.
    pub fn name_raw(&'a self) -> &'a [u8] {
        self.name.as_bytes()
    }

    /// Returns name of the binlog to rotate to as a string (lossy converted).
    pub fn name(&'a self) -> Cow<'a, str> {
        self.name.as_str()
    }

    /// Returns true if it's a fake [`RotateEvent`] (its log postion is `0`).
    pub fn is_fake(&self) -> bool {
        self.position() == 0
    }

    pub fn into_owned(self) -> RotateEvent<'static> {
        RotateEvent {
            position: self.position,
            name: self.name.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for RotateEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            position: buf.parse(())?,
            name: buf.parse(())?,
        })
    }
}

impl MySerialize for RotateEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.position.serialize(&mut *buf);
        self.name.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for RotateEvent<'a> {
    const EVENT_TYPE: EventType = EventType::ROTATE_EVENT;
}

impl<'a> BinlogStruct<'a> for RotateEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(8);
        len += S(self.name.0.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}
