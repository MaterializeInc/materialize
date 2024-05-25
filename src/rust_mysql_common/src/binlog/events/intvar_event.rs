// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::io;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType, IntvarEventType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{int::*, Const},
    proto::{MyDeserialize, MySerialize},
};

/// Integer based session-variables event.
///
/// Written every time a statement uses an AUTO_INCREMENT column or the LAST_INSERT_ID() function;
/// precedes other events for the statement. This is written only before a QUERY_EVENT
/// and is not used with row-based logging. An INTVAR_EVENT is written with a "subtype"
/// in the event data part.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IntvarEvent {
    /// One byte identifying the type of variable stored.
    subtype: Const<IntvarEventType, u8>,
    /// The value of the variable.
    value: RawInt<LeU64>,
}

impl IntvarEvent {
    /// Creates a new instance.
    pub fn new(subtype: IntvarEventType, value: u64) -> Self {
        Self {
            subtype: Const::new(subtype),
            value: RawInt::new(value),
        }
    }

    /// Returns the `subtype` value.
    ///
    /// `subtype` is a one byte identifying the type of variable stored.
    pub fn subtype(&self) -> IntvarEventType {
        self.subtype.0
    }

    /// Returns the `value` value.
    ///
    /// `value` is the value of the variable.
    pub fn value(&self) -> u64 {
        self.value.0
    }

    /// Sets the `subtype` value.
    pub fn with_subtype(mut self, subtype: IntvarEventType) -> Self {
        self.subtype = Const::new(subtype);
        self
    }

    /// Sets the `value` value.
    pub fn with_value(mut self, value: u64) -> Self {
        self.value = RawInt::new(value);
        self
    }
}

impl<'de> MyDeserialize<'de> for IntvarEvent {
    const SIZE: Option<usize> = Some(9);
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            subtype: buf.parse_unchecked(())?,
            value: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for IntvarEvent {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.subtype.serialize(&mut *buf);
        self.value.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for IntvarEvent {
    const EVENT_TYPE: EventType = EventType::INTVAR_EVENT;
}

impl<'a> BinlogStruct<'a> for IntvarEvent {
    fn len(&self, _version: BinlogVersion) -> usize {
        9
    }
}
