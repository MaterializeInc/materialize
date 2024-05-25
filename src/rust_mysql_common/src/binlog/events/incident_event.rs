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
        consts::{BinlogVersion, EventType, IncidentType, UnknownIncidentType},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{bytes::U8Bytes, int::*, RawBytes, RawConst},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

/// Used to log an out of the ordinary event that occurred on the master.
///
/// It notifies the slave that something happened on the master that might cause data
/// to be in an inconsistent state.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IncidentEvent<'a> {
    incident_type: RawConst<LeU16, IncidentType>,
    message: RawBytes<'a, U8Bytes>,
}

impl<'a> IncidentEvent<'a> {
    pub fn new(incident_type: IncidentType, message: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            incident_type: RawConst::new(incident_type as u16),
            message: RawBytes::new(message),
        }
    }

    /// Returns the `incident_type` value, if it's valid.
    pub fn incident_type(&self) -> Result<IncidentType, UnknownIncidentType> {
        self.incident_type.get()
    }

    /// Returns the raw `message` value.
    pub fn message_raw(&'a self) -> &'a [u8] {
        self.message.as_bytes()
    }

    /// Returns `message` value as a string (lossy converted).
    pub fn message(&'a self) -> Cow<'a, str> {
        self.message.as_str()
    }

    /// Sets the `incident_type` value.
    pub fn with_incident_type(mut self, incident_type: IncidentType) -> Self {
        self.incident_type = RawConst::new(incident_type as u16);
        self
    }

    /// Sets the `message` value.
    pub fn with_message(mut self, message: impl Into<Cow<'a, [u8]>>) -> Self {
        self.message = RawBytes::new(message);
        self
    }

    pub fn into_owned(self) -> IncidentEvent<'static> {
        IncidentEvent {
            incident_type: self.incident_type,
            message: self.message.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for IncidentEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            incident_type: buf.parse(())?,
            message: buf.parse(())?,
        })
    }
}

impl MySerialize for IncidentEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.incident_type.serialize(&mut *buf);
        self.message.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for IncidentEvent<'a> {
    const EVENT_TYPE: EventType = EventType::INCIDENT_EVENT;
}

impl<'a> BinlogStruct<'a> for IncidentEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(2);
        len += S(1);
        len += S(min(self.message.0.len(), u8::MAX as usize));

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}
