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

use bytes::BufMut;
use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType, UserVarFlags},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    constants::{ItemResult, UnknownItemResultType},
    io::ParseBuf,
    misc::raw::{bytes::U32Bytes, int::*, RawBytes, RawConst, RawFlags},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

/// User variable event.
///
/// Written every time a statement uses a user variable; precedes other events for the statement.
/// Indicates the value to use for the user variable in the next statement.
/// This is written only before a `QUERY_EVENT` and is not used with row-based logging.
///
/// # Notes on `BinlogEvent` implementation
///
/// * it won't try to read/write anything except `name` and `is_null` if `is_null` is `true`
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct UserVarEvent<'a> {
    /// User variable name.
    name: RawBytes<'a, U32Bytes>,
    /// `true` if value is `NULL`.
    is_null: bool,
    /// Type of a value.
    value_type: RawConst<i8, ItemResult>,
    /// Character set of a value. Will be `0` if `is_null` is `true`.
    charset: RawInt<LeU32>,
    /// Value of a user variable. Will be empty if `is_null` is `true`.
    value: RawBytes<'a, U32Bytes>,
    /// Flags of a user variable. Will be `0` if `is_null` is `true`.
    flags: RawFlags<UserVarFlags, u8>,
}

impl<'a> UserVarEvent<'a> {
    /// Returns the raw name of the variable.
    pub fn name_raw(&'a self) -> &'a [u8] {
        self.name.as_bytes()
    }

    /// Returns the name of the variable as a string (lossy converted).
    pub fn name(&'a self) -> Cow<'a, str> {
        self.name.as_str()
    }

    /// This is `true` if the value is null.
    pub fn is_null(&self) -> bool {
        self.is_null
    }

    /// Returns the type of the variable value.
    pub fn value_type(&self) -> Result<ItemResult, UnknownItemResultType> {
        self.value_type.get()
    }

    /// Returns the charset of the variable value (`0` if [`Self::is_null`] is `true`).
    pub fn charset(&self) -> u32 {
        self.charset.0
    }

    /// Returns the value of the variable (empty if [`Self::is_null`] is `true`).
    pub fn value(&'a self) -> &'a [u8] {
        self.value.as_bytes()
    }

    /// Returns the raw flags.
    pub fn flags_raw(&self) -> u8 {
        self.flags.0
    }

    /// Returns the flags (unknown bits are truncated).
    pub fn flags(&self) -> UserVarFlags {
        self.flags.get()
    }

    pub fn into_owned(self) -> UserVarEvent<'static> {
        UserVarEvent {
            name: self.name.into_owned(),
            is_null: self.is_null,
            value_type: self.value_type,
            charset: self.charset,
            value: self.value.into_owned(),
            flags: self.flags,
        }
    }
}

impl<'de> MyDeserialize<'de> for UserVarEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let name = buf.parse(())?;
        let is_null = buf.parse::<RawInt<u8>>(())?.0 != 0;

        if is_null {
            return Ok(Self {
                name,
                is_null,
                value_type: RawConst::new(ItemResult::STRING_RESULT as i8),
                charset: RawInt::new(63),
                value: RawBytes::default(),
                flags: RawFlags::default(),
            });
        }

        let mut sbuf: ParseBuf = buf.parse(5)?;
        let value_type = sbuf.parse_unchecked(())?;
        let charset = sbuf.parse_unchecked(())?;

        let value = buf.parse(())?;

        // Old servers may not pack flags here.
        let flags = if !buf.is_empty() {
            buf.parse_unchecked(())?
        } else {
            Default::default()
        };

        Ok(Self {
            name,
            is_null,
            value_type,
            charset,
            value,
            flags,
        })
    }
}

impl MySerialize for UserVarEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.name.serialize(&mut *buf);
        buf.put_u8(self.is_null as u8);
        if !self.is_null {
            self.value_type.serialize(&mut *buf);
            self.charset.serialize(&mut *buf);
            self.value.serialize(&mut *buf);
            self.flags.serialize(&mut *buf);
        }
    }
}

impl<'a> BinlogEvent<'a> for UserVarEvent<'a> {
    const EVENT_TYPE: EventType = EventType::USER_VAR_EVENT;
}

impl<'a> BinlogStruct<'a> for UserVarEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(4);
        len += S(min(self.name.0.len(), u32::MAX as usize));
        len += S(1);

        if !self.is_null {
            len += S(1);
            len += S(4);
            len += S(4);
            len += S(min(self.value.len(), u32::MAX as usize));
            len += S(1);
        }

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}
