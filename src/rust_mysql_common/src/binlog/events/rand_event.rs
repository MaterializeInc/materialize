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
    misc::raw::{int::LeU64, RawInt},
    proto::{MyDeserialize, MySerialize},
};

/// Rand event.
///
/// Logs random seed used by the next `RAND()`, and by `PASSWORD()` in 4.1.0. 4.1.1 does not need
/// it (it's repeatable again) so this event needn't be written in 4.1.1 for `PASSWORD()`
/// (but the fact that it is written is just a waste, it does not cause bugs).
///
/// The state of the random number generation consists of 128 bits, which are stored internally
/// as two 64-bit numbers.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RandEvent {
    pub seed1: RawInt<LeU64>,
    pub seed2: RawInt<LeU64>,
}

impl<'de> MyDeserialize<'de> for RandEvent {
    const SIZE: Option<usize> = Some(16);
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            seed1: buf.parse_unchecked(())?,
            seed2: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for RandEvent {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.seed1.serialize(&mut *buf);
        self.seed2.serialize(&mut *buf);
    }
}

impl<'a> BinlogEvent<'a> for RandEvent {
    const EVENT_TYPE: EventType = EventType::RAND_EVENT;
}

impl<'a> BinlogStruct<'a> for RandEvent {
    fn len(&self, _version: BinlogVersion) -> usize {
        8
    }
}
