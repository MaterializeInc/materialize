// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{cmp::min, io};

use saturating::Saturating as S;

use crate::{
    binlog::{
        consts::{BinlogVersion, EventType, Gno, GtidFlags},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{int::*, RawConst, RawFlags},
    proto::{MyDeserialize, MySerialize},
};

use super::BinlogEventHeader;

define_const!(
    ConstU8,
    LogicalTimestampTypecode,
    InvalidLogicalTimestampTypecode("Invalid logical timestamp typecode value for GTID event"),
    2
);

/// GTID stands for Global Transaction IDentifier.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GtidEvent {
    /// Raw flags value.
    flags: RawFlags<GtidFlags, u8>,
    /// UUID representing the SID.
    sid: [u8; Self::ENCODED_SID_LENGTH],
    /// Group number, second component of GTID.
    ///
    ///
    /// Should be an integer between `MIN_GNO` and `MAX_GNO` for GtidEvent
    /// or `0` for AnonymousGtidEvent.
    gno: RawConst<LeU64, Gno>,
    /// If defined, then always equal to the constant [`GtidEvent::LOGICAL_TIMESTAMP_TYPECODE`].
    ///
    /// May be missing for 5.6. Will have different value on 5.7.4 and earlier (ignored).
    lc_typecode: Option<LogicalTimestampTypecode>,
    /// Store the transaction's commit parent `sequence_number`.
    last_committed: RawInt<LeU64>,
    /// The transaction's logical timestamp assigned at prepare phase.
    ///
    /// If it isn't `0` then it must be greater than `last_committed` timestamp.
    sequence_number: RawInt<LeU64>,
    /// Timestamp when the transaction was committed on the nearest master.
    immediate_commit_timestamp: RawInt<LeU56>,
    /// Timestamp when the transaction was committed on the originating master.
    original_commit_timestamp: RawInt<LeU56>,
    /// The packed transaction's length in bytes, including the Gtid.
    tx_length: RawInt<LenEnc>,
    /// The version of the server where the transaction was originally executed.
    original_server_version: RawInt<LeU32>,
    /// The version of the immediate server.
    immediate_server_version: RawInt<LeU32>,
}

impl GtidEvent {
    pub const POST_HEADER_LENGTH: usize = 1 + Self::ENCODED_SID_LENGTH + 8 + 1 + 16;
    pub const ENCODED_SID_LENGTH: usize = 16;
    pub const LOGICAL_TIMESTAMP_TYPECODE: u8 = 2;
    pub const IMMEDIATE_COMMIT_TIMESTAMP_LENGTH: usize = 7;
    pub const ORIGINAL_COMMIT_TIMESTAMP_LENGTH: usize = 7;
    pub const UNDEFINED_SERVER_VERSION: u32 = 999_999;
    pub const IMMEDIATE_SERVER_VERSION_LENGTH: usize = 4;

    pub fn new(sid: [u8; Self::ENCODED_SID_LENGTH], gno: u64) -> Self {
        Self {
            flags: Default::default(),
            sid,
            gno: RawConst::new(gno),
            lc_typecode: Some(LogicalTimestampTypecode::default()),
            last_committed: Default::default(),
            sequence_number: Default::default(),
            immediate_commit_timestamp: Default::default(),
            original_commit_timestamp: Default::default(),
            tx_length: Default::default(),
            original_server_version: Default::default(),
            immediate_server_version: Default::default(),
        }
    }

    /// Defines the `flags` value.
    pub fn with_flags(mut self, flags: GtidFlags) -> Self {
        self.flags = RawFlags::new(flags.bits());
        self
    }

    /// Returns the raw `flags` value.
    pub fn flags_raw(&self) -> u8 {
        self.flags.0
    }

    /// Returns the `flags` value. Unknown bits will be truncated.
    ///
    /// `00000001` – Transaction may have changes logged with SBR.
    ///
    /// In 5.6, 5.7.0-5.7.18, and 8.0.0-8.0.1, this flag is always set. Starting in 5.7.19 and
    /// 8.0.2, this flag is cleared if the transaction only contains row events.
    /// It is set if any part of the transaction is written in statement format.
    pub fn flags(&self) -> GtidFlags {
        self.flags.get()
    }

    /// Defines the `sid` value.
    pub fn with_sid(mut self, sid: [u8; Self::ENCODED_SID_LENGTH]) -> Self {
        self.sid = sid;
        self
    }

    /// Returns the `sid` value.
    ///
    /// `sid` is the UUID representing the SID.
    pub fn sid(&self) -> [u8; Self::ENCODED_SID_LENGTH] {
        self.sid
    }

    /// Defines the `gno` value.
    pub fn with_gno(mut self, gno: u64) -> Self {
        self.gno = RawConst::new(gno);
        self
    }

    /// Returns the `gno` value.
    ///
    /// `gno` is a group number, second component of GTID.
    pub fn gno(&self) -> u64 {
        self.gno.0
    }

    /// Returns the `lc_typecode` value.
    ///
    /// `lc_typecode` is the type of logical timestamp used in the logical clock fields.
    pub fn lc_typecode(&self) -> Option<u8> {
        self.lc_typecode.as_ref().map(|x| x.value())
    }

    /// Sets the `lc_typecode` value to [`GtidEvent::LOGICAL_TIMESTAMP_TYPECODE`].
    ///
    /// This is already by default, but `lc_typecode` might be `None` if `Self` is obtained
    /// from an old MySql server via [`MyDeserialize::deserialize`].
    pub fn with_lc_typecode(mut self) -> Self {
        self.lc_typecode = Some(LogicalTimestampTypecode::default());
        self
    }

    /// Sets the `last_committed` value.
    pub fn with_last_committed(mut self, last_committed: u64) -> Self {
        self.last_committed = RawInt::new(last_committed);
        self
    }

    /// Returns the `last_committed` value.
    ///
    /// `last_committed` stores the transaction's commit parent `sequence_number`.
    pub fn last_committed(&self) -> u64 {
        self.last_committed.0
    }

    /// Sets the `sequence_number` value.
    pub fn with_sequence_number(mut self, sequence_number: u64) -> Self {
        self.sequence_number = RawInt::new(sequence_number);
        self
    }

    /// Returns the `sequence_number` value.
    ///
    /// `sequence_number` is the transaction's logical timestamp assigned at prepare phase.
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number.0
    }

    /// Sets the `immediate_commit_timestamp` value.
    pub fn with_immediate_commit_timestamp(mut self, immediate_commit_timestamp: u64) -> Self {
        self.immediate_commit_timestamp = RawInt::new(immediate_commit_timestamp);
        self
    }

    /// Returns the `immediate_commit_timestamp` value.
    ///
    /// `immediate_commit_timestamp` is a timestamp of commit on the immediate master.
    pub fn immediate_commit_timestamp(&self) -> u64 {
        self.immediate_commit_timestamp.0
    }

    /// Sets the `original_commit_timestamp` value.
    pub fn with_original_commit_timestamp(mut self, original_commit_timestamp: u64) -> Self {
        self.original_commit_timestamp = RawInt::new(original_commit_timestamp);
        self
    }

    /// Returns the `original_commit_timestamp` value.
    ///
    /// `original_commit_timestamp` is the timestamp of commit on the originating master.
    pub fn original_commit_timestamp(&self) -> u64 {
        self.original_commit_timestamp.0
    }

    /// Sets the `tx_length` value.
    pub fn with_tx_length(mut self, tx_length: u64) -> Self {
        self.tx_length = RawInt::new(tx_length);
        self
    }

    /// Returns the `tx_length` value.
    ///
    /// `tx_length` is the packed transaction's length in bytes, including the Gtid.
    pub fn tx_length(&self) -> u64 {
        self.tx_length.0
    }

    /// Sets the `original_server_version` value.
    pub fn with_original_server_version(mut self, original_server_version: u32) -> Self {
        self.original_server_version = RawInt::new(original_server_version);
        self
    }

    /// Returns the `original_server_version` value.
    ///
    /// `original_server_version` is the version of the server where the transaction was originally
    /// executed.
    pub fn original_server_version(&self) -> u32 {
        self.original_server_version.0
    }

    /// Sets the `immediate_server_version` value.
    pub fn with_immediate_server_version(mut self, immediate_server_version: u32) -> Self {
        self.immediate_server_version = RawInt::new(immediate_server_version);
        self
    }

    /// Returns the `immediate_server_version` value.
    ///
    /// `immediate_server_version` is the server version of the immediate server.
    pub fn immediate_server_version(&self) -> u32 {
        self.immediate_server_version.0
    }
}

impl<'de> MyDeserialize<'de> for GtidEvent {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(1 + Self::ENCODED_SID_LENGTH + 8)?;
        let flags = sbuf.parse_unchecked(())?;
        let sid: [u8; Self::ENCODED_SID_LENGTH] = sbuf.parse_unchecked(())?;
        let gno = sbuf.parse_unchecked(())?;

        let mut lc_typecode = None;
        let mut last_committed = RawInt::new(0);
        let mut sequence_number = RawInt::new(0);
        let mut immediate_commit_timestamp = RawInt::new(0);
        let mut original_commit_timestamp = RawInt::new(0);
        let mut tx_length = RawInt::new(0);

        let mut original_server_version = RawInt::new(Self::UNDEFINED_SERVER_VERSION);
        let mut immediate_server_version = RawInt::new(Self::UNDEFINED_SERVER_VERSION);

        // Buf will be empty for MySql 5.6. Condition will be false for MySql <= 5.7.4
        if !buf.is_empty() && buf.0[0] == Self::LOGICAL_TIMESTAMP_TYPECODE {
            lc_typecode = Some(buf.parse_unchecked(())?);

            let mut sbuf: ParseBuf = buf.parse(16)?;
            last_committed = sbuf.parse_unchecked(())?;
            sequence_number = sbuf.parse_unchecked(())?;

            if buf.len() >= Self::IMMEDIATE_COMMIT_TIMESTAMP_LENGTH {
                immediate_commit_timestamp = buf.parse_unchecked(())?;
                if immediate_commit_timestamp.0 & (1 << 55) != 0 {
                    immediate_commit_timestamp.0 &= !(1 << 55);
                    original_commit_timestamp = buf.parse(())?;
                } else {
                    // The transaction originated in the previous server
                    original_commit_timestamp = immediate_commit_timestamp;
                }
            }

            if !buf.is_empty() {
                tx_length = buf.parse_unchecked(())?;
            }

            if buf.len() >= Self::IMMEDIATE_SERVER_VERSION_LENGTH {
                immediate_server_version = buf.parse_unchecked(())?;
                if immediate_server_version.0 & (1 << 31) != 0 {
                    immediate_server_version.0 &= !(1 << 31);
                    original_server_version = buf.parse(())?;
                } else {
                    original_server_version = immediate_server_version;
                }
            }
        }

        Ok(Self {
            flags,
            sid,
            gno,
            lc_typecode,
            last_committed,
            sequence_number,
            immediate_commit_timestamp,
            original_commit_timestamp,
            tx_length,
            original_server_version,
            immediate_server_version,
        })
    }
}

impl MySerialize for GtidEvent {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.flags.serialize(&mut *buf);
        self.sid.serialize(&mut *buf);
        self.gno.serialize(&mut *buf);
        match self.lc_typecode {
            Some(lc_typecode) => lc_typecode.serialize(&mut *buf),
            None => return,
        };
        self.last_committed.serialize(&mut *buf);
        self.sequence_number.serialize(&mut *buf);

        let mut immediate_commit_timestamp_with_flag = *self.immediate_commit_timestamp;
        if self.immediate_commit_timestamp != self.original_commit_timestamp {
            immediate_commit_timestamp_with_flag |= 1 << 55;
        } else {
            immediate_commit_timestamp_with_flag &= !(1 << 55);
        }
        RawInt::<LeU56>::new(immediate_commit_timestamp_with_flag).serialize(&mut *buf);

        if self.immediate_commit_timestamp != self.original_commit_timestamp {
            self.original_commit_timestamp.serialize(&mut *buf);
        }

        self.tx_length.serialize(&mut *buf);

        let mut immediate_server_version_with_flag = *self.immediate_server_version;
        if self.immediate_server_version != self.original_server_version {
            immediate_server_version_with_flag |= 1 << 31;
        } else {
            immediate_server_version_with_flag &= !(1 << 31);
        }
        RawInt::<LeU32>::new(immediate_server_version_with_flag).serialize(&mut *buf);

        if self.immediate_server_version != self.original_server_version {
            self.original_server_version.serialize(&mut *buf);
        }
    }
}

impl<'a> BinlogStruct<'a> for GtidEvent {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        // post header
        len += S(1); // flags
        len += S(Self::ENCODED_SID_LENGTH); // sid
        len += S(8); // gno
        len += S(1); // lc_typecode
        len += S(8); // last_committed
        len += S(8); // sequence_number

        len += S(7); // immediate_commit_timestamp
        if self.immediate_commit_timestamp != self.original_commit_timestamp {
            len += S(7); // original_commit_timestamp
        }

        len += S(crate::misc::lenenc_int_len(*self.tx_length) as usize); // tx_length
        len += S(4); // immediate_server_version
        if self.immediate_server_version != self.original_server_version {
            len += S(4); // original_server_version
        }

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}

impl<'a> BinlogEvent<'a> for GtidEvent {
    const EVENT_TYPE: EventType = EventType::GTID_EVENT;
}
