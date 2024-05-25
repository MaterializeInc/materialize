// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use btoi::btoi;
use bytes::BufMut;
use regex::bytes::Regex;
use smallvec::SmallVec;
use uuid::Uuid;

use std::str::FromStr;
use std::{
    borrow::Cow, cmp::max, collections::HashMap, convert::TryFrom, fmt, io, marker::PhantomData,
};

use crate::collations::CollationId;
use crate::{
    constants::{
        CapabilityFlags, ColumnFlags, ColumnType, Command, CursorType, SessionStateType,
        StatusFlags, StmtExecuteParamFlags, StmtExecuteParamsFlags, MAX_PAYLOAD_LEN,
    },
    io::{BufMutExt, ParseBuf},
    misc::{
        lenenc_str_len,
        raw::{
            bytes::{
                BareBytes, ConstBytes, ConstBytesValue, EofBytes, LenEnc, NullBytes, U32Bytes,
                U8Bytes,
            },
            int::{ConstU32, ConstU8, LeU16, LeU24, LeU32, LeU32LowerHalf, LeU32UpperHalf, LeU64},
            seq::Seq,
            Const, Either, RawBytes, RawConst, RawInt, Skip,
        },
        unexpected_buf_eof,
    },
    proto::{MyDeserialize, MySerialize},
    value::{ClientSide, SerializationSide, Value},
};

use self::session_state_change::SessionStateChange;

lazy_static::lazy_static! {
    static ref MARIADB_VERSION_RE: Regex =
        Regex::new(r"^(?:5.5.5-)?(\d{1,2})\.(\d{1,2})\.(\d{1,3})-MariaDB").unwrap();
    static ref VERSION_RE: Regex = Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})(.*)").unwrap();
}

macro_rules! define_header {
    ($name:ident, $err:ident($msg:literal), $val:literal) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
        #[error($msg)]
        pub struct $err;
        pub type $name = crate::misc::raw::int::ConstU8<$err, $val>;
    };
    ($name:ident, $cmd:ident, $err:ident) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
        #[error("Invalid header for {}", stringify!($cmd))]
        pub struct $err;
        pub type $name = crate::misc::raw::int::ConstU8<$err, { Command::$cmd as u8 }>;
    };
}

macro_rules! define_const {
    ($kind:ident, $name:ident, $err:ident($msg:literal), $val:literal) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
        #[error($msg)]
        pub struct $err;
        pub type $name = $kind<$err, $val>;
    };
}

macro_rules! define_const_bytes {
    ($vname:ident, $name:ident, $err:ident($msg:literal), $val:expr, $len:literal) => {
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
        #[error($msg)]
        pub struct $err;

        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $vname;

        impl ConstBytesValue<$len> for $vname {
            const VALUE: [u8; $len] = $val;
            type Error = $err;
        }

        pub type $name = ConstBytes<$vname, $len>;
    };
}

pub mod binlog_request;
pub mod caching_sha2_password;
pub mod session_state_change;

define_const_bytes!(
    Catalog,
    ColumnDefinitionCatalog,
    InvalidCatalog("Invalid catalog value in the column definition"),
    *b"\x03def",
    4
);

define_const!(
    ConstU8,
    FixedLengthFieldsLen,
    InvalidFixedLengthFieldsLen("Invalid fixed length field length in the column definition"),
    0x0c
);

/// Represents MySql Column (column packet).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Column {
    catalog: ColumnDefinitionCatalog,
    schema: SmallVec<[u8; 16]>,
    table: SmallVec<[u8; 16]>,
    org_table: SmallVec<[u8; 16]>,
    name: SmallVec<[u8; 16]>,
    org_name: SmallVec<[u8; 16]>,
    fixed_length_fields_len: FixedLengthFieldsLen,
    column_length: RawInt<LeU32>,
    character_set: RawInt<LeU16>,
    column_type: Const<ColumnType, u8>,
    flags: Const<ColumnFlags, LeU16>,
    decimals: RawInt<u8>,
    __filler: Skip<2>,
    // COM_FIELD_LIST is deprecated, so we won't support it
}

impl<'de> MyDeserialize<'de> for Column {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let catalog = buf.parse(())?;
        let schema = buf.parse_unchecked(())?;
        let table = buf.parse_unchecked(())?;
        let org_table = buf.parse_unchecked(())?;
        let name = buf.parse_unchecked(())?;
        let org_name = buf.parse_unchecked(())?;
        let mut buf: ParseBuf = buf.parse(13)?;

        Ok(Column {
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            fixed_length_fields_len: buf.parse_unchecked(())?,
            character_set: buf.parse_unchecked(())?,
            column_length: buf.parse_unchecked(())?,
            column_type: buf.parse_unchecked(())?,
            flags: buf.parse_unchecked(())?,
            decimals: buf.parse_unchecked(())?,
            __filler: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for Column {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.catalog.serialize(&mut *buf);
        self.schema.serialize(&mut *buf);
        self.table.serialize(&mut *buf);
        self.org_table.serialize(&mut *buf);
        self.name.serialize(&mut *buf);
        self.org_name.serialize(&mut *buf);
        self.fixed_length_fields_len.serialize(&mut *buf);
        self.column_length.serialize(&mut *buf);
        self.character_set.serialize(&mut *buf);
        self.column_type.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.decimals.serialize(&mut *buf);
        self.__filler.serialize(&mut *buf);
    }
}

impl Column {
    pub fn new(column_type: ColumnType) -> Self {
        Self {
            catalog: Default::default(),
            schema: Default::default(),
            table: Default::default(),
            org_table: Default::default(),
            name: Default::default(),
            org_name: Default::default(),
            fixed_length_fields_len: Default::default(),
            column_length: Default::default(),
            character_set: Default::default(),
            flags: Default::default(),
            column_type: Const::new(column_type),
            decimals: Default::default(),
            __filler: Skip,
        }
    }

    pub fn with_schema(mut self, schema: &[u8]) -> Self {
        self.schema = schema.into();
        self
    }

    pub fn with_table(mut self, table: &[u8]) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_org_table(mut self, org_table: &[u8]) -> Self {
        self.org_table = org_table.into();
        self
    }

    pub fn with_name(mut self, name: &[u8]) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_org_name(mut self, org_name: &[u8]) -> Self {
        self.org_name = org_name.into();
        self
    }

    pub fn with_flags(mut self, flags: ColumnFlags) -> Self {
        self.flags = Const::new(flags);
        self
    }

    pub fn with_column_length(mut self, column_length: u32) -> Self {
        self.column_length = RawInt::new(column_length);
        self
    }

    pub fn with_character_set(mut self, character_set: u16) -> Self {
        self.character_set = RawInt::new(character_set);
        self
    }

    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = RawInt::new(decimals);
        self
    }

    /// Returns value of the column_length field of a column packet.
    ///
    /// Can be used for text-output formatting.
    pub fn column_length(&self) -> u32 {
        *self.column_length
    }

    /// Returns value of the column_type field of a column packet.
    pub fn column_type(&self) -> ColumnType {
        *self.column_type
    }

    /// Returns value of the character_set field of a column packet.
    pub fn character_set(&self) -> u16 {
        *self.character_set
    }

    /// Returns value of the flags field of a column packet.
    pub fn flags(&self) -> ColumnFlags {
        *self.flags
    }

    /// Returns value of the decimals field of a column packet.
    ///
    /// Max shown decimal digits. Can be used for text-output formatting
    ///
    /// *   `0x00` for integers and static strings
    /// *   `0x1f` for dynamic strings, double, float
    /// *   `0x00..=0x51` for decimals
    pub fn decimals(&self) -> u8 {
        *self.decimals
    }

    /// Returns value of the schema field of a column packet as a byte slice.
    pub fn schema_ref(&self) -> &[u8] {
        &self.schema
    }

    /// Returns value of the schema field of a column packet as a string (lossy converted).
    pub fn schema_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.schema_ref())
    }

    /// Returns value of the table field of a column packet as a byte slice.
    pub fn table_ref(&self) -> &[u8] {
        &self.table
    }

    /// Returns value of the table field of a column packet as a string (lossy converted).
    pub fn table_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.table_ref())
    }

    /// Returns value of the org_table field of a column packet as a byte slice.
    ///
    /// "org_table" is for original table name.
    pub fn org_table_ref(&self) -> &[u8] {
        &self.org_table
    }

    /// Returns value of the org_table field of a column packet as a string (lossy converted).
    pub fn org_table_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.org_table_ref())
    }

    /// Returns value of the name field of a column packet as a byte slice.
    pub fn name_ref(&self) -> &[u8] {
        &self.name
    }

    /// Returns value of the name field of a column packet as a string (lossy converted).
    pub fn name_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.name_ref())
    }

    /// Returns value of the org_name field of a column packet as a byte slice.
    ///
    /// "org_name" is for original column name.
    pub fn org_name_ref(&self) -> &[u8] {
        &self.org_name
    }

    /// Returns value of the org_name field of a column packet as a string (lossy converted).
    pub fn org_name_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.org_name_ref())
    }
}

/// Represents change in session state (part of MySql's Ok packet).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SessionStateInfo<'a> {
    data_type: Const<SessionStateType, u8>,
    data: RawBytes<'a, LenEnc>,
}

impl<'a> SessionStateInfo<'a> {
    pub fn into_owned(self) -> SessionStateInfo<'static> {
        let SessionStateInfo { data_type, data } = self;
        SessionStateInfo {
            data_type,
            data: data.into_owned(),
        }
    }

    pub fn data_type(&self) -> SessionStateType {
        *self.data_type
    }

    /// Returns raw session state info data.
    pub fn data_ref(&self) -> &[u8] {
        self.data.as_bytes()
    }

    /// Tries to decode session state info data.
    pub fn decode(&self) -> io::Result<SessionStateChange<'_>> {
        ParseBuf(self.data.as_bytes()).parse_unchecked(*self.data_type)
    }
}

impl<'de> MyDeserialize<'de> for SessionStateInfo<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(SessionStateInfo {
            data_type: buf.parse(())?,
            data: buf.parse(())?,
        })
    }
}

impl MySerialize for SessionStateInfo<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.data_type.serialize(&mut *buf);
        self.data.serialize(buf);
    }
}

/// Represents MySql's Ok packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OkPacketBody<'a> {
    affected_rows: RawInt<LenEnc>,
    last_insert_id: RawInt<LenEnc>,
    status_flags: Const<StatusFlags, LeU16>,
    warnings: RawInt<LeU16>,
    info: RawBytes<'a, LenEnc>,
    session_state_info: RawBytes<'a, LenEnc>,
}

/// OK packet kind (see _OK packet identifier_ section of [WL#7766][1]).
///
/// [1]: https://dev.mysql.com/worklog/task/?id=7766
pub trait OkPacketKind {
    const HEADER: u8;

    fn parse_body<'de>(
        capabilities: CapabilityFlags,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<OkPacketBody<'de>>;
}

/// Ok packet that terminates a result set (text or binary).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ResultSetTerminator;

impl OkPacketKind for ResultSetTerminator {
    const HEADER: u8 = 0xFE;

    fn parse_body<'de>(
        capabilities: CapabilityFlags,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<OkPacketBody<'de>> {
        // We need to skip affected_rows and insert_id here
        // because valid content of EOF packet includes
        // packet marker, server status and warning count only.
        // (see `read_ok_ex` in sql-common/client.cc)
        buf.parse::<RawInt<LenEnc>>(())?;
        buf.parse::<RawInt<LenEnc>>(())?;

        // assume CLIENT_PROTOCOL_41 flag
        let mut sbuf: ParseBuf = buf.parse(4)?;
        let status_flags: Const<StatusFlags, LeU16> = sbuf.parse_unchecked(())?;
        let warnings = sbuf.parse_unchecked(())?;

        let (info, session_state_info) =
            if capabilities.contains(CapabilityFlags::CLIENT_SESSION_TRACK) && !buf.is_empty() {
                let info = buf.parse(())?;
                let session_state_info =
                    if status_flags.contains(StatusFlags::SERVER_SESSION_STATE_CHANGED) {
                        buf.parse(())?
                    } else {
                        RawBytes::default()
                    };
                (info, session_state_info)
            } else if !buf.is_empty() && buf.0[0] > 0 {
                // The `info` field is a `string<EOF>` according to the MySQL Internals
                // Manual, but actually it's a `string<lenenc>`.
                // SEE: sql/protocol_classics.cc `net_send_ok`
                let info = buf.parse(())?;
                (info, RawBytes::default())
            } else {
                (RawBytes::default(), RawBytes::default())
            };

        Ok(OkPacketBody {
            affected_rows: RawInt::new(0),
            last_insert_id: RawInt::new(0),
            status_flags,
            warnings,
            info,
            session_state_info,
        })
    }
}

/// Old deprecated EOF packet.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct OldEofPacket;

impl OkPacketKind for OldEofPacket {
    const HEADER: u8 = 0xFE;

    fn parse_body<'de>(
        _: CapabilityFlags,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<OkPacketBody<'de>> {
        // We assume that CLIENT_PROTOCOL_41 was set
        let mut buf: ParseBuf = buf.parse(4)?;
        let warnings = buf.parse_unchecked(())?;
        let status_flags = buf.parse_unchecked(())?;

        Ok(OkPacketBody {
            affected_rows: RawInt::new(0),
            last_insert_id: RawInt::new(0),
            status_flags,
            warnings,
            info: RawBytes::new(&[][..]),
            session_state_info: RawBytes::new(&[][..]),
        })
    }
}

/// This packet terminates a binlog network stream.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct NetworkStreamTerminator;

impl OkPacketKind for NetworkStreamTerminator {
    const HEADER: u8 = 0xFE;

    fn parse_body<'de>(
        flags: CapabilityFlags,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<OkPacketBody<'de>> {
        OldEofPacket::parse_body(flags, buf)
    }
}

/// Ok packet that is not a result set terminator.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct CommonOkPacket;

impl OkPacketKind for CommonOkPacket {
    const HEADER: u8 = 0x00;

    fn parse_body<'de>(
        capabilities: CapabilityFlags,
        buf: &mut ParseBuf<'de>,
    ) -> io::Result<OkPacketBody<'de>> {
        let affected_rows = buf.parse(())?;
        let last_insert_id = buf.parse(())?;

        // We assume that CLIENT_PROTOCOL_41 was set
        let mut sbuf: ParseBuf = buf.parse(4)?;
        let status_flags: Const<StatusFlags, LeU16> = sbuf.parse_unchecked(())?;
        let warnings = sbuf.parse_unchecked(())?;

        let (info, session_state_info) =
            if capabilities.contains(CapabilityFlags::CLIENT_SESSION_TRACK) && !buf.is_empty() {
                let info = buf.parse(())?;
                let session_state_info =
                    if status_flags.contains(StatusFlags::SERVER_SESSION_STATE_CHANGED) {
                        buf.parse(())?
                    } else {
                        RawBytes::default()
                    };
                (info, session_state_info)
            } else if !buf.is_empty() && buf.0[0] > 0 {
                // The `info` field is a `string<EOF>` according to the MySQL Internals
                // Manual, but actually it's a `string<lenenc>`.
                // SEE: sql/protocol_classics.cc `net_send_ok`
                let info = buf.parse(())?;
                (info, RawBytes::default())
            } else {
                (RawBytes::default(), RawBytes::default())
            };

        Ok(OkPacketBody {
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info,
            session_state_info,
        })
    }
}

impl<'a> TryFrom<OkPacketBody<'a>> for OkPacket<'a> {
    type Error = io::Error;

    fn try_from(body: OkPacketBody<'a>) -> io::Result<Self> {
        Ok(OkPacket {
            affected_rows: *body.affected_rows,
            last_insert_id: if *body.last_insert_id == 0 {
                None
            } else {
                Some(*body.last_insert_id)
            },
            status_flags: *body.status_flags,
            warnings: *body.warnings,
            info: if !body.info.is_empty() {
                Some(body.info)
            } else {
                None
            },
            session_state_info: if !body.session_state_info.is_empty() {
                Some(body.session_state_info)
            } else {
                None
            },
        })
    }
}

/// Represents MySql's Ok packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OkPacket<'a> {
    affected_rows: u64,
    last_insert_id: Option<u64>,
    status_flags: StatusFlags,
    warnings: u16,
    info: Option<RawBytes<'a, LenEnc>>,
    session_state_info: Option<RawBytes<'a, LenEnc>>,
}

impl<'a> OkPacket<'a> {
    pub fn into_owned(self) -> OkPacket<'static> {
        OkPacket {
            affected_rows: self.affected_rows,
            last_insert_id: self.last_insert_id,
            status_flags: self.status_flags,
            warnings: self.warnings,
            info: self.info.map(|x| x.into_owned()),
            session_state_info: self.session_state_info.map(|x| x.into_owned()),
        }
    }

    /// Value of the affected_rows field of an Ok packet.
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Value of the last_insert_id field of an Ok packet.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.last_insert_id
    }

    /// Value of the status_flags field of an Ok packet.
    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags
    }

    /// Value of the warnings field of an Ok packet.
    pub fn warnings(&self) -> u16 {
        self.warnings
    }

    /// Value of the info field of an Ok packet as a byte slice.
    pub fn info_ref(&self) -> Option<&[u8]> {
        self.info.as_ref().map(|x| x.as_bytes())
    }

    /// Value of the info field of an Ok packet as a string (lossy converted).
    pub fn info_str(&self) -> Option<Cow<str>> {
        self.info.as_ref().map(|x| x.as_str())
    }

    /// Returns raw reference to a session state info.
    pub fn session_state_info_ref(&self) -> Option<&[u8]> {
        self.session_state_info.as_ref().map(|x| x.as_bytes())
    }

    /// Tries to parse session state info, if any.
    pub fn session_state_info(&self) -> io::Result<Vec<SessionStateInfo<'_>>> {
        self.session_state_info_ref()
            .map(|data| {
                let mut data = ParseBuf(data);
                let mut entries = Vec::new();
                while !data.is_empty() {
                    entries.push(data.parse(())?);
                }
                Ok(entries)
            })
            .transpose()
            .map(|x| x.unwrap_or_default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkPacketDeserializer<'de, T>(OkPacket<'de>, PhantomData<T>);

impl<'de, T> OkPacketDeserializer<'de, T> {
    pub fn into_inner(self) -> OkPacket<'de> {
        self.0
    }
}

impl<'de, T> From<OkPacketDeserializer<'de, T>> for OkPacket<'de> {
    fn from(x: OkPacketDeserializer<'de, T>) -> Self {
        x.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Invalid OK packet header")]
pub struct InvalidOkPacketHeader;

impl<'de, T: OkPacketKind> MyDeserialize<'de> for OkPacketDeserializer<'de, T> {
    const SIZE: Option<usize> = None;
    type Ctx = CapabilityFlags;

    fn deserialize(capabilities: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        if *buf.parse::<RawInt<u8>>(())? == T::HEADER {
            let body = T::parse_body(capabilities, buf)?;
            let ok = OkPacket::try_from(body)?;
            Ok(Self(ok, PhantomData))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                InvalidOkPacketHeader,
            ))
        }
    }
}

/// Progress report information (may be in an error packet of MariaDB server).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProgressReport<'a> {
    stage: RawInt<u8>,
    max_stage: RawInt<u8>,
    progress: RawInt<LeU24>,
    stage_info: RawBytes<'a, LenEnc>,
}

impl<'a> ProgressReport<'a> {
    pub fn new(
        stage: u8,
        max_stage: u8,
        progress: u32,
        stage_info: impl Into<Cow<'a, [u8]>>,
    ) -> ProgressReport<'a> {
        ProgressReport {
            stage: RawInt::new(stage),
            max_stage: RawInt::new(max_stage),
            progress: RawInt::new(progress),
            stage_info: RawBytes::new(stage_info),
        }
    }

    /// 1 to max_stage
    pub fn stage(&self) -> u8 {
        *self.stage
    }

    pub fn max_stage(&self) -> u8 {
        *self.max_stage
    }

    /// Progress as '% * 1000'
    pub fn progress(&self) -> u32 {
        *self.progress
    }

    /// Status or state name as a byte slice.
    pub fn stage_info_ref(&self) -> &[u8] {
        self.stage_info.as_bytes()
    }

    /// Status or state name as a string (lossy converted).
    pub fn stage_info_str(&self) -> Cow<'_, str> {
        self.stage_info.as_str()
    }

    pub fn into_owned(self) -> ProgressReport<'static> {
        ProgressReport {
            stage: self.stage,
            max_stage: self.max_stage,
            progress: self.progress,
            stage_info: self.stage_info.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for ProgressReport<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(6)?;

        sbuf.skip(1); // Ignore number of strings.

        Ok(ProgressReport {
            stage: sbuf.parse_unchecked(())?,
            max_stage: sbuf.parse_unchecked(())?,
            progress: sbuf.parse_unchecked(())?,
            stage_info: buf.parse(())?,
        })
    }
}

impl MySerialize for ProgressReport<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(1);
        self.stage.serialize(&mut *buf);
        self.max_stage.serialize(&mut *buf);
        self.progress.serialize(&mut *buf);
        self.stage_info.serialize(buf);
    }
}

impl<'a> fmt::Display for ProgressReport<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stage: {} of {} '{}'  {:.2}% of stage done",
            self.stage(),
            self.max_stage(),
            self.progress(),
            self.stage_info_str()
        )
    }
}

define_header!(
    ErrPacketHeader,
    InvalidErrPacketHeader("Invalid error packet header"),
    0xFF
);

/// MySql error packet.
///
/// May hold an error or a progress report.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrPacket<'a> {
    Error(ServerError<'a>),
    Progress(ProgressReport<'a>),
}

impl<'a> ErrPacket<'a> {
    /// Returns false if this error packet contains progress report.
    pub fn is_error(&self) -> bool {
        matches!(self, ErrPacket::Error { .. })
    }

    /// Returns true if this error packet contains progress report.
    pub fn is_progress_report(&self) -> bool {
        !self.is_error()
    }

    /// Will panic if ErrPacket does not contains progress report
    pub fn progress_report(&self) -> &ProgressReport<'_> {
        match *self {
            ErrPacket::Progress(ref progress_report) => progress_report,
            _ => panic!("This ErrPacket does not contains progress report"),
        }
    }

    /// Will panic if ErrPacket does not contains a `ServerError`.
    pub fn server_error(&self) -> &ServerError<'_> {
        match self {
            ErrPacket::Error(error) => error,
            ErrPacket::Progress(_) => panic!("This ErrPacket does not contain a ServerError"),
        }
    }
}

impl<'de> MyDeserialize<'de> for ErrPacket<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = CapabilityFlags;

    fn deserialize(capabilities: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(3)?;
        sbuf.parse_unchecked::<ErrPacketHeader>(())?;
        let code: RawInt<LeU16> = sbuf.parse_unchecked(())?;

        if *code == 0xFFFF && capabilities.contains(CapabilityFlags::CLIENT_PROGRESS_OBSOLETE) {
            buf.parse(()).map(ErrPacket::Progress)
        } else {
            buf.parse((
                *code,
                capabilities.contains(CapabilityFlags::CLIENT_PROTOCOL_41),
            ))
            .map(ErrPacket::Error)
        }
    }
}

impl MySerialize for ErrPacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        ErrPacketHeader::new().serialize(&mut *buf);
        match self {
            ErrPacket::Error(server_error) => {
                server_error.code.serialize(&mut *buf);
                server_error.serialize(buf);
            }
            ErrPacket::Progress(progress_report) => {
                RawInt::<LeU16>::new(0xFFFF).serialize(&mut *buf);
                progress_report.serialize(buf);
            }
        }
    }
}

impl<'a> fmt::Display for ErrPacket<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrPacket::Error(server_error) => write!(f, "{}", server_error),
            ErrPacket::Progress(progress_report) => write!(f, "{}", progress_report),
        }
    }
}

define_header!(
    SqlStateMarker,
    InvalidSqlStateMarker("Invalid SqlStateMarker value"),
    b'#'
);

/// MySql error state.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SqlState {
    __state_marker: SqlStateMarker,
    state: [u8; 5],
}

impl SqlState {
    /// Creates new sql state.
    pub fn new(state: [u8; 5]) -> Self {
        Self {
            __state_marker: SqlStateMarker::new(),
            state,
        }
    }

    /// Returns an sql state as bytes.
    pub fn as_bytes(&self) -> [u8; 5] {
        self.state
    }

    /// Returns an sql state as a string (lossy converted).
    pub fn as_str(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.state)
    }
}

impl<'de> MyDeserialize<'de> for SqlState {
    const SIZE: Option<usize> = Some(6);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __state_marker: buf.parse(())?,
            state: buf.parse(())?,
        })
    }
}

impl MySerialize for SqlState {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__state_marker.serialize(buf);
        self.state.serialize(buf);
    }
}

/// MySql error packet.
///
/// May hold an error or a progress report.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerError<'a> {
    code: RawInt<LeU16>,
    state: Option<SqlState>,
    message: RawBytes<'a, EofBytes>,
}

impl<'a> ServerError<'a> {
    pub fn new(code: u16, state: Option<SqlState>, msg: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            code: RawInt::new(code),
            state,
            message: RawBytes::new(msg),
        }
    }

    /// Returns an error code.
    pub fn error_code(&self) -> u16 {
        *self.code
    }

    /// Returns an sql state.
    pub fn sql_state_ref(&self) -> Option<&SqlState> {
        self.state.as_ref()
    }

    /// Returns an error message.
    pub fn message_ref(&self) -> &[u8] {
        self.message.as_bytes()
    }

    /// Returns an error message as a string (lossy converted).
    pub fn message_str(&self) -> Cow<'_, str> {
        self.message.as_str()
    }

    pub fn into_owned(self) -> ServerError<'static> {
        ServerError {
            code: self.code,
            state: self.state,
            message: self.message.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for ServerError<'de> {
    const SIZE: Option<usize> = None;
    /// An error packet error code + whether CLIENT_PROTOCOL_41 capability was negotiated.
    type Ctx = (u16, bool);

    fn deserialize((code, protocol_41): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let server_error = if protocol_41 {
            ServerError {
                code: RawInt::new(code),
                state: Some(buf.parse(())?),
                message: buf.parse(())?,
            }
        } else {
            ServerError {
                code: RawInt::new(code),
                state: None,
                message: buf.parse(())?,
            }
        };
        Ok(server_error)
    }
}

impl MySerialize for ServerError<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        if let Some(state) = &self.state {
            state.serialize(buf);
        }
        self.message.serialize(buf);
    }
}

impl fmt::Display for ServerError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sql_state_str = self
            .sql_state_ref()
            .map(|s| format!(" ({})", s.as_str()))
            .unwrap_or_default();

        write!(
            f,
            "ERROR {}{}: {}",
            self.error_code(),
            sql_state_str,
            self.message_str()
        )
    }
}

define_header!(
    LocalInfileHeader,
    InvalidLocalInfileHeader("Invalid LOCAL_INFILE header"),
    0xFB
);

/// Represents MySql's local infile packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalInfilePacket<'a> {
    __header: LocalInfileHeader,
    file_name: RawBytes<'a, EofBytes>,
}

impl<'a> LocalInfilePacket<'a> {
    pub fn new(file_name: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            __header: LocalInfileHeader::new(),
            file_name: RawBytes::new(file_name),
        }
    }

    /// Value of the file_name field of a local infile packet as a byte slice.
    pub fn file_name_ref(&self) -> &[u8] {
        self.file_name.as_bytes()
    }

    /// Value of the file_name field of a local infile packet as a string (lossy converted).
    pub fn file_name_str(&self) -> Cow<'_, str> {
        self.file_name.as_str()
    }

    pub fn into_owned(self) -> LocalInfilePacket<'static> {
        LocalInfilePacket {
            __header: self.__header,
            file_name: self.file_name.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for LocalInfilePacket<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(LocalInfilePacket {
            __header: buf.parse(())?,
            file_name: buf.parse(())?,
        })
    }
}

impl MySerialize for LocalInfilePacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(buf);
        self.file_name.serialize(buf);
    }
}

const MYSQL_OLD_PASSWORD_PLUGIN_NAME: &[u8] = b"mysql_old_password";
const MYSQL_NATIVE_PASSWORD_PLUGIN_NAME: &[u8] = b"mysql_native_password";
const CACHING_SHA2_PASSWORD_PLUGIN_NAME: &[u8] = b"caching_sha2_password";
const MYSQL_CLEAR_PASSWORD_PLUGIN_NAME: &[u8] = b"mysql_clear_password";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthPluginData<'a> {
    /// Auth data for the `mysql_old_password` plugin.
    Old([u8; 8]),
    /// Auth data for the `mysql_native_password` plugin.
    Native([u8; 20]),
    /// Auth data for `sha2_password` and `caching_sha2_password` plugins.
    Sha2([u8; 32]),
    /// Clear password for `mysql_clear_password` plugin.
    Clear(Cow<'a, [u8]>),
}

impl<'a> AuthPluginData<'a> {
    pub fn into_owned(self) -> AuthPluginData<'static> {
        match self {
            AuthPluginData::Old(x) => AuthPluginData::Old(x),
            AuthPluginData::Native(x) => AuthPluginData::Native(x),
            AuthPluginData::Sha2(x) => AuthPluginData::Sha2(x),
            AuthPluginData::Clear(x) => AuthPluginData::Clear(Cow::Owned(x.into_owned())),
        }
    }
}

impl std::ops::Deref for AuthPluginData<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Sha2(x) => &x[..],
            Self::Native(x) => &x[..],
            Self::Old(x) => &x[..],
            Self::Clear(x) => &x[..],
        }
    }
}

impl MySerialize for AuthPluginData<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Sha2(x) => buf.put_slice(&x[..]),
            Self::Native(x) => buf.put_slice(&x[..]),
            Self::Old(x) => {
                buf.put_slice(&x[..]);
                buf.push(0);
            }
            Self::Clear(x) => {
                buf.put_slice(x);
                buf.push(0);
            }
        }
    }
}

/// Authentication plugin
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum AuthPlugin<'a> {
    /// Old Password Authentication
    MysqlOldPassword,
    /// Client-Side Cleartext Pluggable Authentication
    MysqlClearPassword,
    /// Legacy authentication plugin
    MysqlNativePassword,
    /// Default since MySql v8.0.4
    CachingSha2Password,
    Other(Cow<'a, [u8]>),
}

impl<'de> MyDeserialize<'de> for AuthPlugin<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self::from_bytes(buf.eat_all()))
    }
}

impl MySerialize for AuthPlugin<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(self.as_bytes());
        buf.put_u8(0);
    }
}

impl<'a> AuthPlugin<'a> {
    pub fn from_bytes(name: &'a [u8]) -> AuthPlugin<'a> {
        let name = if let [name @ .., 0] = name {
            name
        } else {
            name
        };
        match name {
            CACHING_SHA2_PASSWORD_PLUGIN_NAME => AuthPlugin::CachingSha2Password,
            MYSQL_NATIVE_PASSWORD_PLUGIN_NAME => AuthPlugin::MysqlNativePassword,
            MYSQL_OLD_PASSWORD_PLUGIN_NAME => AuthPlugin::MysqlOldPassword,
            MYSQL_CLEAR_PASSWORD_PLUGIN_NAME => AuthPlugin::MysqlClearPassword,
            name => AuthPlugin::Other(Cow::Borrowed(name)),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            AuthPlugin::CachingSha2Password => CACHING_SHA2_PASSWORD_PLUGIN_NAME,
            AuthPlugin::MysqlNativePassword => MYSQL_NATIVE_PASSWORD_PLUGIN_NAME,
            AuthPlugin::MysqlOldPassword => MYSQL_OLD_PASSWORD_PLUGIN_NAME,
            AuthPlugin::MysqlClearPassword => MYSQL_CLEAR_PASSWORD_PLUGIN_NAME,
            AuthPlugin::Other(name) => name,
        }
    }

    pub fn into_owned(self) -> AuthPlugin<'static> {
        match self {
            AuthPlugin::CachingSha2Password => AuthPlugin::CachingSha2Password,
            AuthPlugin::MysqlNativePassword => AuthPlugin::MysqlNativePassword,
            AuthPlugin::MysqlOldPassword => AuthPlugin::MysqlOldPassword,
            AuthPlugin::MysqlClearPassword => AuthPlugin::MysqlClearPassword,
            AuthPlugin::Other(name) => AuthPlugin::Other(Cow::Owned(name.into_owned())),
        }
    }

    pub fn borrow(&self) -> AuthPlugin<'_> {
        match self {
            AuthPlugin::CachingSha2Password => AuthPlugin::CachingSha2Password,
            AuthPlugin::MysqlNativePassword => AuthPlugin::MysqlNativePassword,
            AuthPlugin::MysqlOldPassword => AuthPlugin::MysqlOldPassword,
            AuthPlugin::MysqlClearPassword => AuthPlugin::MysqlClearPassword,
            AuthPlugin::Other(name) => AuthPlugin::Other(Cow::Borrowed(name.as_ref())),
        }
    }

    /// Generates auth plugin data for this plugin.
    ///
    /// It'll generate `None` if password is `None` or empty.
    ///
    /// Note, that you should trim terminating null character from the `nonce`.
    pub fn gen_data<'b>(&self, pass: Option<&'b str>, nonce: &[u8]) -> Option<AuthPluginData<'b>> {
        use super::scramble::{scramble_323, scramble_native, scramble_sha256};

        match pass {
            Some(pass) if !pass.is_empty() => match self {
                AuthPlugin::CachingSha2Password => {
                    scramble_sha256(nonce, pass.as_bytes()).map(AuthPluginData::Sha2)
                }
                AuthPlugin::MysqlNativePassword => {
                    scramble_native(nonce, pass.as_bytes()).map(AuthPluginData::Native)
                }
                AuthPlugin::MysqlOldPassword => {
                    scramble_323(nonce.chunks(8).next().unwrap(), pass.as_bytes())
                        .map(AuthPluginData::Old)
                }
                AuthPlugin::MysqlClearPassword => {
                    Some(AuthPluginData::Clear(Cow::Borrowed(pass.as_bytes())))
                }
                AuthPlugin::Other(_) => None,
            },
            _ => None,
        }
    }
}

define_header!(
    AuthMoreDataHeader,
    InvalidAuthMoreDataHeader("Invalid AuthMoreData header"),
    0x01
);

/// Extra auth-data beyond the initial challenge.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthMoreData<'a> {
    __header: AuthMoreDataHeader,
    data: RawBytes<'a, EofBytes>,
}

impl<'a> AuthMoreData<'a> {
    pub fn new(data: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            __header: AuthMoreDataHeader::new(),
            data: RawBytes::new(data),
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_bytes()
    }

    pub fn into_owned(self) -> AuthMoreData<'static> {
        AuthMoreData {
            __header: self.__header,
            data: self.data.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for AuthMoreData<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
            data: buf.parse(())?,
        })
    }
}

impl MySerialize for AuthMoreData<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
        self.data.serialize(buf);
    }
}

define_header!(
    PublicKeyResponseHeader,
    InvalidPublicKeyResponse("Invalid PublicKeyResponse header"),
    0x01
);

/// A server response to a [`PublicKeyRequest`] containing a public RSA key for authentication protection.
///
/// [`PublicKeyRequest`]: crate::packets::caching_sha2_password::PublicKeyRequest
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublicKeyResponse<'a> {
    __header: PublicKeyResponseHeader,
    rsa_key: RawBytes<'a, EofBytes>,
}

impl<'a> PublicKeyResponse<'a> {
    pub fn new(rsa_key: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            __header: PublicKeyResponseHeader::new(),
            rsa_key: RawBytes::new(rsa_key),
        }
    }

    /// The server's RSA public key in PEM format.
    pub fn rsa_key(&self) -> Cow<'_, str> {
        self.rsa_key.as_str()
    }
}

impl<'de> MyDeserialize<'de> for PublicKeyResponse<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
            rsa_key: buf.parse(())?,
        })
    }
}

impl MySerialize for PublicKeyResponse<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
        self.rsa_key.serialize(buf);
    }
}

define_header!(
    AuthSwitchRequestHeader,
    InvalidAuthSwithRequestHeader("Invalid auth switch request header"),
    0xFE
);

/// Old Authentication Method Switch Request Packet.
///
/// Used for It is sent by server to request client to switch to Old Password Authentication
/// if `CLIENT_PLUGIN_AUTH` capability is not supported (by either the client or the server).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OldAuthSwitchRequest {
    __header: AuthSwitchRequestHeader,
}

impl OldAuthSwitchRequest {
    pub fn new() -> Self {
        Self {
            __header: AuthSwitchRequestHeader::new(),
        }
    }

    pub const fn auth_plugin(&self) -> AuthPlugin<'static> {
        AuthPlugin::MysqlOldPassword
    }
}

impl Default for OldAuthSwitchRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de> MyDeserialize<'de> for OldAuthSwitchRequest {
    const SIZE: Option<usize> = Some(1);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
        })
    }
}

impl MySerialize for OldAuthSwitchRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
    }
}

/// Authentication Method Switch Request Packet.
///
/// If both server and client support `CLIENT_PLUGIN_AUTH` capability, server can send this packet
/// to ask client to use another authentication method.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthSwitchRequest<'a> {
    __header: AuthSwitchRequestHeader,
    auth_plugin: RawBytes<'a, NullBytes>,
    plugin_data: RawBytes<'a, EofBytes>,
}

impl<'a> AuthSwitchRequest<'a> {
    pub fn new(
        auth_plugin: impl Into<Cow<'a, [u8]>>,
        plugin_data: impl Into<Cow<'a, [u8]>>,
    ) -> Self {
        Self {
            __header: AuthSwitchRequestHeader::new(),
            auth_plugin: RawBytes::new(auth_plugin),
            plugin_data: RawBytes::new(plugin_data),
        }
    }

    pub fn auth_plugin(&self) -> AuthPlugin<'_> {
        ParseBuf(self.auth_plugin.as_bytes())
            .parse(())
            .expect("infallible")
    }

    pub fn plugin_data(&self) -> &[u8] {
        match self.plugin_data.as_bytes() {
            [head @ .., 0] => head,
            all => all,
        }
    }

    pub fn into_owned(self) -> AuthSwitchRequest<'static> {
        AuthSwitchRequest {
            __header: self.__header,
            auth_plugin: self.auth_plugin.into_owned(),
            plugin_data: self.plugin_data.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for AuthSwitchRequest<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
            auth_plugin: buf.parse(())?,
            plugin_data: buf.parse(())?,
        })
    }
}

impl MySerialize for AuthSwitchRequest<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
        self.auth_plugin.serialize(&mut *buf);
        self.plugin_data.serialize(buf);
    }
}

/// Represents MySql's initial handshake packet.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HandshakePacket<'a> {
    protocol_version: RawInt<u8>,
    server_version: RawBytes<'a, NullBytes>,
    connection_id: RawInt<LeU32>,
    scramble_1: [u8; 8],
    __filler: Skip<1>,
    // lower 16 bytes
    capabilities_1: Const<CapabilityFlags, LeU32LowerHalf>,
    default_collation: RawInt<u8>,
    status_flags: Const<StatusFlags, LeU16>,
    // upper 16 bytes
    capabilities_2: Const<CapabilityFlags, LeU32UpperHalf>,
    auth_plugin_data_len: RawInt<u8>,
    __reserved: Skip<10>,
    scramble_2: Option<RawBytes<'a, BareBytes<{ (u8::MAX as usize) - 8 }>>>,
    auth_plugin_name: Option<RawBytes<'a, NullBytes>>,
}

impl<'de> MyDeserialize<'de> for HandshakePacket<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let protocol_version = buf.parse(())?;
        let server_version = buf.parse(())?;

        // includes trailing 10 bytes filler
        let mut sbuf: ParseBuf = buf.parse(31)?;
        let connection_id = sbuf.parse_unchecked(())?;
        let scramble_1 = sbuf.parse_unchecked(())?;
        let __filler = sbuf.parse_unchecked(())?;
        let capabilities_1: RawConst<LeU32LowerHalf, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let default_collation = sbuf.parse_unchecked(())?;
        let status_flags = sbuf.parse_unchecked(())?;
        let capabilities_2: RawConst<LeU32UpperHalf, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let auth_plugin_data_len: RawInt<u8> = sbuf.parse_unchecked(())?;
        let __reserved = sbuf.parse_unchecked(())?;
        let mut scramble_2 = None;
        if capabilities_1.0 & CapabilityFlags::CLIENT_SECURE_CONNECTION.bits() > 0 {
            let len = max(13, auth_plugin_data_len.0 as i8 - 8) as usize;
            scramble_2 = buf.parse(len).map(Some)?;
        }
        let mut auth_plugin_name = None;
        if capabilities_2.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH.bits() > 0 {
            auth_plugin_name = match buf.eat_all() {
                [head @ .., 0] => Some(RawBytes::new(head)),
                // missing trailing `0` is a known bug in mysql
                all => Some(RawBytes::new(all)),
            }
        }

        Ok(Self {
            protocol_version,
            server_version,
            connection_id,
            scramble_1,
            __filler,
            capabilities_1: Const::new(CapabilityFlags::from_bits_truncate(capabilities_1.0)),
            default_collation,
            status_flags,
            capabilities_2: Const::new(CapabilityFlags::from_bits_truncate(capabilities_2.0)),
            auth_plugin_data_len,
            __reserved,
            scramble_2,
            auth_plugin_name,
        })
    }
}

impl MySerialize for HandshakePacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.protocol_version.serialize(&mut *buf);
        self.server_version.serialize(&mut *buf);
        self.connection_id.serialize(&mut *buf);
        self.scramble_1.serialize(&mut *buf);
        buf.put_u8(0x00);
        self.capabilities_1.serialize(&mut *buf);
        self.default_collation.serialize(&mut *buf);
        self.status_flags.serialize(&mut *buf);
        self.capabilities_2.serialize(&mut *buf);

        if self
            .capabilities_2
            .contains(CapabilityFlags::CLIENT_PLUGIN_AUTH)
        {
            buf.put_u8(
                self.scramble_2
                    .as_ref()
                    .map(|x| (x.len() + 8) as u8)
                    .unwrap_or_default(),
            );
        } else {
            buf.put_u8(0);
        }

        buf.put_slice(&[0_u8; 10][..]);

        // Assume that the packet is well formed:
        // * the CLIENT_SECURE_CONNECTION is set.
        if let Some(scramble_2) = &self.scramble_2 {
            scramble_2.serialize(&mut *buf);
        }

        // Assume that the packet is well formed:
        // * the CLIENT_PLUGIN_AUTH is set.
        if let Some(client_plugin_auth) = &self.auth_plugin_name {
            client_plugin_auth.serialize(buf);
        }
    }
}

impl<'a> HandshakePacket<'a> {
    pub fn new(
        protocol_version: u8,
        server_version: impl Into<Cow<'a, [u8]>>,
        connection_id: u32,
        scramble_1: [u8; 8],
        scramble_2: Option<impl Into<Cow<'a, [u8]>>>,
        capabilities: CapabilityFlags,
        default_collation: u8,
        status_flags: StatusFlags,
        auth_plugin_name: Option<impl Into<Cow<'a, [u8]>>>,
    ) -> Self {
        // Safety:
        // * capabilities are given as a valid CapabilityFlags instance
        // * the BitAnd operation can't set new bits
        let (capabilities_1, capabilities_2) = (
            CapabilityFlags::from_bits_retain(capabilities.bits() & 0x0000_FFFF),
            CapabilityFlags::from_bits_retain(capabilities.bits() & 0xFFFF_0000),
        );

        let scramble_2 = scramble_2.map(RawBytes::new);

        HandshakePacket {
            protocol_version: RawInt::new(protocol_version),
            server_version: RawBytes::new(server_version),
            connection_id: RawInt::new(connection_id),
            scramble_1,
            __filler: Skip,
            capabilities_1: Const::new(capabilities_1),
            default_collation: RawInt::new(default_collation),
            status_flags: Const::new(status_flags),
            capabilities_2: Const::new(capabilities_2),
            auth_plugin_data_len: RawInt::new(
                scramble_2
                    .as_ref()
                    .map(|x| x.len() as u8)
                    .unwrap_or_default(),
            ),
            __reserved: Skip,
            scramble_2,
            auth_plugin_name: auth_plugin_name.map(RawBytes::new),
        }
    }

    pub fn into_owned(self) -> HandshakePacket<'static> {
        HandshakePacket {
            protocol_version: self.protocol_version,
            server_version: self.server_version.into_owned(),
            connection_id: self.connection_id,
            scramble_1: self.scramble_1,
            __filler: self.__filler,
            capabilities_1: self.capabilities_1,
            default_collation: self.default_collation,
            status_flags: self.status_flags,
            capabilities_2: self.capabilities_2,
            auth_plugin_data_len: self.auth_plugin_data_len,
            __reserved: self.__reserved,
            scramble_2: self.scramble_2.map(|x| x.into_owned()),
            auth_plugin_name: self.auth_plugin_name.map(RawBytes::into_owned),
        }
    }

    /// Value of the protocol_version field of an initial handshake packet.
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version.0
    }

    /// Value of the server_version field of an initial handshake packet as a byte slice.
    pub fn server_version_ref(&self) -> &[u8] {
        self.server_version.as_bytes()
    }

    /// Value of the server_version field of an initial handshake packet as a string
    /// (lossy converted).
    pub fn server_version_str(&self) -> Cow<'_, str> {
        self.server_version.as_str()
    }

    /// Parsed server version.
    ///
    /// Will parse first \d+.\d+.\d+ of a server version string (if any).
    pub fn server_version_parsed(&self) -> Option<(u16, u16, u16)> {
        VERSION_RE
            .captures(self.server_version_ref())
            .map(|captures| {
                // Should not panic because validated with regex
                (
                    btoi::<u16>(captures.get(1).unwrap().as_bytes()).unwrap(),
                    btoi::<u16>(captures.get(2).unwrap().as_bytes()).unwrap(),
                    btoi::<u16>(captures.get(3).unwrap().as_bytes()).unwrap(),
                )
            })
    }

    /// Parsed mariadb server version.
    pub fn maria_db_server_version_parsed(&self) -> Option<(u16, u16, u16)> {
        MARIADB_VERSION_RE
            .captures(self.server_version_ref())
            .map(|captures| {
                // Should not panic because validated with regex
                (
                    btoi::<u16>(captures.get(1).unwrap().as_bytes()).unwrap(),
                    btoi::<u16>(captures.get(2).unwrap().as_bytes()).unwrap(),
                    btoi::<u16>(captures.get(3).unwrap().as_bytes()).unwrap(),
                )
            })
    }

    /// Value of the connection_id field of an initial handshake packet.
    pub fn connection_id(&self) -> u32 {
        self.connection_id.0
    }

    /// Value of the scramble_1 field of an initial handshake packet as a byte slice.
    pub fn scramble_1_ref(&self) -> &[u8] {
        self.scramble_1.as_ref()
    }

    /// Value of the scramble_2 field of an initial handshake packet as a byte slice.
    ///
    /// Note that this may include a terminating null character.
    pub fn scramble_2_ref(&self) -> Option<&[u8]> {
        self.scramble_2.as_ref().map(|x| x.as_bytes())
    }

    /// Returns concatenated auth plugin nonce.
    pub fn nonce(&self) -> Vec<u8> {
        let mut out = Vec::from(self.scramble_1_ref());
        out.extend_from_slice(self.scramble_2_ref().unwrap_or(&[][..]));

        // Trim zero terminator. Fill with zeroes if nonce
        // is somehow smaller than 20 bytes.
        out.resize(20, 0);
        out
    }

    /// Value of a server capabilities.
    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities_1.0 | self.capabilities_2.0
    }

    /// Value of the default_collation field of an initial handshake packet.
    pub fn default_collation(&self) -> u8 {
        self.default_collation.0
    }

    /// Value of a status flags.
    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags.0
    }

    /// Value of the auth_plugin_name field of an initial handshake packet as a byte slice.
    pub fn auth_plugin_name_ref(&self) -> Option<&[u8]> {
        self.auth_plugin_name.as_ref().map(|x| x.as_bytes())
    }

    /// Value of the auth_plugin_name field of an initial handshake packet as a string
    /// (lossy converted).
    pub fn auth_plugin_name_str(&self) -> Option<Cow<'_, str>> {
        self.auth_plugin_name.as_ref().map(|x| x.as_str())
    }

    /// Auth plugin of a handshake packet
    pub fn auth_plugin(&self) -> Option<AuthPlugin<'_>> {
        self.auth_plugin_name.as_ref().map(|x| match x.as_bytes() {
            [name @ .., 0] => ParseBuf(name).parse_unchecked(()).expect("infallible"),
            all => ParseBuf(all).parse_unchecked(()).expect("infallible"),
        })
    }
}

define_header!(
    ComChangeUserHeader,
    InvalidComChangeUserHeader("Invalid COM_CHANGE_USER header"),
    0x11
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComChangeUser<'a> {
    __header: ComChangeUserHeader,
    user: RawBytes<'a, NullBytes>,
    // Only CLIENT_SECURE_CONNECTION capable servers are supported
    auth_plugin_data: RawBytes<'a, U8Bytes>,
    database: RawBytes<'a, NullBytes>,
    more_data: Option<ComChangeUserMoreData<'a>>,
}

impl<'a> ComChangeUser<'a> {
    pub fn new() -> Self {
        Self {
            __header: ComChangeUserHeader::new(),
            user: Default::default(),
            auth_plugin_data: Default::default(),
            database: Default::default(),
            more_data: None,
        }
    }

    pub fn with_user(mut self, user: Option<impl Into<Cow<'a, [u8]>>>) -> Self {
        self.user = user.map(RawBytes::new).unwrap_or_default();
        self
    }

    pub fn with_database(mut self, database: Option<impl Into<Cow<'a, [u8]>>>) -> Self {
        self.database = database.map(RawBytes::new).unwrap_or_default();
        self
    }

    pub fn with_auth_plugin_data(
        mut self,
        auth_plugin_data: Option<impl Into<Cow<'a, [u8]>>>,
    ) -> Self {
        self.auth_plugin_data = auth_plugin_data.map(RawBytes::new).unwrap_or_default();
        self
    }

    pub fn with_more_data(mut self, more_data: Option<ComChangeUserMoreData<'a>>) -> Self {
        self.more_data = more_data;
        self
    }

    pub fn into_owned(self) -> ComChangeUser<'static> {
        ComChangeUser {
            __header: self.__header,
            user: self.user.into_owned(),
            auth_plugin_data: self.auth_plugin_data.into_owned(),
            database: self.database.into_owned(),
            more_data: self.more_data.map(|x| x.into_owned()),
        }
    }
}

impl<'de> MyDeserialize<'de> for ComChangeUser<'de> {
    const SIZE: Option<usize> = None;

    type Ctx = CapabilityFlags;

    fn deserialize(flags: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            __header: buf.parse(())?,
            user: buf.parse(())?,
            auth_plugin_data: buf.parse(())?,
            database: buf.parse(())?,
            more_data: if !buf.is_empty() {
                Some(buf.parse(flags)?)
            } else {
                None
            },
        })
    }
}

impl MySerialize for ComChangeUser<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
        self.user.serialize(&mut *buf);
        self.auth_plugin_data.serialize(&mut *buf);
        self.database.serialize(&mut *buf);
        if let Some(ref more_data) = self.more_data {
            more_data.serialize(&mut *buf);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComChangeUserMoreData<'a> {
    character_set: RawInt<LeU16>,
    auth_plugin: Option<AuthPlugin<'a>>,
    connect_attributes: Option<HashMap<RawBytes<'a, LenEnc>, RawBytes<'a, LenEnc>>>,
}

impl<'a> ComChangeUserMoreData<'a> {
    pub fn new(character_set: u16) -> Self {
        Self {
            character_set: RawInt::new(character_set),
            auth_plugin: None,
            connect_attributes: None,
        }
    }

    pub fn with_auth_plugin(mut self, auth_plugin: Option<AuthPlugin<'a>>) -> Self {
        self.auth_plugin = auth_plugin;
        self
    }

    pub fn with_connect_attributes(
        mut self,
        connect_attributes: Option<HashMap<String, String>>,
    ) -> Self {
        self.connect_attributes = connect_attributes.map(|attrs| {
            attrs
                .into_iter()
                .map(|(k, v)| (RawBytes::new(k.into_bytes()), RawBytes::new(v.into_bytes())))
                .collect()
        });
        self
    }

    pub fn into_owned(self) -> ComChangeUserMoreData<'static> {
        ComChangeUserMoreData {
            character_set: self.character_set,
            auth_plugin: self.auth_plugin.map(|x| x.into_owned()),
            connect_attributes: self.connect_attributes.map(|x| {
                x.into_iter()
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            }),
        }
    }
}

// Helper that deserializes connect attributes.
fn deserialize_connect_attrs<'de>(
    buf: &mut ParseBuf<'de>,
) -> io::Result<HashMap<RawBytes<'de, LenEnc>, RawBytes<'de, LenEnc>>> {
    let data_len = buf.parse::<RawInt<LenEnc>>(())?;
    let mut data: ParseBuf = buf.parse(data_len.0 as usize)?;
    let mut attrs = HashMap::new();
    while !data.is_empty() {
        let key = data.parse::<RawBytes<LenEnc>>(())?;
        let value = data.parse::<RawBytes<LenEnc>>(())?;
        attrs.insert(key, value);
    }
    Ok(attrs)
}

// Helper that serializes connect attributes.
fn serialize_connect_attrs<'a>(
    connect_attributes: &HashMap<RawBytes<'a, LenEnc>, RawBytes<'a, LenEnc>>,
    buf: &mut Vec<u8>,
) {
    let len = connect_attributes
        .iter()
        .map(|(k, v)| lenenc_str_len(k.as_bytes()) + lenenc_str_len(v.as_bytes()))
        .sum::<u64>();
    buf.put_lenenc_int(len);

    for (name, value) in connect_attributes {
        name.serialize(&mut *buf);
        value.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for ComChangeUserMoreData<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = CapabilityFlags;

    fn deserialize(flags: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        // always assume CLIENT_PROTOCOL_41
        let character_set = buf.parse(())?;
        let mut auth_plugin = None;
        let mut connect_attributes = None;

        if flags.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
            // plugin name is null-terminated here
            match buf.parse::<RawBytes<NullBytes>>(())?.0 {
                Cow::Borrowed(bytes) => {
                    let mut auth_plugin_buf = ParseBuf(bytes);
                    auth_plugin = Some(auth_plugin_buf.parse(())?);
                }
                _ => unreachable!(),
            }
        };

        if flags.contains(CapabilityFlags::CLIENT_CONNECT_ATTRS) {
            connect_attributes = Some(deserialize_connect_attrs(&mut *buf)?);
        };

        Ok(Self {
            character_set,
            auth_plugin,
            connect_attributes,
        })
    }
}

impl<'a> MySerialize for ComChangeUserMoreData<'a> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.character_set.serialize(&mut *buf);
        if let Some(ref auth_plugin) = self.auth_plugin {
            auth_plugin.serialize(&mut *buf);
        }
        if let Some(ref connect_attributes) = self.connect_attributes {
            serialize_connect_attrs(connect_attributes, buf);
        } else {
            // We'll always act like CLIENT_CONNECT_ATTRS is set,
            // this is to avoid looking into the actual connection flags.
            serialize_connect_attrs(&Default::default(), buf);
        }
    }
}

/// Actual serialization of this field depends on capability flags values.
type ScrambleBuf<'a> =
    Either<RawBytes<'a, LenEnc>, Either<RawBytes<'a, U8Bytes>, RawBytes<'a, NullBytes>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeResponse<'a> {
    capabilities: Const<CapabilityFlags, LeU32>,
    max_packet_size: RawInt<LeU32>,
    collation: RawInt<u8>,
    scramble_buf: ScrambleBuf<'a>,
    user: RawBytes<'a, NullBytes>,
    db_name: Option<RawBytes<'a, NullBytes>>,
    auth_plugin: Option<AuthPlugin<'a>>,
    connect_attributes: Option<HashMap<RawBytes<'a, LenEnc>, RawBytes<'a, LenEnc>>>,
}

impl<'a> HandshakeResponse<'a> {
    pub fn new(
        scramble_buf: Option<impl Into<Cow<'a, [u8]>>>,
        server_version: (u16, u16, u16),
        user: Option<impl Into<Cow<'a, [u8]>>>,
        db_name: Option<impl Into<Cow<'a, [u8]>>>,
        auth_plugin: Option<AuthPlugin<'a>>,
        mut capabilities: CapabilityFlags,
        connect_attributes: Option<HashMap<String, String>>,
        max_packet_size: u32,
    ) -> Self {
        let scramble_buf =
            if capabilities.contains(CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
                Either::Left(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                ))
            } else if capabilities.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
                Either::Right(Either::Left(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                )))
            } else {
                Either::Right(Either::Right(RawBytes::new(
                    scramble_buf.map(Into::into).unwrap_or_default(),
                )))
            };

        if db_name.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        }

        if auth_plugin.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_PLUGIN_AUTH);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_PLUGIN_AUTH);
        }

        if connect_attributes.is_some() {
            capabilities.insert(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        } else {
            capabilities.remove(CapabilityFlags::CLIENT_CONNECT_ATTRS);
        }

        Self {
            scramble_buf,
            collation: if server_version >= (5, 5, 3) {
                RawInt::new(CollationId::UTF8MB4_GENERAL_CI as u8)
            } else {
                RawInt::new(CollationId::UTF8MB3_GENERAL_CI as u8)
            },
            user: user.map(RawBytes::new).unwrap_or_default(),
            db_name: db_name.map(RawBytes::new),
            auth_plugin,
            capabilities: Const::new(capabilities),
            connect_attributes: connect_attributes.map(|attrs| {
                attrs
                    .into_iter()
                    .map(|(k, v)| (RawBytes::new(k.into_bytes()), RawBytes::new(v.into_bytes())))
                    .collect()
            }),
            max_packet_size: RawInt::new(max_packet_size),
        }
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities.0
    }

    pub fn collation(&self) -> u8 {
        self.collation.0
    }

    pub fn scramble_buf(&self) -> &[u8] {
        match &self.scramble_buf {
            Either::Left(x) => x.as_bytes(),
            Either::Right(x) => match x {
                Either::Left(x) => x.as_bytes(),
                Either::Right(x) => x.as_bytes(),
            },
        }
    }

    pub fn user(&self) -> &[u8] {
        self.user.as_bytes()
    }

    pub fn db_name(&self) -> Option<&[u8]> {
        self.db_name.as_ref().map(|x| x.as_bytes())
    }

    pub fn auth_plugin(&self) -> Option<&AuthPlugin<'a>> {
        self.auth_plugin.as_ref()
    }

    #[must_use = "entails computation"]
    pub fn connect_attributes(&self) -> Option<HashMap<String, String>> {
        self.connect_attributes.as_ref().map(|attrs| {
            attrs
                .iter()
                .map(|(k, v)| (k.as_str().into_owned(), v.as_str().into_owned()))
                .collect()
        })
    }
}

impl<'de> MyDeserialize<'de> for HandshakeResponse<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(4 + 4 + 1 + 23)?;
        let client_flags: RawConst<LeU32, CapabilityFlags> = sbuf.parse_unchecked(())?;
        let max_packet_size: RawInt<LeU32> = sbuf.parse_unchecked(())?;
        let collation = sbuf.parse_unchecked(())?;
        sbuf.parse_unchecked::<Skip<23>>(())?;

        let user = buf.parse(())?;
        let scramble_buf =
            if client_flags.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.bits() > 0 {
                Either::Left(buf.parse(())?)
            } else if client_flags.0 & CapabilityFlags::CLIENT_SECURE_CONNECTION.bits() > 0 {
                Either::Right(Either::Left(buf.parse(())?))
            } else {
                Either::Right(Either::Right(buf.parse(())?))
            };

        let mut db_name = None;
        if client_flags.0 & CapabilityFlags::CLIENT_CONNECT_WITH_DB.bits() > 0 {
            db_name = buf.parse(()).map(Some)?;
        }

        let mut auth_plugin = None;
        if client_flags.0 & CapabilityFlags::CLIENT_PLUGIN_AUTH.bits() > 0 {
            let auth_plugin_name = buf.eat_null_str();
            auth_plugin = Some(AuthPlugin::from_bytes(auth_plugin_name));
        }

        let mut connect_attributes = None;
        if client_flags.0 & CapabilityFlags::CLIENT_CONNECT_ATTRS.bits() > 0 {
            connect_attributes = Some(deserialize_connect_attrs(&mut *buf)?);
        }

        Ok(Self {
            capabilities: Const::new(CapabilityFlags::from_bits_truncate(client_flags.0)),
            max_packet_size,
            collation,
            scramble_buf,
            user,
            db_name,
            auth_plugin,
            connect_attributes,
        })
    }
}

impl MySerialize for HandshakeResponse<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.capabilities.serialize(&mut *buf);
        self.max_packet_size.serialize(&mut *buf);
        self.collation.serialize(&mut *buf);
        buf.put_slice(&[0; 23]);
        self.user.serialize(&mut *buf);
        self.scramble_buf.serialize(&mut *buf);

        if let Some(db_name) = &self.db_name {
            db_name.serialize(&mut *buf);
        }

        if let Some(auth_plugin) = &self.auth_plugin {
            auth_plugin.serialize(&mut *buf);
        }

        if let Some(attrs) = &self.connect_attributes {
            let len = attrs
                .iter()
                .map(|(k, v)| lenenc_str_len(k.as_bytes()) + lenenc_str_len(v.as_bytes()))
                .sum::<u64>();
            buf.put_lenenc_int(len);

            for (name, value) in attrs {
                name.serialize(&mut *buf);
                value.serialize(&mut *buf);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SslRequest {
    capabilities: Const<CapabilityFlags, LeU32>,
    max_packet_size: RawInt<LeU32>,
    character_set: RawInt<u8>,
    __skip: Skip<23>,
}

impl SslRequest {
    pub fn new(capabilities: CapabilityFlags, max_packet_size: u32, character_set: u8) -> Self {
        Self {
            capabilities: Const::new(capabilities),
            max_packet_size: RawInt::new(max_packet_size),
            character_set: RawInt::new(character_set),
            __skip: Skip,
        }
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        self.capabilities.0
    }

    pub fn max_packet_size(&self) -> u32 {
        self.max_packet_size.0
    }

    pub fn character_set(&self) -> u8 {
        self.character_set.0
    }
}

impl<'de> MyDeserialize<'de> for SslRequest {
    const SIZE: Option<usize> = Some(4 + 4 + 1 + 23);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut buf: ParseBuf = buf.parse(Self::SIZE.unwrap())?;
        let raw_capabilities = buf.parse_unchecked::<RawConst<LeU32, CapabilityFlags>>(())?;
        Ok(Self {
            capabilities: Const::new(CapabilityFlags::from_bits_truncate(raw_capabilities.0)),
            max_packet_size: buf.parse_unchecked(())?,
            character_set: buf.parse_unchecked(())?,
            __skip: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for SslRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.capabilities.serialize(&mut *buf);
        self.max_packet_size.serialize(&mut *buf);
        self.character_set.serialize(&mut *buf);
        self.__skip.serialize(&mut *buf);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Invalid statement packet status")]
pub struct InvalidStmtPacketStatus;

/// Represents MySql's statement packet.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StmtPacket {
    status: ConstU8<InvalidStmtPacketStatus, 0x00>,
    statement_id: RawInt<LeU32>,
    num_columns: RawInt<LeU16>,
    num_params: RawInt<LeU16>,
    __skip: Skip<1>,
    warning_count: RawInt<LeU16>,
}

impl<'de> MyDeserialize<'de> for StmtPacket {
    const SIZE: Option<usize> = Some(12);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut buf: ParseBuf = buf.parse(Self::SIZE.unwrap())?;
        Ok(StmtPacket {
            status: buf.parse_unchecked(())?,
            statement_id: buf.parse_unchecked(())?,
            num_columns: buf.parse_unchecked(())?,
            num_params: buf.parse_unchecked(())?,
            __skip: buf.parse_unchecked(())?,
            warning_count: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for StmtPacket {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.status.serialize(&mut *buf);
        self.statement_id.serialize(&mut *buf);
        self.num_columns.serialize(&mut *buf);
        self.num_params.serialize(&mut *buf);
        self.__skip.serialize(&mut *buf);
        self.warning_count.serialize(&mut *buf);
    }
}

impl StmtPacket {
    /// Value of the statement_id field of a statement packet.
    pub fn statement_id(&self) -> u32 {
        *self.statement_id
    }

    /// Value of the num_columns field of a statement packet.
    pub fn num_columns(&self) -> u16 {
        *self.num_columns
    }

    /// Value of the num_params field of a statement packet.
    pub fn num_params(&self) -> u16 {
        *self.num_params
    }

    /// Value of the warning_count field of a statement packet.
    pub fn warning_count(&self) -> u16 {
        *self.warning_count
    }
}

/// Null-bitmap.
///
/// <http://dev.mysql.com/doc/internals/en/null-bitmap.html>
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NullBitmap<T, U: AsRef<[u8]> = Vec<u8>>(U, PhantomData<T>);

impl<'de, T: SerializationSide> MyDeserialize<'de> for NullBitmap<T, Cow<'de, [u8]>> {
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    fn deserialize(num_columns: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let bitmap_len = Self::bitmap_len(num_columns);
        let bytes = buf.checked_eat(bitmap_len).ok_or_else(unexpected_buf_eof)?;
        Ok(Self::from_bytes(Cow::Borrowed(bytes)))
    }
}

impl<T: SerializationSide> NullBitmap<T, Vec<u8>> {
    /// Creates new null-bitmap for a given number of columns.
    pub fn new(num_columns: usize) -> Self {
        Self::from_bytes(vec![0; Self::bitmap_len(num_columns)])
    }

    /// Will read null-bitmap for a given number of columns from `input`.
    pub fn read(input: &mut &[u8], num_columns: usize) -> Self {
        let bitmap_len = Self::bitmap_len(num_columns);
        assert!(input.len() >= bitmap_len);

        let bitmap = Self::from_bytes(input[..bitmap_len].to_vec());
        *input = &input[bitmap_len..];

        bitmap
    }
}

impl<T: SerializationSide, U: AsRef<[u8]>> NullBitmap<T, U> {
    pub fn bitmap_len(num_columns: usize) -> usize {
        (num_columns + 7 + T::BIT_OFFSET) / 8
    }

    fn byte_and_bit(&self, column_index: usize) -> (usize, u8) {
        let offset = column_index + T::BIT_OFFSET;
        let byte = offset / 8;
        let bit = 1 << (offset % 8) as u8;

        assert!(byte < self.0.as_ref().len());

        (byte, bit)
    }

    /// Creates new null-bitmap from given bytes.
    pub fn from_bytes(bytes: U) -> Self {
        Self(bytes, PhantomData)
    }

    /// Returns `true` if given column is `NULL` in this `NullBitmap`.
    pub fn is_null(&self, column_index: usize) -> bool {
        let (byte, bit) = self.byte_and_bit(column_index);
        self.0.as_ref()[byte] & bit > 0
    }
}

impl<T: SerializationSide, U: AsRef<[u8]> + AsMut<[u8]>> NullBitmap<T, U> {
    /// Sets flag value for given column.
    pub fn set(&mut self, column_index: usize, is_null: bool) {
        let (byte, bit) = self.byte_and_bit(column_index);
        if is_null {
            self.0.as_mut()[byte] |= bit
        } else {
            self.0.as_mut()[byte] &= !bit
        }
    }
}

impl<T, U: AsRef<[u8]>> AsRef<[u8]> for NullBitmap<T, U> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComStmtExecuteRequestBuilder {
    pub stmt_id: u32,
}

impl ComStmtExecuteRequestBuilder {
    pub const NULL_BITMAP_OFFSET: usize = 10;

    pub fn new(stmt_id: u32) -> Self {
        Self { stmt_id }
    }
}

impl ComStmtExecuteRequestBuilder {
    pub fn build(self, params: &[Value]) -> (ComStmtExecuteRequest<'_>, bool) {
        let bitmap_len = NullBitmap::<ClientSide>::bitmap_len(params.len());

        let mut bitmap_bytes = vec![0; bitmap_len];
        let mut bitmap = NullBitmap::<ClientSide, _>::from_bytes(&mut bitmap_bytes);
        let params = params.iter().collect::<Vec<_>>();

        let meta_len = params.len() * 2;

        let mut data_len = 0;
        for (i, param) in params.iter().enumerate() {
            match param.bin_len() as usize {
                0 => bitmap.set(i, true),
                x => data_len += x,
            }
        }

        let total_len = 10 + bitmap_len + 1 + meta_len + data_len;

        let as_long_data = total_len > MAX_PAYLOAD_LEN;

        (
            ComStmtExecuteRequest {
                com_stmt_execute: ConstU8::new(),
                stmt_id: RawInt::new(self.stmt_id),
                flags: Const::new(CursorType::CURSOR_TYPE_NO_CURSOR),
                iteration_count: ConstU32::new(),
                params_flags: Const::new(StmtExecuteParamsFlags::NEW_PARAMS_BOUND),
                bitmap: RawBytes::new(bitmap_bytes),
                params,
                as_long_data,
            },
            as_long_data,
        )
    }
}

define_header!(
    ComStmtExecuteHeader,
    COM_STMT_EXECUTE,
    InvalidComStmtExecuteHeader
);

define_const!(
    ConstU32,
    IterationCount,
    InvalidIterationCount("Invalid iteration count for COM_STMT_EXECUTE"),
    1
);

#[derive(Debug, Clone, PartialEq)]
pub struct ComStmtExecuteRequest<'a> {
    com_stmt_execute: ComStmtExecuteHeader,
    stmt_id: RawInt<LeU32>,
    flags: Const<CursorType, u8>,
    iteration_count: IterationCount,
    // max params / bits per byte = 8192
    bitmap: RawBytes<'a, BareBytes<8192>>,
    params_flags: Const<StmtExecuteParamsFlags, u8>,
    params: Vec<&'a Value>,
    as_long_data: bool,
}

impl<'a> ComStmtExecuteRequest<'a> {
    pub fn stmt_id(&self) -> u32 {
        self.stmt_id.0
    }

    pub fn flags(&self) -> CursorType {
        self.flags.0
    }

    pub fn bitmap(&self) -> &[u8] {
        self.bitmap.as_bytes()
    }

    pub fn params_flags(&self) -> StmtExecuteParamsFlags {
        self.params_flags.0
    }

    pub fn params(&self) -> &[&'a Value] {
        self.params.as_ref()
    }

    pub fn as_long_data(&self) -> bool {
        self.as_long_data
    }
}

impl MySerialize for ComStmtExecuteRequest<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.com_stmt_execute.serialize(&mut *buf);
        self.stmt_id.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.iteration_count.serialize(&mut *buf);

        if !self.params.is_empty() {
            self.bitmap.serialize(&mut *buf);
            self.params_flags.serialize(&mut *buf);
        }

        for param in &self.params {
            let (column_type, flags) = match param {
                Value::NULL => (ColumnType::MYSQL_TYPE_NULL, StmtExecuteParamFlags::empty()),
                Value::Bytes(_) => (
                    ColumnType::MYSQL_TYPE_VAR_STRING,
                    StmtExecuteParamFlags::empty(),
                ),
                Value::Int(_) => (
                    ColumnType::MYSQL_TYPE_LONGLONG,
                    StmtExecuteParamFlags::empty(),
                ),
                Value::UInt(_) => (
                    ColumnType::MYSQL_TYPE_LONGLONG,
                    StmtExecuteParamFlags::UNSIGNED,
                ),
                Value::Float(_) => (ColumnType::MYSQL_TYPE_FLOAT, StmtExecuteParamFlags::empty()),
                Value::Double(_) => (
                    ColumnType::MYSQL_TYPE_DOUBLE,
                    StmtExecuteParamFlags::empty(),
                ),
                Value::Date(..) => (
                    ColumnType::MYSQL_TYPE_DATETIME,
                    StmtExecuteParamFlags::empty(),
                ),
                Value::Time(..) => (ColumnType::MYSQL_TYPE_TIME, StmtExecuteParamFlags::empty()),
            };

            buf.put_slice(&[column_type as u8, flags.bits()]);
        }

        for param in &self.params {
            match **param {
                Value::Int(_)
                | Value::UInt(_)
                | Value::Float(_)
                | Value::Double(_)
                | Value::Date(..)
                | Value::Time(..) => {
                    param.serialize(buf);
                }
                Value::Bytes(_) if !self.as_long_data => {
                    param.serialize(buf);
                }
                Value::Bytes(_) | Value::NULL => {}
            }
        }
    }
}

define_header!(
    ComStmtSendLongDataHeader,
    COM_STMT_SEND_LONG_DATA,
    InvalidComStmtSendLongDataHeader
);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ComStmtSendLongData<'a> {
    __header: ComStmtSendLongDataHeader,
    stmt_id: RawInt<LeU32>,
    param_index: RawInt<LeU16>,
    data: RawBytes<'a, EofBytes>,
}

impl<'a> ComStmtSendLongData<'a> {
    pub fn new(stmt_id: u32, param_index: u16, data: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            __header: ComStmtSendLongDataHeader::new(),
            stmt_id: RawInt::new(stmt_id),
            param_index: RawInt::new(param_index),
            data: RawBytes::new(data),
        }
    }

    pub fn into_owned(self) -> ComStmtSendLongData<'static> {
        ComStmtSendLongData {
            __header: self.__header,
            stmt_id: self.stmt_id,
            param_index: self.param_index,
            data: self.data.into_owned(),
        }
    }
}

impl MySerialize for ComStmtSendLongData<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.__header.serialize(&mut *buf);
        self.stmt_id.serialize(&mut *buf);
        self.param_index.serialize(&mut *buf);
        self.data.serialize(&mut *buf);
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ComStmtClose {
    pub stmt_id: u32,
}

impl ComStmtClose {
    pub fn new(stmt_id: u32) -> Self {
        Self { stmt_id }
    }
}

impl MySerialize for ComStmtClose {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(Command::COM_STMT_CLOSE as u8);
        buf.put_u32_le(self.stmt_id);
    }
}

define_header!(
    ComRegisterSlaveHeader,
    COM_REGISTER_SLAVE,
    InvalidComRegisterSlaveHeader
);

/// Registers a slave at the master. Should be sent before requesting a binlog events
/// with `COM_BINLOG_DUMP`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ComRegisterSlave<'a> {
    header: ComRegisterSlaveHeader,
    /// The slaves server-id.
    server_id: RawInt<LeU32>,
    /// The host name or IP address of the slave to be reported to the master during slave
    /// registration. Usually empty.
    hostname: RawBytes<'a, U8Bytes>,
    /// The account user name of the slave to be reported to the master during slave registration.
    /// Usually empty.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 255 bytes.
    user: RawBytes<'a, U8Bytes>,
    /// The account password of the slave to be reported to the master during slave registration.
    /// Usually empty.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 255 bytes.
    password: RawBytes<'a, U8Bytes>,
    /// The TCP/IP port number for connecting to the slave, to be reported to the master during
    /// slave registration. Usually empty.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 255 bytes.
    port: RawInt<LeU16>,
    /// Ignored.
    replication_rank: RawInt<LeU32>,
    /// Usually 0. Appears as "master id" in `SHOW SLAVE HOSTS` on the master. Unknown what else
    /// it impacts.
    master_id: RawInt<LeU32>,
}

impl<'a> ComRegisterSlave<'a> {
    /// Creates new `ComRegisterSlave` with the given server identifier. Other fields will be empty.
    pub fn new(server_id: u32) -> Self {
        Self {
            header: Default::default(),
            server_id: RawInt::new(server_id),
            hostname: Default::default(),
            user: Default::default(),
            password: Default::default(),
            port: Default::default(),
            replication_rank: Default::default(),
            master_id: Default::default(),
        }
    }

    /// Sets the `hostname` field of the packet (maximum length is 255 bytes).
    pub fn with_hostname(mut self, hostname: impl Into<Cow<'a, [u8]>>) -> Self {
        self.hostname = RawBytes::new(hostname);
        self
    }

    /// Sets the `user` field of the packet (maximum length is 255 bytes).
    pub fn with_user(mut self, user: impl Into<Cow<'a, [u8]>>) -> Self {
        self.user = RawBytes::new(user);
        self
    }

    /// Sets the `password` field of the packet (maximum length is 255 bytes).
    pub fn with_password(mut self, password: impl Into<Cow<'a, [u8]>>) -> Self {
        self.password = RawBytes::new(password);
        self
    }

    /// Sets the `port` field of the packet.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = RawInt::new(port);
        self
    }

    /// Sets the `replication_rank` field of the packet.
    pub fn with_replication_rank(mut self, replication_rank: u32) -> Self {
        self.replication_rank = RawInt::new(replication_rank);
        self
    }

    /// Sets the `master_id` field of the packet.
    pub fn with_master_id(mut self, master_id: u32) -> Self {
        self.master_id = RawInt::new(master_id);
        self
    }

    /// Returns the `server_id` field of the packet.
    pub fn server_id(&self) -> u32 {
        self.server_id.0
    }

    /// Returns the raw `hostname` field value.
    pub fn hostname_raw(&'a self) -> &[u8] {
        self.hostname.as_bytes()
    }

    /// Returns the `hostname` field as a UTF-8 string (lossy converted).
    pub fn hostname(&'a self) -> Cow<'a, str> {
        self.hostname.as_str()
    }

    /// Returns the raw `user` field value.
    pub fn user_raw(&'a self) -> &[u8] {
        self.user.as_bytes()
    }

    /// Returns the `user` field as a UTF-8 string (lossy converted).
    pub fn user(&'a self) -> Cow<'a, str> {
        self.user.as_str()
    }

    /// Returns the raw `password` field value.
    pub fn password_raw(&'a self) -> &[u8] {
        self.password.as_bytes()
    }

    /// Returns the `password` field as a UTF-8 string (lossy converted).
    pub fn password(&'a self) -> Cow<'a, str> {
        self.password.as_str()
    }

    /// Returns the `port` field of the packet.
    pub fn port(&self) -> u16 {
        self.port.0
    }

    /// Returns the `replication_rank` field of the packet.
    pub fn replication_rank(&self) -> u32 {
        self.replication_rank.0
    }

    /// Returns the `master_id` field of the packet.
    pub fn master_id(&self) -> u32 {
        self.master_id.0
    }
}

impl MySerialize for ComRegisterSlave<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.header.serialize(&mut *buf);
        self.server_id.serialize(&mut *buf);
        self.hostname.serialize(&mut *buf);
        self.user.serialize(&mut *buf);
        self.password.serialize(&mut *buf);
        self.port.serialize(&mut *buf);
        self.replication_rank.serialize(&mut *buf);
        self.master_id.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for ComRegisterSlave<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(5)?;
        let header = sbuf.parse_unchecked(())?;
        let server_id = sbuf.parse_unchecked(())?;

        let hostname = buf.parse(())?;
        let user = buf.parse(())?;
        let password = buf.parse(())?;

        let mut sbuf: ParseBuf = buf.parse(10)?;
        let port = sbuf.parse_unchecked(())?;
        let replication_rank = sbuf.parse_unchecked(())?;
        let master_id = sbuf.parse_unchecked(())?;

        Ok(Self {
            header,
            server_id,
            hostname,
            user,
            password,
            port,
            replication_rank,
            master_id,
        })
    }
}

define_header!(
    ComTableDumpHeader,
    COM_TABLE_DUMP,
    InvalidComTableDumpHeader
);

/// COM_TABLE_DUMP command.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ComTableDump<'a> {
    header: ComTableDumpHeader,
    /// Database name.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 255 bytes.
    database: RawBytes<'a, U8Bytes>,
    /// Table name.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 255 bytes.
    table: RawBytes<'a, U8Bytes>,
}

impl<'a> ComTableDump<'a> {
    /// Creates new instance.
    pub fn new(database: impl Into<Cow<'a, [u8]>>, table: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            header: Default::default(),
            database: RawBytes::new(database),
            table: RawBytes::new(table),
        }
    }

    /// Returns the raw `database` field value.
    pub fn database_raw(&self) -> &[u8] {
        self.database.as_bytes()
    }

    /// Returns the `database` field value as a UTF-8 string (lossy converted).
    pub fn database(&self) -> Cow<str> {
        self.database.as_str()
    }

    /// Returns the raw `table` field value.
    pub fn table_raw(&self) -> &[u8] {
        self.table.as_bytes()
    }

    /// Returns the `table` field value as a UTF-8 string (lossy converted).
    pub fn table(&self) -> Cow<str> {
        self.table.as_str()
    }
}

impl MySerialize for ComTableDump<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.header.serialize(&mut *buf);
        self.database.serialize(&mut *buf);
        self.table.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for ComTableDump<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            header: buf.parse(())?,
            database: buf.parse(())?,
            table: buf.parse(())?,
        })
    }
}

my_bitflags! {
    BinlogDumpFlags,
    #[error("Unknown flags in the raw value of BinlogDumpFlags (raw={:b})", _0)]
    UnknownBinlogDumpFlags,
    u16,

    /// Empty flags of a `LoadEvent`.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct BinlogDumpFlags: u16 {
        /// If there is no more event to send a EOF_Packet instead of blocking the connection
        const BINLOG_DUMP_NON_BLOCK = 0x01;
        const BINLOG_THROUGH_POSITION = 0x02;
        const BINLOG_THROUGH_GTID = 0x04;
    }
}

define_header!(
    ComBinlogDumpHeader,
    COM_BINLOG_DUMP,
    InvalidComBinlogDumpHeader
);

/// Command to request a binlog-stream from the master starting a given position.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ComBinlogDump<'a> {
    header: ComBinlogDumpHeader,
    /// Position in the binlog-file to start the stream with (`0` by default).
    pos: RawInt<LeU32>,
    /// Command flags (empty by default).
    ///
    /// Only `BINLOG_DUMP_NON_BLOCK` is supported for this command.
    flags: Const<BinlogDumpFlags, LeU16>,
    /// Server id of this slave.
    server_id: RawInt<LeU32>,
    /// Filename of the binlog on the master.
    ///
    /// If the binlog-filename is empty, the server will send the binlog-stream of the first known
    /// binlog.
    filename: RawBytes<'a, EofBytes>,
}

impl<'a> ComBinlogDump<'a> {
    /// Creates new instance with default values for `pos` and `flags`.
    pub fn new(server_id: u32) -> Self {
        Self {
            header: Default::default(),
            pos: Default::default(),
            flags: Default::default(),
            server_id: RawInt::new(server_id),
            filename: Default::default(),
        }
    }

    /// Defines position for this instance.
    pub fn with_pos(mut self, pos: u32) -> Self {
        self.pos = RawInt::new(pos);
        self
    }

    /// Defines flags for this instance.
    pub fn with_flags(mut self, flags: BinlogDumpFlags) -> Self {
        self.flags = Const::new(flags);
        self
    }

    /// Defines filename for this instance.
    pub fn with_filename(mut self, filename: impl Into<Cow<'a, [u8]>>) -> Self {
        self.filename = RawBytes::new(filename);
        self
    }

    /// Returns parsed `pos` field with unknown bits truncated.
    pub fn pos(&self) -> u32 {
        *self.pos
    }

    /// Returns parsed `flags` field with unknown bits truncated.
    pub fn flags(&self) -> BinlogDumpFlags {
        *self.flags
    }

    /// Returns parsed `server_id` field with unknown bits truncated.
    pub fn server_id(&self) -> u32 {
        *self.server_id
    }

    /// Returns the raw `filename` field value.
    pub fn filename_raw(&self) -> &[u8] {
        self.filename.as_bytes()
    }

    /// Returns the `filename` field value as a UTF-8 string (lossy converted).
    pub fn filename(&self) -> Cow<str> {
        self.filename.as_str()
    }
}

impl MySerialize for ComBinlogDump<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.header.serialize(&mut *buf);
        self.pos.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.server_id.serialize(&mut *buf);
        self.filename.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for ComBinlogDump<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(11)?;
        Ok(Self {
            header: sbuf.parse_unchecked(())?,
            pos: sbuf.parse_unchecked(())?,
            flags: sbuf.parse_unchecked(())?,
            server_id: sbuf.parse_unchecked(())?,
            filename: buf.parse(())?,
        })
    }
}

/// GnoInterval. Stored within [`Sid`]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct GnoInterval {
    start: RawInt<LeU64>,
    end: RawInt<LeU64>,
}

impl GnoInterval {
    /// Creates a new interval.
    pub fn new(start: u64, end: u64) -> Self {
        Self {
            start: RawInt::new(start),
            end: RawInt::new(end),
        }
    }
    /// Checks if the [start, end) interval is valid and creates it.
    pub fn check_and_new(start: u64, end: u64) -> io::Result<Self> {
        if start >= end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("start({}) >= end({}) in GnoInterval", start, end),
            ));
        }
        if start == 0 || end == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Gno can't be zero",
            ));
        }
        Ok(Self::new(start, end))
    }
}

impl MySerialize for GnoInterval {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.start.serialize(&mut *buf);
        self.end.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for GnoInterval {
    const SIZE: Option<usize> = Some(16);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            start: buf.parse_unchecked(())?,
            end: buf.parse_unchecked(())?,
        })
    }
}

/// Length of a Uuid in `COM_BINLOG_DUMP_GTID` command packet.
pub const UUID_LEN: usize = 16;

/// SID is a part of the `COM_BINLOG_DUMP_GTID` command. It's a GtidSet whose
/// has only one Uuid.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Sid<'a> {
    uuid: [u8; UUID_LEN],
    intervals: Seq<'a, GnoInterval, LeU64>,
}

impl<'a> Sid<'a> {
    /// Creates a new instance.
    pub fn new(uuid: [u8; UUID_LEN]) -> Self {
        Self {
            uuid,
            intervals: Default::default(),
        }
    }

    /// Returns the `uuid` field value.
    pub fn uuid(&self) -> [u8; UUID_LEN] {
        self.uuid
    }

    /// Returns the `intervals` field value.
    pub fn intervals(&self) -> &[GnoInterval] {
        &self.intervals[..]
    }

    /// Appends an GnoInterval to this block.
    pub fn with_interval(mut self, interval: GnoInterval) -> Self {
        let mut intervals = self.intervals.0.into_owned();
        intervals.push(interval);
        self.intervals = Seq::new(intervals);
        self
    }

    /// Sets the `intevals` value for this block.
    pub fn with_intervals(mut self, intervals: Vec<GnoInterval>) -> Self {
        self.intervals = Seq::new(intervals);
        self
    }

    fn len(&self) -> u64 {
        use saturating::Saturating as S;
        let mut len = S(UUID_LEN as u64); // SID
        len += S(8); // n_intervals
        len += S((self.intervals.len() * 16) as u64);
        len.0
    }
}

impl MySerialize for Sid<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.uuid.serialize(&mut *buf);
        self.intervals.serialize(buf);
    }
}

impl<'de> MyDeserialize<'de> for Sid<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self {
            uuid: buf.parse(())?,
            intervals: buf.parse(())?,
        })
    }
}

impl Sid<'_> {
    fn wrap_err(msg: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, msg)
    }

    fn parse_interval_num(to_parse: &str, full: &str) -> Result<u64, io::Error> {
        let n: u64 = to_parse.parse().map_err(|e| {
            Sid::wrap_err(format!(
                "invalid GnoInterval format: {}, error: {}",
                full, e
            ))
        })?;
        Ok(n)
    }
}

impl<'a> FromStr for Sid<'a> {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uuid, intervals) = s
            .split_once(':')
            .ok_or_else(|| Sid::wrap_err(format!("invalid sid format: {}", s)))?;
        let uuid = Uuid::parse_str(uuid)
            .map_err(|e| Sid::wrap_err(format!("invalid uuid format: {}, error: {}", s, e)))?;
        let intervals = intervals
            .split(':')
            .map(|interval| {
                let nums = interval.split('-').collect::<Vec<_>>();
                if nums.len() != 1 && nums.len() != 2 {
                    return Err(Sid::wrap_err(format!("invalid GnoInterval format: {}", s)));
                }
                if nums.len() == 1 {
                    let start = Sid::parse_interval_num(nums[0], s)?;
                    let interval = GnoInterval::check_and_new(start, start + 1)?;
                    Ok(interval)
                } else {
                    let start = Sid::parse_interval_num(nums[0], s)?;
                    let end = Sid::parse_interval_num(nums[1], s)?;
                    let interval = GnoInterval::check_and_new(start, end + 1)?;
                    Ok(interval)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            uuid: *uuid.as_bytes(),
            intervals: Seq::new(intervals),
        })
    }
}

define_header!(
    ComBinlogDumpGtidHeader,
    COM_BINLOG_DUMP_GTID,
    InvalidComBinlogDumpGtidHeader
);

/// Command to request a binlog-stream from the master starting a given position.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ComBinlogDumpGtid<'a> {
    header: ComBinlogDumpGtidHeader,
    /// Command flags (empty by default).
    flags: Const<BinlogDumpFlags, LeU16>,
    /// Server id of this slave.
    server_id: RawInt<LeU32>,
    /// Filename of the binlog on the master.
    ///
    /// If the binlog-filename is empty, the server will send the binlog-stream of the first known
    /// binlog.
    ///
    /// # Note
    ///
    /// Serialization will truncate this value if length is greater than 2^32 - 1 bytes.
    filename: RawBytes<'a, U32Bytes>,
    /// Position in the binlog-file to start the stream with (`0` by default).
    pos: RawInt<LeU64>,
    /// SID block.
    sid_block: Seq<'a, Sid<'a>, LeU64>,
}

impl<'a> ComBinlogDumpGtid<'a> {
    /// Creates new instance with default values for `pos`, `data` and `flags` fields.
    pub fn new(server_id: u32) -> Self {
        Self {
            header: Default::default(),
            pos: Default::default(),
            flags: Default::default(),
            server_id: RawInt::new(server_id),
            filename: Default::default(),
            sid_block: Default::default(),
        }
    }

    /// Returns the `server_id` field value.
    pub fn server_id(&self) -> u32 {
        self.server_id.0
    }

    /// Returns the `flags` field value.
    pub fn flags(&self) -> BinlogDumpFlags {
        self.flags.0
    }

    /// Returns the `filename` field value.
    pub fn filename_raw(&self) -> &[u8] {
        self.filename.as_bytes()
    }

    /// Returns the `filename` field value as a UTF-8 string (lossy converted).
    pub fn filename(&self) -> Cow<str> {
        self.filename.as_str()
    }

    /// Returns the `pos` field value.
    pub fn pos(&self) -> u64 {
        self.pos.0
    }

    /// Returns the sequence of sids in this packet.
    pub fn sids(&self) -> &[Sid<'a>] {
        &self.sid_block
    }

    /// Defines filename for this instance.
    pub fn with_filename(self, filename: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            header: self.header,
            flags: self.flags,
            server_id: self.server_id,
            filename: RawBytes::new(filename),
            pos: self.pos,
            sid_block: self.sid_block,
        }
    }

    /// Sets the `server_id` field value.
    pub fn with_server_id(mut self, server_id: u32) -> Self {
        self.server_id.0 = server_id;
        self
    }

    /// Sets the `flags` field value.
    pub fn with_flags(mut self, mut flags: BinlogDumpFlags) -> Self {
        if self.sid_block.is_empty() {
            flags.remove(BinlogDumpFlags::BINLOG_THROUGH_GTID);
        } else {
            flags.insert(BinlogDumpFlags::BINLOG_THROUGH_GTID);
        }
        self.flags.0 = flags;
        self
    }

    /// Sets the `pos` field value.
    pub fn with_pos(mut self, pos: u64) -> Self {
        self.pos.0 = pos;
        self
    }

    /// Sets the `sid_block` field value.
    pub fn with_sid(mut self, sid: Sid<'a>) -> Self {
        self.flags.0.insert(BinlogDumpFlags::BINLOG_THROUGH_GTID);
        self.sid_block.push(sid);
        self
    }

    /// Sets the `sid_block` field value.
    pub fn with_sids(mut self, sids: impl Into<Cow<'a, [Sid<'a>]>>) -> Self {
        self.sid_block = Seq::new(sids);
        if self.sid_block.is_empty() {
            self.flags.0.remove(BinlogDumpFlags::BINLOG_THROUGH_GTID);
        } else {
            self.flags.0.insert(BinlogDumpFlags::BINLOG_THROUGH_GTID);
        }
        self
    }

    fn sid_block_len(&self) -> u32 {
        use saturating::Saturating as S;
        let mut len = S(8); // n_sids
        for sid in self.sid_block.iter() {
            len += S(sid.len() as u32);
        }
        len.0
    }
}

impl MySerialize for ComBinlogDumpGtid<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.header.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
        self.server_id.serialize(&mut *buf);
        self.filename.serialize(&mut *buf);
        self.pos.serialize(&mut *buf);
        buf.put_u32_le(self.sid_block_len());
        self.sid_block.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for ComBinlogDumpGtid<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(7)?;
        let header = sbuf.parse_unchecked(())?;
        let flags: Const<BinlogDumpFlags, LeU16> = sbuf.parse_unchecked(())?;
        let server_id = sbuf.parse_unchecked(())?;

        let filename = buf.parse(())?;
        let pos = buf.parse(())?;

        // `flags` should contain `BINLOG_THROUGH_GTID` flag if sid_block isn't empty
        let sid_data_len: RawInt<LeU32> = buf.parse(())?;
        let mut buf: ParseBuf = buf.parse(sid_data_len.0 as usize)?;
        let sid_block = buf.parse(())?;

        Ok(Self {
            header,
            flags,
            server_id,
            filename,
            pos,
            sid_block,
        })
    }
}

define_header!(
    SemiSyncAckPacketPacketHeader,
    InvalidSemiSyncAckPacketPacketHeader("Invalid semi-sync ack packet header"),
    0xEF
);

/// Each Semi Sync Binlog Event with the `SEMI_SYNC_ACK_REQ` flag set the slave has to acknowledge
/// with Semi-Sync ACK packet.
pub struct SemiSyncAckPacket<'a> {
    header: SemiSyncAckPacketPacketHeader,
    position: RawInt<LeU64>,
    filename: RawBytes<'a, EofBytes>,
}

impl<'a> SemiSyncAckPacket<'a> {
    pub fn new(position: u64, filename: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            header: Default::default(),
            position: RawInt::new(position),
            filename: RawBytes::new(filename),
        }
    }

    /// Sets the `position` field value.
    pub fn with_position(mut self, position: u64) -> Self {
        self.position.0 = position;
        self
    }

    /// Sets the `filename` field value.
    pub fn with_filename(mut self, filename: impl Into<Cow<'a, [u8]>>) -> Self {
        self.filename = RawBytes::new(filename);
        self
    }

    /// Returns the `position` field value.
    pub fn position(&self) -> u64 {
        self.position.0
    }

    /// Returns the raw `filename` field value.
    pub fn filename_raw(&self) -> &[u8] {
        self.filename.as_bytes()
    }

    /// Returns the `filename` field value as a string (lossy converted).
    pub fn filename(&self) -> Cow<'_, str> {
        self.filename.as_str()
    }
}

impl MySerialize for SemiSyncAckPacket<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.header.serialize(&mut *buf);
        self.position.serialize(&mut *buf);
        self.filename.serialize(&mut *buf);
    }
}

impl<'de> MyDeserialize<'de> for SemiSyncAckPacket<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(9)?;
        Ok(Self {
            header: sbuf.parse_unchecked(())?,
            position: sbuf.parse_unchecked(())?,
            filename: buf.parse(())?,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        constants::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags},
        proto::{MyDeserialize, MySerialize},
    };

    proptest::proptest! {
        #[test]
        fn com_table_dump_roundtrip(database: Vec<u8>, table: Vec<u8>) {
            let cmd = ComTableDump::new(database, table);

            let mut output = Vec::new();
            cmd.serialize(&mut output);

            assert_eq!(cmd, ComTableDump::deserialize((), &mut ParseBuf(&output[..]))?);
        }

        #[test]
        fn com_binlog_dump_roundtrip(
            server_id: u32,
            filename: Vec<u8>,
            pos: u32,
            flags: u16,
        ) {
            let cmd = ComBinlogDump::new(server_id)
                .with_filename(filename)
                .with_pos(pos)
                .with_flags(crate::packets::BinlogDumpFlags::from_bits_truncate(flags));

            let mut output = Vec::new();
            cmd.serialize(&mut output);

            assert_eq!(cmd, ComBinlogDump::deserialize((), &mut ParseBuf(&output[..]))?);
        }

        #[test]
        fn com_register_slave_roundtrip(
            server_id: u32,
            hostname in r"\w{0,256}",
            user in r"\w{0,256}",
            password in r"\w{0,256}",
            port: u16,
            replication_rank: u32,
            master_id: u32,
        ) {
            let cmd = ComRegisterSlave::new(server_id)
                .with_hostname(hostname.as_bytes())
                .with_user(user.as_bytes())
                .with_password(password.as_bytes())
                .with_port(port)
                .with_replication_rank(replication_rank)
                .with_master_id(master_id);

            let mut output = Vec::new();
            cmd.serialize(&mut output);
            let parsed = ComRegisterSlave::deserialize((), &mut ParseBuf(&output[..]))?;

            if hostname.len() > 255 || user.len() > 255 || password.len() > 255 {
                assert_ne!(cmd, parsed);
            } else {
                assert_eq!(cmd, parsed);
            }
        }

        #[test]
        fn com_binlog_dump_gtid_roundtrip(
            flags: u16,
            server_id: u32,
            filename: Vec<u8>,
            pos: u64,
            n_sid_blocks in 0_u64..1024,
        ) {
            let mut cmd = ComBinlogDumpGtid::new(server_id)
                .with_filename(filename)
                .with_pos(pos)
                .with_flags(crate::packets::BinlogDumpFlags::from_bits_truncate(flags));

            let mut sids = Vec::new();
            for i in 0..n_sid_blocks {
                let mut block = Sid::new([i as u8; 16]);
                for j in 0..i {
                    block = block.with_interval(GnoInterval::new(i, j));
                }
                sids.push(block);
            }

            cmd = cmd.with_sids(sids);

            let mut output = Vec::new();
            cmd.serialize(&mut output);

            assert_eq!(cmd, ComBinlogDumpGtid::deserialize((), &mut ParseBuf(&output[..]))?);
        }
    }

    #[test]
    fn should_parse_local_infile_packet() {
        const LIP: &[u8] = b"\xfbfile_name";

        let lip = LocalInfilePacket::deserialize((), &mut ParseBuf(LIP)).unwrap();
        assert_eq!(lip.file_name_str(), "file_name");
    }

    #[test]
    fn should_parse_stmt_packet() {
        const SP: &[u8] = b"\x00\x01\x00\x00\x00\x01\x00\x02\x00\x00\x00\x00";
        const SP_2: &[u8] = b"\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

        let sp = StmtPacket::deserialize((), &mut ParseBuf(SP)).unwrap();
        assert_eq!(sp.statement_id(), 0x01);
        assert_eq!(sp.num_columns(), 0x01);
        assert_eq!(sp.num_params(), 0x02);
        assert_eq!(sp.warning_count(), 0x00);

        let sp = StmtPacket::deserialize((), &mut ParseBuf(SP_2)).unwrap();
        assert_eq!(sp.statement_id(), 0x01);
        assert_eq!(sp.num_columns(), 0x00);
        assert_eq!(sp.num_params(), 0x00);
        assert_eq!(sp.warning_count(), 0x00);
    }

    #[test]
    fn should_parse_handshake_packet() {
        const HSP: &[u8] = b"\x0a5.5.5-10.0.17-MariaDB-log\x00\x0b\x00\
                             \x00\x00\x64\x76\x48\x40\x49\x2d\x43\x4a\x00\xff\xf7\x08\x02\x00\
                             \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2a\x34\x64\
                             \x7c\x63\x5a\x77\x6b\x34\x5e\x5d\x3a\x00";

        const HSP_2: &[u8] = b"\x0a\x35\x2e\x36\x2e\x34\x2d\x6d\x37\x2d\x6c\x6f\
                               \x67\x00\x56\x0a\x00\x00\x52\x42\x33\x76\x7a\x26\x47\x72\x00\xff\
                               \xff\x08\x02\x00\x0f\xc0\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                               \x00\x2b\x79\x44\x26\x2f\x5a\x5a\x33\x30\x35\x5a\x47\x00\x6d\x79\
                               \x73\x71\x6c\x5f\x6e\x61\x74\x69\x76\x65\x5f\x70\x61\x73\x73\x77\
                               \x6f\x72\x64\x00";

        const HSP_3: &[u8] = b"\x0a\x35\x2e\x36\x2e\x34\x2d\x6d\x37\x2d\x6c\x6f\
                                \x67\x00\x56\x0a\x00\x00\x52\x42\x33\x76\x7a\x26\x47\x72\x00\xff\
                                \xff\x08\x02\x00\x0f\xc0\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\
                                \x00\x2b\x79\x44\x26\x2f\x5a\x5a\x33\x30\x35\x5a\x47\x00\x6d\x79\
                                \x73\x71\x6c\x5f\x6e\x61\x74\x69\x76\x65\x5f\x70\x61\x73\x73\x77\
                                \x6f\x72\x64\x00";

        let hsp = HandshakePacket::deserialize((), &mut ParseBuf(HSP)).unwrap();
        assert_eq!(hsp.protocol_version(), 0x0a);
        assert_eq!(hsp.server_version_str(), "5.5.5-10.0.17-MariaDB-log");
        assert_eq!(hsp.server_version_parsed(), Some((5, 5, 5)));
        assert_eq!(hsp.maria_db_server_version_parsed(), Some((10, 0, 17)));
        assert_eq!(hsp.connection_id(), 0x0b);
        assert_eq!(hsp.scramble_1_ref(), b"dvH@I-CJ");
        assert_eq!(
            hsp.capabilities(),
            CapabilityFlags::from_bits_truncate(0xf7ff)
        );
        assert_eq!(hsp.default_collation(), 0x08);
        assert_eq!(hsp.status_flags(), StatusFlags::from_bits_truncate(0x0002));
        assert_eq!(hsp.scramble_2_ref(), Some(&b"*4d|cZwk4^]:\x00"[..]));
        assert_eq!(hsp.auth_plugin_name_ref(), None);

        let mut output = Vec::new();
        hsp.serialize(&mut output);
        assert_eq!(&output, HSP);

        let hsp = HandshakePacket::deserialize((), &mut ParseBuf(HSP_2)).unwrap();
        assert_eq!(hsp.protocol_version(), 0x0a);
        assert_eq!(hsp.server_version_str(), "5.6.4-m7-log");
        assert_eq!(hsp.server_version_parsed(), Some((5, 6, 4)));
        assert_eq!(hsp.maria_db_server_version_parsed(), None);
        assert_eq!(hsp.connection_id(), 0x0a56);
        assert_eq!(hsp.scramble_1_ref(), b"RB3vz&Gr");
        assert_eq!(
            hsp.capabilities(),
            CapabilityFlags::from_bits_truncate(0xc00fffff)
        );
        assert_eq!(hsp.default_collation(), 0x08);
        assert_eq!(hsp.status_flags(), StatusFlags::from_bits_truncate(0x0002));
        assert_eq!(hsp.scramble_2_ref(), Some(&b"+yD&/ZZ305ZG\0"[..]));
        assert_eq!(
            hsp.auth_plugin_name_ref(),
            Some(&b"mysql_native_password"[..])
        );

        let mut output = Vec::new();
        hsp.serialize(&mut output);
        assert_eq!(&output, HSP_2);

        let hsp = HandshakePacket::deserialize((), &mut ParseBuf(HSP_3)).unwrap();
        assert_eq!(hsp.protocol_version(), 0x0a);
        assert_eq!(hsp.server_version_str(), "5.6.4-m7-log");
        assert_eq!(hsp.server_version_parsed(), Some((5, 6, 4)));
        assert_eq!(hsp.maria_db_server_version_parsed(), None);
        assert_eq!(hsp.connection_id(), 0x0a56);
        assert_eq!(hsp.scramble_1_ref(), b"RB3vz&Gr");
        assert_eq!(
            hsp.capabilities(),
            CapabilityFlags::from_bits_truncate(0xc00fffff)
        );
        assert_eq!(hsp.default_collation(), 0x08);
        assert_eq!(hsp.status_flags(), StatusFlags::from_bits_truncate(0x0002));
        assert_eq!(hsp.scramble_2_ref(), Some(&b"+yD&/ZZ305ZG\0"[..]));
        assert_eq!(
            hsp.auth_plugin_name_ref(),
            Some(&b"mysql_native_password"[..])
        );

        let mut output = Vec::new();
        hsp.serialize(&mut output);
        assert_eq!(&output, HSP_3);
    }

    #[test]
    fn should_parse_err_packet() {
        const ERR_PACKET: &[u8] = b"\xff\x48\x04\x23\x48\x59\x30\x30\x30\x4e\x6f\x20\x74\x61\x62\
        \x6c\x65\x73\x20\x75\x73\x65\x64";
        const ERR_PACKET_NO_STATE: &[u8] = b"\xff\x10\x04\x54\x6f\x6f\x20\x6d\x61\x6e\x79\x20\x63\
        \x6f\x6e\x6e\x65\x63\x74\x69\x6f\x6e\x73";
        const PROGRESS_PACKET: &[u8] = b"\xff\xff\xff\x01\x01\x0a\xcc\x5b\x00\x0astage name";

        let err_packet = ErrPacket::deserialize(
            CapabilityFlags::CLIENT_PROTOCOL_41,
            &mut ParseBuf(ERR_PACKET),
        )
        .unwrap();
        let err_packet = err_packet.server_error();
        assert_eq!(err_packet.error_code(), 1096);
        assert_eq!(err_packet.sql_state_ref().unwrap().as_str(), "HY000");
        assert_eq!(err_packet.message_str(), "No tables used");

        let err_packet =
            ErrPacket::deserialize(CapabilityFlags::empty(), &mut ParseBuf(ERR_PACKET_NO_STATE))
                .unwrap();
        let server_error = err_packet.server_error();
        assert_eq!(server_error.error_code(), 1040);
        assert_eq!(server_error.sql_state_ref(), None);
        assert_eq!(server_error.message_str(), "Too many connections");

        let err_packet = ErrPacket::deserialize(
            CapabilityFlags::CLIENT_PROGRESS_OBSOLETE,
            &mut ParseBuf(PROGRESS_PACKET),
        )
        .unwrap();
        assert!(err_packet.is_progress_report());
        let progress_report = err_packet.progress_report();
        assert_eq!(progress_report.stage(), 1);
        assert_eq!(progress_report.max_stage(), 10);
        assert_eq!(progress_report.progress(), 23500);
        assert_eq!(progress_report.stage_info_str(), "stage name");
    }

    #[test]
    fn should_parse_column_packet() {
        const COLUMN_PACKET: &[u8] = b"\x03def\x06schema\x05table\x09org_table\x04name\
              \x08org_name\x0c\x21\x00\x0F\x00\x00\x00\x00\x01\x00\x08\x00\x00";
        let column = Column::deserialize((), &mut ParseBuf(COLUMN_PACKET)).unwrap();
        assert_eq!(column.schema_str(), "schema");
        assert_eq!(column.table_str(), "table");
        assert_eq!(column.org_table_str(), "org_table");
        assert_eq!(column.name_str(), "name");
        assert_eq!(column.org_name_str(), "org_name");
        assert_eq!(
            column.character_set(),
            CollationId::UTF8MB3_GENERAL_CI as u16
        );
        assert_eq!(column.column_length(), 15);
        assert_eq!(column.column_type(), ColumnType::MYSQL_TYPE_DECIMAL);
        assert_eq!(column.flags(), ColumnFlags::NOT_NULL_FLAG);
        assert_eq!(column.decimals(), 8);
    }

    #[test]
    fn should_parse_auth_switch_request() {
        const PAYLOAD: &[u8] = b"\xfe\x6d\x79\x73\x71\x6c\x5f\x6e\x61\x74\x69\x76\x65\x5f\x70\x61\
                                 \x73\x73\x77\x6f\x72\x64\x00\x7a\x51\x67\x34\x69\x36\x6f\x4e\x79\
                                 \x36\x3d\x72\x48\x4e\x2f\x3e\x2d\x62\x29\x41\x00";
        let packet = AuthSwitchRequest::deserialize((), &mut ParseBuf(PAYLOAD)).unwrap();
        assert_eq!(packet.auth_plugin().as_bytes(), b"mysql_native_password",);
        assert_eq!(packet.plugin_data(), b"zQg4i6oNy6=rHN/>-b)A",)
    }

    #[test]
    fn should_parse_auth_more_data() {
        const PAYLOAD: &[u8] = b"\x01\x04";
        let packet = AuthMoreData::deserialize((), &mut ParseBuf(PAYLOAD)).unwrap();
        assert_eq!(packet.data(), b"\x04",);
    }

    #[test]
    fn should_parse_ok_packet() {
        const PLAIN_OK: &[u8] = b"\x00\x01\x00\x02\x00\x00\x00";
        const RESULT_SET_TERMINATOR: &[u8] = &[
            0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, 0x52, 0x65, 0x61, 0x64, 0x20, 0x31,
            0x20, 0x72, 0x6f, 0x77, 0x73, 0x2c, 0x20, 0x31, 0x2e, 0x30, 0x30, 0x20, 0x42, 0x20,
            0x69, 0x6e, 0x20, 0x30, 0x2e, 0x30, 0x30, 0x32, 0x20, 0x73, 0x65, 0x63, 0x2e, 0x2c,
            0x20, 0x36, 0x31, 0x31, 0x2e, 0x33, 0x34, 0x20, 0x72, 0x6f, 0x77, 0x73, 0x2f, 0x73,
            0x65, 0x63, 0x2e, 0x2c, 0x20, 0x36, 0x31, 0x31, 0x2e, 0x33, 0x34, 0x20, 0x42, 0x2f,
            0x73, 0x65, 0x63, 0x2e,
        ];
        const SESS_STATE_SYS_VAR_OK: &[u8] =
            b"\x00\x00\x00\x02\x40\x00\x00\x00\x11\x00\x0f\x0a\x61\
              \x75\x74\x6f\x63\x6f\x6d\x6d\x69\x74\x03\x4f\x46\x46";
        const SESS_STATE_SCHEMA_OK: &[u8] =
            b"\x00\x00\x00\x02\x40\x00\x00\x00\x07\x01\x05\x04\x74\x65\x73\x74";
        const SESS_STATE_TRACK_OK: &[u8] = b"\x00\x00\x00\x02\x40\x00\x00\x00\x04\x02\x02\x01\x31";
        const EOF: &[u8] = b"\xfe\x00\x00\x02\x00";

        // packet starting with 0x00 is not an ok packet if it terminates a result set
        OkPacketDeserializer::<ResultSetTerminator>::deserialize(
            CapabilityFlags::empty(),
            &mut ParseBuf(PLAIN_OK),
        )
        .unwrap_err();

        let ok_packet: OkPacket = OkPacketDeserializer::<CommonOkPacket>::deserialize(
            CapabilityFlags::empty(),
            &mut ParseBuf(PLAIN_OK),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 1);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        assert_eq!(ok_packet.session_state_info_ref(), None);

        let ok_packet: OkPacket = OkPacketDeserializer::<CommonOkPacket>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(PLAIN_OK),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 1);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        assert_eq!(ok_packet.session_state_info_ref(), None);

        let ok_packet: OkPacket = OkPacketDeserializer::<ResultSetTerminator>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(RESULT_SET_TERMINATOR),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 0);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(ok_packet.status_flags(), StatusFlags::empty());
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(
            ok_packet.info_str(),
            Some(Cow::Borrowed(
                "Read 1 rows, 1.00 B in 0.002 sec., 611.34 rows/sec., 611.34 B/sec."
            ))
        );
        assert_eq!(ok_packet.session_state_info_ref(), None);

        let ok_packet: OkPacket = OkPacketDeserializer::<CommonOkPacket>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(SESS_STATE_SYS_VAR_OK),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 0);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT | StatusFlags::SERVER_SESSION_STATE_CHANGED
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        let sess_state_info = ok_packet.session_state_info().unwrap().pop().unwrap();

        match sess_state_info.decode().unwrap() {
            SessionStateChange::SystemVariables(mut vals) => {
                let val = vals.pop().unwrap();
                assert_eq!(val.name_bytes(), b"autocommit");
                assert_eq!(val.value_bytes(), b"OFF");
                assert!(vals.is_empty());
            }
            _ => panic!(),
        }

        let ok_packet: OkPacket = OkPacketDeserializer::<CommonOkPacket>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(SESS_STATE_SCHEMA_OK),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 0);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT | StatusFlags::SERVER_SESSION_STATE_CHANGED
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        let sess_state_info = ok_packet.session_state_info().unwrap().pop().unwrap();
        match sess_state_info.decode().unwrap() {
            SessionStateChange::Schema(schema) => assert_eq!(schema.as_bytes(), b"test"),
            _ => panic!(),
        }

        let ok_packet: OkPacket = OkPacketDeserializer::<CommonOkPacket>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(SESS_STATE_TRACK_OK),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 0);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT | StatusFlags::SERVER_SESSION_STATE_CHANGED
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        let sess_state_info = ok_packet.session_state_info().unwrap().pop().unwrap();
        assert_eq!(
            sess_state_info.decode().unwrap(),
            SessionStateChange::IsTracked(true),
        );

        let ok_packet: OkPacket = OkPacketDeserializer::<OldEofPacket>::deserialize(
            CapabilityFlags::CLIENT_SESSION_TRACK,
            &mut ParseBuf(EOF),
        )
        .unwrap()
        .into();
        assert_eq!(ok_packet.affected_rows(), 0);
        assert_eq!(ok_packet.last_insert_id(), None);
        assert_eq!(
            ok_packet.status_flags(),
            StatusFlags::SERVER_STATUS_AUTOCOMMIT
        );
        assert_eq!(ok_packet.warnings(), 0);
        assert_eq!(ok_packet.info_ref(), None);
        assert_eq!(ok_packet.session_state_info_ref(), None);
    }

    #[test]
    fn should_build_handshake_response() {
        let flags_without_db_name = CapabilityFlags::from_bits_truncate(0x81aea205);
        let response = HandshakeResponse::new(
            Some(&[][..]),
            (5u16, 5, 5),
            Some(&b"root"[..]),
            None::<&'static [u8]>,
            Some(AuthPlugin::MysqlNativePassword),
            flags_without_db_name,
            None,
            1_u32.to_be(),
        );
        let mut actual = Vec::new();
        response.serialize(&mut actual);

        let expected: Vec<u8> = [
            0x05, 0xa2, 0xae, 0x81, // client capabilities
            0x00, 0x00, 0x00, 0x01, // max packet
            0x2d, // charset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
            0x72, 0x6f, 0x6f, 0x74, 0x00, // username=root
            0x00, // blank scramble
            0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70,
            0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00, // mysql_native_password
        ]
        .to_vec();

        assert_eq!(expected, actual);

        let flags_with_db_name = flags_without_db_name | CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        let response = HandshakeResponse::new(
            Some(&[][..]),
            (5u16, 5, 5),
            Some(&b"root"[..]),
            Some(&b"mydb"[..]),
            Some(AuthPlugin::MysqlNativePassword),
            flags_with_db_name,
            None,
            1_u32.to_be(),
        );
        let mut actual = Vec::new();
        response.serialize(&mut actual);

        let expected: Vec<u8> = [
            0x0d, 0xa2, 0xae, 0x81, // client capabilities
            0x00, 0x00, 0x00, 0x01, // max packet
            0x2d, // charset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
            0x72, 0x6f, 0x6f, 0x74, 0x00, // username=root
            0x00, // blank scramble
            0x6d, 0x79, 0x64, 0x62, 0x00, // dbname
            0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70,
            0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00, // mysql_native_password
        ]
        .to_vec();

        assert_eq!(expected, actual);

        let response = HandshakeResponse::new(
            Some(&[][..]),
            (5u16, 5, 5),
            Some(&b"root"[..]),
            Some(&b"mydb"[..]),
            Some(AuthPlugin::MysqlNativePassword),
            flags_without_db_name,
            None,
            1_u32.to_be(),
        );
        let mut actual = Vec::new();
        response.serialize(&mut actual);
        assert_eq!(expected, actual);

        let response = HandshakeResponse::new(
            Some(&[][..]),
            (5u16, 5, 5),
            Some(&b"root"[..]),
            Some(&[][..]),
            Some(AuthPlugin::MysqlNativePassword),
            flags_with_db_name,
            None,
            1_u32.to_be(),
        );
        let mut actual = Vec::new();
        response.serialize(&mut actual);

        let expected: Vec<u8> = [
            0x0d, 0xa2, 0xae, 0x81, // client capabilities
            0x00, 0x00, 0x00, 0x01, // max packet
            0x2d, // charset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
            0x72, 0x6f, 0x6f, 0x74, 0x00, // username=root
            0x00, // blank db_name
            0x00, // blank scramble
            0x6d, 0x79, 0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70,
            0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x00, // mysql_native_password
        ]
        .to_vec();
        assert_eq!(expected, actual);
    }

    #[test]
    fn parse_str_to_sid() {
        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:23";
        let sid = input.parse::<Sid>().unwrap();
        let expected_sid = Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429562").unwrap();
        assert_eq!(sid.uuid, *expected_sid.as_bytes());
        assert_eq!(sid.intervals.len(), 1);
        assert_eq!(sid.intervals[0].start.0, 23);
        assert_eq!(sid.intervals[0].end.0, 24);

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10-15";
        let sid = input.parse::<Sid>().unwrap();
        assert_eq!(sid.uuid, *expected_sid.as_bytes());
        assert_eq!(sid.intervals.len(), 2);
        assert_eq!(sid.intervals[0].start.0, 1);
        assert_eq!(sid.intervals[0].end.0, 6);
        assert_eq!(sid.intervals[1].start.0, 10);
        assert_eq!(sid.intervals[1].end.0, 16);

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let e = input.parse::<Sid>().unwrap_err();
        assert_eq!(
            e.to_string(),
            "invalid sid format: 3E11FA47-71CA-11E1-9E33-C80AA9429562".to_string()
        );

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10-15:20-";
        let e = input.parse::<Sid>().unwrap_err();
        assert_eq!(e.to_string(), "invalid GnoInterval format: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:10-15:20-, error: cannot parse integer from empty string".to_string());

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:1aaa";
        let e = input.parse::<Sid>().unwrap_err();
        assert_eq!(e.to_string(), "invalid GnoInterval format: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:1aaa, error: invalid digit found in string".to_string());

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:0-3";
        let e = input.parse::<Sid>().unwrap_err();
        assert_eq!(e.to_string(), "Gno can't be zero".to_string());

        let input = "3E11FA47-71CA-11E1-9E33-C80AA9429562:4-3";
        let e = input.parse::<Sid>().unwrap_err();
        assert_eq!(
            e.to_string(),
            "start(4) >= end(4) in GnoInterval".to_string()
        );
    }

    #[test]
    fn should_parse_rsa_public_key_response_packet() {
        const PUBLIC_RSA_KEY_RESPONSE: &[u8] = b"\x01test";

        let rsa_public_key_response =
            PublicKeyResponse::deserialize((), &mut ParseBuf(PUBLIC_RSA_KEY_RESPONSE));

        assert!(rsa_public_key_response.is_ok());
        assert_eq!(rsa_public_key_response.unwrap().rsa_key(), "test");
    }

    #[test]
    fn should_build_rsa_public_key_response_packet() {
        let rsa_public_key_response = PublicKeyResponse::new("test".as_bytes());

        let mut actual = Vec::new();
        rsa_public_key_response.serialize(&mut actual);

        let expected = b"\x01test".to_vec();

        assert_eq!(expected, actual);
    }
}
