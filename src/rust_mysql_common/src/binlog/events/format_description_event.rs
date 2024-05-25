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
    misc::raw::{
        bytes::{EofBytes, FixedLengthText},
        int::{ConstU8, LeU16, LeU32},
        Const, RawBytes, RawInt,
    },
    proto::{MyDeserialize, MySerialize},
};

use super::{BinlogEventFooter, BinlogEventHeader};

/// Length of a server version string.
pub const SERVER_VER_LEN: usize = 50;

define_const!(
    ConstU8,
    EventHeaderLength,
    InvalidEventHeaderLength("Invalid event_header_length value for format description event"),
    19
);

/// A format description event is the first event of a binlog for binlog-version 4.
///
/// It describes how the other events are layed out.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FormatDescriptionEvent<'a> {
    /// Version of this binlog format.
    binlog_version: Const<BinlogVersion, LeU16>,
    /// Version of the MySQL Server that created the binlog (len=50).
    ///
    /// The string is evaluted to apply work-arounds in the slave.
    server_version: RawBytes<'a, FixedLengthText<{ SERVER_VER_LEN }>>,
    /// Seconds since Unix epoch when the binlog was created.
    create_timestamp: RawInt<LeU32>,
    /// Event header length. Aloway `19`.
    event_header_length: EventHeaderLength,
    /// An array indexed by Binlog Event Type - 1 to extract the length of the event specific
    /// header.
    ///
    /// Use [`Self::get_event_type_header_length`] to get header length for particular event type.
    event_type_header_lengths: RawBytes<'a, EofBytes>,
    /// This event structure also stores a footer containig checksum algorithm description.
    ///
    /// # Note
    ///
    /// Footer must be assigned manualy after `Self::read`
    footer: BinlogEventFooter,
}

impl<'a> FormatDescriptionEvent<'a> {
    /// Length of a server version string.
    pub const SERVER_VER_LEN: usize = SERVER_VER_LEN;
    /// Offset of a server version string.
    pub const SERVER_VER_OFFSET: usize = 2;

    // Other format-related constants
    /// Length of a query event post-header, where 3.23, 4.x and 5.0 agree.
    pub const QUERY_HEADER_MINIMAL_LEN: usize = (4 + 4 + 1 + 2);
    /// Length of a query event post-header, where 5.0 differs: 2 for length of N-bytes vars.
    pub const QUERY_HEADER_LEN: usize = Self::QUERY_HEADER_MINIMAL_LEN + 2;
    /// Length of a stop event post-header.
    pub const STOP_HEADER_LEN: usize = 0;
    /// Length of a start event post-header.
    pub const START_V3_HEADER_LEN: usize = 2 + Self::SERVER_VER_LEN + 4;
    /// Length of a rotate event post-header.
    pub const ROTATE_HEADER_LEN: usize = 8;
    /// Length of an intvar event post-header.
    pub const INTVAR_HEADER_LEN: usize = 0;
    /// Length of an append block event post-header.
    pub const APPEND_BLOCK_HEADER_LEN: usize = 4;
    /// Length of a delete file event post-header.
    pub const DELETE_FILE_HEADER_LEN: usize = 4;
    /// Length of a rand event post-header.
    pub const RAND_HEADER_LEN: usize = 0;
    /// Length of a user var event post-header.
    pub const USER_VAR_HEADER_LEN: usize = 0;
    /// Length of a fde event post-header.
    pub const FORMAT_DESCRIPTION_HEADER_LEN: usize =
        (Self::START_V3_HEADER_LEN + EventType::ENUM_END_EVENT as usize);
    /// Length of a xid event post-header.
    pub const XID_HEADER_LEN: usize = 0;
    /// Length of a begin load query event post-header.
    pub const BEGIN_LOAD_QUERY_HEADER_LEN: usize = Self::APPEND_BLOCK_HEADER_LEN;
    /// Length of a v1 rows query event post-header.
    pub const ROWS_HEADER_LEN_V1: usize = 8;
    /// Length of a table map event post-header.
    pub const TABLE_MAP_HEADER_LEN: usize = 8;
    /// Length of an execute load query event extra header.
    pub const EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN: usize = (4 + 4 + 4 + 1);
    /// Length of an execute load query event post-header.
    pub const EXECUTE_LOAD_QUERY_HEADER_LEN: usize =
        (Self::QUERY_HEADER_LEN + Self::EXECUTE_LOAD_QUERY_EXTRA_HEADER_LEN);
    /// Length of an incident event post-header.
    pub const INCIDENT_HEADER_LEN: usize = 2;
    /// Length of a heartbeat event post-header.
    pub const HEARTBEAT_HEADER_LEN: usize = 0;
    /// Length of an ignorable event post-header.
    pub const IGNORABLE_HEADER_LEN: usize = 0;
    /// Length of a rows events post-header.
    pub const ROWS_HEADER_LEN_V2: usize = 10;
    /// Length of a gtid events post-header.
    pub const GTID_HEADER_LEN: usize = 42;
    /// Length of an incident event post-header.
    pub const TRANSACTION_CONTEXT_HEADER_LEN: usize = 18;
    /// Length of a view change event post-header.
    pub const VIEW_CHANGE_HEADER_LEN: usize = 52;
    /// Length of a xa prepare event post-header.
    pub const XA_PREPARE_HEADER_LEN: usize = 0;
    /// Length of a transaction payload event post-header.
    pub const TRANSACTION_PAYLOAD_HEADER_LEN: usize = 0;

    /// Creates new instance.
    pub fn new(binlog_version: BinlogVersion) -> Self {
        Self {
            binlog_version: Const::new(binlog_version),
            server_version: Default::default(),
            create_timestamp: Default::default(),
            event_header_length: Default::default(),
            event_type_header_lengths: Default::default(),
            footer: Default::default(),
        }
    }

    /// Defines the `server_version` field.
    pub fn with_binlog_version(mut self, binlog_version: BinlogVersion) -> Self {
        self.binlog_version = Const::new(binlog_version);
        self
    }

    /// Defines the `server_version` field.
    pub fn with_server_version(mut self, server_version: impl Into<Cow<'a, [u8]>>) -> Self {
        self.server_version = RawBytes::new(server_version);
        self
    }

    /// Defines the `server_version` field.
    pub fn with_create_timestamp(mut self, create_timestamp: u32) -> Self {
        self.create_timestamp = RawInt::new(create_timestamp);
        self
    }

    /// Defines the `server_version` field.
    pub fn with_event_type_header_lengths(
        mut self,
        event_type_header_lengths: impl Into<Cow<'a, [u8]>>,
    ) -> Self {
        self.event_type_header_lengths = RawBytes::new(event_type_header_lengths);
        self
    }

    /// Defines the `server_version` field.
    pub fn with_footer(mut self, footer: BinlogEventFooter) -> Self {
        self.footer = footer;
        self
    }

    /// Returns the `binlog_version` field value.
    pub fn binlog_version(&self) -> BinlogVersion {
        self.binlog_version.0
    }

    /// Returns the `server_version` field value.
    ///
    /// `server_version` is the version of the MySQL Server that created the binlog.
    pub fn server_version_raw(&'a self) -> &'a [u8] {
        self.server_version.as_bytes()
    }

    /// Returns the `server_version` field value as string (lossy converted).
    pub fn server_version(&'a self) -> Cow<'a, str> {
        self.server_version.as_str()
    }

    /// Returns the `create_timestamp` field value.
    ///
    /// `created_timestamp` is the creation timestamp, if non-zero, is the time in seconds
    /// when this event was created.
    pub fn create_timestamp(&self) -> u32 {
        self.create_timestamp.0
    }

    /// Returns the `event_header_length` field value.
    ///
    /// `event_header_length` is the length of the event header. This value includes the
    /// `extra_headers` field, so this header length is `19`.
    pub fn event_header_length(&self) -> u8 {
        self.event_header_length.value()
    }

    /// Returns the `event_type_header_lengths` field value.
    ///
    /// `event_type_header_lengths` is the lengths for the fixed data part of each event. An array
    /// indexed by Binlog Event Type - 1.
    pub fn event_type_header_lengths(&'a self) -> &'a [u8] {
        self.event_type_header_lengths.as_bytes()
    }

    /// Returns the `footer` field value.
    pub fn footer(&self) -> BinlogEventFooter {
        self.footer
    }

    pub(crate) fn footer_mut(&mut self) -> &mut BinlogEventFooter {
        &mut self.footer
    }

    /// Returns a parsed MySql version.
    pub fn split_version(&self) -> (u8, u8, u8) {
        crate::misc::split_version(&self.server_version.0)
    }

    /// Returns header length for the given event type, if defined.
    pub fn get_event_type_header_length(&self, event_type: EventType) -> u8 {
        if event_type == EventType::UNKNOWN_EVENT {
            return 0;
        }

        self.event_type_header_lengths
            .as_bytes()
            .get(usize::from(event_type as u8).saturating_sub(1))
            .copied()
            .unwrap_or_else(|| match event_type {
                EventType::UNKNOWN_EVENT => 0,
                EventType::START_EVENT_V3 => Self::START_V3_HEADER_LEN,
                EventType::QUERY_EVENT => Self::QUERY_HEADER_LEN,
                EventType::STOP_EVENT => Self::STOP_HEADER_LEN,
                EventType::ROTATE_EVENT => Self::ROTATE_HEADER_LEN,
                EventType::INTVAR_EVENT => Self::INTVAR_HEADER_LEN,
                EventType::LOAD_EVENT => 0,
                EventType::SLAVE_EVENT => 0,
                EventType::CREATE_FILE_EVENT => 0,
                EventType::APPEND_BLOCK_EVENT => Self::APPEND_BLOCK_HEADER_LEN,
                EventType::EXEC_LOAD_EVENT => 0,
                EventType::DELETE_FILE_EVENT => Self::DELETE_FILE_HEADER_LEN,
                EventType::NEW_LOAD_EVENT => 0,
                EventType::RAND_EVENT => Self::RAND_HEADER_LEN,
                EventType::USER_VAR_EVENT => Self::USER_VAR_HEADER_LEN,
                EventType::FORMAT_DESCRIPTION_EVENT => Self::FORMAT_DESCRIPTION_HEADER_LEN,
                EventType::XID_EVENT => Self::XID_HEADER_LEN,
                EventType::BEGIN_LOAD_QUERY_EVENT => Self::BEGIN_LOAD_QUERY_HEADER_LEN,
                EventType::EXECUTE_LOAD_QUERY_EVENT => Self::EXECUTE_LOAD_QUERY_HEADER_LEN,
                EventType::TABLE_MAP_EVENT => Self::TABLE_MAP_HEADER_LEN,
                EventType::PRE_GA_WRITE_ROWS_EVENT => 0,
                EventType::PRE_GA_UPDATE_ROWS_EVENT => 0,
                EventType::PRE_GA_DELETE_ROWS_EVENT => 0,
                EventType::WRITE_ROWS_EVENT_V1 => Self::ROWS_HEADER_LEN_V1,
                EventType::UPDATE_ROWS_EVENT_V1 => Self::ROWS_HEADER_LEN_V1,
                EventType::DELETE_ROWS_EVENT_V1 => Self::ROWS_HEADER_LEN_V1,
                EventType::INCIDENT_EVENT => Self::INCIDENT_HEADER_LEN,
                EventType::HEARTBEAT_EVENT => 0,
                EventType::IGNORABLE_EVENT => Self::IGNORABLE_HEADER_LEN,
                EventType::ROWS_QUERY_EVENT => Self::IGNORABLE_HEADER_LEN,
                EventType::WRITE_ROWS_EVENT => Self::ROWS_HEADER_LEN_V2,
                EventType::UPDATE_ROWS_EVENT => Self::ROWS_HEADER_LEN_V2,
                EventType::DELETE_ROWS_EVENT => Self::ROWS_HEADER_LEN_V2,
                EventType::GTID_EVENT => Self::GTID_HEADER_LEN,
                EventType::ANONYMOUS_GTID_EVENT => Self::GTID_HEADER_LEN,
                EventType::PREVIOUS_GTIDS_EVENT => Self::IGNORABLE_HEADER_LEN,
                EventType::TRANSACTION_CONTEXT_EVENT => Self::TRANSACTION_CONTEXT_HEADER_LEN,
                EventType::VIEW_CHANGE_EVENT => Self::VIEW_CHANGE_HEADER_LEN,
                EventType::XA_PREPARE_LOG_EVENT => Self::XA_PREPARE_HEADER_LEN,
                EventType::PARTIAL_UPDATE_ROWS_EVENT => Self::ROWS_HEADER_LEN_V2,
                EventType::TRANSACTION_PAYLOAD_EVENT => Self::TRANSACTION_PAYLOAD_HEADER_LEN,
                EventType::ENUM_END_EVENT => 0,
            } as u8)
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> FormatDescriptionEvent<'static> {
        FormatDescriptionEvent {
            binlog_version: self.binlog_version,
            server_version: self.server_version.into_owned(),
            create_timestamp: self.create_timestamp,
            event_header_length: self.event_header_length,
            event_type_header_lengths: self.event_type_header_lengths.into_owned(),
            footer: self.footer,
        }
    }
}

impl<'de> MyDeserialize<'de> for FormatDescriptionEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(57)?;
        let binlog_version = sbuf.parse_unchecked(())?;
        let server_version = sbuf.parse_unchecked(())?;
        let create_timestamp = sbuf.parse_unchecked(())?;
        let event_header_length = sbuf.parse_unchecked(())?;

        let event_type_header_lengths = buf.parse(())?;

        Ok(Self {
            binlog_version,
            server_version,
            create_timestamp,
            event_header_length,
            event_type_header_lengths,
            footer: Default::default(),
        })
    }
}

impl MySerialize for FormatDescriptionEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.binlog_version.serialize(&mut *buf);
        self.server_version.serialize(&mut *buf);
        self.create_timestamp.serialize(&mut *buf);
        self.event_header_length.serialize(&mut *buf);
        self.event_type_header_lengths.serialize(&mut *buf);
    }
}

impl<'a> BinlogStruct<'a> for FormatDescriptionEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(2);
        len += S(Self::SERVER_VER_LEN);
        len += S(4);
        len += S(1);
        len += S(self.event_type_header_lengths.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}

impl<'a> BinlogEvent<'a> for FormatDescriptionEvent<'a> {
    const EVENT_TYPE: EventType = EventType::FORMAT_DESCRIPTION_EVENT;
}
