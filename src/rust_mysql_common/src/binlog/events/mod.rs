// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bitvec::prelude::*;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::BufMut;
use saturating::Saturating as S;

use crate::{
    io::ParseBuf,
    misc::raw::{int::*, RawConst, RawFlags},
    proto::{MyDeserialize, MySerialize},
};

pub use self::{
    anonymous_gtid_event::AnonymousGtidEvent,
    begin_load_query_event::BeginLoadQueryEvent,
    delete_rows_event::DeleteRowsEvent,
    delete_rows_event_v1::DeleteRowsEventV1,
    execute_load_query_event::ExecuteLoadQueryEvent,
    format_description_event::FormatDescriptionEvent,
    gtid_event::GtidEvent,
    incident_event::IncidentEvent,
    intvar_event::IntvarEvent,
    partial_update_rows_event::PartialUpdateRowsEvent,
    query_event::{QueryEvent, StatusVar, StatusVarVal, StatusVars, StatusVarsIterator},
    rand_event::RandEvent,
    rotate_event::RotateEvent,
    rows_event::{RowsEvent, RowsEventRows},
    rows_query_event::RowsQueryEvent,
    table_map_event::*,
    transaction_payload_event::{TransactionPayloadEvent, TransactionPayloadReader},
    update_rows_event::UpdateRowsEvent,
    update_rows_event_v1::UpdateRowsEventV1,
    user_var_event::UserVarEvent,
    write_rows_event::WriteRowsEvent,
    write_rows_event_v1::WriteRowsEventV1,
    xid_event::XidEvent,
};

use std::{
    any::type_name,
    borrow::Cow,
    cmp::min,
    io::{self, Read, Write},
    u16,
};

use super::{
    consts::{
        BinlogChecksumAlg, BinlogVersion, EventFlags, EventType, RowsEventFlags,
        UnknownChecksumAlg, UnknownEventType,
    },
    misc::LimitWrite,
    BinlogCtx, BinlogEvent,
};

mod anonymous_gtid_event;
mod begin_load_query_event;
mod delete_rows_event;
mod delete_rows_event_v1;
mod execute_load_query_event;
mod format_description_event;
mod gtid_event;
mod incident_event;
mod intvar_event;
mod partial_update_rows_event;
mod query_event;
mod rand_event;
mod rotate_event;
mod rows_event;
mod rows_query_event;
mod table_map_event;
mod transaction_payload_event;
mod update_rows_event;
mod update_rows_event_v1;
mod user_var_event;
mod write_rows_event;
mod write_rows_event_v1;
mod xid_event;

/// Raw binlog event.
///
/// A binlog event starts with a Binlog Event header and is followed by a Binlog Event Type
/// specific data part.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Event {
    /// Format description event.
    fde: FormatDescriptionEvent<'static>,
    /// Common header of an event.
    header: BinlogEventHeader,
    /// An event-type specific data.
    ///
    /// Checksum-related suffix is truncated:
    ///
    /// *   checksum algorithm description (for fde) will go to `footer`;
    /// *   checksum will go to `checksum`.
    data: Vec<u8>,
    /// Log event footer.
    footer: BinlogEventFooter,
    /// Event checksum.
    ///
    /// Makes sense only if checksum algorithm is defined in `footer`.
    checksum: [u8; BinlogEventFooter::BINLOG_CHECKSUM_LEN],
}

impl Event {
    /// Reads an event from `input`.
    pub fn read<'a, T: Read>(
        fde: &'a FormatDescriptionEvent<'a>,
        mut input: T,
    ) -> io::Result<Self> {
        let binlog_header_len = BinlogEventHeader::LEN;
        let mut fde = fde.clone().into_owned();

        let mut header_buf = [0u8; BinlogEventHeader::LEN];
        input.read_exact(&mut header_buf)?;
        let header = BinlogEventHeader::deserialize((), &mut ParseBuf(&header_buf))?;

        let mut data = vec![0_u8; (S(header.event_size() as usize) - S(binlog_header_len)).0];
        input.read_exact(&mut data).unwrap();

        let is_fde = header.event_type.0 == EventType::FORMAT_DESCRIPTION_EVENT as u8;
        let mut bytes_to_truncate = 0;
        let mut checksum = [0_u8; BinlogEventFooter::BINLOG_CHECKSUM_LEN];

        let footer = if is_fde {
            let footer = BinlogEventFooter::read(&data)?;
            if footer.checksum_alg.is_some() {
                // truncate checksum algorithm description
                bytes_to_truncate += BinlogEventFooter::BINLOG_CHECKSUM_ALG_DESC_LEN;
            }
            // We'll update dummy fde footer
            fde = fde.with_footer(footer);
            footer
        } else {
            fde.footer()
        };

        // * fde will always contain checksum (see WL#2540)
        // * events inside of a Transaction_payload_event are not checksummed (see WL#3549)
        let contains_checksum = footer.checksum_alg.is_some()
            && (is_fde || footer.checksum_alg != Some(RawConst::new(0)))
            && footer.checksum_enabled;

        if contains_checksum {
            // truncate checksum
            bytes_to_truncate += BinlogEventFooter::BINLOG_CHECKSUM_LEN;
            checksum.copy_from_slice(&data[data.len() - BinlogEventFooter::BINLOG_CHECKSUM_LEN..]);
        }

        data.truncate(data.len() - bytes_to_truncate);

        Ok(Self {
            fde,
            header,
            data,
            footer,
            checksum,
        })
    }

    /// Writes this event into the `output`.
    pub fn write<T: Write>(&self, version: BinlogVersion, mut output: T) -> io::Result<()> {
        let is_fde = self.header.event_type.0 == EventType::FORMAT_DESCRIPTION_EVENT as u8;
        let mut output = output.limit(S(self.len(version)));

        let mut header_buf = Vec::with_capacity(BinlogEventHeader::LEN);
        self.header.serialize(&mut header_buf);
        output.write_all(&header_buf)?;
        output.write_all(&self.data)?;

        if let Ok(Some(alg)) = self.footer.get_checksum_alg() {
            if is_fde {
                output.write_u8(alg as u8)?;
            }
            if alg == BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32 || is_fde {
                output.write_u32::<LittleEndian>(self.calc_checksum(alg))?;
            }
        }

        Ok(())
    }

    /// Returns a length of a serialized representation of this event.
    fn len(&self, _version: BinlogVersion) -> usize {
        let is_fde = self.header.event_type.0 == EventType::FORMAT_DESCRIPTION_EVENT as u8;
        let mut len = S(0);

        len += S(BinlogEventHeader::LEN);
        len += S(self.data.len());
        if let Ok(Some(alg)) = self.footer.get_checksum_alg() {
            if is_fde {
                len += S(BinlogEventFooter::BINLOG_CHECKSUM_ALG_DESC_LEN);
            }
            if is_fde || alg != BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_OFF {
                len += S(BinlogEventFooter::BINLOG_CHECKSUM_LEN);
            }
        }

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }

    /// Returns a reference to the corresponding format description event.
    pub fn fde(&self) -> &FormatDescriptionEvent<'static> {
        &self.fde
    }

    /// Returns a reference to the event header.
    pub fn header(&self) -> BinlogEventHeader {
        self.header
    }

    /// Returns a reference to the event data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns a reference to the event footer.
    pub fn footer(&self) -> BinlogEventFooter {
        self.footer
    }

    /// Returns the checksum, if it is defined.
    pub fn checksum(&self) -> Option<[u8; BinlogEventFooter::BINLOG_CHECKSUM_LEN]> {
        let contains_checksum = self.footer.checksum_alg.is_some()
            && (self.header.event_type.0 == (EventType::FORMAT_DESCRIPTION_EVENT as u8)
                || self.footer.checksum_alg != Some(RawConst::new(0)));
        contains_checksum.then_some(self.checksum)
    }

    /// Read event-type specific data as a binlog struct.
    pub fn read_event<'a, T: BinlogEvent<'a>>(&'a self) -> io::Result<T> {
        // we'll use data.len() here because of truncated event footer
        let event_size = BinlogEventHeader::LEN + self.data.len();
        let event_data = &mut ParseBuf(&self.data);
        let ctx = BinlogCtx::new(event_size, &self.fde);

        let event = event_data.parse(ctx)?;

        // it is an error if the `event_data` isn't fully consumed
        if !event_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "bytes remaining on stream while reading {}",
                    type_name::<T>()
                ),
            ));
        }

        Ok(event)
    }

    /// Reads event data. Returns `None` if event type is unknown.
    pub fn read_data(&self) -> io::Result<Option<EventData<'_>>> {
        use EventType::*;

        let event_type = match self.header.event_type.get() {
            Ok(event_type) => event_type,
            _ => return Ok(None),
        };

        let event_data = match event_type {
            ENUM_END_EVENT | UNKNOWN_EVENT => EventData::UnknownEvent,
            START_EVENT_V3 => EventData::StartEventV3(Cow::Borrowed(&*self.data)),
            QUERY_EVENT => EventData::QueryEvent(self.read_event()?),
            STOP_EVENT => EventData::StopEvent,
            ROTATE_EVENT => EventData::RotateEvent(self.read_event()?),
            INTVAR_EVENT => EventData::IntvarEvent(self.read_event()?),
            LOAD_EVENT => EventData::LoadEvent(Cow::Borrowed(&*self.data)),
            SLAVE_EVENT => EventData::SlaveEvent,
            CREATE_FILE_EVENT => EventData::CreateFileEvent(Cow::Borrowed(&*self.data)),
            APPEND_BLOCK_EVENT => EventData::AppendBlockEvent(Cow::Borrowed(&*self.data)),
            EXEC_LOAD_EVENT => EventData::ExecLoadEvent(Cow::Borrowed(&*self.data)),
            DELETE_FILE_EVENT => EventData::DeleteFileEvent(Cow::Borrowed(&*self.data)),
            NEW_LOAD_EVENT => EventData::NewLoadEvent(Cow::Borrowed(&*self.data)),
            RAND_EVENT => EventData::RandEvent(self.read_event()?),
            USER_VAR_EVENT => EventData::UserVarEvent(self.read_event()?),
            FORMAT_DESCRIPTION_EVENT => {
                let fde = self
                    .read_event::<FormatDescriptionEvent>()?
                    .with_footer(self.footer);
                EventData::FormatDescriptionEvent(fde)
            }
            XID_EVENT => EventData::XidEvent(self.read_event()?),
            BEGIN_LOAD_QUERY_EVENT => EventData::BeginLoadQueryEvent(self.read_event()?),
            EXECUTE_LOAD_QUERY_EVENT => EventData::ExecuteLoadQueryEvent(self.read_event()?),
            TABLE_MAP_EVENT => EventData::TableMapEvent(self.read_event()?),
            PRE_GA_WRITE_ROWS_EVENT => EventData::PreGaWriteRowsEvent(Cow::Borrowed(&*self.data)),
            PRE_GA_UPDATE_ROWS_EVENT => EventData::PreGaUpdateRowsEvent(Cow::Borrowed(&*self.data)),
            PRE_GA_DELETE_ROWS_EVENT => EventData::PreGaDeleteRowsEvent(Cow::Borrowed(&*self.data)),
            WRITE_ROWS_EVENT_V1 => {
                EventData::RowsEvent(RowsEventData::WriteRowsEventV1(self.read_event()?))
            }
            UPDATE_ROWS_EVENT_V1 => {
                EventData::RowsEvent(RowsEventData::UpdateRowsEventV1(self.read_event()?))
            }
            DELETE_ROWS_EVENT_V1 => {
                EventData::RowsEvent(RowsEventData::DeleteRowsEventV1(self.read_event()?))
            }
            INCIDENT_EVENT => EventData::IncidentEvent(self.read_event()?),
            HEARTBEAT_EVENT => EventData::HeartbeatEvent,
            IGNORABLE_EVENT => EventData::IgnorableEvent(Cow::Borrowed(&*self.data)),
            ROWS_QUERY_EVENT => EventData::RowsQueryEvent(self.read_event()?),
            WRITE_ROWS_EVENT => {
                EventData::RowsEvent(RowsEventData::WriteRowsEvent(self.read_event()?))
            }
            UPDATE_ROWS_EVENT => {
                EventData::RowsEvent(RowsEventData::UpdateRowsEvent(self.read_event()?))
            }
            DELETE_ROWS_EVENT => {
                EventData::RowsEvent(RowsEventData::DeleteRowsEvent(self.read_event()?))
            }
            GTID_EVENT => EventData::GtidEvent(self.read_event()?),
            ANONYMOUS_GTID_EVENT => EventData::AnonymousGtidEvent(self.read_event()?),
            PREVIOUS_GTIDS_EVENT => EventData::PreviousGtidsEvent(Cow::Borrowed(&*self.data)),
            TRANSACTION_CONTEXT_EVENT => {
                EventData::TransactionContextEvent(Cow::Borrowed(&*self.data))
            }
            VIEW_CHANGE_EVENT => EventData::ViewChangeEvent(Cow::Borrowed(&*self.data)),
            XA_PREPARE_LOG_EVENT => EventData::XaPrepareLogEvent(Cow::Borrowed(&*self.data)),
            PARTIAL_UPDATE_ROWS_EVENT => {
                EventData::RowsEvent(RowsEventData::PartialUpdateRowsEvent(self.read_event()?))
            }
            TRANSACTION_PAYLOAD_EVENT => EventData::TransactionPayloadEvent(self.read_event()?),
        };

        Ok(Some(event_data))
    }

    /// Calculates checksum for this event.
    pub fn calc_checksum(&self, alg: BinlogChecksumAlg) -> u32 {
        let is_fde = self.header.event_type.0 == EventType::FORMAT_DESCRIPTION_EVENT as u8;

        let mut hasher = crc32fast::Hasher::new();
        let mut header = Vec::with_capacity(BinlogEventHeader::LEN);
        let mut header_struct = self.header;
        if header_struct
            .flags
            .get()
            .contains(EventFlags::LOG_EVENT_BINLOG_IN_USE_F)
        {
            // In case this is a Format_description_log_event, we need to clear
            // the LOG_EVENT_BINLOG_IN_USE_F flag before computing the checksum,
            // since the flag will be cleared when the binlog is closed.
            // On verification, the flag is also dropped before computing the checksum.
            header_struct.flags.0 &= !(EventFlags::LOG_EVENT_BINLOG_IN_USE_F.bits());
        }
        header_struct.serialize(&mut header);
        hasher.update(&header);
        hasher.update(&self.data);
        if is_fde {
            hasher.update(&[alg as u8][..]);
        }
        hasher.finalize()
    }
}

/// The binlog event header starts each event and is 19 bytes long assuming binlog version >= 4.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BinlogEventHeader {
    /// Seconds since unix epoch.
    timestamp: RawInt<LeU32>,
    /// Raw event Type.
    event_type: RawConst<u8, EventType>,
    /// Server-id of the originating mysql-server.
    ///
    /// Used to filter out events in circular replication.
    server_id: RawInt<LeU32>,
    /// Size of the event (header, post-header, body).
    event_size: RawInt<LeU32>,
    /// Position of the next event.
    log_pos: RawInt<LeU32>,
    /// Binlog Event Flag.
    ///
    /// This field contains the raw value. Use [`Self::flags()`] to get the actual flags.
    flags: RawFlags<EventFlags, LeU16>,
}

impl BinlogEventHeader {
    /// Binlog event header length for version >= 4.
    pub const LEN: usize = 19;

    /// Creates a new `BinlogEventHeader`.
    pub fn new(
        timestamp: u32,
        event_type: EventType,
        server_id: u32,
        event_size: u32,
        log_pos: u32,
        flags: EventFlags,
    ) -> Self {
        Self {
            timestamp: RawInt::new(timestamp),
            event_type: RawConst::new(event_type as u8),
            server_id: RawInt::new(server_id),
            event_size: RawInt::new(event_size),
            log_pos: RawInt::new(log_pos),
            flags: RawFlags::new(flags.bits()),
        }
    }

    /// Returns the `timestamp` value.
    ///
    /// `timestamp` is in seconds since unix epoch.
    pub fn timestamp(&self) -> u32 {
        self.timestamp.0
    }

    /// Returns the raw event type.
    pub fn event_type_raw(&self) -> u8 {
        self.event_type.0
    }

    /// Returns the event type, if it's valid.
    pub fn event_type(&self) -> Result<EventType, UnknownEventType> {
        self.event_type.get()
    }

    /// Returns the server Id of the originating mysql-server.
    ///
    /// Used to filter out events in circular replication.
    pub fn server_id(&self) -> u32 {
        self.server_id.0
    }

    /// Returns the size of the event (header, post-header, body).
    pub fn event_size(&self) -> u32 {
        self.event_size.0
    }

    /// Returns the position of the next event.
    pub fn log_pos(&self) -> u32 {
        self.log_pos.0
    }

    /// Returns event flags (unknown bits are truncated).
    pub fn flags(&self) -> EventFlags {
        self.flags.get()
    }

    /// Returns raw event flags (unknown bits are preserved).
    pub fn flags_raw(&self) -> u16 {
        self.flags.0
    }
}

impl<'de> MyDeserialize<'de> for BinlogEventHeader {
    const SIZE: Option<usize> = Some(Self::LEN);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut buf: ParseBuf = buf.parse_unchecked(Self::LEN)?;
        Ok(Self {
            timestamp: buf.parse_unchecked(())?,
            event_type: buf.parse_unchecked(())?,
            server_id: buf.parse_unchecked(())?,
            event_size: buf.parse_unchecked(())?,
            log_pos: buf.parse_unchecked(())?,
            flags: buf.parse_unchecked(())?,
        })
    }
}

impl MySerialize for BinlogEventHeader {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.timestamp.serialize(&mut *buf);
        self.event_type.serialize(&mut *buf);
        self.server_id.serialize(&mut *buf);
        self.event_size.serialize(&mut *buf);
        self.log_pos.serialize(&mut *buf);
        self.flags.serialize(&mut *buf);
    }
}

/// Binlog event footer.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BinlogEventFooter {
    /// Raw checksum algorithm description.
    checksum_alg: Option<RawConst<u8, BinlogChecksumAlg>>,

    /// Checksum enabled
    checksum_enabled: bool,
}

impl BinlogEventFooter {
    /// Length of the checksum algorithm description.
    pub const BINLOG_CHECKSUM_ALG_DESC_LEN: usize = 1;
    /// Length of the checksum.
    pub const BINLOG_CHECKSUM_LEN: usize = 4;
    /// Minimum MySql version that supports checksums.
    pub const CHECKSUM_VERSION_PRODUCT: (u8, u8, u8) = (5, 6, 1);

    pub fn new(checksum_alg: BinlogChecksumAlg) -> Self {
        Self {
            checksum_alg: Some(RawConst::new(checksum_alg as u8)),
            checksum_enabled: true,
        }
    }

    /// Returns parsed checksum algorithm, or raw value if algorithm is unknown.
    pub fn get_checksum_alg(&self) -> Result<Option<BinlogChecksumAlg>, UnknownChecksumAlg> {
        self.checksum_alg.as_ref().map(RawConst::get).transpose()
    }

    /// Returns `true` if checksum is enabled.
    pub fn get_checksum_enabled(&self) -> bool {
        self.checksum_enabled
    }

    /// Set checksum enabled flag.
    pub fn set_checksum_enabled(&mut self, enabled: bool) {
        self.checksum_enabled = enabled;
    }

    /// Reads binlog event footer from the given buffer.
    ///
    /// Requires that buf contains `FormatDescriptionEvent` data.
    pub fn read(buf: &[u8]) -> io::Result<Self> {
        let checksum_alg = if buf.len()
            >= FormatDescriptionEvent::SERVER_VER_OFFSET + FormatDescriptionEvent::SERVER_VER_LEN
        {
            let mut server_version = vec![0_u8; FormatDescriptionEvent::SERVER_VER_LEN];
            (&buf[FormatDescriptionEvent::SERVER_VER_OFFSET..]).read_exact(&mut server_version)?;
            server_version[FormatDescriptionEvent::SERVER_VER_LEN - 1] = 0;
            let version = crate::misc::split_version(&server_version);
            if version < Self::CHECKSUM_VERSION_PRODUCT {
                None
            } else {
                let offset = buf.len()
                    - (BinlogEventFooter::BINLOG_CHECKSUM_ALG_DESC_LEN
                        + BinlogEventFooter::BINLOG_CHECKSUM_LEN);
                Some(buf[offset])
            }
        } else {
            None
        };

        Ok(Self {
            checksum_alg: checksum_alg.map(RawConst::new),
            checksum_enabled: true,
        })
    }
}

impl Default for BinlogEventFooter {
    fn default() -> Self {
        BinlogEventFooter {
            checksum_alg: Some(RawConst::new(
                BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_OFF as u8,
            )),
            checksum_enabled: true,
        }
    }
}

/// Parsed event data.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum EventData<'a> {
    UnknownEvent,
    /// Ignored by this implementation
    StartEventV3(Cow<'a, [u8]>),
    QueryEvent(QueryEvent<'a>),
    StopEvent,
    RotateEvent(RotateEvent<'a>),
    IntvarEvent(IntvarEvent),
    /// Ignored by this implementation
    LoadEvent(Cow<'a, [u8]>),
    SlaveEvent,
    CreateFileEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    AppendBlockEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    ExecLoadEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    DeleteFileEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    NewLoadEvent(Cow<'a, [u8]>),
    RandEvent(RandEvent),
    UserVarEvent(UserVarEvent<'a>),
    FormatDescriptionEvent(FormatDescriptionEvent<'a>),
    XidEvent(XidEvent),
    BeginLoadQueryEvent(BeginLoadQueryEvent<'a>),
    ExecuteLoadQueryEvent(ExecuteLoadQueryEvent<'a>),
    TableMapEvent(TableMapEvent<'a>),
    /// Ignored by this implementation
    PreGaWriteRowsEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    PreGaUpdateRowsEvent(Cow<'a, [u8]>),
    /// Ignored by this implementation
    PreGaDeleteRowsEvent(Cow<'a, [u8]>),
    IncidentEvent(IncidentEvent<'a>),
    HeartbeatEvent,
    IgnorableEvent(Cow<'a, [u8]>),
    RowsQueryEvent(RowsQueryEvent<'a>),
    GtidEvent(GtidEvent),
    /// Not yet implemented.
    AnonymousGtidEvent(AnonymousGtidEvent),
    /// Not yet implemented.
    PreviousGtidsEvent(Cow<'a, [u8]>),
    /// Not yet implemented.
    TransactionContextEvent(Cow<'a, [u8]>),
    /// Not yet implemented.
    ViewChangeEvent(Cow<'a, [u8]>),
    /// Not yet implemented.
    XaPrepareLogEvent(Cow<'a, [u8]>),
    RowsEvent(RowsEventData<'a>),
    TransactionPayloadEvent(TransactionPayloadEvent<'a>),
}

impl<'a> EventData<'a> {
    pub fn into_owned(self) -> EventData<'static> {
        match self {
            EventData::UnknownEvent => EventData::UnknownEvent,
            EventData::StartEventV3(ev) => EventData::StartEventV3(Cow::Owned(ev.into_owned())),
            Self::QueryEvent(ev) => EventData::QueryEvent(ev.into_owned()),
            Self::StopEvent => EventData::StopEvent,
            Self::RotateEvent(ev) => EventData::RotateEvent(ev.into_owned()),
            Self::IntvarEvent(ev) => EventData::IntvarEvent(ev),
            Self::LoadEvent(ev) => EventData::LoadEvent(Cow::Owned(ev.into_owned())),
            Self::SlaveEvent => EventData::SlaveEvent,
            Self::CreateFileEvent(ev) => EventData::CreateFileEvent(Cow::Owned(ev.into_owned())),
            Self::AppendBlockEvent(ev) => EventData::AppendBlockEvent(Cow::Owned(ev.into_owned())),
            Self::ExecLoadEvent(ev) => EventData::ExecLoadEvent(Cow::Owned(ev.into_owned())),
            Self::DeleteFileEvent(ev) => EventData::DeleteFileEvent(Cow::Owned(ev.into_owned())),
            Self::NewLoadEvent(ev) => EventData::NewLoadEvent(Cow::Owned(ev.into_owned())),
            Self::RandEvent(ev) => EventData::RandEvent(ev),
            Self::UserVarEvent(ev) => EventData::UserVarEvent(ev.into_owned()),
            Self::FormatDescriptionEvent(ev) => EventData::FormatDescriptionEvent(ev.into_owned()),
            Self::XidEvent(ev) => EventData::XidEvent(ev),
            Self::BeginLoadQueryEvent(ev) => EventData::BeginLoadQueryEvent(ev.into_owned()),
            Self::ExecuteLoadQueryEvent(ev) => EventData::ExecuteLoadQueryEvent(ev.into_owned()),
            Self::TableMapEvent(ev) => EventData::TableMapEvent(ev.into_owned()),
            Self::PreGaWriteRowsEvent(ev) => {
                EventData::PreGaWriteRowsEvent(Cow::Owned(ev.into_owned()))
            }
            Self::PreGaUpdateRowsEvent(ev) => {
                EventData::PreGaUpdateRowsEvent(Cow::Owned(ev.into_owned()))
            }
            Self::PreGaDeleteRowsEvent(ev) => {
                EventData::PreGaDeleteRowsEvent(Cow::Owned(ev.into_owned()))
            }
            Self::IncidentEvent(ev) => EventData::IncidentEvent(ev.into_owned()),
            Self::HeartbeatEvent => EventData::HeartbeatEvent,
            Self::IgnorableEvent(ev) => EventData::IgnorableEvent(Cow::Owned(ev.into_owned())),
            Self::RowsQueryEvent(ev) => EventData::RowsQueryEvent(ev.into_owned()),
            Self::GtidEvent(ev) => EventData::GtidEvent(ev),
            Self::AnonymousGtidEvent(ev) => EventData::AnonymousGtidEvent(ev),
            Self::PreviousGtidsEvent(ev) => {
                EventData::PreviousGtidsEvent(Cow::Owned(ev.into_owned()))
            }
            Self::TransactionContextEvent(ev) => {
                EventData::TransactionContextEvent(Cow::Owned(ev.into_owned()))
            }
            Self::ViewChangeEvent(ev) => EventData::ViewChangeEvent(Cow::Owned(ev.into_owned())),
            Self::XaPrepareLogEvent(ev) => {
                EventData::XaPrepareLogEvent(Cow::Owned(ev.into_owned()))
            }
            Self::RowsEvent(ev) => EventData::RowsEvent(ev.into_owned()),
            Self::TransactionPayloadEvent(ev) => {
                EventData::TransactionPayloadEvent(ev.into_owned())
            }
        }
    }
}

impl MySerialize for EventData<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            EventData::UnknownEvent => (),
            EventData::StartEventV3(ev) => buf.put_slice(ev),
            EventData::QueryEvent(ev) => ev.serialize(buf),
            EventData::StopEvent => (),
            EventData::RotateEvent(ev) => ev.serialize(buf),
            EventData::IntvarEvent(ev) => ev.serialize(buf),
            EventData::LoadEvent(ev) => buf.put_slice(ev),
            EventData::SlaveEvent => (),
            EventData::CreateFileEvent(ev) => buf.put_slice(ev),
            EventData::AppendBlockEvent(ev) => buf.put_slice(ev),
            EventData::ExecLoadEvent(ev) => buf.put_slice(ev),
            EventData::DeleteFileEvent(ev) => buf.put_slice(ev),
            EventData::NewLoadEvent(ev) => buf.put_slice(ev),
            EventData::RandEvent(ev) => ev.serialize(buf),
            EventData::UserVarEvent(ev) => ev.serialize(buf),
            EventData::FormatDescriptionEvent(ev) => ev.serialize(buf),
            EventData::XidEvent(ev) => ev.serialize(buf),
            EventData::BeginLoadQueryEvent(ev) => ev.serialize(buf),
            EventData::ExecuteLoadQueryEvent(ev) => ev.serialize(buf),
            EventData::TableMapEvent(ev) => ev.serialize(buf),
            EventData::PreGaWriteRowsEvent(ev) => buf.put_slice(ev),
            EventData::PreGaUpdateRowsEvent(ev) => buf.put_slice(ev),
            EventData::PreGaDeleteRowsEvent(ev) => buf.put_slice(ev),
            EventData::IncidentEvent(ev) => ev.serialize(buf),
            EventData::HeartbeatEvent => (),
            EventData::IgnorableEvent(ev) => buf.put_slice(ev),
            EventData::RowsQueryEvent(ev) => ev.serialize(buf),
            EventData::GtidEvent(ev) => ev.serialize(buf),
            EventData::AnonymousGtidEvent(ev) => ev.serialize(buf),
            EventData::PreviousGtidsEvent(ev) => buf.put_slice(ev),
            EventData::TransactionContextEvent(ev) => buf.put_slice(ev),
            EventData::ViewChangeEvent(ev) => buf.put_slice(ev),
            EventData::XaPrepareLogEvent(ev) => buf.put_slice(ev),
            EventData::RowsEvent(ev) => ev.serialize(buf),
            EventData::TransactionPayloadEvent(ev) => ev.serialize(buf),
        }
    }
}

/// Rows events are unified under this enum (see [`EventData`]).
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum RowsEventData<'a> {
    WriteRowsEventV1(WriteRowsEventV1<'a>),
    UpdateRowsEventV1(UpdateRowsEventV1<'a>),
    DeleteRowsEventV1(DeleteRowsEventV1<'a>),
    WriteRowsEvent(WriteRowsEvent<'a>),
    UpdateRowsEvent(UpdateRowsEvent<'a>),
    DeleteRowsEvent(DeleteRowsEvent<'a>),
    PartialUpdateRowsEvent(PartialUpdateRowsEvent<'a>),
}

impl<'a> RowsEventData<'a> {
    /// Returns the number that identifies the table (see `TableMapEvent`).
    pub fn table_id(&self) -> u64 {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.table_id(),
            RowsEventData::UpdateRowsEventV1(ev) => ev.table_id(),
            RowsEventData::DeleteRowsEventV1(ev) => ev.table_id(),
            RowsEventData::WriteRowsEvent(ev) => ev.table_id(),
            RowsEventData::UpdateRowsEvent(ev) => ev.table_id(),
            RowsEventData::DeleteRowsEvent(ev) => ev.table_id(),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.table_id(),
        }
    }

    /// Returns the number of columns in the table.
    pub fn num_columns(&self) -> u64 {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.num_columns(),
            RowsEventData::UpdateRowsEventV1(ev) => ev.num_columns(),
            RowsEventData::DeleteRowsEventV1(ev) => ev.num_columns(),
            RowsEventData::WriteRowsEvent(ev) => ev.num_columns(),
            RowsEventData::UpdateRowsEvent(ev) => ev.num_columns(),
            RowsEventData::DeleteRowsEvent(ev) => ev.num_columns(),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.num_columns(),
        }
    }

    /// Returns columns in the before-image (only for DELETE and UPDATE).
    ///
    /// Each bit indicates whether corresponding column is used in the image.
    pub fn columns_before_image(&'a self) -> Option<&'a BitSlice<u8>> {
        match self {
            RowsEventData::WriteRowsEventV1(_) => None,
            RowsEventData::UpdateRowsEventV1(ev) => Some(ev.columns_before_image()),
            RowsEventData::DeleteRowsEventV1(ev) => Some(ev.columns_before_image()),
            RowsEventData::WriteRowsEvent(_) => None,
            RowsEventData::UpdateRowsEvent(ev) => Some(ev.columns_before_image()),
            RowsEventData::DeleteRowsEvent(ev) => Some(ev.columns_before_image()),
            RowsEventData::PartialUpdateRowsEvent(ev) => Some(ev.columns_before_image()),
        }
    }

    /// Returns columns in the after-image (only for WRITE and UPDATE).
    ///
    /// Each bit indicates whether corresponding column is used in the image.
    pub fn columns_after_image(&'a self) -> Option<&'a BitSlice<u8>> {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => Some(ev.columns_after_image()),
            RowsEventData::UpdateRowsEventV1(ev) => Some(ev.columns_after_image()),
            RowsEventData::DeleteRowsEventV1(_) => None,
            RowsEventData::WriteRowsEvent(ev) => Some(ev.columns_after_image()),
            RowsEventData::UpdateRowsEvent(ev) => Some(ev.columns_after_image()),
            RowsEventData::DeleteRowsEvent(_) => None,
            RowsEventData::PartialUpdateRowsEvent(ev) => Some(ev.columns_after_image()),
        }
    }

    /// Returns raw rows data.
    pub fn rows_data(&'a self) -> &'a [u8] {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.rows_data(),
            RowsEventData::UpdateRowsEventV1(ev) => ev.rows_data(),
            RowsEventData::DeleteRowsEventV1(ev) => ev.rows_data(),
            RowsEventData::WriteRowsEvent(ev) => ev.rows_data(),
            RowsEventData::UpdateRowsEvent(ev) => ev.rows_data(),
            RowsEventData::DeleteRowsEvent(ev) => ev.rows_data(),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.rows_data(),
        }
    }

    /// Returns event flags.
    pub fn flags(&self) -> RowsEventFlags {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.flags(),
            RowsEventData::UpdateRowsEventV1(ev) => ev.flags(),
            RowsEventData::DeleteRowsEventV1(ev) => ev.flags(),
            RowsEventData::WriteRowsEvent(ev) => ev.flags(),
            RowsEventData::UpdateRowsEvent(ev) => ev.flags(),
            RowsEventData::DeleteRowsEvent(ev) => ev.flags(),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.flags(),
        }
    }

    /// Returns an iterator over event's rows given the corresponding `TableMapEvent`.
    pub fn rows(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.rows(table_map_event),
            RowsEventData::UpdateRowsEventV1(ev) => ev.rows(table_map_event),
            RowsEventData::DeleteRowsEventV1(ev) => ev.rows(table_map_event),
            RowsEventData::WriteRowsEvent(ev) => ev.rows(table_map_event),
            RowsEventData::UpdateRowsEvent(ev) => ev.rows(table_map_event),
            RowsEventData::DeleteRowsEvent(ev) => ev.rows(table_map_event),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.rows(table_map_event),
        }
    }

    pub fn into_owned(self) -> RowsEventData<'static> {
        match self {
            Self::WriteRowsEventV1(ev) => RowsEventData::WriteRowsEventV1(ev.into_owned()),
            Self::UpdateRowsEventV1(ev) => RowsEventData::UpdateRowsEventV1(ev.into_owned()),
            Self::DeleteRowsEventV1(ev) => RowsEventData::DeleteRowsEventV1(ev.into_owned()),
            Self::WriteRowsEvent(ev) => RowsEventData::WriteRowsEvent(ev.into_owned()),
            Self::UpdateRowsEvent(ev) => RowsEventData::UpdateRowsEvent(ev.into_owned()),
            Self::DeleteRowsEvent(ev) => RowsEventData::DeleteRowsEvent(ev.into_owned()),
            Self::PartialUpdateRowsEvent(ev) => {
                RowsEventData::PartialUpdateRowsEvent(ev.into_owned())
            }
        }
    }
}

impl MySerialize for RowsEventData<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            RowsEventData::WriteRowsEventV1(ev) => ev.serialize(buf),
            RowsEventData::UpdateRowsEventV1(ev) => ev.serialize(buf),
            RowsEventData::DeleteRowsEventV1(ev) => ev.serialize(buf),
            RowsEventData::WriteRowsEvent(ev) => ev.serialize(buf),
            RowsEventData::UpdateRowsEvent(ev) => ev.serialize(buf),
            RowsEventData::DeleteRowsEvent(ev) => ev.serialize(buf),
            RowsEventData::PartialUpdateRowsEvent(ev) => ev.serialize(buf),
        }
    }
}
