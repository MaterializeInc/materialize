// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! Binlog-related structures and functions. This implementation assumes
//! binlog version >= 4 (MySql >= 5.0.0).
//!
//! All structures of this module contains raw data that may not necessarily be valid.
//! Please consult the MySql documentation.

// #![cfg(features = "binlog")]

use std::{
    collections::HashMap,
    convert::TryFrom,
    hash::Hash,
    io::{
        self, BufRead, Error,
        ErrorKind::{InvalidData, UnexpectedEof},
        Read, Write,
    },
};

use crate::{
    constants::ColumnType,
    proto::{MyDeserialize, MySerialize},
};

#[allow(unused)]
use self::events::TransactionPayloadEvent;

use self::{
    consts::{BinlogVersion, EventType},
    events::{Event, FormatDescriptionEvent, RotateEvent, TableMapEvent},
};

pub mod consts;
pub mod decimal;
pub mod events;
pub mod jsonb;
pub mod jsondiff;
pub mod misc;
pub mod row;
pub mod value;

pub struct BinlogCtx<'a> {
    pub event_size: usize,
    pub fde: &'a FormatDescriptionEvent<'a>,
}

impl<'a> BinlogCtx<'a> {
    pub fn new(event_size: usize, fde: &'a FormatDescriptionEvent<'a>) -> Self {
        Self { event_size, fde }
    }
}

/// Binlog event.
pub trait BinlogStruct<'a>: MySerialize + MyDeserialize<'a, Ctx = BinlogCtx<'a>> {
    /// Returns serialized length of this struct in bytes.
    ///
    /// *   implementation must truncate each field to its maximum length.
    fn len(&self, version: BinlogVersion) -> usize;
}

pub trait BinlogEvent<'a>: BinlogStruct<'a> {
    /// An event type, associated with this struct (if any).
    const EVENT_TYPE: EventType;
}

/// A binlog file starts with a Binlog File Header `[ fe 'bin' ]`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BinlogFileHeader;

impl BinlogFileHeader {
    /// Length of a binlog file header.
    pub const LEN: usize = 4;
    /// Value of a binlog file header.
    pub const VALUE: [u8; Self::LEN] = [0xfe, b'b', b'i', b'n'];

    pub fn read<T: Read>(mut input: T) -> io::Result<Self> {
        let mut buf = [0_u8; Self::LEN];
        input.read_exact(&mut buf)?;

        if buf != Self::VALUE {
            return Err(Error::new(InvalidData, "invalid binlog file header"));
        }

        Ok(Self)
    }

    pub fn write<T: Write>(&self, _version: BinlogVersion, mut output: T) -> io::Result<()> {
        output.write_all(&Self::VALUE)
    }

    pub fn len(&self, _version: BinlogVersion) -> usize {
        Self::LEN
    }
}

/// Reader for binlog events.
///
/// # Note
///
/// It's a low-level stream reader and only maintains actual fde and table map,
/// so one should properly handle encountered events (see docs on [`EventStreamReader::read`]).
#[derive(Debug)]
pub struct EventStreamReader {
    fde: FormatDescriptionEvent<'static>,
    table_map: HashMap<u64, TableMapEvent<'static>>,
}

impl EventStreamReader {
    /// Creates a new instance.
    pub fn new(version: BinlogVersion) -> Self {
        Self {
            fde: FormatDescriptionEvent::new(version),
            table_map: Default::default(),
        }
    }

    /// Returns the format description event.
    ///
    /// Returns the default placeholder if there was no FDE yet.
    pub fn get_fde(&self) -> &FormatDescriptionEvent<'static> {
        &self.fde
    }

    /// Disable/Enable checksum verification without changing the original algorithm.
    ///
    /// See [`EventStreamReader::read_decompressed`].
    pub(crate) fn set_checksum_enabled(&mut self, enabled: bool) {
        self.fde.footer_mut().set_checksum_enabled(enabled);
    }

    /// Returns the table map event for the given table id.
    ///
    /// Should be availeble if rows event with this table id encountered in the stream.
    pub fn get_tme(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.table_map.get(&table_id)
    }

    /// Will read next event from the given stream (Returns None if stream is exhausted).
    ///
    /// # Note
    ///
    /// Since MySql 8.0.20 it is possible for an event stream to contain an embedded
    /// stream of events in form of a [`TransactionPayloadEvent`]. This means that
    /// to properly handle table maps it is necessary to read the embedded stream
    /// as soon as it is encountered (see [`EventStreamReader::read_decompressed`]).
    pub fn read<T: BufRead>(&mut self, mut input: T) -> io::Result<Option<Event>> {
        if input.fill_buf().map(|x| x.is_empty())? {
            return Ok(None);
        }

        let event = Event::read(&self.fde, input)?;

        self.handle_event(&event)?;

        Ok(Some(event))
    }

    /// This function reads decompressed payload of a Transaction_payload_event
    /// (see [`TransactionPayloadEvent::decompressed`]).
    ///
    /// The difference is that checksum verification will be disabled according to the WL#3549.
    ///
    /// # Warning
    ///
    /// This function can't be used to skip checksum verification for regular events.
    ///
    /// # Errors
    ///
    /// There is a list of events that should never be a part of a transaction payload and the list
    /// includes the [`TransactionPayloadEvent`] itself.
    /// This function will emit an [`io::ErrorKind::Other`] if [`TransactionPayloadEvent`]
    /// is encountered within the compressed payload.
    pub fn read_decompressed<T: BufRead>(&mut self, input: T) -> io::Result<Option<Event>> {
        self.set_checksum_enabled(false);
        let result = self.read(input);
        self.set_checksum_enabled(true);
        let Some(event) = result? else {
            return Ok(None);
        };

        if event.header().event_type_raw() == EventType::TRANSACTION_PAYLOAD_EVENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "TRANSACTION_PAYLOAD_EVENT encountered",
            ));
        }

        self.handle_event(&event)?;

        Ok(Some(event))
    }

    fn handle_event(&mut self, event: &Event) -> io::Result<()> {
        let event_type = event.header().event_type_raw();

        if event_type == EventType::FORMAT_DESCRIPTION_EVENT as u8 {
            // we'll redefine fde with an actual one
            let fde = event.read_event::<FormatDescriptionEvent>()?;
            self.fde = fde.into_owned().with_footer(event.footer());
        } else if event_type == EventType::TABLE_MAP_EVENT as u8 {
            // we'll maintain known table maps
            let tme = event.read_event::<TableMapEvent>()?;
            self.table_map.insert(tme.table_id(), tme.into_owned());
        } else if event_type == EventType::ROTATE_EVENT as u8 {
            // we'll keep table map size within reasonlable bounds

            // TODO: This value is arbitrary
            const TABLE_MAP_MAX_SIZE: usize = 64;

            let re = event.read_event::<RotateEvent>()?;
            if !re.is_fake() {
                self.table_map.clear();
                self.table_map.shrink_to(TABLE_MAP_MAX_SIZE);
            }
        }

        Ok(())
    }
}

/// Binlog file.
///
/// It's an iterator over events in a binlog file.
#[derive(Debug)]
pub struct BinlogFile<T> {
    reader: EventStreamReader,
    read: T,
}

impl<T: BufRead> BinlogFile<T> {
    /// Creates a new instance.
    ///
    /// It'll try to read binlog file header.
    pub fn new(version: BinlogVersion, mut read: T) -> io::Result<Self> {
        let reader = EventStreamReader::new(version);
        BinlogFileHeader::read(&mut read)?;
        Ok(Self { reader, read })
    }

    /// Returns a reference to the binlog stream reader.
    pub fn reader(&self) -> &EventStreamReader {
        &self.reader
    }

    /// Returns a mutable reference to the binlog stream reader.
    pub fn reader_mut(&mut self) -> &mut EventStreamReader {
        &mut self.reader
    }
}

impl<T: BufRead> Iterator for BinlogFile<T> {
    type Item = io::Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read(&mut self.read) {
            Ok(event) => event.map(Ok),
            Err(err) if err.kind() == UnexpectedEof => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl ColumnType {
    /// Returns type-specific metadata for this column type,
    /// as well as the total number of occupied bytes.
    ///
    /// `is_array` must be true if `self` is from `MYSQL_TYPE_TYPED_ARRAY` metadata.
    fn get_metadata<'a>(&self, ptr: &'a [u8], is_array: bool) -> Option<(&'a [u8], usize)> {
        match self {
            Self::MYSQL_TYPE_TINY_BLOB
            | Self::MYSQL_TYPE_BLOB
            | Self::MYSQL_TYPE_MEDIUM_BLOB
            | Self::MYSQL_TYPE_LONG_BLOB
            | Self::MYSQL_TYPE_DOUBLE
            | Self::MYSQL_TYPE_FLOAT
            | Self::MYSQL_TYPE_GEOMETRY
            | Self::MYSQL_TYPE_TIME2
            | Self::MYSQL_TYPE_DATETIME2
            | Self::MYSQL_TYPE_TIMESTAMP2
            | Self::MYSQL_TYPE_JSON => ptr.get(..1).map(|x| (x, 1)),
            Self::MYSQL_TYPE_VARCHAR => {
                if is_array {
                    ptr.get(..3).map(|x| (x, 3))
                } else {
                    ptr.get(..2).map(|x| (x, 2))
                }
            }
            Self::MYSQL_TYPE_NEWDECIMAL
            | Self::MYSQL_TYPE_SET
            | Self::MYSQL_TYPE_ENUM
            | Self::MYSQL_TYPE_STRING
            | Self::MYSQL_TYPE_BIT => ptr.get(..2).map(|x| (x, 2)),
            Self::MYSQL_TYPE_TYPED_ARRAY => Self::try_from(*ptr.first()?)
                .ok()?
                .get_metadata(ptr.get(1..)?, true)
                .map(|(x, n)| (x, n + 1)),
            _ => Some((&[], 0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        iter::{once, repeat},
    };

    use super::{
        consts::{EventFlags, EventType},
        events::{BinlogEventHeader, EventData, GtidEvent},
        BinlogFile, BinlogFileHeader, BinlogVersion,
    };

    use crate::{
        binlog::{events::RowsEventData, value::BinlogValue},
        collations::CollationId,
        constants::ColumnFlags,
        proto::MySerialize,
        value::Value,
    };

    const BINLOG_FILE: &[u8] = &[
        0xfe, 0x62, 0x69, 0x6e, 0xfc, 0x35, 0xbb, 0x4a, 0x0f, 0x01, 0x00, 0x00, 0x00, 0x5e, 0x00,
        0x00, 0x00, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x35, 0x2e, 0x30, 0x2e, 0x38,
        0x36, 0x2d, 0x64, 0x65, 0x62, 0x75, 0x67, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xfc, 0x35, 0xbb, 0x4a, 0x13, 0x38, 0x0d, 0x00, 0x08, 0x00, 0x12, 0x00, 0x04, 0x04, 0x04,
        0x04, 0x12, 0x00, 0x00, 0x4b, 0x00, 0x04, 0x1a, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00,
        0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0xc6, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04,
        0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x63, 0x72, 0x65, 0x61,
        0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x31, 0x28, 0x61, 0x20, 0x69,
        0x6e, 0x74, 0x29, 0x20, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x20, 0x69, 0x6e, 0x6e,
        0x6f, 0x64, 0x62, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x65, 0x00, 0x00,
        0x00, 0x2b, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08,
        0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20, 0x74,
        0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x32, 0x28, 0x61, 0x20, 0x69, 0x6e, 0x74, 0x29, 0x20,
        0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x20, 0x69, 0x6e, 0x6e, 0x6f, 0x64, 0x62, 0xfd,
        0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x00, 0x70, 0x01, 0x00,
        0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a,
        0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73,
        0x71, 0x6c, 0x00, 0x42, 0x45, 0x47, 0x49, 0x4e, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00,
        0x00, 0x00, 0x5c, 0x00, 0x00, 0x00, 0xcc, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04,
        0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x69, 0x6e, 0x73, 0x65,
        0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x31, 0x20, 0x28, 0x61, 0x29, 0x20,
        0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x31, 0x29, 0xfd, 0x35, 0xbb, 0x4a, 0x02,
        0x01, 0x00, 0x00, 0x00, 0x5d, 0x00, 0x00, 0x00, 0x29, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74,
        0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00, 0x69,
        0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x32, 0x20, 0x28,
        0x61, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x31, 0x29, 0xfd, 0x35,
        0xbb, 0x4a, 0x10, 0x01, 0x00, 0x00, 0x00, 0x1b, 0x00, 0x00, 0x00, 0x44, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x35, 0xbb, 0x4a, 0x02,
        0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0xa8, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74,
        0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x63, 0x72,
        0x65, 0x61, 0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x33, 0x28, 0x61,
        0x20, 0x69, 0x6e, 0x74, 0x29, 0x20, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x20, 0x69,
        0x6e, 0x6e, 0x6f, 0x64, 0x62, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x65,
        0x00, 0x00, 0x00, 0x0d, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08,
        0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65,
        0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x34, 0x28, 0x61, 0x20, 0x69, 0x6e, 0x74,
        0x29, 0x20, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x20, 0x6d, 0x79, 0x69, 0x73, 0x61,
        0x6d, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x00, 0x52,
        0x03, 0x00, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00,
        0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d,
        0x79, 0x73, 0x71, 0x6c, 0x00, 0x42, 0x45, 0x47, 0x49, 0x4e, 0xfd, 0x35, 0xbb, 0x4a, 0x02,
        0x01, 0x00, 0x00, 0x00, 0x5c, 0x00, 0x00, 0x00, 0xae, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74,
        0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x69, 0x6e,
        0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x33, 0x20, 0x28, 0x61,
        0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x32, 0x29, 0xfd, 0x35, 0xbb,
        0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x5d, 0x00, 0x00, 0x00, 0x0b, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00,
        0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03,
        0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c,
        0x00, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x34,
        0x20, 0x28, 0x61, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x32, 0x29,
        0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x48, 0x00, 0x00, 0x00, 0x53, 0x04,
        0x00, 0x00, 0x08, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00,
        0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79,
        0x73, 0x71, 0x6c, 0x00, 0x52, 0x4f, 0x4c, 0x4c, 0x42, 0x41, 0x43, 0x4b, 0xfd, 0x35, 0xbb,
        0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0xb4, 0x04, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00,
        0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03,
        0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00,
        0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x35,
        0x28, 0x61, 0x20, 0x69, 0x6e, 0x74, 0x29, 0x20, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d,
        0x20, 0x4e, 0x44, 0x42, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x62, 0x00,
        0x00, 0x00, 0x16, 0x05, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00,
        0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20,
        0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x74, 0x36, 0x28, 0x61, 0x20, 0x69, 0x6e, 0x74, 0x29,
        0x20, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x20, 0x4e, 0x44, 0x42, 0xfd, 0x35, 0xbb,
        0x4a, 0x02, 0x01, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x00, 0x5b, 0x05, 0x00, 0x00, 0x08,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00,
        0x00, 0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03,
        0x73, 0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c,
        0x00, 0x42, 0x45, 0x47, 0x49, 0x4e, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00, 0x00, 0x00,
        0x5c, 0x00, 0x00, 0x00, 0xb7, 0x05, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x08, 0x00,
        0x08, 0x00, 0x08, 0x00, 0x74, 0x65, 0x73, 0x74, 0x00, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74,
        0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x35, 0x20, 0x28, 0x61, 0x29, 0x20, 0x76, 0x61,
        0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x33, 0x29, 0xfd, 0x35, 0xbb, 0x4a, 0x02, 0x01, 0x00,
        0x00, 0x00, 0x5d, 0x00, 0x00, 0x00, 0x14, 0x06, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04,
        0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00, 0x69, 0x6e, 0x73,
        0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x36, 0x20, 0x28, 0x61, 0x29,
        0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x33, 0x29, 0xfd, 0x35, 0xbb, 0x4a,
        0x02, 0x01, 0x00, 0x00, 0x00, 0x46, 0x00, 0x00, 0x00, 0x5a, 0x06, 0x00, 0x00, 0x08, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,
        0x40, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73,
        0x74, 0x64, 0x04, 0x08, 0x00, 0x08, 0x00, 0x08, 0x00, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x00,
        0x43, 0x4f, 0x4d, 0x4d, 0x49, 0x54, 0xfd, 0x35, 0xbb, 0x4a, 0x04, 0x01, 0x00, 0x00, 0x00,
        0x2c, 0x00, 0x00, 0x00, 0x86, 0x06, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x6d, 0x61, 0x73, 0x74, 0x65, 0x72, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x32,
    ];

    #[test]
    fn binlog_file_header_roundtrip() -> io::Result<()> {
        let mut output = Vec::new();

        let binlog_file_header = BinlogFileHeader::read(BINLOG_FILE)?;
        binlog_file_header.write(BinlogVersion::Version4, &mut output)?;

        assert_eq!(&output[..], &BINLOG_FILE[..BinlogFileHeader::LEN]);

        Ok(())
    }

    #[test]
    fn binlog_file_iterator() -> io::Result<()> {
        let binlog_file = BinlogFile::new(BinlogVersion::Version4, BINLOG_FILE)?;

        let mut total = 0;
        let mut ev_pos = 4;

        for (i, ev) in binlog_file.enumerate() {
            let data_start = ev_pos + BinlogEventHeader::LEN;
            let ev = ev?;
            match i {
                0 => {
                    assert_eq!(
                        ev.header(),
                        BinlogEventHeader::new(
                            1253783036,
                            EventType::FORMAT_DESCRIPTION_EVENT,
                            1,
                            94,
                            98,
                            EventFlags::empty()
                        )
                    )
                }
                1 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        100,
                        198,
                        EventFlags::empty()
                    )
                ),
                2 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        101,
                        299,
                        EventFlags::empty()
                    )
                ),
                3 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        69,
                        368,
                        EventFlags::LOG_EVENT_SUPPRESS_USE_F
                    )
                ),
                4 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        92,
                        460,
                        EventFlags::empty()
                    )
                ),
                5 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        93,
                        553,
                        EventFlags::empty()
                    )
                ),
                6 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::XID_EVENT,
                        1,
                        27,
                        580,
                        EventFlags::empty()
                    )
                ),
                7 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        100,
                        680,
                        EventFlags::empty()
                    )
                ),
                8 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        101,
                        781,
                        EventFlags::empty()
                    )
                ),
                9 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        69,
                        850,
                        EventFlags::LOG_EVENT_SUPPRESS_USE_F
                    )
                ),
                10 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        92,
                        942,
                        EventFlags::empty()
                    )
                ),
                11 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        93,
                        1035,
                        EventFlags::empty()
                    )
                ),
                12 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        72,
                        1107,
                        EventFlags::LOG_EVENT_SUPPRESS_USE_F
                    )
                ),
                13 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        97,
                        1204,
                        EventFlags::empty()
                    )
                ),
                14 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        98,
                        1302,
                        EventFlags::empty()
                    )
                ),
                15 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        69,
                        1371,
                        EventFlags::LOG_EVENT_SUPPRESS_USE_F
                    )
                ),
                16 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        92,
                        1463,
                        EventFlags::empty()
                    )
                ),
                17 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        93,
                        1556,
                        EventFlags::empty()
                    )
                ),
                18 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::QUERY_EVENT,
                        1,
                        70,
                        1626,
                        EventFlags::LOG_EVENT_SUPPRESS_USE_F
                    )
                ),
                19 => assert_eq!(
                    ev.header(),
                    BinlogEventHeader::new(
                        1253783037,
                        EventType::ROTATE_EVENT,
                        1,
                        44,
                        1670,
                        EventFlags::empty()
                    )
                ),
                _ => panic!("too many"),
            }

            assert_eq!(
                ev.data(),
                &BINLOG_FILE[data_start
                    ..(data_start + ev.header().event_size() as usize - BinlogEventHeader::LEN)],
            );

            total += 1;
            ev_pos = ev.header().log_pos() as usize;
        }

        assert_eq!(total, 20);
        Ok(())
    }

    #[test]
    fn binlog_event_roundtrip() -> io::Result<()> {
        const PATH: &str = "./test-data/binlogs";

        let binlogs = std::fs::read_dir(PATH)?
            .filter_map(|path| path.ok())
            .map(|entry| entry.path())
            .filter(|path| path.file_name().is_some());

        'outer: for file_path in binlogs {
            let file_data = std::fs::read(dbg!(&file_path))?;
            let mut binlog_file = BinlogFile::new(BinlogVersion::Version4, &file_data[..])?;

            let mut ev_pos = 4;
            let mut table_map_events = HashMap::new();

            while let Some(ev) = binlog_file.next() {
                let ev = ev?;
                let _ = dbg!(ev.header().event_type());
                let ev_end = ev_pos + ev.header().event_size() as usize;
                let binlog_version = binlog_file.reader.fde.binlog_version();

                let mut output = Vec::new();
                ev.write(binlog_version, &mut output)?;

                let event = match ev.read_data() {
                    Ok(event) => {
                        let event = match event {
                            Some(e) => e,
                            None => {
                                if file_path.file_name().unwrap() == "mariadb-bin.000001" {
                                    continue;
                                } else {
                                    dbg!(&ev);
                                    panic!();
                                }
                            }
                        };
                        match event {
                            EventData::TableMapEvent(ref ev) => {
                                // store table maps for later use
                                table_map_events.insert(ev.table_id(), ev.clone().into_owned());

                                event
                            }
                            EventData::RowsEvent(ref rows_event) => {
                                // iterate rows in a rows event
                                let table_map_event =
                                    binlog_file.reader().get_tme(rows_event.table_id()).unwrap();
                                for row in rows_event.rows(table_map_event) {
                                    let _row = row.unwrap();
                                    if file_path.file_name().unwrap() == "mariadb-bin.000001" {
                                        // should parse metadata for `binlog_row_metadata=FULL`
                                        let after = _row.1.as_ref().unwrap();
                                        let columns = after.columns_ref();

                                        for col in columns.iter() {
                                            assert_eq!(col.schema_ref(), b"toddy_test");
                                            assert_eq!(col.table_ref(), b"outbox");
                                            assert_eq!(col.org_table_ref(), b"outbox");
                                        }

                                        for (col, col_name) in columns.iter().zip([
                                            "id",
                                            "topic",
                                            "event_type",
                                            "event",
                                            "created",
                                        ]) {
                                            assert_eq!(col.name_ref(), col_name.as_bytes());
                                        }

                                        for (col, f) in columns.iter().zip(
                                            once(ColumnFlags::PRI_KEY_FLAG)
                                                .chain(repeat(ColumnFlags::empty())),
                                        ) {
                                            assert_eq!(col.flags(), f);
                                        }

                                        for (col, charset) in columns.iter().zip([
                                            CollationId::UNKNOWN_COLLATION_ID,
                                            CollationId::UTF8MB4_GENERAL_CI,
                                            CollationId::UTF8MB4_GENERAL_CI,
                                            CollationId::BINARY,
                                            CollationId::UNKNOWN_COLLATION_ID,
                                        ]) {
                                            assert_eq!(col.character_set(), charset as u16);
                                        }
                                    }
                                }

                                event
                            }
                            _ => event,
                        }
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::Other
                            && ev.header().event_type() == Ok(EventType::XID_EVENT)
                            && ev.header().event_size() == 0x26
                            && file_path.file_name().unwrap() == "ver_5_1-wl2325_r.001" =>
                    {
                        // ver_5_1-wl2325_r.001 testfile contains broken xid event.
                        continue 'outer;
                    }
                    Err(err)
                        if err.kind() == std::io::ErrorKind::UnexpectedEof
                            && ev.header().event_type() == Ok(EventType::QUERY_EVENT)
                            && ev.header().event_size() == 171
                            && file_path.file_name().unwrap() == "corrupt-relay-bin.000624" =>
                    {
                        // corrupt-relay-bin.000624 testfile contains broken query event.
                        continue 'outer;
                    }
                    other => other.transpose().unwrap()?,
                };

                if file_path.file_name().unwrap() == "binlog-invisible-columns.000001" {
                    if let Some(EventData::TableMapEvent(ev)) = ev.read_data().unwrap() {
                        let optional_meta = ev.iter_optional_meta();
                        for meta in optional_meta {
                            meta.unwrap();
                        }
                    }
                }

                if file_path.file_name().unwrap() == "mysql-enum-string-set.000001" {
                    if let Some(EventData::RowsEvent(data)) = ev.read_data().unwrap() {
                        let table_map_event =
                            binlog_file.reader().get_tme(data.table_id()).unwrap();
                        for row in data.rows(table_map_event) {
                            let (before, after) = row.unwrap();
                            match data {
                                RowsEventData::WriteRowsEvent(_) => {
                                    assert!(before.is_none());
                                    let after = after.unwrap().unwrap();
                                    let mut j = 0;
                                    for v in after {
                                        j += 1;
                                        match j {
                                            1 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".into())),
                                            2 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                            3 => assert_eq!(v, BinlogValue::Value(1_i8.into())),
                                            4 => assert_eq!(v, BinlogValue::Value([0b00000101_u8].into())),
                                            5 => assert_eq!(v, BinlogValue::Value("0123456789".into())),

                                            _ => panic!(),
                                        }
                                    }
                                    assert_eq!(j, 5);
                                }
                                RowsEventData::UpdateRowsEvent(_) => {
                                    let before = before.unwrap().unwrap();
                                    let mut j = 0;
                                    for v in before {
                                        j += 1;
                                        match j {
                                            1 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".into())),
                                            2 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                            3 => assert_eq!(v, BinlogValue::Value(1_i8.into())),
                                            4 => assert_eq!(v, BinlogValue::Value([0b00000101_u8].into())),
                                            5 => assert_eq!(v, BinlogValue::Value("0123456789".into())),

                                            _ => panic!(),
                                        }
                                    }
                                    assert_eq!(j, 5);

                                    let after = after.unwrap().unwrap();
                                    let mut j = 0;
                                    for v in after {
                                        j += 1;
                                        match j {
                                            1 => assert_eq!(v, BinlogValue::Value("field1".into())),
                                            2 => assert_eq!(v, BinlogValue::Value("field_2".into())),
                                            3 => assert_eq!(v, BinlogValue::Value(2_i8.into())),
                                            4 => assert_eq!(v, BinlogValue::Value([0b00001010_u8].into())),
                                            5 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                            _ => panic!(),
                                        }
                                    }
                                    assert_eq!(j, 5);
                                }
                                RowsEventData::DeleteRowsEvent(_) => {
                                    assert!(after.is_none());

                                    let before = before.unwrap().unwrap();
                                    let mut j = 0;
                                    for v in before {
                                        j += 1;
                                        match j {
                                            1 => assert_eq!(v, BinlogValue::Value("field1".into())),
                                            2 => assert_eq!(v, BinlogValue::Value("field_2".into())),
                                            3 => assert_eq!(v, BinlogValue::Value(2_i8.into())),
                                            4 => assert_eq!(v, BinlogValue::Value([0b00001010_u8].into())),
                                            5 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                            _ => panic!(),
                                        }
                                    }
                                    assert_eq!(j, 5);
                                }
                                _ => panic!(),
                            }
                        }
                    }
                }

                if file_path.file_name().unwrap() == "mariadb-bin.000001" {
                    // Extraneous bytes in RotateEvent file name
                    // https://github.com/blackbeam/mysql_async/issues/189
                    if let Some(EventData::RotateEvent(ev)) = ev.read_data().unwrap() {
                        assert_ne!(ev.name_raw(), b"mariadb-bin.000001");
                    }
                }

                if file_path.file_name().unwrap() == "mysql_type_bit.000001" {
                    if let Some(EventData::RowsEvent(ev)) = ev.read_data().unwrap() {
                        let table_map_event = binlog_file.reader().get_tme(ev.table_id()).unwrap();
                        for row in ev.rows(table_map_event) {
                            let (before, after) = row.unwrap();
                            assert_eq!(before, None);
                            assert_eq!(
                                after.unwrap().unwrap(),
                                vec![
                                    BinlogValue::Value(Value::Bytes(vec![0b100])),
                                    BinlogValue::Value(Value::Bytes(b"foo".to_vec())),
                                    BinlogValue::Value(Value::Bytes(vec![0b100000])),
                                ],
                            );
                        }
                    }
                }

                if file_path.file_name().unwrap() != "mariadb-bin.000001" {
                    assert_eq!(output, &file_data[ev_pos..ev_end]);
                }

                if file_path.file_name().unwrap() == "transaction_compression.000001" {
                    if let Some(EventData::TransactionPayloadEvent(data)) = ev.read_data().unwrap()
                    {
                        let mut payload = data.decompressed()?;
                        let reader = binlog_file.reader_mut();

                        let mut binlog_ev = reader.read_decompressed(&mut payload)?.unwrap();
                        assert_eq!(binlog_ev.header().event_type(), Ok(EventType::QUERY_EVENT));

                        binlog_ev = reader.read_decompressed(&mut payload)?.unwrap();
                        assert_eq!(
                            binlog_ev.header().event_type(),
                            Ok(EventType::TABLE_MAP_EVENT)
                        );

                        binlog_ev = reader.read_decompressed(&mut payload)?.unwrap();
                        assert_eq!(
                            binlog_ev.header().event_type(),
                            Ok(EventType::WRITE_ROWS_EVENT)
                        );

                        binlog_ev = reader.read_decompressed(&mut payload)?.unwrap();
                        assert_eq!(binlog_ev.header().event_type(), Ok(EventType::XID_EVENT));
                        assert!(reader.read_decompressed(&mut payload)?.is_none());
                    }
                }
                output = Vec::new();
                event.serialize(&mut output);

                if matches!(event, EventData::UserVarEvent(_)) {
                    // Server may or may not write the flags field, but we will always write it.
                    assert_eq!(&output[..ev.data().len()], ev.data());
                    assert!(output.len() == ev.data().len() || output.len() == ev.data().len() + 1);
                } else if (matches!(event, EventData::GtidEvent(_))
                    || matches!(event, EventData::AnonymousGtidEvent(_)))
                    && ev.fde().split_version() < (5, 7, 0)
                {
                    // MySql 5.6 does not write TS_TYPE and following post-header fields
                    assert_eq!(&output[..GtidEvent::POST_HEADER_LENGTH - 1 - 16], ev.data());
                } else if (matches!(event, EventData::GtidEvent(_))
                    || matches!(event, EventData::AnonymousGtidEvent(_)))
                    && ev.fde().split_version() < (5, 8, 0)
                {
                    // MySql 5.7 contains only post-header in this event
                    assert_eq!(&output[..GtidEvent::POST_HEADER_LENGTH], ev.data());
                } else {
                    assert_eq!(output, ev.data());
                }

                ev_pos = ev_end;
            }
        }

        Ok(())
    }
}
