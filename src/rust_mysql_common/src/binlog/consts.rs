// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::convert::{TryFrom, TryInto};

/// Depending on the MySQL Version that created the binlog the format is slightly different.
#[repr(u16)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BinlogVersion {
    /// MySQL 3.23 - < 4.0.0
    Version1 = 1,
    /// MySQL 4.0.0 - 4.0.1
    Version2,
    /// MySQL 4.0.2 - < 5.0.0
    Version3,
    /// MySQL 5.0.0+
    Version4,
}

impl From<BinlogVersion> for u16 {
    fn from(x: BinlogVersion) -> Self {
        x as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown binlog version {}", _0)]
#[repr(transparent)]
pub struct UnknownBinlogVersion(pub u16);

impl From<UnknownBinlogVersion> for u16 {
    fn from(x: UnknownBinlogVersion) -> Self {
        x.0
    }
}

impl TryFrom<u16> for BinlogVersion {
    type Error = UnknownBinlogVersion;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Version1),
            2 => Ok(Self::Version2),
            3 => Ok(Self::Version3),
            4 => Ok(Self::Version4),
            x => Err(UnknownBinlogVersion(x)),
        }
    }
}

/// Binlog Event Type
#[allow(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum EventType {
    /// Ignored event.
    UNKNOWN_EVENT = 0x00,
    /// A start event is the first event of a binlog for binlog-version 1 to 3.
    ///
    /// Superseded by `FORMAT_DESCRIPTION_EVENT` since mysql v5.0.0.
    START_EVENT_V3 = 0x01,
    /// A `QUERY_EVENT` is created for each query that modifies the database,
    /// unless the query is logged row-based.
    QUERY_EVENT = 0x02,
    /// A `STOP_EVENT` has no payload or post-header.
    STOP_EVENT = 0x03,
    /// The rotate event is added to the binlog as last event
    /// to tell the reader what binlog to request next.
    ROTATE_EVENT = 0x04,
    INTVAR_EVENT = 0x05,
    LOAD_EVENT = 0x06,
    /// Ignored event.
    SLAVE_EVENT = 0x07,
    CREATE_FILE_EVENT = 0x08,
    APPEND_BLOCK_EVENT = 0x09,
    EXEC_LOAD_EVENT = 0x0a,
    DELETE_FILE_EVENT = 0x0b,
    NEW_LOAD_EVENT = 0x0c,
    RAND_EVENT = 0x0d,
    USER_VAR_EVENT = 0x0e,
    ///  A format description event is the first event of a binlog for binlog-version 4. It describes how the other events are layed out.
    ///
    /// # Note
    ///
    /// Added in MySQL 5.0.0 as replacement for START_EVENT_V3
    FORMAT_DESCRIPTION_EVENT = 0x0f,
    XID_EVENT = 0x10,
    BEGIN_LOAD_QUERY_EVENT = 0x11,
    EXECUTE_LOAD_QUERY_EVENT = 0x12,
    TABLE_MAP_EVENT = 0x13,
    PRE_GA_WRITE_ROWS_EVENT = 0x14,
    PRE_GA_UPDATE_ROWS_EVENT = 0x15,
    PRE_GA_DELETE_ROWS_EVENT = 0x16,
    WRITE_ROWS_EVENT_V1 = 0x17,
    UPDATE_ROWS_EVENT_V1 = 0x18,
    DELETE_ROWS_EVENT_V1 = 0x19,
    INCIDENT_EVENT = 0x1a,
    HEARTBEAT_EVENT = 0x1b,
    IGNORABLE_EVENT = 0x1c,
    ROWS_QUERY_EVENT = 0x1d,
    WRITE_ROWS_EVENT = 0x1e,
    UPDATE_ROWS_EVENT = 0x1f,
    DELETE_ROWS_EVENT = 0x20,
    GTID_EVENT = 0x21,
    ANONYMOUS_GTID_EVENT = 0x22,
    PREVIOUS_GTIDS_EVENT = 0x23,
    TRANSACTION_CONTEXT_EVENT = 0x24,
    VIEW_CHANGE_EVENT = 0x25,
    /// Prepared XA transaction terminal event similar to Xid.
    XA_PREPARE_LOG_EVENT = 0x26,
    /// Extension of UPDATE_ROWS_EVENT, allowing partial values according
    /// to binlog_row_value_options.
    PARTIAL_UPDATE_ROWS_EVENT = 0x27,
    TRANSACTION_PAYLOAD_EVENT = 0x28,
    /// Total number of known events.
    ENUM_END_EVENT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown event type {}", _0)]
#[repr(transparent)]
pub struct UnknownEventType(pub u8);

impl From<UnknownEventType> for u8 {
    fn from(x: UnknownEventType) -> Self {
        x.0
    }
}

impl TryFrom<u8> for EventType {
    type Error = UnknownEventType;

    fn try_from(byte: u8) -> Result<Self, UnknownEventType> {
        match byte {
            0x00 => Ok(Self::UNKNOWN_EVENT),
            0x01 => Ok(Self::START_EVENT_V3),
            0x02 => Ok(Self::QUERY_EVENT),
            0x03 => Ok(Self::STOP_EVENT),
            0x04 => Ok(Self::ROTATE_EVENT),
            0x05 => Ok(Self::INTVAR_EVENT),
            0x06 => Ok(Self::LOAD_EVENT),
            0x07 => Ok(Self::SLAVE_EVENT),
            0x08 => Ok(Self::CREATE_FILE_EVENT),
            0x09 => Ok(Self::APPEND_BLOCK_EVENT),
            0x0a => Ok(Self::EXEC_LOAD_EVENT),
            0x0b => Ok(Self::DELETE_FILE_EVENT),
            0x0c => Ok(Self::NEW_LOAD_EVENT),
            0x0d => Ok(Self::RAND_EVENT),
            0x0e => Ok(Self::USER_VAR_EVENT),
            0x0f => Ok(Self::FORMAT_DESCRIPTION_EVENT),
            0x10 => Ok(Self::XID_EVENT),
            0x11 => Ok(Self::BEGIN_LOAD_QUERY_EVENT),
            0x12 => Ok(Self::EXECUTE_LOAD_QUERY_EVENT),
            0x13 => Ok(Self::TABLE_MAP_EVENT),
            0x14 => Ok(Self::PRE_GA_WRITE_ROWS_EVENT),
            0x15 => Ok(Self::PRE_GA_UPDATE_ROWS_EVENT),
            0x16 => Ok(Self::PRE_GA_DELETE_ROWS_EVENT),
            0x17 => Ok(Self::WRITE_ROWS_EVENT_V1),
            0x18 => Ok(Self::UPDATE_ROWS_EVENT_V1),
            0x19 => Ok(Self::DELETE_ROWS_EVENT_V1),
            0x1a => Ok(Self::INCIDENT_EVENT),
            0x1b => Ok(Self::HEARTBEAT_EVENT),
            0x1c => Ok(Self::IGNORABLE_EVENT),
            0x1d => Ok(Self::ROWS_QUERY_EVENT),
            0x1e => Ok(Self::WRITE_ROWS_EVENT),
            0x1f => Ok(Self::UPDATE_ROWS_EVENT),
            0x20 => Ok(Self::DELETE_ROWS_EVENT),
            0x21 => Ok(Self::GTID_EVENT),
            0x22 => Ok(Self::ANONYMOUS_GTID_EVENT),
            0x23 => Ok(Self::PREVIOUS_GTIDS_EVENT),
            0x24 => Ok(Self::TRANSACTION_CONTEXT_EVENT),
            0x25 => Ok(Self::VIEW_CHANGE_EVENT),
            0x26 => Ok(Self::XA_PREPARE_LOG_EVENT),
            0x27 => Ok(Self::PARTIAL_UPDATE_ROWS_EVENT),
            0x28 => Ok(Self::TRANSACTION_PAYLOAD_EVENT),
            x => Err(UnknownEventType(x)),
        }
    }
}

my_bitflags! {
    EventFlags,
    #[error("Unknown flags in the raw value of EventFlags (raw={:b})", _0)]
    UnknownEventFlags,
    u16,

    /// Binlog Event Flags
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct EventFlags: u16 {
        /// Gets unset in the `FORMAT_DESCRIPTION_EVENT`
        /// when the file gets closed to detect broken binlogs.
        const LOG_EVENT_BINLOG_IN_USE_F = 0x0001;

        /// Unused.
        const LOG_EVENT_FORCED_ROTATE_F = 0x0002;

        /// event is thread specific (`CREATE TEMPORARY TABLE` ...).
        const LOG_EVENT_THREAD_SPECIFIC_F = 0x0004;

        /// Event doesn't need default database to be updated (`CREATE DATABASE`, ...).
        const LOG_EVENT_SUPPRESS_USE_F = 0x0008;

        /// Unused.
        const LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F = 0x0010;

        /// Event is created by the slaves SQL-thread and shouldn't update the master-log pos.
        const LOG_EVENT_ARTIFICIAL_F = 0x0020;

        /// Event is created by the slaves IO-thread when written to the relay log.
        const LOG_EVENT_RELAY_LOG_F = 0x0040;

        /// Setting this flag will mark an event as Ignorable.
        const LOG_EVENT_IGNORABLE_F = 0x0080;

        /// Events with this flag are not filtered (e.g. on the current
        /// database) and are always written to the binary log regardless of
        /// filters.
        const LOG_EVENT_NO_FILTER_F = 0x0100;

        /// MTS: group of events can be marked to force its execution in isolation from
        /// any other Workers.
        const LOG_EVENT_MTS_ISOLATE_F = 0x0200;
    }
}

/// Enumeration spcifying checksum algorithm used to encode a binary log event.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[allow(non_camel_case_types)]
#[repr(u8)]
pub enum BinlogChecksumAlg {
    /// Events are without checksum though its generator is checksum-capable New Master (NM).
    BINLOG_CHECKSUM_ALG_OFF = 0,
    /// CRC32 of zlib algorithm
    BINLOG_CHECKSUM_ALG_CRC32 = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown checksum algorithm {}", _0)]
#[repr(transparent)]
pub struct UnknownChecksumAlg(pub u8);

impl From<UnknownChecksumAlg> for u8 {
    fn from(x: UnknownChecksumAlg) -> Self {
        x.0
    }
}

impl TryFrom<u8> for BinlogChecksumAlg {
    type Error = UnknownChecksumAlg;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::BINLOG_CHECKSUM_ALG_OFF),
            1 => Ok(Self::BINLOG_CHECKSUM_ALG_CRC32),
            x => Err(UnknownChecksumAlg(x)),
        }
    }
}

/// Binlog query event status vars keys.
#[repr(u8)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum StatusVarKey {
    /// Contains `Flags2` flags.
    Flags2 = 0,
    /// Contains `SqlMode` flags.
    SqlMode,
    /// Contains values in the following order:
    ///
    /// *   1 byte `length`,
    /// *   `length` bytes catalog,
    /// *   NULL byte.
    ///
    /// `length + 2` bytes in total.
    Catalog,
    /// Contains values in the following order:
    ///
    /// *   2 bytes unsigned little-endian auto_increment_increment,
    /// *   2 bytes unsigned little-endian auto_increment_offset.
    ///
    /// Four bytes in total.
    AutoIncrement,
    /// Contains values in the following order:
    ///
    /// *   2 bytes unsigned little-endian character_set_client,
    /// *   2 bytes unsigned little-endian collation_connection,
    /// *   2 bytes unsigned little-endian collation_server.
    ///
    /// Six bytes in total.
    Charset,
    /// Contains values in the following order:
    ///
    /// *   1 byte `length`,
    /// *   `length` bytes timezone.
    ///
    /// `length + 1` bytes in total.
    TimeZone,
    /// Contains values in the following order:
    ///
    /// *   1 byte `length`,
    /// *   `length` bytes catalog.
    ///
    /// `length + 1` bytes in total.
    CatalogNz,
    /// Contains 2 bytes code identifying a table of month and day names.
    ///
    /// The mapping from codes to languages is defined in sql_locale.cc.
    LcTimeNames,
    /// Contains 2 bytes value of the collation_database system variable.
    CharsetDatabase,
    /// Contains 8 bytes value of the table map that is to be updated
    /// by the multi-table update query statement.
    TableMapForUpdate,
    /// Contains 4 bytes bitfield.
    MasterDataWritten,
    /// Contains values in the following order:
    ///
    /// *   1 byte `user_length`,
    /// *   `user_length` bytes user,
    /// *   1 byte `host_length`,
    /// *   `host_length` bytes host.
    ///
    /// `user_length + host_length + 2` bytes in total.
    Invoker,
    /// Contains values in the following order:
    ///
    /// *   1 byte `count`,
    /// *   `count` times:
    ///     *   null-terminated db_name.
    ///
    /// `1 + db_names_lens.sum()` bytes in total.
    UpdatedDbNames,
    /// Contains 3 bytes unsigned little-endian integer.
    Microseconds,
    CommitTs,
    CommitTs2,
    /// Contains 1 byte boolean.
    ExplicitDefaultsForTimestamp,
    /// Contains 8 bytes unsigned little-endian integer carrying xid info of 2pc-aware
    /// (recoverable) DDL queries.
    DdlLoggedWithXid,
    /// Contains 2 bytes unsigned little-endian integer carrying
    /// the default collation for the utf8mb4 character set.
    DefaultCollationForUtf8mb4,
    /// Contains 1 byte value.
    SqlRequirePrimaryKey,
    /// Contains 1 byte value.
    DefaultTableEncryption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown status var key {}", _0)]
#[repr(transparent)]
pub struct UnknownStatusVarKey(pub u8);

impl TryFrom<u8> for StatusVarKey {
    type Error = UnknownStatusVarKey;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StatusVarKey::Flags2),
            1 => Ok(StatusVarKey::SqlMode),
            2 => Ok(StatusVarKey::Catalog),
            3 => Ok(StatusVarKey::AutoIncrement),
            4 => Ok(StatusVarKey::Charset),
            5 => Ok(StatusVarKey::TimeZone),
            6 => Ok(StatusVarKey::CatalogNz),
            7 => Ok(StatusVarKey::LcTimeNames),
            8 => Ok(StatusVarKey::CharsetDatabase),
            9 => Ok(StatusVarKey::TableMapForUpdate),
            10 => Ok(StatusVarKey::MasterDataWritten),
            11 => Ok(StatusVarKey::Invoker),
            12 => Ok(StatusVarKey::UpdatedDbNames),
            13 => Ok(StatusVarKey::Microseconds),
            14 => Ok(StatusVarKey::CommitTs),
            15 => Ok(StatusVarKey::CommitTs2),
            16 => Ok(StatusVarKey::ExplicitDefaultsForTimestamp),
            17 => Ok(StatusVarKey::DdlLoggedWithXid),
            18 => Ok(StatusVarKey::DefaultCollationForUtf8mb4),
            19 => Ok(StatusVarKey::SqlRequirePrimaryKey),
            20 => Ok(StatusVarKey::DefaultTableEncryption),
            x => Err(UnknownStatusVarKey(x)),
        }
    }
}

my_bitflags! {
    SemiSyncFlags,
    #[error("Unknown flags in the raw value of SemiSyncFlags (raw={:b})", _0)]
    UnknownSemiSyncFlags,
    u8,

    /// Semi-sync binlog flags.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct SemiSyncFlags: u8 {
        // If the SEMI_SYNC_ACK_REQ flag is set the master waits for a Semi Sync ACK packet
        // from the slave before it sends the next event.
        const SEMI_SYNC_ACK_REQ = 0x01;
    }
}

/// Variants of this enum describe how LOAD DATA handles duplicates.
#[repr(u8)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum LoadDuplicateHandling {
    LOAD_DUP_ERROR = 0,
    LOAD_DUP_IGNORE,
    LOAD_DUP_REPLACE,
}

impl From<LoadDuplicateHandling> for u8 {
    fn from(x: LoadDuplicateHandling) -> Self {
        x as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown duplicate handling variant {}", _0)]
#[repr(transparent)]
pub struct UnknownDuplicateHandling(pub u8);

impl From<UnknownDuplicateHandling> for u8 {
    fn from(x: UnknownDuplicateHandling) -> Self {
        x.0
    }
}

impl TryFrom<u8> for LoadDuplicateHandling {
    type Error = UnknownDuplicateHandling;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::LOAD_DUP_ERROR),
            1 => Ok(Self::LOAD_DUP_IGNORE),
            2 => Ok(Self::LOAD_DUP_REPLACE),
            x => Err(UnknownDuplicateHandling(x)),
        }
    }
}

/// Enumerates types of optional metadata fields.
#[repr(u8)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum OptionalMetadataFieldType {
    /// UNSIGNED flag of numeric columns.
    ///
    /// # Value format
    ///
    /// For each numeric column, a bit indicates whether the numeric colunm has unsigned flag.
    /// `1` means it is unsigned. The number of bytes needed for this
    /// is `int((column_count + 7) / 8)`.
    SIGNEDNESS = 1,
    /// Character set of string columns.
    ///
    /// This field should not appear with `COLUMN_CHARSET`.
    ///
    /// # Value format
    ///
    /// *   default charset as a lenght-encoded integer,
    /// *   then for every character column which charset isn't default:
    ///     *   column index as a lenght-encoded integer,
    ///     *   column charset as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    DEFAULT_CHARSET,
    /// Character set of string columns.
    ///
    /// This field should not appear with `DEFAULT_CHARSET`.
    ///
    /// # Value format
    ///
    /// *   for every character column:
    ///     *   column charset as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    COLUMN_CHARSET,
    /// Collumn name of columns (included if `binlog_row_metadata=FULL`).
    ///
    /// # Value format
    ///
    /// *   for every column:
    ///     *   column name as a length-encoded string.
    ///
    /// The order is the same as the order of `column_type` field.
    COLUMN_NAME,
    /// Name of each variant in a SET columns.
    ///
    /// # Value format
    ///
    /// *   for every SET column:
    ///     *   number of variants as a length-encoded integer,
    ///     *   for each variant:
    ///         *   name of a variant as a length-encoded string.
    ///
    /// The order is the same as the order of `column_type` field.
    SET_STR_VALUE,
    /// Name of each variant in an ENUM columns.
    ///
    /// # Value format
    ///
    /// *   for every ENUM column:
    ///     *   number of variants as a length-encoded integer,
    ///     *   for each variant:
    ///         *   name of a variant as a length-encoded string.
    ///
    /// The order is the same as the order of `column_type` field.
    ENUM_STR_VALUE,
    /// Real type of geometry columns.
    ///
    /// # Value format
    ///
    /// *   for every geometry column:
    ///     *   geometry type as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    GEOMETRY_TYPE,
    /// Primary key without prefix (included if `binlog_row_metadata=FULL`).
    ///
    /// This field should not appear with `PRIMARY_KEY_WITH_PREFIX`.
    ///
    /// # Value format
    ///
    /// *   for every PK index column:
    ///     *   column index as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    SIMPLE_PRIMARY_KEY,
    /// Primary key with prefix (included if `binlog_row_metadata=FULL`).
    ///
    /// This field should not appear with `SIMPLE_PRIMARY_KEY`.
    ///
    /// # Value format
    ///
    /// *   for every PK index column:
    ///     *   column index as a length-encoded integer,
    ///     *   prefix length as a length-encoded integer
    ///
    /// The order is the same as the order of `column_type` field.
    ///
    /// ## Note
    ///
    /// *   prefix length is a character length, i.e. prefix byte length divided by the maximum
    ///     length of a character in the correspoding charset;
    /// *   prefix length `0` means that the whole column value is used.
    PRIMARY_KEY_WITH_PREFIX,
    /// Character set of enum and set columns.
    ///
    /// This field should not appear with `ENUM_AND_SET_COLUMN_CHARSET`.
    ///
    /// # Value format
    ///
    /// *   default charset as a lenght-encoded integer,
    /// *   then for every SET or ENUM column which charset isn't default:
    ///     *   column index as a lenght-encoded integer,
    ///     *   column charset as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    ENUM_AND_SET_DEFAULT_CHARSET,
    /// Character set of enum and set columns.
    ///
    /// This field should not appear with `ENUM_AND_SET_DEFAULT_CHARSET`.
    ///
    /// # Value format
    ///
    /// *   for every SET or ENUM column:
    ///     *   column charset as a length-encoded integer.
    ///
    /// The order is the same as the order of `column_type` field.
    ENUM_AND_SET_COLUMN_CHARSET,
    /// A flag that indicates column visibility attribute.
    COLUMN_VISIBILITY,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Unknown optional metadata field type {}", _0)]
pub struct UnknownOptionalMetadataFieldType(pub u8);

impl From<UnknownOptionalMetadataFieldType> for u8 {
    fn from(x: UnknownOptionalMetadataFieldType) -> Self {
        x.0
    }
}

impl TryFrom<u8> for OptionalMetadataFieldType {
    type Error = UnknownOptionalMetadataFieldType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::SIGNEDNESS),
            2 => Ok(Self::DEFAULT_CHARSET),
            3 => Ok(Self::COLUMN_CHARSET),
            4 => Ok(Self::COLUMN_NAME),
            5 => Ok(Self::SET_STR_VALUE),
            6 => Ok(Self::ENUM_STR_VALUE),
            7 => Ok(Self::GEOMETRY_TYPE),
            8 => Ok(Self::SIMPLE_PRIMARY_KEY),
            9 => Ok(Self::PRIMARY_KEY_WITH_PREFIX),
            10 => Ok(Self::ENUM_AND_SET_DEFAULT_CHARSET),
            11 => Ok(Self::ENUM_AND_SET_COLUMN_CHARSET),
            12 => Ok(Self::COLUMN_VISIBILITY),
            x => Err(UnknownOptionalMetadataFieldType(x)),
        }
    }
}

/// Type of an incident event.
#[repr(u16)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IncidentType {
    /// No incident.
    INCIDENT_NONE = 0,
    /// There are possibly lost events in the replication stream.
    INCIDENT_LOST_EVENTS = 1,
}

impl From<IncidentType> for u16 {
    fn from(x: IncidentType) -> Self {
        x as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown item incident type {}", _0)]
#[repr(transparent)]
pub struct UnknownIncidentType(pub u16);

impl From<UnknownIncidentType> for u16 {
    fn from(x: UnknownIncidentType) -> Self {
        x.0
    }
}

impl TryFrom<u16> for IncidentType {
    type Error = UnknownIncidentType;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::INCIDENT_NONE),
            1 => Ok(Self::INCIDENT_LOST_EVENTS),
            x => Err(UnknownIncidentType(x)),
        }
    }
}

/// Type of an `InvarEvent`.
#[repr(u8)]
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IntvarEventType {
    INVALID_INT_EVENT,
    /// Indicates the value to use for the `LAST_INSERT_ID()` function in the next statement.
    LAST_INSERT_ID_EVENT,
    /// Indicates the value to use for an `AUTO_INCREMENT` column in the next statement.
    INSERT_ID_EVENT,
}

impl From<IntvarEventType> for u8 {
    fn from(x: IntvarEventType) -> Self {
        x as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown intvar event type {}", _0)]
#[repr(transparent)]
pub struct UnknownIntvarEventType(pub u8);

impl From<UnknownIntvarEventType> for u8 {
    fn from(x: UnknownIntvarEventType) -> Self {
        x.0
    }
}

impl TryFrom<u8> for IntvarEventType {
    type Error = UnknownIntvarEventType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::INVALID_INT_EVENT),
            1 => Ok(Self::LAST_INSERT_ID_EVENT),
            2 => Ok(Self::INSERT_ID_EVENT),
            x => Err(UnknownIntvarEventType(x)),
        }
    }
}

my_bitflags! {
    GtidFlags,
    #[error("Unknown flags in the raw value of GtidFlags (raw={:b})", _0)]
    UnknownGtidFlags,
    u8,

    /// Gtid event flags.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct GtidFlags: u8 {
        /// Transaction may have changes logged with SBR.
        ///
        /// In 5.6, 5.7.0-5.7.18, and 8.0.0-8.0.1, this flag is always set.
        /// Starting in 5.7.19 and 8.0.2, this flag is cleared if the transaction
        /// only contains row events. It is set if any part of the transaction is
        /// written in statement format.
        const MAY_HAVE_SBR = 0x01;
    }
}

/// Group number of a Gtid event.
///
/// Should be between `MIN_GNO` and `MAX_GNO` for GtidEvent and `0` for AnonymousGtidEvent.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Gno(u64);

impl Gno {
    pub const MIN_GNO: u64 = 1;
    pub const MAX_GNO: u64 = i64::MAX as u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error(
    "Group number {} is out of range [{}, {}]",
    _0,
    Gno::MIN_GNO,
    Gno::MAX_GNO
)]
#[repr(transparent)]
pub struct InvalidGno(pub u64);

impl From<InvalidGno> for u64 {
    fn from(x: InvalidGno) -> Self {
        x.0
    }
}

impl TryFrom<u64> for Gno {
    type Error = InvalidGno;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 || (Self::MIN_GNO..Self::MAX_GNO).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidGno(value))
        }
    }
}

my_bitflags! {
    RowsEventFlags,
    #[error("Unknown flags in the raw value of RowsEventFlags (raw={:b})", _0)]
    UnknownRowsEventFlags,
    u16,

    /// Rows event flags.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct RowsEventFlags: u16 {
        /// Last event of a statement.
        const STMT_END = 0x0001;
        /// No foreign key checks.
        const NO_FOREIGN_KEY_CHECKS   = 0x0002;
        /// No unique key checks.
        const RELAXED_UNIQUE_CHECKS  = 0x0004;
        /// Indicates that rows in this event are complete,
        /// that is contain values for all columns of the table.
        const COMPLETE_ROWS = 0x0008;
    }
}

my_bitflags! {
    UserVarFlags,
    #[error("Unknown flags in the raw value of UserVarFlags (raw={:b})", _0)]
    UnknownUserVarFlags,
    u8,

    /// Flags of a user variable.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct UserVarFlags: u8 {
        const UNSIGNED = 0x01;
    }
}

/// The on-the-wire fields for Transaction_payload
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TransactionPayloadFields {
    // Marks the end of the payload header.
    OTW_PAYLOAD_HEADER_END_MARK = 0,

    // The payload field
    OTW_PAYLOAD_SIZE_FIELD = 1,

    // The compression type field
    OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2,

    // The uncompressed size field
    OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3,
    // Other fields are appended here.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown TransactionPayloadFields operation {}", _0)]
#[repr(transparent)]
pub struct UnknownTransactionPayloadFields(pub u8);

impl TryFrom<u8> for TransactionPayloadFields {
    type Error = UnknownTransactionPayloadFields;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::OTW_PAYLOAD_HEADER_END_MARK),
            1 => Ok(Self::OTW_PAYLOAD_SIZE_FIELD),
            2 => Ok(Self::OTW_PAYLOAD_COMPRESSION_TYPE_FIELD),
            3 => Ok(Self::OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD),
            x => Err(UnknownTransactionPayloadFields(x)),
        }
    }
}

impl From<TransactionPayloadFields> for u8 {
    fn from(x: TransactionPayloadFields) -> Self {
        x as u8
    }
}

impl From<TransactionPayloadFields> for u64 {
    fn from(x: TransactionPayloadFields) -> Self {
        x as u64
    }
}

impl TryFrom<u64> for TransactionPayloadFields {
    type Error = UnknownTransactionPayloadFields;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::OTW_PAYLOAD_HEADER_END_MARK),
            1 => Ok(Self::OTW_PAYLOAD_SIZE_FIELD),
            2 => Ok(Self::OTW_PAYLOAD_COMPRESSION_TYPE_FIELD),
            3 => Ok(Self::OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD),
            x => Err(UnknownTransactionPayloadFields(x.try_into().unwrap())),
        }
    }
}
/// The supported compression algorithms for Transaction_payload
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TransactionPayloadCompressionType {
    // ZSTD compression.
    ZSTD = 0,

    // No compression.
    NONE = 255,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown TransactionPayloadCompressionType {}", _0)]
#[repr(transparent)]
pub struct UnknownTransactionPayloadCompressionType(pub u8);

impl TryFrom<u8> for TransactionPayloadCompressionType {
    type Error = UnknownTransactionPayloadCompressionType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ZSTD),
            255 => Ok(Self::NONE),
            x => Err(UnknownTransactionPayloadCompressionType(x)),
        }
    }
}

impl TryFrom<u64> for TransactionPayloadCompressionType {
    type Error = UnknownTransactionPayloadCompressionType;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ZSTD),
            255 => Ok(Self::NONE),
            x => Err(UnknownTransactionPayloadCompressionType(
                x.try_into().unwrap(),
            )),
        }
    }
}
