// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::convert::TryFrom;

pub static MAX_PAYLOAD_LEN: usize = 16_777_215;
pub static DEFAULT_MAX_ALLOWED_PACKET: usize = 4 * 1024 * 1024;
pub static MIN_COMPRESS_LENGTH: usize = 50;

pub static UTF8_GENERAL_CI: u16 = 33;
pub static UTF8MB4_GENERAL_CI: u16 = 45;

my_bitflags! {
    StatusFlags,
    #[error("Unknown flags in the raw value of StatusFlags (raw={:b})", _0)]
    UnknownStatusFlags,
    u16,

    /// MySql server status flags
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct StatusFlags: u16 {
        /// Is raised when a multi-statement transaction has been started, either explicitly,
        /// by means of BEGIN or COMMIT AND CHAIN, or implicitly, by the first transactional
        /// statement, when autocommit=off.
        const SERVER_STATUS_IN_TRANS             = 0x0001;

        /// Server in auto_commit mode.
        const SERVER_STATUS_AUTOCOMMIT           = 0x0002;

        /// Multi query - next query exists.
        const SERVER_MORE_RESULTS_EXISTS         = 0x0008;

        const SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010;

        const SERVER_STATUS_NO_INDEX_USED        = 0x0020;

        /// The server was able to fulfill the clients request and opened a read-only
        /// non-scrollable cursor for a query. This flag comes in reply to COM_STMT_EXECUTE
        /// and COM_STMT_FETCH commands. Used by Binary Protocol Resultset to signal that
        /// COM_STMT_FETCH must be used to fetch the row-data.
        const SERVER_STATUS_CURSOR_EXISTS        = 0x0040;

        /// This flag is sent when a read-only cursor is exhausted, in reply to
        /// COM_STMT_FETCH command.
        const SERVER_STATUS_LAST_ROW_SENT        = 0x0080;

        /// A database was dropped.
        const SERVER_STATUS_DB_DROPPED           = 0x0100;

        const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;

        /// Sent to the client if after a prepared statement reprepare we discovered
        /// that the new statement returns a different number of result set columns.
        const SERVER_STATUS_METADATA_CHANGED     = 0x0400;

        const SERVER_QUERY_WAS_SLOW              = 0x0800;

        /// To mark ResultSet containing output parameter values.
        const SERVER_PS_OUT_PARAMS               = 0x1000;

        /// Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement
        /// transaction is a read-only transaction. Cleared when the transaction commits
        /// or aborts. Since this flag is sent to clients in OK and EOF packets, the flag
        /// indicates the transaction status at the end of command execution.
        const SERVER_STATUS_IN_TRANS_READONLY    = 0x2000;

        /// This status flag, when on, implies that one of the state information has
        /// changed on the server because of the execution of the last statement.
        const SERVER_SESSION_STATE_CHANGED       = 0x4000;

        /// Introduced by mariadb. Contain the information about ANSI_QUOTES SQL_MODE.
        const SERVER_STATUS_ANSI_QUOTES       = 0x8000;
    }
}

my_bitflags! {
    CapabilityFlags,
    #[error("Unknown flags in the raw value of CapabilityFlags (raw={:b})", _0)]
    UnknownCapabilityFlags,
    u32,

    /// Client capability flags
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct CapabilityFlags: u32 {
        /// Use the improved version of Old Password Authentication. Assumed to be set since 4.1.1.
        const CLIENT_LONG_PASSWORD                  = 0x0000_0001;

        /// Send found rows instead of affected rows in EOF_Packet.
        const CLIENT_FOUND_ROWS                     = 0x0000_0002;

        /// Get all column flags.
        /// Longer flags in Protocol::ColumnDefinition320.
        ///
        /// ### Server
        /// Supports longer flags.
        ///
        /// ### Client
        /// Expects longer flags.
        const CLIENT_LONG_FLAG                      = 0x0000_0004;

        /// Database (schema) name can be specified on connect in Handshake Response Packet.
        /// ### Server
        /// Supports schema-name in Handshake Response Packet.
        ///
        /// ### Client
        /// Handshake Response Packet contains a schema-name.
        const CLIENT_CONNECT_WITH_DB                = 0x0000_0008;

        /// Don't allow database.table.column.
        const CLIENT_NO_SCHEMA                      = 0x0000_0010;

        /// Compression protocol supported.
        ///
        /// ### Server
        /// Supports compression.
        ///
        /// ### Client
        /// Switches to Compression compressed protocol after successful authentication.
        const CLIENT_COMPRESS                       = 0x0000_0020;

        /// Special handling of ODBC behavior.
        const CLIENT_ODBC                           = 0x0000_0040;

        /// Can use LOAD DATA LOCAL.
        ///
        /// ### Server
        /// Enables the LOCAL INFILE request of LOAD DATA|XML.
        ///
        /// ### Client
        /// Will handle LOCAL INFILE request.
        const CLIENT_LOCAL_FILES                    = 0x0000_0080;

        /// Ignore spaces before '('.
        ///
        /// ### Server
        /// Parser can ignore spaces before '('.
        ///
        /// ### Client
        /// Let the parser ignore spaces before '('.
        const CLIENT_IGNORE_SPACE                   = 0x0000_0100;

        const CLIENT_PROTOCOL_41                    = 0x0000_0200;

        /// This is an interactive client.
        /// Use System_variables::net_wait_timeout versus System_variables::net_interactive_timeout.
        ///
        /// ### Server
        /// Supports interactive and noninteractive clients.
        ///
        /// ### Client
        /// Client is interactive.
        const CLIENT_INTERACTIVE                    = 0x0000_0400;

        /// Use SSL encryption for the session.
        ///
        /// ### Server
        /// Supports SSL
        ///
        /// ### Client
        /// Switch to SSL after sending the capability-flags.
        const CLIENT_SSL                            = 0x0000_0800;

        /// Client only flag. Not used.
        ///
        /// ### Client
        /// Do not issue SIGPIPE if network failures occur (libmysqlclient only).
        const CLIENT_IGNORE_SIGPIPE                 = 0x0000_1000;

        /// Client knows about transactions.
        ///
        /// ### Server
        /// Can send status flags in OK_Packet / EOF_Packet.
        ///
        /// ### Client
        /// Expects status flags in OK_Packet / EOF_Packet.
        ///
        /// ### Note
        /// This flag is optional in 3.23, but always set by the server since 4.0.
        const CLIENT_TRANSACTIONS                   = 0x0000_2000;

        const CLIENT_RESERVED                       = 0x0000_4000;

        const CLIENT_SECURE_CONNECTION              = 0x0000_8000;

        /// Enable/disable multi-stmt support.
        /// Also sets CLIENT_MULTI_RESULTS. Currently not checked anywhere.
        ///
        /// ### Server
        /// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
        ///
        /// ### Client
        /// May send multiple statements per COM_QUERY and COM_STMT_PREPARE.
        const CLIENT_MULTI_STATEMENTS               = 0x0001_0000;

        /// Enable/disable multi-results.
        ///
        /// ### Server
        /// Can send multiple resultsets for COM_QUERY. Error if the server needs to send
        /// them and client does not support them.
        ///
        /// ### Client
        /// Can handle multiple resultsets for COM_QUERY.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_MULTI_RESULTS                  = 0x0002_0000;

        /// Multi-results and OUT parameters in PS-protocol.
        ///
        /// ### Server
        /// Can send multiple resultsets for COM_STMT_EXECUTE.
        ///
        /// ### Client
        /// Can handle multiple resultsets for COM_STMT_EXECUTE.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_PS_MULTI_RESULTS               = 0x0004_0000;

        /// Client supports plugin authentication.
        ///
        /// ### Server
        /// Sends extra data in Initial Handshake Packet and supports the pluggable
        /// authentication protocol.
        ///
        /// ### Client
        /// Supports authentication plugins.
        ///
        /// ### Requires
        /// `CLIENT_PROTOCOL_41`
        const CLIENT_PLUGIN_AUTH                    = 0x0008_0000;

        /// Client supports connection attributes.
        ///
        /// ### Server
        /// Permits connection attributes in Protocol::HandshakeResponse41.
        ///
        /// ### Client
        /// Sends connection attributes in Protocol::HandshakeResponse41.
        const CLIENT_CONNECT_ATTRS                  = 0x0010_0000;

        /// Enable authentication response packet to be larger than 255 bytes.
        /// When the ability to change default plugin require that the initial password
        /// field in the Protocol::HandshakeResponse41 paclet can be of arbitrary size.
        /// However, the 4.1 client-server protocol limits the length of the auth-data-field
        /// sent from client to server to 255 bytes. The solution is to change the type of
        /// the field to a true length encoded string and indicate the protocol change with
        /// this client capability flag.
        ///
        /// ### Server
        /// Understands length-encoded integer for auth response data in
        /// Protocol::HandshakeResponse41.
        ///
        /// ### Client
        /// Length of auth response data in Protocol::HandshakeResponse41 is a
        /// length-encoded integer.
        ///
        /// ### Note
        /// The flag was introduced in 5.6.6, but had the wrong value.
        const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020_0000;

        /// Don't close the connection for a user account with expired password.
        ///
        /// ### Server
        /// Announces support for expired password extension.
        ///
        /// ### Client
        /// Can handle expired passwords.
        const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x0040_0000;

        /// Capable of handling server state change information.
        /// Its a hint to the server to include the state change information in OK_Packet.
        ///
        /// ### Server
        /// Can set SERVER_SESSION_STATE_CHANGED in the SERVER_STATUS_flags_enum and send
        /// Session State Information in a OK_Packet.
        ///
        /// ### Client
        /// Expects the server to send Session State Information in a OK_Packet.
        const CLIENT_SESSION_TRACK                  = 0x0080_0000;

        /// Client no longer needs EOF_Packet and will use OK_Packet instead.
        ///
        /// ### Server
        /// Can send OK after a Text Resultset.
        ///
        /// ### Client
        /// Expects an OK_Packet (instead of EOF_Packet) after the resultset
        /// rows of a Text Resultset.
        ///
        /// ### Background
        /// To support CLIENT_SESSION_TRACK, additional information must be sent after all
        /// successful commands. Although the OK_Packet is extensible, the EOF_Packet is
        /// not due to the overlap of its bytes with the content of the Text Resultset Row.
        ///
        /// Therefore, the EOF_Packet in the Text Resultset is replaced with an OK_Packet.
        /// EOF_Packet is deprecated as of MySQL 5.7.5.
        const CLIENT_DEPRECATE_EOF                  = 0x0100_0000;

        /// The client can handle optional metadata information in the resultset.
        const CLIENT_OPTIONAL_RESULTSET_METADATA    = 0x0200_0000;

        /// Compression protocol extended to support zstd compression method.
        ///
        /// This capability flag is used to send zstd compression level between client and server
        /// provided both client and server are enabled with this flag.
        ///
        /// # Server
        ///
        /// Server sets this flag when global variable protocol-compression-algorithms has zstd
        /// in its list of supported values.
        ///
        /// # Client
        ///
        /// Client sets this flag when it is configured to use zstd compression method.
        const CLIENT_ZSTD_COMPRESSION_ALGORITHM     = 0x0400_0000;

        /// Support optional extension for query parameters into the COM_QUERY
        /// and COM_STMT_EXECUTE packets.
        ///
        /// # Server
        ///
        /// Expects an optional part containing the query parameter set(s).
        /// Executes the query for each set of parameters or returns an error if more than 1 set
        /// of parameters is sent and the server can't execute it.
        ///
        /// # Client
        ///
        /// Can send the optional part containing the query parameter set(s).
        const CLIENT_QUERY_ATTRIBUTES               = 0x0800_0000;

        /// Support Multi factor authentication.
        ///
        /// # Server
        ///
        /// Server sends AuthNextFactor packet after every nth factor
        /// authentication method succeeds, except the last factor authentication.
        ///
        /// # Client
        ///
        /// Client reads AuthNextFactor packet sent by server
        /// and initiates next factor authentication method.
        const MULTI_FACTOR_AUTHENTICATION           = 0x1000_0000;

        /// Client or server supports progress reports within error packet.
        const CLIENT_PROGRESS_OBSOLETE              = 0x2000_0000;

        /// Verify server certificate. Client only flag.
        ///
        /// Deprecated in favor of –ssl-mode.
        const CLIENT_SSL_VERIFY_SERVER_CERT         = 0x4000_0000;

        /// Don't reset the options after an unsuccessful connect. Client only flag.
        const CLIENT_REMEMBER_OPTIONS               = 0x8000_0000;
    }
}

my_bitflags! {
    CursorType,
    #[error("Unknown flags in the raw value of CursorType (raw={:b})", _0)]
    UnknownCursorType,
    u8,

    /// Mysql cursor type.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct CursorType: u8 {
        const CURSOR_TYPE_NO_CURSOR  = 0_u8;
        const CURSOR_TYPE_READ_ONLY  = 1_u8;
        const CURSOR_TYPE_FOR_UPDATE = 2_u8;
        const CURSOR_TYPE_SCROLLABLE = 4_u8;
    }
}

my_bitflags! {
    StmtExecuteParamsFlags,
    #[error("Unknown flags in the raw value of StmtExecuteParamsFlags (raw={:b})", _0)]
    UnknownStmtExecuteParamsFlags,
    u8,

    /// MySql stmt execute params flags.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct StmtExecuteParamsFlags: u8 {
        const NEW_PARAMS_BOUND  = 1_u8;
    }
}

my_bitflags! {
    StmtExecuteParamFlags,
    #[error("Unknown flags in the raw value of StmtExecuteParamFlags (raw={:b})", _0)]
    UnknownStmtExecuteParamFlags,
    u8,

    /// MySql stmt execute params flags.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct StmtExecuteParamFlags: u8 {
        const UNSIGNED  = 128_u8;
    }
}

my_bitflags! {
    ColumnFlags,
    #[error("Unknown flags in the raw value of ColumnFlags (raw={:b})", _0)]
    UnknownColumnFlags,
    u16,

    /// MySql column flags
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct ColumnFlags: u16 {
        /// Field can't be NULL.
        const NOT_NULL_FLAG         = 1u16;

        /// Field is part of a primary key.
        const PRI_KEY_FLAG          = 2u16;

        /// Field is part of a unique key.
        const UNIQUE_KEY_FLAG       = 4u16;

        /// Field is part of a key.
        const MULTIPLE_KEY_FLAG     = 8u16;

        /// Field is a blob.
        const BLOB_FLAG             = 16u16;

        /// Field is unsigned.
        const UNSIGNED_FLAG         = 32u16;

        /// Field is zerofill.
        const ZEROFILL_FLAG         = 64u16;

        /// Field is binary.
        const BINARY_FLAG           = 128u16;

        /// Field is an enum.
        const ENUM_FLAG             = 256u16;

        /// Field is a autoincrement field.
        const AUTO_INCREMENT_FLAG   = 512u16;

        /// Field is a timestamp.
        const TIMESTAMP_FLAG        = 1024u16;

        /// Field is a set.
        const SET_FLAG              = 2048u16;

        /// Field doesn't have default value.
        const NO_DEFAULT_VALUE_FLAG = 4096u16;

        /// Field is set to NOW on UPDATE.
        const ON_UPDATE_NOW_FLAG    = 8192u16;

        /// Intern; Part of some key.
        const PART_KEY_FLAG         = 16384u16;

        /// Field is num (for clients).
        const NUM_FLAG              = 32768u16;
    }
}

/// MySql server commands
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum Command {
    COM_SLEEP = 0,
    COM_QUIT,
    COM_INIT_DB,
    COM_QUERY,
    COM_FIELD_LIST,
    COM_CREATE_DB,
    COM_DROP_DB,
    COM_REFRESH,
    COM_DEPRECATED_1,
    COM_STATISTICS,
    COM_PROCESS_INFO,
    COM_CONNECT,
    COM_PROCESS_KILL,
    COM_DEBUG,
    COM_PING,
    COM_TIME,
    COM_DELAYED_INSERT,
    COM_CHANGE_USER,
    COM_BINLOG_DUMP,
    COM_TABLE_DUMP,
    COM_CONNECT_OUT,
    COM_REGISTER_SLAVE,
    COM_STMT_PREPARE,
    COM_STMT_EXECUTE,
    COM_STMT_SEND_LONG_DATA,
    COM_STMT_CLOSE,
    COM_STMT_RESET,
    COM_SET_OPTION,
    COM_STMT_FETCH,
    COM_DAEMON,
    COM_BINLOG_DUMP_GTID,
    COM_RESET_CONNECTION,
    COM_END,
}

/// Type of state change information (part of MySql's Ok packet).
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum SessionStateType {
    /// Session system variables.
    SESSION_TRACK_SYSTEM_VARIABLES,
    /// Current schema.
    SESSION_TRACK_SCHEMA,
    /// track session state changes
    SESSION_TRACK_STATE_CHANGE,
    /// See also: session_track_gtids.
    SESSION_TRACK_GTIDS,
    /// Transaction characteristics.
    SESSION_TRACK_TRANSACTION_CHARACTERISTICS,
    /// Transaction state.
    SESSION_TRACK_TRANSACTION_STATE,
}

impl From<SessionStateType> for u8 {
    fn from(x: SessionStateType) -> u8 {
        x as u8
    }
}

impl TryFrom<u8> for SessionStateType {
    type Error = UnknownSessionStateType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(SessionStateType::SESSION_TRACK_SYSTEM_VARIABLES),
            0x01 => Ok(SessionStateType::SESSION_TRACK_SCHEMA),
            0x02 => Ok(SessionStateType::SESSION_TRACK_STATE_CHANGE),
            0x03 => Ok(SessionStateType::SESSION_TRACK_GTIDS),
            0x04 => Ok(SessionStateType::SESSION_TRACK_TRANSACTION_CHARACTERISTICS),
            0x05 => Ok(SessionStateType::SESSION_TRACK_TRANSACTION_STATE),
            x => Err(UnknownSessionStateType(x)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Unknown session state type {}", _0)]
pub struct UnknownSessionStateType(pub u8);

/// Geometry type.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
#[allow(non_camel_case_types)]
#[repr(u8)]
pub enum GeometryType {
    GEOM_GEOMETRY,
    GEOM_POINT,
    GEOM_LINESTRING,
    GEOM_POLYGON,
    GEOM_MULTIPOINT,
    GEOM_MULTILINESTRING,
    GEOM_MULTIPOLYGON,
    GEOM_GEOMETRYCOLLECTION,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Unknown geometry type {}", _0)]
#[repr(transparent)]
pub struct UnknownGeometryType(pub u8);

impl From<UnknownGeometryType> for u8 {
    fn from(x: UnknownGeometryType) -> Self {
        x.0
    }
}

impl TryFrom<u8> for GeometryType {
    type Error = UnknownGeometryType;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(GeometryType::GEOM_GEOMETRY),
            1 => Ok(GeometryType::GEOM_POINT),
            2 => Ok(GeometryType::GEOM_LINESTRING),
            3 => Ok(GeometryType::GEOM_POLYGON),
            4 => Ok(GeometryType::GEOM_MULTIPOINT),
            5 => Ok(GeometryType::GEOM_MULTILINESTRING),
            6 => Ok(GeometryType::GEOM_MULTIPOLYGON),
            7 => Ok(GeometryType::GEOM_GEOMETRYCOLLECTION),
            x => Err(UnknownGeometryType(x)),
        }
    }
}

/// Type of MySql column field
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug, Hash)]
#[repr(u8)]
pub enum ColumnType {
    MYSQL_TYPE_DECIMAL = 0,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, // Internal to MySql
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_TYPED_ARRAY, // Used for replication only
    MYSQL_TYPE_UNKNOWN = 243,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255,
}

impl ColumnType {
    pub fn is_numeric_type(&self) -> bool {
        use ColumnType::*;
        matches!(
            self,
            MYSQL_TYPE_TINY
                | MYSQL_TYPE_SHORT
                | MYSQL_TYPE_INT24
                | MYSQL_TYPE_LONG
                | MYSQL_TYPE_LONGLONG
                | MYSQL_TYPE_DECIMAL
                | MYSQL_TYPE_NEWDECIMAL
                | MYSQL_TYPE_FLOAT
                | MYSQL_TYPE_DOUBLE
                | MYSQL_TYPE_YEAR // Yes, this is also included
        )
    }

    pub fn is_character_type(&self) -> bool {
        use ColumnType::*;
        matches!(
            self,
            MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_BLOB
        )
    }

    pub fn is_enum_or_set_type(&self) -> bool {
        use ColumnType::*;
        matches!(self, MYSQL_TYPE_ENUM | MYSQL_TYPE_SET)
    }

    pub fn is_enum_type(&self) -> bool {
        matches!(self, ColumnType::MYSQL_TYPE_ENUM)
    }

    pub fn is_set_type(&self) -> bool {
        matches!(self, ColumnType::MYSQL_TYPE_SET)
    }

    pub fn is_geometry_type(&self) -> bool {
        matches!(self, ColumnType::MYSQL_TYPE_GEOMETRY)
    }
}

impl TryFrom<u8> for ColumnType {
    type Error = UnknownColumnType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00_u8 => Ok(ColumnType::MYSQL_TYPE_DECIMAL),
            0x01_u8 => Ok(ColumnType::MYSQL_TYPE_TINY),
            0x02_u8 => Ok(ColumnType::MYSQL_TYPE_SHORT),
            0x03_u8 => Ok(ColumnType::MYSQL_TYPE_LONG),
            0x04_u8 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
            0x05_u8 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
            0x06_u8 => Ok(ColumnType::MYSQL_TYPE_NULL),
            0x07_u8 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
            0x08_u8 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
            0x09_u8 => Ok(ColumnType::MYSQL_TYPE_INT24),
            0x0a_u8 => Ok(ColumnType::MYSQL_TYPE_DATE),
            0x0b_u8 => Ok(ColumnType::MYSQL_TYPE_TIME),
            0x0c_u8 => Ok(ColumnType::MYSQL_TYPE_DATETIME),
            0x0d_u8 => Ok(ColumnType::MYSQL_TYPE_YEAR),
            0x0f_u8 => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
            0x10_u8 => Ok(ColumnType::MYSQL_TYPE_BIT),
            0x11_u8 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP2),
            0x12_u8 => Ok(ColumnType::MYSQL_TYPE_DATETIME2),
            0x13_u8 => Ok(ColumnType::MYSQL_TYPE_TIME2),
            0x14_u8 => Ok(ColumnType::MYSQL_TYPE_TYPED_ARRAY),
            0xf3_u8 => Ok(ColumnType::MYSQL_TYPE_UNKNOWN),
            0xf5_u8 => Ok(ColumnType::MYSQL_TYPE_JSON),
            0xf6_u8 => Ok(ColumnType::MYSQL_TYPE_NEWDECIMAL),
            0xf7_u8 => Ok(ColumnType::MYSQL_TYPE_ENUM),
            0xf8_u8 => Ok(ColumnType::MYSQL_TYPE_SET),
            0xf9_u8 => Ok(ColumnType::MYSQL_TYPE_TINY_BLOB),
            0xfa_u8 => Ok(ColumnType::MYSQL_TYPE_MEDIUM_BLOB),
            0xfb_u8 => Ok(ColumnType::MYSQL_TYPE_LONG_BLOB),
            0xfc_u8 => Ok(ColumnType::MYSQL_TYPE_BLOB),
            0xfd_u8 => Ok(ColumnType::MYSQL_TYPE_VAR_STRING),
            0xfe_u8 => Ok(ColumnType::MYSQL_TYPE_STRING),
            0xff_u8 => Ok(ColumnType::MYSQL_TYPE_GEOMETRY),
            x => Err(UnknownColumnType(x)),
        }
    }
}

impl From<ColumnType> for u8 {
    fn from(val: ColumnType) -> u8 {
        val as u8
    }
}

my_bitflags! {
    Flags2,
    #[error("Unknown flags in the raw value of Flags2 (raw={:b})", _0)]
    UnknownFlags2,
    u32,

    /// Bitmask of flags that are usually set with `SET`.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct Flags2: u32 {
        const OPTION_AUTO_IS_NULL          = 0x00004000;
        const OPTION_NOT_AUTOCOMMIT        = 0x00080000;
        const OPTION_NO_FOREIGN_KEY_CHECKS = 0x04000000;
        const OPTION_RELAXED_UNIQUE_CHECKS = 0x08000000;
    }
}

my_bitflags! {
    SqlMode,
    #[error("Unknown flags in the raw value of SqlMode (raw={:b})", _0)]
    UnknownSqlMode,
    u64,

    /// Bitmask of flags that are usually set with `SET sql_mode`.
    #[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
    pub struct SqlMode: u64 {
        const MODE_REAL_AS_FLOAT              = 0x00000001;
        const MODE_PIPES_AS_CONCAT            = 0x00000002;
        const MODE_ANSI_QUOTES                = 0x00000004;
        const MODE_IGNORE_SPACE               = 0x00000008;
        const MODE_NOT_USED                   = 0x00000010;
        const MODE_ONLY_FULL_GROUP_BY         = 0x00000020;
        const MODE_NO_UNSIGNED_SUBTRACTION    = 0x00000040;
        const MODE_NO_DIR_IN_CREATE           = 0x00000080;
        const MODE_POSTGRESQL                 = 0x00000100;
        const MODE_ORACLE                     = 0x00000200;
        const MODE_MSSQL                      = 0x00000400;
        const MODE_DB2                        = 0x00000800;
        const MODE_MAXDB                      = 0x00001000;
        const MODE_NO_KEY_OPTIONS             = 0x00002000;
        const MODE_NO_FIELD_OPTIONS           = 0x00008000;
        const MODE_NO_TABLE_OPTIONS           = 0x00004000;
        const MODE_MYSQL40                    = 0x00020000;
        const MODE_MYSQL323                   = 0x00010000;
        const MODE_ANSI                       = 0x00040000;
        const MODE_NO_AUTO_VALUE_ON_ZERO      = 0x00080000;
        const MODE_NO_BACKSLASH_ESCAPES       = 0x00100000;
        const MODE_STRICT_TRANS_TABLES        = 0x00200000;
        const MODE_STRICT_ALL_TABLES          = 0x00400000;
        const MODE_NO_ZERO_IN_DATE            = 0x00800000;
        const MODE_NO_ZERO_DATE               = 0x01000000;
        const MODE_INVALID_DATES              = 0x02000000;
        const MODE_ERROR_FOR_DIVISION_BY_ZERO = 0x04000000;
        const MODE_TRADITIONAL                = 0x08000000;
        const MODE_NO_AUTO_CREATE_USER        = 0x10000000;
        const MODE_HIGH_NOT_PRECEDENCE        = 0x20000000;
        const MODE_NO_ENGINE_SUBSTITUTION     = 0x40000000;
        const MODE_PAD_CHAR_TO_FULL_LENGTH    = 0x80000000;
        const MODE_TIME_TRUNCATE_FRACTIONAL   = 0x100000000;
        const MODE_LAST                       = 0x200000000;
    }
}

/// Type of the user defined function return slot and arguments.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[allow(non_camel_case_types)]
#[repr(i8)]
pub enum ItemResult {
    /// not valid for UDFs
    INVALID_RESULT = -1,
    /// char *
    STRING_RESULT = 0,
    REAL_RESULT,
    /// double
    /// long long
    INT_RESULT,
    /// not valid for UDFs
    ROW_RESULT,
    /// char *, to be converted to/from a decimal
    DECIMAL_RESULT,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown item result type {}", _0)]
pub struct UnknownItemResultType(pub i8);

impl From<UnknownItemResultType> for i8 {
    fn from(x: UnknownItemResultType) -> Self {
        x.0
    }
}

impl TryFrom<i8> for ItemResult {
    type Error = UnknownItemResultType;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(ItemResult::INVALID_RESULT),
            0 => Ok(ItemResult::STRING_RESULT),
            1 => Ok(ItemResult::REAL_RESULT),
            2 => Ok(ItemResult::INT_RESULT),
            3 => Ok(ItemResult::ROW_RESULT),
            4 => Ok(ItemResult::DECIMAL_RESULT),
            x => Err(UnknownItemResultType(x)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("Unknown column type {}", _0)]
pub struct UnknownColumnType(pub u8);

impl From<UnknownColumnType> for u8 {
    fn from(x: UnknownColumnType) -> Self {
        x.0
    }
}
