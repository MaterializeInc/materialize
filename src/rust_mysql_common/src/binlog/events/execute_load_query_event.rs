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
        consts::{BinlogVersion, EventType, LoadDuplicateHandling},
        BinlogCtx, BinlogEvent, BinlogStruct,
    },
    io::ParseBuf,
    misc::raw::{
        bytes::{BareU8Bytes, EofBytes},
        int::*,
        Const, RawBytes, RawInt, Skip,
    },
    proto::{MyDeserialize, MySerialize},
};

use super::{BinlogEventHeader, StatusVars};

/// Execute load query event.
///
/// Used for LOAD DATA INFILE statements as of MySQL 5.0.
///
/// It similar to Query_log_event but before executing the query it substitutes original filename
/// in LOAD DATA query with name of temporary file.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ExecuteLoadQueryEvent<'a> {
    // post-header
    thread_id: RawInt<LeU32>,
    execution_time: RawInt<LeU32>,
    schema_len: RawInt<u8>,
    error_code: RawInt<LeU16>,
    status_vars_len: RawInt<LeU16>,

    // payload
    /// File_id of a temporary file.
    file_id: RawInt<LeU32>,
    /// Pointer to the part of the query that should be substituted.
    start_pos: RawInt<LeU32>,
    /// Pointer to the end of this part of query
    end_pos: RawInt<LeU32>,
    /// How to handle duplicates.
    dup_handling: Const<LoadDuplicateHandling, u8>,

    status_vars: StatusVars<'a>,
    schema: RawBytes<'a, BareU8Bytes>,
    __skip: Skip<1>,
    query: RawBytes<'a, EofBytes>,
}

impl<'a> ExecuteLoadQueryEvent<'a> {
    /// Creates a new instance.
    pub fn new(
        file_id: u32,
        dup_handling: LoadDuplicateHandling,
        status_vars: impl Into<Cow<'a, [u8]>>,
        schema: impl Into<Cow<'a, [u8]>>,
    ) -> Self {
        let status_vars = StatusVars(RawBytes::new(status_vars));
        let schema = RawBytes::new(schema);
        Self {
            thread_id: Default::default(),
            execution_time: Default::default(),
            schema_len: RawInt::new(schema.len() as u8),
            error_code: Default::default(),
            status_vars_len: RawInt::new(status_vars.0.len() as u16),
            file_id: RawInt::new(file_id),
            start_pos: Default::default(),
            end_pos: Default::default(),
            dup_handling: Const::new(dup_handling),
            status_vars,
            schema,
            __skip: Default::default(),
            query: Default::default(),
        }
    }

    /// Sets the `thread_id` value.
    pub fn with_thread_id(mut self, thread_id: u32) -> Self {
        self.thread_id = RawInt::new(thread_id);
        self
    }

    /// Sets the `execution_time` value.
    pub fn with_execution_time(mut self, execution_time: u32) -> Self {
        self.execution_time = RawInt::new(execution_time);
        self
    }

    /// Sets the `error_code` value.
    pub fn with_error_code(mut self, error_code: u16) -> Self {
        self.error_code = RawInt::new(error_code);
        self
    }

    /// Sets the `file_id` value.
    pub fn with_file_id(mut self, file_id: u32) -> Self {
        self.file_id = RawInt::new(file_id);
        self
    }

    /// Sets the `start_pos` value.
    pub fn with_start_pos(mut self, start_pos: u32) -> Self {
        self.start_pos = RawInt::new(start_pos);
        self
    }

    /// Sets the `end_pos` value.
    pub fn with_end_pos(mut self, end_pos: u32) -> Self {
        self.end_pos = RawInt::new(end_pos);
        self
    }

    /// Sets the `dup_handling` value.
    pub fn with_dup_handling(mut self, dup_handling: LoadDuplicateHandling) -> Self {
        self.dup_handling = Const::new(dup_handling);
        self
    }

    /// Sets the `status_vars` value (max length is `u16::MAX).
    pub fn with_status_vars(mut self, status_vars: impl Into<Cow<'a, [u8]>>) -> Self {
        self.status_vars = StatusVars(RawBytes::new(status_vars));
        self.status_vars_len.0 = self.status_vars.0.len() as u16;
        self
    }

    /// Sets the `schema` value (max length is `u8::MAX).
    pub fn with_schema(mut self, schema: impl Into<Cow<'a, [u8]>>) -> Self {
        self.schema = RawBytes::new(schema);
        self.schema_len.0 = self.schema.len() as u8;
        self
    }

    /// Sets the `query` value.
    pub fn with_query(mut self, query: impl Into<Cow<'a, [u8]>>) -> Self {
        self.query = RawBytes::new(query);
        self
    }

    /// Returns the `thread_id` value.
    ///
    /// `thread_id` is the ID of the thread that issued this statement.
    /// It is needed for temporary tables.
    pub fn thread_id(&self) -> u32 {
        self.thread_id.0
    }

    /// Returns the `execution_time` value.
    ///
    /// `execution_time` is the time from when the query started to when it was logged
    /// in the binlog, in seconds.
    pub fn execution_time(&self) -> u32 {
        self.execution_time.0
    }

    /// Returns the `error_code` value.
    ///
    /// `error_code` is the error code generated by the master. If the master fails, the slave will
    /// fail with the same error code.
    pub fn error_code(&self) -> u16 {
        self.error_code.0
    }

    /// Returns the `file_id` value.
    ///
    /// `file_id` is the ID of the temporary file to load.
    pub fn file_id(&self) -> u32 {
        self.file_id.0
    }

    /// Returns the `start_pos` value.
    ///
    /// `start_pos` is the start position within the statement for filename substitution.
    pub fn start_pos(&self) -> u32 {
        self.start_pos.0
    }

    /// Returns the `end_pos` value.
    ///
    /// `end_pos` is the end position within the statement for filename substitution.
    pub fn end_pos(&self) -> u32 {
        self.end_pos.0
    }

    /// Returns the `dup_handling` value.
    ///
    /// `dup_handling` represents the information on how to handle duplicates.
    pub fn dup_handling(&self) -> LoadDuplicateHandling {
        self.dup_handling.0
    }

    /// Returns the `status_vars` value.
    ///
    /// `status_vars` contains zero or more status variables. Each status variable consists of one
    /// byte identifying the variable stored, followed by the value of the variable.
    pub fn status_vars_raw(&'a self) -> &'a [u8] {
        self.status_vars.0.as_bytes()
    }

    /// Returns an iterator over status variables.
    pub fn status_vars(&'a self) -> &'a StatusVars<'a> {
        &self.status_vars
    }

    /// Returns the `schema` value.
    ///
    /// `schema` is schema name.
    pub fn schema_raw(&'a self) -> &'a [u8] {
        self.schema.as_bytes()
    }

    /// Returns the `schema` value as a string (lossy converted).
    pub fn schema(&'a self) -> Cow<'a, str> {
        self.schema.as_str()
    }

    /// Returns the `query` value.
    ///
    /// `query` is the corresponding LOAD DATA INFILE statement.
    pub fn query_raw(&'a self) -> &'a [u8] {
        self.query.as_bytes()
    }

    /// Returns the `query` value as a string (lossy converted).
    pub fn query(&'a self) -> Cow<'a, str> {
        self.query.as_str()
    }

    pub fn into_owned(self) -> ExecuteLoadQueryEvent<'static> {
        ExecuteLoadQueryEvent {
            thread_id: self.thread_id,
            execution_time: self.execution_time,
            schema_len: self.schema_len,
            error_code: self.error_code,
            status_vars_len: self.status_vars_len,
            file_id: self.file_id,
            start_pos: self.start_pos,
            end_pos: self.end_pos,
            dup_handling: self.dup_handling,
            status_vars: self.status_vars.into_owned(),
            schema: self.schema.into_owned(),
            __skip: self.__skip,
            query: self.query.into_owned(),
        }
    }
}

impl<'de> MyDeserialize<'de> for ExecuteLoadQueryEvent<'de> {
    const SIZE: Option<usize> = None;
    type Ctx = BinlogCtx<'de>;

    fn deserialize(_: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut sbuf: ParseBuf = buf.parse(26)?;

        let thread_id = sbuf.parse_unchecked(())?;
        let execution_time = sbuf.parse_unchecked(())?;
        let schema_len: RawInt<u8> = sbuf.parse_unchecked(())?;
        let error_code = sbuf.parse_unchecked(())?;
        let status_vars_len: RawInt<LeU16> = sbuf.parse_unchecked(())?;
        let file_id = sbuf.parse_unchecked(())?;
        let start_pos = sbuf.parse_unchecked(())?;
        let end_pos = sbuf.parse_unchecked(())?;
        let dup_handling = sbuf.parse_unchecked(())?;

        let status_vars = buf.parse(*status_vars_len)?;
        let schema = buf.parse(*schema_len as usize)?;
        let __skip = buf.parse(())?;
        let query = buf.parse(())?;

        Ok(Self {
            thread_id,
            execution_time,
            schema_len,
            error_code,
            status_vars_len,
            file_id,
            start_pos,
            end_pos,
            dup_handling,
            status_vars,
            schema,
            __skip,
            query,
        })
    }
}

impl MySerialize for ExecuteLoadQueryEvent<'_> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.thread_id.serialize(&mut *buf);
        self.execution_time.serialize(&mut *buf);
        self.schema_len.serialize(&mut *buf);
        self.error_code.serialize(&mut *buf);
        self.status_vars_len.serialize(&mut *buf);
        self.file_id.serialize(&mut *buf);
        self.start_pos.serialize(&mut *buf);
        self.end_pos.serialize(&mut *buf);
        self.dup_handling.serialize(&mut *buf);
        self.status_vars.serialize(&mut *buf);
        self.schema.serialize(&mut *buf);
        self.__skip.serialize(&mut *buf);
        self.query.serialize(&mut *buf);
    }
}

impl<'a> BinlogStruct<'a> for ExecuteLoadQueryEvent<'a> {
    fn len(&self, _version: BinlogVersion) -> usize {
        let mut len = S(0);

        len += S(4); // thread_id
        len += S(4); // query_exec_time
        len += S(1); // db_len
        len += S(2); // error_code
        len += S(2); // status_vars_len
        len += S(4); // file_id
        len += S(4); // start_pos
        len += S(4); // end_pos
        len += S(1); // dup_handling_flags
        len += S(min(self.status_vars.0.len(), u16::MAX as usize - 13)); // status_vars
        len += S(min(self.schema.0.len(), u8::MAX as usize)); // db_len
        len += S(1); // null-byte
        len += S(self.query.0.len());

        min(len.0, u32::MAX as usize - BinlogEventHeader::LEN)
    }
}

impl<'a> BinlogEvent<'a> for ExecuteLoadQueryEvent<'a> {
    const EVENT_TYPE: EventType = EventType::EXECUTE_LOAD_QUERY_EVENT;
}
