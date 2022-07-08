// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! pgtest is a Postgres wire protocol tester using
//! datadriven test files. It can be used to send [specific
//! messages](https://www.postgresql.org/docs/current/protocol-message-formats.html)
//! to any Postgres-compatible server and record received messages.
//!
//! The following datadriven directives are supported. They support a
//! `conn=name` argument to specify a non-default connection.
//! - `send`: Sends input messages to the server. Arguments, if needed,
//! are specified using JSON. Refer to the associated types to see
//! supported arguments. Arguments can be omitted to use defaults.
//! - `until`: Waits until input messages have been received from the
//! server. Additional messages are accumulated and returned as well.
//!
//! During debugging, set the environment variable `PGTEST_VERBOSE=1` to see
//! messages sent and received.
//!
//! Supported `send` types:
//! - [`Query`](struct.Query.html)
//! - [`Parse`](struct.Parse.html)
//! - [`Describe`](struct.Describe.html)
//! - [`Bind`](struct.Bind.html)
//! - [`Execute`](struct.Execute.html)
//! - `Sync`
//!
//! Supported `until` arguments:
//! - `no_error_fields` causes `ErrorResponse` messages to have empty
//! contents. Useful when none of our fields match Postgres. For example `until
//! no_error_fields`.
//! - `err_field_typs` specifies the set of error message fields
//! ([reference](https://www.postgresql.org/docs/current/protocol-error-fields.html)).
//! The default is `CMS` (code, message, severity).
//! For example: `until err_field_typs=SC` would return the severity and code
//! fields in any ErrorResponse message.
//!
//! For example, to execute a simple prepared statement:
//! ```pgtest
//! send
//! Parse {"query": "SELECT $1::text, 1 + $2::int4"}
//! Bind {"values": ["blah", "4"]}
//! Execute
//! Sync
//! ----
//!
//! until
//! ReadyForQuery
//! ----
//! ParseComplete
//! BindComplete
//! DataRow {"fields":["blah","5"]}
//! CommandComplete {"tag":"SELECT 1"}
//! ReadyForQuery {"status":"I"}
//! ```
//!
//! # Usage while writing tests
//!
//! The expected way to use this while writing tests is to generate output from a postgres server.
//! Use the `pgtest-mz` directory if our output differs incompatibly from postgres.
//! Write your test, excluding any lines after the `----` of the `until` directive.
//! For example:
//! ```pgtest
//! send
//! Query {"query": "SELECT 1"}
//! ----
//!
//! until
//! ReadyForQuery
//! ----
//! ```
//! Then run the pgtest binary, enabling rewrites and pointing it at postgres:
//! ```shell
//! REWRITE=1 cargo run --bin mz-pgtest -- test/pgtest/test.pt --addr localhost:5432 --user postgres
//! ```
//! This will generate the expected output for the `until` directive.
//! Now rerun against a running Materialize server:
//! ```shell
//! cargo run --bin mz-pgtest -- test/pgtest/test.pt
//! ```

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use anyhow::bail;
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use mz_ore::collections::CollectionExt;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::IsNull;
use serde::{Deserialize, Serialize};

struct PgConn {
    stream: TcpStream,
    recv_buf: BytesMut,
    send_buf: BytesMut,
    timeout: Duration,
    verbose: bool,
}

impl PgConn {
    fn new(addr: &str, user: &str, timeout: Duration, verbose: bool) -> anyhow::Result<Self> {
        let mut conn = Self {
            stream: TcpStream::connect(addr)?,
            recv_buf: BytesMut::new(),
            send_buf: BytesMut::new(),
            timeout,
            verbose,
        };

        conn.stream.set_read_timeout(Some(timeout))?;
        conn.send(|buf| frontend::startup_message(vec![("user", user)], buf).unwrap())?;
        match conn.recv()?.1 {
            Message::AuthenticationOk => {}
            _ => bail!("expected AuthenticationOk"),
        };
        conn.until(vec!["ReadyForQuery"], vec!['C', 'S', 'M'], HashSet::new())?;
        Ok(conn)
    }

    fn send<F: Fn(&mut BytesMut)>(&mut self, f: F) -> anyhow::Result<()> {
        self.send_buf.clear();
        f(&mut self.send_buf);
        self.stream.write_all(&self.send_buf)?;
        Ok(())
    }
    fn until(
        &mut self,
        until: Vec<&str>,
        err_field_typs: Vec<char>,
        ignore: HashSet<String>,
    ) -> anyhow::Result<Vec<String>> {
        let mut msgs = Vec::with_capacity(until.len());
        for expect in until {
            loop {
                let (ch, msg) = match self.recv() {
                    Ok((ch, msg)) => (ch, msg),
                    Err(err) => bail!("{}: waiting for {}, saw {:#?}", err, expect, msgs),
                };
                let (typ, args) = match msg {
                    Message::ReadyForQuery(body) => (
                        "ReadyForQuery",
                        serde_json::to_string(&ReadyForQuery {
                            status: (body.status() as char).to_string(),
                        })?,
                    ),
                    Message::RowDescription(body) => (
                        "RowDescription",
                        serde_json::to_string(&RowDescription {
                            fields: body
                                .fields()
                                .map(|f| {
                                    Ok(Field {
                                        name: f.name().to_string(),
                                    })
                                })
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    Message::DataRow(body) => {
                        let buf = body.buffer();
                        (
                            "DataRow",
                            serde_json::to_string(&DataRow {
                                fields: body
                                    .ranges()
                                    .map(|range| {
                                        match range {
                                            Some(range) => {
                                                // Attempt to convert to a String. If not utf8, print as array of bytes instead.
                                                Ok(String::from_utf8(
                                                    buf[range.start..range.end].to_vec(),
                                                )
                                                .unwrap_or_else(|_| {
                                                    format!(
                                                        "{:?}",
                                                        buf[range.start..range.end].to_vec()
                                                    )
                                                }))
                                            }
                                            None => Ok("NULL".into()),
                                        }
                                    })
                                    .collect()
                                    .unwrap(),
                            })?,
                        )
                    }
                    Message::CommandComplete(body) => (
                        "CommandComplete",
                        serde_json::to_string(&CommandComplete {
                            tag: body.tag().unwrap().to_string(),
                        })?,
                    ),
                    Message::ParseComplete => ("ParseComplete", "".to_string()),
                    Message::BindComplete => ("BindComplete", "".to_string()),
                    Message::PortalSuspended => ("PortalSuspended", "".to_string()),
                    Message::ErrorResponse(body) => (
                        "ErrorResponse",
                        serde_json::to_string(&ErrorResponse {
                            fields: body
                                .fields()
                                .filter_map(|f| {
                                    let typ = f.type_() as char;
                                    if err_field_typs.contains(&typ) {
                                        Ok(Some(ErrorField {
                                            typ,
                                            value: f.value().to_string(),
                                        }))
                                    } else {
                                        Ok(None)
                                    }
                                })
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    Message::NoticeResponse(body) => (
                        "NoticeResponse",
                        serde_json::to_string(&ErrorResponse {
                            fields: body
                                .fields()
                                .filter_map(|f| {
                                    let typ = f.type_() as char;
                                    if err_field_typs.contains(&typ) {
                                        Ok(Some(ErrorField {
                                            typ,
                                            value: f.value().to_string(),
                                        }))
                                    } else {
                                        Ok(None)
                                    }
                                })
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    Message::CopyOutResponse(body) => (
                        "CopyOut",
                        serde_json::to_string(&CopyOut {
                            format: format_name(body.format()),
                            column_formats: body
                                .column_formats()
                                .map(|format| Ok(format_name(format as u8)))
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    Message::CopyInResponse(body) => (
                        "CopyIn",
                        serde_json::to_string(&CopyOut {
                            format: format_name(body.format()),
                            column_formats: body
                                .column_formats()
                                .map(|format| Ok(format_name(format as u8)))
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    Message::CopyData(body) => (
                        "CopyData",
                        serde_json::to_string(
                            &std::str::from_utf8(body.data())
                                .map(|s| s.to_string())
                                .unwrap_or_else(|_| format!("{:?}", body.data())),
                        )?,
                    ),
                    Message::CopyDone => ("CopyDone", "".to_string()),
                    Message::ParameterDescription(body) => (
                        "ParameterDescription",
                        serde_json::to_string(&ParameterDescription {
                            parameters: body.parameters().collect().unwrap(),
                        })?,
                    ),
                    Message::ParameterStatus(_) => continue,
                    Message::NoData => ("NoData", "".to_string()),
                    Message::EmptyQueryResponse => ("EmptyQueryResponse", "".to_string()),
                    _ => ("UNKNOWN", format!("'{}'", ch)),
                };
                if self.verbose {
                    println!("RECV {}: {:?}", ch, typ);
                }
                if ignore.contains(typ) {
                    continue;
                }
                let mut s = typ.to_string();
                if !args.is_empty() {
                    s.push(' ');
                    s.push_str(&args);
                }
                msgs.push(s);
                if expect == typ {
                    break;
                }
            }
        }
        Ok(msgs)
    }
    /// Returns the PostgreSQL message format and the `Message`.
    ///
    /// An error is returned if a new message is not received within the timeout.
    pub fn recv(&mut self) -> anyhow::Result<(char, Message)> {
        let mut buf = [0; 1024];
        let until = Instant::now();
        loop {
            if until.elapsed() > self.timeout {
                bail!("timeout after {:?} waiting for new message", self.timeout);
            }
            let mut ch: char = '0';
            if self.recv_buf.len() > 0 {
                ch = self.recv_buf[0] as char;
            }
            if let Some(msg) = Message::parse(&mut self.recv_buf)? {
                return Ok((ch, msg));
            };
            // If there was no message, read more bytes.
            let sz = self.stream.read(&mut buf)?;
            self.recv_buf.extend_from_slice(&buf[..sz]);
        }
    }
}

const DEFAULT_CONN: &str = "";

pub struct PgTest {
    addr: String,
    user: String,
    timeout: Duration,
    conns: HashMap<String, PgConn>,
    verbose: bool,
}

impl PgTest {
    pub fn new(addr: String, user: String, timeout: Duration) -> anyhow::Result<Self> {
        let verbose = std::env::var_os("PGTEST_VERBOSE").is_some();
        let conn = PgConn::new(&addr, &user, timeout.clone(), verbose)?;
        let mut conns = HashMap::new();
        conns.insert(DEFAULT_CONN.to_string(), conn);

        Ok(PgTest {
            addr,
            user,
            timeout,
            conns,
            verbose,
        })
    }

    fn get_conn(&mut self, name: Option<String>) -> anyhow::Result<&mut PgConn> {
        let name = name.unwrap_or_else(|| DEFAULT_CONN.to_string());
        if !self.conns.contains_key(&name) {
            let conn = PgConn::new(&self.addr, &self.user, self.timeout.clone(), self.verbose)?;
            self.conns.insert(name.clone(), conn);
        }
        Ok(self.conns.get_mut(&name).expect("must exist"))
    }

    pub fn send<F: Fn(&mut BytesMut)>(&mut self, conn: Option<String>, f: F) -> anyhow::Result<()> {
        let conn = self.get_conn(conn)?;
        conn.send(f)
    }

    pub fn until(
        &mut self,
        conn: Option<String>,
        until: Vec<&str>,
        err_field_typs: Vec<char>,
        ignore: HashSet<String>,
    ) -> anyhow::Result<Vec<String>> {
        let conn = self.get_conn(conn)?;
        conn.until(until, err_field_typs, ignore)
    }
}

// Backend messages

#[derive(Serialize)]
pub struct ReadyForQuery {
    pub status: String,
}

#[derive(Serialize)]
pub struct RowDescription {
    pub fields: Vec<Field>,
}

#[derive(Serialize)]
pub struct Field {
    pub name: String,
}

#[derive(Serialize)]
pub struct DataRow {
    pub fields: Vec<String>,
}

#[derive(Serialize)]
pub struct CopyOut {
    pub format: String,
    pub column_formats: Vec<String>,
}

#[derive(Serialize)]
pub struct ParameterDescription {
    parameters: Vec<u32>,
}

#[derive(Serialize)]
pub struct CommandComplete {
    pub tag: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub fields: Vec<ErrorField>,
}

#[derive(Serialize)]
pub struct ErrorField {
    pub typ: char,
    pub value: String,
}

impl Drop for PgTest {
    fn drop(&mut self) {
        for conn in self.conns.values_mut() {
            let _ = conn.send(frontend::terminate);
        }
    }
}

fn format_name(format: u8) -> String {
    match format {
        0 => "text".to_string(),
        1 => "binary".to_string(),
        _ => format!("unknown: {}", format),
    }
}

pub fn walk(addr: String, user: String, timeout: Duration, dir: &str) {
    datadriven::walk(dir, |tf| run_test(tf, addr.clone(), user.clone(), timeout));
}

pub fn run_test(tf: &mut datadriven::TestFile, addr: String, user: String, timeout: Duration) {
    let mut pgt = PgTest::new(addr, user, timeout).unwrap();
    tf.run(|tc| -> String {
        let lines = tc.input.lines();
        let mut args = tc.args.clone();
        let conn: Option<String> = args
            .remove("conn")
            .map(|args| Some(args.into_first()))
            .unwrap_or(None);
        match tc.directive.as_str() {
            "send" => {
                for line in lines {
                    if pgt.verbose {
                        println!("SEND {}", line);
                    }
                    let mut line = line.splitn(2, ' ');
                    let typ = line.next().unwrap_or("");
                    let args = line.next().unwrap_or("{}");
                    pgt.send(conn.clone(), |buf| match typ {
                        "Query" => {
                            let v: Query = serde_json::from_str(args).unwrap();
                            frontend::query(&v.query, buf).unwrap();
                        }
                        "Parse" => {
                            let v: Parse = serde_json::from_str(args).unwrap();
                            frontend::parse(
                                &v.name.unwrap_or_else(|| "".into()),
                                &v.query,
                                vec![],
                                buf,
                            )
                            .unwrap();
                        }
                        "Sync" => frontend::sync(buf),
                        "Bind" => {
                            let v: Bind = serde_json::from_str(args).unwrap();
                            let values = v.values.unwrap_or_default();
                            if frontend::bind(
                                &v.portal.unwrap_or_else(|| "".into()),
                                &v.statement.unwrap_or_else(|| "".into()),
                                vec![], // formats
                                values, // values
                                |t, buf| {
                                    buf.put_slice(t.as_bytes());
                                    Ok(IsNull::No)
                                }, // serializer
                                v.result_formats.unwrap_or_default(),
                                buf,
                            )
                            .is_err()
                            {
                                panic!("bind error");
                            }
                        }
                        "Describe" => {
                            let v: Describe = serde_json::from_str(args).unwrap();
                            frontend::describe(
                                v.variant.unwrap_or_else(|| "S".into()).as_bytes()[0],
                                &v.name.unwrap_or_else(|| "".into()),
                                buf,
                            )
                            .unwrap();
                        }
                        "Execute" => {
                            let v: Execute = serde_json::from_str(args).unwrap();
                            frontend::execute(
                                &v.portal.unwrap_or_else(|| "".into()),
                                v.max_rows.unwrap_or(0),
                                buf,
                            )
                            .unwrap();
                        }
                        "CopyData" => {
                            let v: String = serde_json::from_str(args).unwrap();
                            frontend::CopyData::new(v.as_bytes()).unwrap().write(buf);
                        }
                        "CopyDone" => {
                            frontend::copy_done(buf);
                        }
                        "CopyFail" => {
                            let v: String = serde_json::from_str(args).unwrap();
                            frontend::copy_fail(&v, buf).unwrap();
                        }
                        _ => panic!("unknown message type {}", typ),
                    })
                    .unwrap();
                }
                "".to_string()
            }
            "until" => {
                // Our error field values don't always match postgres. Default to reporting
                // the error code (C) and message (M), but allow the user to specify which ones
                // they want.
                let err_field_typs = if let Some(_) = args.remove("no_error_fields") {
                    vec![]
                } else {
                    match args.remove("err_field_typs") {
                        Some(typs) => typs.join("").chars().collect(),
                        None => vec!['C', 'S', 'M'],
                    }
                };
                let mut ignore = HashSet::new();
                if let Some(values) = args.remove("ignore") {
                    for v in values {
                        ignore.insert(v);
                    }
                }
                if !args.is_empty() {
                    panic!("extra until arguments: {:?}", args);
                }
                format!(
                    "{}\n",
                    pgt.until(conn, lines.collect(), err_field_typs, ignore)
                        .unwrap()
                        .join("\n")
                )
            }
            _ => panic!("unknown directive {}", tc.input),
        }
    })
}

// Frontend messages

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub query: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parse {
    pub name: Option<String>,
    pub query: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bind {
    pub portal: Option<String>,
    pub statement: Option<String>,
    pub values: Option<Vec<String>>,
    pub result_formats: Option<Vec<i16>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execute {
    pub portal: Option<String>,
    pub max_rows: Option<i32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Describe {
    pub variant: Option<String>,
    pub name: Option<String>,
}
