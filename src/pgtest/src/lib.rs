// Copyright Materialize, Inc. All rights reserved.
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
//! The following datadriven directives are supported:
//! - `send`: Sends input messages to the server. Arguments, if needed,
//! are specified using JSON. Refer to the associated types to see
//! supported arguments. Arguments can be omitted to use defaults.
//! - `until`: Waits until input messages have been received from the
//! server. Additional messages are accumulated and returned as well.
//!
//! Supported `send` types:
//! - [`Query`](struct.Query.html)
//! - [`Parse`](struct.Parse.html)
//! - [`Bind`](struct.Bind.html)
//! - [`Execute`](struct.Execute.html)
//! - `Sync`
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

use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::IsNull;
use serde::{Deserialize, Serialize};

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub struct PGTest {
    stream: TcpStream,
    recv_buf: BytesMut,
    send_buf: BytesMut,
    timeout: Duration,
}

impl PGTest {
    pub fn new(addr: &str, user: &str, timeout: Duration) -> Result<Self> {
        let mut pgtest = PGTest {
            stream: TcpStream::connect(addr)?,
            recv_buf: BytesMut::new(),
            send_buf: BytesMut::new(),
            timeout,
        };
        pgtest.send(|buf| frontend::startup_message(vec![("user", user)], buf).unwrap())?;
        match pgtest.recv()?.1 {
            Message::AuthenticationOk => {}
            _ => return Err(anyhow!("expected AuthenticationOk")),
        };
        pgtest.until(vec!["ReadyForQuery"])?;
        Ok(pgtest)
    }
    pub fn send<F: Fn(&mut BytesMut)>(&mut self, f: F) -> Result<()> {
        self.send_buf.clear();
        f(&mut self.send_buf);
        self.stream.write_all(&self.send_buf)?;
        Ok(())
    }
    pub fn until(&mut self, until: Vec<&str>) -> Result<Vec<String>> {
        let mut msgs = Vec::with_capacity(until.len());
        for expect in until {
            loop {
                let (ch, msg) = match self.recv() {
                    Ok((ch, msg)) => (ch, msg),
                    Err(err) => {
                        return Err(anyhow!("{}: waiting for {}, saw {:?}", err, expect, msgs))
                    }
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
                                        let range = range.unwrap();
                                        // TODO(mjibson): support not strings.
                                        Ok(String::from_utf8(buf[range.start..range.end].to_vec())
                                            .unwrap())
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
                                .map(|f| {
                                    Ok(ErrorField {
                                        typ: f.type_(),
                                        value: f.value().to_string(),
                                    })
                                })
                                .collect()
                                .unwrap(),
                        })?,
                    ),
                    _ => ("UNKNOWN", format!("'{}'", ch)),
                };
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
    // recv returns the Postgres message format and the Message. An error is
    // returned if a new message is not received within the timeout.
    fn recv(&mut self) -> Result<(char, Message)> {
        let mut buf = [0; 1024];
        let until = Instant::now();
        loop {
            if until.elapsed() > self.timeout {
                return Err(anyhow!(
                    "timeout after {:?} waiting for new message",
                    self.timeout
                ));
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
pub struct CommandComplete {
    pub tag: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub fields: Vec<ErrorField>,
}

#[derive(Serialize)]
pub struct ErrorField {
    pub typ: u8,
    pub value: String,
}

impl Drop for PGTest {
    fn drop(&mut self) {
        let _ = self.send(|buf| frontend::terminate(buf));
    }
}

pub fn walk(addr: &str, user: &str, timeout: Duration, dir: &str) {
    datadriven::walk(dir, |tf| {
        let mut pgt = PGTest::new(addr, user, timeout).unwrap();
        tf.run(|tc| -> String {
            let lines = tc.input.trim().split('\n');
            match tc.directive.as_str() {
                "send" => {
                    for line in lines {
                        let mut line = line.splitn(2, ' ');
                        let typ = line.next().unwrap_or("");
                        let args = line.next().unwrap_or("{}");
                        pgt.send(|buf| match typ {
                            "Query" => {
                                let v: Query = serde_json::from_str(args).unwrap();
                                frontend::query(&v.query, buf).unwrap();
                            }
                            "ReadyForQuery" => {
                                let v: Query = serde_json::from_str(args).unwrap();
                                frontend::query(&v.query, buf).unwrap();
                            }
                            "Parse" => {
                                let v: Parse = serde_json::from_str(args).unwrap();
                                frontend::parse("", &v.query, vec![], buf).unwrap();
                            }
                            "Sync" => frontend::sync(buf),
                            "Bind" => {
                                let v: Bind = serde_json::from_str(args).unwrap();
                                let values = v.values.unwrap_or_default();
                                if frontend::bind(
                                    "",     // portal
                                    "",     // statement
                                    vec![], // formats
                                    values, // values
                                    |t, buf| {
                                        buf.put_slice(t.as_bytes());
                                        Ok(IsNull::No)
                                    }, // serializer
                                    vec![], // result_formats
                                    buf,
                                )
                                .is_err()
                                {
                                    panic!("bind error");
                                }
                            }
                            "Execute" => {
                                let v: Execute = serde_json::from_str(args).unwrap();
                                frontend::execute("", v.max_rows.unwrap_or(0), buf).unwrap();
                            }
                            _ => panic!("unknown message type {}", typ),
                        })
                        .unwrap();
                    }
                    "".to_string()
                }
                "until" => format!("{}\n", pgt.until(lines.collect()).unwrap().join("\n")),
                _ => panic!("unknown directive {}", tc.input),
            }
        })
    });
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
    pub query: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bind {
    pub values: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execute {
    pub max_rows: Option<i32>,
}
