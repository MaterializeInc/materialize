// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Materialize-specific runner for sqllogictest.
//!
//! slt tests expect a serialized execution of sql statements and queries.
//! To get the same results in materialize we track current_timestamp and increment it whenever we execute a statement.
//!
//! The high-level workflow is:
//!   for each record in the test file:
//!     if record is a sql statement:
//!       run sql in postgres, observe changes and copy them to materialize using LocalInput::Updates(..)
//!       advance current_timestamp
//!       promise to never send updates for times < current_timestamp using LocalInput::Watermark(..)
//!       compare to expected results
//!       if wrong, bail out and stop processing this file
//!     if record is a sql query:
//!       peek query at current_timestamp
//!       compare to expected results
//!       if wrong, record the error

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops;
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, bail};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use fallible_iterator::FallibleIterator;
use futures::sink::SinkExt;
use md5::{Digest, Md5};
use mz_persist_client::cache::PersistClientCache;
use once_cell::sync::Lazy;
use postgres_protocol::types;
use regex::Regex;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, Mutex};
use tokio_postgres::types::FromSql;
use tokio_postgres::types::Kind as PgKind;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::{NoTls, Row, SimpleQueryMessage};
use tower_http::cors::AllowOrigin;
use uuid::Uuid;

use mz_controller::ControllerConfig;
use mz_environmentd::SecretsControllerConfig;
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_ore::id_gen::PortAllocator;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task;
use mz_ore::thread::{JoinHandleExt, JoinOnDropHandle};
use mz_persist_client::PersistLocation;
use mz_pgrepr::{Interval, Jsonb, Numeric, Value};
use mz_repr::adt::numeric;
use mz_repr::ColumnName;
use mz_sql::ast::Statement;

use crate::ast::{Location, Mode, Output, QueryOutput, Record, Sort, Type};
use crate::util;

#[derive(Debug)]
pub enum Outcome<'a> {
    Unsupported {
        error: anyhow::Error,
        location: Location,
    },
    ParseFailure {
        error: anyhow::Error,
        location: Location,
    },
    PlanFailure {
        error: anyhow::Error,
        location: Location,
    },
    UnexpectedPlanSuccess {
        expected_error: &'a str,
        location: Location,
    },
    WrongNumberOfRowsInserted {
        expected_count: u64,
        actual_count: u64,
        location: Location,
    },
    WrongColumnCount {
        expected_count: usize,
        actual_count: usize,
        location: Location,
    },
    WrongColumnNames {
        expected_column_names: &'a Vec<ColumnName>,
        actual_column_names: Vec<ColumnName>,
        location: Location,
    },
    OutputFailure {
        expected_output: &'a Output,
        actual_raw_output: Vec<Row>,
        actual_output: Output,
        location: Location,
    },
    Bail {
        cause: Box<Outcome<'a>>,
        location: Location,
    },
    Success,
}

const NUM_OUTCOMES: usize = 10;
const SUCCESS_OUTCOME: usize = NUM_OUTCOMES - 1;

impl<'a> Outcome<'a> {
    fn code(&self) -> usize {
        match self {
            Outcome::Unsupported { .. } => 0,
            Outcome::ParseFailure { .. } => 1,
            Outcome::PlanFailure { .. } => 2,
            Outcome::UnexpectedPlanSuccess { .. } => 3,
            Outcome::WrongNumberOfRowsInserted { .. } => 4,
            Outcome::WrongColumnCount { .. } => 5,
            Outcome::WrongColumnNames { .. } => 6,
            Outcome::OutputFailure { .. } => 7,
            Outcome::Bail { .. } => 8,
            Outcome::Success => 9,
        }
    }

    fn success(&self) -> bool {
        matches!(self, Outcome::Success)
    }
}

impl fmt::Display for Outcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        const INDENT: &str = "\n        ";
        match self {
            Unsupported { error, location } => write!(f, "Unsupported:{}:\n{:#}", location, error),
            ParseFailure { error, location } => {
                write!(f, "ParseFailure:{}:\n{:#}", location, error)
            }
            PlanFailure { error, location } => write!(f, "PlanFailure:{}:\n{:#}", location, error),
            UnexpectedPlanSuccess {
                expected_error,
                location,
            } => write!(
                f,
                "UnexpectedPlanSuccess:{} expected error: {}",
                location, expected_error
            ),
            WrongNumberOfRowsInserted {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongNumberOfRowsInserted:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnCount {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongColumnCount:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnNames {
                expected_column_names,
                actual_column_names,
                location,
            } => write!(
                f,
                "Wrong Column Names:{}:{}expected column names: {}{}inferred column names: {}",
                location,
                INDENT,
                expected_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                actual_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            OutputFailure {
                expected_output,
                actual_raw_output,
                actual_output,
                location,
            } => write!(
                f,
                "OutputFailure:{}{}expected: {:?}{}actually: {:?}{}actual raw: {:?}",
                location, INDENT, expected_output, INDENT, actual_output, INDENT, actual_raw_output
            ),
            Bail { cause, location } => write!(f, "Bail:{} {}", location, cause),
            Success => f.write_str("Success"),
        }
    }
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct Outcomes([usize; NUM_OUTCOMES]);

impl ops::AddAssign<Outcomes> for Outcomes {
    fn add_assign(&mut self, rhs: Outcomes) {
        for (lhs, rhs) in self.0.iter_mut().zip(rhs.0.iter()) {
            *lhs += rhs
        }
    }
}
impl Outcomes {
    pub fn any_failed(&self) -> bool {
        self.0[SUCCESS_OUTCOME] < self.0.iter().sum::<usize>()
    }

    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "unsupported": self.0[0],
            "parse_failure": self.0[1],
            "plan_failure": self.0[2],
            "unexpected_plan_success": self.0[3],
            "wrong_number_of_rows_affected": self.0[4],
            "wrong_column_count": self.0[5],
            "wrong_column_names": self.0[6],
            "output_failure": self.0[7],
            "bail": self.0[8],
            "success": self.0[9],
        })
    }

    pub fn display(&self, no_fail: bool) -> OutcomesDisplay<'_> {
        OutcomesDisplay {
            inner: self,
            no_fail,
        }
    }
}

pub struct OutcomesDisplay<'a> {
    inner: &'a Outcomes,
    no_fail: bool,
}

impl<'a> fmt::Display for OutcomesDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total: usize = self.inner.0.iter().sum();
        write!(
            f,
            "{}:",
            if self.inner.0[SUCCESS_OUTCOME] == total {
                "PASS"
            } else if self.no_fail {
                "FAIL-IGNORE"
            } else {
                "FAIL"
            }
        )?;
        static NAMES: Lazy<Vec<&'static str>> = Lazy::new(|| {
            vec![
                "unsupported",
                "parse-failure",
                "plan-failure",
                "unexpected-plan-success",
                "wrong-number-of-rows-inserted",
                "wrong-column-count",
                "wrong-column-names",
                "output-failure",
                "bail",
                "success",
                "total",
            ]
        });
        for (i, n) in self.inner.0.iter().enumerate() {
            if *n > 0 {
                write!(f, " {}={}", NAMES[i], n)?;
            }
        }
        write!(f, " total={}", total)
    }
}

pub(crate) struct Runner {
    server_addr: SocketAddr,
    // Drop order matters for these fields.
    client: tokio_postgres::Client,
    clients: HashMap<String, tokio_postgres::Client>,
    _shutdown_trigger: oneshot::Sender<()>,
    _server_thread: JoinOnDropHandle<()>,
    _temp_dir: TempDir,
}

#[derive(Debug)]
pub struct Slt(Value);

impl<'a> FromSql<'a> for Slt {
    fn from_sql(
        ty: &PgType,
        mut raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + 'static + Send + Sync>> {
        Ok(match *ty {
            PgType::BOOL => Self(Value::Bool(types::bool_from_sql(raw)?)),
            PgType::BYTEA => Self(Value::Bytea(types::bytea_from_sql(raw).to_vec())),
            PgType::CHAR => Self(Value::Char(u8::from_be_bytes(
                types::char_from_sql(raw)?.to_be_bytes(),
            ))),
            PgType::FLOAT4 => Self(Value::Float4(types::float4_from_sql(raw)?)),
            PgType::FLOAT8 => Self(Value::Float8(types::float8_from_sql(raw)?)),
            PgType::DATE => Self(Value::Date(NaiveDate::from_sql(ty, raw)?)),
            PgType::INT2 => Self(Value::Int2(types::int2_from_sql(raw)?)),
            PgType::INT4 => Self(Value::Int4(types::int4_from_sql(raw)?)),
            PgType::INT8 => Self(Value::Int8(types::int8_from_sql(raw)?)),
            PgType::INTERVAL => Self(Value::Interval(Interval::from_sql(ty, raw)?)),
            PgType::JSONB => Self(Value::Jsonb(Jsonb::from_sql(ty, raw)?)),
            PgType::NUMERIC => Self(Value::Numeric(Numeric::from_sql(ty, raw)?)),
            PgType::OID => Self(Value::Oid(types::oid_from_sql(raw)?)),
            PgType::REGCLASS => Self(Value::Oid(types::oid_from_sql(raw)?)),
            PgType::REGPROC => Self(Value::Oid(types::oid_from_sql(raw)?)),
            PgType::REGTYPE => Self(Value::Oid(types::oid_from_sql(raw)?)),
            PgType::TEXT | PgType::BPCHAR | PgType::VARCHAR => {
                Self(Value::Text(types::text_from_sql(raw)?.to_string()))
            }
            PgType::TIME => Self(Value::Time(NaiveTime::from_sql(ty, raw)?)),
            PgType::TIMESTAMP => Self(Value::Timestamp(NaiveDateTime::from_sql(ty, raw)?)),
            PgType::TIMESTAMPTZ => Self(Value::TimestampTz(DateTime::<Utc>::from_sql(ty, raw)?)),
            PgType::UUID => Self(Value::Uuid(Uuid::from_sql(ty, raw)?)),
            PgType::RECORD => {
                let num_fields = read_be_i32(&mut raw)?;
                let mut tuple = vec![];
                for _ in 0..num_fields {
                    let oid = read_be_i32(&mut raw)? as u32;
                    let typ = match PgType::from_oid(oid) {
                        Some(typ) => typ,
                        None => return Err("unknown oid".into()),
                    };
                    let v = read_value::<Option<Slt>>(&typ, &mut raw)?;
                    tuple.push(v.map(|v| v.0));
                }
                Self(Value::Record(tuple))
            }

            _ => match ty.kind() {
                PgKind::Array(arr_type) => {
                    let arr = types::array_from_sql(raw)?;
                    let elements: Vec<Option<Value>> = arr
                        .values()
                        .map(|v| match v {
                            Some(v) => Ok(Some(Slt::from_sql(arr_type, v)?)),
                            None => Ok(None),
                        })
                        .collect::<Vec<Option<Slt>>>()?
                        .into_iter()
                        // Map a Vec<Option<Slt>> to Vec<Option<Value>>.
                        .map(|v| v.map(|v| v.0))
                        .collect();
                    Self(Value::Array {
                        dims: arr
                            .dimensions()
                            .map(|d| {
                                Ok(mz_repr::adt::array::ArrayDimension {
                                    lower_bound: d.lower_bound as usize,
                                    length: d.len as usize,
                                })
                            })
                            .collect()?,
                        elements,
                    })
                }
                _ => unreachable!(),
            },
        })
    }
    fn accepts(ty: &PgType) -> bool {
        match ty.kind() {
            PgKind::Array(_) | PgKind::Composite(_) => return true,
            _ => {}
        }
        matches!(
            *ty,
            PgType::BOOL
                | PgType::BYTEA
                | PgType::CHAR
                | PgType::DATE
                | PgType::FLOAT4
                | PgType::FLOAT8
                | PgType::INT2
                | PgType::INT4
                | PgType::INT8
                | PgType::INTERVAL
                | PgType::JSONB
                | PgType::NUMERIC
                | PgType::OID
                | PgType::REGCLASS
                | PgType::REGPROC
                | PgType::REGTYPE
                | PgType::RECORD
                | PgType::TEXT
                | PgType::BPCHAR
                | PgType::VARCHAR
                | PgType::TIME
                | PgType::TIMESTAMP
                | PgType::TIMESTAMPTZ
                | PgType::UUID
        )
    }
}

// From postgres-types/src/private.rs.
fn read_be_i32(buf: &mut &[u8]) -> Result<i32, Box<dyn Error + Sync + Send>> {
    if buf.len() < 4 {
        return Err("invalid buffer size".into());
    }
    let mut bytes = [0; 4];
    bytes.copy_from_slice(&buf[..4]);
    *buf = &buf[4..];
    Ok(i32::from_be_bytes(bytes))
}

// From postgres-types/src/private.rs.
fn read_value<'a, T>(type_: &PgType, buf: &mut &'a [u8]) -> Result<T, Box<dyn Error + Sync + Send>>
where
    T: FromSql<'a>,
{
    let len = read_be_i32(buf)?;
    let value = if len < 0 {
        None
    } else {
        if len as usize > buf.len() {
            return Err("invalid buffer size".into());
        }
        let (head, tail) = buf.split_at(len as usize);
        *buf = tail;
        Some(head)
    };
    T::from_sql_nullable(type_, value)
}

fn format_datum(d: Slt, typ: &Type, mode: Mode, col: usize) -> String {
    match (typ, d.0) {
        (Type::Bool, Value::Bool(b)) => b.to_string(),

        (Type::Integer, Value::Int2(i)) => i.to_string(),
        (Type::Integer, Value::Int4(i)) => i.to_string(),
        (Type::Integer, Value::Int8(i)) => i.to_string(),
        (Type::Integer, Value::Oid(i)) => i.to_string(),
        (Type::Integer, Value::Float4(f)) => format!("{}", f as i64),
        (Type::Integer, Value::Float8(f)) => format!("{}", f as i64),
        // This is so wrong, but sqlite needs it.
        (Type::Integer, Value::Text(_)) => "0".to_string(),
        (Type::Integer, Value::Bool(b)) => i8::from(b).to_string(),
        (Type::Integer, Value::Numeric(d)) => {
            let mut d = d.0 .0.clone();
            let mut cx = numeric::cx_datum();
            cx.round(&mut d);
            numeric::munge_numeric(&mut d).unwrap();
            d.to_standard_notation_string()
        }

        (Type::Real, Value::Int2(i)) => format!("{:.3}", i),
        (Type::Real, Value::Int4(i)) => format!("{:.3}", i),
        (Type::Real, Value::Int8(i)) => format!("{:.3}", i),
        (Type::Real, Value::Float4(f)) => match mode {
            Mode::Standard => format!("{:.3}", f),
            Mode::Cockroach => format!("{}", f),
        },
        (Type::Real, Value::Float8(f)) => match mode {
            Mode::Standard => format!("{:.3}", f),
            Mode::Cockroach => format!("{}", f),
        },
        (Type::Real, Value::Numeric(d)) => match mode {
            Mode::Standard => {
                let mut d = d.0 .0.clone();
                if d.exponent() < -3 {
                    numeric::rescale(&mut d, 3).unwrap();
                }
                numeric::munge_numeric(&mut d).unwrap();
                d.to_standard_notation_string()
            }
            Mode::Cockroach => d.0 .0.to_standard_notation_string(),
        },

        (Type::Text, Value::Text(s)) => {
            if s.is_empty() {
                "(empty)".to_string()
            } else {
                s
            }
        }
        (Type::Text, Value::Bool(b)) => b.to_string(),
        (Type::Text, Value::Float4(f)) => format!("{:.3}", f),
        (Type::Text, Value::Float8(f)) => format!("{:.3}", f),
        // Bytes are printed as text iff they are valid UTF-8. This
        // seems guaranteed to confuse everyone, but it is required for
        // compliance with the CockroachDB sqllogictest runner. [0]
        //
        // [0]: https://github.com/cockroachdb/cockroach/blob/970782487/pkg/sql/logictest/logic.go#L2038-L2043
        (Type::Text, Value::Bytea(b)) => match str::from_utf8(&b) {
            Ok(s) => s.to_string(),
            Err(_) => format!("{:?}", b),
        },
        (Type::Text, Value::Numeric(d)) => d.0 .0.to_standard_notation_string(),
        // Everything else gets normal text encoding. This correctly handles things
        // like arrays, tuples, and strings that need to be quoted.
        (Type::Text, d) => {
            let mut buf = BytesMut::new();
            d.encode_text(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        }

        (Type::Oid, Value::Oid(o)) => o.to_string(),

        (_, d) => panic!(
            "Don't know how to format {:?} as {:?} in column {}",
            d, typ, col,
        ),
    }
}

fn format_row(row: &Row, types: &[Type], mode: Mode, sort: &Sort) -> Vec<String> {
    let mut formatted: Vec<String> = vec![];
    for i in 0..row.len() {
        let t: Option<Slt> = row.get::<usize, Option<Slt>>(i);
        let t: Option<String> = t.map(|d| format_datum(d, &types[i], mode, i));
        formatted.push(match t {
            Some(t) => t,
            None => "NULL".into(),
        });
    }
    if mode == Mode::Cockroach && sort.yes() {
        formatted
            .iter()
            .flat_map(|s| {
                crate::parser::split_cols(&s, types.len())
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        formatted
    }
}

impl Runner {
    pub async fn start(config: &RunConfig<'_>) -> Result<Self, anyhow::Error> {
        let temp_dir = tempfile::tempdir()?;
        let (consensus_uri, catalog_postgres_stash, storage_postgres_stash) = {
            let postgres_url = &config.postgres_url;
            let (client, conn) = tokio_postgres::connect(&postgres_url, NoTls).await?;
            task::spawn(|| "sqllogictest_connect", async move {
                if let Err(e) = conn.await {
                    panic!("connection error: {}", e);
                }
            });
            client
                .batch_execute(&format!(
                    "DROP SCHEMA IF EXISTS sqllogictest_consensus CASCADE;
                     DROP SCHEMA IF EXISTS sqllogictest_catalog CASCADE;
                     DROP SCHEMA IF EXISTS sqllogictest_storage CASCADE;
                     CREATE SCHEMA sqllogictest_consensus;
                     CREATE SCHEMA sqllogictest_catalog;
                     CREATE SCHEMA sqllogictest_storage;",
                ))
                .await?;
            (
                format!("{postgres_url}?options=--search_path=sqllogictest_consensus"),
                format!("{postgres_url}?options=--search_path=sqllogictest_catalog"),
                format!("{postgres_url}?options=--search_path=sqllogictest_storage"),
            )
        };
        let orchestrator = ProcessOrchestrator::new(ProcessOrchestratorConfig {
            image_dir: env::current_exe()?.parent().unwrap().to_path_buf(),
            port_allocator: Arc::new(PortAllocator::new(2100, 2200)),
            suppress_output: false,
            data_dir: temp_dir.path().to_path_buf(),
            command_wrapper: vec![],
        })
        .await?;
        let metrics_registry = MetricsRegistry::new();
        let persist_clients = PersistClientCache::new(&metrics_registry);
        let persist_clients = Arc::new(Mutex::new(persist_clients));
        let server_config = mz_environmentd::Config {
            catalog_postgres_stash,
            controller: ControllerConfig {
                build_info: &mz_environmentd::BUILD_INFO,
                orchestrator: Arc::new(orchestrator),
                storaged_image: "storaged".into(),
                computed_image: "computed".into(),
                persist_location: PersistLocation {
                    blob_uri: format!("file://{}/persist/blob", temp_dir.path().display()),
                    consensus_uri,
                },
                persist_clients,
                storage_stash_url: storage_postgres_stash,
            },
            secrets_controller: SecretsControllerConfig::LocalFileSystem(
                temp_dir.path().to_path_buf().join("secrets"),
            ),
            // Setting the port to 0 means that the OS will automatically
            // allocate an available port.
            sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            internal_sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            internal_http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            tls: None,
            frontegg: None,
            cors_allowed_origin: AllowOrigin::list([]),
            unsafe_mode: true,
            metrics_registry,
            now: SYSTEM_TIME.clone(),
            replica_sizes: Default::default(),
            availability_zones: Default::default(),
            connection_context: Default::default(),
            otel_enable_callback: mz_ore::tracing::OpenTelemetryEnableCallback::none(),
        };
        // We need to run the server on its own Tokio runtime, which in turn
        // requires its own thread, so that we can wait for any tasks spawned
        // by the server to be shutdown at the end of each file. If we were to
        // share a Tokio runtime, tasks from the last file's server would still
        // be live at the start of the next file's server.
        let (server_addr_tx, server_addr_rx) = oneshot::channel();
        let (shutdown_trigger, shutdown_tripwire) = oneshot::channel();
        let server_thread = thread::spawn(|| {
            let runtime = match Runtime::new() {
                Ok(runtime) => runtime,
                Err(e) => {
                    server_addr_tx
                        .send(Err(e.into()))
                        .expect("receiver should not drop first");
                    return;
                }
            };
            let server = match runtime.block_on(mz_environmentd::serve(server_config)) {
                Ok(runtime) => runtime,
                Err(e) => {
                    server_addr_tx
                        .send(Err(e))
                        .expect("receiver should not drop first");
                    return;
                }
            };
            server_addr_tx
                .send(Ok(server.sql_local_addr()))
                .expect("receiver should not drop first");
            let _ = runtime.block_on(shutdown_tripwire);
        });
        let server_addr = server_addr_rx.await??;
        let client = connect(server_addr).await;

        Ok(Runner {
            server_addr,
            _shutdown_trigger: shutdown_trigger,
            _server_thread: server_thread.join_on_drop(),
            _temp_dir: temp_dir,
            client,
            clients: HashMap::new(),
        })
    }

    async fn run_record<'a>(
        &mut self,
        record: &'a Record<'a>,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        match &record {
            Record::Statement {
                expected_error,
                rows_affected,
                sql,
                location,
            } => {
                match self
                    .run_statement(*expected_error, *rows_affected, sql, location.clone())
                    .await?
                {
                    Outcome::Success => Ok(Outcome::Success),
                    other => {
                        if expected_error.is_some() {
                            Ok(other)
                        } else {
                            // If we failed to execute a statement that was supposed to succeed,
                            // running the rest of the tests in this file will probably cause
                            // false positives, so just give up on the file entirely.
                            Ok(Outcome::Bail {
                                cause: Box::new(other),
                                location: location.clone(),
                            })
                        }
                    }
                }
            }
            Record::Query {
                sql,
                output,
                location,
            } => self.run_query(sql, output, location.clone()).await,
            Record::Simple {
                conn,
                sql,
                output,
                location,
                ..
            } => self.run_simple(*conn, sql, output, location.clone()).await,
            Record::Copy {
                table_name,
                tsv_path,
            } => {
                let tsv = tokio::fs::read(tsv_path).await?;
                let copy = self
                    .client
                    .copy_in(&*format!("COPY {} FROM STDIN", table_name))
                    .await?;
                tokio::pin!(copy);
                copy.send(bytes::Bytes::from(tsv)).await?;
                copy.finish().await?;
                Ok(Outcome::Success)
            }
            _ => Ok(Outcome::Success),
        }
    }

    async fn run_statement<'a>(
        &mut self,
        expected_error: Option<&'a str>,
        expected_rows_affected: Option<u64>,
        sql: &'a str,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        static UNSUPPORTED_INDEX_STATEMENT_REGEX: Lazy<Regex> =
            Lazy::new(|| Regex::new("^(CREATE UNIQUE INDEX|REINDEX)").unwrap());
        if UNSUPPORTED_INDEX_STATEMENT_REGEX.is_match(sql) {
            // sure, we totally made you an index
            return Ok(Outcome::Success);
        }

        match self.client.execute(sql, &[]).await {
            Ok(actual) => {
                if let Some(expected_error) = expected_error {
                    return Ok(Outcome::UnexpectedPlanSuccess {
                        expected_error,
                        location,
                    });
                }
                match expected_rows_affected {
                    None => Ok(Outcome::Success),
                    Some(expected) => {
                        if expected != actual {
                            Ok(Outcome::WrongNumberOfRowsInserted {
                                expected_count: expected,
                                actual_count: actual,
                                location,
                            })
                        } else {
                            Ok(Outcome::Success)
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(expected_error) = expected_error {
                    if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::PlanFailure {
                    error: anyhow!(error),
                    location,
                })
            }
        }
    }

    async fn run_query<'a>(
        &mut self,
        sql: &'a str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        // get statement
        let statements = match mz_sql::parse::parse(sql) {
            Ok(statements) => statements,
            Err(e) => match output {
                Ok(_) => {
                    return Ok(Outcome::ParseFailure {
                        error: e.into(),
                        location,
                    });
                }
                Err(expected_error) => {
                    if Regex::new(expected_error)?.is_match(&format!("{:#}", e)) {
                        return Ok(Outcome::Success);
                    } else {
                        return Ok(Outcome::ParseFailure {
                            error: e.into(),
                            location,
                        });
                    }
                }
            },
        };
        let statement = match &*statements {
            [] => bail!("Got zero statements?"),
            [statement] => statement,
            _ => bail!("Got multiple statements: {:?}", statements),
        };
        match statement {
            Statement::CreateView { .. }
            | Statement::Select { .. }
            | Statement::ShowIndexes { .. } => (),
            _ => {
                if output.is_err() {
                    // We're not interested in testing our hacky handling of INSERT etc
                    return Ok(Outcome::Success);
                }
            }
        }

        let rows = match self.client.query(sql, &[]).await {
            Ok(rows) => rows,
            Err(error) => {
                return match output {
                    Ok(_) => {
                        let error_string = format!("{}", error);
                        if error_string.contains("supported") || error_string.contains("overload") {
                            // this is a failure, but it's caused by lack of support rather than by bugs
                            Ok(Outcome::Unsupported {
                                error: anyhow!(error),
                                location,
                            })
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                    Err(expected_error) => {
                        if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                            Ok(Outcome::Success)
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                };
            }
        };

        // unpack expected output
        let QueryOutput {
            sort,
            types: expected_types,
            column_names: expected_column_names,
            output: expected_output,
            mode,
            ..
        } = match output {
            Err(expected_error) => {
                return Ok(Outcome::UnexpectedPlanSuccess {
                    expected_error,
                    location,
                });
            }
            Ok(query_output) => query_output,
        };

        // Various checks as long as there are returned rows.
        if let Some(row) = rows.get(0) {
            // check column names
            if let Some(expected_column_names) = expected_column_names {
                let actual_column_names = row
                    .columns()
                    .iter()
                    .map(|t| ColumnName::from(t.name()))
                    .collect::<Vec<_>>();
                if expected_column_names != &actual_column_names {
                    return Ok(Outcome::WrongColumnNames {
                        expected_column_names,
                        actual_column_names,
                        location,
                    });
                }
            }
        }

        // format output
        let mut formatted_rows = vec![];
        for row in &rows {
            if row.len() != expected_types.len() {
                return Ok(Outcome::WrongColumnCount {
                    expected_count: expected_types.len(),
                    actual_count: row.len(),
                    location,
                });
            }
            let row = format_row(row, &expected_types, *mode, sort);
            formatted_rows.push(row);
        }

        // sort formatted output
        if let Sort::Row = sort {
            formatted_rows.sort();
        }
        let mut values = formatted_rows.into_iter().flatten().collect::<Vec<_>>();
        if let Sort::Value = sort {
            values.sort();
        }

        // check output
        match expected_output {
            Output::Values(expected_values) => {
                if values != *expected_values {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Values(values),
                        location,
                    });
                }
            }
            Output::Hashed {
                num_values,
                md5: expected_md5,
            } => {
                let mut hasher = Md5::new();
                for value in &values {
                    hasher.update(value);
                    hasher.update("\n");
                }
                let md5 = format!("{:x}", hasher.finalize());
                if values.len() != *num_values || md5 != *expected_md5 {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Hashed {
                            num_values: values.len(),
                            md5,
                        },
                        location,
                    });
                }
            }
        }

        Ok(Outcome::Success)
    }

    async fn get_conn(&mut self, name: Option<&str>) -> &tokio_postgres::Client {
        match name {
            None => &self.client,
            Some(name) => {
                if !self.clients.contains_key(name) {
                    let client = connect(self.server_addr).await;
                    self.clients.insert(name.into(), client);
                }
                self.clients.get(name).unwrap()
            }
        }
    }

    async fn run_simple<'a>(
        &mut self,
        conn: Option<&'a str>,
        sql: &'a str,
        output: &'a Output,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        let client = self.get_conn(conn).await;
        let actual = Output::Values(match client.simple_query(sql).await {
            Ok(result) => result
                .into_iter()
                .map(|m| match m {
                    SimpleQueryMessage::Row(row) => {
                        let mut s = vec![];
                        for i in 0..row.len() {
                            s.push(row.get(i).unwrap_or("NULL"));
                        }
                        s.join(",")
                    }
                    SimpleQueryMessage::CommandComplete(count) => format!("COMPLETE {}", count),
                    _ => panic!("unexpected"),
                })
                .collect::<Vec<_>>(),
            // Errors can contain multiple lines (say if there are details), and rewrite
            // sticks them each on their own line, so we need to split up the lines here to
            // each be its own String in the Vec.
            Err(error) => error.to_string().lines().map(|s| s.to_string()).collect(),
        });
        if *output != actual {
            Ok(Outcome::OutputFailure {
                expected_output: output,
                actual_raw_output: vec![],
                actual_output: actual,
                location,
            })
        } else {
            Ok(Outcome::Success)
        }
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=materialize", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    .unwrap();

    task::spawn(|| "sqllogictest_connect", async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
}

pub trait WriteFmt {
    fn write_fmt(&self, fmt: fmt::Arguments<'_>);
}

pub struct RunConfig<'a> {
    pub stdout: &'a dyn WriteFmt,
    pub stderr: &'a dyn WriteFmt,
    pub verbosity: usize,
    pub postgres_url: String,
    pub no_fail: bool,
    pub fail_fast: bool,
}

fn print_record(config: &RunConfig<'_>, record: &Record) {
    match record {
        Record::Statement { sql, .. } | Record::Query { sql, .. } => {
            writeln!(config.stdout, "{}", crate::util::indent(sql, 4))
        }
        _ => (),
    }
}

pub async fn run_string(
    config: &RunConfig<'_>,
    source: &str,
    input: &str,
) -> Result<Outcomes, anyhow::Error> {
    let mut outcomes = Outcomes::default();
    let mut state = Runner::start(config).await.unwrap();
    let mut parser = crate::parser::Parser::new(source, input);
    writeln!(config.stdout, "==> {}", source);
    for record in parser.parse_records()? {
        // In maximal-verbosity mode, print the query before attempting to run
        // it. Running the query might panic, so it is important to print out
        // what query we are trying to run *before* we panic.
        if config.verbosity >= 2 {
            print_record(config, &record);
        }

        let outcome = state
            .run_record(&record)
            .await
            .map_err(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // Print failures in verbose mode.
        if config.verbosity >= 1 && !outcome.success() {
            if config.verbosity < 2 {
                // If `verbosity >= 2`, we'll already have printed the record,
                // so don't print it again. Yes, this is an ugly bit of logic.
                // Please don't try to consolidate it with the `print_record`
                // call above, as it's important to have a mode in which records
                // are printed before they are run, so that if running the
                // record panics, you can tell which record caused it.
                print_record(config, &record);
            }
            writeln!(config.stdout, "{}", util::indent(&outcome.to_string(), 4));
            writeln!(config.stdout, "{}", util::indent("----", 4));
        }

        outcomes.0[outcome.code()] += 1;

        if let Outcome::Bail { .. } = outcome {
            break;
        }

        if config.fail_fast && !outcome.success() {
            break;
        }
    }
    Ok(outcomes)
}

pub async fn run_file(config: &RunConfig<'_>, filename: &Path) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    File::open(filename)?.read_to_string(&mut input)?;
    run_string(config, &format!("{}", filename.display()), &input).await
}

pub async fn rewrite_file(config: &RunConfig<'_>, filename: &Path) -> Result<(), anyhow::Error> {
    let mut file = OpenOptions::new().read(true).write(true).open(filename)?;

    let mut input = String::new();
    file.read_to_string(&mut input)?;

    let mut buf = RewriteBuffer::new(&input);

    let mut state = Runner::start(config).await?;
    let mut parser = crate::parser::Parser::new(filename.to_str().unwrap_or(""), &input);
    writeln!(config.stdout, "==> {}", filename.display());
    for record in parser.parse_records()? {
        let record = record;
        let outcome = state.run_record(&record).await?;

        // If we see an output failure for a query, rewrite the expected output
        // to match the observed output.
        if let (
            Record::Query {
                output:
                    Ok(QueryOutput {
                        mode,
                        output: Output::Values(_),
                        output_str: expected_output,
                        types,
                        ..
                    }),
                ..
            },
            Outcome::OutputFailure {
                actual_output: Output::Values(actual_output),
                ..
            },
        ) = (&record, &outcome)
        {
            buf.append_header(&input, expected_output);

            for (i, row) in actual_output.chunks(types.len()).enumerate() {
                match mode {
                    // In Cockroach mode, output each row on its own line, with
                    // two spaces between each column.
                    Mode::Cockroach => {
                        if i != 0 {
                            buf.append("\n");
                        }
                        buf.append(&row.join("  "));
                    }
                    // In standard mode, output each value on its own line,
                    // and ignore row boundaries.
                    Mode::Standard => {
                        for (j, col) in row.iter().enumerate() {
                            if i != 0 || j != 0 {
                                buf.append("\n");
                            }
                            buf.append(col);
                        }
                    }
                }
            }
        } else if let (
            Record::Query {
                output:
                    Ok(QueryOutput {
                        output: Output::Hashed { .. },
                        output_str: expected_output,
                        ..
                    }),
                ..
            },
            Outcome::OutputFailure {
                actual_output: Output::Hashed { num_values, md5 },
                ..
            },
        ) = (&record, &outcome)
        {
            buf.append_header(&input, expected_output);

            buf.append(format!("{} values hashing to {}\n", num_values, md5).as_str())
        } else if let (
            Record::Simple {
                output_str: expected_output,
                ..
            },
            Outcome::OutputFailure {
                actual_output: Output::Values(actual_output),
                ..
            },
        ) = (&record, &outcome)
        {
            buf.append_header(&input, expected_output);

            for (i, row) in actual_output.iter().enumerate() {
                if i != 0 {
                    buf.append("\n");
                }
                buf.append(row);
            }
        } else if let Outcome::Success = outcome {
            // Ok.
        } else {
            bail!("unexpected: {:?} {:?}", record, outcome);
        }
    }

    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(buf.finish().as_bytes())?;
    file.sync_all()?;
    Ok(())
}

struct RewriteBuffer<'a> {
    input: &'a str,
    input_offset: usize,
    output: String,
}

impl<'a> RewriteBuffer<'a> {
    fn new(input: &'a str) -> RewriteBuffer<'a> {
        RewriteBuffer {
            input,
            input_offset: 0,
            output: String::new(),
        }
    }

    fn flush_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        let chunk = &self.input[self.input_offset..offset];
        self.output.push_str(chunk);
        self.input_offset = offset;
    }

    fn skip_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        self.input_offset = offset;
    }

    fn append(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn append_header(&mut self, input: &String, expected_output: &str) {
        // Output everything before this record.
        let offset = expected_output.as_ptr() as usize - input.as_ptr() as usize;
        self.flush_to(offset);
        self.skip_to(offset + expected_output.len());

        // Attempt to install the result separator (----), if it does
        // not already exist.
        if self.peek_last(5) == "\n----" {
            self.append("\n");
        } else if self.peek_last(6) != "\n----\n" {
            self.append("\n----\n");
        }
    }

    fn peek_last(&self, n: usize) -> &str {
        &self.output[self.output.len() - n..]
    }

    fn finish(mut self) -> String {
        self.flush_to(self.input.len());
        self.output
    }
}
