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

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fmt, ops, str, thread};

use anyhow::{anyhow, bail};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDateTime, NaiveTime, Utc};
use fallible_iterator::FallibleIterator;
use futures::sink::SinkExt;
use md5::{Digest, Md5};
use mz_controller::ControllerConfig;
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_ore::cast::{CastFrom, ReinterpretCast};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_ore::thread::{JoinHandleExt, JoinOnDropHandle};
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::PersistLocation;
use mz_pgrepr::{oid, Interval, Jsonb, Numeric, UInt2, UInt4, UInt8, Value};
use mz_repr::adt::date::Date;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::adt::numeric;
use mz_repr::ColumnName;
use mz_secrets::SecretsController;
use mz_sql::ast::{Expr, Raw, Statement};
use mz_sql::catalog::EnvironmentId;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateIndexStatement, CreateViewStatement, CteBlock, DropObjectsStatement, Ident,
    IfExistsBehavior, ObjectType, OrderByExpr, Query, RawItemName, Select, SelectItem,
    SelectStatement, SetExpr, Statement as AstStatement, TableFactor, TableWithJoins,
    UnresolvedItemName, UnresolvedObjectName, ViewDefinition,
};
use mz_sql_parser::parser;
use mz_stash::StashFactory;
use mz_storage_client::types::connections::ConnectionContext;
use once_cell::sync::Lazy;
use postgres_protocol::types;
use regex::Regex;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_postgres::types::{FromSql, Kind as PgKind, Type as PgType};
use tokio_postgres::{NoTls, Row, SimpleQueryMessage};
use tower_http::cors::AllowOrigin;
use tracing::{error, info};
use uuid::fmt::Simple;
use uuid::Uuid;

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
    InconsistentViewOutcome {
        query_outcome: Box<Outcome<'a>>,
        view_outcome: Box<Outcome<'a>>,
        location: Location,
    },
    Bail {
        cause: Box<Outcome<'a>>,
        location: Location,
    },
    Success,
}

const NUM_OUTCOMES: usize = 11;
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
            Outcome::InconsistentViewOutcome { .. } => 9,
            Outcome::Success => 10,
        }
    }

    fn success(&self) -> bool {
        matches!(self, Outcome::Success)
    }

    /// Returns an error message that will match self. Appropriate for
    /// rewriting error messages (i.e. not inserting error messages where we
    /// currently expect success).
    fn err_msg(&self) -> Option<String> {
        match self {
            Outcome::Unsupported { error, .. }
            | Outcome::ParseFailure { error, .. }
            | Outcome::PlanFailure { error, .. } => Some(
                // This value gets fed back into regex to check that it matches
                // `self`, so escape its meta characters.
                regex::escape(
                    // Take only first string in error message, which should be
                    // sufficient for meaningfully matching error.
                    error.to_string().split('\n').next().unwrap(),
                ),
            ),
            _ => None,
        }
    }
}

impl fmt::Display for Outcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        const INDENT: &str = "\n        ";
        match self {
            Unsupported { error, location } => write!(
                f,
                "Unsupported:{}:\n{}",
                location,
                error.display_with_causes()
            ),
            ParseFailure { error, location } => {
                write!(
                    f,
                    "ParseFailure:{}:\n{}",
                    location,
                    error.display_with_causes()
                )
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
            InconsistentViewOutcome {
                query_outcome,
                view_outcome,
                location,
            } => write!(
                f,
                "InconsistentViewOutcome:{}{}expected from query: {:?}{}actually from indexed view: {:?}{}",
                location, INDENT, query_outcome, INDENT, view_outcome, INDENT
            ),
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
            "inconsistent_view_outcome": self.0[9],
            "success": self.0[10],
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
                "inconsistent-view-outcome",
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

struct QueryInfo {
    is_select: bool,
    num_attributes: Option<usize>,
}

enum PrepareQueryOutcome<'a> {
    QueryPrepared(QueryInfo),
    Outcome(Outcome<'a>),
}

pub struct Runner<'a> {
    config: &'a RunConfig<'a>,
    inner: Option<RunnerInner>,
}

pub struct RunnerInner {
    server_addr: SocketAddr,
    internal_server_addr: SocketAddr,
    // Drop order matters for these fields.
    client: tokio_postgres::Client,
    system_client: tokio_postgres::Client,
    clients: BTreeMap<String, tokio_postgres::Client>,
    auto_index_tables: bool,
    auto_index_selects: bool,
    auto_transactions: bool,
    enable_table_keys: bool,
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
            PgType::DATE => Self(Value::Date(Date::from_pg_epoch(types::int4_from_sql(
                raw,
            )?)?)),
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
            PgType::TIMESTAMP => Self(Value::Timestamp(
                NaiveDateTime::from_sql(ty, raw)?.try_into()?,
            )),
            PgType::TIMESTAMPTZ => Self(Value::TimestampTz(
                DateTime::<Utc>::from_sql(ty, raw)?.try_into()?,
            )),
            PgType::UUID => Self(Value::Uuid(Uuid::from_sql(ty, raw)?)),
            PgType::RECORD => {
                let num_fields = read_be_i32(&mut raw)?;
                let mut tuple = vec![];
                for _ in 0..num_fields {
                    let oid = u32::reinterpret_cast(read_be_i32(&mut raw)?);
                    let typ = match PgType::from_oid(oid) {
                        Some(typ) => typ,
                        None => return Err("unknown oid".into()),
                    };
                    let v = read_value::<Option<Slt>>(&typ, &mut raw)?;
                    tuple.push(v.map(|v| v.0));
                }
                Self(Value::Record(tuple))
            }
            PgType::INT4_RANGE
            | PgType::INT8_RANGE
            | PgType::DATE_RANGE
            | PgType::NUM_RANGE
            | PgType::TS_RANGE
            | PgType::TSTZ_RANGE => {
                use mz_repr::adt::range::Range;
                let range: Range<Slt> = Range::from_sql(ty, raw)?;
                Self(Value::Range(range.into_bounds(|b| Box::new(b.0))))
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
                                    lower_bound: isize::cast_from(d.lower_bound),
                                    length: usize::try_from(d.len)
                                        .expect("cannot have negative length"),
                                })
                            })
                            .collect()?,
                        elements,
                    })
                }
                _ => match ty.oid() {
                    oid::TYPE_UINT2_OID => Self(Value::UInt2(UInt2::from_sql(ty, raw)?)),
                    oid::TYPE_UINT4_OID => Self(Value::UInt4(UInt4::from_sql(ty, raw)?)),
                    oid::TYPE_UINT8_OID => Self(Value::UInt8(UInt8::from_sql(ty, raw)?)),
                    oid::TYPE_MZ_TIMESTAMP_OID => {
                        let s = types::text_from_sql(raw)?;
                        let t: mz_repr::Timestamp = s.parse()?;
                        Self(Value::MzTimestamp(t))
                    }
                    oid::TYPE_MZ_ACL_ITEM_OID => Self(Value::MzAclItem(MzAclItem::decode_binary(
                        types::bytea_from_sql(raw),
                    )?)),
                    _ => unreachable!(),
                },
            },
        })
    }
    fn accepts(ty: &PgType) -> bool {
        match ty.kind() {
            PgKind::Array(_) | PgKind::Composite(_) => return true,
            _ => {}
        }
        match ty.oid() {
            oid::TYPE_UINT2_OID
            | oid::TYPE_UINT4_OID
            | oid::TYPE_UINT8_OID
            | oid::TYPE_MZ_TIMESTAMP_OID
            | oid::TYPE_MZ_ACL_ITEM_OID => return true,
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
                | PgType::INT4_RANGE
                | PgType::INT4_RANGE_ARRAY
                | PgType::INT8_RANGE
                | PgType::INT8_RANGE_ARRAY
                | PgType::DATE_RANGE
                | PgType::DATE_RANGE_ARRAY
                | PgType::NUM_RANGE
                | PgType::NUM_RANGE_ARRAY
                | PgType::TS_RANGE
                | PgType::TS_RANGE_ARRAY
                | PgType::TSTZ_RANGE
                | PgType::TSTZ_RANGE_ARRAY
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
    let value = match usize::try_from(read_be_i32(buf)?) {
        Err(_) => None,
        Ok(len) => {
            if len > buf.len() {
                return Err("invalid buffer size".into());
            }
            let (head, tail) = buf.split_at(len);
            *buf = tail;
            Some(head)
        }
    };
    T::from_sql_nullable(type_, value)
}

fn format_datum(d: Slt, typ: &Type, mode: Mode, col: usize) -> String {
    match (typ, d.0) {
        (Type::Bool, Value::Bool(b)) => b.to_string(),

        (Type::Integer, Value::Int2(i)) => i.to_string(),
        (Type::Integer, Value::Int4(i)) => i.to_string(),
        (Type::Integer, Value::Int8(i)) => i.to_string(),
        (Type::Integer, Value::UInt2(u)) => u.0.to_string(),
        (Type::Integer, Value::UInt4(u)) => u.0.to_string(),
        (Type::Integer, Value::UInt8(u)) => u.0.to_string(),
        (Type::Integer, Value::Oid(i)) => i.to_string(),
        // TODO(benesch): rewrite to avoid `as`.
        #[allow(clippy::as_conversions)]
        (Type::Integer, Value::Float4(f)) => format!("{}", f as i64),
        // TODO(benesch): rewrite to avoid `as`.
        #[allow(clippy::as_conversions)]
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
                crate::parser::split_cols(s, types.len())
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        formatted
    }
}

impl<'a> Runner<'a> {
    pub async fn start(config: &'a RunConfig<'a>) -> Result<Runner<'a>, anyhow::Error> {
        let mut runner = Self {
            config,
            inner: None,
        };
        runner.reset().await?;
        Ok(runner)
    }

    pub async fn reset(&mut self) -> Result<(), anyhow::Error> {
        // Explicitly drop the old runner here to ensure that we wait for threads to terminate
        // before starting a new runner
        drop(self.inner.take());
        self.inner = Some(RunnerInner::start(self.config).await?);

        Ok(())
    }

    async fn run_record<'r>(
        &mut self,
        record: &'r Record<'r>,
        in_transaction: &mut bool,
    ) -> Result<Outcome<'r>, anyhow::Error> {
        if let Record::ResetServer = record {
            self.reset().await?;
            Ok(Outcome::Success)
        } else {
            self.inner
                .as_mut()
                .expect("RunnerInner missing")
                .run_record(record, in_transaction)
                .await
        }
    }

    async fn reset_database(&mut self) -> Result<(), anyhow::Error> {
        let inner = self.inner.as_mut().expect("RunnerInner missing");

        inner.client.batch_execute("ROLLBACK;").await?;

        inner
            .system_client
            .batch_execute(
                "ROLLBACK;
                 SET cluster = mz_introspection;
                 RESET cluster_replica;",
            )
            .await?;

        inner
            .system_client
            .batch_execute("ALTER SYSTEM RESET ALL")
            .await?;

        // Drop all databases, then recreate the `materialize` database.
        for row in inner
            .system_client
            .query("SELECT name FROM mz_databases", &[])
            .await?
        {
            let name: &str = row.get("name");
            inner
                .system_client
                .batch_execute(&format!("DROP DATABASE {name}"))
                .await?;
        }
        inner
            .system_client
            .batch_execute("CREATE DATABASE materialize")
            .await?;

        // Ensure default cluster exists with one replica of size '1'. We don't
        // destroy the existing default cluster replica if it exists, as turning
        // on a cluster replica is exceptionally slow.
        let mut needs_default_cluster = true;
        for row in inner
            .system_client
            .query("SELECT name FROM mz_clusters WHERE id LIKE 'u%'", &[])
            .await?
        {
            match row.get("name") {
                "default" => needs_default_cluster = false,
                name => {
                    inner
                        .system_client
                        .batch_execute(&format!("DROP CLUSTER {name}"))
                        .await?
                }
            }
        }
        if needs_default_cluster {
            inner
                .system_client
                .batch_execute("CREATE CLUSTER default REPLICAS ()")
                .await?;
        }
        let mut needs_default_replica = true;
        for row in inner
            .system_client
            .query(
                "SELECT name, size FROM mz_cluster_replicas
                 WHERE cluster_id = (SELECT id FROM mz_clusters WHERE name = 'default')",
                &[],
            )
            .await?
        {
            match (row.get("name"), row.get("size")) {
                ("r1", "1") => needs_default_replica = false,
                (name, _) => {
                    inner
                        .system_client
                        .batch_execute(&format!("DROP CLUSTER REPLICA default.{name}"))
                        .await?
                }
            }
        }
        if needs_default_replica {
            inner
                .system_client
                .batch_execute("CREATE CLUSTER REPLICA default.r1 SIZE '1'")
                .await?;
        }

        // Grant initial privileges.
        inner
            .system_client
            .batch_execute("GRANT USAGE ON DATABASE materialize TO PUBLIC")
            .await?;
        inner
            .system_client
            .batch_execute("GRANT CREATE ON DATABASE materialize TO materialize")
            .await?;
        inner
            .system_client
            .batch_execute("GRANT CREATE ON SCHEMA materialize.public TO materialize")
            .await?;
        inner
            .system_client
            .batch_execute("GRANT USAGE ON CLUSTER default TO PUBLIC")
            .await?;
        inner
            .system_client
            .batch_execute("GRANT CREATE ON CLUSTER default TO materialize")
            .await?;

        // Some sqllogic tests require more than the default amount of tables, so we increase the
        // limit for all tests.
        inner
            .system_client
            .simple_query("ALTER SYSTEM SET max_tables = 100")
            .await?;

        if inner.enable_table_keys {
            inner
                .system_client
                .simple_query("ALTER SYSTEM SET enable_table_keys = true")
                .await?;
        }

        inner.client = connect(inner.server_addr, None).await;
        inner.system_client = connect(inner.internal_server_addr, Some("mz_system")).await;
        inner.clients = BTreeMap::new();

        Ok(())
    }
}

impl RunnerInner {
    pub async fn start(config: &RunConfig<'_>) -> Result<RunnerInner, anyhow::Error> {
        let temp_dir = tempfile::tempdir()?;
        let environment_id = EnvironmentId::for_tests();
        let (consensus_uri, adapter_stash_url, storage_stash_url) = {
            let postgres_url = &config.postgres_url;
            info!(%postgres_url, "starting server");
            let (client, conn) = Retry::default()
                .max_tries(5)
                .retry_async(|_| async {
                    match tokio_postgres::connect(postgres_url, NoTls).await {
                        Ok(c) => Ok(c),
                        Err(e) => {
                            error!(%e, "failed to connect to postgres");
                            Err(e)
                        }
                    }
                })
                .await?;
            task::spawn(|| "sqllogictest_connect", async move {
                if let Err(e) = conn.await {
                    panic!("connection error: {}", e);
                }
            });
            client
                .batch_execute(
                    "DROP SCHEMA IF EXISTS sqllogictest_consensus CASCADE;
                     DROP SCHEMA IF EXISTS sqllogictest_adapter CASCADE;
                     DROP SCHEMA IF EXISTS sqllogictest_storage CASCADE;
                     CREATE SCHEMA sqllogictest_consensus;
                     CREATE SCHEMA sqllogictest_adapter;
                     CREATE SCHEMA sqllogictest_storage;",
                )
                .await?;
            (
                format!("{postgres_url}?options=--search_path=sqllogictest_consensus"),
                format!("{postgres_url}?options=--search_path=sqllogictest_adapter"),
                format!("{postgres_url}?options=--search_path=sqllogictest_storage"),
            )
        };

        let orchestrator = Arc::new(
            ProcessOrchestrator::new(ProcessOrchestratorConfig {
                image_dir: env::current_exe()?.parent().unwrap().to_path_buf(),
                suppress_output: false,
                environment_id: environment_id.to_string(),
                secrets_dir: temp_dir.path().join("secrets"),
                command_wrapper: config
                    .orchestrator_process_wrapper
                    .as_ref()
                    .map_or(Ok(vec![]), |s| shell_words::split(s))?,
                propagate_crashes: true,
                tcp_proxy: None,
                scratch_directory: None,
            })
            .await?,
        );
        let now = SYSTEM_TIME.clone();
        let metrics_registry = MetricsRegistry::new();
        let persist_clients = PersistClientCache::new(
            PersistConfig::new(&mz_environmentd::BUILD_INFO, now.clone()),
            &metrics_registry,
            |_, _| PubSubClientConnection::noop(),
        );
        let persist_clients = Arc::new(persist_clients);
        let postgres_factory = StashFactory::new(&metrics_registry);
        let secrets_controller = Arc::clone(&orchestrator);
        let connection_context = ConnectionContext::for_tests(orchestrator.reader());
        let server_config = mz_environmentd::Config {
            adapter_stash_url,
            controller: ControllerConfig {
                build_info: &mz_environmentd::BUILD_INFO,
                orchestrator,
                clusterd_image: "clusterd".into(),
                init_container_image: None,
                persist_location: PersistLocation {
                    blob_uri: format!("file://{}/persist/blob", temp_dir.path().display()),
                    consensus_uri,
                },
                persist_clients,
                storage_stash_url,
                now: SYSTEM_TIME.clone(),
                postgres_factory: postgres_factory.clone(),
                metrics_registry: metrics_registry.clone(),
                scratch_directory_enabled: false,
                persist_pubsub_url: "http://not-needed-for-sqllogictests".into(),
            },
            secrets_controller,
            cloud_resource_controller: None,
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
            all_features: false,
            metrics_registry,
            now,
            environment_id,
            cluster_replica_sizes: Default::default(),
            bootstrap_default_cluster_replica_size: "1".into(),
            bootstrap_builtin_cluster_replica_size: "1".into(),
            system_parameter_defaults: Default::default(),
            default_storage_cluster_size: None,
            availability_zones: Default::default(),
            connection_context,
            tracing_handle: TracingHandle::disabled(),
            storage_usage_collection_interval: Duration::from_secs(3600),
            storage_usage_retention_period: None,
            segment_api_key: None,
            egress_ips: vec![],
            aws_account_id: None,
            aws_privatelink_availability_zones: None,
            launchdarkly_sdk_key: None,
            launchdarkly_key_map: Default::default(),
            config_sync_loop_interval: None,
            bootstrap_role: Some("materialize".into()),
            deploy_generation: None,
            waiting_on_leader_promotion: None,
        };
        // We need to run the server on its own Tokio runtime, which in turn
        // requires its own thread, so that we can wait for any tasks spawned
        // by the server to be shutdown at the end of each file. If we were to
        // share a Tokio runtime, tasks from the last file's server would still
        // be live at the start of the next file's server.
        let (server_addr_tx, server_addr_rx) = oneshot::channel();
        let (internal_server_addr_tx, internal_server_addr_rx) = oneshot::channel();
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
            internal_server_addr_tx
                .send(server.internal_sql_local_addr())
                .expect("receiver should not drop first");
            let _ = runtime.block_on(shutdown_tripwire);
        });
        let server_addr = server_addr_rx.await??;
        let internal_server_addr = internal_server_addr_rx.await?;

        let system_client = connect(internal_server_addr, Some("mz_system")).await;
        let client = connect(server_addr, None).await;

        Ok(RunnerInner {
            server_addr,
            internal_server_addr,
            _shutdown_trigger: shutdown_trigger,
            _server_thread: server_thread.join_on_drop(),
            _temp_dir: temp_dir,
            client,
            system_client,
            clients: BTreeMap::new(),
            auto_index_tables: config.auto_index_tables,
            auto_index_selects: config.auto_index_selects,
            auto_transactions: config.auto_transactions,
            enable_table_keys: config.enable_table_keys,
        })
    }

    async fn run_record<'r>(
        &mut self,
        record: &'r Record<'r>,
        in_transaction: &mut bool,
    ) -> Result<Outcome<'r>, anyhow::Error> {
        match &record {
            Record::Statement {
                expected_error,
                rows_affected,
                sql,
                location,
            } => {
                if self.auto_transactions && *in_transaction {
                    self.client.execute("COMMIT", &[]).await?;
                    *in_transaction = false;
                }
                match self
                    .run_statement(*expected_error, *rows_affected, sql, location.clone())
                    .await?
                {
                    Outcome::Success => {
                        if self.auto_index_tables {
                            let additional = mutate(sql);
                            for stmt in additional {
                                self.client.execute(&stmt, &[]).await?;
                            }
                        }
                        Ok(Outcome::Success)
                    }
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
            } => {
                self.run_query(sql, output, location.clone(), in_transaction)
                    .await
            }
            Record::Simple {
                conn,
                user,
                sql,
                output,
                location,
                ..
            } => {
                self.run_simple(*conn, *user, sql, output, location.clone())
                    .await
            }
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
        &self,
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

    async fn prepare_query<'a>(
        &self,
        sql: &str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
        in_transaction: &mut bool,
    ) -> Result<PrepareQueryOutcome<'a>, anyhow::Error> {
        // get statement
        let statements = match mz_sql::parse::parse(sql) {
            Ok(statements) => statements,
            Err(e) => match output {
                Ok(_) => {
                    return Ok(PrepareQueryOutcome::Outcome(Outcome::ParseFailure {
                        error: e.into(),
                        location,
                    }));
                }
                Err(expected_error) => {
                    if Regex::new(expected_error)?.is_match(&format!("{:#}", e)) {
                        return Ok(PrepareQueryOutcome::Outcome(Outcome::Success));
                    } else {
                        return Ok(PrepareQueryOutcome::Outcome(Outcome::ParseFailure {
                            error: e.into(),
                            location,
                        }));
                    }
                }
            },
        };
        let statement = match &*statements {
            [] => bail!("Got zero statements?"),
            [statement] => statement,
            _ => bail!("Got multiple statements: {:?}", statements),
        };
        let mut is_select = false;
        let mut num_attributes = None;
        match statement {
            Statement::Select(stmt) => {
                is_select = true;
                num_attributes = derive_num_attributes(&stmt.query.body);
            }
            _ => (),
        }

        match output {
            Ok(_) => {
                if self.auto_transactions && !*in_transaction {
                    // No ISOLATION LEVEL SERIALIZABLE because of #18136
                    self.client.execute("BEGIN", &[]).await?;
                    *in_transaction = true;
                }
            }
            Err(_) => {
                if self.auto_transactions && *in_transaction {
                    self.client.execute("COMMIT", &[]).await?;
                    *in_transaction = false;
                }
            }
        }

        // `SHOW` commands reference catalog schema, thus are not in the same timedomain and not
        // allowed in the same transaction, see:
        // https://materialize.com/docs/sql/begin/#same-timedomain-error
        match statement {
            Statement::Show(..) => {
                if self.auto_transactions && *in_transaction {
                    self.client.execute("COMMIT", &[]).await?;
                    *in_transaction = false;
                }
            }
            _ => (),
        }
        Ok(PrepareQueryOutcome::QueryPrepared(QueryInfo {
            is_select,
            num_attributes,
        }))
    }

    async fn execute_query<'a>(
        &self,
        sql: &str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
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
            let row = format_row(row, expected_types, *mode, sort);
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

    async fn execute_view<'a>(
        &self,
        sql: &str,
        num_attributes: Option<usize>,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        // Create indexed view SQL commands and execute `CREATE VIEW`.
        let expected_column_names = if let Ok(QueryOutput { column_names, .. }) = output {
            column_names.clone()
        } else {
            None
        };
        let (create_view, create_index, view_sql, drop_view) = generate_view_sql(
            sql,
            Uuid::new_v4().as_simple(),
            num_attributes,
            expected_column_names,
        );
        let create_view_result = self.client.execute(create_view.as_str(), &[]).await;

        // Either handle a view creation error or alternatively index
        // and query the view.
        let view_outcome;
        if let Err(view_error) = create_view_result {
            if let Err(expected_error) = output {
                view_outcome = if Regex::new(expected_error)?.is_match(&format!("{:#}", view_error))
                {
                    Outcome::Success
                } else {
                    Outcome::PlanFailure {
                        error: view_error.into(),
                        location: location.clone(),
                    }
                }
            } else {
                view_outcome = Outcome::PlanFailure {
                    error: view_error.into(),
                    location: location.clone(),
                }
            }
        } else {
            self.client.execute(create_index.as_str(), &[]).await?;
            view_outcome = self
                .execute_query(view_sql.as_str(), output, location.clone())
                .await?;

            // Remember to clean up after ourselves by dropping the view.
            self.client.execute(drop_view.as_str(), &[]).await?;
        }
        Ok(view_outcome)
    }

    async fn run_query<'a>(
        &self,
        sql: &'a str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
        in_transaction: &mut bool,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        let prepare_outcome = self
            .prepare_query(sql, output, location.clone(), in_transaction)
            .await?;
        match prepare_outcome {
            PrepareQueryOutcome::QueryPrepared(QueryInfo {
                is_select,
                num_attributes,
            }) => {
                let query_outcome = self.execute_query(sql, output, location.clone()).await?;
                if is_select && self.auto_index_selects {
                    let view_outcome = self
                        .execute_view(sql, num_attributes, output, location.clone())
                        .await?;

                    // We compare here the query-based and view-based outcomes.
                    // We only produce a test failure if the outcomes are of different
                    // variant types, thus accepting smaller deviations in the details
                    // produced for each variant.
                    if std::mem::discriminant::<Outcome>(&query_outcome)
                        != std::mem::discriminant::<Outcome>(&view_outcome)
                    {
                        return Ok(Outcome::InconsistentViewOutcome {
                            query_outcome: Box::new(query_outcome),
                            view_outcome: Box::new(view_outcome),
                            location: location.clone(),
                        });
                    }
                }
                Ok(query_outcome)
            }
            PrepareQueryOutcome::Outcome(outcome) => Ok(outcome),
        }
    }

    async fn get_conn(
        &mut self,
        name: Option<&str>,
        user: Option<&str>,
    ) -> &tokio_postgres::Client {
        match name {
            None => &self.client,
            Some(name) => {
                if !self.clients.contains_key(name) {
                    let addr = if matches!(user, Some("mz_system") | Some("mz_introspection")) {
                        self.internal_server_addr
                    } else {
                        self.server_addr
                    };
                    let client = connect(addr, user).await;
                    self.clients.insert(name.into(), client);
                }
                self.clients.get(name).unwrap()
            }
        }
    }

    async fn run_simple<'a>(
        &mut self,
        conn: Option<&'a str>,
        user: Option<&'a str>,
        sql: &'a str,
        output: &'a Output,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        let client = self.get_conn(conn, user).await;
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

async fn connect(addr: SocketAddr, user: Option<&str>) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user={}",
            addr.ip(),
            addr.port(),
            user.unwrap_or("materialize")
        ),
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
    pub auto_index_tables: bool,
    pub auto_index_selects: bool,
    pub auto_transactions: bool,
    pub enable_table_keys: bool,
    pub orchestrator_process_wrapper: Option<String>,
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
    runner: &mut Runner<'_>,
    source: &str,
    input: &str,
) -> Result<Outcomes, anyhow::Error> {
    runner.reset_database().await?;

    let mut outcomes = Outcomes::default();
    let mut parser = crate::parser::Parser::new(source, input);
    // Transactions are currently relatively slow. Since sqllogictest runs in a single connection
    // there should be no difference in having longer running transactions.
    let mut in_transaction = false;
    writeln!(runner.config.stdout, "==> {}", source);

    for record in parser.parse_records()? {
        // In maximal-verbosity mode, print the query before attempting to run
        // it. Running the query might panic, so it is important to print out
        // what query we are trying to run *before* we panic.
        if runner.config.verbosity >= 2 {
            print_record(runner.config, &record);
        }

        let outcome = runner
            .run_record(&record, &mut in_transaction)
            .await
            .map_err(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // Print failures in verbose mode.
        if runner.config.verbosity >= 1 && !outcome.success() {
            if runner.config.verbosity < 2 {
                // If `verbosity >= 2`, we'll already have printed the record,
                // so don't print it again. Yes, this is an ugly bit of logic.
                // Please don't try to consolidate it with the `print_record`
                // call above, as it's important to have a mode in which records
                // are printed before they are run, so that if running the
                // record panics, you can tell which record caused it.
                print_record(runner.config, &record);
            }
            writeln!(
                runner.config.stdout,
                "{}",
                util::indent(&outcome.to_string(), 4)
            );
            writeln!(runner.config.stdout, "{}", util::indent("----", 4));
        }

        outcomes.0[outcome.code()] += 1;

        if let Outcome::Bail { .. } = outcome {
            break;
        }

        if runner.config.fail_fast && !outcome.success() {
            break;
        }
    }
    Ok(outcomes)
}

pub async fn run_file(runner: &mut Runner<'_>, filename: &Path) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    File::open(filename)?.read_to_string(&mut input)?;
    run_string(runner, &format!("{}", filename.display()), &input).await
}

pub async fn rewrite_file(runner: &mut Runner<'_>, filename: &Path) -> Result<(), anyhow::Error> {
    runner.reset_database().await?;

    let mut file = OpenOptions::new().read(true).write(true).open(filename)?;

    let mut input = String::new();
    file.read_to_string(&mut input)?;

    let mut buf = RewriteBuffer::new(&input);

    let mut parser = crate::parser::Parser::new(filename.to_str().unwrap_or(""), &input);
    writeln!(runner.config.stdout, "==> {}", filename.display());
    let mut in_transaction = false;

    for record in parser.parse_records()? {
        let record = record;
        let outcome = runner.run_record(&record, &mut in_transaction).await?;

        match (&record, &outcome) {
            // If we see an output failure for a query, rewrite the expected output
            // to match the observed output.
            (
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
            ) => {
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
                }
            }
            (
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
            ) => {
                buf.append_header(&input, expected_output);

                buf.append(format!("{} values hashing to {}\n", num_values, md5).as_str())
            }
            (
                Record::Simple {
                    output_str: expected_output,
                    ..
                },
                Outcome::OutputFailure {
                    actual_output: Output::Values(actual_output),
                    ..
                },
            ) => {
                buf.append_header(&input, expected_output);

                for (i, row) in actual_output.iter().enumerate() {
                    if i != 0 {
                        buf.append("\n");
                    }
                    buf.append(row);
                }
            }
            (
                Record::Query {
                    sql,
                    output: Err(err),
                    ..
                },
                outcome,
            )
            | (
                Record::Statement {
                    expected_error: Some(err),
                    sql,
                    ..
                },
                outcome,
            ) if outcome.err_msg().is_some() => {
                buf.rewrite_expected_error(&input, err, &outcome.err_msg().unwrap(), sql)
            }
            (_, Outcome::Success) => {}
            _ => bail!("unexpected: {:?} {:?}", record, outcome),
        }
    }

    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(buf.finish().as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Provides a means to rewrite the `.slt` file while iterating over it.
///
/// This struct takes the slt file as its `input`, tracks a cursor into it
/// (`input_offset`), and provides a buffe (`output`) to store the rewritten
/// results.
///
/// Functions that modify the file will lazily move `input` into `output` using
/// `flush_to`. However, those calls should all be interior to other functions.
#[derive(Debug)]
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
        // TODO(benesch): is it possible to rewrite this to avoid `as`?
        #[allow(clippy::as_conversions)]
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

    fn rewrite_expected_error(
        &mut self,
        input: &String,
        old_err: &str,
        new_err: &str,
        query: &str,
    ) {
        // Output everything before this error message.
        // TODO(benesch): is it possible to rewrite this to avoid `as`?
        #[allow(clippy::as_conversions)]
        let err_offset = old_err.as_ptr() as usize - input.as_ptr() as usize;
        self.flush_to(err_offset);
        self.append(new_err);
        self.append("\n");
        self.append(query);
        // TODO(benesch): is it possible to rewrite this to avoid `as`?
        #[allow(clippy::as_conversions)]
        self.skip_to(query.as_ptr() as usize - input.as_ptr() as usize + query.len())
    }

    fn peek_last(&self, n: usize) -> &str {
        &self.output[self.output.len() - n..]
    }

    fn finish(mut self) -> String {
        self.flush_to(self.input.len());
        self.output
    }
}

/// Generates view creation, view indexing, view querying, and view
/// dropping SQL commands for a given `SELECT` query. If the number
/// of attributes produced by the query is known, the view commands
/// are specialized to avoid issues with column ambiguity. This
/// function is a helper for `--auto_index_selects` and assumes that
/// the provided input SQL has already been run through the parser,
/// resulting in a valid `SELECT` statement.
fn generate_view_sql(
    sql: &str,
    view_uuid: &Simple,
    num_attributes: Option<usize>,
    expected_column_names: Option<Vec<ColumnName>>,
) -> (String, String, String, String) {
    // We create a view for the select query and index it. Since
    // one-shot SELECT statements may contain ambiguous column names,
    // we either use the expected column names, if that option was
    // provided, or else just rename the output schema of the view
    // using numerically increasing attribute names, whenever possible.
    // This strategy makes it possible to use `CREATE INDEX`, thus
    // matching the behavior of the option `auto_index_tables`. However,
    // we may be presented with a `SELECT *` query, in which case the parser
    // does not produce sufficient information to allow us to compute
    // the number of output columns. In the latter case, we are supplied
    // with `None` for `num_attributes` and just employ the command
    // `CREATE DEFAULT INDEX` instead. Additionally, the view is created
    // without schema renaming. This strategy is insufficient to dodge
    // column name ambiguity in all cases, but we assume here that we
    // can adjust the (hopefully) small number of tests that eventually
    // challenge us in this particular way.
    let name = UnresolvedItemName(vec![Ident::new(format!("v{}", view_uuid))]);
    let columns = expected_column_names.map_or(
        num_attributes.map_or(vec![], |n| {
            (1..=n).map(|i| Ident::new(format!("a{i}"))).collect()
        }),
        |cols| cols.iter().map(|c| Ident::new(c.as_str())).collect(),
    );

    // To create the view, re-parse the sql; note that we must find exactly
    // one statement and it must be a `SELECT`.
    // NOTE(vmarcos): Direct string manipulation was attempted while
    // prototyping the code below, which avoids the extra parsing and
    // data structure cloning. However, running DDL is so slow that
    // it did not matter in terms of runtime. We can revisit this if
    // DDL cost drops dramatically in the future.
    let stmts = parser::parse_statements(sql).unwrap_or_default();
    assert!(stmts.len() == 1);
    let query = match &stmts[0] {
        Statement::Select(stmt) => &stmt.query,
        _ => unreachable!("This function should only be called for SELECTs"),
    };
    let create_view = AstStatement::<Raw>::CreateView(CreateViewStatement {
        if_exists: IfExistsBehavior::Error,
        temporary: false,
        definition: ViewDefinition {
            name: name.clone(),
            columns: columns.clone(),
            query: query.clone(),
        },
    })
    .to_ast_string_stable();

    // We then create either a `CREATE INDEX` or a `CREATE DEFAULT INDEX`
    // statement, depending on whether we could obtain the number of
    // attributes from the original `SELECT`.
    let create_index = AstStatement::<Raw>::CreateIndex(CreateIndexStatement {
        name: None,
        in_cluster: None,
        on_name: RawItemName::Name(name.clone()),
        key_parts: if columns.len() == 0 {
            None
        } else {
            Some(
                columns
                    .iter()
                    .map(|ident| Expr::Identifier(vec![ident.clone()]))
                    .collect(),
            )
        },
        with_options: Vec::new(),
        if_not_exists: false,
    })
    .to_ast_string_stable();

    // Prior to querying the view, process the `ORDER BY` clause of
    // the `SELECT` query, if any. Ordering is not preserved when a
    // view includes an `ORDER BY` clause and must be re-enforced by
    // an external `ORDER BY` clause when querying the view.
    let view_order_by = derive_order_by(&query.body, &query.order_by);

    // `SELECT * FROM {name} [ORDER BY <num_seq>]`
    let view_sql = AstStatement::<Raw>::Select(SelectStatement {
        query: Query {
            ctes: CteBlock::Simple(vec![]),
            body: SetExpr::Select(Box::new(Select {
                distinct: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: RawItemName::Name(name.clone()),
                        alias: None,
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
                options: vec![],
            })),
            order_by: view_order_by,
            limit: None,
            offset: None,
        },
        as_of: None,
    })
    .to_ast_string_stable();

    // `DROP VIEW {name}`
    let drop_view = AstStatement::<Raw>::DropObjects(DropObjectsStatement {
        object_type: ObjectType::View,
        if_exists: false,
        names: vec![UnresolvedObjectName::Item(name)],
        cascade: false,
    })
    .to_ast_string_stable();

    (create_view, create_index, view_sql, drop_view)
}

/// Analyzes the provided query `body` to derive the number of
/// attributes in the query. We only consider syntactic cues,
/// so we may end up deriving `None` for the number of attributes
/// as a conservative approximation.
fn derive_num_attributes(body: &SetExpr<Raw>) -> Option<usize> {
    let Some(projection) = find_projection(body) else { return None };
    derive_num_attributes_from_projection(projection)
}

/// Analyzes a query's `ORDER BY` clause to derive an `ORDER BY`
/// clause that is exclusively making numeric references. The
/// rewritten `ORDER BY` clause is then usable when querying a
/// view that contains the same `SELECT` as the given query.
fn derive_order_by(body: &SetExpr<Raw>, order_by: &Vec<OrderByExpr<Raw>>) -> Vec<OrderByExpr<Raw>> {
    let Some(projection) = find_projection(body) else { return vec![] };
    derive_order_by_from_projection(projection, order_by)
}

/// Finds the projection list in a `SELECT` query body.
fn find_projection(body: &SetExpr<Raw>) -> Option<&Vec<SelectItem<Raw>>> {
    // Iterate to peel off the query body until the query's
    // projection list is found.
    let mut set_expr = body;
    loop {
        match set_expr {
            SetExpr::Select(select) => {
                return Some(&select.projection);
            }
            SetExpr::SetOperation { left, .. } => set_expr = left.as_ref(),
            SetExpr::Query(query) => set_expr = &query.body,
            _ => return None,
        }
    }
}

/// Computes the number of attributes that are obtained by the
/// projection of a `SELECT` query. The projection may include
/// wildcards, in which case the analysis just returns `None`.
fn derive_num_attributes_from_projection(projection: &Vec<SelectItem<Raw>>) -> Option<usize> {
    let mut num_attributes = 0usize;
    for item in projection.iter() {
        let SelectItem::Expr { expr, .. } = item else { return None };
        match expr {
            Expr::QualifiedWildcard(..) | Expr::WildcardAccess(..) => {
                return None;
            }
            _ => {
                num_attributes += 1;
            }
        }
    }
    Some(num_attributes)
}

/// Computes an `ORDER BY` clause with only numeric references
/// from given projection and `ORDER BY` of a `SELECT` query.
/// If the derivation fails to match a given expression, the
/// matched prefix is returned. Note that this could be empty.
fn derive_order_by_from_projection(
    projection: &Vec<SelectItem<Raw>>,
    order_by: &Vec<OrderByExpr<Raw>>,
) -> Vec<OrderByExpr<Raw>> {
    let mut view_order_by: Vec<OrderByExpr<Raw>> = vec![];
    for order_by_expr in order_by.iter() {
        let query_expr = &order_by_expr.expr;
        let view_expr = match query_expr {
            Expr::Value(mz_sql_parser::ast::Value::Number(_)) => query_expr.clone(),
            _ => {
                // Find expression in query projection, if we can.
                if let Some(i) = projection.iter().position(|item| match item {
                    SelectItem::Expr { expr, alias } => {
                        expr == query_expr
                            || match query_expr {
                                Expr::Identifier(ident) => {
                                    ident.len() == 1 && Some(&ident[0]) == alias.as_ref()
                                }
                                _ => false,
                            }
                    }
                    SelectItem::Wildcard => false,
                }) {
                    Expr::Value(mz_sql_parser::ast::Value::Number((i + 1).to_string()))
                } else {
                    // Stop at prefix if we cannot match further.
                    break;
                }
            }
        };
        view_order_by.push(OrderByExpr {
            expr: view_expr,
            asc: order_by_expr.asc,
            nulls_last: order_by_expr.nulls_last,
        });
    }
    view_order_by
}

/// Returns extra statements to execute after `stmt` is executed.
fn mutate(sql: &str) -> Vec<String> {
    let stmts = parser::parse_statements(sql).unwrap_or_default();
    let mut additional = Vec::new();
    for stmt in stmts {
        match stmt {
            AstStatement::CreateTable(stmt) => additional.push(
                // CREATE TABLE -> CREATE INDEX. Specify all columns manually in case CREATE
                // DEFAULT INDEX ever goes away.
                AstStatement::<Raw>::CreateIndex(CreateIndexStatement {
                    name: None,
                    in_cluster: None,
                    on_name: RawItemName::Name(stmt.name.clone()),
                    key_parts: Some(
                        stmt.columns
                            .iter()
                            .map(|def| Expr::Identifier(vec![def.name.clone()]))
                            .collect(),
                    ),
                    with_options: Vec::new(),
                    if_not_exists: false,
                })
                .to_ast_string_stable(),
            ),
            _ => {}
        }
    }
    additional
}

#[mz_ore::test]
fn test_generate_view_sql() {
    let uuid = Uuid::parse_str("67e5504410b1426f9247bb680e5fe0c8").unwrap();
    let cases = vec![
        (("SELECT * FROM t", None, None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" AS SELECT * FROM "t""#.to_string(),
            r#"CREATE DEFAULT INDEX ON "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        (("SELECT a, b, c FROM t1, t2", Some(3), Some(vec![ColumnName::from("a"), ColumnName::from("b"), ColumnName::from("c")])),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" ("a", "b", "c") AS SELECT "a", "b", "c" FROM "t1", "t2""#.to_string(),
            r#"CREATE INDEX ON "v67e5504410b1426f9247bb680e5fe0c8" ("a", "b", "c")"#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        (("SELECT a, b, c FROM t1, t2", Some(3), None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" ("a1", "a2", "a3") AS SELECT "a", "b", "c" FROM "t1", "t2""#.to_string(),
            r#"CREATE INDEX ON "v67e5504410b1426f9247bb680e5fe0c8" ("a1", "a2", "a3")"#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        // A case with ambiguity that is accepted by the function, illustrating that
        // our measures to dodge this issue are imperfect.
        (("SELECT * FROM (SELECT a, sum(b) AS a FROM t GROUP BY a)", None, None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" AS SELECT * FROM (SELECT "a", "sum"("b") AS "a" FROM "t" GROUP BY "a")"#.to_string(),
            r#"CREATE DEFAULT INDEX ON "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        (("SELECT a, b, b + d AS c, a + b AS d FROM t1, t2 ORDER BY a, c, a + b", Some(4), Some(vec![ColumnName::from("a"), ColumnName::from("b"), ColumnName::from("c"), ColumnName::from("d")])),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" ("a", "b", "c", "d") AS SELECT "a", "b", "b" + "d" AS "c", "a" + "b" AS "d" FROM "t1", "t2" ORDER BY "a", "c", "a" + "b""#.to_string(),
            r#"CREATE INDEX ON "v67e5504410b1426f9247bb680e5fe0c8" ("a", "b", "c", "d")"#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8" ORDER BY 1, 3, 4"#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        (("((SELECT 1 AS a UNION SELECT 2 AS b) UNION SELECT 3 AS c) ORDER BY a", Some(1), None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" ("a1") AS (SELECT 1 AS "a" UNION SELECT 2 AS "b") UNION SELECT 3 AS "c" ORDER BY "a""#.to_string(),
            r#"CREATE INDEX ON "v67e5504410b1426f9247bb680e5fe0c8" ("a1")"#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8" ORDER BY 1"#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        (("SELECT * FROM (SELECT a, sum(b) AS a FROM t GROUP BY a) ORDER BY 1", None, None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" AS SELECT * FROM (SELECT "a", "sum"("b") AS "a" FROM "t" GROUP BY "a") ORDER BY 1"#.to_string(),
            r#"CREATE DEFAULT INDEX ON "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8" ORDER BY 1"#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
        // A case illustrating that we are not always able to rewrite `ORDER BY`.
        (("SELECT * FROM (SELECT a, sum(b) AS a FROM t GROUP BY a) ORDER BY a", None, None),
        (
            r#"CREATE VIEW "v67e5504410b1426f9247bb680e5fe0c8" AS SELECT * FROM (SELECT "a", "sum"("b") AS "a" FROM "t" GROUP BY "a") ORDER BY "a""#.to_string(),
            r#"CREATE DEFAULT INDEX ON "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"SELECT * FROM "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
            r#"DROP VIEW "v67e5504410b1426f9247bb680e5fe0c8""#.to_string(),
        )),
    ];
    for ((sql, num_attributes, expected_column_names), expected) in cases {
        let view_sql =
            generate_view_sql(sql, uuid.as_simple(), num_attributes, expected_column_names);
        assert_eq!(expected, view_sql);
    }
}

#[mz_ore::test]
fn test_mutate() {
    let cases = vec![
        ("CREATE TABLE t ()", vec![r#"CREATE INDEX ON "t" ()"#]),
        (
            "CREATE TABLE t (a INT)",
            vec![r#"CREATE INDEX ON "t" ("a")"#],
        ),
        (
            "CREATE TABLE t (a INT, b TEXT)",
            vec![r#"CREATE INDEX ON "t" ("a", "b")"#],
        ),
        // Invalid syntax works, just returns nothing.
        ("BAD SYNTAX", Vec::new()),
    ];
    for (sql, expected) in cases {
        let stmts = mutate(sql);
        assert_eq!(expected, stmts, "sql: {sql}");
    }
}
