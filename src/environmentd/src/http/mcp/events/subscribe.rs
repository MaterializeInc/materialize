// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `subscribe` event: changes to a `SELECT` result, delivered from a `SUBSCRIBE`.

use std::num::NonZeroU64;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::future::BoxFuture;
use itertools::Itertools;
use mz_adapter::client::RecordFirstRowStream;
use mz_adapter::statement_logging::StatementEndedExecutionReason;
use mz_adapter::{
    AdapterError, ExecuteContextGuard, PeekResponseUnary, SessionClient, verify_datum_desc,
};
use mz_ore::cast::CastFrom;
use mz_repr::{RelationDesc, RowIterator, RowRef};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    AsOf, Expr, Statement, SubscribeOption, SubscribeOptionName, SubscribeOutput,
    SubscribeRelation, SubscribeStatement, Value as AstValue, WithOptionValue,
};
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;

use super::{DEFAULT_TTL, EventStream, Start, StartedSubscription, StreamEnd};
use crate::http::AuthedClient;
use crate::http::mcp::McpError;
use crate::http::mcp::events_protocol::{EventDefinition, StreamParams};
use crate::http::sql::{
    Error, ExtendedRequest, ResultSender, SqlRequest, SqlResult, StatementResult, execute_request,
};

const NAME: &str = "subscribe";

pub(super) fn definition() -> EventDefinition {
    EventDefinition {
        name: NAME,
        delivery: ["push"],
        description: "Stream SELECT changes. Non-NULL cells use lossless PostgreSQL text. SQL NULL is JSON null. Resume from a notification cursor to receive updates at or after it.",
        input_schema: schema::<Arguments>(),
        payload_schema: schema::<Payload<'_>>(),
    }
}

fn schema<T: JsonSchema>() -> Schema {
    SchemaSettings::draft2020_12()
        .with(|settings| settings.inline_subschemas = true)
        .into_generator()
        .into_root_schema_for::<T>()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Arguments {
    query: String,
    #[serde(default)]
    parameters: Vec<SqlParameter>,
    cluster: String,
    #[serde(default)]
    snapshot: bool,
    #[serde(rename = "ttlMs")]
    #[schemars(
        with = "u64",
        range(min = 1, max = 86400000),
        default = "default_ttl_ms"
    )]
    ttl_ms: Option<u64>,
}

fn default_ttl_ms() -> u64 {
    DEFAULT_TTL.as_secs() * 1000
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(untagged)]
enum SqlParameter {
    Null,
    String(String),
    Bool(bool),
    Number(serde_json::Number),
}

impl SqlParameter {
    fn to_sql_text(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::String(value) => Some(value.clone()),
            Self::Bool(value) => Some(value.to_string()),
            Self::Number(value) => Some(value.to_string()),
        }
    }
}

/// The delivered frontier, including completion of the maximum SQL timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Cursor {
    At(u64),
    Complete,
}

impl Cursor {
    fn after(timestamp: u64) -> Self {
        timestamp.checked_add(1).map_or(Self::Complete, Self::At)
    }
}

impl Serialize for Cursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::At(timestamp) => serializer.collect_str(timestamp),
            Self::Complete => serializer.serialize_str("18446744073709551616"),
        }
    }
}

/// A positive resume frontier with a representable predecessor for strict AS OF.
#[derive(Debug, Clone, Copy)]
struct ResumeCursor {
    as_of: u64,
}

impl ResumeCursor {
    fn frontier(self) -> Cursor {
        Cursor::after(self.as_of)
    }
}

impl<'de> Deserialize<'de> for ResumeCursor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let as_of = text
            .parse::<u128>()
            .ok()
            .and_then(|value| value.checked_sub(1))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                serde::de::Error::custom(
                    "cursor must be a positive decimal string no greater than 18446744073709551616",
                )
            })?;
        Ok(Self { as_of })
    }
}

#[derive(Debug, Serialize, JsonSchema)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    typ: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Insert,
    Delete,
}

#[derive(Debug, Serialize, JsonSchema)]
struct Change {
    operation: Operation,
    #[serde(serialize_with = "serialize_decimal")]
    #[schemars(with = "String", regex(pattern = "^[1-9][0-9]*$"))]
    count: NonZeroU64,
    row: Vec<Option<String>>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct Payload<'a> {
    #[serde(serialize_with = "serialize_decimal")]
    #[schemars(with = "String", regex(pattern = "^[0-9]+$"))]
    logical_timestamp: u64,
    #[serde(rename = "final")]
    last: bool,
    columns: &'a [Column],
    changes: Vec<Change>,
}

fn serialize_decimal<T: std::fmt::Display, S: Serializer>(
    value: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

fn invalid_params(message: String) -> McpError {
    McpError {
        code: -32602,
        message,
        data: None,
    }
}

pub(super) async fn stream(
    mut client: AuthedClient,
    params: StreamParams,
    start: Start<'_>,
) -> Result<StartedSubscription, McpError> {
    let arguments: Arguments = serde_json::from_value(params.arguments)
        .map_err(|error| invalid_params(error.to_string()))?;
    let cursor: Option<ResumeCursor> =
        serde_json::from_value(params.cursor).map_err(|error| invalid_params(error.to_string()))?;
    if cursor.is_some() && arguments.snapshot {
        return Err(invalid_params(
            "cursor cannot be combined with snapshot".into(),
        ));
    }
    let request = request(&arguments, cursor).map_err(invalid_params)?;
    client
        .client
        .session()
        .vars_mut()
        .set_cluster(arguments.cluster);
    let ttl = arguments.ttl_ms.map(Duration::from_millis);
    super::start(start, NAME, ttl, move |stream| async move {
        let mut sender = Sender {
            stream,
            cursor: cursor.map(ResumeCursor::frontier),
            rows_delivered: 0,
        };
        if let Err(error) = execute_request(&mut client, request, &mut sender).await {
            sender.stream.record(error.into());
        }
        sender.stream
    })
    .await
}

/// Builds the SUBSCRIBE for `arguments`. A `cursor` of `F` resumes with every update at a logical
/// timestamp `>= F` and no snapshot.
fn request(arguments: &Arguments, cursor: Option<ResumeCursor>) -> Result<SqlRequest, String> {
    let mut statements =
        mz_sql::parse::parse_with_limit(&arguments.query)?.map_err(|e| e.to_string())?;
    if statements.len() != 1 {
        return Err("query must contain exactly one SELECT statement".into());
    }
    let Statement::Select(select) = statements.remove(0).ast else {
        return Err("query must be a SELECT statement".into());
    };
    if select.as_of.is_some() {
        return Err("SELECT AS OF is unsupported for subscriptions".into());
    }
    let statement = SubscribeStatement {
        relation: SubscribeRelation::Query(select.query),
        options: vec![
            SubscribeOption {
                name: SubscribeOptionName::Progress,
                value: Some(WithOptionValue::Value(AstValue::Boolean(true))),
            },
            SubscribeOption {
                name: SubscribeOptionName::Snapshot,
                value: Some(WithOptionValue::Value(AstValue::Boolean(
                    arguments.snapshot,
                ))),
            },
        ],
        // Without a snapshot, the subscribe sink emits only times strictly after the as-of, so
        // `F - 1` resumes at `F`. `AS OF AT LEAST` would advance a compacted as-of to the since
        // and silently skip updates, so a resume must use a strict `AS OF`.
        as_of: cursor.map(|f| AsOf::At(Expr::Value(AstValue::Number(f.as_of.to_string())))),
        up_to: None,
        output: SubscribeOutput::Diffs,
    };
    let params = arguments
        .parameters
        .iter()
        .map(SqlParameter::to_sql_text)
        .collect();
    Ok(SqlRequest::Extended {
        queries: vec![ExtendedRequest {
            query: statement.to_ast_string_stable(),
            params,
        }],
    })
}

impl StreamEnd {
    fn retirement(&self, rows_delivered: u64) -> StatementEndedExecutionReason {
        if self.is_success() {
            StatementEndedExecutionReason::Success {
                result_size: None,
                rows_returned: Some(rows_delivered),
                execution_strategy: None,
            }
        } else if matches!(self, Self::Cancelled) {
            StatementEndedExecutionReason::Canceled
        } else {
            StatementEndedExecutionReason::Errored {
                error: self.to_string(),
            }
        }
    }

    fn sql_result(&self) -> Result<Result<(), ()>, Error> {
        if self.is_success() {
            Ok(Ok(()))
        } else if matches!(self, Self::Cancelled) {
            Err(AdapterError::Canceled.into())
        } else {
            Err(Error::Unstructured(anyhow::anyhow!(self.to_string())))
        }
    }
}

impl From<Error> for StreamEnd {
    fn from(error: Error) -> Self {
        if matches!(error, Error::Adapter(AdapterError::Canceled)) {
            Self::Cancelled
        } else {
            Self::Failed {
                code: -32603,
                message: error.to_string(),
            }
        }
    }
}

impl From<AdapterError> for StreamEnd {
    fn from(error: AdapterError) -> Self {
        Self::from(Error::from(error))
    }
}

fn columns(desc: &RelationDesc) -> Vec<Column> {
    desc.iter()
        .skip(3)
        .map(|(name, typ)| Column {
            name: name.to_string(),
            typ: mz_pgrepr::Type::from(&typ.scalar_type).to_string(),
        })
        .collect()
}

/// Decodes one `SUBSCRIBE ... WITH (PROGRESS)` row into its logical timestamp and, for a data
/// row, the change it carries. A progress row has no change.
fn encode_row(desc: &RelationDesc, row: &RowRef) -> Result<(u64, Option<Change>), Error> {
    let datums = row.iter().collect::<Vec<_>>();
    let logical_timestamp = datums[0]
        .unwrap_numeric()
        .0
        .to_standard_notation_string()
        .parse::<u64>()
        .map_err(|e| Error::Unstructured(e.into()))?;
    if datums[1].unwrap_bool() {
        return Ok((logical_timestamp, None));
    }
    let diff = datums[2].unwrap_int64();
    let count = NonZeroU64::new(diff.unsigned_abs())
        .ok_or_else(|| Error::Unstructured(anyhow::anyhow!("zero subscription multiplicity")))?;
    let values = datums
        .iter()
        .zip_eq(&desc.typ().column_types)
        .skip(3)
        .map(|(d, typ)| {
            let Some(value) = mz_pgrepr::Value::from_datum(*d, &typ.scalar_type) else {
                return Ok(None);
            };
            let mut text = BytesMut::new();
            value.encode_text(&mut text, mz_pgrepr::TextEncodeSettings::STABLE);
            String::from_utf8(text.to_vec())
                .map(Some)
                .map_err(|e| Error::Unstructured(e.into()))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok((
        logical_timestamp,
        Some(Change {
            operation: if diff > 0 {
                Operation::Insert
            } else {
                Operation::Delete
            },
            count,
            row: values,
        }),
    ))
}

/// Changes at one logical timestamp that have not been sent yet.
#[derive(Default)]
struct Batch {
    timestamp: Option<u64>,
    changes: Vec<Change>,
    /// Serialized size of `changes`, counting one separator per change.
    bytes: usize,
}

struct BatchPart {
    timestamp: u64,
    changes: Vec<Change>,
    last: bool,
}

enum BatchAction {
    Deliver(BatchPart),
    Progress(Cursor),
}

struct BatchAssembler {
    batch: Batch,
    max_response_size: usize,
    event_overhead: usize,
}

impl BatchAssembler {
    fn new(max_response_size: usize, event_overhead: usize) -> Self {
        Self {
            batch: Batch::default(),
            max_response_size,
            event_overhead,
        }
    }

    fn take_part(&mut self, last: bool) -> Option<BatchPart> {
        let timestamp = self.batch.timestamp?;
        let changes = std::mem::take(&mut self.batch.changes);
        self.batch.bytes = 0;
        if last {
            self.batch.timestamp = None;
        }
        Some(BatchPart {
            timestamp,
            changes,
            last,
        })
    }

    /// Produces at most one batch part followed by a progress update per row.
    fn push(
        &mut self,
        desc: &RelationDesc,
        row: &RowRef,
    ) -> Result<[Option<BatchAction>; 2], StreamEnd> {
        if row.byte_len() > self.max_response_size {
            return Err(StreamEnd::oversized(
                "subscription row exceeds maximum response size",
            ));
        }
        let (timestamp, change) = encode_row(desc, row)?;
        self.push_decoded(timestamp, change)
    }

    fn push_decoded(
        &mut self,
        timestamp: u64,
        change: Option<Change>,
    ) -> Result<[Option<BatchAction>; 2], StreamEnd> {
        let mut part = if self.batch.timestamp.is_some_and(|t| t < timestamp) {
            self.take_part(true)
        } else {
            None
        };
        let Some(change) = change else {
            return Ok([
                part.map(BatchAction::Deliver),
                Some(BatchAction::Progress(Cursor::At(timestamp))),
            ]);
        };
        let size = serde_json::to_string(&change)?.len() + 1;
        if !self.batch.changes.is_empty()
            && self.event_overhead + self.batch.bytes + size > self.max_response_size
        {
            part = self.take_part(false);
        }
        self.batch.timestamp = Some(timestamp);
        self.batch.bytes += size;
        self.batch.changes.push(change);
        Ok([part.map(BatchAction::Deliver), None])
    }

    fn finish(&mut self) -> Option<BatchAction> {
        self.take_part(true).map(BatchAction::Deliver)
    }
}

struct Sender {
    stream: EventStream<Cursor>,
    /// The resume frontier, reported on activation.
    cursor: Option<Cursor>,
    rows_delivered: u64,
}

impl Sender {
    /// Advances the cursor only once a timestamp's final part is queued.
    async fn apply(&mut self, action: BatchAction, columns: &[Column]) -> Result<(), StreamEnd> {
        match action {
            BatchAction::Deliver(part) => {
                let rows = u64::cast_from(part.changes.len());
                let cursor = part.last.then(|| Cursor::after(part.timestamp));
                let payload = Payload {
                    logical_timestamp: part.timestamp,
                    last: part.last,
                    columns,
                    changes: part.changes,
                };
                self.stream.event(payload, cursor).await?;
                self.rows_delivered += rows;
            }
            // A resumed subscribe first reports an AS OF one less than its cursor, which
            // `advance` ignores.
            BatchAction::Progress(frontier) => self.stream.advance(frontier),
        }
        Ok(())
    }

    async fn deliver(
        &mut self,
        desc: &RelationDesc,
        rx: &mut RecordFirstRowStream,
    ) -> Result<(), StreamEnd> {
        self.stream.activate(self.cursor).await?;
        let columns = columns(desc);
        // The 64 bytes cover fields whose width varies between events: the cursor, the `eventId`
        // sequence number, and the sub-second digits of `timestamp`.
        let event_overhead = self.stream.event_len(Payload {
            logical_timestamp: u64::MAX,
            last: false,
            columns: &columns,
            changes: Vec::new(),
        })? + 64;
        let mut batch = BatchAssembler::new(self.stream.max_response_size, event_overhead);
        loop {
            match self.stream.next(rx.recv()).await? {
                Some(PeekResponseUnary::Rows(mut rows)) => {
                    verify_datum_desc(desc, &mut rows)?;
                    while let Some(row) = rows.next() {
                        // Ready batches can outlive a heartbeat interval without yielding.
                        self.stream.check()?;
                        for action in batch.push(desc, row)?.into_iter().flatten() {
                            self.apply(action, &columns).await?;
                        }
                    }
                }
                Some(PeekResponseUnary::Error(e)) => return Err(e.into()),
                Some(PeekResponseUnary::DependencyDropped(dep)) => {
                    return Err(dep.to_concurrent_dependency_drop().into());
                }
                Some(PeekResponseUnary::Canceled) => return Err(AdapterError::Canceled.into()),
                None => {
                    if let Some(action) = batch.finish() {
                        self.apply(action, &columns).await?;
                    }
                    return Ok(());
                }
            }
        }
    }
}

#[async_trait]
impl ResultSender for Sender {
    async fn add_result(
        &mut self,
        _client: &mut SessionClient,
        res: StatementResult,
    ) -> (
        Result<Result<(), ()>, Error>,
        Option<(StatementEndedExecutionReason, ExecuteContextGuard)>,
    ) {
        let StatementResult::Subscribe {
            desc,
            mut rx,
            ctx_extra,
            ..
        } = res
        else {
            self.stream.reject(self.startup_error(res));
            return (Ok(Err(())), None);
        };
        let result = self.deliver(&desc, &mut rx).await;
        let outcome = self
            .stream
            .record(result.err().unwrap_or(StreamEnd::Completed));
        let retirement = outcome.retirement(self.rows_delivered);
        (outcome.sql_result(), Some((retirement, ctx_extra)))
    }

    fn connection_error(&mut self) -> BoxFuture<'_, Error> {
        let closed = self.stream.closed();
        Box::pin(async {
            closed.await;
            AdapterError::Canceled.into()
        })
    }

    fn allow_subscribe(&self) -> bool {
        true
    }
}

impl Sender {
    fn startup_error(&self, res: StatementResult) -> McpError {
        // A strict `AS OF` below the inputs' since is the only way a resume finds no valid
        // timestamp. The variant is lost in the `SqlError` conversion, so match its message.
        let history_unavailable = AdapterError::ImpossibleTimestampConstraints {
            constraints: String::new(),
        }
        .to_string();
        match res {
            StatementResult::SqlResult(SqlResult::Err { error, .. })
                if self.cursor.is_some() && error.message == history_unavailable =>
            {
                McpError {
                    code: -32011,
                    message: "history unavailable for cursor".into(),
                    data: Some(json!({"reason":"history_unavailable"})),
                }
            }
            StatementResult::SqlResult(SqlResult::Err { error, .. }) => McpError {
                code: if error.code == "42501" {
                    -32012
                } else {
                    -32000
                },
                message: error.message,
                data: None,
            },
            _ => McpError {
                code: -32603,
                message: "expected a SUBSCRIBE result".into(),
                data: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, Row, SqlScalarType};
    use serde_json::Value;

    use super::*;

    #[mz_ore::test]
    fn resume_cursor_has_a_representable_predecessor() {
        for (text, as_of) in [
            ("1", 0),
            ("18446744073709551615", u64::MAX - 1),
            ("18446744073709551616", u64::MAX),
        ] {
            let cursor: ResumeCursor = serde_json::from_value(json!(text)).unwrap();
            assert_eq!(cursor.as_of, as_of);
            assert_eq!(serde_json::to_value(cursor.frontier()).unwrap(), text);
        }
        for value in [
            json!("0"),
            json!("18446744073709551617"),
            json!(1),
            Value::Null,
        ] {
            assert!(
                serde_json::from_value::<ResumeCursor>(value.clone()).is_err(),
                "{value}"
            );
        }
    }

    #[mz_ore::test]
    fn encoding_preserves_positions_and_precision() {
        let desc = RelationDesc::builder()
            .with_column(
                "mz_timestamp",
                SqlScalarType::Numeric { max_scale: None }.nullable(false),
            )
            .with_column("mz_progressed", SqlScalarType::Bool.nullable(false))
            .with_column("mz_diff", SqlScalarType::Int64.nullable(true))
            .with_column("duplicate", SqlScalarType::Int64.nullable(true))
            .with_column("duplicate", SqlScalarType::String.nullable(true))
            .finish();
        let timestamp = "18446744073709551615".parse().unwrap();
        let row = Row::pack([
            Datum::Numeric(timestamp),
            Datum::False,
            Datum::Int64(i64::MIN),
            Datum::Int64(9007199254740993),
            Datum::Null,
        ]);
        let (time, data) = encode_row(&desc, row.as_row_ref()).unwrap();
        assert_eq!(time, u64::MAX);
        let data = serde_json::to_value(data.unwrap()).unwrap();
        assert_eq!(data["operation"], "delete");
        assert_eq!(data["count"], "9223372036854775808");
        assert_eq!(data["row"], json!(["9007199254740993", null]));
        let columns = columns(&desc);
        assert_eq!(columns[0].name, columns[1].name);
        let progress = Row::pack([
            Datum::Numeric(timestamp),
            Datum::True,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ]);
        let (time, data) = encode_row(&desc, progress.as_row_ref()).unwrap();
        assert_eq!(time, u64::MAX);
        assert!(data.is_none());
    }

    fn change() -> Change {
        Change {
            operation: Operation::Insert,
            count: NonZeroU64::new(1).unwrap(),
            row: vec![Some("1".into())],
        }
    }

    #[mz_ore::test]
    fn batch_split_finishes_before_progress_or_eof() {
        for progress in [false, true] {
            let mut batch = BatchAssembler::new(1, 0);
            assert!(
                batch
                    .push_decoded(10, Some(change()))
                    .unwrap()
                    .iter()
                    .all(Option::is_none)
            );
            let [Some(BatchAction::Deliver(part)), None] =
                batch.push_decoded(10, Some(change())).unwrap()
            else {
                panic!()
            };
            assert_eq!(
                (part.timestamp, part.changes.len(), part.last),
                (10, 1, false)
            );
            let final_action = if progress {
                let [part, Some(BatchAction::Progress(frontier))] =
                    batch.push_decoded(20, None).unwrap()
                else {
                    panic!()
                };
                assert_eq!(frontier, Cursor::At(20));
                part
            } else {
                batch.finish()
            };
            let Some(BatchAction::Deliver(part)) = final_action else {
                panic!()
            };
            assert_eq!(
                (part.timestamp, part.changes.len(), part.last),
                (10, 1, true)
            );
            assert!(batch.finish().is_none());
        }
    }
}
