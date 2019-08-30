// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Strictly ordered queues.
//!
//! The queues in this module will eventually grow to include queues that
//! provide distribution, replication, and durability. At the moment,
//! only a simple, transient, single-node queue is provided.

use comm::Switchboard;
use dataflow::DataflowCommand;
use dataflow_types::{Dataflow, Exfiltration, Sink, SinkConnector, TailSinkConnector};
use futures::Stream;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sql::{Plan, Session};
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

pub mod transient;

/// Incoming raw SQL from users.
pub struct Command {
    pub conn_id: u32,
    pub sql: String,
    pub session: Session,
    pub tx: futures::sync::oneshot::Sender<Response>,
}

/// Responses from the queue to SQL commands.
pub struct Response {
    pub sql_result: Result<SqlResponse, failure::Error>,
    pub session: sql::Session,
}

pub fn translate_plan<C>(
    plan: Plan,
    switchboard: &mut Switchboard<C>,
    num_timely_workers: usize,
) -> (SqlResponse, Option<DataflowCommand>)
where
    C: AsyncRead + AsyncWrite + Send + 'static,
{
    match plan {
        Plan::CreateSource(source) => (
            SqlResponse::CreatedSource,
            Some(DataflowCommand::CreateDataflows(vec![Dataflow::Source(
                source,
            )])),
        ),
        Plan::CreateSources(sources) => (
            SqlResponse::SendRows {
                typ: RelationType::new(vec![ColumnType::new(ScalarType::String).name("Topic")]),
                rows: sources
                    .iter()
                    .map(|s| vec![Datum::from(s.name.to_owned())])
                    .collect(),
                rx: Box::new(futures::stream::empty()),
            },
            Some(DataflowCommand::CreateDataflows(
                sources.into_iter().map(|s| Dataflow::Source(s)).collect(),
            )),
        ),
        Plan::CreateSink(sink) => (
            SqlResponse::CreatedSink,
            Some(DataflowCommand::CreateDataflows(vec![Dataflow::Sink(sink)])),
        ),
        Plan::CreateView(view) => (
            SqlResponse::CreatedView,
            Some(DataflowCommand::CreateDataflows(vec![Dataflow::View(view)])),
        ),
        Plan::DropSources(names) => (
            // Though the plan specifies a number of sources to drop, multiple
            // sources can only be dropped via DROP SOURCE root CASCADE, so
            // the tagline is still singular, as in "DROP SOURCE".
            SqlResponse::DroppedSource,
            Some(DataflowCommand::DropDataflows(names)),
        ),
        Plan::DropViews(names) => (
            // See note in `DropSources` about the conversion from plural to
            // singular.
            SqlResponse::DroppedView,
            Some(DataflowCommand::DropDataflows(names)),
        ),
        Plan::EmptyQuery => (SqlResponse::EmptyQuery, None),
        Plan::DidSetVariable => (SqlResponse::SetVariable, None),
        Plan::Peek { source, when } => {
            let (tx, rx) = switchboard.mpsc();
            (
                SqlResponse::SendRows {
                    typ: source.typ(),
                    rows: vec![],
                    rx: Box::new(rx.take(num_timely_workers as u64)),
                },
                Some(DataflowCommand::Peek { tx, source, when }),
            )
        }
        Plan::Tail(source) => {
            let (tx, rx) = switchboard.mpsc();
            (
                SqlResponse::Tailing { rx: Box::new(rx) },
                Some(DataflowCommand::CreateDataflows(vec![Dataflow::Sink(
                    Sink {
                        name: format!("<temp_{}>", Uuid::new_v4()),
                        from: (source.name().to_owned(), source.typ().clone()),
                        connector: SinkConnector::Tail(TailSinkConnector { tx }),
                    },
                )])),
            )
        }
        Plan::SendRows { typ, rows } => (
            SqlResponse::SendRows {
                typ,
                rows,
                rx: Box::new(futures::stream::empty()),
            },
            None,
        ),
        Plan::ExplainPlan { typ, relation_expr } => {
            let (tx, rx) = switchboard.mpsc();
            (
                SqlResponse::SendRows {
                    typ,
                    rows: vec![],
                    // Only the coordinator responds to this.
                    //
                    // TODO(benesch): this module should be responsible for
                    // generating the explain output.
                    rx: Box::new(rx.take(1)),
                },
                Some(DataflowCommand::Explain { tx, relation_expr }),
            )
        }
    }
}

/// Responses from the planner to SQL commands.
pub enum SqlResponse {
    CreatedSink,
    CreatedSource,
    CreatedView,
    DroppedSource,
    DroppedView,
    EmptyQuery,
    SendRows {
        typ: RelationType,
        rows: Vec<Vec<Datum>>,
        rx: Box<dyn Stream<Item = Exfiltration, Error = bincode::Error> + Send>,
    },
    SetVariable,
    Tailing {
        rx: Box<dyn Stream<Item = Exfiltration, Error = bincode::Error> + Send>,
    },
}
