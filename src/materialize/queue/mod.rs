// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Strictly ordered queues.
//!
//! The queues in this module will eventually grow to include queues that
//! provide distribution, replication, and durability. At the moment,
//! only a simple, transient, single-node queue is provided.

use dataflow::DataflowCommand;
use dataflow_types::{Dataflow, RowSetTransformation, Sink, SinkConnector, TailSinkConnector};
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sql::{Plan, Session};
use uuid::Uuid;

pub mod transient;

/// Incoming raw SQL from users.
pub struct Command {
    pub conn_id: u32,
    pub sql: String,
    pub session: sql::Session,
    pub tx: futures::sync::oneshot::Sender<Response>,
}

/// Responses from the queue to SQL commands.
pub struct Response {
    pub sql_result: Result<SqlResponse, failure::Error>,
    pub session: Session,
}

pub fn translate_plan(plan: Plan, conn_id: u32) -> (SqlResponse, Option<DataflowCommand>) {
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
                wait_for: WaitFor::NoOne,
                transform: Default::default(),
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
        Plan::Peek {
            source,
            when,
            transform,
        } => (
            SqlResponse::SendRows {
                typ: source.typ(),
                rows: vec![],
                wait_for: WaitFor::Workers,
                transform: transform.clone(),
            },
            Some(DataflowCommand::Peek {
                conn_id,
                source,
                when,
                transform,
            }),
        ),
        Plan::Tail(source) => (
            SqlResponse::Tailing,
            Some(DataflowCommand::CreateDataflows(vec![Dataflow::Sink(
                Sink {
                    name: format!("<temp_{}>", Uuid::new_v4()),
                    from: (source.name().to_owned(), source.typ().clone()),
                    connector: SinkConnector::Tail(TailSinkConnector { conn_id }),
                },
            )])),
        ),
        Plan::SendRows { typ, rows } => (
            SqlResponse::SendRows {
                typ,
                rows,
                wait_for: WaitFor::NoOne,
                transform: Default::default(),
            },
            None,
        ),
        Plan::ExplainPlan { typ, relation_expr } => (
            SqlResponse::SendRows {
                typ,
                rows: vec![],
                wait_for: WaitFor::Optimizer,
                transform: Default::default(),
            },
            Some(DataflowCommand::Explain {
                conn_id,
                relation_expr,
            }),
        ),
    }
}

/// Flag for whether optimizer or workers will chime in as well.
#[derive(Debug)]
pub enum WaitFor {
    NoOne,
    Optimizer,
    Workers,
}

#[derive(Debug)]
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
        wait_for: WaitFor,
        transform: RowSetTransformation,
    },
    SetVariable,
    Tailing,
}
