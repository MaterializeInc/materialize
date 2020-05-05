// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! On-disk state.
//!
//! These structures must only be evolved backwards-compatibly, or loading
//! catalogs created by previous versions of Materialize will fail.

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use catalog::{Catalog, CatalogItemSerializer, Index, PlanContext, Sink, Source, View};
use failure::bail;
use ore::collections::CollectionExt;
use repr::Row;
use sql::{Params, Plan};
use transform::Optimizer;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CatalogItem {
    V1 {
        create_sql: String,
        eval_env: Option<EvalEnv>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvalEnv {
    pub logical_time: Option<u64>,
    pub wall_time: Option<DateTime<Utc>>,
}

impl From<EvalEnv> for PlanContext {
    fn from(eval_env: EvalEnv) -> PlanContext {
        PlanContext {
            wall_time: eval_env.wall_time.unwrap_or_else(|| Utc.timestamp(0, 0)),
        }
    }
}

impl From<PlanContext> for EvalEnv {
    fn from(eval_env: PlanContext) -> EvalEnv {
        EvalEnv {
            logical_time: None,
            wall_time: Some(eval_env.wall_time),
        }
    }
}

pub struct SqlSerializer;

impl CatalogItemSerializer for SqlSerializer {
    fn serialize(item: &catalog::CatalogItem) -> Vec<u8> {
        let item = match item {
            catalog::CatalogItem::Source(source) => CatalogItem::V1 {
                create_sql: source.create_sql.clone(),
                eval_env: Some(source.plan_cx.clone().into()),
            },
            catalog::CatalogItem::View(view) => CatalogItem::V1 {
                create_sql: view.create_sql.clone(),
                eval_env: Some(view.plan_cx.clone().into()),
            },
            catalog::CatalogItem::Index(index) => CatalogItem::V1 {
                create_sql: index.create_sql.clone(),
                eval_env: Some(index.plan_cx.clone().into()),
            },
            catalog::CatalogItem::Sink(sink) => CatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
                eval_env: Some(sink.plan_cx.clone().into()),
            },
        };
        serde_json::to_vec(&item).expect("catalog serialization cannot fail")
    }

    fn deserialize(
        catalog: &Catalog,
        bytes: Vec<u8>,
    ) -> Result<catalog::CatalogItem, failure::Error> {
        let CatalogItem::V1 {
            create_sql,
            eval_env,
        } = serde_json::from_slice(&bytes)?;
        let params = Params {
            datums: Row::pack(&[]),
            types: vec![],
        };
        let pcx = match eval_env {
            // Old sources and sinks don't have plan contexts, but it's safe to
            // just give them a default, as they clearly don't depend on the
            // plan context.
            None => PlanContext::default(),
            Some(eval_env) => eval_env.into(),
        };
        let stmt = sql::parse(create_sql)?.into_element();
        let plan = sql::plan(&pcx, catalog, &sql::InternalSession, stmt, &params)?;
        Ok(match plan {
            Plan::CreateSource { source, .. } => catalog::CatalogItem::Source(Source {
                create_sql: source.create_sql,
                plan_cx: pcx,
                connector: source.connector,
                desc: source.desc,
            }),
            Plan::CreateView { view, .. } => {
                let mut optimizer = Optimizer::default();
                catalog::CatalogItem::View(View {
                    create_sql: view.create_sql,
                    plan_cx: pcx,
                    optimized_expr: optimizer.optimize(view.expr, catalog.indexes())?,
                    desc: view.desc,
                })
            }
            Plan::CreateIndex { index, .. } => catalog::CatalogItem::Index(Index {
                create_sql: index.create_sql,
                plan_cx: pcx,
                on: index.on,
                keys: index.keys,
            }),
            Plan::CreateSink { sink, .. } => catalog::CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                plan_cx: pcx,
                from: sink.from,
                connector: catalog::SinkConnectorState::Pending(sink.connector_builder),
            }),
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }
}
