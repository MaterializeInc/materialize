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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use catalog::{Catalog, CatalogItemSerializer, Index, Sink, Source, View};
use expr::transform::Optimizer;
use failure::bail;
use ore::collections::CollectionExt;
use ore::future::MaybeFuture;
use repr::Row;
use sql::{Params, Plan};

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

impl From<EvalEnv> for expr::EvalEnv {
    fn from(eval_env: EvalEnv) -> expr::EvalEnv {
        expr::EvalEnv {
            logical_time: eval_env.logical_time,
            wall_time: eval_env.wall_time,
        }
    }
}

impl From<expr::EvalEnv> for EvalEnv {
    fn from(eval_env: expr::EvalEnv) -> EvalEnv {
        EvalEnv {
            logical_time: eval_env.logical_time,
            wall_time: eval_env.wall_time,
        }
    }
}

pub struct SqlSerializer;

impl CatalogItemSerializer for SqlSerializer {
    fn serialize(item: &catalog::CatalogItem) -> Vec<u8> {
        let item = match item {
            catalog::CatalogItem::Source(source) => CatalogItem::V1 {
                create_sql: source.create_sql.clone(),
                eval_env: None,
            },
            catalog::CatalogItem::View(view) => CatalogItem::V1 {
                create_sql: view.create_sql.clone(),
                eval_env: Some(view.eval_env.clone().into()),
            },
            catalog::CatalogItem::Index(index) => CatalogItem::V1 {
                create_sql: index.create_sql.clone(),
                eval_env: Some(index.eval_env.clone().into()),
            },
            catalog::CatalogItem::Sink(sink) => CatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
                eval_env: None,
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
        let stmt = sql::parse(create_sql)?.into_element();
        let plan = match sql::plan(catalog, &sql::InternalSession, stmt, &params) {
            MaybeFuture::Immediate(Some(Ok(plan))) => plan,
            MaybeFuture::Immediate(Some(Err(e))) => return Err(e),
            MaybeFuture::Immediate(None) => unreachable!(),
            MaybeFuture::Future(f) => futures::executor::block_on(async {
                // TODO(benesch): the whole point of MaybeFuture is to indicate
                // when a maybe-asynchronous task is ready immediately, and yet
                // here we find ourselves polling the future variant because we
                // humans know statically that it is ready immediately, even
                // though Rust does not. Rework so that the type system can
                // enforce this invariant.
                use std::task::Poll;
                match futures::poll!(f) {
                    Poll::Ready(res) => res,
                    Poll::Pending => {
                        bail!("catalog entry inappropriately depends on external state")
                    }
                }
            })?,
        };
        Ok(match plan {
            Plan::CreateSource { source, .. } => catalog::CatalogItem::Source(Source {
                create_sql: source.create_sql,
                connector: source.connector,
                desc: source.desc,
            }),
            Plan::CreateView { view, .. } => {
                let mut optimizer = Optimizer::default();
                let eval_env = match eval_env {
                    None => bail!("view missing eval env"),
                    Some(eval_env) => eval_env.into(),
                };
                catalog::CatalogItem::View(View {
                    create_sql: view.create_sql,
                    expr: optimizer.optimize(view.expr, catalog.indexes(), &eval_env),
                    eval_env,
                    desc: view.desc,
                })
            }
            Plan::CreateIndex { index, .. } => catalog::CatalogItem::Index(Index {
                create_sql: index.create_sql,
                on: index.on,
                keys: index.keys,
                eval_env: match eval_env {
                    None => bail!("index missing eval env"),
                    Some(eval_env) => eval_env.into(),
                },
            }),
            Plan::CreateSink { sink, .. } => catalog::CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connector: sink.connector,
            }),
            _ => bail!("catalog entry generated inappropriate plan"),
        })
    }
}
