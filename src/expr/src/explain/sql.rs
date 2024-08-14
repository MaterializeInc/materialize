// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS SYNTAX` support for structures defined in this crate.

use std::collections::BTreeMap;

use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_ore::str::Indent;
use mz_repr::explain::sql::DisplaySql;
use mz_repr::explain::PlanRenderingContext;
use mz_repr::GlobalId;
use mz_sql_parser::ast::{
    Cte, CteBlock, Ident, Query, Raw, RawItemName, SetExpr, TableAlias, UnresolvedItemName, Values,
};

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{Id, LocalId, MirRelationExpr, RECURSION_LIMIT};

type SqlQuery = Query<Raw>;

impl<'a, T: 'a> DisplaySql for ExplainSinglePlan<'a, T>
where
    T: DisplaySql<PlanRenderingContext<'a, T>> + Ord,
{
    fn to_sql_query(&self, _ctx: &mut ()) -> Query<Raw> {
        let mut ctx = PlanRenderingContext::new(
            Indent::default(),
            self.context.humanizer,
            self.plan.annotations.clone(),
            self.context.config,
        );

        self.plan.plan.to_sql_query(&mut ctx)
    }
}

impl<'a, T: 'a> DisplaySql for ExplainMultiPlan<'a, T>
where
    T: DisplaySql<PlanRenderingContext<'a, T>> + Ord,
{
    fn to_sql_query(&self, _ctx: &mut ()) -> Query<Raw> {
        todo!()
    }
}

impl DisplaySql<PlanRenderingContext<'_, MirRelationExpr>> for MirRelationExpr {
    fn to_sql_query(&self, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>) -> Query<Raw> {
        MirToSql::new()
            .to_sql_query(self, &mut BTreeMap::new(), ctx)
            .unwrap_or_else(|e| {
                ::tracing::error!("could not convert MIR to SQL: {e:?}");

                // dummy empty select
                Query::<Raw> {
                    ctes: CteBlock::Simple(vec![]),
                    body: SetExpr::Values(Values(vec![])),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                }
            })
    }
}

/// Errors in converting MIR-to-SQL.
#[derive(Clone, Debug)]
enum SqlConversionError {
    UnboundId {
        id: Id,
    },
    BadGlobalName {
        id: GlobalId,
    },
    /// Recursion depth exceeded
    Recursion {
        /// The error that aborted recursion
        error: RecursionLimitError,
    },
}

impl From<RecursionLimitError> for SqlConversionError {
    fn from(error: RecursionLimitError) -> Self {
        SqlConversionError::Recursion { error }
    }
}
struct MirToSql {
    wmr_counter: u64,
    recursion_guard: RecursionGuard,
}

impl<'a> CheckedRecursion for MirToSql {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl MirToSql {
    fn new() -> Self {
        Self {
            wmr_counter: 0,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    fn column_info(
        &self,
        expr: &MirRelationExpr,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Vec<Ident> {
        let Some(attribs) = ctx.annotations.get(expr) else {
            return vec![Ident::new_unchecked("unknown")];
        };

        let Some(names) = &attribs.column_names else {
            let Some(arity) = attribs.arity else {
                return vec![Ident::new_unchecked("unknown")];
            };

            return (0..arity)
                .map(|n| Ident::new_unchecked(format!("unk{n}")))
                .collect();
        };

        names
            .iter()
            .map(|name| Ident::new_unchecked(name))
            .collect()
    }

    fn to_sql_query(
        &mut self,
        expr: &MirRelationExpr,
        bindings: &mut BTreeMap<LocalId, RawItemName>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Query<Raw>, SqlConversionError> {
        use MirRelationExpr::*;

        self.checked_recur_mut(|sc: &mut MirToSql| match expr {
            Constant { rows, typ } => {
                let rows = match rows {
                    Err(_) => vec![],
                    Ok(raw_rows) => raw_rows.into_iter().map(|(r, _)| todo!()).collect(),
                };

                Ok(SqlQuery {
                    ctes: CteBlock::Simple(vec![]),
                    body: SetExpr::Values(Values(rows)),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            Get {
                id: Id::Local(id), ..
            } => {
                // find the corresponding name in our CTEs
                let name = bindings
                    .get(id)
                    .ok_or_else(|| SqlConversionError::UnboundId {
                        id: Id::Local(id.clone()),
                    })?;

                Ok(SqlQuery {
                    ctes: CteBlock::Simple(vec![]),
                    body: SetExpr::Table(name.clone()),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            Get {
                id: Id::Global(id), ..
            } => {
                let name = ctx.humanizer.humanize_id(*id).ok_or_else(|| {
                    SqlConversionError::UnboundId {
                        id: Id::Global(id.clone()),
                    }
                })?;

                let ident =
                    Ident::new(name).map_err(|e| SqlConversionError::BadGlobalName { id: *id })?;

                Ok(SqlQuery {
                    ctes: CteBlock::Simple(Vec::new()),
                    body: SetExpr::Table(RawItemName::Name(UnresolvedItemName(vec![ident]))),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                })
            }
            Let { id, value, body } => {
                let q_value = sc.to_sql_query(value, bindings, ctx)?;

                // prepare the CTE block
                let ident = Ident::new_unchecked(format!("l{id}"));
                let name = RawItemName::Name(UnresolvedItemName(vec![ident.clone()]));
                let columns = sc.column_info(value, ctx);
                let cte_value = Cte::<Raw> {
                    alias: TableAlias {
                        name: ident,
                        columns,
                        strict: false,
                    },
                    id: (),
                    query: q_value,
                };
                // record the name used in binding
                bindings.insert(*id, name);

                let mut q_body = sc.to_sql_query(body, bindings, ctx)?;

                // add the CTE to the query
                match &mut q_body.ctes {
                    CteBlock::Simple(ctes) => {
                        ctes.push(cte_value);
                    }
                    CteBlock::MutuallyRecursive(..) => {
                        let n = sc.wmr_counter;
                        sc.wmr_counter += 1;
                        let ident = Ident::new_unchecked(format!("wmr{n}"));

                        let columns = sc.column_info(body, ctx);

                        q_body = SqlQuery {
                            ctes: CteBlock::Simple(vec![
                                cte_value,
                                Cte {
                                    alias: TableAlias {
                                        name: ident.clone(),
                                        columns,
                                        strict: false,
                                    },
                                    id: (),
                                    query: q_body.take(),
                                },
                            ]),
                            body: SetExpr::Table(RawItemName::Name(UnresolvedItemName(vec![
                                ident,
                            ]))),
                            order_by: Vec::new(),
                            limit: None,
                            offset: None,
                        };
                    }
                }

                Ok(q_body)
            }
            LetRec {
                ids,
                values,
                limits,
                body,
            } => todo!(),
            Project { input, outputs } => todo!(),
            Map { input, scalars } => todo!(),
            FlatMap { input, func, exprs } => todo!(),
            Filter { input, predicates } => todo!(),
            Join {
                inputs,
                equivalences,
                implementation,
            } => todo!(),
            Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => todo!(),
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
            } => todo!(),
            Negate { input } => todo!(),
            Threshold { input } => todo!(),
            Union { base, inputs } => todo!(),
            ArrangeBy { input, keys } => todo!(),
        })
    }
}
