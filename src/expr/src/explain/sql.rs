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

use itertools::Itertools;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_ore::str::Indent;
use mz_repr::explain::sql::DisplaySql;
use mz_repr::explain::PlanRenderingContext;
use mz_repr::{Datum, GlobalId};
use mz_sql_parser::ast::{
    Cte, CteBlock, Expr, Ident, IdentError, JoinConstraint, JoinOperator, Query, Raw, RawItemName,
    Select, SelectItem, SelectOption, SelectOptionName, SetExpr, TableAlias, TableFactor,
    TableWithJoins, UnresolvedItemName, Value, Values, WithOptionValue,
};

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{
    AggregateExpr, AggregateFunc, EvalError, Id, LocalId, MirRelationExpr, MirScalarExpr,
    RECURSION_LIMIT,
};

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
#[allow(dead_code)]
enum SqlConversionError {
    UnboundId {
        id: Id,
    },
    BadConstant {
        err: EvalError,
    },
    BadGlobalName {
        id: GlobalId,
        err: IdentError,
    },
    UnexpectedWMR,
    UnexpectedNegation,
    UnexpectedThreshold,
    UnexpectedMixedDistinctReduce,
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
    fresh_name_counter: u64,
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
            fresh_name_counter: 0,
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    fn fresh_ident(&mut self, s: &str) -> Ident {
        let n = self.fresh_name_counter;
        self.fresh_name_counter += 1;
        Ident::new_unchecked(format!("{s}{n}"))
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
            Constant { rows, .. } => {
                let rows = match rows {
                    Err(err) => return Err(SqlConversionError::BadConstant { err: err.clone() }),
                    Ok(raw_rows) => raw_rows
                        .into_iter()
                        .map(|(row, _id)| {
                            row.unpack()
                                .into_iter()
                                .map(|datum| sc.to_sql_value(datum))
                                .collect()
                        })
                        .collect(),
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

                let ident = Ident::new(name)
                    .map_err(|err| SqlConversionError::BadGlobalName { id: *id, err })?;

                //
                Ok(SqlQuery {
                    ctes: CteBlock::Simple(Vec::new()),
                    body: SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection: sc
                            .column_info(expr, ctx)
                            .into_iter()
                            .map(|i| SelectItem::Expr {
                                expr: Expr::Identifier(vec![i.clone()]),
                                alias: Some(i),
                            })
                            .collect(),
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: None,
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    })),
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
                let body_columns = sc.column_info(body, ctx);

                sc.add_cte_to_query(&mut q_body, body_columns, cte_value);

                Ok(q_body)
            }
            Project { input, outputs } => {
                let mut q = sc.to_sql_query(input, bindings, ctx)?;

                match &mut q.body {
                    SetExpr::Select(select) => {
                        // winnow the existing projection
                        let mut new_projection = Vec::with_capacity(outputs.len());

                        for col in outputs {
                            new_projection.push(select.projection[*col].clone());
                        }

                        select.projection = new_projection;
                    }
                    body => {
                        // extract the body and project in a separate query
                        let ident = sc.fresh_ident("proj");
                        let columns = sc.column_info(input, ctx);

                        let cte = Cte::<Raw> {
                            alias: TableAlias {
                                name: ident.clone(),
                                columns: columns.clone(),
                                strict: false,
                            },
                            id: (),
                            query: SqlQuery {
                                ctes: CteBlock::Simple(vec![]),
                                body: body.take(),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                            },
                        };

                        sc.add_cte_to_query(&mut q, columns.clone(), cte);

                        let mut projection = Vec::with_capacity(outputs.len());
                        for col in outputs {
                            let i = &columns[*col];

                            projection.push(SelectItem::Expr {
                                expr: Expr::Identifier(vec![i.clone()]),
                                alias: Some(i.clone()),
                            });
                        }

                        q.body = SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        }))
                    }
                }

                Ok(q)
            }
            Map { input, scalars } => {
                let mut q = sc.to_sql_query(input, bindings, ctx)?;
                let inner_columns = sc.column_info(input, ctx);
                let num_inner_columns = inner_columns.len();

                let columns = sc.column_info(expr, ctx);
                let mut new_select_items = Vec::with_capacity(scalars.len());
                for (col, expr) in scalars.iter().enumerate() {
                    let cols_so_far = num_inner_columns + col;
                    new_select_items.push(SelectItem::Expr {
                        expr: sc.to_sql_expr(expr, &columns[0..cols_so_far])?,
                        alias: Some(columns[cols_so_far].clone()),
                    });
                }

                match &mut q.body {
                    SetExpr::Select(select) => {
                        select.projection.extend(new_select_items.into_iter());
                    }
                    body => {
                        // extract the body and add columns in a separate query
                        let ident = sc.fresh_ident("map");

                        let cte = Cte::<Raw> {
                            alias: TableAlias {
                                name: ident.clone(),
                                columns: inner_columns.clone(),
                                strict: false,
                            },
                            id: (),
                            query: SqlQuery {
                                ctes: CteBlock::Simple(vec![]),
                                body: body.take(),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                            },
                        };

                        sc.add_cte_to_query(&mut q, inner_columns.clone(), cte);

                        let mut projection: Vec<SelectItem<Raw>> = inner_columns
                            .into_iter()
                            .map(|i| SelectItem::Expr {
                                expr: Expr::Identifier(vec![i.clone()]),
                                alias: Some(i),
                            })
                            .collect();
                        projection.extend(new_select_items.into_iter());

                        q.body = SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        }))
                    }
                }

                Ok(q)
            }
            FlatMap {
                input: _,
                func: _,
                exprs: _,
            } => unimplemented!("MIR-to-SQL FlatMap"),
            Filter { input, predicates } => {
                let mut q = sc.to_sql_query(input, bindings, ctx)?;
                if predicates.is_empty() {
                    return Ok(q);
                }

                let columns = sc.column_info(input, ctx);
                let mut predicates = predicates.into_iter();
                let mut selection = sc.to_sql_expr(predicates.next().unwrap(), &columns)?;
                for expr in predicates {
                    selection = selection.and(sc.to_sql_expr(&expr, &columns)?);
                }

                match &mut q.body {
                    SetExpr::Select(select) => {
                        select.selection = Some(selection);
                    }
                    body => {
                        // extract the body and add columns in a separate query
                        let ident = sc.fresh_ident("filter");

                        let cte = Cte::<Raw> {
                            alias: TableAlias {
                                name: ident.clone(),
                                columns: columns.clone(),
                                strict: false,
                            },
                            id: (),
                            query: SqlQuery {
                                ctes: CteBlock::Simple(vec![]),
                                body: body.take(),
                                order_by: vec![],
                                limit: None,
                                offset: None,
                            },
                        };

                        sc.add_cte_to_query(&mut q, columns.clone(), cte);

                        q.body = SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection: columns
                                .into_iter()
                                .map(|i| SelectItem::Expr {
                                    expr: Expr::Identifier(vec![i.clone()]),
                                    alias: Some(i),
                                })
                                .collect(),
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: Some(selection),
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        }))
                    }
                }

                Ok(q)
            }
            Join {
                inputs,
                equivalences,
                implementation,
            } => {
                let num_inputs = inputs.len();

                if num_inputs == 0 {
                    return Ok(SqlQuery {
                        ctes: CteBlock::Simple(vec![]),
                        body: SetExpr::Values(Values(vec![])),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    });
                }

                if num_inputs == 1 {
                    return sc.to_sql_query(&inputs[0], bindings, ctx);
                }

                let mut q_inputs = Vec::with_capacity(num_inputs);
                let mut q_columns = Vec::with_capacity(num_inputs);
                for input in inputs.into_iter() {
                    let q = sc.to_sql_query(input, bindings, ctx)?;
                    q_inputs.push(q);
                    q_columns.push(sc.column_info(input, ctx));
                }
                let columns = q_columns.clone().concat();
                assert!(columns == sc.column_info(expr, ctx));

                let q_idents: Vec<Ident> =
                    (0..num_inputs).map(|_| sc.fresh_ident("join")).collect();

                let (first, rest): (_, Vec<_>) = match implementation {
                    crate::JoinImplementation::Differential((start, _, _), order) => {
                        (*start, order.into_iter().map(|(idx, _, _)| *idx).collect())
                    }
                    crate::JoinImplementation::DeltaQuery(orders) => (
                        orders[0][0].0,
                        orders[0][1..].into_iter().map(|(idx, _, _)| *idx).collect(),
                    ),
                    crate::JoinImplementation::IndexedFilter(_, _, _, _)
                    | crate::JoinImplementation::Unimplemented => {
                        (0, (1..q_inputs.len()).collect())
                    }
                };

                let from = vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: RawItemName::Name(UnresolvedItemName(vec![q_idents[first].clone()])),
                        alias: None,
                    },
                    joins: rest
                        .into_iter()
                        .map(|idx| mz_sql_parser::ast::Join {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![
                                    q_idents[idx].clone()
                                ])),
                                alias: None,
                            },
                            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::Value(
                                Value::Boolean(true),
                            ))),
                        })
                        .collect(),
                }];

                // TODO(mgree): would be nice to denest these
                // TODO(mgree): generate CTEs in same order as from
                let ctes = CteBlock::Simple(
                    q_inputs
                        .into_iter()
                        .enumerate()
                        .map(|(i, query)| Cte {
                            alias: TableAlias {
                                name: q_idents[i].clone(),
                                columns: q_columns[i].clone(),
                                strict: false,
                            },
                            id: (),
                            query,
                        })
                        .collect(),
                );

                // TODO(mgree): rather than giving ON(true) and giving a where clause, move these equivalences into the join itself
                let mut selection: Option<Expr<Raw>> = None;
                for equivalence in equivalences {
                    if let Some(conjunct) = sc.equivalence_to_conjunct(equivalence, &columns)? {
                        if let Some(sel) = selection {
                            selection = Some(sel.and(conjunct))
                        } else {
                            selection = Some(conjunct);
                        }
                    }
                }

                let projection = columns
                    .into_iter()
                    .map(|i| SelectItem::Expr {
                        expr: Expr::Identifier(vec![i.clone()]),
                        alias: Some(i),
                    })
                    .collect();

                let body = SetExpr::Select(Box::new(Select {
                    distinct: None,
                    projection,
                    from,
                    selection,
                    group_by: vec![],
                    having: None,
                    options: vec![],
                }));

                Ok(Query {
                    ctes,
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                })
            }
            Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _monotonic,
                expected_group_size,
            } => {
                let mut q = sc.to_sql_query(input, bindings, ctx)?;

                let outer_columns = sc.column_info(expr, ctx);
                assert!(outer_columns.len() == group_key.len() + aggregates.len());
                let columns = sc.column_info(input, ctx);

                // !!! selects group_key followed by each aggregate
                let mut group_by = Vec::with_capacity(group_key.len());
                let mut projection = Vec::with_capacity(outer_columns.len());

                for (gk, ident) in group_key.iter().zip(outer_columns.iter()) {
                    let expr = sc.to_sql_expr(gk, &columns)?;
                    group_by.push(expr.clone());
                    projection.push(SelectItem::Expr {
                        expr,
                        alias: Some(ident.clone()),
                    });
                }

                let mut shared_distinct_value = None;
                for (agg, ident) in aggregates
                    .iter()
                    .zip(outer_columns.iter().dropping(group_key.len()))
                {
                    if let Some(distinct) = shared_distinct_value {
                        if agg.distinct != distinct {
                            // TODO(mgree) if a reduce mixes distinct and non-distinct aggregates, we need to generate separate reduces and a join (i think???)
                            return Err(SqlConversionError::UnexpectedMixedDistinctReduce);
                        }
                    } else {
                        shared_distinct_value = Some(agg.distinct);
                    }

                    let expr = sc.agg_to_sql_expr(agg, &columns)?;
                    projection.push(SelectItem::Expr {
                        expr,
                        alias: Some(ident.clone()),
                    });
                }

                let body = &mut q.body;

                // extract the body and add columns in a separate query
                let ident = sc.fresh_ident("reduce");

                let cte = Cte::<Raw> {
                    alias: TableAlias {
                        name: ident.clone(),
                        columns: columns.clone(),
                        strict: false,
                    },
                    id: (),
                    query: SqlQuery {
                        ctes: CteBlock::Simple(vec![]),
                        body: body.take(),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                };

                sc.add_cte_to_query(&mut q, columns.clone(), cte);

                let options = expected_group_size.map_or_else(
                    || vec![],
                    |egs| {
                        vec![SelectOption {
                            name: SelectOptionName::ExpectedGroupSize,
                            value: Some(WithOptionValue::Value(Value::Number(egs.to_string()))),
                        }]
                    },
                );

                q.body = SetExpr::Select(Box::new(Select {
                    distinct: None,
                    projection,
                    from: vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                            alias: None,
                        },
                        joins: vec![],
                    }],
                    selection: None,
                    group_by,
                    having: None,
                    options,
                }));

                Ok(q)
            }
            TopK {
                input: _,
                group_key: _,
                order_key: _,
                limit: _,
                offset: _,
                monotonic: _,
                expected_group_size: _,
            } => unimplemented!("MIR-to-SQL TopK"),
            Union { base: _, inputs: _ } => unimplemented!("MIR-to-SQL Union"),
            ArrangeBy { input, keys: _keys } => sc.to_sql_query(input, bindings, ctx),
            // Negate forms are handled under `Union` but not elsewhere (SQL doesn't have negative cardinalities!)
            Negate { input: _ } => Err(SqlConversionError::UnexpectedNegation),
            // Threshold forms are handled under `Union`'s LEFT JOIN detection but not elsewhere (SQL doesn't have negative cardinalities!)
            Threshold { input: _ } => Err(SqlConversionError::UnexpectedThreshold),
            LetRec {
                ids: _,
                values: _,
                limits: _,
                body: _,
            } => Err(SqlConversionError::UnexpectedWMR),
        })
    }

    fn equivalence_to_conjunct(
        &mut self,
        exprs: &Vec<MirScalarExpr>,
        columns: &[Ident],
    ) -> Result<Option<Expr<Raw>>, SqlConversionError> {
        let mut iter = exprs.into_iter();
        let Some(lhs) = iter.next() else {
            return Ok(None);
        };
        let Some(rhs) = iter.next() else {
            return Ok(None);
        };

        let canonical = self.to_sql_expr(lhs, columns)?;

        let right = self.to_sql_expr(rhs, columns)?;
        let mut equiv = Expr::equals(canonical.clone(), right);

        for rhs in iter {
            let right = self.to_sql_expr(rhs, columns)?;
            equiv = equiv.and(canonical.clone().equals(right));
        }

        Ok(Some(equiv))
    }

    fn to_sql_expr(
        &mut self,
        expr: &MirScalarExpr,
        columns: &[Ident],
    ) -> Result<Expr<Raw>, SqlConversionError> {
        use MirScalarExpr::*;
        fn call<S: ToString>(f: S, args: Vec<Expr<Raw>>) -> Expr<Raw> {
            Expr::call(
                RawItemName::Name(UnresolvedItemName(vec![Ident::new_unchecked(
                    f.to_string(),
                )])),
                args,
            )
        }

        match expr {
            Column(col) => Ok(Expr::Identifier(vec![columns[*col].clone()])),
            Literal(Ok(row), _) => Ok(Expr::Row {
                exprs: row
                    .unpack()
                    .into_iter()
                    .map(|datum| self.to_sql_value(datum))
                    .collect(),
            }),
            Literal(Err(err), _) => Err(SqlConversionError::BadConstant { err: err.clone() }),
            CallUnmaterializable(uf) => Ok(call(uf, vec![])),
            CallUnary { func, expr } => {
                let arg = self.to_sql_expr(expr, columns)?;
                Ok(call(func, vec![arg]))
            }
            CallBinary { func, expr1, expr2 } => {
                let arg1 = self.to_sql_expr(expr1, columns)?;
                let arg2 = self.to_sql_expr(expr2, columns)?;
                Ok(call(func, vec![arg1, arg2]))
            }
            CallVariadic { func, exprs } => {
                let mut args = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    args.push(self.to_sql_expr(expr, columns)?);
                }
                Ok(call(func, args))
            }
            If { cond, then, els } => Ok(Expr::Case {
                operand: None,
                conditions: vec![self.to_sql_expr(cond, columns)?],
                results: vec![self.to_sql_expr(then, columns)?],
                else_result: Some(Box::new(self.to_sql_expr(els, columns)?)),
            }),
        }
    }

    fn agg_to_sql_expr(
        &mut self,
        expr: &AggregateExpr,
        columns: &[Ident],
    ) -> Result<Expr<Raw>, SqlConversionError> {
        let arg = self.to_sql_expr(&expr.expr, columns)?;

        use AggregateFunc::*;
        match expr.func {
            Any
            | All
            | JsonbAgg { .. }
            | JsonbObjectAgg { .. }
            | MapAgg { .. }
            | ArrayConcat { .. }
            | ListConcat { .. }
            | StringAgg { .. }
            | RowNumber { .. }
            | Rank { .. }
            | DenseRank { .. }
            | LagLead { .. }
            | FirstValue { .. }
            | LastValue { .. }
            | WindowAggregate { .. }
            | Dummy { .. } => unimplemented!("MIR-to-SQL AggregateFunc"),
            _ => (),
        };

        let name = RawItemName::Name(UnresolvedItemName(vec![Ident::new_unchecked(
            expr.func.name(),
        )]));

        Ok(Expr::call(name, vec![arg]))
    }

    fn to_sql_value(&self, datum: Datum) -> Expr<Raw> {
        match datum {
            Datum::False => Expr::Value(Value::Boolean(false)),
            Datum::True => Expr::Value(Value::Boolean(true)),
            Datum::Int16(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::Int32(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::Int64(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::UInt8(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::UInt16(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::UInt32(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::UInt64(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::Float32(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::Float64(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::String(s) => Expr::Value(Value::String(s.to_string())),
            Datum::Numeric(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::Dummy => Expr::Value(Value::String("!!!DUMMY!!!".to_string())),
            Datum::Null => Expr::null(),
            Datum::Date(_)
            | Datum::Time(_)
            | Datum::Timestamp(_)
            | Datum::TimestampTz(_)
            | Datum::Interval(_)
            | Datum::Bytes(_)
            | Datum::Array(_)
            | Datum::List(_)
            | Datum::Map(_)
            | Datum::JsonNull
            | Datum::Uuid(_)
            | Datum::MzTimestamp(_)
            | Datum::Range(_)
            | Datum::MzAclItem(_)
            | Datum::AclItem(_) => unimplemented!("MIR-to-SQL esoteric Datum"),
        }
    }

    fn add_cte_to_query(&mut self, query: &mut Query<Raw>, columns: Vec<Ident>, cte: Cte<Raw>) {
        match &mut query.ctes {
            CteBlock::Simple(ctes) => {
                ctes.push(cte);
            }
            CteBlock::MutuallyRecursive(..) => {
                let ident = self.fresh_ident("wmr");

                *query = SqlQuery {
                    ctes: CteBlock::Simple(vec![
                        cte,
                        Cte {
                            alias: TableAlias {
                                name: ident.clone(),
                                columns: columns.clone(),
                                strict: false,
                            },
                            id: (),
                            query: query.take(),
                        },
                    ]),
                    body: SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection: columns
                            .into_iter()
                            .map(|i| SelectItem::Expr {
                                expr: Expr::Identifier(vec![i.clone()]),
                                alias: Some(i),
                            })
                            .collect(),
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![ident])),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: None,
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    })),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                };
            }
        }
    }
}
