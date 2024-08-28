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
    Cte, CteBlock, Expr, Ident, JoinConstraint, JoinOperator, Query, Raw, RawItemName, Select,
    SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, UnresolvedItemName, Value,
    Values,
};

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{Id, LocalId, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};

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
    UnexpectedWMR,
    UnexpectedNegation,
    UnexpectedThreshold,
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
            Constant { rows, typ } => {
                let rows = match rows {
                    Err(_) => vec![],
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

                let ident =
                    Ident::new(name).map_err(|e| SqlConversionError::BadGlobalName { id: *id })?;

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
                    new_select_items.push(SelectItem::Expr {
                        expr: sc.to_sql_expr(expr, bindings, ctx)?,
                        alias: Some(columns[num_inner_columns + col].clone()),
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
            FlatMap { input, func, exprs } => todo!(),
            Filter { input, predicates } => {
                let mut q = sc.to_sql_query(input, bindings, ctx)?;
                if predicates.is_empty() {
                    return Ok(q);
                }

                let mut predicates = predicates.into_iter();
                let mut selection = sc.to_sql_expr(predicates.next().unwrap(), bindings, ctx)?;
                for expr in predicates {
                    selection = selection.and(sc.to_sql_expr(&expr, bindings, ctx)?);
                }

                match &mut q.body {
                    SetExpr::Select(select) => {
                        select.selection = Some(selection);
                    }
                    body => {
                        // extract the body and add columns in a separate query
                        let ident = sc.fresh_ident("filter");
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

                let projection = q_columns
                    .concat()
                    .into_iter()
                    .map(|i| SelectItem::Expr {
                        expr: Expr::Identifier(vec![i.clone()]),
                        alias: Some(i),
                    })
                    .collect();

                // TODO(mgree): rather than giving ON(true) and giving a where clause, move these equivalences into the join itself
                let mut selection: Option<Expr<Raw>> = None;
                for equivalence in equivalences {
                    if let Some(conjunct) =
                        sc.equivalence_to_conjunct(equivalence, bindings, ctx)?
                    {
                        if let Some(sel) = selection {
                            selection = Some(sel.and(conjunct))
                        } else {
                            selection = Some(conjunct);
                        }
                    }
                }

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
            Union { base, inputs } => todo!(),
            ArrangeBy { input, keys: _keys } => sc.to_sql_query(input, bindings, ctx),
            // Negate forms are handled under `Union` but not elsewhere (SQL doesn't have negative cardinalities!)
            Negate { input: _input } => Err(SqlConversionError::UnexpectedNegation),
            Threshold { input } => Err(SqlConversionError::UnexpectedThreshold),
            LetRec {
                ids: _ids,
                values: _values,
                limits: _limits,
                body: _body,
            } => Err(SqlConversionError::UnexpectedWMR),
        })
    }

    fn equivalence_to_conjunct(
        &mut self,
        exprs: &Vec<MirScalarExpr>,
        bindings: &mut BTreeMap<LocalId, RawItemName>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Option<Expr<Raw>>, SqlConversionError> {
        let mut iter = exprs.into_iter();
        let Some(lhs) = iter.next() else {
            return Ok(None);
        };
        let Some(rhs) = iter.next() else {
            return Ok(None);
        };

        let canonical = self.to_sql_expr(lhs, bindings, ctx)?;

        let right = self.to_sql_expr(rhs, bindings, ctx)?;
        let mut equiv = Expr::equals(canonical.clone(), right);

        for rhs in iter {
            let right = self.to_sql_expr(rhs, bindings, ctx)?;
            equiv = equiv.and(canonical.clone().equals(right));
        }

        Ok(Some(equiv))
    }

    fn to_sql_expr(
        &mut self,
        expr: &MirScalarExpr,
        bindings: &mut BTreeMap<LocalId, RawItemName>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Expr<Raw>, SqlConversionError> {
        todo!()
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
            Datum::Date(_) => todo!(),
            Datum::Time(_) => todo!(),
            Datum::Timestamp(_) => todo!(),
            Datum::TimestampTz(_) => todo!(),
            Datum::Interval(i) => todo!(),
            Datum::Bytes(_) => todo!(),
            Datum::String(s) => Expr::Value(Value::String(s.to_string())),
            Datum::Array(_) => todo!(),
            Datum::List(_) => todo!(),
            Datum::Map(_) => todo!(),
            Datum::Numeric(n) => Expr::Value(Value::Number(n.to_string())),
            Datum::JsonNull => todo!(),
            Datum::Uuid(_) => todo!(),
            Datum::MzTimestamp(_) => todo!(),
            Datum::Range(_) => todo!(),
            Datum::MzAclItem(_) => todo!(),
            Datum::AclItem(_) => todo!(),
            Datum::Dummy => Expr::Value(Value::String("!!!DUMMY!!!".to_string())),
            Datum::Null => Expr::null(),
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
