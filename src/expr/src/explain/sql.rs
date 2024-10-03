// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS SQL` support for structures defined in this crate.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_ore::str::Indent;
use mz_repr::explain::sql::DisplaySql;
use mz_repr::explain::PlanRenderingContext;
use mz_repr::{Datum, GlobalId, ScalarType};
use mz_sql_parser::ast::{
    Cte, CteBlock, Distinct, Expr, Ident, IdentError, JoinConstraint, JoinOperator, Limit,
    OrderByExpr, Query, Raw, RawDataType, RawItemName, Select, SelectItem, SelectOption,
    SelectOptionName, SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins,
    UnresolvedItemName, Value, Values, WithOptionValue,
};

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::{
    AggregateExpr, AggregateFunc, ColumnOrder, EvalError, Id, LocalId, MirRelationExpr,
    MirScalarExpr, RECURSION_LIMIT,
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
        if self.plans.is_empty() {
            ::tracing::error!("could not convert MIR to SQL: empty ExplainMultiPlan");

            // dummy empty select
            return Query::<Raw> {
                ctes: CteBlock::Simple(vec![]),
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
            };
        }

        let mut ctes = Vec::with_capacity(self.plans.len());
        for (id, plan) in self.plans.iter() {
            let mut ctx = PlanRenderingContext::new(
                Indent::default(),
                self.context.humanizer,
                plan.annotations.clone(),
                self.context.config,
            );

            let query = plan.plan.to_sql_query(&mut ctx);

            ctes.push(Cte {
                alias: TableAlias {
                    name: Ident::new_unchecked(id),
                    columns: vec![],
                    strict: false,
                },
                id: (),
                query,
            });
        }

        let query = ctes.pop().unwrap().query;

        let CteBlock::Simple(last_ctes) = query.ctes else {
            unimplemented!("unexpected WMR\n{:?}", query.ctes);
        };
        ctes.extend(last_ctes);

        SqlQuery {
            ctes: CteBlock::Simple(ctes),
            body: query.body,
            order_by: query.order_by,
            limit: query.limit,
            offset: query.offset,
        }
    }
}

impl DisplaySql<PlanRenderingContext<'_, MirRelationExpr>> for MirRelationExpr {
    fn to_sql_query(&self, ctx: &mut PlanRenderingContext<'_, MirRelationExpr>) -> Query<Raw> {
        MirToSql::new().to_sql_query(self, ctx).unwrap_or_else(|e| {
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
    UnexpectedFullyNegatedUnion,
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
    // vec![(name_0, columns_0, query_0), ..., (name_n-1, columns_n-1, query_n-1)]
    //
    // is equivalent to
    //
    // WITH name_0 (columns_0) (query_0)
    // ...
    // WITH name_n-1 (columns_n-2) (query_n-2)
    // (query_n-1)
    query: Vec<(Ident, Vec<Ident>, PreQuery)>,
    fresh_name_counter: u64,
    recursion_guard: RecursionGuard,
}

impl CheckedRecursion for MirToSql {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl MirToSql {
    fn new() -> Self {
        Self {
            query: vec![],
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
        ctx: &PlanRenderingContext<'_, MirRelationExpr>,
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

        names.iter().map(Ident::new_unchecked).collect()
    }

    fn to_sql_query(
        &mut self,
        expr: &MirRelationExpr,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Query<Raw>, SqlConversionError> {
        let _ = self.build_query(expr, &mut BTreeMap::new(), ctx)?;

        Ok(Query::from(self))
    }

    fn build_query(
        &mut self,
        expr: &MirRelationExpr,
        bindings: &mut BTreeMap<LocalId, (Ident, Vec<Ident>)>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<(Ident, Vec<Ident>), SqlConversionError> {
        use MirRelationExpr::*;

        self.checked_recur_mut(|sc: &mut MirToSql| {
            let columns = sc.column_info(expr, ctx);

            match expr {
                Constant { rows, .. } => {
                    let rows = match rows {
                        Err(err) => {
                            return Err(SqlConversionError::BadConstant { err: err.clone() })
                        }
                        Ok(raw_rows) => raw_rows
                            .into_iter()
                            .map(|(row, _id)| {
                                row.unpack()
                                    .iter()
                                    .map(|datum| sc.to_sql_value(&datum))
                                    .collect()
                            })
                            .collect(),
                    };

                    let ident = sc.fresh_ident("const");
                    sc.push_body(
                        ident.clone(),
                        columns.clone(),
                        SetExpr::Values(Values(rows)),
                    )
                }
                Get {
                    id: Id::Local(id), ..
                } => {
                    // find the corresponding name in our CTEs
                    bindings
                        .get(id)
                        .cloned()
                        .ok_or_else(|| SqlConversionError::UnboundId {
                            id: Id::Local(id.clone()),
                        })
                }
                Get {
                    id: Id::Global(id), ..
                } => {
                    let name = ctx.humanizer.humanize_id_unqualified(*id).ok_or_else(|| {
                        SqlConversionError::UnboundId {
                            id: Id::Global(id.clone()),
                        }
                    })?;

                    let ident = Ident::new(name)
                        .map_err(|err| SqlConversionError::BadGlobalName { id: *id, err })?;

                    Ok((ident, columns))
                }
                Let { id, value, body } => {
                    // build value query, store binding
                    let v_query = sc.build_query(value, bindings, ctx)?;
                    bindings.insert(*id, v_query);

                    // build body
                    sc.build_query(body, bindings, ctx)
                }
                Project { input, outputs } => {
                    let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;
                    let ident = sc.fresh_ident("proj");

                    // TODO(mgree) this always generates a new CTE, but we can sometimes just project the prior one (needs QB support)
                    let mut projection = Vec::with_capacity(outputs.len());
                    for col in outputs {
                        let i = &inner_columns[*col];

                        projection.push(SelectItem::Expr {
                            expr: Expr::Identifier(vec![inner.clone(), i.clone()]),
                            alias: Some(i.clone()),
                        });
                    }

                    sc.push_body(
                        ident,
                        columns.clone(),
                        SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection,
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: RawItemName::Name(UnresolvedItemName(vec![inner])),
                                    alias: None,
                                },
                                joins: vec![],
                            }],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        })),
                    )
                }
                Map { input, scalars } => {
                    let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;
                    let num_inner_columns = inner_columns.len();
                    let fq_columns = inner_columns
                        .iter()
                        .map(|col_name| vec![inner.clone(), col_name.clone()])
                        .collect::<Vec<_>>();
                    let mut fq_colexprs = fq_columns
                        .iter()
                        .cloned()
                        .map(Expr::Identifier)
                        .collect::<Vec<_>>();

                    let mut new_select_items = Vec::with_capacity(scalars.len());
                    for (col, expr) in scalars.iter().enumerate() {
                        let alias = Some(columns[num_inner_columns + col].clone());

                        let expr = sc.to_sql_expr(expr, &fq_colexprs, ctx)?;
                        fq_colexprs.push(expr.clone());
                        new_select_items.push(SelectItem::Expr { expr, alias });
                    }

                    // TODO(mgree) this always generates a new CTE, but we can sometimes just add to the prior one (needs QB support)
                    let ident = sc.fresh_ident("map");

                    let mut projection: Vec<SelectItem<Raw>> = fq_columns
                        .into_iter()
                        .map(|fqi| {
                            let alias = fqi.last().cloned();
                            SelectItem::Expr {
                                expr: Expr::Identifier(fqi),
                                alias,
                            }
                        })
                        .collect();
                    projection.extend(new_select_items);

                    let body = SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection,
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![inner])),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: None,
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    }));

                    sc.push_body(ident, columns, body)
                }
                FlatMap {
                    input: _,
                    func: _,
                    exprs: _,
                } => unimplemented!("MIR-to-SQL FlatMap\n{expr:?}"),
                Filter { input, predicates } => {
                    let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;

                    if predicates.is_empty() {
                        return Ok((inner, inner_columns));
                    }

                    let fq_columns = inner_columns
                        .iter()
                        .map(|col_name| vec![inner.clone(), col_name.clone()])
                        .collect::<Vec<_>>();
                    let fq_colexprs = fq_columns
                        .iter()
                        .cloned()
                        .map(Expr::Identifier)
                        .collect::<Vec<_>>();

                    let mut predicates = predicates.into_iter();
                    let mut selection =
                        sc.to_sql_expr(predicates.next().unwrap(), &fq_colexprs, ctx)?;
                    for expr in predicates {
                        selection = selection.and(sc.to_sql_expr(expr, &fq_colexprs, ctx)?);
                    }

                    // TODO(mgree) this always generates a new CTE, but we can sometimes just add to the prior one (needs QB support)
                    let ident = sc.fresh_ident("filter");
                    let body = SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection: fq_columns
                            .into_iter()
                            .map(|fqi| {
                                let alias = fqi.last().cloned();
                                SelectItem::Expr {
                                    expr: Expr::Identifier(fqi),
                                    alias,
                                }
                            })
                            .collect(),
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![inner])),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: Some(selection),
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    }));

                    sc.push_body(ident, columns, body)
                }
                Join {
                    inputs,
                    equivalences,
                    implementation,
                } => {
                    let num_inputs = inputs.len();

                    // empty join
                    if num_inputs == 0 {
                        let ident = sc.fresh_ident("empty_join");
                        return sc.push_body(ident, vec![], SetExpr::Values(Values(vec![])));
                    }

                    // singleton join
                    if num_inputs == 1 {
                        return sc.build_query(&inputs[0], bindings, ctx);
                    }

                    // NOTE: metadata are stored in AST order!

                    // recursively generate inputs in AST order...
                    let mut fq_columns = Vec::with_capacity(columns.len());
                    let mut fq_colexprs: Vec<Expr<Raw>> = Vec::with_capacity(columns.len());
                    let mut q_idents = Vec::with_capacity(num_inputs);
                    let mut q_aliases = Vec::with_capacity(num_inputs);
                    let mut q_columns = Vec::with_capacity(num_inputs);
                    for input in inputs {
                        let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;
                        let alias = sc.fresh_ident(inner.as_str());
                        assert_eq!(inner_columns, sc.column_info(input, ctx));

                        let cols = inner_columns
                            .iter()
                            .map(|col_name| vec![alias.clone(), col_name.clone()])
                            .collect::<Vec<_>>();
                        fq_columns.extend(cols.clone());
                        fq_colexprs.extend(cols.into_iter().map(Expr::Identifier));

                        q_aliases.push(alias);
                        q_idents.push(inner);
                        q_columns.push(inner_columns);
                    }
                    assert_eq!(
                        columns.len(),
                        q_columns.iter().map(|cols| cols.len()).sum::<usize>()
                    );

                    // a real join! find the order...
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
                            (0, (1..num_inputs).collect())
                        }
                    };

                    // build the parts of the actual JOINing SELECT, using join order
                    let from = vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: RawItemName::Name(UnresolvedItemName(vec![
                                q_idents[first].clone()
                            ])),
                            alias: Some(TableAlias {
                                name: q_aliases[first].clone(),
                                columns: vec![],
                                strict: false,
                            }),
                        },
                        joins: rest
                            .into_iter()
                            .map(|idx| mz_sql_parser::ast::Join {
                                relation: TableFactor::Table {
                                    name: RawItemName::Name(UnresolvedItemName(vec![q_idents
                                        [idx]
                                        .clone()])),
                                    alias: Some(TableAlias {
                                        name: q_aliases[idx].clone(),
                                        columns: vec![],
                                        strict: false,
                                    }),
                                },
                                join_operator: JoinOperator::Inner(JoinConstraint::On(
                                    Expr::Value(Value::Boolean(true)),
                                )),
                            })
                            .collect(),
                    }];

                    // TODO(mgree): rather than giving ON(true) and giving a where clause, move these equivalences into the join itself
                    let mut selection: Option<Expr<Raw>> = None;
                    for equivalence in equivalences {
                        if let Some(conjunct) =
                            sc.equivalence_to_conjunct(equivalence, &fq_colexprs, ctx)?
                        {
                            if let Some(sel) = selection {
                                selection = Some(sel.and(conjunct))
                            } else {
                                selection = Some(conjunct);
                            }
                        }
                    }

                    let projection = fq_columns
                        .into_iter()
                        .map(|fqi| {
                            let alias = fqi.last().cloned();
                            SelectItem::Expr {
                                expr: Expr::Identifier(fqi),
                                alias,
                            }
                        })
                        .collect();

                    let ident = sc.fresh_ident("join");
                    sc.push_body(
                        ident,
                        columns,
                        SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection,
                            from,
                            selection,
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        })),
                    )
                }
                Reduce {
                    input,
                    group_key,
                    aggregates,
                    monotonic: _monotonic,
                    expected_group_size,
                } => {
                    assert_eq!(columns.len(), group_key.len() + aggregates.len());

                    let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;

                    eprintln!("EAS: {expr:?}");
                    let fq_colexprs = inner_columns
                        .iter()
                        .map(|col_name| Expr::Identifier(vec![inner.clone(), col_name.clone()]))
                        .collect::<Vec<_>>();
                    // !!! selects group_key followed by each aggregate
                    let mut group_by = Vec::with_capacity(group_key.len());
                    let mut projection = Vec::with_capacity(columns.len());

                    for (gk, ident) in group_key.iter().zip(columns.iter()) {
                        let expr = sc.to_sql_expr(gk, &fq_colexprs, ctx)?;
                        group_by.push(expr.clone());
                        projection.push(SelectItem::Expr {
                            expr,
                            alias: Some(ident.clone()),
                        });
                    }

                    let mut shared_distinct_value = None;
                    let distinct = if aggregates.is_empty() {
                        let mut distinct = vec![];
                        std::mem::swap(&mut distinct, &mut group_by);
                        Some(Distinct::On(distinct))
                    } else {
                        None
                    };
                    for (agg, ident) in aggregates
                        .iter()
                        .zip(columns.iter().dropping(group_key.len()))
                    {
                        if let Some(distinct) = shared_distinct_value {
                            if agg.distinct != distinct {
                                // TODO(mgree) if a reduce mixes distinct and non-distinct aggregates, we need to generate separate reduces and a join (i think???)
                                return Err(SqlConversionError::UnexpectedMixedDistinctReduce);
                            }
                        } else {
                            shared_distinct_value = Some(agg.distinct);
                        }

                        let expr = sc.agg_to_sql_expr(agg, &fq_colexprs, ctx)?;
                        projection.push(SelectItem::Expr {
                            expr,
                            alias: Some(ident.clone()),
                        });
                    }

                    let options = expected_group_size.map_or_else(Vec::new, |egs| {
                        vec![SelectOption {
                            name: SelectOptionName::ExpectedGroupSize,
                            value: Some(WithOptionValue::Value(Value::Number(egs.to_string()))),
                        }]
                    });

                    let ident = sc.fresh_ident("reduce");
                    let body = SetExpr::Select(Box::new(Select {
                        distinct,
                        projection,
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: RawItemName::Name(UnresolvedItemName(vec![inner])),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: None,
                        group_by,
                        having: None,
                        options,
                    }));
                    sc.push_body(ident, columns, body)
                }
                TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    offset,
                    monotonic: _,
                    expected_group_size,
                } => {
                    let (inner, inner_columns) = sc.build_query(input, bindings, ctx)?;
                    assert_eq!(inner_columns.len(), columns.len());

                    let fq_columns = inner_columns
                        .iter()
                        .map(|col_name| vec![inner.clone(), col_name.clone()])
                        .collect::<Vec<_>>();

                    let ident = sc.fresh_ident("topk");

                    // SELECT key_col, ... FROM
                    //    (SELECT DISTINCT key_col FROM tbl) grp,
                    //    LATERAL (
                    //        SELECT col1, col2..., order_col
                    //        FROM tbl
                    //        WHERE key_col = grp.key_col
                    //        OPTIONS (LIMT INPUT GROUP SIZE = ...)
                    //        ORDER BY order_col LIMIT k
                    //    )
                    // ORDER BY key_col, order_col

                    let tbl: TableWithJoins<Raw> = TableWithJoins {
                        relation: TableFactor::Table {
                            name: RawItemName::Name(UnresolvedItemName(vec![inner])),
                            alias: None,
                        },
                        joins: vec![],
                    };

                    let mut keys = Vec::with_capacity(group_key.len());
                    let mut fq_keys = Vec::with_capacity(group_key.len());
                    for col in group_key {
                        keys.push(columns[*col].clone());
                        fq_keys.push(fq_columns[*col].clone());
                    }
                    let fq_colexprs = fq_columns
                        .iter()
                        .cloned()
                        .map(Expr::Identifier)
                        .collect::<Vec<_>>();

                    let limit = if let Some(limit) = limit {
                        Some(Limit {
                            with_ties: false,
                            quantity: sc.to_sql_expr(limit, &fq_colexprs, ctx)?,
                        })
                    } else {
                        None
                    };

                    //    (SELECT DISTINCT key_col FROM tbl) grp,
                    let group_alias = sc.fresh_ident("grp");
                    let key_query = Query {
                        ctes: CteBlock::Simple(vec![]),
                        body: SetExpr::Select(Box::new(Select {
                            distinct: Some(Distinct::EntireRow),
                            projection: fq_keys
                                .iter()
                                .map(|fqi| {
                                    let alias = fqi.last().cloned();
                                    SelectItem::Expr {
                                        expr: Expr::Identifier(fqi.clone()),
                                        alias,
                                    }
                                })
                                .collect(),
                            from: vec![tbl.clone()],
                            selection: None,
                            group_by: vec![],
                            having: None,
                            options: vec![],
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    };

                    let mut order_by = Vec::with_capacity(order_key.len());
                    for ColumnOrder {
                        column,
                        desc,
                        nulls_last,
                    } in order_key
                    {
                        let expr =
                            sc.to_sql_expr(&MirScalarExpr::Column(*column), &fq_colexprs, ctx)?;
                        let asc = Some(!*desc);
                        let nulls_last = Some(*nulls_last);
                        order_by.push(OrderByExpr {
                            expr,
                            asc,
                            nulls_last,
                        });
                    }

                    let mut outer_order_by = Vec::with_capacity(group_key.len() + order_key.len());
                    for col in group_key {
                        let expr =
                            sc.to_sql_expr(&MirScalarExpr::Column(*col), &fq_colexprs, ctx)?;
                        outer_order_by.push(OrderByExpr {
                            expr,
                            asc: None,
                            nulls_last: None,
                        });
                    }
                    outer_order_by.extend(order_by.iter().cloned());

                    //    LATERAL (
                    //        SELECT col1, col2..., order_col
                    //        FROM tbl
                    //        WHERE key_col = grp.key_col
                    //        OPTIONS (LIMT INPUT GROUP SIZE = ...)
                    //        ORDER BY order_col LIMIT k
                    //    )
                    let lateral_alias = sc.fresh_ident("lateral");
                    let conjuncts = keys
                        .iter()
                        .zip_eq(fq_keys)
                        .map(|(k, fqk)| {
                            Expr::Identifier(vec![group_alias.clone(), k.clone()])
                                .equals(Expr::Identifier(fqk.clone()))
                        })
                        .collect::<Vec<_>>();
                    let selection = conjuncts.into_iter().reduce(Expr::and);

                    let mut options = Vec::with_capacity(1);
                    if let Some(expected_group_size) = expected_group_size {
                        options.push(SelectOption {
                            name: SelectOptionName::ExpectedGroupSize,
                            value: Some(WithOptionValue::Value(Value::Number(
                                expected_group_size.to_string(),
                            ))),
                        })
                    }

                    let projection = fq_columns
                        .into_iter()
                        .map(|fqi| {
                            let alias = fqi.last().cloned();
                            SelectItem::Expr {
                                expr: Expr::Identifier(fqi),
                                alias,
                            }
                        })
                        .collect::<Vec<_>>();

                    let lateral_query = Query {
                        ctes: CteBlock::Simple(vec![]),
                        body: SetExpr::Select(Box::new(Select {
                            distinct: None,
                            projection,
                            selection,
                            from: vec![tbl],
                            group_by: vec![],
                            having: None,
                            options,
                        })),
                        order_by,
                        limit,
                        offset: None,
                    };

                    // SELECT key_col, ... FROM
                    // ...
                    // ORDER BY key_col, order_col

                    let projection = columns
                        .iter()
                        .map(|i| {
                            let alias = Some(i.clone());
                            SelectItem::Expr {
                                expr: Expr::Identifier(vec![lateral_alias.clone(), i.clone()]),
                                alias,
                            }
                        })
                        .collect::<Vec<_>>();

                    let body = SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection,
                        from: vec![
                            TableWithJoins {
                                relation: TableFactor::Derived {
                                    lateral: false,
                                    subquery: Box::new(key_query),
                                    alias: Some(TableAlias {
                                        name: group_alias,
                                        columns: keys,
                                        strict: false,
                                    }),
                                },
                                joins: vec![],
                            },
                            TableWithJoins {
                                relation: TableFactor::Derived {
                                    lateral: true,
                                    subquery: Box::new(lateral_query),
                                    alias: None,
                                },
                                joins: vec![],
                            },
                        ],
                        selection: None,
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    }));

                    sc.push_prequery(
                        ident,
                        columns,
                        PreQuery {
                            body,
                            order_by: outer_order_by,
                            limit: None,
                            offset: Some(Expr::Value(Value::Number(offset.to_string()))),
                        },
                    )
                }
                Union { base, inputs } => {
                    // detect aggregates
                    if let Some(res) = sc.detect_aggregate_union(base, inputs, bindings, ctx) {
                        return Ok(res);
                    }

                    // in general, we'll have something of the form:
                    //
                    // Union
                    //   [Negate]
                    //     base
                    //   [Negate]
                    //     inputs[0]
                    //   ...
                    //   [Negate]
                    //     inputs[n]
                    //
                    // it should not be the case that _all_ of them are negated

                    // build the input queries and catalog them as positive (not negated) or negative (negated)
                    let num_inputs = inputs.len() + 1;
                    let mut pos = Vec::with_capacity(num_inputs);
                    let mut neg = Vec::with_capacity(num_inputs);

                    let base = &**base;
                    for mut expr in std::iter::once(base).chain(inputs) {
                        let mut positive = true;

                        while let Negate { input } = expr {
                            expr = &**input;
                            positive = !positive;
                        }

                        let q = sc.build_query(expr, bindings, ctx)?;
                        if positive {
                            pos.push(q)
                        } else {
                            neg.push(q)
                        }
                    }

                    // oops... they're all negated!
                    if pos.is_empty() {
                        return Err(SqlConversionError::UnexpectedFullyNegatedUnion);
                    }

                    let mut body = pos
                        .into_iter()
                        .map(|(i, _)| {
                            SetExpr::Table::<Raw>(RawItemName::Name(
                                UnresolvedItemName::unqualified(i),
                            ))
                        })
                        .reduce(|left, right| SetExpr::SetOperation {
                            op: SetOperator::Union,
                            all: true,
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                        .unwrap();

                    for (i, _) in neg.into_iter() {
                        let right = Box::new(SetExpr::Table::<Raw>(RawItemName::Name(
                            UnresolvedItemName::unqualified(i),
                        )));
                        body = SetExpr::SetOperation {
                            op: SetOperator::Except,
                            all: true,
                            left: Box::new(body),
                            right,
                        };
                    }

                    let ident = sc.fresh_ident("union");
                    sc.push_body(ident, columns, body)
                }
                ArrangeBy { input, keys: _keys } => sc.build_query(input, bindings, ctx),
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
            }
        })
    }

    fn detect_aggregate_union(
        &mut self,
        base: &MirRelationExpr,
        inputs: &Vec<MirRelationExpr>,
        bindings: &mut BTreeMap<LocalId, (Ident, Vec<Ident>)>,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Option<(Ident, Vec<Ident>)> {
        // detect the following idiom:
        //
        // Union
        //   Get l0
        //   Map (VALUE)
        //     Union
        //       Negate
        //         Project ()
        //           Get l0
        //       Constant
        //         - ()
        //
        // where:
        //   cte l0 =
        //     Reduce aggregates=[...]

        use MirRelationExpr::*;

        let Get { id, .. } = base else { return None };

        if inputs.len() != 1 {
            return None;
        }
        let Map { input, .. } = &inputs[0] else {
            return None;
        };

        let Union {
            base: inner_base,
            inputs: inner_inputs,
        } = &**input
        else {
            return None;
        };

        if inner_inputs.len() != 1 {
            return None;
        }
        let Constant { rows, .. } = &inner_inputs[0] else {
            return None;
        };
        let Ok(rows) = rows else {
            return None;
        };
        if !rows.len() == 1 {
            return None;
        }
        if !rows[0].0.unpack().is_empty() {
            return None;
        }

        let Negate { input } = &**inner_base else {
            return None;
        };

        let Project { input, outputs } = &**input else {
            return None;
        };
        if !outputs.is_empty() {
            return None;
        }

        let Get { id: inner_id, .. } = &**input else {
            return None;
        };
        if *id != *inner_id {
            return None;
        }

        // if we conformed to that pattern... just behave like Get l0,
        // which should be the rendering of the (MRE) reduce as a (SQL) aggregate
        self.build_query(base, bindings, ctx).map_or_else(
            |err| {
                ::tracing::error!("MIR-to-SQL error caused aggregate detection to fail: {err:?}",);
                None
            },
            Some,
        )
    }

    fn equivalence_to_conjunct(
        &mut self,
        exprs: &Vec<MirScalarExpr>,
        columns: &[Expr<Raw>],
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Option<Expr<Raw>>, SqlConversionError> {
        let mut iter = exprs.into_iter();
        let Some(lhs) = iter.next() else {
            return Ok(None);
        };
        let Some(rhs) = iter.next() else {
            return Ok(None);
        };

        let canonical = self.to_sql_expr(lhs, columns, ctx)?;

        let right = self.to_sql_expr(rhs, columns, ctx)?;
        let mut equiv = Expr::equals(canonical.clone(), right);

        for rhs in iter {
            let right = self.to_sql_expr(rhs, columns, ctx)?;
            equiv = equiv.and(canonical.clone().equals(right));
        }

        Ok(Some(equiv))
    }

    fn to_sql_expr(
        &mut self,
        expr: &MirScalarExpr,
        columns: &[Expr<Raw>],
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
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
            Column(col) => Ok(columns[*col].clone()),
            Literal(Ok(row), ty) => {
                let mut datums = row.unpack();
                if datums.len() != 1 {
                    return Err(SqlConversionError::BadConstant {
                        err: EvalError::Internal("literal with more than one datum".to_string()),
                    });
                }

                let datum = datums.pop().unwrap();
                let mut value = self.to_sql_value(&datum);

                if datum.is_null() {
                    value = Expr::cast(value, self.to_sql_type(&ty.scalar_type, ctx));
                }
                Ok(value)
            }
            Literal(Err(err), _) => Err(SqlConversionError::BadConstant { err: err.clone() }),
            CallUnmaterializable(uf) => Ok(call(uf, vec![])),
            CallUnary { func, expr } => {
                let arg = self.to_sql_expr(expr, columns, ctx)?;
                Ok(call(func, vec![arg]))
            }
            CallBinary { func, expr1, expr2 } => {
                let arg1 = self.to_sql_expr(expr1, columns, ctx)?;
                let arg2 = self.to_sql_expr(expr2, columns, ctx)?;
                Ok(call(func, vec![arg1, arg2]))
            }
            CallVariadic { func, exprs } => {
                let mut args = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    args.push(self.to_sql_expr(expr, columns, ctx)?);
                }
                Ok(call(func, args))
            }
            If { cond, then, els } => Ok(Expr::Case {
                operand: None,
                conditions: vec![self.to_sql_expr(cond, columns, ctx)?],
                results: vec![self.to_sql_expr(then, columns, ctx)?],
                else_result: Some(Box::new(self.to_sql_expr(els, columns, ctx)?)),
            }),
        }
    }

    fn agg_to_sql_expr(
        &mut self,
        AggregateExpr {
            expr,
            func,
            distinct: _distinct,
        }: &AggregateExpr,
        columns: &[Expr<Raw>],
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> Result<Expr<Raw>, SqlConversionError> {
        let arg = self.to_sql_expr(expr, columns, ctx)?;

        use AggregateFunc::*;
        match func {
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
            | Dummy { .. } => unimplemented!("MIR-to-SQL AggregateFunc: {:?}", func),
            _ => (),
        };

        let name = RawItemName::Name(UnresolvedItemName(vec![Ident::new_unchecked(func.name())]));

        Ok(Expr::call(name, vec![arg]))
    }

    fn to_sql_value(&self, datum: &Datum) -> Expr<Raw> {
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
            | Datum::AclItem(_) => unimplemented!("MIR-to-SQL esoteric Datum: {datum}"),
        }
    }

    fn to_sql_type(
        &self,
        scalar_type: &ScalarType,
        ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> RawDataType {
        fn named<S: Into<String>>(s: S) -> RawDataType {
            RawDataType::Other {
                name: RawItemName::Name(UnresolvedItemName(vec![Ident::new_unchecked(s)])),
                typ_mod: vec![],
            }
        }

        match scalar_type {
            ScalarType::Array(scalar_type) => {
                RawDataType::Array(Box::new(self.to_sql_type(&**scalar_type, ctx)))
            }
            ScalarType::List { element_type, .. } => {
                RawDataType::List(Box::new(self.to_sql_type(element_type, ctx)))
            }
            ScalarType::Record { .. } => named("record"),
            ScalarType::Map { value_type, .. } => RawDataType::Map {
                key_type: Box::new(named("text")),
                value_type: Box::new(self.to_sql_type(value_type, ctx)),
            },
            ScalarType::Range { element_type } => {
                let inner_name = ctx.humanizer.humanize_scalar_type(element_type);
                named(format!("{inner_name}range"))
            }
            ScalarType::Char {
                length: Some(length),
            } => named(format!("char({})", length.into_u32())),
            ScalarType::Char { length: None } => named("char"),
            ScalarType::VarChar {
                max_length: Some(max_length),
            } => named(format!("varchar({})", max_length.into_u32())),
            ScalarType::VarChar { max_length: None } => named("varchar"),
            ScalarType::Timestamp { precision: None } => named("timestamp"),
            ScalarType::Timestamp {
                precision: Some(precision),
            } => named(format!("timestamp({})", precision.into_u8())),
            ScalarType::TimestampTz { precision: None } => named("timestamptz"),
            ScalarType::TimestampTz {
                precision: Some(precision),
            } => named(format!("timestamptz({})", precision.into_u8())),
            ScalarType::Numeric {
                max_scale: Some(max_scale),
            } => named(format!("numeric(39, {})", max_scale.into_u8())), // precision is always 39
            ScalarType::Numeric { max_scale: None } => named("numeric"),
            ScalarType::Bool
            | ScalarType::Int16
            | ScalarType::Int32
            | ScalarType::Int64
            | ScalarType::UInt16
            | ScalarType::UInt32
            | ScalarType::UInt64
            | ScalarType::Float32
            | ScalarType::Float64
            | ScalarType::Date
            | ScalarType::Time
            | ScalarType::Interval
            | ScalarType::PgLegacyChar
            | ScalarType::PgLegacyName
            | ScalarType::Bytes
            | ScalarType::String
            | ScalarType::Jsonb
            | ScalarType::Uuid
            | ScalarType::Oid
            | ScalarType::RegProc
            | ScalarType::RegType
            | ScalarType::RegClass
            | ScalarType::Int2Vector
            | ScalarType::MzTimestamp
            | ScalarType::MzAclItem
            | ScalarType::AclItem => named(ctx.humanizer.humanize_scalar_type(scalar_type)),
        }
    }

    fn push_prequery(
        &mut self,
        ident: Ident,
        columns: Vec<Ident>,
        query: PreQuery,
    ) -> Result<(Ident, Vec<Ident>), SqlConversionError> {
        self.query.push((ident.clone(), columns.clone(), query));
        Ok((ident, columns))
    }

    fn push_body(
        &mut self,
        ident: Ident,
        columns: Vec<Ident>,
        body: SetExpr<Raw>,
    ) -> Result<(Ident, Vec<Ident>), SqlConversionError> {
        self.push_prequery(
            ident,
            columns,
            PreQuery {
                body,
                order_by: vec![],
                limit: None,
                offset: None,
            },
        )
    }
}

#[derive(Clone, Debug)]
struct PreQuery {
    body: SetExpr<Raw>,
    order_by: Vec<OrderByExpr<Raw>>,
    limit: Option<Limit<Raw>>,
    offset: Option<Expr<Raw>>,
}

impl From<PreQuery> for Query<Raw> {
    fn from(
        PreQuery {
            body,
            order_by,
            limit,
            offset,
        }: PreQuery,
    ) -> Query<Raw> {
        Query {
            ctes: CteBlock::empty(),
            body,
            order_by,
            limit,
            offset,
        }
    }
}

impl PreQuery {
    fn to_cte(self, ident: Ident, columns: Vec<Ident>) -> Cte<Raw> {
        Cte {
            alias: TableAlias {
                name: ident,
                columns,
                strict: false,
            },
            id: (),
            query: self.into(),
        }
    }
}

impl From<&mut MirToSql> for Query<Raw> {
    fn from(q: &mut MirToSql) -> Self {
        let Some((
            _,
            _,
            PreQuery {
                body,
                order_by,
                limit,
                offset,
            },
        )) = q.query.pop()
        else {
            ::tracing::warn!("converting empty query builder to SQL");

            // dummy empty select
            return Query::<Raw> {
                ctes: CteBlock::Simple(vec![]),
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
            };
        };

        Query {
            ctes: CteBlock::Simple(
                q.query
                    .drain(..)
                    .map(|(ident, columns, pq)| pq.to_cte(ident, columns))
                    .collect(),
            ),
            body,
            order_by,
            limit,
            offset,
        }
    }
}
