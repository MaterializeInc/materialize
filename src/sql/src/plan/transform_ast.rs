// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations of SQL ASTs.
//!
//! Most query optimizations are performed by the dataflow layer, but some
//! are much easier to perform in SQL. Someday, we'll want our own SQL IR,
//! but for now we just use the parser's AST directly.

use uuid::Uuid;

use crate::names::Aug;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    Expr, Function, FunctionArgs, Ident, Op, OrderByExpr, Query, Select, SelectItem, TableAlias,
    TableFactor, TableFunction, TableWithJoins, UnresolvedObjectName, Value,
};

use crate::normalize;
use crate::plan::{PlanError, StatementContext};

pub fn transform_query<'a>(
    scx: &StatementContext,
    query: &'a mut Query<Aug>,
) -> Result<(), PlanError> {
    run_transforms(scx, |t, query| t.visit_query_mut(query), query)
}

pub fn transform_expr(scx: &StatementContext, expr: &mut Expr<Aug>) -> Result<(), PlanError> {
    run_transforms(scx, |t, expr| t.visit_expr_mut(expr), expr)
}

pub(crate) fn run_transforms<F, A>(
    scx: &StatementContext,
    mut f: F,
    ast: &mut A,
) -> Result<(), PlanError>
where
    F: for<'ast> FnMut(&mut dyn VisitMut<'ast, Aug>, &'ast mut A),
{
    let mut func_rewriter = FuncRewriter::new(scx);
    f(&mut func_rewriter, ast);
    func_rewriter.status?;

    let mut desugarer = Desugarer::new();
    f(&mut desugarer, ast);
    desugarer.status
}

// Transforms various functions to forms that are more easily handled by the
// planner.
//
// Specifically:
//
//   * Rewrites the `mod` function to the `%` binary operator, so the modulus
//     code only needs to handle the operator form.
//
//   * Rewrites the `nullif` function to a `CASE` statement, to reuse the code
//     for planning equality of datums.
//
//   * Rewrites `avg(col)` to `sum(col) / count(col)`, so that we can pretend
//     the `avg` aggregate function doesn't exist from here on out. This also
//     has the nice side effect of reusing the division planning logic, which
//     is not trivial for some types, like decimals.
//
//   * Rewrites the suite of standard deviation and variance functions in a
//     manner similar to `avg`.
//
// TODO(sploiselle): rewrite these in terms of func::sql_op!
struct FuncRewriter<'a> {
    scx: &'a StatementContext<'a>,
    status: Result<(), PlanError>,
}

impl<'a> FuncRewriter<'a> {
    fn new(scx: &'a StatementContext<'a>) -> FuncRewriter<'a> {
        FuncRewriter {
            scx,
            status: Ok(()),
        }
    }

    // Divides `lhs` by `rhs` but replaces division-by-zero errors with NULL;
    // note that this is semantically equivalent to `NULLIF(rhs, 0)`.
    fn plan_divide(lhs: Expr<Aug>, rhs: Expr<Aug>) -> Expr<Aug> {
        lhs.divide(Expr::Case {
            operand: None,
            conditions: vec![rhs.clone().equals(Expr::number("0"))],
            results: vec![Expr::null()],
            else_result: Some(Box::new(rhs)),
        })
    }

    fn plan_agg(
        name: UnresolvedObjectName,
        expr: Expr<Aug>,
        order_by: Vec<OrderByExpr<Aug>>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
    ) -> Expr<Aug> {
        Expr::Function(Function {
            name,
            args: FunctionArgs::Args {
                args: vec![expr],
                order_by,
            },
            filter,
            over: None,
            distinct,
        })
    }

    fn plan_avg(expr: Expr<Aug>, filter: Option<Box<Expr<Aug>>>, distinct: bool) -> Expr<Aug> {
        let sum = Self::plan_agg(
            UnresolvedObjectName::qualified(&["pg_catalog", "sum"]),
            expr.clone(),
            vec![],
            filter.clone(),
            distinct,
        )
        .call_unary(vec!["mz_internal", "mz_avg_promotion"]);
        let count = Self::plan_agg(
            UnresolvedObjectName::qualified(&["pg_catalog", "count"]),
            expr,
            vec![],
            filter,
            distinct,
        );
        Self::plan_divide(sum, count)
    }

    fn plan_variance(
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        sample: bool,
    ) -> Expr<Aug> {
        // N.B. this variance calculation uses the "textbook" algorithm, which
        // is known to accumulate problematic amounts of error. The numerically
        // stable variants, the most well-known of which is Welford's, are
        // however difficult to implement inside of Differential Dataflow, as
        // they do not obviously support retractions efficiently (#1240).
        //
        // The code below converts var_samp(x) into
        //
        //     (sum(x²) - sum(x)² / count(x)) / (count(x) - 1)
        //
        // and var_pop(x) into:
        //
        //     (sum(x²) - sum(x)² / count(x)) / count(x)
        //
        let expr = expr.call_unary(vec!["mz_internal", "mz_avg_promotion"]);
        let expr_squared = expr.clone().multiply(expr.clone());
        let sum_squares = Self::plan_agg(
            UnresolvedObjectName::qualified(&["pg_catalog", "sum"]),
            expr_squared,
            vec![],
            filter.clone(),
            distinct,
        );
        let sum = Self::plan_agg(
            UnresolvedObjectName::qualified(&["pg_catalog", "sum"]),
            expr.clone(),
            vec![],
            filter.clone(),
            distinct,
        );
        let sum_squared = sum.clone().multiply(sum);
        let count = Self::plan_agg(
            UnresolvedObjectName::qualified(&["pg_catalog", "count"]),
            expr,
            vec![],
            filter,
            distinct,
        );
        Self::plan_divide(
            sum_squares.minus(Self::plan_divide(sum_squared, count.clone())),
            if sample {
                count.minus(Expr::number("1"))
            } else {
                count
            },
        )
    }

    fn plan_stddev(
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        sample: bool,
    ) -> Expr<Aug> {
        Self::plan_variance(expr, filter, distinct, sample).call_unary(vec!["sqrt"])
    }

    fn rewrite_expr(&mut self, expr: &Expr<Aug>) -> Option<(Ident, Expr<Aug>)> {
        match expr {
            Expr::Function(Function {
                name,
                args: FunctionArgs::Args { args, order_by: _ },
                filter,
                distinct,
                over: None,
            }) => {
                let name = normalize::unresolved_object_name(name.clone()).ok()?;
                if let Some(database) = &name.database {
                    // If a database name is provided, we need only verify that
                    // the database exists, as presently functions can only
                    // exist in ambient schemas.
                    if let Err(e) = self.scx.catalog.resolve_database(database) {
                        self.status = Err(e.into());
                    }
                }
                if name.schema.is_some() && name.schema.as_deref() != Some("pg_catalog") {
                    return None;
                }
                let filter = filter.clone();
                let distinct = *distinct;
                let expr = if args.len() == 1 {
                    let arg = args[0].clone();
                    match name.item.as_str() {
                        "avg" => Self::plan_avg(arg, filter, distinct),
                        "variance" | "var_samp" => Self::plan_variance(arg, filter, distinct, true),
                        "var_pop" => Self::plan_variance(arg, filter, distinct, false),
                        "stddev" | "stddev_samp" => Self::plan_stddev(arg, filter, distinct, true),
                        "stddev_pop" => Self::plan_stddev(arg, filter, distinct, false),
                        _ => return None,
                    }
                } else if args.len() == 2 {
                    let (lhs, rhs) = (args[0].clone(), args[1].clone());
                    match name.item.as_str() {
                        "mod" => lhs.modulo(rhs),
                        "pow" => Expr::call(vec!["pg_catalog", "power"], vec![lhs, rhs]),
                        _ => return None,
                    }
                } else {
                    return None;
                };
                Some((Ident::new(name.item), expr))
            }
            // Rewrites special keywords that SQL considers to be function calls
            // to actual function calls. For example, `SELECT current_timestamp`
            // is rewritten to `SELECT current_timestamp()`.
            Expr::Identifier(ident) if ident.len() == 1 => {
                let ident = normalize::ident(ident[0].clone());
                let fn_ident = match ident.as_str() {
                    "current_role" => Some("current_user"),
                    "current_schema" | "current_timestamp" | "current_user" | "session_user" => {
                        Some(ident.as_str())
                    }
                    _ => None,
                };
                match fn_ident {
                    None => None,
                    Some(fn_ident) => {
                        let expr = Expr::call_nullary(vec![fn_ident]);
                        Some((Ident::new(ident), expr))
                    }
                }
            }
            _ => None,
        }
    }
}

impl<'ast> VisitMut<'ast, Aug> for FuncRewriter<'_> {
    fn visit_select_item_mut(&mut self, item: &'ast mut SelectItem<Aug>) {
        if let SelectItem::Expr { expr, alias: None } = item {
            visit_mut::visit_expr_mut(self, expr);
            if let Some((alias, expr)) = self.rewrite_expr(expr) {
                *item = SelectItem::Expr {
                    expr,
                    alias: Some(alias),
                };
            }
        } else {
            visit_mut::visit_select_item_mut(self, item);
        }
    }

    fn visit_expr_mut(&mut self, expr: &'ast mut Expr<Aug>) {
        visit_mut::visit_expr_mut(self, expr);
        if let Some((_name, new_expr)) = self.rewrite_expr(expr) {
            *expr = new_expr;
        }
    }
}

/// Removes syntax sugar to simplify the planner.
///
/// For example, `<expr> NOT IN (<subquery>)` is rewritten to `expr <> ALL
/// (<subquery>)`.
struct Desugarer {
    status: Result<(), PlanError>,
    recursion_guard: RecursionGuard,
}

impl CheckedRecursion for Desugarer {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<'ast> VisitMut<'ast, Aug> for Desugarer {
    fn visit_expr_mut(&mut self, expr: &'ast mut Expr<Aug>) {
        self.visit_internal(Self::visit_expr_mut_internal, expr);
    }
}

impl Desugarer {
    fn visit_internal<F, X>(&mut self, f: F, x: X)
    where
        F: Fn(&mut Self, X) -> Result<(), PlanError>,
    {
        if self.status.is_ok() {
            // self.status could have changed from a deeper call, so don't blindly
            // overwrite it with the result of this call.
            let status = self.checked_recur_mut(|d| f(d, x));
            if self.status.is_ok() {
                self.status = status;
            }
        }
    }

    fn new() -> Desugarer {
        Desugarer {
            status: Ok(()),
            recursion_guard: RecursionGuard::with_limit(1024), // chosen arbitrarily
        }
    }

    fn visit_expr_mut_internal(&mut self, expr: &mut Expr<Aug>) -> Result<(), PlanError> {
        // `($expr)` => `$expr`
        while let Expr::Nested(e) = expr {
            *expr = e.take();
        }

        // `$expr BETWEEN $low AND $high` => `$expr >= $low AND $expr <= $low`
        // `$expr NOT BETWEEN $low AND $high` => `$expr < $low OR $expr > $low`
        if let Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } = expr
        {
            if *negated {
                *expr = e.clone().lt(low.take()).or(e.take().gt(high.take()));
            } else {
                *expr = e.clone().gt_eq(low.take()).and(e.take().lt_eq(high.take()));
            }
        }

        // `$expr IN ($subquery)` => `$expr = ANY ($subquery)`
        // `$expr NOT IN ($subquery)` => `$expr <> ALL ($subquery)`
        if let Expr::InSubquery {
            expr: e,
            subquery,
            negated,
        } = expr
        {
            if *negated {
                *expr = Expr::AllSubquery {
                    left: Box::new(e.take()),
                    op: Op::bare("<>"),
                    right: Box::new(subquery.take()),
                };
            } else {
                *expr = Expr::AnySubquery {
                    left: Box::new(e.take()),
                    op: Op::bare("="),
                    right: Box::new(subquery.take()),
                };
            }
        }

        // `$expr = ALL ($array_expr)`
        // =>
        // `$expr = ALL (SELECT elem FROM unnest($array_expr) _ (elem))`
        //
        // and analogously for other operators and ANY.
        if let Expr::AnyExpr { left, op, right } | Expr::AllExpr { left, op, right } = expr {
            let binding = Ident::new("elem");

            let subquery = Query::select(
                Select::default()
                    .from(TableWithJoins {
                        relation: TableFactor::Function {
                            function: TableFunction {
                                name: UnresolvedObjectName(vec![
                                    Ident::new("mz_catalog"),
                                    Ident::new("unnest"),
                                ]),
                                args: FunctionArgs::args(vec![right.take()]),
                            },
                            alias: Some(TableAlias {
                                name: Ident::new("_"),
                                columns: vec![binding.clone()],
                                strict: true,
                            }),
                            with_ordinality: false,
                        },
                        joins: vec![],
                    })
                    .project(SelectItem::Expr {
                        expr: Expr::Identifier(vec![binding]),
                        alias: None,
                    }),
            );

            let left = Box::new(left.take());

            let op = op.clone();

            *expr = match expr {
                Expr::AnyExpr { .. } => Expr::AnySubquery {
                    left,
                    op,
                    right: Box::new(subquery),
                },
                Expr::AllExpr { .. } => Expr::AllSubquery {
                    left,
                    op,
                    right: Box::new(subquery),
                },
                _ => unreachable!(),
            };
        }

        // `$expr = ALL ($subquery)`
        // =>
        // `(SELECT mz_internal.mz_all($expr = $binding) FROM ($subquery) AS _ ($binding))
        //
        // and analogously for other operators and ANY.
        if let Expr::AnySubquery { left, op, right } | Expr::AllSubquery { left, op, right } = expr
        {
            let left = match &mut **left {
                Expr::Row { .. } => left.take(),
                _ => Expr::Row {
                    exprs: vec![left.take()],
                },
            };

            let arity = match &left {
                Expr::Row { exprs } => exprs.len(),
                _ => unreachable!(),
            };

            let bindings: Vec<_> = (0..arity)
                .map(|_| Ident::new(format!("right_{}", Uuid::new_v4())))
                .collect();

            let select = Select::default()
                .from(TableWithJoins::subquery(
                    right.take(),
                    TableAlias {
                        name: Ident::new("subquery"),
                        columns: bindings.clone(),
                        strict: true,
                    },
                ))
                .project(SelectItem::Expr {
                    expr: left
                        .binop(
                            op.clone(),
                            Expr::Row {
                                exprs: bindings
                                    .into_iter()
                                    .map(|b| Expr::Identifier(vec![b]))
                                    .collect(),
                            },
                        )
                        .call_unary(match expr {
                            Expr::AnySubquery { .. } => vec!["mz_internal", "mz_any"],
                            Expr::AllSubquery { .. } => vec!["mz_internal", "mz_all"],
                            _ => unreachable!(),
                        }),
                    alias: None,
                });

            *expr = Expr::Subquery(Box::new(Query::select(select)));
        }

        // Expands row comparisons.
        //
        // ROW($l1, $l2, ..., $ln) = ROW($r1, $r2, ..., $rn)
        // =>
        // $l1 = $r1 AND $l2 = $r2 AND ... AND $ln = $rn
        //
        // ROW($l1, $l2, ..., $ln) < ROW($r1, $r2, ..., $rn)
        // =>
        // $l1 < $r1 OR ($l1 = $r1 AND ($l2 < $r2 OR ($l2 = $r2 AND ... ($ln < $rn))))
        //
        // ROW($l1, $l2, ..., $ln) <= ROW($r1, $r2, ..., $rn)
        // =>
        // $l1 < $r1 OR ($l1 = $r1 AND ($l2 < $r2 OR ($l2 = $r2 AND ... ($ln <= $rn))))
        //
        // and analogously for the inverse operations !=, >, and >=.
        if let Expr::Op {
            op,
            expr1: left,
            expr2: Some(right),
        } = expr
        {
            if let (Expr::Row { exprs: left }, Expr::Row { exprs: right }) =
                (&mut **left, &mut **right)
            {
                if matches!(normalize::op(op)?, "=" | "<>" | "<" | "<=" | ">" | ">=") {
                    if left.len() != right.len() {
                        sql_bail!("unequal number of entries in row expressions");
                    }
                    if left.is_empty() {
                        assert!(right.is_empty());
                        sql_bail!("cannot compare rows of zero length");
                    }
                }
                match normalize::op(op)? {
                    "=" | "<>" => {
                        let mut new = Expr::Value(Value::Boolean(true));
                        for (l, r) in left.iter_mut().zip(right) {
                            new = l.take().equals(r.take()).and(new);
                        }
                        if normalize::op(op)? == "<>" {
                            new = new.negate();
                        }
                        *expr = new;
                    }
                    "<" | "<=" | ">" | ">=" => {
                        let strict_op = match normalize::op(op)? {
                            "<" | "<=" => "<",
                            ">" | ">=" => ">",
                            _ => unreachable!(),
                        };
                        let (l, r) = (left.last_mut().unwrap(), right.last_mut().unwrap());
                        let mut new = l.take().binop(op.clone(), r.take());
                        for (l, r) in left.iter_mut().zip(right).rev().skip(1) {
                            new = l
                                .clone()
                                .binop(Op::bare(strict_op), r.clone())
                                .or(l.take().equals(r.take()).and(new));
                        }
                        *expr = new;
                    }
                    _ => (),
                }
            }
        }

        visit_mut::visit_expr_mut(self, expr);
        Ok(())
    }
}
