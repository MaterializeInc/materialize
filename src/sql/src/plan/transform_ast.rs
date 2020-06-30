// Copyright Materialize, Inc. All rights reserved.
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

use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{Expr, Function, FunctionArgs, Ident, ObjectName, Query, SelectItem};

use crate::normalize;

pub fn transform(query: &mut Query) {
    FuncRewriter.visit_query_mut(query);
    IdentFuncRewriter.visit_query_mut(query);
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
struct FuncRewriter;

impl FuncRewriter {
    // Divides `lhs` by `rhs` but replaces division-by-zero errors with NULL.
    fn plan_divide(lhs: Expr, rhs: Expr) -> Expr {
        lhs.divide(Self::plan_null_if(rhs, Expr::number("0")))
    }

    fn plan_agg(name: &'static str, expr: Expr, filter: Option<Box<Expr>>, distinct: bool) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec![name.into()]),
            args: FunctionArgs::Args(vec![expr]),
            filter,
            over: None,
            distinct,
        })
    }

    fn plan_avg(expr: Expr, filter: Option<Box<Expr>>, distinct: bool) -> Expr {
        let sum = Self::plan_agg("sum", expr.clone(), filter.clone(), distinct)
            .call_unary("internal_avg_promotion");
        let count = Self::plan_agg("count", expr, filter, distinct);
        Self::plan_divide(sum, count)
    }

    fn plan_variance(expr: Expr, filter: Option<Box<Expr>>, distinct: bool, sample: bool) -> Expr {
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
        let expr = expr.call_unary("internal_avg_promotion");
        let expr_squared = expr.clone().multiply(expr.clone());
        let sum_squares = Self::plan_agg("sum", expr_squared, filter.clone(), distinct);
        let sum = Self::plan_agg("sum", expr.clone(), filter.clone(), distinct);
        let sum_squared = sum.clone().multiply(sum);
        let count = Self::plan_agg("count", expr, filter, distinct);
        Self::plan_divide(
            sum_squares.minus(Self::plan_divide(sum_squared, count.clone())),
            if sample {
                count.minus(Expr::number("1"))
            } else {
                count
            },
        )
    }

    fn plan_stddev(expr: Expr, filter: Option<Box<Expr>>, distinct: bool, sample: bool) -> Expr {
        Self::plan_variance(expr, filter, distinct, sample).call_unary("sqrt")
    }

    fn plan_null_if(left: Expr, right: Expr) -> Expr {
        Expr::Case {
            operand: None,
            conditions: vec![left.clone().equals(right)],
            results: vec![Expr::null()],
            else_result: Some(Box::new(left)),
        }
    }

    fn rewrite_expr(expr: &Expr) -> Option<(Ident, Expr)> {
        match expr {
            Expr::Function(Function {
                name,
                args: FunctionArgs::Args(args),
                filter,
                distinct,
                over: None,
            }) => {
                let name = normalize::function_name(name.clone()).ok()?;
                let filter = filter.clone();
                let distinct = *distinct;
                let expr = if args.len() == 1 {
                    let arg = args[0].clone();
                    match name.as_str() {
                        "avg" => Self::plan_avg(arg, filter, distinct),
                        "variance" | "var_samp" => Self::plan_variance(arg, filter, distinct, true),
                        "var_pop" => Self::plan_variance(arg, filter, distinct, false),
                        "stddev" | "stddev_samp" => Self::plan_stddev(arg, filter, distinct, true),
                        "stddev_pop" => Self::plan_stddev(arg, filter, distinct, false),
                        _ => return None,
                    }
                } else if args.len() == 2 {
                    let (lhs, rhs) = (args[0].clone(), args[1].clone());
                    match name.as_str() {
                        "mod" => lhs.modulo(rhs),
                        "nullif" => Self::plan_null_if(lhs, rhs),
                        _ => return None,
                    }
                } else {
                    return None;
                };
                Some((Ident::new(name), expr))
            }
            _ => None,
        }
    }
}

impl<'ast> VisitMut<'ast> for FuncRewriter {
    fn visit_select_item_mut(&mut self, item: &'ast mut SelectItem) {
        if let SelectItem::UnnamedExpr(expr) = item {
            visit_mut::visit_expr_mut(self, expr);
            if let Some((alias, expr)) = Self::rewrite_expr(expr) {
                *item = SelectItem::ExprWithAlias { expr, alias }
            }
        } else {
            visit_mut::visit_select_item_mut(self, item);
        }
    }

    fn visit_expr_mut(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr_mut(self, expr);
        if let Some((_name, new_expr)) = Self::rewrite_expr(expr) {
            *expr = new_expr;
        }
    }
}

// Rewrites special keywords that SQL considers to be function calls to actual
// function calls. For example, `SELECT current_timestamp` is rewritten to
// `SELECT current_timestamp()`.
struct IdentFuncRewriter;

impl<'ast> VisitMut<'ast> for IdentFuncRewriter {
    fn visit_expr_mut(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr_mut(self, expr);
        if let Expr::Identifier(ident) = expr {
            if ident.len() != 1 {
                return;
            }
            if normalize::ident(ident[0].clone()) == "current_timestamp" {
                *expr = Expr::call_nullary("current_timestamp");
            }
        }
    }
}
