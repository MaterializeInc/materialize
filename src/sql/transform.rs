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
use sql_parser::ast::{
    BinaryOperator, Expr, Function, Ident, ObjectName, Query, SelectItem, Value,
};

use crate::normalize;

pub fn transform(query: &mut Query) {
    AggFuncRewriter.visit_query(query);
    IdentFuncRewriter.visit_query(query);
}

// Rewrites `avg(col)` to `sum(col) / count(col)`, so that we can pretend the
// `avg` aggregate function doesn't exist from here on out. This also has the
// nice side effect of reusing the division planning logic, which is not trivial
// for some types, like decimals.
struct AggFuncRewriter;

impl AggFuncRewriter {
    fn plan_avg(expr: Expr, distinct: bool) -> Expr {
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: vec![expr.clone()],
            over: None,
            distinct,
        });
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["internal_avg_promotion".into()]),
            args: vec![sum],
            over: None,
            distinct: false,
        });
        let count = Expr::Function(Function {
            name: ObjectName(vec!["count".into()]),
            args: vec![expr],
            over: None,
            distinct,
        });
        Expr::BinaryOp {
            left: Box::new(sum),
            op: BinaryOperator::Divide,
            right: Box::new(count),
        }
    }

    fn plan_variance(expr: Expr, distinct: bool, sample: bool) -> Expr {
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
        let expr = Expr::Function(Function {
            name: ObjectName(vec!["internal_avg_promotion".into()]),
            args: vec![expr],
            over: None,
            distinct: false,
        });
        let sum_squares = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: vec![Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::Multiply,
                right: Box::new(expr.clone()),
            }],
            over: None,
            distinct,
        });
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: vec![expr.clone()],
            over: None,
            distinct,
        });
        let sum_squared = Expr::BinaryOp {
            left: Box::new(sum.clone()),
            op: BinaryOperator::Multiply,
            right: Box::new(sum),
        };
        let count = Expr::Function(Function {
            name: ObjectName(vec!["count".into()]),
            args: vec![expr],
            over: None,
            distinct,
        });
        Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(sum_squares),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(sum_squared),
                    op: BinaryOperator::Divide,
                    right: Box::new(count.clone()),
                }),
            }),
            op: BinaryOperator::Divide,
            right: Box::new(if sample {
                Expr::BinaryOp {
                    left: Box::new(count),
                    op: BinaryOperator::Minus,
                    right: Box::new(Expr::Value(Value::Number("1".into()))),
                }
            } else {
                count
            }),
        }
    }

    fn plan_stddev(expr: Expr, distinct: bool, sample: bool) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec!["sqrt".into()]),
            args: vec![Self::plan_variance(expr, distinct, sample)],
            over: None,
            distinct: false,
        })
    }

    fn rewrite_expr(expr: &Expr) -> Option<(Ident, Expr)> {
        let func = match expr {
            Expr::Function(func) => func,
            _ => return None,
        };
        let name = normalize::function_name(func.name.clone()).ok()?;
        if func.args.len() != 1 {
            return None;
        }
        let arg = func.args[0].clone();
        let expr = match name.as_str() {
            "avg" => Some(Self::plan_avg(arg, func.distinct)),
            "variance" | "var_samp" => Some(Self::plan_variance(arg, func.distinct, true)),
            "var_pop" => Some(Self::plan_variance(arg, func.distinct, false)),
            "stddev" | "stddev_samp" => Some(Self::plan_stddev(arg, func.distinct, true)),
            "stddev_pop" => Some(Self::plan_stddev(arg, func.distinct, false)),
            _ => None,
        };
        expr.map(|expr| (func.name.0[0].clone(), expr))
    }
}

impl<'ast> VisitMut<'ast> for AggFuncRewriter {
    fn visit_select_item(&mut self, item: &'ast mut SelectItem) {
        if let SelectItem::UnnamedExpr(expr) = item {
            visit_mut::visit_expr(self, expr);
            if let Some((alias, expr)) = Self::rewrite_expr(expr) {
                *item = SelectItem::ExprWithAlias { expr, alias }
            }
        } else {
            visit_mut::visit_select_item(self, item);
        }
    }

    fn visit_expr(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr(self, expr);
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
    fn visit_expr(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr(self, expr);
        if let Expr::Identifier(ident) = expr {
            if normalize::ident(ident.clone()) == "current_timestamp" {
                *expr = Expr::Function(Function {
                    name: ObjectName(vec!["current_timestamp".into()]),
                    args: vec![],
                    over: None,
                    distinct: false,
                })
            }
        }
    }
}
