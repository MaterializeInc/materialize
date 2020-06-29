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
    BinaryOperator, Expr, Function, FunctionArgs, Ident, ObjectName, Query, SelectItem, Value,
};

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
        Expr::BinaryOp {
            left: Box::new(lhs),
            op: BinaryOperator::Divide,
            right: Box::new(Self::plan_null_if(
                &rhs,
                &Expr::Value(Value::Number("0".into())),
            )),
        }
    }

    fn plan_avg(expr: &Expr, filter: Option<&Expr>, distinct: bool) -> Expr {
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: FunctionArgs::Args(vec![expr.clone()]),
            filter: filter.map(|e| Box::new(e.clone())),
            over: None,
            distinct,
        });
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["internal_avg_promotion".into()]),
            args: FunctionArgs::Args(vec![sum]),
            filter: None,
            over: None,
            distinct: false,
        });
        let count = Expr::Function(Function {
            name: ObjectName(vec!["count".into()]),
            args: FunctionArgs::Args(vec![expr.clone()]),
            filter: filter.map(|e| Box::new(e.clone())),
            over: None,
            distinct,
        });
        Self::plan_divide(sum, count)
    }

    fn plan_variance(expr: &Expr, filter: Option<&Expr>, distinct: bool, sample: bool) -> Expr {
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
            args: FunctionArgs::Args(vec![expr.clone()]),
            filter: None,
            over: None,
            distinct: false,
        });
        let sum_squares = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: FunctionArgs::Args(vec![Expr::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::Multiply,
                right: Box::new(expr.clone()),
            }]),
            filter: filter.map(|e| Box::new(e.clone())),
            over: None,
            distinct,
        });
        let sum = Expr::Function(Function {
            name: ObjectName(vec!["sum".into()]),
            args: FunctionArgs::Args(vec![expr.clone()]),
            filter: filter.map(|e| Box::new(e.clone())),
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
            args: FunctionArgs::Args(vec![expr]),
            filter: filter.map(|e| Box::new(e.clone())),
            over: None,
            distinct,
        });
        Self::plan_divide(
            Expr::BinaryOp {
                left: Box::new(sum_squares),
                op: BinaryOperator::Minus,
                right: Box::new(Self::plan_divide(sum_squared, count.clone())),
            },
            if sample {
                Expr::BinaryOp {
                    left: Box::new(count),
                    op: BinaryOperator::Minus,
                    right: Box::new(Expr::Value(Value::Number("1".into()))),
                }
            } else {
                count
            },
        )
    }

    fn plan_stddev(expr: &Expr, filter: Option<&Expr>, distinct: bool, sample: bool) -> Expr {
        Expr::Function(Function {
            name: ObjectName(vec!["sqrt".into()]),
            args: FunctionArgs::Args(vec![Self::plan_variance(expr, filter, distinct, sample)]),
            filter: None,
            over: None,
            distinct: false,
        })
    }

    fn plan_mod(left: &Expr, right: &Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOperator::Modulus,
            right: Box::new(right.clone()),
        }
    }

    fn plan_null_if(left: &Expr, right: &Expr) -> Expr {
        let condition = Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOperator::Eq,
            right: Box::new(right.clone()),
        };
        Expr::Case {
            operand: None,
            conditions: vec![condition],
            results: vec![Expr::Value(Value::Null)],
            else_result: Some(Box::new(left.clone())),
        }
    }

    fn rewrite_expr(expr: &Expr) -> Option<(Ident, Expr)> {
        let func = match expr {
            Expr::Function(func) => func,
            _ => return None,
        };
        let name = normalize::function_name(func.name.clone()).ok()?;
        let args = match &func.args {
            FunctionArgs::Star => return None,
            FunctionArgs::Args(args) => args,
        };
        let filter = func.filter.as_deref();
        let expr = match (name.as_str(), args.as_slice()) {
            ("avg", [arg]) => Some(Self::plan_avg(arg, filter, func.distinct)),
            ("mod", [lhs, rhs]) => Some(Self::plan_mod(lhs, rhs)),
            ("nullif", [lhs, rhs]) => Some(Self::plan_null_if(lhs, rhs)),
            ("variance", [arg]) | ("var_samp", [arg]) => {
                Some(Self::plan_variance(arg, filter, func.distinct, true))
            }
            ("var_pop", [arg]) => Some(Self::plan_variance(arg, filter, func.distinct, false)),
            ("stddev", [arg]) | ("stddev_samp", [arg]) => {
                Some(Self::plan_stddev(arg, filter, func.distinct, true))
            }
            ("stddev_pop", [arg]) => Some(Self::plan_stddev(arg, filter, func.distinct, false)),
            _ => None,
        };
        expr.map(|expr| (func.name.0[0].clone(), expr))
    }
}

impl<'ast> VisitMut<'ast> for FuncRewriter {
    fn visit_select_item_mut(&mut self, item: &'ast mut SelectItem) {
        if let SelectItem::Expr { expr, alias: None } = item {
            visit_mut::visit_expr_mut(self, expr);
            if let Some((alias, expr)) = Self::rewrite_expr(expr) {
                *item = SelectItem::Expr {
                    expr,
                    alias: Some(alias),
                };
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
                *expr = Expr::Function(Function {
                    name: ObjectName(vec!["current_timestamp".into()]),
                    args: FunctionArgs::Args(vec![]),
                    filter: None,
                    over: None,
                    distinct: false,
                })
            }
        }
    }
}
