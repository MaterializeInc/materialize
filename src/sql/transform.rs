// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Transformations of SQL ASTs.
//!
//! Most query optimizations are performed by the dataflow layer, but some
//! are much easier to perform in SQL. Someday, we'll want our own SQL IR,
//! but for now we just use the parser's AST directly.

use std::convert::TryFrom;

use sql_parser::ast::visit_mut::{self, VisitMut};
use sql_parser::ast::{BinaryOperator, Expr, Function, ObjectName, Query, Value};

use catalog::QualName;

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
            name: ObjectName(vec!["internal".into(), "avg_promotion".into()]),
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
            name: ObjectName(vec!["internal".into(), "avg_promotion".into()]),
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
}

impl<'ast> VisitMut<'ast> for AggFuncRewriter {
    fn visit_expr(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr(self, expr);
        if let Expr::Function(func) = expr {
            let name = match QualName::try_from(func.name.clone()) {
                Ok(name) => name,
                Err(_) => return,
            };
            if func.args.len() != 1 {
                return;
            }
            let arg = func.args[0].clone();
            if &name == "avg" {
                *expr = Self::plan_avg(arg, func.distinct)
            } else if &name == "variance" || &name == "var_samp" {
                *expr = Self::plan_variance(arg, func.distinct, true)
            } else if &name == "var_pop" {
                *expr = Self::plan_variance(arg, func.distinct, false)
            } else if &name == "stddev" || &name == "stddev_samp" {
                *expr = Self::plan_stddev(arg, func.distinct, true)
            } else if &name == "stddev_pop" {
                *expr = Self::plan_stddev(arg, func.distinct, false)
            }
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
            let name = ObjectName(vec![ident.clone()]);
            if QualName::name_equals(name, "current_timestamp") {
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
