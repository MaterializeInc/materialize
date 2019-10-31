// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Transformations of SQL ASTs.
//!
//! Most query optimizations are performed by the dataflow layer, but some
//! are much easier to perform in SQL. Someday, we'll want our own SQL IR,
//! but for now we just use the parser's AST directly.

use sqlparser::ast::visit_mut::{self, VisitMut};
use sqlparser::ast::{BinaryOperator, Expr, Function, ObjectName, Query};

pub fn transform(query: &mut Query) {
    AvgFuncRewriter.visit_query(query);
}

// Rewrites `avg(col)` to `sum(col) / count(col)`, so that we can pretend the
// `avg` aggregate function doesn't exist from here on out. This also has the
// nice side effect of reusing the division planning logic, which is not trivial
// for some types, like decimals.
struct AvgFuncRewriter;

impl<'ast> VisitMut<'ast> for AvgFuncRewriter {
    fn visit_expr(&mut self, expr: &'ast mut Expr) {
        visit_mut::visit_expr(self, expr);
        if let Expr::Function(func) = expr {
            if func.name.to_string().to_lowercase() == "avg" {
                let args = func.args.clone();
                let sum = Expr::Function(Function {
                    name: ObjectName(vec!["sum".into()]),
                    args: args.clone(),
                    over: None,
                    distinct: func.distinct,
                });
                let sum = Expr::Function(Function {
                    name: ObjectName(vec!["internal".into(), "avg_promotion".into()]),
                    args: vec![sum],
                    over: None,
                    distinct: false,
                });
                let count = Expr::Function(Function {
                    name: ObjectName(vec!["count".into()]),
                    args: args.clone(),
                    over: None,
                    distinct: func.distinct,
                });
                let div = Expr::BinaryOp {
                    left: Box::new(sum),
                    op: BinaryOperator::Divide,
                    right: Box::new(count),
                };
                *expr = Expr::Nested(Box::new(div));
            }
        }
    }
}
