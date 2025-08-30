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

use itertools::Itertools;
use mz_ore::id_gen::IdGen;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::namespaces::{MZ_CATALOG_SCHEMA, MZ_UNSAFE_SCHEMA, PG_CATALOG_SCHEMA};
use mz_sql_parser::ast::visit_mut::{self, VisitMut, VisitMutNode};
use mz_sql_parser::ast::{
    Expr, Function, FunctionArgs, HomogenizingFunction, Ident, IsExprConstruct, Op, OrderByExpr,
    Query, Select, SelectItem, TableAlias, TableFactor, TableWithJoins, Value, WindowSpec,
};
use mz_sql_parser::ident;

use crate::names::{Aug, PartialItemName, ResolvedDataType, ResolvedItemName};
use crate::plan::{PlanError, StatementContext};
use crate::{ORDINALITY_COL_NAME, normalize};

pub(crate) fn transform<N>(scx: &StatementContext, node: &mut N) -> Result<(), PlanError>
where
    N: for<'a> VisitMutNode<'a, Aug>,
{
    let mut func_rewriter = FuncRewriter::new(scx);
    node.visit_mut(&mut func_rewriter);
    func_rewriter.status?;

    let mut desugarer = Desugarer::new(scx);
    node.visit_mut(&mut desugarer);
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
    rewriting_table_factor: bool,
}

impl<'a> FuncRewriter<'a> {
    fn new(scx: &'a StatementContext<'a>) -> FuncRewriter<'a> {
        FuncRewriter {
            scx,
            status: Ok(()),
            rewriting_table_factor: false,
        }
    }

    fn resolve_known_valid_data_type(&self, name: &PartialItemName) -> ResolvedDataType {
        let item = self
            .scx
            .catalog
            .resolve_type(name)
            .expect("data type known to be valid");
        let full_name = self.scx.catalog.resolve_full_name(item.name());
        ResolvedDataType::Named {
            id: item.id(),
            qualifiers: item.name().qualifiers.clone(),
            full_name,
            modifiers: vec![],
            print_id: true,
        }
    }

    fn int32_data_type(&self) -> ResolvedDataType {
        self.resolve_known_valid_data_type(&PartialItemName {
            database: None,
            schema: Some(PG_CATALOG_SCHEMA.into()),
            item: "int4".into(),
        })
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
        &mut self,
        name: ResolvedItemName,
        expr: Expr<Aug>,
        order_by: Vec<OrderByExpr<Aug>>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        if self.rewriting_table_factor && self.status.is_ok() {
            self.status = Err(PlanError::Unstructured(
                "aggregate functions are not supported in functions in FROM".to_string(),
            ))
        }
        Expr::Function(Function {
            name,
            args: FunctionArgs::Args {
                args: vec![expr],
                order_by,
            },
            filter,
            over,
            distinct,
        })
    }

    fn plan_avg(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        let sum = self
            .plan_agg(
                self.scx
                    .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
                expr.clone(),
                vec![],
                filter.clone(),
                distinct,
                over.clone(),
            )
            .call_unary(
                self.scx
                    .dangerous_resolve_name(vec![MZ_UNSAFE_SCHEMA, "mz_avg_promotion"]),
            );
        let count = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "count"]),
            expr,
            vec![],
            filter,
            distinct,
            over,
        );
        Self::plan_divide(sum, count)
    }

    /// Same as `plan_avg` but internally uses `mz_avg_promotion_internal_v1`.
    fn plan_avg_internal_v1(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        let sum = self
            .plan_agg(
                self.scx
                    .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
                expr.clone(),
                vec![],
                filter.clone(),
                distinct,
                over.clone(),
            )
            .call_unary(
                self.scx
                    .dangerous_resolve_name(vec![MZ_UNSAFE_SCHEMA, "mz_avg_promotion_internal_v1"]),
            );
        let count = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "count"]),
            expr,
            vec![],
            filter,
            distinct,
            over,
        );
        Self::plan_divide(sum, count)
    }

    fn plan_variance(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        sample: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        // N.B. this variance calculation uses the "textbook" algorithm, which
        // is known to accumulate problematic amounts of error. The numerically
        // stable variants, the most well-known of which is Welford's, are
        // however difficult to implement inside of Differential Dataflow, as
        // they do not obviously support retractions efficiently (database-issues#436).
        //
        // The code below converts var_samp(x) into
        //
        //     (sum(x²) - sum(x)² / count(x)) / (count(x) - 1)
        //
        // and var_pop(x) into:
        //
        //     (sum(x²) - sum(x)² / count(x)) / count(x)
        //
        let expr = expr.call_unary(
            self.scx
                .dangerous_resolve_name(vec![MZ_UNSAFE_SCHEMA, "mz_avg_promotion"]),
        );
        let expr_squared = expr.clone().multiply(expr.clone());
        let sum_squares = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
            expr_squared,
            vec![],
            filter.clone(),
            distinct,
            over.clone(),
        );
        let sum = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
            expr.clone(),
            vec![],
            filter.clone(),
            distinct,
            over.clone(),
        );
        let sum_squared = sum.clone().multiply(sum);
        let count = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "count"]),
            expr,
            vec![],
            filter,
            distinct,
            over,
        );
        let result = Self::plan_divide(
            sum_squares.minus(Self::plan_divide(sum_squared, count.clone())),
            if sample {
                count.minus(Expr::number("1"))
            } else {
                count
            },
        );
        // Result is _basically_ what we want, except
        // that due to numerical inaccuracy, it might be a negative
        // number very close to zero when it should mathematically be zero.
        // This makes it so `stddev` fails as it tries to take the square root
        // of a negative number.
        // So, we need the following logic:
        // If `result` is NULL, return NULL (no surprise here)
        // Otherwise, if `result` is >0, return `result` (no surprise here either)
        // Otherwise, return 0.
        //
        // Unfortunately, we can't use `GREATEST` directly for this,
        // since `greatest(NULL, 0)` is 0, not NULL, so we need to
        // create a `Case` expression that computes `result`
        // twice. Hopefully the optimizer can deal with this!
        let result_is_null = Expr::IsExpr {
            expr: Box::new(result.clone()),
            construct: IsExprConstruct::Null,
            negated: false,
        };
        Expr::Case {
            operand: None,
            conditions: vec![result_is_null],
            results: vec![Expr::Value(Value::Null)],
            else_result: Some(Box::new(Expr::HomogenizingFunction {
                function: HomogenizingFunction::Greatest,
                exprs: vec![result, Expr::number("0")],
            })),
        }
    }

    fn plan_stddev(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        sample: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        self.plan_variance(expr, filter, distinct, sample, over)
            .call_unary(
                self.scx
                    .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sqrt"]),
            )
    }

    fn plan_bool_and(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        // The code below converts `bool_and(x)` into:
        //
        //     sum((NOT x)::int4) = 0
        //
        // It is tempting to use `count` instead, but count does not return NULL
        // when all input values are NULL, as required.
        //
        // The `NOT x` expression has the side effect of implicitly casting `x`
        // to `bool`. We intentionally do not write `NOT x::bool`, because that
        // would perform an explicit cast, and to match PostgreSQL we must
        // perform only an implicit cast.
        let sum = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
            expr.negate().cast(self.int32_data_type()),
            vec![],
            filter,
            distinct,
            over,
        );
        sum.equals(Expr::Value(Value::Number(0.to_string())))
    }

    fn plan_bool_or(
        &mut self,
        expr: Expr<Aug>,
        filter: Option<Box<Expr<Aug>>>,
        distinct: bool,
        over: Option<WindowSpec<Aug>>,
    ) -> Expr<Aug> {
        // The code below converts `bool_or(x)`z into:
        //
        //     sum((x OR false)::int4) > 0
        //
        // It is tempting to use `count` instead, but count does not return NULL
        // when all input values are NULL, as required.
        //
        // The `(x OR false)` expression implicitly casts `x` to `bool` without
        // changing its logical value. It is tempting to use `x::bool` instead,
        // but that performs an explicit cast, and to match PostgreSQL we must
        // perform only an implicit cast.
        let sum = self.plan_agg(
            self.scx
                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "sum"]),
            expr.or(Expr::Value(Value::Boolean(false)))
                .cast(self.int32_data_type()),
            vec![],
            filter,
            distinct,
            over,
        );
        sum.gt(Expr::Value(Value::Number(0.to_string())))
    }

    fn rewrite_function(&mut self, func: &Function<Aug>) -> Option<(Ident, Expr<Aug>)> {
        if let Function {
            name,
            args: FunctionArgs::Args { args, order_by: _ },
            filter,
            distinct,
            over,
        } = func
        {
            let pg_catalog_id = self
                .scx
                .catalog
                .resolve_schema(None, PG_CATALOG_SCHEMA)
                .expect("pg_catalog schema exists")
                .id();
            let mz_catalog_id = self
                .scx
                .catalog
                .resolve_schema(None, MZ_CATALOG_SCHEMA)
                .expect("mz_catalog schema exists")
                .id();
            let name = match name {
                ResolvedItemName::Item {
                    qualifiers,
                    full_name,
                    ..
                } => {
                    if ![*pg_catalog_id, *mz_catalog_id].contains(&qualifiers.schema_spec) {
                        return None;
                    }
                    full_name.item.clone()
                }
                _ => unreachable!(),
            };

            let filter = filter.clone();
            let distinct = *distinct;
            let over = over.clone();
            let expr = if args.len() == 1 {
                let arg = args[0].clone();
                match name.as_str() {
                    "avg_internal_v1" => self.plan_avg_internal_v1(arg, filter, distinct, over),
                    "avg" => self.plan_avg(arg, filter, distinct, over),
                    "variance" | "var_samp" => {
                        self.plan_variance(arg, filter, distinct, true, over)
                    }
                    "var_pop" => self.plan_variance(arg, filter, distinct, false, over),
                    "stddev" | "stddev_samp" => self.plan_stddev(arg, filter, distinct, true, over),
                    "stddev_pop" => self.plan_stddev(arg, filter, distinct, false, over),
                    "bool_and" => self.plan_bool_and(arg, filter, distinct, over),
                    "bool_or" => self.plan_bool_or(arg, filter, distinct, over),
                    _ => return None,
                }
            } else if args.len() == 2 {
                let (lhs, rhs) = (args[0].clone(), args[1].clone());
                match name.as_str() {
                    "mod" => lhs.modulo(rhs),
                    "pow" => Expr::call(
                        self.scx
                            .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, "power"]),
                        vec![lhs, rhs],
                    ),
                    _ => return None,
                }
            } else {
                return None;
            };
            Some((Ident::new_unchecked(name), expr))
        } else {
            None
        }
    }

    fn rewrite_expr(&mut self, expr: &Expr<Aug>) -> Option<(Ident, Expr<Aug>)> {
        match expr {
            Expr::Function(function) => self.rewrite_function(function),
            // Rewrites special keywords that SQL considers to be function calls
            // to actual function calls. For example, `SELECT current_timestamp`
            // is rewritten to `SELECT current_timestamp()`.
            Expr::Identifier(ident) if ident.len() == 1 => {
                let ident = normalize::ident(ident[0].clone());
                let fn_ident = match ident.as_str() {
                    "current_role" => Some("current_user"),
                    "current_schema" | "current_timestamp" | "current_user" | "session_user"
                    | "current_catalog" => Some(ident.as_str()),
                    _ => None,
                };
                match fn_ident {
                    None => None,
                    Some(fn_ident) => {
                        let expr = Expr::call_nullary(
                            self.scx
                                .dangerous_resolve_name(vec![PG_CATALOG_SCHEMA, fn_ident]),
                        );
                        Some((Ident::new_unchecked(ident), expr))
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

    fn visit_table_with_joins_mut(&mut self, item: &'ast mut TableWithJoins<Aug>) {
        visit_mut::visit_table_with_joins_mut(self, item);
        match &mut item.relation {
            TableFactor::Function {
                function,
                alias,
                with_ordinality,
            } => {
                self.rewriting_table_factor = true;
                // Functions that get rewritten must be rewritten as exprs
                // because their catalog functions cannot be planned.
                if let Some((ident, expr)) = self.rewrite_function(function) {
                    let mut select = Select::default().project(SelectItem::Expr {
                        expr,
                        alias: Some(match &alias {
                            Some(TableAlias { name, columns, .. }) => {
                                columns.get(0).unwrap_or(name).clone()
                            }
                            None => ident,
                        }),
                    });

                    if *with_ordinality {
                        select = select.project(SelectItem::Expr {
                            expr: Expr::Value(Value::Number("1".into())),
                            alias: Some(ident!(ORDINALITY_COL_NAME)),
                        });
                    }

                    item.relation = TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(Query {
                            ctes: mz_sql_parser::ast::CteBlock::Simple(vec![]),
                            body: mz_sql_parser::ast::SetExpr::Select(Box::new(select)),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                        }),
                        alias: alias.clone(),
                    }
                }
                self.rewriting_table_factor = false;
            }
            _ => {}
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
struct Desugarer<'a> {
    scx: &'a StatementContext<'a>,
    status: Result<(), PlanError>,
    id_gen: IdGen,
    recursion_guard: RecursionGuard,
}

impl<'a> CheckedRecursion for Desugarer<'a> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<'a, 'ast> VisitMut<'ast, Aug> for Desugarer<'a> {
    fn visit_expr_mut(&mut self, expr: &'ast mut Expr<Aug>) {
        self.visit_internal(Self::visit_expr_mut_internal, expr);
    }
}

impl<'a> Desugarer<'a> {
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

    fn new(scx: &'a StatementContext) -> Desugarer<'a> {
        Desugarer {
            scx,
            status: Ok(()),
            id_gen: Default::default(),
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
                *expr = Expr::lt(*e.clone(), low.take()).or(e.take().gt(high.take()));
            } else {
                *expr = e.clone().gt_eq(low.take()).and(e.take().lt_eq(high.take()));
            }
        }

        // When `$expr` is a `ROW` constructor, we need to desugar as described
        // below in order to enable the row comparision expansion at the end of
        // this function. We don't do this desugaring unconditionally (i.e.,
        // when `$expr` is not a `ROW` constructor) because the implementation
        // in `plan_in_list` is more efficient when row comparison expansion is
        // not required.
        //
        // `$expr IN ($list)` => `$expr = $list[0] OR $expr = $list[1] ... OR $expr = $list[n]`
        // `$expr NOT IN ($list)` => `$expr <> $list[0] AND $expr <> $list[1] ... AND $expr <> $list[n]`
        if let Expr::InList {
            expr: e,
            list,
            negated,
        } = expr
        {
            if let Expr::Row { .. } = &**e {
                if *negated {
                    *expr = list
                        .drain(..)
                        .map(|r| e.clone().not_equals(r))
                        .reduce(|e1, e2| e1.and(e2))
                        .expect("list known to contain at least one element");
                } else {
                    *expr = list
                        .drain(..)
                        .map(|r| e.clone().equals(r))
                        .reduce(|e1, e2| e1.or(e2))
                        .expect("list known to contain at least one element");
                }
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
            let binding = ident!("elem");

            let subquery = Query::select(
                Select::default()
                    .from(TableWithJoins {
                        relation: TableFactor::Function {
                            function: Function {
                                name: self
                                    .scx
                                    .dangerous_resolve_name(vec![MZ_CATALOG_SCHEMA, "unnest"]),
                                args: FunctionArgs::args(vec![right.take()]),
                                filter: None,
                                over: None,
                                distinct: false,
                            },
                            alias: Some(TableAlias {
                                name: ident!("_"),
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
        // `(SELECT mz_unsafe.mz_all($expr = $binding) FROM ($subquery) AS _ ($binding))
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
                // Note: using unchecked is okay here because we know the value will be less than
                // our maximum length.
                .map(|col| {
                    let unique_id = self.id_gen.allocate_id();
                    Ident::new_unchecked(format!("right_col{col}_{unique_id}"))
                })
                .collect();

            let subquery_unique_id = self.id_gen.allocate_id();
            // Note: kay to use unchecked here because we know the value will be small enough.
            let subquery_name = Ident::new_unchecked(format!("subquery{subquery_unique_id}"));
            let select = Select::default()
                .from(TableWithJoins::subquery(
                    right.take(),
                    TableAlias {
                        name: subquery_name,
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
                        .call_unary(self.scx.dangerous_resolve_name(match expr {
                            Expr::AnySubquery { .. } => vec![MZ_UNSAFE_SCHEMA, "mz_any"],
                            Expr::AllSubquery { .. } => vec![MZ_UNSAFE_SCHEMA, "mz_all"],
                            _ => unreachable!(),
                        })),
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
                        let mut pairs = left.iter_mut().zip_eq(right);
                        let mut new = pairs
                            .next()
                            .map(|(l, r)| l.take().equals(r.take()))
                            .expect("cannot compare rows of zero length");
                        for (l, r) in pairs {
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
                        for (l, r) in left
                            .iter_mut()
                            .rev()
                            .zip_eq(right.into_iter().rev())
                            .skip(1)
                        {
                            new = l
                                .clone()
                                .binop(Op::bare(strict_op), r.clone())
                                .or(l.take().equals(r.take()).and(new));
                        }
                        *expr = new;
                    }
                    _ if left.len() == 1 && right.len() == 1 => {
                        let left = left.remove(0);
                        let right = right.remove(0);
                        *expr = left.binop(op.clone(), right);
                    }
                    _ => (),
                }
            }
        }

        visit_mut::visit_expr_mut(self, expr);
        Ok(())
    }
}
