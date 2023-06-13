// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::collections::CollectionExt;
use proc_macro2::LineColumn;
use syn::parse::{Parse, ParseStream, Parser};
use syn::spanned::Spanned;
use syn::Error;

use super::TestCatalog;

use self::util::*;

/// Builds a [mz_expr::MirRelationExpr] from a string.
pub fn try_parse_mir(catalog: &TestCatalog, s: &str) -> Result<mz_expr::MirRelationExpr, String> {
    // Define a Parser that constructs a (read-only) parsing context `ctx` and
    // delegates to `relation::parse_expr` by passing a `ctx` as a shared ref.
    let parser = move |input: ParseStream| {
        let ctx = Ctx { catalog };
        relation::parse_expr(&ctx, input)
    };
    // Since the syn lexer doesn't parse comments, we replace all `// {`
    // occurrences in the input string with `:: {`.
    let s = s.replace("// {", ":: {");
    // Call the parser with the given input string.
    let mut expr = parser.parse_str(&s).map_err(|err| {
        let (line, column) = (err.span().start().line, err.span().start().column);
        format!("parse error at {line}:{column}:\n{err}\n")
    })?;
    // Fix the types of the local let bindings of the parsed expression in a
    // post-processing pass.
    relation::fix_types(&mut expr, &mut relation::FixTypesCtx::default())?;
    // Return the parsed, post-processed expression.
    Ok(expr)
}

/// Builds a source definition from a string.
pub fn try_parse_def(catalog: &TestCatalog, s: &str) -> Result<Def, String> {
    // Define a Parser that constructs a (read-only) parsing context `ctx` and
    // delegates to `relation::parse_expr` by passing a `ctx` as a shared ref.
    let parser = move |input: ParseStream| {
        let ctx = Ctx { catalog };
        def::parse_def(&ctx, input)
    };
    // Call the parser with the given input string.
    let def = parser.parse_str(s).map_err(|err| {
        let (line, column) = (err.span().start().line, err.span().start().column);
        format!("parse error at {line}:{column}:\n{err}\n")
    })?;
    // Return the parsed, post-processed expression.
    Ok(def)
}

/// Support for parsing [mz_expr::MirRelationExpr].
mod relation {
    use std::collections::BTreeMap;

    use mz_expr::{Id, JoinImplementation, LocalId, MirRelationExpr};
    use mz_repr::{Diff, RelationType, Row, ScalarType};

    use super::*;

    type Result = syn::Result<MirRelationExpr>;

    pub fn parse_expr(ctx: CtxRef, input: ParseStream) -> Result {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::Constant) {
            parse_constant(ctx, input)
        } else if lookahead.peek(kw::Get) {
            parse_get(ctx, input)
        } else if lookahead.peek(kw::Return) {
            parse_let(ctx, input)
        } else if lookahead.peek(kw::Project) {
            parse_project(ctx, input)
        } else if lookahead.peek(kw::Map) {
            parse_map(ctx, input)
        } else if lookahead.peek(kw::FlatMap) {
            parse_flat_map(ctx, input)
        } else if lookahead.peek(kw::Filter) {
            parse_filter(ctx, input)
        } else if lookahead.peek(kw::CrossJoin) {
            parse_cross_join(ctx, input)
        } else if lookahead.peek(kw::Join) {
            parse_join(ctx, input)
        } else if lookahead.peek(kw::Distinct) {
            parse_distinct(ctx, input)
        } else if lookahead.peek(kw::Reduce) {
            parse_reduce(ctx, input)
        } else if lookahead.peek(kw::TopK) {
            parse_top_k(ctx, input)
        } else if lookahead.peek(kw::Negate) {
            parse_negate(ctx, input)
        } else if lookahead.peek(kw::Threshold) {
            parse_threshold(ctx, input)
        } else if lookahead.peek(kw::Union) {
            parse_union(ctx, input)
        } else if lookahead.peek(kw::ArrangeBy) {
            parse_arrange_by(ctx, input)
        } else {
            Err(lookahead.error())
        }
    }

    fn parse_constant(ctx: CtxRef, input: ParseStream) -> Result {
        let constant = input.parse::<kw::Constant>()?;

        let parse_typ = |input: ParseStream| -> syn::Result<RelationType> {
            let attrs = attributes::parse_attributes(input)?;
            let Some(column_types) = attrs.types else {
                let msg = "Missing expected `types` attribute for Constant line";
                Err(Error::new(input.span(), msg))?
            };
            let keys = attrs.keys.unwrap_or_default();
            Ok(RelationType { column_types, keys })
        };

        if input.eat3(syn::Token![<], kw::empty, syn::Token![>]) {
            let typ = parse_typ(input)?;
            Ok(MirRelationExpr::constant(vec![], typ))
        } else {
            let typ = parse_typ(input)?;
            let parse_children = ParseChildren::new(input, constant.span().start());
            let rows = Ok(parse_children.parse_many(ctx, parse_constant_entry)?);
            Ok(MirRelationExpr::Constant { rows, typ })
        }
    }

    fn parse_constant_entry(_ctx: CtxRef, input: ParseStream) -> syn::Result<(Row, Diff)> {
        input.parse::<syn::Token![-]>()?;

        let (row, diff);

        let inner1;
        syn::parenthesized!(inner1 in input);

        if inner1.peek(syn::token::Paren) {
            let inner2;
            syn::parenthesized!(inner2 in inner1);
            row = inner2.parse::<Parsed<Row>>()?.into();
            inner1.parse::<kw::x>()?;
            diff = match inner1.parse::<syn::Lit>()? {
                syn::Lit::Int(l) => Ok(l.base10_parse::<Diff>()?),
                _ => Err(Error::new(inner1.span(), "expected Diff literal")),
            }?;
        } else {
            row = inner1.parse::<Parsed<Row>>()?.into();
            diff = 1;
        }

        Ok((row, diff))
    }

    fn parse_get(ctx: CtxRef, input: ParseStream) -> Result {
        input.parse::<kw::Get>()?;

        let ident = input.parse::<syn::Ident>()?;
        match ctx.catalog.get(&ident.to_string()) {
            Some((id, typ)) => Ok(MirRelationExpr::Get {
                id: Id::Global(*id),
                typ: typ.clone(),
            }),
            None => Ok(MirRelationExpr::Get {
                id: Id::Local(parse_local_id(ident)?),
                typ: RelationType::empty(),
            }),
        }
    }

    fn parse_let(ctx: CtxRef, input: ParseStream) -> Result {
        let return_ = input.parse::<kw::Return>()?;
        let parse_body = ParseChildren::new(input, return_.span().start());
        let mut body = parse_body.parse_one(ctx, parse_expr)?;

        let with = input.parse::<kw::With>()?;
        let recursive = input.eat2(kw::Mutually, kw::Recursive);
        let parse_ctes = ParseChildren::new(input, with.span().start());
        let ctes = parse_ctes.parse_many(ctx, parse_cte)?;

        if ctes.is_empty() {
            let msg = "At least one `let cte` binding expected";
            Err(Error::new(input.span(), msg))?
        }

        if recursive {
            let (mut ids, mut values, mut limits) = (vec![], vec![], vec![]);
            for (id, attrs, value) in ctes.into_iter().rev() {
                let typ = {
                    let Some(column_types) = attrs.types else {
                        let msg = format!("`let {}` needs a `types` attribute", id);
                        Err(Error::new(with.span(), msg))?
                    };
                    let keys = attrs.keys.unwrap_or_default();
                    RelationType { column_types, keys }
                };

                // An ugly-ugly hack to pass the type information of the WMR CTE
                // to the `fix_types` pass.
                let value = {
                    let get_cte = MirRelationExpr::Get {
                        id: Id::Local(id),
                        typ: typ.clone(),
                    };
                    // Do not use the `union` smart constructor here!
                    MirRelationExpr::Union {
                        base: Box::new(get_cte),
                        inputs: vec![value],
                    }
                };

                ids.push(id);
                values.push(value);
                limits.push(None); // TODO: support limits
            }

            Ok(MirRelationExpr::LetRec {
                ids,
                values,
                limits,
                body: Box::new(body),
            })
        } else {
            for (id, _, value) in ctes.into_iter() {
                body = MirRelationExpr::Let {
                    id,
                    value: Box::new(value),
                    body: Box::new(body),
                };
            }
            Ok(body)
        }
    }

    fn parse_cte(
        ctx: CtxRef,
        input: ParseStream,
    ) -> syn::Result<(LocalId, attributes::Attributes, MirRelationExpr)> {
        let cte = input.parse::<kw::cte>()?;

        let ident = input.parse::<syn::Ident>()?;
        let id = parse_local_id(ident)?;

        input.parse::<syn::Token![=]>()?;

        let attrs = attributes::parse_attributes(input)?;

        let parse_value = ParseChildren::new(input, cte.span().start());
        let value = parse_value.parse_one(ctx, parse_expr)?;

        Ok((id, attrs, value))
    }

    fn parse_project(ctx: CtxRef, input: ParseStream) -> Result {
        let project = input.parse::<kw::Project>()?;

        let content;
        syn::parenthesized!(content in input);
        let outputs = content.parse_comma_sep(scalar::parse_column_index)?;
        let parse_input = ParseChildren::new(input, project.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Project { input, outputs })
    }

    fn parse_map(ctx: CtxRef, input: ParseStream) -> Result {
        let map = input.parse::<kw::Map>()?;

        let scalars = {
            let inner;
            syn::parenthesized!(inner in input);
            scalar::parse_exprs(&inner)?
        };

        let parse_input = ParseChildren::new(input, map.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Map { input, scalars })
    }

    fn parse_flat_map(ctx: CtxRef, input: ParseStream) -> Result {
        use mz_expr::TableFunc::*;

        let flat_map = input.parse::<kw::FlatMap>()?;

        let ident = input.parse::<syn::Ident>()?;
        let func = match ident.to_string().to_lowercase().as_str() {
            "unnest_list" => UnnestList {
                el_typ: mz_repr::ScalarType::Bool, // FIXME
            },
            "wrap1" => Wrap {
                types: vec![
                    ScalarType::Int64.nullable(true), // FIXME
                ],
                width: 1,
            },
            "wrap2" => Wrap {
                types: vec![
                    ScalarType::Int64.nullable(true), // FIXME
                    ScalarType::Int64.nullable(true), // FIXME
                ],
                width: 2,
            },
            "generate_series" => GenerateSeriesInt64,
            "jsonb_object_keys" => JsonbObjectKeys,
            _ => Err(Error::new(ident.span(), "unsupported function name"))?,
        };

        let exprs = {
            let inner;
            syn::parenthesized!(inner in input);
            scalar::parse_exprs(&inner)?
        };

        let parse_input = ParseChildren::new(input, flat_map.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::FlatMap { input, func, exprs })
    }

    fn parse_filter(ctx: CtxRef, input: ParseStream) -> Result {
        use mz_expr::MirScalarExpr::CallVariadic;
        use mz_expr::VariadicFunc::And;

        let filter = input.parse::<kw::Filter>()?;

        let predicates = match scalar::parse_expr(input)? {
            CallVariadic { func: And, exprs } => exprs,
            expr => vec![expr],
        };

        let parse_input = ParseChildren::new(input, filter.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Filter { input, predicates })
    }

    fn parse_cross_join(ctx: CtxRef, input: ParseStream) -> Result {
        let join = input.parse::<kw::CrossJoin>()?;

        let parse_inputs = ParseChildren::new(input, join.span().start());
        let inputs = parse_inputs.parse_many(ctx, parse_expr)?;

        Ok(MirRelationExpr::Join {
            inputs,
            equivalences: vec![],
            implementation: JoinImplementation::Unimplemented,
        })
    }

    fn parse_join(ctx: CtxRef, input: ParseStream) -> Result {
        let join = input.parse::<kw::Join>()?;

        input.parse::<kw::on>()?;
        input.parse::<syn::Token![=]>()?;
        let inner;
        syn::parenthesized!(inner in input);
        let equivalences = scalar::parse_join_equivalences(&inner)?;

        let parse_inputs = ParseChildren::new(input, join.span().start());
        let inputs = parse_inputs.parse_many(ctx, parse_expr)?;

        Ok(MirRelationExpr::Join {
            inputs,
            equivalences,
            implementation: JoinImplementation::Unimplemented,
        })
    }

    fn parse_distinct(ctx: CtxRef, input: ParseStream) -> Result {
        let reduce = input.parse::<kw::Distinct>()?;

        let group_key = if input.eat(kw::group_by) {
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(scalar::parse_expr)?
        } else {
            vec![]
        };

        let monotonic = input.eat(kw::monotonic);

        let expected_group_size = if input.eat(kw::exp_group_size) {
            input.parse::<syn::Token![=]>()?;
            Some(input.parse::<syn::LitInt>()?.base10_parse::<u64>()?)
        } else {
            None
        };

        let parse_inputs = ParseChildren::new(input, reduce.span().start());
        let input = Box::new(parse_inputs.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates: vec![],
            monotonic,
            expected_group_size,
        })
    }

    fn parse_reduce(ctx: CtxRef, input: ParseStream) -> Result {
        let reduce = input.parse::<kw::Reduce>()?;

        let group_key = if input.eat(kw::group_by) {
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(scalar::parse_expr)?
        } else {
            vec![]
        };

        let aggregates = {
            input.parse::<kw::aggregates>()?;
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(aggregate::parse_expr)?
        };

        let monotonic = input.eat(kw::monotonic);

        let expected_group_size = if input.eat(kw::exp_group_size) {
            input.parse::<syn::Token![=]>()?;
            Some(input.parse::<syn::LitInt>()?.base10_parse::<u64>()?)
        } else {
            None
        };

        let parse_inputs = ParseChildren::new(input, reduce.span().start());
        let input = Box::new(parse_inputs.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        })
    }

    fn parse_top_k(ctx: CtxRef, input: ParseStream) -> Result {
        let top_k = input.parse::<kw::TopK>()?;

        let group_key = if input.eat(kw::group_by) {
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(scalar::parse_column_index)?
        } else {
            vec![]
        };

        let order_key = if input.eat(kw::order_by) {
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(scalar::parse_column_order)?
        } else {
            vec![]
        };

        let limit = if input.eat(kw::limit) {
            input.parse::<syn::Token![=]>()?;
            Some(input.parse::<syn::LitInt>()?.base10_parse::<usize>()?)
        } else {
            None
        };

        let offset = if input.eat(kw::offset) {
            input.parse::<syn::Token![=]>()?;
            input.parse::<syn::LitInt>()?.base10_parse::<usize>()?
        } else {
            0
        };

        let monotonic = input.eat(kw::monotonic);

        let expected_group_size = if input.eat(kw::exp_group_size) {
            input.parse::<syn::Token![=]>()?;
            Some(input.parse::<syn::LitInt>()?.base10_parse::<u64>()?)
        } else {
            None
        };

        let parse_inputs = ParseChildren::new(input, top_k.span().start());
        let input = Box::new(parse_inputs.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
            expected_group_size,
        })
    }

    fn parse_negate(ctx: CtxRef, input: ParseStream) -> Result {
        let negate = input.parse::<kw::Negate>()?;

        let parse_input = ParseChildren::new(input, negate.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Negate { input })
    }

    fn parse_threshold(ctx: CtxRef, input: ParseStream) -> Result {
        let threshold = input.parse::<kw::Threshold>()?;

        let parse_input = ParseChildren::new(input, threshold.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::Threshold { input })
    }

    fn parse_union(ctx: CtxRef, input: ParseStream) -> Result {
        let union = input.parse::<kw::Union>()?;

        let parse_inputs = ParseChildren::new(input, union.span().start());
        let mut children = parse_inputs.parse_many(ctx, parse_expr)?;
        let inputs = children.split_off(1);
        let base = Box::new(children.into_element());

        Ok(MirRelationExpr::Union { base, inputs })
    }

    fn parse_arrange_by(ctx: CtxRef, input: ParseStream) -> Result {
        let arrange_by = input.parse::<kw::ArrangeBy>()?;

        let keys = {
            input.parse::<kw::keys>()?;
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(|input| {
                let inner;
                syn::bracketed!(inner in input);
                scalar::parse_exprs(&inner)
            })?
        };

        let parse_input = ParseChildren::new(input, arrange_by.span().start());
        let input = Box::new(parse_input.parse_one(ctx, parse_expr)?);

        Ok(MirRelationExpr::ArrangeBy { input, keys })
    }

    fn parse_local_id(ident: syn::Ident) -> syn::Result<LocalId> {
        if ident.to_string().starts_with('l') {
            let n = ident.to_string()[1..]
                .parse::<u64>()
                .map_err(|err| Error::new(ident.span(), err.to_string()))?;
            Ok(mz_expr::LocalId::new(n))
        } else {
            Err(Error::new(ident.span(), "invalid LocalId"))
        }
    }

    #[derive(Default)]
    pub struct FixTypesCtx {
        env: BTreeMap<LocalId, RelationType>,
        typ: Vec<RelationType>,
    }

    pub fn fix_types(
        expr: &mut MirRelationExpr,
        ctx: &mut FixTypesCtx,
    ) -> std::result::Result<(), String> {
        match expr {
            MirRelationExpr::Let { id, value, body } => {
                fix_types(value, ctx)?;
                let value_typ = ctx.typ.pop().expect("value type");
                let prior_typ = ctx.env.insert(id.clone(), value_typ);
                fix_types(body, ctx)?;
                ctx.env.remove(id);
                if let Some(prior_typ) = prior_typ {
                    ctx.env.insert(id.clone(), prior_typ);
                }
            }
            MirRelationExpr::LetRec {
                ids,
                values,
                body,
                limits: _,
            } => {
                // An ugly-ugly hack to pass the type information of the WMR CTE
                // to the `fix_types` pass.
                let mut prior_typs = BTreeMap::default();
                for (id, value) in std::iter::zip(ids.iter_mut(), values.iter_mut()) {
                    let MirRelationExpr::Union { base, mut inputs } = value.take_dangerous() else {
                        unreachable!("ensured by construction");
                    };
                    let MirRelationExpr::Get { id: _, typ } = *base else {
                        unreachable!("ensured by construction");
                    };
                    if let Some(prior_typ) = ctx.env.insert(id.clone(), typ) {
                        prior_typs.insert(id.clone(), prior_typ);
                    }
                    *value = inputs.pop().expect("ensured by construction");
                }
                for value in values.iter_mut() {
                    fix_types(value, ctx)?;
                }
                fix_types(body, ctx)?;
                for id in ids.iter() {
                    ctx.env.remove(id);
                    if let Some(prior_typ) = prior_typs.remove(id) {
                        ctx.env.insert(id.clone(), prior_typ);
                    }
                }
            }
            MirRelationExpr::Get {
                id: Id::Local(id),
                typ,
            } => {
                let env_typ = match ctx.env.get(&*id) {
                    Some(env_typ) => env_typ,
                    None => Err(format!("Cannot fix type of unbound CTE {}", id))?,
                };
                *typ = env_typ.clone();
                ctx.typ.push(env_typ.clone());
            }
            _ => {
                for input in expr.children_mut() {
                    fix_types(input, ctx)?;
                }
                let input_types = ctx.typ.split_off(ctx.typ.len() - expr.num_inputs());
                ctx.typ.push(expr.typ_with_input_types(&input_types));
            }
        };

        Ok(())
    }
}

/// Support for parsing [mz_expr::MirScalarExpr].
mod scalar {
    use mz_expr::{ColumnOrder, MirScalarExpr};
    use mz_repr::{AsColumnType, Datum, Row};

    use super::*;

    type Result = syn::Result<MirScalarExpr>;

    pub fn parse_exprs(input: ParseStream) -> syn::Result<Vec<MirScalarExpr>> {
        input.parse_comma_sep(parse_expr)
    }

    /// Parses a single expression.
    ///
    /// Because in EXPLAIN contexts parentheses might be optional, we need to
    /// correctly handle operator precedence of infix operators.
    ///
    /// Currently, this works in two steps:
    ///
    /// 1. Convert the original infix expression to a postfix expression using
    ///    an adapted variant of this [algorithm] with precedence taken from the
    ///    Postgres [precedence] docs. Parenthesized operands are parsed in one
    ///    step, so steps (3-4) from the [algorithm] are not needed here.
    /// 2. Convert the postfix vector into a single [MirScalarExpr].
    ///
    /// [algorithm]: <https://www.prepbytes.com/blog/stacks/infix-to-postfix-conversion-using-stack/>
    /// [precedence]: <https://www.postgresql.org/docs/7.2/sql-precedence.html>
    pub fn parse_expr(input: ParseStream) -> Result {
        let line = input.span().start().line;

        /// Helper struct to keep track of the parsing state.
        #[derive(Debug)]
        enum Op {
            Unr(mz_expr::UnaryFunc), // unary
            Neg(mz_expr::UnaryFunc), // negated unary (append -.not() on fold)
            Bin(mz_expr::BinaryFunc),
            Var(mz_expr::VariadicFunc),
        }

        impl Op {
            fn precedence(&self) -> Option<usize> {
                match self {
                    // 01: logical disjunction
                    Op::Var(mz_expr::VariadicFunc::Or) => Some(1),
                    // 02: logical conjunction
                    Op::Var(mz_expr::VariadicFunc::And) => Some(2),
                    // 04: equality, assignment
                    Op::Bin(mz_expr::BinaryFunc::Eq) => Some(4),
                    Op::Bin(mz_expr::BinaryFunc::NotEq) => Some(4),
                    // 05: less than, greater than
                    Op::Bin(mz_expr::BinaryFunc::Gt) => Some(5),
                    Op::Bin(mz_expr::BinaryFunc::Gte) => Some(5),
                    Op::Bin(mz_expr::BinaryFunc::Lt) => Some(5),
                    Op::Bin(mz_expr::BinaryFunc::Lte) => Some(5),
                    // 13: test for TRUE, FALSE, UNKNOWN, NULL
                    Op::Unr(mz_expr::UnaryFunc::IsNull(_)) => Some(13),
                    Op::Neg(mz_expr::UnaryFunc::IsNull(_)) => Some(13),
                    Op::Unr(mz_expr::UnaryFunc::IsTrue(_)) => Some(13),
                    Op::Neg(mz_expr::UnaryFunc::IsTrue(_)) => Some(13),
                    Op::Unr(mz_expr::UnaryFunc::IsFalse(_)) => Some(13),
                    Op::Neg(mz_expr::UnaryFunc::IsFalse(_)) => Some(13),
                    // 14: addition, subtraction
                    Op::Bin(mz_expr::BinaryFunc::AddInt64) => Some(14),
                    // 14: multiplication, division, modulo
                    Op::Bin(mz_expr::BinaryFunc::MulInt64) => Some(15),
                    Op::Bin(mz_expr::BinaryFunc::DivInt64) => Some(15),
                    Op::Bin(mz_expr::BinaryFunc::ModInt64) => Some(15),
                    // unsupported
                    _ => None,
                }
            }
        }

        /// Helper struct for entries in the postfix vector.
        #[derive(Debug)]
        enum Entry {
            Operand(MirScalarExpr),
            Operator(Op),
        }

        let mut opstack = vec![];
        let mut postfix = vec![];
        let mut exp_opd = true; // expects an argument of an operator

        // Scan the given infix expression from left to right.
        while !input.is_empty() && input.span().start().line == line {
            // Operands and operators alternate.
            if exp_opd {
                postfix.push(Entry::Operand(parse_operand(input)?));
                exp_opd = false;
            } else {
                // If the current symbol is an operator, then bind it to op.
                // Else it is an operand - append it to postfix and continue.
                let op = if input.eat(syn::Token![=]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::Eq)
                } else if input.eat(syn::Token![!=]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::NotEq)
                } else if input.eat(syn::Token![>=]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::Gte)
                } else if input.eat(syn::Token![>]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::Gt)
                } else if input.eat(syn::Token![<=]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::Lte)
                } else if input.eat(syn::Token![<]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::Lt)
                } else if input.eat(syn::Token![+]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::AddInt64) // TODO: fix placeholder
                } else if input.eat(syn::Token![*]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::MulInt64) // TODO: fix placeholder
                } else if input.eat(syn::Token![/]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::DivInt64) // TODO: fix placeholder
                } else if input.eat(syn::Token![%]) {
                    exp_opd = true;
                    Op::Bin(mz_expr::BinaryFunc::ModInt64) // TODO: fix placeholder
                } else if input.eat(kw::AND) {
                    exp_opd = true;
                    Op::Var(mz_expr::VariadicFunc::And)
                } else if input.eat(kw::OR) {
                    exp_opd = true;
                    Op::Var(mz_expr::VariadicFunc::Or)
                } else if input.eat(kw::IS) {
                    let negate = input.eat(kw::NOT);

                    let lookahead = input.lookahead1();
                    let func = if input.look_and_eat(kw::NULL, &lookahead) {
                        mz_expr::func::IsNull.into()
                    } else if input.look_and_eat(kw::TRUE, &lookahead) {
                        mz_expr::func::IsTrue.into()
                    } else if input.look_and_eat(kw::FALSE, &lookahead) {
                        mz_expr::func::IsFalse.into()
                    } else {
                        Err(lookahead.error())?
                    };

                    if negate {
                        Op::Neg(func)
                    } else {
                        Op::Unr(func)
                    }
                } else {
                    // We were expecting an optional operator but didn't find
                    // anything. Exit the parsing loop and process the postfix
                    // vector.
                    break;
                };

                // First, pop the operators which are already on the opstack that
                // have higher or equal precedence than the current operator and
                // append them to the postfix.
                while opstack
                    .last()
                    .map(|op1: &Op| op1.precedence() >= op.precedence())
                    .unwrap_or(false)
                {
                    let op1 = opstack.pop().expect("non-empty opstack");
                    postfix.push(Entry::Operator(op1));
                }

                // Then push the op from this iteration onto the stack.
                opstack.push(op);
            }
        }

        // Pop all remaining symbols from opstack and append them to postfix.
        postfix.extend(opstack.into_iter().rev().map(Entry::Operator));

        if postfix.is_empty() {
            let msg = "Cannot parse an empty expression";
            Err(Error::new(input.span(), msg))?
        }

        // Flatten the postfix vector into a single MirScalarExpr.
        let mut stack = vec![];
        postfix.reverse();
        while let Some(entry) = postfix.pop() {
            match entry {
                Entry::Operand(expr) => {
                    stack.push(expr);
                }
                Entry::Operator(Op::Unr(func)) => {
                    let expr = Box::new(stack.pop().expect("non-empty stack"));
                    stack.push(MirScalarExpr::CallUnary { func, expr });
                }
                Entry::Operator(Op::Neg(func)) => {
                    let expr = Box::new(stack.pop().expect("non-empty stack"));
                    stack.push(MirScalarExpr::CallUnary { func, expr }.not());
                }
                Entry::Operator(Op::Bin(func)) => {
                    let expr2 = Box::new(stack.pop().expect("non-empty stack"));
                    let expr1 = Box::new(stack.pop().expect("non-empty stack"));
                    stack.push(MirScalarExpr::CallBinary { func, expr1, expr2 });
                }
                Entry::Operator(Op::Var(func)) => {
                    let expr2 = stack.pop().expect("non-empty stack");
                    let expr1 = stack.pop().expect("non-empty stack");
                    let mut exprs = vec![];
                    for expr in [expr1, expr2] {
                        match expr {
                            MirScalarExpr::CallVariadic { func: f, exprs: es } if f == func => {
                                exprs.extend(es.into_iter());
                            }
                            expr => {
                                exprs.push(expr);
                            }
                        }
                    }
                    stack.push(MirScalarExpr::CallVariadic { func, exprs });
                }
            }
        }

        if stack.len() != 1 {
            let msg = "Cannot fold postfix vector into a single MirScalarExpr";
            Err(Error::new(input.span(), msg))?
        }

        Ok(stack.pop().unwrap())
    }

    pub fn parse_operand(input: ParseStream) -> Result {
        let lookahead = input.lookahead1();
        if lookahead.peek(syn::Token![#]) {
            parse_column(input)
        } else if lookahead.peek(syn::Lit) || lookahead.peek(kw::null) {
            parse_literal_ok(input)
        } else if lookahead.peek(kw::error) {
            parse_literal_err(input)
        } else if lookahead.peek(syn::Ident) {
            parse_apply(input)
        } else if lookahead.peek(syn::token::Paren) {
            let inner;
            syn::parenthesized!(inner in input);
            parse_expr(&inner)
        } else {
            Err(lookahead.error()) // FIXME: support IfThenElse variants
        }
    }

    pub fn parse_column(input: ParseStream) -> Result {
        Ok(MirScalarExpr::Column(parse_column_index(input)?))
    }

    pub fn parse_column_index(input: ParseStream) -> syn::Result<usize> {
        input.parse::<syn::Token![#]>()?;
        input.parse::<syn::LitInt>()?.base10_parse::<usize>()
    }

    pub fn parse_column_order(input: ParseStream) -> syn::Result<ColumnOrder> {
        input.parse::<syn::Token![#]>()?;
        let column = input.parse::<syn::LitInt>()?.base10_parse::<usize>()?;
        let desc = input.eat(kw::desc) || !input.eat(kw::asc);
        let nulls_last = input.eat(kw::nulls_last) || !input.eat(kw::nulls_first);
        Ok(ColumnOrder {
            column,
            desc,
            nulls_last,
        })
    }

    fn parse_literal_ok(input: ParseStream) -> Result {
        let mut row = Row::default();
        let mut packer = row.packer();

        let typ = if input.eat(kw::null) {
            packer.push(Datum::Null);
            input.parse::<syn::Token![::]>()?;
            attributes::parse_scalar_type(input)?.nullable(true)
        } else {
            match input.parse::<syn::Lit>()? {
                syn::Lit::Str(l) => {
                    packer.push(Datum::from(l.value().as_str()));
                    Ok(String::as_column_type())
                }
                syn::Lit::Int(l) => {
                    packer.push(Datum::from(l.base10_parse::<i64>()?));
                    Ok(i64::as_column_type())
                }
                syn::Lit::Float(l) => {
                    packer.push(Datum::from(l.base10_parse::<f64>()?));
                    Ok(f64::as_column_type())
                }
                syn::Lit::Bool(l) => {
                    packer.push(Datum::from(l.value));
                    Ok(bool::as_column_type())
                }
                _ => Err(Error::new(input.span(), "cannot parse literal")),
            }?
        };

        Ok(MirScalarExpr::Literal(Ok(row), typ))
    }

    fn parse_literal_err(input: ParseStream) -> Result {
        input.parse::<kw::error>()?;
        let mut msg = {
            let content;
            syn::parenthesized!(content in input);
            content.parse::<syn::LitStr>()?.value()
        };
        let err = if msg.starts_with("internal error: ") {
            Ok(mz_expr::EvalError::Internal(msg.split_off(16)))
        } else {
            Err(Error::new(msg.span(), "expected `internal error: $msg`"))
        }?;
        Ok(MirScalarExpr::Literal(Err(err), bool::as_column_type())) // FIXME
    }

    fn parse_apply(input: ParseStream) -> Result {
        use mz_expr::func::{BinaryFunc::*, UnmaterializableFunc::*, VariadicFunc::*, *};

        let ident = input.parse::<syn::Ident>()?;

        // parse parentheses
        let inner;
        syn::parenthesized!(inner in input);

        let parse_nullary = |func: UnmaterializableFunc| -> Result {
            Ok(MirScalarExpr::CallUnmaterializable(func))
        };
        let parse_unary = |func: UnaryFunc| -> Result {
            let expr = Box::new(parse_expr(&inner)?);
            Ok(MirScalarExpr::CallUnary { func, expr })
        };
        let parse_binary = |func: BinaryFunc| -> Result {
            let expr1 = Box::new(parse_expr(&inner)?);
            inner.parse::<syn::Token![,]>()?;
            let expr2 = Box::new(parse_expr(&inner)?);
            Ok(MirScalarExpr::CallBinary { func, expr1, expr2 })
        };
        let parse_variadic = |func: VariadicFunc| -> Result {
            let exprs = inner.parse_comma_sep(parse_expr)?;
            Ok(MirScalarExpr::CallVariadic { func, exprs })
        };

        // Infix binary and variadic function calls are handled in `parse_scalar_expr`.
        //
        // Some restrictions apply with the current state of the code,
        // most notably one cannot handle overloaded function names because we don't want to do
        // name resolution in the parser.
        match ident.to_string().to_lowercase().as_str() {
            // Supported unmaterializable (a.k.a. nullary) functions:
            "mz_environment_id" => parse_nullary(MzEnvironmentId),
            // Supported unary functions:
            "abs" => parse_unary(AbsInt64.into()),
            "not" => parse_unary(Not.into()),
            // Supported binary functions:
            "ltrim" => parse_binary(TrimLeading),
            // Supported variadic functions:
            "greatest" => parse_variadic(Greatest),
            _ => Err(Error::new(ident.span(), "unsupported function name")),
        }
    }

    pub fn parse_join_equivalences(input: ParseStream) -> syn::Result<Vec<Vec<MirScalarExpr>>> {
        let mut equivalences = vec![];
        while !input.is_empty() {
            if input.eat(kw::eq) {
                let inner;
                syn::parenthesized!(inner in input);
                equivalences.push(inner.parse_comma_sep(parse_expr)?);
            } else {
                let lhs = parse_operand(input)?;
                input.parse::<syn::Token![=]>()?;
                let rhs = parse_operand(input)?;
                equivalences.push(vec![lhs, rhs]);
            }
            input.eat(kw::AND);
        }
        Ok(equivalences)
    }
}

/// Support for parsing [mz_expr::AggregateExpr].
mod aggregate {
    use mz_expr::{AggregateExpr, MirScalarExpr};

    use super::*;

    type Result = syn::Result<AggregateExpr>;

    pub fn parse_expr(input: ParseStream) -> Result {
        use mz_expr::AggregateFunc::*;

        // Some restrictions apply with the current state of the code,
        // most notably one cannot handle overloaded function names because we don't want to do
        // name resolution in the parser.
        let ident = input.parse::<syn::Ident>()?;
        let func = match ident.to_string().to_lowercase().as_str() {
            "count" => Count,
            "any" => Any,
            "all" => All,
            "max" => MaxInt64,
            "min" => MinInt64,
            "sum" => SumInt64,
            _ => Err(Error::new(ident.span(), "unsupported function name"))?,
        };

        // parse parentheses
        let inner;
        syn::parenthesized!(inner in input);

        if func == Count && inner.eat(syn::Token![*]) {
            Ok(AggregateExpr {
                func,
                expr: MirScalarExpr::literal_true(),
                distinct: false, // TODO: fix explain output
            })
        } else {
            let distinct = inner.eat(kw::distinct);
            let expr = scalar::parse_expr(&inner)?;
            Ok(AggregateExpr {
                func,
                expr,
                distinct,
            })
        }
    }
}

/// Support for parsing [mz_repr::Row].
mod row {
    use mz_repr::{Datum, Row, RowPacker};

    use super::*;

    impl Parse for Parsed<Row> {
        fn parse(input: ParseStream) -> syn::Result<Self> {
            let mut row = Row::default();
            let mut packer = ParseRow::new(&mut row);

            loop {
                if input.is_empty() {
                    break;
                }
                packer.parse_datum(input)?;
                if input.is_empty() {
                    break;
                }
                input.parse::<syn::Token![,]>()?;
            }

            Ok(Parsed(row))
        }
    }

    impl From<Parsed<Row>> for Row {
        fn from(parsed: Parsed<Row>) -> Self {
            parsed.0
        }
    }

    struct ParseRow<'a>(RowPacker<'a>);

    impl<'a> ParseRow<'a> {
        fn new(row: &'a mut Row) -> Self {
            Self(row.packer())
        }

        fn parse_datum(&mut self, input: ParseStream) -> syn::Result<()> {
            if input.eat(kw::null) {
                self.0.push(Datum::Null)
            } else {
                match input.parse::<syn::Lit>()? {
                    syn::Lit::Str(l) => self.0.push(Datum::from(l.value().as_str())),
                    syn::Lit::Int(l) => self.0.push(Datum::from(l.base10_parse::<i64>()?)),
                    syn::Lit::Float(l) => self.0.push(Datum::from(l.base10_parse::<f64>()?)),
                    syn::Lit::Bool(l) => self.0.push(Datum::from(l.value)),
                    _ => Err(Error::new(input.span(), "cannot parse literal"))?,
                }
            }
            Ok(())
        }
    }
}

mod attributes {
    use mz_repr::{ColumnType, ScalarType};

    use super::*;

    #[derive(Default)]
    pub struct Attributes {
        pub types: Option<Vec<ColumnType>>,
        pub keys: Option<Vec<Vec<usize>>>,
    }

    pub fn parse_attributes(input: ParseStream) -> syn::Result<Attributes> {
        let mut attributes = Attributes::default();

        // Attributes are optional, appearing after a `//` at the end of the
        // line. However, since the syn lexer eats comments, we assume that `//`
        // was replaced with `::` upfront.
        if input.eat(syn::Token![::]) {
            let inner;
            syn::braced!(inner in input);

            let (start, end) = (inner.span().start(), inner.span().end());
            if start.line != end.line {
                let msg = "attributes should not span more than one line".to_string();
                Err(Error::new(inner.span(), msg))?
            }

            while inner.peek(syn::Ident) {
                let ident = inner.parse::<syn::Ident>()?.to_string();
                match ident.as_str() {
                    "types" => {
                        inner.parse::<syn::Token![:]>()?;
                        let value = inner.parse::<syn::LitStr>()?.value();
                        attributes.types = Some(parse_types.parse_str(&value)?);
                    }
                    // TODO: support keys
                    key => {
                        let msg = format!("unexpected attribute type `{}`", key);
                        Err(Error::new(inner.span(), msg))?;
                    }
                }
            }
        }
        Ok(attributes)
    }

    fn parse_types(input: ParseStream) -> syn::Result<Vec<ColumnType>> {
        let inner;
        syn::parenthesized!(inner in input);
        inner.parse_comma_sep(parse_column_type)
    }

    pub fn parse_column_type(input: ParseStream) -> syn::Result<ColumnType> {
        let scalar_type = parse_scalar_type(input)?;
        Ok(scalar_type.nullable(input.eat(syn::Token![?])))
    }

    pub fn parse_scalar_type(input: ParseStream) -> syn::Result<ScalarType> {
        let lookahead = input.lookahead1();

        let scalar_type = if input.look_and_eat(bigint, &lookahead) {
            ScalarType::Int64
        } else if input.look_and_eat(double, &lookahead) {
            input.parse::<precision>()?;
            ScalarType::Float64
        } else if input.look_and_eat(boolean, &lookahead) {
            ScalarType::Bool
        } else if input.look_and_eat(character, &lookahead) {
            input.parse::<varying>()?;
            ScalarType::VarChar { max_length: None }
        } else if input.look_and_eat(integer, &lookahead) {
            ScalarType::Int32
        } else if input.look_and_eat(smallint, &lookahead) {
            ScalarType::Int16
        } else if input.look_and_eat(text, &lookahead) {
            ScalarType::String
        } else {
            Err(lookahead.error())?
        };

        Ok(scalar_type)
    }

    syn::custom_keyword!(bigint);
    syn::custom_keyword!(boolean);
    syn::custom_keyword!(character);
    syn::custom_keyword!(double);
    syn::custom_keyword!(integer);
    syn::custom_keyword!(precision);
    syn::custom_keyword!(smallint);
    syn::custom_keyword!(text);
    syn::custom_keyword!(varying);
}

pub enum Def {
    Source {
        name: String,
        typ: mz_repr::RelationType,
    },
}

mod def {
    use mz_repr::{ColumnType, RelationType};

    use super::*;

    pub fn parse_def(ctx: CtxRef, input: ParseStream) -> syn::Result<Def> {
        parse_def_source(ctx, input) // only one variant for now
    }

    fn parse_def_source(ctx: CtxRef, input: ParseStream) -> syn::Result<Def> {
        let reduce = input.parse::<def::DefSource>()?;

        let name = {
            input.parse::<def::name>()?;
            input.parse::<syn::Token![=]>()?;
            input.parse::<syn::Ident>()?.to_string()
        };

        let keys = if input.eat(kw::keys) {
            input.parse::<syn::Token![=]>()?;
            let inner;
            syn::bracketed!(inner in input);
            inner.parse_comma_sep(|input| {
                let inner;
                syn::bracketed!(inner in input);
                inner.parse_comma_sep(scalar::parse_column_index)
            })?
        } else {
            vec![]
        };

        let parse_inputs = ParseChildren::new(input, reduce.span().start());
        let column_types = parse_inputs.parse_many(ctx, parse_def_source_column)?;

        let typ = RelationType { column_types, keys };

        Ok(Def::Source { name, typ })
    }

    fn parse_def_source_column(_ctx: CtxRef, input: ParseStream) -> syn::Result<ColumnType> {
        input.parse::<syn::Token![-]>()?;
        attributes::parse_column_type(input)
    }

    syn::custom_keyword!(DefSource);
    syn::custom_keyword!(name);
}

/// Help utilities used by sibling modules.
mod util {
    use syn::parse::{Lookahead1, ParseBuffer, Peek};

    use super::*;

    /// Extension methods for [`syn::parse::ParseBuffer`].
    pub trait ParseBufferExt<'a> {
        fn look_and_eat<T: Eat>(&self, token: T, lookahead: &Lookahead1<'a>) -> bool;

        /// Consumes a token `T` if present.
        fn eat<T: Eat>(&self, t: T) -> bool;

        /// Consumes two tokens `T1 T2` if present in that order.
        fn eat2<T1: Eat, T2: Eat>(&self, t1: T1, t2: T2) -> bool;

        /// Consumes three tokens `T1 T2 T3` if present in that order.
        fn eat3<T1: Eat, T2: Eat, T3: Eat>(&self, t1: T1, t2: T2, t3: T3) -> bool;

        // Parse a comma-separated list of items into a vector.
        fn parse_comma_sep<T>(&self, p: fn(ParseStream) -> syn::Result<T>) -> syn::Result<Vec<T>>;
    }

    impl<'a> ParseBufferExt<'a> for ParseBuffer<'a> {
        /// Consumes a token `T` if present, looking it up using the provided
        /// [`Lookahead1`] instance.
        fn look_and_eat<T: Eat>(&self, token: T, lookahead: &Lookahead1<'a>) -> bool {
            if lookahead.peek(token) {
                self.parse::<T::Token>().unwrap();
                true
            } else {
                false
            }
        }

        fn eat<T: Eat>(&self, t: T) -> bool {
            if self.peek(t) {
                self.parse::<T::Token>().unwrap();
                true
            } else {
                false
            }
        }

        fn eat2<T1: Eat, T2: Eat>(&self, t1: T1, t2: T2) -> bool {
            if self.peek(t1) && self.peek2(t2) {
                self.parse::<T1::Token>().unwrap();
                self.parse::<T2::Token>().unwrap();
                true
            } else {
                false
            }
        }

        fn eat3<T1: Eat, T2: Eat, T3: Eat>(&self, t1: T1, t2: T2, t3: T3) -> bool {
            if self.peek(t1) && self.peek2(t2) && self.peek3(t3) {
                self.parse::<T1::Token>().unwrap();
                self.parse::<T2::Token>().unwrap();
                self.parse::<T3::Token>().unwrap();
                true
            } else {
                false
            }
        }

        fn parse_comma_sep<T>(&self, p: fn(ParseStream) -> syn::Result<T>) -> syn::Result<Vec<T>> {
            Ok(self
                .parse_terminated(p, syn::Token![,])?
                .into_iter()
                .collect::<Vec<_>>())
        }
    }

    // Helper trait for types that can be eaten.
    //
    // Implementing types must also implement [`Peek`], and the associated
    // [`Peek::Token`] type should implement [`Parse`]). For some reason the
    // latter bound is not present in [`Peek`] even if it makes a lot of sense,
    // which is why we need this helper.
    pub trait Eat: Peek<Token = Self::_Token> {
        type _Token: Parse;
    }

    impl<T> Eat for T
    where
        T: Peek,
        T::Token: Parse,
    {
        type _Token = T::Token;
    }

    pub struct Ctx<'a> {
        pub catalog: &'a TestCatalog,
    }

    pub type CtxRef<'a> = &'a Ctx<'a>;

    /// Newtype for external types that need to implement [Parse].
    pub struct Parsed<T>(pub T);

    /// Provides facilities for parsing
    pub struct ParseChildren<'a> {
        stream: ParseStream<'a>,
        parent: LineColumn,
    }

    impl<'a> ParseChildren<'a> {
        pub fn new(stream: ParseStream<'a>, parent: LineColumn) -> Self {
            Self { stream, parent }
        }

        pub fn parse_one<C, T>(
            &self,
            ctx: C,
            function: fn(C, ParseStream) -> syn::Result<T>,
        ) -> syn::Result<T> {
            match self.maybe_child() {
                Ok(_) => function(ctx, self.stream),
                Err(e) => Err(e),
            }
        }

        pub fn parse_many<C: Copy, T>(
            &self,
            ctx: C,
            function: fn(C, ParseStream) -> syn::Result<T>,
        ) -> syn::Result<Vec<T>> {
            let mut inputs = vec![self.parse_one(ctx, function)?];
            while self.maybe_child().is_ok() {
                inputs.push(function(ctx, self.stream)?);
            }
            Ok(inputs)
        }

        fn maybe_child(&self) -> syn::Result<()> {
            let start = self.stream.span().start();
            if start.line <= self.parent.line {
                let msg = format!("child expected at line > {}", self.parent.line);
                Err(Error::new(self.stream.span(), msg))?
            }
            if start.column != self.parent.column + 2 {
                let msg = format!("child expected at column {}", self.parent.column + 2);
                Err(Error::new(self.stream.span(), msg))?
            }
            Ok(())
        }
    }
}

/// Custom keywords used while parsing.
mod kw {
    syn::custom_keyword!(aggregates);
    syn::custom_keyword!(AND);
    syn::custom_keyword!(ArrangeBy);
    syn::custom_keyword!(asc);
    syn::custom_keyword!(Constant);
    syn::custom_keyword!(CrossJoin);
    syn::custom_keyword!(cte);
    syn::custom_keyword!(desc);
    syn::custom_keyword!(distinct);
    syn::custom_keyword!(Distinct);
    syn::custom_keyword!(empty);
    syn::custom_keyword!(eq);
    syn::custom_keyword!(error);
    syn::custom_keyword!(exp_group_size);
    syn::custom_keyword!(FALSE);
    syn::custom_keyword!(Filter);
    syn::custom_keyword!(FlatMap);
    syn::custom_keyword!(Get);
    syn::custom_keyword!(group_by);
    syn::custom_keyword!(IS);
    syn::custom_keyword!(Join);
    syn::custom_keyword!(keys);
    syn::custom_keyword!(limit);
    syn::custom_keyword!(Map);
    syn::custom_keyword!(monotonic);
    syn::custom_keyword!(Mutually);
    syn::custom_keyword!(Negate);
    syn::custom_keyword!(NOT);
    syn::custom_keyword!(null);
    syn::custom_keyword!(NULL);
    syn::custom_keyword!(nulls_first);
    syn::custom_keyword!(nulls_last);
    syn::custom_keyword!(offset);
    syn::custom_keyword!(on);
    syn::custom_keyword!(OR);
    syn::custom_keyword!(order_by);
    syn::custom_keyword!(Project);
    syn::custom_keyword!(Recursive);
    syn::custom_keyword!(Reduce);
    syn::custom_keyword!(Return);
    syn::custom_keyword!(Threshold);
    syn::custom_keyword!(TopK);
    syn::custom_keyword!(TRUE);
    syn::custom_keyword!(Union);
    syn::custom_keyword!(With);
    syn::custom_keyword!(x);
}
