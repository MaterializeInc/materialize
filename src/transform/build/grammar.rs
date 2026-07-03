// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`chumsky`] grammar for the rewrite DSL, run at build time.
//!
//! Two stages, like the hand-written parser it replaces: a lexer turns the
//! source text into [`Token`]s (skipping whitespace and `#` comments), and a
//! parser turns the token stream into the [`Rule`] AST. The AST types come from
//! `crate::dsl` (the `include!`d `src/eqsat/dsl.rs`).

use chumsky::prelude::*;

use crate::dsl::*;

/// A lexical token. Mirrors the hand-written tokenizer's token set.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Token {
    Ident(String),
    Str(String),
    Int(i64),
    LBrack,
    RBrack,
    LParen,
    RParen,
    LBrace,
    RBrace,
    Comma,
    Arrow,
    Ellipsis,
    Plus,
    Minus,
}

type StrErr<'a> = extra::Err<Rich<'a, char>>;
type TokErr<'a> = extra::Err<Rich<'a, Token>>;

/// Parse a complete rule file: lex, then parse the token stream.
pub fn parse(src: &str) -> Result<Vec<Rule>, Vec<String>> {
    let toks = lexer().parse(src).into_result().map_err(|errs| {
        errs.into_iter()
            .map(|e| format!("{e:?}"))
            .collect::<Vec<_>>()
    })?;
    parser()
        .parse(&toks[..])
        .into_result()
        .map_err(|errs| errs.into_iter().map(|e| format!("{e:?}")).collect())
}

/// Operator keywords start with an uppercase letter; metavariables do not.
fn is_operator(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_uppercase())
}

// --- stage 1: lexer -------------------------------------------------------

fn lexer<'a>() -> impl Parser<'a, &'a str, Vec<Token>, StrErr<'a>> {
    let ws = one_of(" \t\r\n").ignored();
    let comment = just('#')
        .then(none_of("\n").repeated().collect::<Vec<char>>())
        .ignored();
    let pad = choice((ws, comment))
        .repeated()
        .collect::<Vec<()>>()
        .ignored()
        .boxed();

    let ident = any()
        .filter(|c: &char| c.is_ascii_alphabetic() || *c == '_')
        .then(
            any()
                .filter(|c: &char| c.is_ascii_alphanumeric() || *c == '_')
                .repeated()
                .collect::<String>(),
        )
        .map(|(c, rest): (char, String)| {
            let mut s = String::with_capacity(rest.len() + 1);
            s.push(c);
            s.push_str(&rest);
            Token::Ident(s)
        });

    let string = just('"')
        .ignore_then(none_of("\"").repeated().collect::<String>())
        .then_ignore(just('"'))
        .map(Token::Str);

    let int = any()
        .filter(|c: &char| c.is_ascii_digit())
        .repeated()
        .at_least(1)
        .collect::<String>()
        .map(|s: String| Token::Int(s.parse::<i64>().expect("ascii digits parse as i64")));

    let symbol = choice((
        just("=>").to(Token::Arrow),
        just("...").to(Token::Ellipsis),
        just("[").to(Token::LBrack),
        just("]").to(Token::RBrack),
        just("(").to(Token::LParen),
        just(")").to(Token::RParen),
        just("{").to(Token::LBrace),
        just("}").to(Token::RBrace),
        just(",").to(Token::Comma),
        just("+").to(Token::Plus),
        just("-").to(Token::Minus),
    ));

    let token = choice((symbol, string, int, ident));

    pad.clone()
        .ignore_then(
            token
                .then_ignore(pad.clone())
                .repeated()
                .collect::<Vec<Token>>(),
        )
        .then_ignore(end())
}

// --- stage 2: parser ------------------------------------------------------

fn ident<'a>() -> impl Parser<'a, &'a [Token], String, TokErr<'a>> + Clone {
    select! { Token::Ident(s) => s }
}

fn string<'a>() -> impl Parser<'a, &'a [Token], String, TokErr<'a>> + Clone {
    select! { Token::Str(s) => s }
}

fn int<'a>() -> impl Parser<'a, &'a [Token], i64, TokErr<'a>> + Clone {
    select! { Token::Int(n) => n }
}

/// Match a specific keyword identifier.
fn kw<'a>(word: &'static str) -> impl Parser<'a, &'a [Token], (), TokErr<'a>> + Clone {
    ident().filter(move |s: &String| s == word).ignored()
}

fn relvar_ident<'a>() -> impl Parser<'a, &'a [Token], String, TokErr<'a>> + Clone {
    ident().filter(|s: &String| !is_operator(s))
}

/// `[ ident ]` — a single bracketed payload metavariable.
fn bracket_ident<'a>() -> impl Parser<'a, &'a [Token], String, TokErr<'a>> + Clone {
    ident().delimited_by(just(Token::LBrack), just(Token::RBrack))
}

/// An element of a list pattern, pre-classification.
enum ListPatElem {
    Rest(String),
    Item(Pat),
}

fn to_listpat(elems: Vec<ListPatElem>) -> ListPat {
    let mut items = Vec::new();
    let mut rest = None;
    for e in elems {
        match e {
            ListPatElem::Item(p) => items.push(p),
            // A `rest...` only ever appears last in valid input.
            ListPatElem::Rest(r) => rest = Some(r),
        }
    }
    ListPat { items, rest }
}

fn parser<'a>() -> impl Parser<'a, &'a [Token], Vec<Rule>, TokErr<'a>> {
    // --- index expressions ---
    let ixexpr = recursive(|ix| {
        let atom = choice((
            int().map(IxExpr::Lit),
            kw("arity")
                .ignore_then(ident().delimited_by(just(Token::LParen), just(Token::RParen)))
                .map(IxExpr::Arity),
            ix.clone()
                .delimited_by(just(Token::LParen), just(Token::RParen)),
        ));
        let term = recursive(|term| {
            choice((
                just(Token::Minus)
                    .ignore_then(term.clone())
                    .map(|e| IxExpr::Neg(Box::new(e))),
                atom.clone(),
            ))
        });
        let op = choice((just(Token::Plus).to(true), just(Token::Minus).to(false)));
        term.clone()
            .foldl(op.then(term).repeated(), |acc, (is_add, rhs)| {
                if is_add {
                    IxExpr::Add(Box::new(acc), Box::new(rhs))
                } else {
                    IxExpr::Sub(Box::new(acc), Box::new(rhs))
                }
            })
    });

    // --- payload expressions (template payloads) ---
    let pexpr = recursive(|pe| {
        let call2 = |name: &'static str| {
            kw(name)
                .ignore_then(just(Token::LParen))
                .ignore_then(pe.clone())
                .then_ignore(just(Token::Comma))
                .then(pe.clone())
                .then_ignore(just(Token::RParen))
        };
        let concat = call2("concat").map(|(a, b)| PExpr::Concat(Box::new(a), Box::new(b)));
        let compose = call2("compose").map(|(a, b)| PExpr::Compose(Box::new(a), Box::new(b)));
        let shift = kw("shift")
            .ignore_then(just(Token::LParen))
            .ignore_then(pe.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|(p, k)| PExpr::Shift(Box::new(p), k));
        let remap = call2("remap").map(|(a, b)| PExpr::Remap(Box::new(a), Box::new(b)));
        let cols_of = kw("cols_of")
            .ignore_then(
                pe.clone()
                    .delimited_by(just(Token::LParen), just(Token::RParen)),
            )
            .map(|p| PExpr::ColsOf(Box::new(p)));
        let iota = kw("iota")
            .ignore_then(
                ixexpr
                    .clone()
                    .delimited_by(just(Token::LParen), just(Token::RParen)),
            )
            .map(PExpr::Iota);
        let equivs_inner = kw("equivs_inner")
            .ignore_then(just(Token::LParen))
            .ignore_then(pe.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|(p, k)| PExpr::EquivsInner(Box::new(p), k));
        let equivs_outer = kw("equivs_outer")
            .ignore_then(just(Token::LParen))
            .ignore_then(pe.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|(p, k)| PExpr::EquivsOuter(Box::new(p), k));
        let swap_equivs = kw("swap_equivs")
            .ignore_then(just(Token::LParen))
            .ignore_then(pe.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|((p, a), b)| PExpr::SwapEquivs(Box::new(p), a, b));
        let swap_projection = kw("swap_projection")
            .ignore_then(just(Token::LParen))
            .ignore_then(ixexpr.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|(a, b)| PExpr::SwapProjection(a, b));
        choice((
            concat,
            compose,
            shift,
            remap,
            cols_of,
            iota,
            equivs_inner,
            equivs_outer,
            swap_equivs,
            swap_projection,
            ident().map(PExpr::Var),
        ))
    });

    let bracket_pexpr = pexpr
        .clone()
        .delimited_by(just(Token::LBrack), just(Token::RBrack));

    // --- patterns ---
    let pat = recursive(|pat| {
        let paren = pat
            .clone()
            .delimited_by(just(Token::LParen), just(Token::RParen));

        let listpat = {
            let rest = relvar_ident()
                .then_ignore(just(Token::Ellipsis))
                .map(ListPatElem::Rest);
            let item = pat.clone().map(ListPatElem::Item);
            choice((rest, item))
                .separated_by(just(Token::Comma))
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .map(to_listpat)
        };

        let filter = kw("Filter")
            .ignore_then(bracket_ident())
            .then(pat.clone())
            .map(|(preds, input)| Pat::Filter {
                preds,
                input: Box::new(input),
            });
        let map = kw("Map")
            .ignore_then(bracket_ident())
            .then(pat.clone())
            .map(|(scalars, input)| Pat::Map {
                scalars,
                input: Box::new(input),
            });
        let project = kw("Project")
            .ignore_then(bracket_ident())
            .then(pat.clone())
            .map(|(outputs, input)| Pat::Project {
                outputs,
                input: Box::new(input),
            });
        let reduce = kw("Reduce")
            .ignore_then(just(Token::LBrack))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ident())
            .then_ignore(just(Token::RBrack))
            .then(pat.clone())
            .map(|((group_key, aggregates), input)| Pat::Reduce {
                group_key,
                aggregates,
                input: Box::new(input),
            });
        let flatmap = kw("FlatMap")
            .ignore_then(just(Token::LBrack))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ident())
            .then_ignore(just(Token::RBrack))
            .then(pat.clone())
            .map(|((func, exprs), input)| Pat::FlatMap {
                func,
                exprs,
                input: Box::new(input),
            });
        let negate = kw("Negate")
            .ignore_then(pat.clone())
            .map(|p| Pat::Negate(Box::new(p)));
        let threshold = kw("Threshold")
            .ignore_then(pat.clone())
            .map(|p| Pat::Threshold(Box::new(p)));
        let topk = kw("TopK")
            .ignore_then(pat.clone())
            .map(|p| Pat::TopK(Box::new(p)));
        let arrangeby = kw("ArrangeBy")
            .ignore_then(bracket_ident())
            .then(pat.clone())
            .map(|(key, input)| Pat::ArrangeBy {
                key,
                input: Box::new(input),
            });
        let join = kw("Join")
            .ignore_then(bracket_ident())
            .then(listpat.clone())
            .map(|(equivalences, inputs)| Pat::Join {
                equivalences,
                inputs,
            });
        let wcojoin = kw("WcoJoin")
            .ignore_then(bracket_ident())
            .then(listpat.clone())
            .map(|(equivalences, inputs)| Pat::WcoJoin {
                equivalences,
                inputs,
            });
        let union = kw("Union")
            .ignore_then(listpat.clone())
            .map(|inputs| Pat::Union { inputs });
        let sunary = kw("Unary")
            .ignore_then(bracket_ident())
            .then(pat.clone())
            .map(|(func, input)| Pat::SUnary {
                func,
                input: Box::new(input),
            });
        let svariadic = kw("Variadic")
            .ignore_then(bracket_ident())
            .then(listpat.clone())
            .map(|(func, inputs)| Pat::SVariadic { func, inputs });
        // `Binary[<ident>](a, b)` binds the func symbol itself (a metavariable,
        // not a fixed keyword text like `Unary`/`Variadic`'s bracket payload).
        let sbinary_var = kw("Binary")
            .ignore_then(bracket_ident())
            .then_ignore(just(Token::LParen))
            .then(pat.clone())
            .then_ignore(just(Token::Comma))
            .then(pat.clone())
            .then_ignore(just(Token::RParen))
            .map(|((func, e1), e2)| Pat::SBinaryVar {
                func,
                expr1: Box::new(e1),
                expr2: Box::new(e2),
            });
        let sif = kw("If")
            .ignore_then(just(Token::LParen))
            .ignore_then(pat.clone())
            .then_ignore(just(Token::Comma))
            .then(pat.clone())
            .then_ignore(just(Token::Comma))
            .then(pat.clone())
            .then_ignore(just(Token::RParen))
            .map(|((cond, then), els)| Pat::SIf {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            });
        let scalar_any = kw("Scalar")
            .ignore_then(relvar_ident().delimited_by(just(Token::LParen), just(Token::RParen)))
            .map(|binding| Pat::Scalar { binding });
        let relvar = relvar_ident().map(Pat::RelVar);

        choice((
            paren,
            filter,
            map,
            project,
            reduce,
            flatmap,
            negate,
            threshold,
            topk,
            arrangeby,
            join,
            wcojoin,
            union,
            sif,
            svariadic,
            sunary,
            sbinary_var,
            scalar_any,
            relvar,
        ))
    });

    // --- templates ---
    let tmpl = recursive(|tmpl| {
        let paren = tmpl
            .clone()
            .delimited_by(just(Token::LParen), just(Token::RParen));

        let listtmpl = {
            let mapsplice = kw("map")
                .ignore_then(just(Token::LParen))
                .ignore_then(tmpl.clone())
                .then_ignore(just(Token::Comma))
                .then(ident())
                .then_ignore(just(Token::RParen))
                .map(|(func, list)| TElem::MapSplice {
                    func: Box::new(func),
                    list,
                });
            let splice = relvar_ident()
                .then_ignore(just(Token::Ellipsis))
                .map(TElem::Splice);
            let item = tmpl.clone().map(TElem::Item);
            choice((mapsplice, splice, item))
                .separated_by(just(Token::Comma))
                .collect::<Vec<_>>()
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .map(|elems| ListTmpl { elems })
        };

        let empty = kw("Empty")
            .ignore_then(ident().delimited_by(just(Token::LParen), just(Token::RParen)))
            .map(Tmpl::Empty);
        let filter = kw("Filter")
            .ignore_then(bracket_pexpr.clone())
            .then(tmpl.clone())
            .map(|(preds, input)| Tmpl::Filter {
                preds,
                input: Box::new(input),
            });
        let map = kw("Map")
            .ignore_then(bracket_pexpr.clone())
            .then(tmpl.clone())
            .map(|(scalars, input)| Tmpl::Map {
                scalars,
                input: Box::new(input),
            });
        let project = kw("Project")
            .ignore_then(bracket_pexpr.clone())
            .then(tmpl.clone())
            .map(|(outputs, input)| Tmpl::Project {
                outputs,
                input: Box::new(input),
            });
        let reduce = kw("Reduce")
            .ignore_then(just(Token::LBrack))
            .ignore_then(pexpr.clone())
            .then_ignore(just(Token::Comma))
            .then(pexpr.clone())
            .then_ignore(just(Token::RBrack))
            .then(tmpl.clone())
            .map(|((group_key, aggregates), input)| Tmpl::Reduce {
                group_key,
                aggregates,
                input: Box::new(input),
            });
        let flatmap = kw("FlatMap")
            .ignore_then(just(Token::LBrack))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ident())
            .then_ignore(just(Token::RBrack))
            .then(tmpl.clone())
            .map(|((func, exprs), input)| Tmpl::FlatMap {
                func,
                exprs,
                input: Box::new(input),
            });
        let negate = kw("Negate")
            .ignore_then(tmpl.clone())
            .map(|t| Tmpl::Negate(Box::new(t)));
        let threshold = kw("Threshold")
            .ignore_then(tmpl.clone())
            .map(|t| Tmpl::Threshold(Box::new(t)));
        let join = kw("Join")
            .ignore_then(bracket_pexpr.clone())
            .then(listtmpl.clone())
            .map(|(equivalences, inputs)| Tmpl::Join {
                equivalences,
                inputs,
            });
        let wcojoin = kw("WcoJoin")
            .ignore_then(bracket_pexpr.clone())
            .then(listtmpl.clone())
            .map(|(equivalences, inputs)| Tmpl::WcoJoin {
                equivalences,
                inputs,
            });
        let union = kw("Union")
            .ignore_then(listtmpl.clone())
            .map(|inputs| Tmpl::Union { inputs });
        let tsunary = kw("Unary")
            .ignore_then(bracket_ident())
            .then(tmpl.clone())
            .map(|(func, input)| Tmpl::SUnary {
                func,
                input: Box::new(input),
            });
        let tsvariadic = kw("Variadic")
            .ignore_then(bracket_ident())
            .then(listtmpl.clone())
            .map(|(func, inputs)| Tmpl::SVariadic { func, inputs });
        // `Binary[negate(<ident>)](a, b)`: builds a binary call whose function is
        // `negate(func)` for the `BinaryFunc` metavar `func` bound by an
        // `SBinaryVar` pattern. The `negate(...)` sub-bracket is fixed syntax
        // (not a general PExpr), so it is spelled out token-by-token rather than
        // reusing `bracket_pexpr`.
        let tsbinary_negate = kw("Binary")
            .ignore_then(just(Token::LBrack))
            .ignore_then(kw("negate"))
            .ignore_then(just(Token::LParen))
            .ignore_then(ident())
            .then_ignore(just(Token::RParen))
            .then_ignore(just(Token::RBrack))
            .then_ignore(just(Token::LParen))
            .then(tmpl.clone())
            .then_ignore(just(Token::Comma))
            .then(tmpl.clone())
            .then_ignore(just(Token::RParen))
            .map(|((func, e1), e2)| Tmpl::SBinaryNegate {
                func,
                expr1: Box::new(e1),
                expr2: Box::new(e2),
            });
        let tsif = kw("If")
            .ignore_then(just(Token::LParen))
            .ignore_then(tmpl.clone())
            .then_ignore(just(Token::Comma))
            .then(tmpl.clone())
            .then_ignore(just(Token::Comma))
            .then(tmpl.clone())
            .then_ignore(just(Token::RParen))
            .map(|((cond, then), els)| Tmpl::SIf {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            });
        let hole_or_relvar = relvar_ident().map(|name| {
            if name == "_" {
                Tmpl::Hole
            } else {
                Tmpl::RelVar(name)
            }
        });
        let sbool = choice((
            kw("true").to(Tmpl::SBool(true)),
            kw("false").to(Tmpl::SBool(false)),
        ));
        // `builtin` is `ident "(" args ")"`, the same shape as every keyword-led
        // template above (e.g. `Empty(r)`). It must come after them in `choice`
        // so it does not shadow them, and before `hole_or_relvar` (a bare
        // ident) so it is not shadowed in turn: `hole_or_relvar` would happily
        // match just the name and leave the trailing `(...)` unparsed.
        let builtin = ident()
            .then(
                relvar_ident()
                    .separated_by(just(Token::Comma))
                    .collect::<Vec<_>>()
                    .delimited_by(just(Token::LParen), just(Token::RParen)),
            )
            .map(|(name, args)| Tmpl::Builtin { name, args });

        choice((
            paren,
            empty,
            filter,
            map,
            project,
            reduce,
            flatmap,
            negate,
            threshold,
            join,
            wcojoin,
            union,
            tsif,
            tsvariadic,
            tsunary,
            tsbinary_negate,
            sbool,
            builtin,
            hole_or_relvar,
        ))
    });

    // --- side conditions ---
    let one_ident = |name: &'static str| {
        kw(name).ignore_then(ident().delimited_by(just(Token::LParen), just(Token::RParen)))
    };
    let two_idents = |name: &'static str| {
        kw(name)
            .ignore_then(just(Token::LParen))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ident())
            .then_ignore(just(Token::RParen))
    };
    let cond = choice((
        two_idents("uses_only_input").map(|(payload, rel)| Cond::UsesOnlyInput { payload, rel }),
        kw("cols_in_range")
            .ignore_then(just(Token::LParen))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|((payload, lo), hi)| Cond::ColsInRange { payload, lo, hi }),
        one_ident("non_negative").map(|rel| Cond::NonNegative { rel }),
        one_ident("monotonic").map(|rel| Cond::Monotonic { rel }),
        two_idents("is_unique_key").map(|(payload, rel)| Cond::IsUniqueKey { payload, rel }),
        one_ident("empty").map(|payload| Cond::Empty { payload }),
        one_ident("all_true").map(|payload| Cond::AllTrue { payload }),
        one_ident("any_false").map(|payload| Cond::AnyFalse { payload }),
        one_ident("no_false").map(|payload| Cond::NoFalse { payload }),
        one_ident("no_error").map(|payload| Cond::NoError { payload }),
        one_ident("all_columns").map(|payload| Cond::AllColumns { payload }),
        one_ident("is_rel_empty").map(|rel| Cond::IsRelEmpty { rel }),
        one_ident("not_rel_empty").map(|rel| Cond::NotRelEmpty { rel }),
        two_idents("produces_key").map(|(rel, key)| Cond::ProducesKey { rel, key }),
        two_idents("identity_projection")
            .map(|(payload, rel)| Cond::IdentityProjection { payload, rel }),
        kw("join_is_cyclic")
            .ignore_then(just(Token::LParen))
            .then_ignore(just(Token::RParen))
            .to(Cond::JoinIsCyclic),
        kw("has_three_or_more_inputs")
            .ignore_then(just(Token::LParen))
            .then_ignore(just(Token::RParen))
            .to(Cond::HasThreeOrMoreInputs),
        kw("is_binary_join")
            .ignore_then(just(Token::LParen))
            .then_ignore(just(Token::RParen))
            .to(Cond::IsBinaryJoin),
        kw("has_inner_equiv")
            .ignore_then(just(Token::LParen))
            .ignore_then(ident())
            .then_ignore(just(Token::Comma))
            .then(ixexpr.clone())
            .then_ignore(just(Token::RParen))
            .map(|(payload, boundary)| Cond::HasInnerEquiv { payload, boundary }),
        two_idents("non_identity_projection")
            .map(|(payload, rel)| Cond::NonIdentityProjection { payload, rel }),
        one_ident("reads_indexed_global").map(|rel| Cond::ReadsIndexedGlobal { rel }),
        // Nested one level to stay within chumsky's 26-element `choice` tuple limit.
        choice((
            one_ident("scalar_lit_true").map(|scalar| Cond::ScalarLitTrue { scalar }),
            one_ident("scalar_lit_false_or_null")
                .map(|scalar| Cond::ScalarLitFalseOrNull { scalar }),
            one_ident("scalar_no_error").map(|scalar| Cond::ScalarNoError { scalar }),
            one_ident("scalar_any_lit_false").map(|list| Cond::AnyScalarLit { list, value: false }),
            one_ident("scalar_any_lit_true").map(|list| Cond::AnyScalarLit { list, value: true }),
            one_ident("scalar_non_nullable").map(|scalar| Cond::ScalarNonNullable { scalar }),
        )),
    ));

    // --- rule ---
    let phasekw = choice((
        kw("logical").to(Phase::Logical),
        kw("physical").to(Phase::Physical),
        kw("both").to(Phase::Both),
    ));
    let conds = kw("where").ignore_then(cond).repeated().collect::<Vec<_>>();

    let rule = kw("rule")
        .ignore_then(ident())
        .then_ignore(just(Token::LBrace))
        .then(kw("doc").ignore_then(string()).or_not())
        .then(kw("phase").ignore_then(phasekw).or_not())
        // `colored` keyword is optional, must follow `phase` in the rule header.
        .then(kw("colored").or_not().map(|o| o.is_some()))
        .then(pat)
        .then_ignore(just(Token::Arrow))
        .then(tmpl)
        .then(conds)
        .then_ignore(just(Token::RBrace))
        .map(
            |((((((name, doc), phase), colored), lhs), rhs), conds)| Rule {
                name,
                doc,
                phase: phase.unwrap_or(Phase::Both),
                colored,
                lhs,
                rhs,
                conds,
            },
        );

    rule.repeated().collect::<Vec<_>>().then_ignore(end())
}

// NOTE: `grammar.rs` is a build-script module (`#[path]`-included by
// `build.rs`), not part of the crate's normal lib/test target, so `cargo
// test` does not run this. It documents the expected parse shape; behavioral
// coverage comes from `cargo check` failing to build if a real `.rewrite`
// rule using `Unary[..](..)` fails to parse.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scalar_unary_nested() {
        let src = "rule not_not { Unary[not](Unary[not](x)) => x }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(rules.len(), 1);
        match &rules[0].lhs {
            crate::dsl::Pat::SUnary { func, input } => {
                assert_eq!(func, "not");
                assert!(matches!(**input, crate::dsl::Pat::SUnary { .. }));
            }
            other => panic!("expected SUnary, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_variadic_single() {
        let src = "rule and_single { Variadic[and](x) => x }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SVariadic { func, inputs } => {
                assert_eq!(func, "and");
                assert_eq!(inputs.items.len(), 1);
                assert!(inputs.rest.is_none());
            }
            other => panic!("expected SVariadic, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_demorgan_mapsplice() {
        let src = "rule not_demorgan_and { Unary[not](Variadic[and](xs...)) => Variadic[or](map(Unary[not](_), xs)) }";
        let rules = crate::grammar::parse(src).expect("parses");
        // LHS: Not(And(xs...))
        match &rules[0].lhs {
            crate::dsl::Pat::SUnary { input, .. } => match &**input {
                crate::dsl::Pat::SVariadic { func, inputs } => {
                    assert_eq!(func, "and");
                    assert!(inputs.items.is_empty());
                    assert_eq!(inputs.rest.as_deref(), Some("xs"));
                }
                other => panic!("expected SVariadic input, got {other:?}"),
            },
            other => panic!("expected SUnary root, got {other:?}"),
        }
        // RHS: Or(map(Not(_), xs))
        match &rules[0].rhs {
            crate::dsl::Tmpl::SVariadic { func, inputs } => {
                assert_eq!(func, "or");
                assert_eq!(inputs.elems.len(), 1);
                assert!(matches!(
                    inputs.elems[0],
                    crate::dsl::TElem::MapSplice { .. }
                ));
            }
            other => panic!("expected SVariadic template, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_if_with_cond() {
        let src = "rule if_true { If(c, t, e) => t where scalar_lit_true(c) }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert!(matches!(rules[0].lhs, crate::dsl::Pat::SIf { .. }));
        assert!(matches!(
            rules[0].conds[0],
            crate::dsl::Cond::ScalarLitTrue { .. }
        ));
    }

    #[test]
    fn parses_scalar_if_nonlinear() {
        let src = "rule if_same { If(c, x, x) => x where scalar_no_error(c) }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SIf { then, els, .. } => {
                assert!(matches!(**then, crate::dsl::Pat::RelVar(ref n) if n == "x"));
                assert!(matches!(**els, crate::dsl::Pat::RelVar(ref n) if n == "x"));
            }
            _ => panic!("expected SIf"),
        }
    }

    #[test]
    fn parses_scalar_builtin() {
        let src = "rule const_fold { Scalar(e) => const_eval(e) }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(
            rules[0].lhs,
            crate::dsl::Pat::Scalar {
                binding: "e".to_string()
            }
        );
        assert_eq!(
            rules[0].rhs,
            crate::dsl::Tmpl::Builtin {
                name: "const_eval".to_string(),
                args: vec!["e".to_string()],
            }
        );
    }

    #[test]
    fn parses_sbool_and_empty_variadic() {
        let src = "rule and_empty { Variadic[and]() => true }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(
            rules[0].lhs,
            crate::dsl::Pat::SVariadic {
                func: "and".to_string(),
                inputs: crate::dsl::ListPat {
                    items: vec![],
                    rest: None,
                },
            }
        );
        assert_eq!(rules[0].rhs, crate::dsl::Tmpl::SBool(true));
    }

    #[test]
    fn parses_binary_var_and_negate_template() {
        let src =
            "rule not_binary_negate { Unary[not](Binary[f](a, b)) => Binary[negate(f)](a, b) }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SUnary { func, input } => {
                assert_eq!(func, "not");
                assert_eq!(
                    **input,
                    crate::dsl::Pat::SBinaryVar {
                        func: "f".to_string(),
                        expr1: Box::new(crate::dsl::Pat::RelVar("a".to_string())),
                        expr2: Box::new(crate::dsl::Pat::RelVar("b".to_string())),
                    }
                );
            }
            other => panic!("expected SUnary root, got {other:?}"),
        }
        assert_eq!(
            rules[0].rhs,
            crate::dsl::Tmpl::SBinaryNegate {
                func: "f".to_string(),
                expr1: Box::new(crate::dsl::Tmpl::RelVar("a".to_string())),
                expr2: Box::new(crate::dsl::Tmpl::RelVar("b".to_string())),
            }
        );
    }

    #[test]
    fn parses_isnull_fold_with_scalar_non_nullable_cond() {
        let src = "rule isnull_fold { Unary[isnull](x) => false \
            where scalar_no_error(x) where scalar_non_nullable(x) }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert!(matches!(
            &rules[0].lhs,
            crate::dsl::Pat::SUnary { func, .. } if func == "isnull"
        ));
        assert_eq!(rules[0].rhs, crate::dsl::Tmpl::SBool(false));
        assert_eq!(rules[0].conds.len(), 2);
        assert!(matches!(
            rules[0].conds[0],
            crate::dsl::Cond::ScalarNoError { .. }
        ));
        assert!(matches!(
            rules[0].conds[1],
            crate::dsl::Cond::ScalarNonNullable { .. }
        ));
    }

    /// `Binary` is a new leading keyword in both pattern and template position.
    /// Probes that it neither shadows nor is shadowed by neighboring
    /// productions: a bare lowercase metavariable named `binary` still parses
    /// as a plain `RelVar` (the `Binary` keyword match is an exact-string
    /// `kw()` filter, not a prefix match), and a standalone `Binary[f](a, b)`
    /// pattern (not nested under `Unary`) parses as a full rule root without
    /// falling through to `relvar` (which would reject it: `is_operator`
    /// filters out any identifier starting with an uppercase letter).
    #[test]
    fn binary_keyword_does_not_shadow_or_get_shadowed() {
        let src = "rule binary_metavar_is_plain_relvar { binary => binary }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(rules[0].lhs, crate::dsl::Pat::RelVar("binary".to_string()));
        assert_eq!(rules[0].rhs, crate::dsl::Tmpl::RelVar("binary".to_string()));

        let src = "rule binary_var_root { Binary[f](a, b) => a }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(
            rules[0].lhs,
            crate::dsl::Pat::SBinaryVar {
                func: "f".to_string(),
                expr1: Box::new(crate::dsl::Pat::RelVar("a".to_string())),
                expr2: Box::new(crate::dsl::Pat::RelVar("b".to_string())),
            }
        );
    }

    /// `scalar_any_lit_false`/`scalar_any_lit_true` name the same rest metavar
    /// a `Variadic` pattern binds (`xs` in `Variadic[and](xs...)`), confirming
    /// the cond is grammar-general over a variadic's rest-captured list rather
    /// than tied to a scalar-only production.
    #[test]
    fn parses_scalar_any_lit_cond() {
        let src = "rule r { Variadic[and](xs...) => false where scalar_any_lit_false(xs) }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SVariadic { func, inputs } => {
                assert_eq!(func, "and");
                assert_eq!(inputs.rest.as_deref(), Some("xs"));
            }
            other => panic!("expected SVariadic, got {other:?}"),
        }
        assert_eq!(rules[0].conds.len(), 1);
        assert_eq!(
            rules[0].conds[0],
            crate::dsl::Cond::AnyScalarLit {
                list: "xs".to_string(),
                value: false,
            }
        );

        let src = "rule r { Variadic[and](xs...) => false where scalar_any_lit_true(xs) }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert_eq!(
            rules[0].conds[0],
            crate::dsl::Cond::AnyScalarLit {
                list: "xs".to_string(),
                value: true,
            }
        );
    }
}
