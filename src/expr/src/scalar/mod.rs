// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;
use std::mem;

use mz_ore::stack::RecursionLimitError;
use mz_proto::IntoRustIfSome;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_ore::collections::CollectionExt;
use mz_ore::iter::IteratorExt;
use mz_ore::str::separated;
use mz_pgrepr::TypeFromOidError;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::array::InvalidArrayError;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::regex::Regex;
use mz_repr::arb_datum;
use mz_repr::strconv::{ParseError, ParseHexError};
use mz_repr::{ColumnType, Datum, Row, RowArena, ScalarType};

use self::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use self::proto_eval_error::proto_incompatible_array_dimensions::ProtoDims;
use crate::scalar::func::parse_timezone;
use crate::scalar::proto_mir_scalar_expr::*;
use crate::visit::{Visit, VisitChildren};

pub mod func;
pub mod like_pattern;

include!(concat!(env!("OUT_DIR"), "/mz_expr.scalar.rs"));

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum MirScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Result<Row, EvalError>, ColumnType),
    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `MirScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    CallUnmaterializable(UnmaterializableFunc),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<MirScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<MirScalarExpr>,
        expr2: Box<MirScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<MirScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<MirScalarExpr>,
        then: Box<MirScalarExpr>,
        els: Box<MirScalarExpr>,
    },
}

impl Arbitrary for MirScalarExpr {
    type Parameters = ();
    type Strategy = BoxedStrategy<MirScalarExpr>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let leaf = prop::strategy::Union::new(vec![
            (0..10)
                .prop_map(|i| MirScalarExpr::Column(i as usize))
                .boxed(),
            (arb_datum(), any::<ScalarType>())
                .prop_map(|(datum, typ)| MirScalarExpr::literal(Ok((&datum).into()), typ))
                .boxed(),
            (any::<EvalError>(), any::<ScalarType>())
                .prop_map(|(err, typ)| MirScalarExpr::literal(Err(err), typ))
                .boxed(),
            any::<UnmaterializableFunc>()
                .prop_map(MirScalarExpr::CallUnmaterializable)
                .boxed(),
        ]);
        leaf.prop_recursive(3, 6, 7, |inner| {
            prop::strategy::Union::new(vec![
                (
                    any::<VariadicFunc>(),
                    prop::collection::vec(inner.clone(), 1..5),
                )
                    .prop_map(|(func, exprs)| MirScalarExpr::CallVariadic { func, exprs })
                    .boxed(),
                (any::<BinaryFunc>(), inner.clone(), inner.clone())
                    .prop_map(|(func, expr1, expr2)| MirScalarExpr::CallBinary {
                        func,
                        expr1: Box::new(expr1),
                        expr2: Box::new(expr2),
                    })
                    .boxed(),
                (inner.clone(), inner.clone(), inner.clone())
                    .prop_map(|(cond, then, els)| MirScalarExpr::If {
                        cond: Box::new(cond),
                        then: Box::new(then),
                        els: Box::new(els),
                    })
                    .boxed(),
                (any::<UnaryFunc>(), inner)
                    .prop_map(|(func, expr)| MirScalarExpr::CallUnary {
                        func,
                        expr: Box::new(expr),
                    })
                    .boxed(),
            ])
        })
        .boxed()
    }
}

impl RustType<ProtoMirScalarExpr> for MirScalarExpr {
    fn into_proto(&self) -> ProtoMirScalarExpr {
        use proto_mir_scalar_expr::Kind::*;
        ProtoMirScalarExpr {
            kind: Some(match self {
                MirScalarExpr::Column(i) => Column(i.into_proto()),
                MirScalarExpr::Literal(lit, typ) => Literal(ProtoLiteral {
                    lit: Some(lit.into_proto()),
                    typ: Some(typ.into_proto()),
                }),
                MirScalarExpr::CallUnmaterializable(func) => {
                    CallUnmaterializable(func.into_proto())
                }
                MirScalarExpr::CallUnary { func, expr } => CallUnary(Box::new(ProtoCallUnary {
                    func: Some(Box::new(func.into_proto())),
                    expr: Some(expr.into_proto()),
                })),
                MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                    CallBinary(Box::new(ProtoCallBinary {
                        func: Some(func.into_proto()),
                        expr1: Some(expr1.into_proto()),
                        expr2: Some(expr2.into_proto()),
                    }))
                }
                MirScalarExpr::CallVariadic { func, exprs } => CallVariadic(ProtoCallVariadic {
                    func: Some(func.into_proto()),
                    exprs: exprs.into_proto(),
                }),
                MirScalarExpr::If { cond, then, els } => If(Box::new(ProtoIf {
                    cond: Some(cond.into_proto()),
                    then: Some(then.into_proto()),
                    els: Some(els.into_proto()),
                })),
            }),
        }
    }

    fn from_proto(proto: ProtoMirScalarExpr) -> Result<Self, TryFromProtoError> {
        use proto_mir_scalar_expr::Kind::*;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoMirScalarExpr::kind"))?;
        Ok(match kind {
            Column(i) => MirScalarExpr::Column(usize::from_proto(i)?),
            Literal(ProtoLiteral { lit, typ }) => MirScalarExpr::Literal(
                lit.into_rust_if_some("ProtoLiteral::lit")?,
                typ.into_rust_if_some("ProtoLiteral::typ")?,
            ),
            CallUnmaterializable(func) => MirScalarExpr::CallUnmaterializable(func.into_rust()?),
            CallUnary(call_unary) => MirScalarExpr::CallUnary {
                func: call_unary.func.into_rust_if_some("ProtoCallUnary::func")?,
                expr: call_unary.expr.into_rust_if_some("ProtoCallUnary::expr")?,
            },
            CallBinary(call_binary) => MirScalarExpr::CallBinary {
                func: call_binary
                    .func
                    .into_rust_if_some("ProtoCallBinary::func")?,
                expr1: call_binary
                    .expr1
                    .into_rust_if_some("ProtoCallBinary::expr1")?,
                expr2: call_binary
                    .expr2
                    .into_rust_if_some("ProtoCallBinary::expr2")?,
            },
            CallVariadic(ProtoCallVariadic { func, exprs }) => MirScalarExpr::CallVariadic {
                func: func.into_rust_if_some("ProtoCallVariadic::func")?,
                exprs: exprs.into_rust()?,
            },
            If(if_struct) => MirScalarExpr::If {
                cond: if_struct.cond.into_rust_if_some("ProtoIf::cond")?,
                then: if_struct.then.into_rust_if_some("ProtoIf::then")?,
                els: if_struct.els.into_rust_if_some("ProtoIf::els")?,
            },
        })
    }
}

impl RustType<proto_literal::ProtoLiteralData> for Result<Row, EvalError> {
    fn into_proto(&self) -> proto_literal::ProtoLiteralData {
        use proto_literal::proto_literal_data::Result::*;
        proto_literal::ProtoLiteralData {
            result: Some(match self {
                Result::Ok(row) => Ok(row.into_proto()),
                Result::Err(err) => Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: proto_literal::ProtoLiteralData) -> Result<Self, TryFromProtoError> {
        use proto_literal::proto_literal_data::Result::*;
        match proto.result {
            Some(Ok(row)) => Result::Ok(Result::Ok(
                (&row)
                    .try_into()
                    .map_err(TryFromProtoError::RowConversionError)?,
            )),
            Some(Err(err)) => Result::Ok(Result::Err(err.into_rust()?)),
            None => Result::Err(TryFromProtoError::missing_field("ProtoLiteralData::result")),
        }
    }
}

impl MirScalarExpr {
    pub fn columns(is: &[usize]) -> Vec<MirScalarExpr> {
        is.iter().map(|i| MirScalarExpr::Column(*i)).collect()
    }

    pub fn column(column: usize) -> Self {
        MirScalarExpr::Column(column)
    }

    pub fn literal(res: Result<Datum, EvalError>, typ: ScalarType) -> Self {
        let typ = typ.nullable(matches!(res, Ok(Datum::Null)));
        let row = res.map(|datum| Row::pack_slice(&[datum]));
        MirScalarExpr::Literal(row, typ)
    }

    pub fn literal_ok(datum: Datum, typ: ScalarType) -> Self {
        MirScalarExpr::literal(Ok(datum), typ)
    }

    pub fn literal_null(typ: ScalarType) -> Self {
        MirScalarExpr::literal_ok(Datum::Null, typ)
    }

    pub fn literal_false() -> Self {
        MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool)
    }

    pub fn literal_true() -> Self {
        MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool)
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        MirScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        MirScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn if_then_else(self, t: Self, f: Self) -> Self {
        MirScalarExpr::If {
            cond: Box::new(self),
            then: Box::new(t),
            els: Box::new(f),
        }
    }

    pub fn or(self, other: Self) -> Self {
        MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or,
            exprs: vec![self, other],
        }
    }

    pub fn and(self, other: Self) -> Self {
        MirScalarExpr::CallVariadic {
            func: VariadicFunc::And,
            exprs: vec![self, other],
        }
    }

    pub fn not(self) -> Self {
        self.call_unary(UnaryFunc::Not(func::Not))
    }

    pub fn call_is_null(self) -> Self {
        self.call_unary(UnaryFunc::IsNull(func::IsNull))
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        #[allow(deprecated)]
        self.visit_mut_post_nolimit(&mut |e| {
            if let MirScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &std::collections::HashMap<usize, usize>) {
        #[allow(deprecated)]
        self.visit_mut_post_nolimit(&mut |e| {
            if let MirScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> HashSet<usize> {
        let mut support = HashSet::new();
        #[allow(deprecated)]
        self.visit_post_nolimit(&mut |e| {
            if let MirScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn take(&mut self) -> Self {
        mem::replace(self, MirScalarExpr::literal_null(ScalarType::String))
    }

    pub fn as_literal(&self) -> Option<Result<Datum, &EvalError>> {
        if let MirScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.as_ref().map(|row| row.unpack_first()))
        } else {
            None
        }
    }

    pub fn as_literal_str(&self) -> Option<&str> {
        match self.as_literal() {
            Some(Ok(Datum::String(s))) => Some(s),
            _ => None,
        }
    }

    pub fn as_literal_err(&self) -> Option<&EvalError> {
        self.as_literal().and_then(|lit| lit.err())
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(_, _))
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Ok(Datum::True)) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Ok(Datum::False)) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Ok(Datum::Null)) == self.as_literal()
    }

    pub fn is_literal_ok(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(Ok(_), _typ))
    }

    pub fn is_literal_err(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(Err(_), _typ))
    }

    /// If self is a column, return the column index, otherwise `None`.
    pub fn as_column(&self) -> Option<usize> {
        if let MirScalarExpr::Column(c) = self {
            Some(*c)
        } else {
            None
        }
    }

    /// Reduces a complex expression where possible.
    ///
    /// Also canonicalizes the expression.
    ///
    /// ```rust
    /// use mz_expr::MirScalarExpr;
    /// use mz_repr::{ColumnType, Datum, ScalarType};
    ///
    /// let expr_0 = MirScalarExpr::Column(0);
    /// let expr_t = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
    /// let expr_f = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
    ///
    /// let mut test =
    /// expr_t
    ///     .clone()
    ///     .and(expr_f.clone())
    ///     .if_then_else(expr_0, expr_t.clone());
    ///
    /// let input_type = vec![ScalarType::Int32.nullable(false)];
    /// test.reduce(&input_type);
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self, column_types: &[ColumnType]) {
        let temp_storage = &RowArena::new();
        let eval = |e: &MirScalarExpr| {
            MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(column_types).scalar_type)
        };

        // Simplifications run in a loop until `self` no longer changes.
        let mut old_self = MirScalarExpr::column(0);
        while old_self != *self {
            old_self = self.clone();
            #[allow(deprecated)]
            self.visit_mut_pre_post_nolimit(
                &mut |e| {
                    match e {
                        MirScalarExpr::CallUnary { func, expr } => {
                            if *func == UnaryFunc::IsNull(func::IsNull) {
                                // Decompose IsNull expressions into a disjunction
                                // of simpler IsNull subexpressions

                                if let Some(expr) = expr.decompose_is_null() {
                                    *e = expr
                                }
                            } else if *func == UnaryFunc::Not(func::Not) {
                                // Push down not expressions
                                match &mut **expr {
                                    // Two negates cancel each other out.
                                    MirScalarExpr::CallUnary {
                                        expr: inner_expr,
                                        func: UnaryFunc::Not(func::Not),
                                    } => *e = inner_expr.take(),
                                    // Transforms `NOT(a <op> b)` to `a negate(<op>) b`
                                    // if a negation exists.
                                    MirScalarExpr::CallBinary { expr1, expr2, func } => {
                                        if let Some(negated_func) = func.negate() {
                                            *e = MirScalarExpr::CallBinary {
                                                expr1: Box::new(expr1.take()),
                                                expr2: Box::new(expr2.take()),
                                                func: negated_func,
                                            }
                                        }
                                    }
                                    MirScalarExpr::CallVariadic { .. } => {
                                        e.demorgans();
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    };
                    None
                },
                &mut |e| match e {
                    // Evaluate and pull up constants
                    MirScalarExpr::Column(_)
                    | MirScalarExpr::Literal(_, _)
                    | MirScalarExpr::CallUnmaterializable(_) => (),
                    MirScalarExpr::CallUnary { func, expr } => {
                        if expr.is_literal() {
                            *e = eval(e);
                        } else if let UnaryFunc::RecordGet(func::RecordGet(i)) = *func {
                            if let MirScalarExpr::CallVariadic {
                                func: VariadicFunc::RecordCreate { .. },
                                exprs,
                            } = &mut **expr
                            {
                                *e = exprs.swap_remove(i);
                            }
                        }
                    }
                    MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                        if expr1.is_literal() && expr2.is_literal() {
                            *e = eval(e);
                        } else if (expr1.is_literal_null() || expr2.is_literal_null())
                            && func.propagates_nulls()
                        {
                            *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
                        } else if let Some(err) = expr1.as_literal_err() {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(column_types).scalar_type,
                            );
                        } else if let Some(err) = expr2.as_literal_err() {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(column_types).scalar_type,
                            );
                        } else if let BinaryFunc::IsLikeMatch { case_insensitive } = func {
                            if expr2.is_literal() {
                                // We can at least precompile the regex.
                                let pattern = expr2.as_literal_str().unwrap();
                                *e = match like_pattern::compile(&pattern, *case_insensitive) {
                                    Ok(matcher) => expr1.take().call_unary(UnaryFunc::IsLikeMatch(
                                        func::IsLikeMatch(matcher),
                                    )),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(column_types).scalar_type,
                                    ),
                                };
                            }
                        } else if let BinaryFunc::IsRegexpMatch { case_insensitive } = func {
                            if let MirScalarExpr::Literal(Ok(row), _) = &**expr2 {
                                let flags = if *case_insensitive { "i" } else { "" };
                                *e = match func::build_regex(row.unpack_first().unwrap_str(), flags)
                                {
                                    Ok(regex) => expr1.take().call_unary(UnaryFunc::IsRegexpMatch(
                                        func::IsRegexpMatch(Regex(regex)),
                                    )),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(column_types).scalar_type,
                                    ),
                                };
                            }
                        } else if *func == BinaryFunc::ExtractInterval && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractInterval(func::ExtractInterval(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTime && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTime(func::ExtractTime(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTimestamp(func::ExtractTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTimestampTz(func::ExtractTimestampTz(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractDate && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractDate(func::ExtractDate(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartInterval && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartInterval(func::DatePartInterval(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTime && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTime(func::DatePartTime(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTimestamp(func::DatePartTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTimestampTz(
                                        func::DatePartTimestampTz(units),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DateTruncTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DateTruncTimestamp(func::DateTruncTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DateTruncTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DateTruncTimestampTz(
                                        func::DateTruncTimestampTz(units),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::TimezoneTimestamp && expr1.is_literal() {
                            // If the timezone argument is a literal, and we're applying the function on many rows at the same
                            // time we really don't want to parse it again and again, so we parse it once and embed it into the
                            // UnaryFunc enum. The memory footprint of Timezone is small (8 bytes).
                            let tz = expr1.as_literal_str().unwrap();
                            *e = match parse_timezone(tz) {
                                Ok(tz) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::TimezoneTimestamp(func::TimezoneTimestamp(tz)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::TimezoneTimestampTz && expr1.is_literal() {
                            let tz = expr1.as_literal_str().unwrap();
                            *e = match parse_timezone(tz) {
                                Ok(tz) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::TimezoneTimestampTz(
                                        func::TimezoneTimestampTz(tz),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(column_types).scalar_type,
                                ),
                            }
                        } else if let BinaryFunc::TimezoneTime { wall_time } = func {
                            if expr1.is_literal() {
                                let tz = expr1.as_literal_str().unwrap();
                                *e = match parse_timezone(tz) {
                                    Ok(tz) => MirScalarExpr::CallUnary {
                                        func: UnaryFunc::TimezoneTime(func::TimezoneTime {
                                            tz,
                                            wall_time: *wall_time,
                                        }),
                                        expr: Box::new(expr2.take()),
                                    },
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(column_types).scalar_type,
                                    ),
                                }
                            }
                        } else if matches!(*func, BinaryFunc::Eq | BinaryFunc::NotEq)
                            && expr2 < expr1
                        {
                            // Canonically order elements so that deduplication works better.
                            mem::swap(expr1, expr2);
                        } else if let (
                            BinaryFunc::Eq,
                            MirScalarExpr::Literal(
                                Ok(lit_row),
                                ColumnType {
                                    scalar_type:
                                        ScalarType::Record {
                                            fields: field_types,
                                            ..
                                        },
                                    ..
                                },
                            ),
                            MirScalarExpr::CallVariadic {
                                func: VariadicFunc::RecordCreate { .. },
                                exprs: rec_create_args,
                            },
                        ) = (func, (**expr1).clone(), (**expr2).clone())
                        {
                            // Literal([c1, c2]) = record_create(e1, e2)
                            //  -->
                            // c1 = e1 AND c2 = e2
                            //
                            // (Records are represented as lists.)
                            //
                            // `MapFilterProject::literal_constraints` relies on this transform,
                            // because `(e1,e2) IN ((1,2))` is desugared using `record_create`.

                            match lit_row.unpack_first() {
                                Datum::List(datum_list) => {
                                    *e = MirScalarExpr::CallVariadic {
                                        func: VariadicFunc::And,
                                        exprs: itertools::izip!(
                                            datum_list.iter(),
                                            field_types,
                                            rec_create_args
                                        )
                                        .map(|(d, (_, typ), a)| MirScalarExpr::CallBinary {
                                            func: BinaryFunc::Eq,
                                            expr1: Box::new(MirScalarExpr::Literal(
                                                Ok(Row::pack_slice(&[d])),
                                                typ,
                                            )),
                                            expr2: Box::new(a),
                                        })
                                        .collect(),
                                    };
                                }
                                _ => {}
                            }
                        }
                    }
                    MirScalarExpr::CallVariadic { func, exprs } => {
                        if *func == VariadicFunc::Coalesce {
                            // If all inputs are null, output is null. This check must
                            // be done before `exprs.retain...` because `e.typ` requires
                            // > 0 `exprs` remain.
                            if exprs.iter().all(|expr| expr.is_literal_null()) {
                                *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
                                return;
                            }

                            // Remove any null values if not all values are null.
                            exprs.retain(|e| !e.is_literal_null());

                            // Find the first argument that is a literal or non-nullable
                            // column. All arguments after it get ignored, so throw them
                            // away. This intentionally throws away errors that can
                            // never happen.
                            if let Some(i) = exprs
                                .iter()
                                .position(|e| e.is_literal() || !e.typ(column_types).nullable)
                            {
                                exprs.truncate(i + 1);
                            }

                            // Deduplicate arguments in cases like `coalesce(#0, #0)`.
                            let mut prior_exprs = HashSet::new();
                            exprs.retain(|e| prior_exprs.insert(e.clone()));

                            if let Some(expr) = exprs.iter_mut().find(|e| e.is_literal_err()) {
                                // One of the remaining arguments is an error, so
                                // just replace the entire coalesce with that error.
                                *e = expr.take();
                            } else if exprs.len() == 1 {
                                // Only one argument, so the coalesce is a no-op.
                                *e = exprs[0].take();
                            }
                        } else if exprs.iter().all(|e| e.is_literal()) {
                            *e = eval(e);
                        } else if func.propagates_nulls()
                            && exprs.iter().any(|e| e.is_literal_null())
                        {
                            *e = MirScalarExpr::literal_null(e.typ(column_types).scalar_type);
                        } else if let Some(err) = exprs.iter().find_map(|e| e.as_literal_err()) {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(column_types).scalar_type,
                            );
                        } else if *func == VariadicFunc::RegexpMatch
                            && exprs[1].is_literal()
                            && exprs.get(2).map_or(true, |e| e.is_literal())
                        {
                            let needle = exprs[1].as_literal_str().unwrap();
                            let flags = match exprs.len() {
                                3 => exprs[2].as_literal_str().unwrap(),
                                _ => "",
                            };
                            *e = match func::build_regex(needle, flags) {
                                Ok(regex) => mem::take(exprs).into_first().call_unary(
                                    UnaryFunc::RegexpMatch(func::RegexpMatch(Regex(regex))),
                                ),
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(column_types).scalar_type,
                                ),
                            };
                        } else if *func == VariadicFunc::ListIndex && is_list_create_call(&exprs[0])
                        {
                            // We are looking for ListIndex(ListCreate, literal), and eliminate
                            // both the ListIndex and the ListCreate. E.g.: LIST[f1,f2][2] --> f2
                            let ind_exprs = exprs.split_off(1);
                            let top_list_create = exprs.swap_remove(0);
                            *e = reduce_list_create_list_index_literal(top_list_create, ind_exprs);
                        } else if *func == VariadicFunc::Or || *func == VariadicFunc::And {
                            e.flatten_and_or();
                            e.undistribute_and_or();
                            e.reduce_and_canonicalize_and_or();
                        }
                    }
                    MirScalarExpr::If { cond, then, els } => {
                        if let Some(literal) = cond.as_literal() {
                            match literal {
                                Ok(Datum::True) => *e = then.take(),
                                Ok(Datum::False) | Ok(Datum::Null) => *e = els.take(),
                                Err(err) => {
                                    *e = MirScalarExpr::Literal(
                                        Err(err.clone()),
                                        then.typ(column_types)
                                            .union(&els.typ(column_types))
                                            .unwrap(),
                                    )
                                }
                                _ => unreachable!(),
                            }
                        } else if then == els {
                            *e = then.take();
                        } else if then.is_literal_ok() && els.is_literal_ok() {
                            match (then.as_literal(), els.as_literal()) {
                                // Note: NULLs from the condition should not be propagated to the result
                                // of the expression.
                                (Some(Ok(Datum::True)), _) => {
                                    // Rewritten as ((<cond> IS NOT NULL) AND (<cond>)) OR (<els>)
                                    // NULL <cond> results in: (FALSE AND NULL) OR (<els>) => (<els>)
                                    *e = cond
                                        .clone()
                                        .call_is_null()
                                        .not()
                                        .and(cond.take())
                                        .or(els.take());
                                }
                                (Some(Ok(Datum::False)), _) => {
                                    // Rewritten as ((NOT <cond>) OR (<cond> IS NULL)) AND (<els>)
                                    // NULL <cond> results in: (NULL OR TRUE) AND (<els>) => TRUE AND (<els>) => (<els>)
                                    *e = cond
                                        .clone()
                                        .not()
                                        .or(cond.take().call_is_null())
                                        .and(els.take());
                                }
                                (_, Some(Ok(Datum::True))) => {
                                    // Rewritten as (NOT <cond>) OR (<cond> IS NULL) OR (<then>)
                                    // NULL <cond> results in: NULL OR TRUE OR (<then>) => TRUE
                                    *e = cond
                                        .clone()
                                        .not()
                                        .or(cond.take().call_is_null())
                                        .or(then.take());
                                }
                                (_, Some(Ok(Datum::False))) => {
                                    // Rewritten as (<cond> IS NOT NULL) AND (<cond>) AND (<then>)
                                    // NULL <cond> results in: FALSE AND NULL AND (<then>) => FALSE
                                    *e = cond
                                        .clone()
                                        .call_is_null()
                                        .not()
                                        .and(cond.take())
                                        .and(then.take());
                                }
                                _ => {}
                            }
                        }
                    }
                },
            );
        }

        /* #region `reduce_list_create_list_index_literal` and helper functions */

        fn list_create_type(list_create: &MirScalarExpr) -> ScalarType {
            if let MirScalarExpr::CallVariadic {
                func: VariadicFunc::ListCreate { elem_type: typ },
                ..
            } = list_create
            {
                (*typ).clone()
            } else {
                unreachable!()
            }
        }

        fn is_list_create_call(expr: &MirScalarExpr) -> bool {
            matches!(
                expr,
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListCreate { .. },
                    ..
                }
            )
        }

        /// Partial-evaluates a list indexing with a literal directly after a list creation.
        ///
        /// Multi-dimensional lists are handled by a single call to this function, with multiple
        /// elements in index_exprs (of which not all need to be literals), and nested ListCreates
        /// in list_create_to_reduce.
        ///
        /// # Examples
        ///
        /// LIST[f1,f2][2] --> f2.
        ///
        /// A multi-dimensional list, with only some of the indexes being literals:
        /// LIST[[[f1, f2], [f3, f4]], [[f5, f6], [f7, f8]]] [2][n][2] --> LIST[f6, f8] [n]
        ///
        /// See more examples in list.slt.
        fn reduce_list_create_list_index_literal(
            mut list_create_to_reduce: MirScalarExpr,
            mut index_exprs: Vec<MirScalarExpr>,
        ) -> MirScalarExpr {
            // We iterate over the index_exprs and remove literals, but keep non-literals.
            // When we encounter a non-literal, we need to dig into the nested ListCreates:
            // `list_create_mut_refs` will contain all the ListCreates of the current level. If an
            // element of `list_create_mut_refs` is not actually a ListCreate, then we break out of
            // the loop. When we remove a literal, we need to partial-evaluate all ListCreates
            // that are at the current level (except those that disappeared due to
            // literals at earlier levels), and change each element in `list_create_mut_refs`
            // to the result of the partial evaluation.
            let mut list_create_mut_refs = vec![&mut list_create_to_reduce];
            let mut i = 0;
            while i < index_exprs.len()
                && list_create_mut_refs
                    .iter()
                    .all(|lc| is_list_create_call(lc))
            {
                if index_exprs[i].is_literal_ok() {
                    // We can remove this index.
                    let removed_index = index_exprs.remove(i);
                    let index_i64 = match removed_index.as_literal().unwrap().unwrap() {
                        Datum::Int64(sql_index_i64) => sql_index_i64 - 1,
                        _ => unreachable!(), // always an Int64, see plan_index_list
                    };
                    // For each list_create referenced by list_create_mut_refs, substitute it by its
                    // `index`th argument (or null).
                    for list_create in &mut list_create_mut_refs {
                        let list_create_args = match list_create {
                            MirScalarExpr::CallVariadic {
                                func: VariadicFunc::ListCreate { .. },
                                exprs,
                            } => exprs,
                            _ => unreachable!(), // func cannot be anything else than a ListCreate
                        };
                        // ListIndex gives null on an out-of-bounds index
                        if index_i64 >= 0 && index_i64 < list_create_args.len().try_into().unwrap()
                        {
                            let index: usize = index_i64.try_into().unwrap();
                            **list_create = list_create_args.swap_remove(index);
                        } else {
                            let typ = list_create_type(list_create);
                            **list_create = MirScalarExpr::literal_null(typ);
                        }
                    }
                } else {
                    // We can't remove this index, so we can't reduce any of the ListCreates at this
                    // level. So we change list_create_mut_refs to refer to all the arguments of all
                    // the ListCreates currently referenced by list_create_mut_refs.
                    list_create_mut_refs = list_create_mut_refs
                        .into_iter()
                        .flat_map(|list_create| match list_create {
                            MirScalarExpr::CallVariadic {
                                func: VariadicFunc::ListCreate { .. },
                                exprs: list_create_args,
                            } => list_create_args,
                            // func cannot be anything else than a ListCreate
                            _ => unreachable!(),
                        })
                        .collect();
                    i += 1; // next index_expr
                }
            }
            // If all list indexes have been evaluated, return the reduced expression.
            // Otherwise, rebuild the ListIndex call with the remaining ListCreates and indexes.
            if index_exprs.is_empty() {
                assert_eq!(list_create_mut_refs.len(), 1);
                list_create_to_reduce
            } else {
                let mut exprs: Vec<MirScalarExpr> = vec![list_create_to_reduce];
                exprs.append(&mut index_exprs);
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::ListIndex,
                    exprs,
                }
            }
        }

        /* #endregion */
    }

    /// Decompose an IsNull expression into a disjunction of
    /// simpler expressions.
    ///
    /// Assumes that `self` is the expression inside of an IsNull.
    /// Returns `Some(expressions)` if the outer IsNull is to be
    /// replaced by some other expression. Note: if it returns
    /// None, it might still have mutated *self.
    fn decompose_is_null(&mut self) -> Option<MirScalarExpr> {
        // TODO: allow simplification of unmaterializable functions

        match self {
            MirScalarExpr::CallUnary {
                func,
                expr: inner_expr,
            } => {
                if !func.introduces_nulls() {
                    if func.propagates_nulls() {
                        *self = inner_expr.take();
                        return self.decompose_is_null();
                    } else {
                        // Different from CallBinary and CallVariadic, because of determinism. See
                        // https://materializeinc.slack.com/archives/C01BE3RN82F/p1657644478517709
                        return Some(MirScalarExpr::literal_false());
                    }
                }
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                // (<expr1> <op> <expr2>) IS NULL can often be simplified to
                // (<expr1> IS NULL) OR (<expr2> IS NULL).
                if func.propagates_nulls() && !func.introduces_nulls() {
                    let expr1 = expr1.take().call_is_null();
                    let expr2 = expr2.take().call_is_null();
                    return Some(expr1.or(expr2));
                }
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                if func.propagates_nulls() && !func.introduces_nulls() {
                    let exprs = exprs.into_iter().map(|e| e.take().call_is_null()).collect();
                    return Some(MirScalarExpr::CallVariadic {
                        func: VariadicFunc::Or,
                        exprs,
                    });
                }
            }
            _ => {}
        }

        None
    }

    /// Flattens a chain of ORs or a chain of ANDs
    /// todo: We could do this for any associative function
    fn flatten_and_or(&mut self) {
        if let MirScalarExpr::CallVariadic {
            exprs: outer_operands,
            func: outer_func @ (VariadicFunc::Or | VariadicFunc::And),
        } = self
        {
            *outer_operands = outer_operands
                .into_iter()
                .flat_map(|o| {
                    if let MirScalarExpr::CallVariadic {
                        exprs: inner_operands,
                        func: inner_func,
                    } = o
                    {
                        if *inner_func == *outer_func {
                            mem::take(inner_operands)
                        } else {
                            vec![o.take()]
                        }
                    } else {
                        vec![o.take()]
                    }
                })
                .collect();
        }
    }

    /* #region AND/OR canonicalization and transformations  */

    /// Canonicalizes AND/OR, and does some straightforward simplifications
    fn reduce_and_canonicalize_and_or(&mut self) {
        // We do this until fixed point, because after undistribute_and_or calls us, it relies on
        // the property that self is not an 1-arg AND/OR. Just one application of our loop body
        // can't ensure this, because the application itself might create a 1-arg AND/OR.
        let mut old_self = MirScalarExpr::column(0);
        while old_self != *self {
            old_self = self.clone();
            match self {
                MirScalarExpr::CallVariadic {
                    func: func @ (VariadicFunc::And | VariadicFunc::Or),
                    exprs,
                } => {
                    // Canonically order elements so that various deduplications work better,
                    // e.g., in undistribute_and_or.
                    // Also, extract_equal_or_both_null_inner depends on the args being sorted.
                    exprs.sort();

                    // x AND/OR x --> x
                    exprs.dedup(); // this also needs the above sorting

                    if exprs.len() == 1 {
                        // AND/OR of 1 argument evaluates to that argument
                        *self = exprs.swap_remove(0);
                    } else if exprs.len() == 0 {
                        // AND/OR of 0 arguments evaluates to true/false
                        *self = func.unit_of_and_or();
                    } else if exprs.iter().any(|e| *e == func.zero_of_and_or()) {
                        // short-circuiting
                        *self = func.zero_of_and_or();
                    } else {
                        // a AND true --> a
                        // a OR false --> a
                        exprs.retain(|e| *e != func.unit_of_and_or());
                    }
                }
                _ => {}
            }
        }
    }

    /// Transforms !(a && b) into !a || !b, and !(a || b) into !a && !b
    fn demorgans(&mut self) {
        if let MirScalarExpr::CallUnary {
            expr: inner,
            func: UnaryFunc::Not(func::Not),
        } = self
        {
            inner.flatten_and_or();
            match &mut **inner {
                MirScalarExpr::CallVariadic {
                    func: inner_func @ (VariadicFunc::And | VariadicFunc::Or),
                    exprs,
                } => {
                    *inner_func = inner_func.switch_and_or();
                    *exprs = exprs.into_iter().map(|e| e.take().not()).collect();
                    *self = (*inner).take(); // Removes the outer not
                }
                _ => {}
            }
        }
    }

    /// AND/OR undistribution to apply at each `ScalarExpr`.
    /// Transforms (a && b) || (a && c) into a && (b || c)
    /// Transforms (a || b) && (a || c) into a || (b && c)
    ///
    /// Assumes that AND/OR are already flattened (flatten_and_or)
    ///
    /// Also works with more than 2 arguments at the top, e.g.:
    /// (a && b) || (a && c) || (a && d)  -->  a && (b || c || d)
    ///
    /// It also handles the absorption law (<https://en.wikipedia.org/wiki/Absorption_law>):
    ///   a || (a && c)  -->  a
    ///   a && (a || c)  -->  a
    /// For example,
    ///   a || (a && c) || (a && d)
    ///   -->
    ///   a && (true || c || d)
    ///   -->
    ///   a && true
    ///   -->
    ///   a
    /// where only the first step is performed by this function, the rest is done by
    /// reduce_and_canonicalize_and_or.
    fn undistribute_and_or(&mut self) {
        self.reduce_and_canonicalize_and_or(); // We don't want to deal with 1-arg AND/OR
        if let MirScalarExpr::CallVariadic {
            exprs: outer_operands,
            func: outer_func @ (VariadicFunc::Or | VariadicFunc::And),
        } = self
        {
            let inner_func = outer_func.switch_and_or();

            // Make sure that each outer operand is a call to inner_func, by wrapping in a 1-arg
            // call if necessary.
            outer_operands.iter_mut().for_each(|o| {
                if !matches!(o, MirScalarExpr::CallVariadic {func: f, ..} if *f == inner_func) {
                    *o = MirScalarExpr::CallVariadic {
                        func: inner_func.clone(),
                        exprs: vec![o.take()],
                    };
                }
            });

            let mut inner_operands_refs: Vec<&mut Vec<MirScalarExpr>> = outer_operands
                .iter_mut()
                .map(|o| match o {
                    MirScalarExpr::CallVariadic { func: f, exprs } if *f == inner_func => exprs,
                    _ => unreachable!(), // the wrapping made sure that we'll get a match
                })
                .collect();

            // Find inner operands to undistribute, i.e., which are in all of the outer operands.
            let mut intersection = inner_operands_refs
                .iter()
                .map(|v| (**v).clone())
                .reduce(|ops1, ops2| ops1.into_iter().filter(|e| ops2.contains(e)).collect())
                .unwrap();
            intersection.sort();
            intersection.dedup();

            if !intersection.is_empty() {
                // Remove the intersection from the inner operands
                inner_operands_refs
                    .iter_mut()
                    .for_each(|ops| (**ops).retain(|o| !intersection.contains(o)));

                // Simplify terms that now have only 0 or 1 args due to removing the intersection.
                outer_operands
                    .iter_mut()
                    .for_each(|o| o.reduce_and_canonicalize_and_or());

                // Add the intersection at the beginning
                *self = MirScalarExpr::CallVariadic {
                    func: inner_func,
                    exprs: intersection
                        .into_iter()
                        .chain_one((*self).clone())
                        .collect(),
                };
            } else {
                // Undo the 1-arg wrapping that we did at the beginning.
                outer_operands
                    .iter_mut()
                    .for_each(|o| o.reduce_and_canonicalize_and_or());
            }
        }
    }

    /* #endregion */

    /// Adds any columns that *must* be non-Null for `self` to be non-Null.
    pub fn non_null_requirements(&self, columns: &mut HashSet<usize>) {
        match self {
            MirScalarExpr::Column(col) => {
                columns.insert(*col);
            }
            MirScalarExpr::Literal(..) => {}
            MirScalarExpr::CallUnmaterializable(_) => (),
            MirScalarExpr::CallUnary { func, expr } => {
                if func.propagates_nulls() {
                    expr.non_null_requirements(columns);
                }
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.propagates_nulls() {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                if func.propagates_nulls() {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            MirScalarExpr::If { .. } => (),
        }
    }

    pub fn typ(&self, column_types: &[ColumnType]) -> ColumnType {
        match self {
            MirScalarExpr::Column(i) => column_types[*i].clone(),
            MirScalarExpr::Literal(_, typ) => typ.clone(),
            MirScalarExpr::CallUnmaterializable(func) => func.output_type(),
            MirScalarExpr::CallUnary { expr, func } => func.output_type(expr.typ(column_types)),
            MirScalarExpr::CallBinary { expr1, expr2, func } => {
                func.output_type(expr1.typ(column_types), expr2.typ(column_types))
            }
            MirScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(column_types)).collect())
            }
            MirScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(column_types);
                let else_type = els.typ(column_types);
                then_type.union(&else_type).unwrap()
            }
        }
    }

    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            MirScalarExpr::Column(index) => Ok(datums[*index].clone()),
            MirScalarExpr::Literal(res, _column_type) => match res {
                Ok(row) => Ok(row.unpack_first()),
                Err(e) => Err(e.clone()),
            },
            // Unmaterializable functions must be transformed away before
            // evaluation. Their purpose is as a placeholder for data that is
            // not known at plan time but can be inlined before runtime.
            MirScalarExpr::CallUnmaterializable(x) => Err(EvalError::Internal(format!(
                "cannot evaluate unmaterializable function: {:?}",
                x
            ))),
            MirScalarExpr::CallUnary { func, expr } => func.eval(datums, temp_storage, expr),
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                func.eval(datums, temp_storage, expr1, expr2)
            }
            MirScalarExpr::CallVariadic { func, exprs } => func.eval(datums, temp_storage, exprs),
            MirScalarExpr::If { cond, then, els } => match cond.eval(datums, temp_storage)? {
                Datum::True => then.eval(datums, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, temp_storage),
                d => Err(EvalError::Internal(format!(
                    "if condition evaluated to non-boolean datum: {:?}",
                    d
                ))),
            },
        }
    }

    /// True iff the expression contains
    /// `UnmaterializableFunc::MzLogicalTimestamp`.
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        #[allow(deprecated)]
        self.visit_post_nolimit(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzLogicalTimestamp) = e
            {
                contains = true;
            }
        });
        contains
    }

    /// True iff the expression contains an `UnmaterializableFunc`.
    pub fn contains_unmaterializable(&self) -> bool {
        let mut contains = false;
        #[allow(deprecated)]
        self.visit_post_nolimit(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(_) = e {
                contains = true;
            }
        });
        contains
    }
}

impl fmt::Display for MirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use MirScalarExpr::*;
        match self {
            Column(i) => write!(f, "#{}", i)?,
            Literal(Ok(row), _) => write!(f, "{}", row.unpack_first())?,
            Literal(Err(e), _) => write!(f, "(err: {})", e)?,
            CallUnmaterializable(func) => write!(f, "{}()", func)?,
            CallUnary { func, expr } => {
                if let UnaryFunc::Not(_) = *func {
                    if let CallUnary {
                        func,
                        expr: inner_expr,
                    } = &**expr
                    {
                        if let Some(is) = func.is() {
                            write!(f, "({}) IS NOT {}", inner_expr, is)?;
                            return Ok(());
                        }
                    }
                }
                if let Some(is) = func.is() {
                    write!(f, "({}) IS {}", expr, is)?;
                } else {
                    write!(f, "{}({})", func, expr)?;
                }
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)?;
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)?;
                }
            }
            CallVariadic { func, exprs } => {
                if func.is_infix_op() && exprs.len() > 1 {
                    write!(f, "({})", separated(&*format!(" {} ", func), exprs.clone()))?;
                } else {
                    write!(f, "{}({})", func, separated(", ", exprs.clone()))?;
                }
            }
            If { cond, then, els } => {
                write!(f, "if {} then {{{}}} else {{{}}}", cond, then, els)?;
            }
        }
        Ok(())
    }
}

impl VisitChildren<Self> for MirScalarExpr {
    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr);
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr);
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr)?;
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
        E: From<RecursionLimitError>,
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr)?;
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum DomainLimit {
    None,
    Inclusive(i64),
    Exclusive(i64),
}

impl RustType<ProtoDomainLimit> for DomainLimit {
    fn into_proto(&self) -> ProtoDomainLimit {
        use proto_domain_limit::Kind::*;
        let kind = match self {
            DomainLimit::None => None(()),
            DomainLimit::Inclusive(v) => Inclusive(*v),
            DomainLimit::Exclusive(v) => Exclusive(*v),
        };
        ProtoDomainLimit { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoDomainLimit) -> Result<Self, TryFromProtoError> {
        use proto_domain_limit::Kind::*;
        if let Some(kind) = proto.kind {
            match kind {
                None(()) => Ok(DomainLimit::None),
                Inclusive(v) => Ok(DomainLimit::Inclusive(v)),
                Exclusive(v) => Ok(DomainLimit::Exclusive(v)),
            }
        } else {
            Err(TryFromProtoError::missing_field("ProtoDomainLimit::kind"))
        }
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum EvalError {
    CharacterNotValidForEncoding(i32),
    CharacterTooLargeForEncoding(i32),
    DateBinOutOfRange(String),
    DivisionByZero,
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    FloatOverflow,
    FloatUnderflow,
    NumericFieldOverflow,
    Float32OutOfRange,
    Float64OutOfRange,
    Int16OutOfRange,
    Int32OutOfRange,
    Int64OutOfRange,
    UInt16OutOfRange,
    UInt32OutOfRange,
    UInt64OutOfRange,
    OidOutOfRange,
    IntervalOutOfRange,
    TimestampOutOfRange,
    CharOutOfRange,
    IndexOutOfRange {
        provided: i32,
        // The last valid index position, i.e. `v.len() - 1`
        valid_end: i32,
    },
    InvalidBase64Equals,
    InvalidBase64Symbol(char),
    InvalidBase64EndSequence,
    InvalidTimezone(String),
    InvalidTimezoneInterval,
    InvalidTimezoneConversion,
    InvalidLayer {
        max_layer: usize,
        val: i64,
    },
    InvalidArray(InvalidArrayError),
    InvalidEncodingName(String),
    InvalidHashAlgorithm(String),
    InvalidByteSequence {
        byte_sequence: String,
        encoding_name: String,
    },
    InvalidJsonbCast {
        from: String,
        to: String,
    },
    InvalidRegex(String),
    InvalidRegexFlag(char),
    InvalidParameterValue(String),
    NegSqrt,
    NullCharacterNotPermitted,
    UnknownUnits(String),
    UnsupportedUnits(String, String),
    UnterminatedLikeEscapeSequence,
    Parse(ParseError),
    ParseHex(ParseHexError),
    Internal(String),
    InfinityOutOfDomain(String),
    NegativeOutOfDomain(String),
    ZeroOutOfDomain(String),
    OutOfDomain(DomainLimit, DomainLimit, String),
    ComplexOutOfRange(String),
    MultipleRowsFromSubquery,
    Undefined(String),
    LikePatternTooLong,
    LikeEscapeTooLong,
    StringValueTooLong {
        target_type: String,
        length: usize,
    },
    MultidimensionalArrayRemovalNotSupported,
    IncompatibleArrayDimensions {
        dims: Option<(usize, usize)>,
    },
    TypeFromOid(String),
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EvalError::CharacterNotValidForEncoding(v) => {
                write!(f, "requested character not valid for encoding: {v}")
            }
            EvalError::CharacterTooLargeForEncoding(v) => {
                write!(f, "requested character too large for encoding: {v}")
            }
            EvalError::DateBinOutOfRange(message) => f.write_str(message),
            EvalError::DivisionByZero => f.write_str("division by zero"),
            EvalError::Unsupported { feature, issue_no } => {
                write!(f, "{} not yet supported", feature)?;
                if let Some(issue_no) = issue_no {
                    write!(f, ", see https://github.com/MaterializeInc/materialize/issues/{} for more details", issue_no)?;
                }
                Ok(())
            }
            EvalError::FloatOverflow => f.write_str("value out of range: overflow"),
            EvalError::FloatUnderflow => f.write_str("value out of range: underflow"),
            EvalError::NumericFieldOverflow => f.write_str("numeric field overflow"),
            EvalError::Float32OutOfRange => f.write_str("real out of range"),
            EvalError::Float64OutOfRange => f.write_str("double precision out of range"),
            EvalError::Int16OutOfRange => f.write_str("smallint out of range"),
            EvalError::Int32OutOfRange => f.write_str("integer out of range"),
            EvalError::Int64OutOfRange => f.write_str("bigint out of range"),
            EvalError::UInt16OutOfRange => f.write_str("uint2 out of range"),
            EvalError::UInt32OutOfRange => f.write_str("uint4 out of range"),
            EvalError::UInt64OutOfRange => f.write_str("uint8 out of range"),
            EvalError::OidOutOfRange => f.write_str("OID out of range"),
            EvalError::IntervalOutOfRange => f.write_str("interval out of range"),
            EvalError::TimestampOutOfRange => f.write_str("timestamp out of range"),
            EvalError::CharOutOfRange => f.write_str("\"char\" out of range"),
            EvalError::IndexOutOfRange {
                provided,
                valid_end,
            } => write!(f, "index {provided} out of valid range, 0..{valid_end}",),
            EvalError::InvalidBase64Equals => {
                f.write_str("unexpected \"=\" while decoding base64 sequence")
            }
            EvalError::InvalidBase64Symbol(c) => write!(
                f,
                "invalid symbol \"{}\" found while decoding base64 sequence",
                c.escape_default()
            ),
            EvalError::InvalidBase64EndSequence => f.write_str("invalid base64 end sequence"),
            EvalError::InvalidJsonbCast { from, to } => {
                write!(f, "cannot cast jsonb {} to type {}", from, to)
            }
            EvalError::InvalidTimezone(tz) => write!(f, "invalid time zone '{}'", tz),
            EvalError::InvalidTimezoneInterval => {
                f.write_str("timezone interval must not contain months or years")
            }
            EvalError::InvalidTimezoneConversion => f.write_str("invalid timezone conversion"),
            EvalError::InvalidLayer { max_layer, val } => write!(
                f,
                "invalid layer: {}; must use value within [1, {}]",
                val, max_layer
            ),
            EvalError::InvalidArray(e) => e.fmt(f),
            EvalError::InvalidEncodingName(name) => write!(f, "invalid encoding name '{}'", name),
            EvalError::InvalidHashAlgorithm(alg) => write!(f, "invalid hash algorithm '{}'", alg),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => write!(
                f,
                "invalid byte sequence '{}' for encoding '{}'",
                byte_sequence, encoding_name
            ),
            EvalError::NegSqrt => f.write_str("cannot take square root of a negative number"),
            EvalError::NullCharacterNotPermitted => f.write_str("null character not permitted"),
            EvalError::InvalidRegex(e) => write!(f, "invalid regular expression: {}", e),
            EvalError::InvalidRegexFlag(c) => write!(f, "invalid regular expression flag: {}", c),
            EvalError::InvalidParameterValue(s) => f.write_str(s),
            EvalError::UnknownUnits(units) => write!(f, "unit '{}' not recognized", units),
            EvalError::UnsupportedUnits(units, typ) => {
                write!(f, "unit '{}' not supported for type {}", units, typ)
            }
            EvalError::UnterminatedLikeEscapeSequence => {
                f.write_str("unterminated escape sequence in LIKE")
            }
            EvalError::Parse(e) => e.fmt(f),
            EvalError::ParseHex(e) => e.fmt(f),
            EvalError::Internal(s) => write!(f, "internal error: {}", s),
            EvalError::InfinityOutOfDomain(s) => {
                write!(f, "function {} is only defined for finite arguments", s)
            }
            EvalError::NegativeOutOfDomain(s) => {
                write!(f, "function {} is not defined for negative numbers", s)
            }
            EvalError::ZeroOutOfDomain(s) => {
                write!(f, "function {} is not defined for zero", s)
            }
            EvalError::OutOfDomain(lower, upper, s) => {
                use DomainLimit::*;
                write!(f, "function {s} is defined for numbers ")?;
                match (lower, upper) {
                    (Inclusive(n), None) => write!(f, "greater than or equal to {n}"),
                    (Exclusive(n), None) => write!(f, "greater than {n}"),
                    (None, Inclusive(n)) => write!(f, "less than or equal to {n}"),
                    (None, Exclusive(n)) => write!(f, "less than {n}"),
                    (Inclusive(lo), Inclusive(hi)) => write!(f, "between {lo} and {hi} inclusive"),
                    (Exclusive(lo), Exclusive(hi)) => write!(f, "between {lo} and {hi} exclusive"),
                    (Inclusive(lo), Exclusive(hi)) => {
                        write!(f, "between {lo} inclusive and {hi} exclusive")
                    }
                    (Exclusive(lo), Inclusive(hi)) => {
                        write!(f, "between {lo} exclusive and {hi} inclusive")
                    }
                    (None, None) => panic!("invalid domain error"),
                }
            }
            EvalError::ComplexOutOfRange(s) => {
                write!(f, "function {} cannot return complex numbers", s)
            }
            EvalError::MultipleRowsFromSubquery => {
                write!(f, "more than one record produced in subquery")
            }
            EvalError::Undefined(s) => {
                write!(f, "{} is undefined", s)
            }
            EvalError::LikePatternTooLong => {
                write!(f, "LIKE pattern exceeds maximum length")
            }
            EvalError::LikeEscapeTooLong => {
                write!(f, "invalid escape string")
            }
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => {
                write!(f, "value too long for type {}({})", target_type, length)
            }
            EvalError::MultidimensionalArrayRemovalNotSupported => {
                write!(
                    f,
                    "removing elements from multidimensional arrays is not supported"
                )
            }
            EvalError::IncompatibleArrayDimensions { dims: _ } => {
                write!(f, "cannot concatenate incompatible arrays")
            }
            EvalError::TypeFromOid(msg) => write!(f, "{msg}"),
        }
    }
}

impl EvalError {
    pub fn detail(&self) -> Option<String> {
        match self {
            EvalError::IncompatibleArrayDimensions { dims: None } => Some(
                "Arrays with differing dimensions are not compatible for concatenation."
                    .to_string(),
            ),
            EvalError::IncompatibleArrayDimensions {
                dims: Some((a_dims, b_dims)),
            } => Some(format!(
                "Arrays of {} and {} dimensions are not compatible for concatenation.",
                a_dims, b_dims
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            EvalError::InvalidBase64EndSequence => Some(
                "Input data is missing padding, is truncated, or is otherwise corrupted.".into(),
            ),
            EvalError::LikeEscapeTooLong => {
                Some("Escape string must be empty or one character.".into())
            }
            _ => None,
        }
    }
}

impl std::error::Error for EvalError {}

impl From<ParseError> for EvalError {
    fn from(e: ParseError) -> EvalError {
        EvalError::Parse(e)
    }
}

impl From<ParseHexError> for EvalError {
    fn from(e: ParseHexError) -> EvalError {
        EvalError::ParseHex(e)
    }
}

impl From<InvalidArrayError> for EvalError {
    fn from(e: InvalidArrayError) -> EvalError {
        EvalError::InvalidArray(e)
    }
}

impl From<regex::Error> for EvalError {
    fn from(e: regex::Error) -> EvalError {
        EvalError::InvalidRegex(e.to_string())
    }
}

impl From<TypeFromOidError> for EvalError {
    fn from(e: TypeFromOidError) -> EvalError {
        EvalError::TypeFromOid(e.to_string())
    }
}

impl RustType<ProtoEvalError> for EvalError {
    fn into_proto(&self) -> ProtoEvalError {
        use proto_eval_error::Kind::*;
        use proto_eval_error::*;
        let kind = match self {
            EvalError::CharacterNotValidForEncoding(v) => CharacterNotValidForEncoding(*v),
            EvalError::CharacterTooLargeForEncoding(v) => CharacterTooLargeForEncoding(*v),
            EvalError::DateBinOutOfRange(v) => DateBinOutOfRange(v.clone()),
            EvalError::DivisionByZero => DivisionByZero(()),
            EvalError::Unsupported { feature, issue_no } => Unsupported(ProtoUnsupported {
                feature: feature.clone(),
                issue_no: issue_no.into_proto(),
            }),
            EvalError::FloatOverflow => FloatOverflow(()),
            EvalError::FloatUnderflow => FloatUnderflow(()),
            EvalError::NumericFieldOverflow => NumericFieldOverflow(()),
            EvalError::Float32OutOfRange => Float32OutOfRange(()),
            EvalError::Float64OutOfRange => Float64OutOfRange(()),
            EvalError::Int16OutOfRange => Int16OutOfRange(()),
            EvalError::Int32OutOfRange => Int32OutOfRange(()),
            EvalError::Int64OutOfRange => Int64OutOfRange(()),
            EvalError::UInt16OutOfRange => Uint16OutOfRange(()),
            EvalError::UInt32OutOfRange => Uint32OutOfRange(()),
            EvalError::UInt64OutOfRange => Uint64OutOfRange(()),
            EvalError::OidOutOfRange => OidOutOfRange(()),
            EvalError::IntervalOutOfRange => IntervalOutOfRange(()),
            EvalError::TimestampOutOfRange => TimestampOutOfRange(()),
            EvalError::CharOutOfRange => CharOutOfRange(()),
            EvalError::IndexOutOfRange {
                provided,
                valid_end,
            } => IndexOutOfRange(ProtoIndexOutOfRange {
                provided: *provided,
                valid_end: *valid_end,
            }),
            EvalError::InvalidBase64Equals => InvalidBase64Equals(()),
            EvalError::InvalidBase64Symbol(sym) => InvalidBase64Symbol(sym.into_proto()),
            EvalError::InvalidBase64EndSequence => InvalidBase64EndSequence(()),
            EvalError::InvalidTimezone(tz) => InvalidTimezone(tz.clone()),
            EvalError::InvalidTimezoneInterval => InvalidTimezoneInterval(()),
            EvalError::InvalidTimezoneConversion => InvalidTimezoneConversion(()),
            EvalError::InvalidLayer { max_layer, val } => InvalidLayer(ProtoInvalidLayer {
                max_layer: max_layer.into_proto(),
                val: *val,
            }),
            EvalError::InvalidArray(error) => InvalidArray(error.into_proto()),
            EvalError::InvalidEncodingName(v) => InvalidEncodingName(v.clone()),
            EvalError::InvalidHashAlgorithm(v) => InvalidHashAlgorithm(v.clone()),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => InvalidByteSequence(ProtoInvalidByteSequence {
                byte_sequence: byte_sequence.clone(),
                encoding_name: encoding_name.clone(),
            }),
            EvalError::InvalidJsonbCast { from, to } => InvalidJsonbCast(ProtoInvalidJsonbCast {
                from: from.clone(),
                to: to.clone(),
            }),
            EvalError::InvalidRegex(v) => InvalidRegex(v.clone()),
            EvalError::InvalidRegexFlag(v) => InvalidRegexFlag(v.into_proto()),
            EvalError::InvalidParameterValue(v) => InvalidParameterValue(v.clone()),
            EvalError::NegSqrt => NegSqrt(()),
            EvalError::NullCharacterNotPermitted => NullCharacterNotPermitted(()),
            EvalError::UnknownUnits(v) => UnknownUnits(v.clone()),
            EvalError::UnsupportedUnits(units, typ) => UnsupportedUnits(ProtoUnsupportedUnits {
                units: units.clone(),
                typ: typ.clone(),
            }),
            EvalError::UnterminatedLikeEscapeSequence => UnterminatedLikeEscapeSequence(()),
            EvalError::Parse(error) => Parse(error.into_proto()),
            EvalError::ParseHex(error) => ParseHex(error.into_proto()),
            EvalError::Internal(v) => Internal(v.clone()),
            EvalError::InfinityOutOfDomain(v) => InfinityOutOfDomain(v.clone()),
            EvalError::NegativeOutOfDomain(v) => NegativeOutOfDomain(v.clone()),
            EvalError::ZeroOutOfDomain(v) => ZeroOutOfDomain(v.clone()),
            EvalError::OutOfDomain(lower, upper, id) => OutOfDomain(ProtoOutOfDomain {
                lower: Some(lower.into_proto()),
                upper: Some(upper.into_proto()),
                id: id.clone(),
            }),
            EvalError::ComplexOutOfRange(v) => ComplexOutOfRange(v.clone()),
            EvalError::MultipleRowsFromSubquery => MultipleRowsFromSubquery(()),
            EvalError::Undefined(v) => Undefined(v.clone()),
            EvalError::LikePatternTooLong => LikePatternTooLong(()),
            EvalError::LikeEscapeTooLong => LikeEscapeTooLong(()),
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => StringValueTooLong(ProtoStringValueTooLong {
                target_type: target_type.clone(),
                length: length.into_proto(),
            }),
            EvalError::MultidimensionalArrayRemovalNotSupported => {
                MultidimensionalArrayRemovalNotSupported(())
            }
            EvalError::IncompatibleArrayDimensions { dims } => {
                IncompatibleArrayDimensions(ProtoIncompatibleArrayDimensions {
                    dims: dims.into_proto(),
                })
            }
            EvalError::TypeFromOid(v) => TypeFromOid(v.clone()),
        };
        ProtoEvalError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoEvalError) -> Result<Self, TryFromProtoError> {
        use proto_eval_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                CharacterNotValidForEncoding(v) => Ok(EvalError::CharacterNotValidForEncoding(v)),
                CharacterTooLargeForEncoding(v) => Ok(EvalError::CharacterTooLargeForEncoding(v)),
                DateBinOutOfRange(v) => Ok(EvalError::DateBinOutOfRange(v)),
                DivisionByZero(()) => Ok(EvalError::DivisionByZero),
                Unsupported(v) => Ok(EvalError::Unsupported {
                    feature: v.feature,
                    issue_no: v.issue_no.into_rust()?,
                }),
                FloatOverflow(()) => Ok(EvalError::FloatOverflow),
                FloatUnderflow(()) => Ok(EvalError::FloatUnderflow),
                NumericFieldOverflow(()) => Ok(EvalError::NumericFieldOverflow),
                Float32OutOfRange(()) => Ok(EvalError::Float32OutOfRange),
                Float64OutOfRange(()) => Ok(EvalError::Float64OutOfRange),
                Int16OutOfRange(()) => Ok(EvalError::Int16OutOfRange),
                Int32OutOfRange(()) => Ok(EvalError::Int32OutOfRange),
                Int64OutOfRange(()) => Ok(EvalError::Int64OutOfRange),
                Uint16OutOfRange(()) => Ok(EvalError::UInt16OutOfRange),
                Uint32OutOfRange(()) => Ok(EvalError::UInt32OutOfRange),
                Uint64OutOfRange(()) => Ok(EvalError::UInt64OutOfRange),
                OidOutOfRange(()) => Ok(EvalError::OidOutOfRange),
                IntervalOutOfRange(()) => Ok(EvalError::IntervalOutOfRange),
                TimestampOutOfRange(()) => Ok(EvalError::TimestampOutOfRange),
                CharOutOfRange(()) => Ok(EvalError::CharOutOfRange),
                IndexOutOfRange(v) => Ok(EvalError::IndexOutOfRange {
                    provided: v.provided,
                    valid_end: v.valid_end,
                }),
                InvalidBase64Equals(()) => Ok(EvalError::InvalidBase64Equals),
                InvalidBase64Symbol(v) => char::from_proto(v).map(EvalError::InvalidBase64Symbol),
                InvalidBase64EndSequence(()) => Ok(EvalError::InvalidBase64EndSequence),
                InvalidTimezone(v) => Ok(EvalError::InvalidTimezone(v)),
                InvalidTimezoneInterval(()) => Ok(EvalError::InvalidTimezoneInterval),
                InvalidTimezoneConversion(()) => Ok(EvalError::InvalidTimezoneConversion),
                InvalidLayer(v) => Ok(EvalError::InvalidLayer {
                    max_layer: usize::from_proto(v.max_layer)?,
                    val: v.val,
                }),
                InvalidArray(error) => Ok(EvalError::InvalidArray(error.into_rust()?)),
                InvalidEncodingName(v) => Ok(EvalError::InvalidEncodingName(v)),
                InvalidHashAlgorithm(v) => Ok(EvalError::InvalidHashAlgorithm(v)),
                InvalidByteSequence(v) => Ok(EvalError::InvalidByteSequence {
                    byte_sequence: v.byte_sequence,
                    encoding_name: v.encoding_name,
                }),
                InvalidJsonbCast(v) => Ok(EvalError::InvalidJsonbCast {
                    from: v.from,
                    to: v.to,
                }),
                InvalidRegex(v) => Ok(EvalError::InvalidRegex(v)),
                InvalidRegexFlag(v) => Ok(EvalError::InvalidRegexFlag(char::from_proto(v)?)),
                InvalidParameterValue(v) => Ok(EvalError::InvalidParameterValue(v)),
                NegSqrt(()) => Ok(EvalError::NegSqrt),
                NullCharacterNotPermitted(()) => Ok(EvalError::NullCharacterNotPermitted),
                UnknownUnits(v) => Ok(EvalError::UnknownUnits(v)),
                UnsupportedUnits(v) => Ok(EvalError::UnsupportedUnits(v.units, v.typ)),
                UnterminatedLikeEscapeSequence(()) => Ok(EvalError::UnterminatedLikeEscapeSequence),
                Parse(error) => Ok(EvalError::Parse(error.into_rust()?)),
                ParseHex(error) => Ok(EvalError::ParseHex(error.into_rust()?)),
                Internal(v) => Ok(EvalError::Internal(v)),
                InfinityOutOfDomain(v) => Ok(EvalError::InfinityOutOfDomain(v)),
                NegativeOutOfDomain(v) => Ok(EvalError::NegativeOutOfDomain(v)),
                ZeroOutOfDomain(v) => Ok(EvalError::ZeroOutOfDomain(v)),
                OutOfDomain(v) => Ok(EvalError::OutOfDomain(
                    v.lower.into_rust_if_some("ProtoDomainLimit::lower")?,
                    v.upper.into_rust_if_some("ProtoDomainLimit::upper")?,
                    v.id,
                )),
                ComplexOutOfRange(v) => Ok(EvalError::ComplexOutOfRange(v)),
                MultipleRowsFromSubquery(()) => Ok(EvalError::MultipleRowsFromSubquery),
                Undefined(v) => Ok(EvalError::Undefined(v)),
                LikePatternTooLong(()) => Ok(EvalError::LikePatternTooLong),
                LikeEscapeTooLong(()) => Ok(EvalError::LikeEscapeTooLong),
                StringValueTooLong(v) => Ok(EvalError::StringValueTooLong {
                    target_type: v.target_type,
                    length: usize::from_proto(v.length)?,
                }),
                MultidimensionalArrayRemovalNotSupported(()) => {
                    Ok(EvalError::MultidimensionalArrayRemovalNotSupported)
                }
                IncompatibleArrayDimensions(v) => Ok(EvalError::IncompatibleArrayDimensions {
                    dims: v.dims.into_rust()?,
                }),
                TypeFromOid(v) => Ok(EvalError::TypeFromOid(v)),
            },
            None => Err(TryFromProtoError::missing_field("ProtoEvalError::kind")),
        }
    }
}

impl RustType<ProtoDims> for (usize, usize) {
    fn into_proto(&self) -> ProtoDims {
        ProtoDims {
            f0: self.0.into_proto(),
            f1: self.1.into_proto(),
        }
    }

    fn from_proto(proto: ProtoDims) -> Result<Self, TryFromProtoError> {
        Ok((proto.f0.into_rust()?, proto.f1.into_rust()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_proto::protobuf_roundtrip;

    #[test]
    fn test_reduce() {
        let relation_type = vec![
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(false),
        ];
        let col = MirScalarExpr::Column;
        let err = |e| MirScalarExpr::literal(Err(e), ScalarType::Int64);
        let lit = |i| MirScalarExpr::literal_ok(Datum::Int64(i), ScalarType::Int64);
        let null = || MirScalarExpr::literal_null(ScalarType::Int64);

        struct TestCase {
            input: MirScalarExpr,
            output: MirScalarExpr,
        }

        let test_cases = vec![
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), lit(2)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), lit(2), null()],
                },
                output: lit(2),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), col(0), null(), col(1), lit(2), lit(3)],
                },
                output: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(1), lit(2)],
                },
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2), col(1)],
                },
                output: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2)],
                },
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), err(EvalError::DivisionByZero)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), err(EvalError::DivisionByZero)],
                },
                output: err(EvalError::DivisionByZero),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![
                        null(),
                        err(EvalError::DivisionByZero),
                        err(EvalError::NumericFieldOverflow),
                    ],
                },
                output: err(EvalError::DivisionByZero),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), err(EvalError::DivisionByZero)],
                },
                output: err(EvalError::DivisionByZero),
            },
        ];

        for tc in test_cases {
            let mut actual = tc.input.clone();
            actual.reduce(&relation_type);
            assert!(
                actual == tc.output,
                "input: {}\nactual: {}\nexpected: {}",
                tc.input,
                actual,
                tc.output
            );
        }
    }

    proptest! {
        #[test]
        fn mir_scalar_expr_protobuf_roundtrip(expect in any::<MirScalarExpr>()) {
            let actual = protobuf_roundtrip::<_, ProtoMirScalarExpr>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn domain_limit_protobuf_roundtrip(expect in any::<DomainLimit>()) {
            let actual = protobuf_roundtrip::<_, ProtoDomainLimit>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn eval_error_protobuf_roundtrip(expect in any::<EvalError>()) {
            let actual = protobuf_roundtrip::<_, ProtoEvalError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
