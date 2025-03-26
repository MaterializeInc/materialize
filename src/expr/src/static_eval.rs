// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use mz_repr::{Datum, Row, RowArena};

type StaticMSEFn = for<'a> fn(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError>;

#[derive(Debug)]
pub struct StaticMirScalarExpr {
    func: StaticMSEFn,
    params: StaticMirScalarExprParams,
}

impl StaticMirScalarExpr {
    pub(crate) fn params(&self) -> &StaticMirScalarExprParams {
        &self.params
    }

    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        (self.func)(self, datums, temp_storage)
    }
}

fn static_column<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(datums[this.params.unwrap_column()])
}

fn static_literal<'a>(
    this: &'a StaticMirScalarExpr,
    _datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let res = this.params.unwrap_literal();
    match res {
        Ok(row) => Ok(row.unpack_first()),
        Err(e) => Err(e.clone()),
    }
}

fn static_call_unmaterializable<'a>(
    this: &'a StaticMirScalarExpr,
    _datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let x = this.params.unwrap_call_unmaterializable();
    Err(EvalError::Internal(
        format!("cannot evaluate unmaterializable function: {:?}", x).into(),
    ))
}

fn static_call_binary<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (func, expr1, expr2) = this.params.unwrap_call_binary();
    let a = expr1.eval(datums, temp_storage)?;
    let b = expr2.eval(datums, temp_storage)?;
    func.eval_input(temp_storage, a, b)
}

fn static_call_variadic<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (func, exprs) = this.params.unwrap_call_variadic();
    func.eval_static(datums, temp_storage, exprs)
}

fn static_if<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (cond, then, els) = this.params.unwrap_if();
    match cond.eval(datums, temp_storage)? {
        Datum::True => then.eval(datums, temp_storage),
        Datum::False | Datum::Null => els.eval(datums, temp_storage),
        d => Err(EvalError::Internal(
            format!("if condition evaluated to non-boolean datum: {:?}", d).into(),
        )),
    }
}

#[derive(Debug)]
pub(crate) enum StaticMirScalarExprParams {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Result<Row, EvalError>),
    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `MirScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    CallUnmaterializable(UnmaterializableFunc),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<StaticMirScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<StaticMirScalarExpr>,
        expr2: Box<StaticMirScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Box<[StaticMirScalarExpr]>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<StaticMirScalarExpr>,
        then: Box<StaticMirScalarExpr>,
        els: Box<StaticMirScalarExpr>,
    },
}

impl StaticMirScalarExprParams {
    fn unwrap_column(&self) -> usize {
        match self {
            Self::Column(i) => *i,
            _ => panic!("expected column, got {:?}", self),
        }
    }

    fn unwrap_literal(&self) -> &Result<Row, EvalError> {
        match self {
            Self::Literal(res) => res,
            _ => panic!("expected literal, got {:?}", self),
        }
    }

    fn unwrap_call_unmaterializable(&self) -> &UnmaterializableFunc {
        match self {
            Self::CallUnmaterializable(f) => f,
            _ => panic!("expected call unmaterializable, got {:?}", self),
        }
    }

    pub(crate) fn unwrap_call_unary(&self) -> (&UnaryFunc, &StaticMirScalarExpr) {
        match self {
            Self::CallUnary { func, expr } => (func, expr),
            _ => panic!("expected call unary, got {:?}", self),
        }
    }

    fn unwrap_call_binary(&self) -> (&BinaryFunc, &StaticMirScalarExpr, &StaticMirScalarExpr) {
        match self {
            Self::CallBinary { func, expr1, expr2 } => (func, expr1, expr2),
            _ => panic!("expected call binary, got {:?}", self),
        }
    }

    fn unwrap_call_variadic(&self) -> (&VariadicFunc, &[StaticMirScalarExpr]) {
        match self {
            Self::CallVariadic { func, exprs } => (func, exprs),
            _ => panic!("expected call variadic, got {:?}", self),
        }
    }

    fn unwrap_if(
        &self,
    ) -> (
        &StaticMirScalarExpr,
        &StaticMirScalarExpr,
        &StaticMirScalarExpr,
    ) {
        match self {
            Self::If { cond, then, els } => (cond, then, els),
            _ => panic!("expected if, got {:?}", self),
        }
    }
}

impl From<MirScalarExpr> for StaticMirScalarExpr {
    fn from(value: MirScalarExpr) -> Self {
        match value {
            MirScalarExpr::Column(col, _name) => StaticMirScalarExpr {
                func: static_column,
                params: StaticMirScalarExprParams::Column(col),
            },
            MirScalarExpr::Literal(res, _) => StaticMirScalarExpr {
                func: static_literal,
                params: StaticMirScalarExprParams::Literal(res),
            },
            MirScalarExpr::CallUnmaterializable(f) => StaticMirScalarExpr {
                func: static_call_unmaterializable,
                params: StaticMirScalarExprParams::CallUnmaterializable(f),
            },
            MirScalarExpr::CallUnary { func, expr } => StaticMirScalarExpr {
                func: func.static_fn(),
                params: StaticMirScalarExprParams::CallUnary {
                    func,
                    expr: Box::new((*expr).into()),
                },
            },
            MirScalarExpr::CallBinary { func, expr1, expr2 } => StaticMirScalarExpr {
                func: static_call_binary,
                params: StaticMirScalarExprParams::CallBinary {
                    func,
                    expr1: Box::new((*expr1).into()),
                    expr2: Box::new((*expr2).into()),
                },
            },
            MirScalarExpr::CallVariadic { func, exprs } => StaticMirScalarExpr {
                func: static_call_variadic,
                params: StaticMirScalarExprParams::CallVariadic {
                    func,
                    exprs: exprs
                        .into_iter()
                        .map(|e| e.into())
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                },
            },
            MirScalarExpr::If { cond, then, els } => StaticMirScalarExpr {
                func: static_if,
                params: StaticMirScalarExprParams::If {
                    cond: Box::new((*cond).into()),
                    then: Box::new((*then).into()),
                    els: Box::new((*els).into()),
                },
            },
        }
    }
}
