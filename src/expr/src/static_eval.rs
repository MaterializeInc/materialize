// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, Row, RowArena};

use crate::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc};

type StaticMSEFn = for<'a> fn(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError>;

#[derive(Debug)]
pub struct StaticMirScalarExpr {
    func: StaticMSEFn,
    params: Option<Box<StaticMirScalarExprParams>>,
}

impl StaticMirScalarExpr {
    #[inline(always)]
    pub(crate) fn params(&self) -> &StaticMirScalarExprParams {
        self.params.as_ref().expect("params should exist")
    }

    #[inline(always)]
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
    Ok(datums[this.params().unwrap_column()])
}

fn static_column_0<'a>(
    _this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(datums[0])
}

fn static_column_1<'a>(
    _this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(datums[1])
}

fn static_column_2<'a>(
    _this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(datums[2])
}

fn static_column_3<'a>(
    _this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(datums[3])
}

fn static_literal<'a>(
    this: &'a StaticMirScalarExpr,
    _datums: &[Datum<'a>],
    _temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let res = this.params().unwrap_literal();
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
    let x = this.params().unwrap_call_unmaterializable();
    Err(EvalError::Internal(
        format!("cannot evaluate unmaterializable function: {:?}", x).into(),
    ))
}

fn static_call_binary<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (func, expr1, expr2) = this.params().unwrap_call_binary();
    let a = expr1.eval(datums, temp_storage)?;
    let b = expr2.eval(datums, temp_storage)?;
    func.eval_input(temp_storage, a, b)
}

fn static_call_variadic<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (func, exprs) = this.params().unwrap_call_variadic();
    func.eval_static(datums, temp_storage, exprs)
}

fn static_if<'a>(
    this: &'a StaticMirScalarExpr,
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let (cond, then, els) = this.params().unwrap_if();
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
        expr: StaticMirScalarExpr,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: StaticMirScalarExpr,
        expr2: StaticMirScalarExpr,
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
        cond: StaticMirScalarExpr,
        then: StaticMirScalarExpr,
        els: StaticMirScalarExpr,
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

impl From<&MirScalarExpr> for StaticMirScalarExpr {
    fn from(value: &MirScalarExpr) -> Self {
        match value {
            MirScalarExpr::Column(0, _name) => StaticMirScalarExpr {
                func: static_column_0,
                params: None,
            },
            MirScalarExpr::Column(1, _name) => StaticMirScalarExpr {
                func: static_column_1,
                params: None,
            },
            MirScalarExpr::Column(2, _name) => StaticMirScalarExpr {
                func: static_column_2,
                params: None,
            },
            MirScalarExpr::Column(3, _name) => StaticMirScalarExpr {
                func: static_column_3,
                params: None,
            },
            MirScalarExpr::Column(col, _name) => StaticMirScalarExpr {
                func: static_column,
                params: Some(Box::new(StaticMirScalarExprParams::Column(*col))),
            },
            MirScalarExpr::Literal(res, _) => StaticMirScalarExpr {
                func: static_literal,
                params: Some(Box::new(StaticMirScalarExprParams::Literal(res.clone()))),
            },
            MirScalarExpr::CallUnmaterializable(f) => StaticMirScalarExpr {
                func: static_call_unmaterializable,
                params: Some(Box::new(StaticMirScalarExprParams::CallUnmaterializable(
                    f.clone(),
                ))),
            },
            MirScalarExpr::CallUnary { func, expr } => StaticMirScalarExpr {
                func: func.static_fn(),
                params: Box::new(StaticMirScalarExprParams::CallUnary {
                    func: func.clone(),
                    expr: expr.as_ref().into(),
                })
                .into(),
            },
            MirScalarExpr::CallBinary { func, expr1, expr2 } => StaticMirScalarExpr {
                func: static_call_binary,
                params: Box::new(StaticMirScalarExprParams::CallBinary {
                    func: func.clone(),
                    expr1: expr1.as_ref().into(),
                    expr2: expr2.as_ref().into(),
                })
                .into(),
            },
            MirScalarExpr::CallVariadic { func, exprs } => StaticMirScalarExpr {
                func: static_call_variadic,
                params: Box::new(StaticMirScalarExprParams::CallVariadic {
                    func: func.clone(),
                    exprs: exprs
                        .into_iter()
                        .map(|e| e.into())
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                })
                .into(),
            },
            MirScalarExpr::If { cond, then, els } => StaticMirScalarExpr {
                func: static_if,
                params: Box::new(StaticMirScalarExprParams::If {
                    cond: cond.as_ref().into(),
                    then: then.as_ref().into(),
                    els: els.as_ref().into(),
                })
                .into(),
            },
        }
    }
}
