// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scalar expressions in a stable format.
//! These are closely related to [mz_expr::MirScalarExpr], but:
//!   (1) these are a stable type we write down, and
//!   (2) these do not have unmaterializable functions in them.

use std::fmt::Display as _;
use std::sync::Arc;

use itertools::Itertools;
use mz_expr::explain::{HumanizedExplain, HumanizedExpr, HumanizerMode};
use mz_expr::{
    BinaryFunc, Columns, Eval, EvalError, MapFilterProject, MfpPlan, MirScalarExpr,
    OptimizableExpr, SafeMfpPlan, UnaryFunc, UnmaterializableFunc, VariadicFunc,
};
use mz_ore::str::separated;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::explain::ScalarOps;
use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, RowArena};
use serde::{Deserialize, Serialize};

/// Scalar expressions, as appear in MFPs.
/// This is the stable, low-level, LIR definition of scalr expressions.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LirScalarExpr {
    /// A column of the input row
    Column(usize, TreatAsEqual<Option<Arc<str>>>),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Result<Row, EvalError>, ReprColumnType),
    /// A function call that takes one expression as an argument.
    CallUnary {
        /// Function
        func: UnaryFunc,
        /// Argument
        expr: Box<LirScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        /// Function
        func: BinaryFunc,
        /// First argument
        expr1: Box<LirScalarExpr>,
        /// Second argument
        expr2: Box<LirScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        /// Function
        func: VariadicFunc,
        /// Arguments
        exprs: Vec<LirScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        /// Condition
        cond: Box<LirScalarExpr>,
        /// Then branch
        then: Box<LirScalarExpr>,
        /// Else branch
        els: Box<LirScalarExpr>,
    },
}

impl LirScalarExpr {
    /// Generates an LSE representing the given column reference.
    pub fn column(c: usize) -> Self {
        LirScalarExpr::Column(c, TreatAsEqual(None))
    }

    /// Packs a `Datum` or `EvalError` into a literal row of the given type.
    pub fn literal(res: Result<Datum, EvalError>, typ: ReprScalarType) -> Self {
        let typ = ReprColumnType {
            scalar_type: typ,
            nullable: matches!(res, Ok(Datum::Null)),
        };
        let row = res.map(|datum| Row::pack_slice(&[datum]));
        LirScalarExpr::Literal(row, typ)
    }

    /// Generates a literal of the given type.
    pub fn literal_ok(datum: Datum, typ: ReprScalarType) -> Self {
        LirScalarExpr::literal(Ok(datum), typ)
    }

    /// If the expression is a literal, this returns the literal's Datum or the literal's EvalError.
    /// Otherwise, it returns None.
    pub fn as_literal(&self) -> Option<Result<Datum<'_>, &EvalError>> {
        if let LirScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.as_ref().map(|row| row.unpack_first()))
        } else {
            None
        }
    }

    /// Returns true if the expression is a literal true.
    pub fn is_literal_true(&self) -> bool {
        Some(Ok(Datum::True)) == self.as_literal()
    }

    /// If the expression is an int64, returns the literal.
    pub fn as_literal_int64(&self) -> Option<i64> {
        match self.as_literal() {
            Some(Ok(Datum::Int64(i))) => Some(i),
            _ => None,
        }
    }

    /// Calls a unary function, with `self` as the argument.
    pub fn call_unary<U: Into<UnaryFunc>>(self, func: U) -> Self {
        LirScalarExpr::CallUnary {
            func: func.into(),
            expr: Box::new(self),
        }
    }

    /// Calls a binary function, with `self` as the first argument `other` as the second.
    pub fn call_binary<B: Into<BinaryFunc>>(self, other: Self, func: B) -> Self {
        LirScalarExpr::CallBinary {
            func: func.into(),
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    /// Visits all subexpressions in DFS preorder.
    pub fn visit_pre<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        let mut worklist = vec![self];
        while let Some(e) = worklist.pop() {
            f(e);
            worklist.extend(e.children().rev());
        }
    }

    /// Iterative pre-order visitor.
    pub fn visit_pre_mut<F: FnMut(&mut Self)>(&mut self, mut f: F) {
        let mut worklist = vec![self];
        while let Some(expr) = worklist.pop() {
            f(expr);
            worklist.extend(expr.children_mut().rev());
        }
    }

    /// Iterates through references to child expressions.
    pub fn children(&self) -> impl DoubleEndedIterator<Item = &Self> {
        let mut first = None;
        let mut second = None;
        let mut third = None;
        let mut variadic = None;

        use LirScalarExpr::*;
        match self {
            Column(_, _) | Literal(_, _) => (),
            CallUnary { expr, .. } => {
                first = Some(&**expr);
            }
            CallBinary { expr1, expr2, .. } => {
                first = Some(&**expr1);
                second = Some(&**expr2);
            }
            CallVariadic { exprs, .. } => {
                variadic = Some(exprs);
            }
            If { cond, then, els } => {
                first = Some(&**cond);
                second = Some(&**then);
                third = Some(&**els);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(third)
            .chain(variadic.into_iter().flatten())
    }

    /// Iterates through mutable references to child expressions.
    pub fn children_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut Self> {
        let mut first = None;
        let mut second = None;
        let mut third = None;
        let mut variadic = None;

        use LirScalarExpr::*;
        match self {
            Column(_, _) | Literal(_, _) => (),
            CallUnary { expr, .. } => {
                first = Some(&mut **expr);
            }
            CallBinary { expr1, expr2, .. } => {
                first = Some(&mut **expr1);
                second = Some(&mut **expr2);
            }
            CallVariadic { exprs, .. } => {
                variadic = Some(exprs);
            }
            If { cond, then, els } => {
                first = Some(&mut **cond);
                second = Some(&mut **then);
                third = Some(&mut **els);
            }
        }

        first
            .into_iter()
            .chain(second)
            .chain(third)
            .chain(variadic.into_iter().flatten())
    }
}

impl mz_expr::visit::VisitChildren<LirScalarExpr> for LirScalarExpr {
    fn visit_children<F>(&self, f: F)
    where
        F: FnMut(&Self),
    {
        self.children().for_each(f);
    }

    fn visit_mut_children<F>(&mut self, f: F)
    where
        F: FnMut(&mut Self),
    {
        self.children_mut().for_each(f);
    }

    fn try_visit_children<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Self) -> Result<(), E>,
    {
        for child in self.children() {
            f(child)?;
        }
        Ok(())
    }

    fn try_visit_mut_children<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&mut Self) -> Result<(), E>,
    {
        use LirScalarExpr::*;
        match self {
            Column(_, _) | Literal(_, _) => (),
            CallUnary { expr, .. } => f(expr)?,
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

    fn children<'a>(&'a self) -> impl DoubleEndedIterator<Item = &'a LirScalarExpr>
    where
        LirScalarExpr: 'a,
    {
        LirScalarExpr::children(self)
    }

    fn children_mut<'a>(&'a mut self) -> impl DoubleEndedIterator<Item = &'a mut LirScalarExpr>
    where
        LirScalarExpr: 'a,
    {
        LirScalarExpr::children_mut(self)
    }
}

impl Columns for LirScalarExpr {
    fn column(c: usize) -> Self {
        LirScalarExpr::Column(c, TreatAsEqual(None))
    }

    /// Visits each column reference and applies `action` to the column.
    ///
    /// Useful for remapping columns, or for collecting expression support.
    fn visit_columns<F>(&mut self, mut action: F)
    where
        F: FnMut(&mut usize),
    {
        self.visit_pre_mut(|e| {
            if let LirScalarExpr::Column(col, _) = e {
                action(col);
            }
        });
    }

    fn is_column(&self) -> bool {
        matches!(self, LirScalarExpr::Column(_, _))
    }

    fn as_column(&self) -> Option<usize> {
        if let LirScalarExpr::Column(i, _) = self {
            Some(*i)
        } else {
            None
        }
    }

    fn as_column_mut(&mut self) -> Option<&mut usize> {
        if let LirScalarExpr::Column(i, _) = self {
            Some(i)
        } else {
            None
        }
    }

    fn support_into(&self, support: &mut std::collections::BTreeSet<usize>) {
        self.visit_pre(|e| {
            if let LirScalarExpr::Column(i, _) = e {
                support.insert(*i);
            }
        });
    }
}

impl Eval for LirScalarExpr {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        use LirScalarExpr::*;
        match self {
            Column(index, _name) => Ok(datums[*index]),
            Literal(res, _column_type) => match res {
                Ok(row) => Ok(row.unpack_first()),
                Err(e) => Err(e.clone()),
            },
            CallUnary { func, expr } => func.eval(datums, temp_storage, expr.as_ref()),
            CallBinary { func, expr1, expr2 } => {
                func.eval(datums, temp_storage, &[expr1.as_ref(), expr2.as_ref()])
            }
            CallVariadic { func, exprs } => func.eval(datums, temp_storage, exprs.as_slice()),
            If { cond, then, els } => match cond.eval(datums, temp_storage)? {
                Datum::True => then.eval(datums, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, temp_storage),
                d => Err(EvalError::Internal(
                    format!("if condition evaluated to non-boolean datum: {:?}", d).into(),
                )),
            },
        }
    }

    /// True iff evaluation could possibly error on non-error input `Datum`.
    fn could_error(&self) -> bool {
        use LirScalarExpr::*;
        match self {
            Column(_col, _name) => false,
            Literal(row, ..) => row.is_err(),
            CallUnary { func, expr } => func.could_error() || expr.could_error(),
            CallBinary { func, expr1, expr2 } => {
                func.could_error() || expr1.could_error() || expr2.could_error()
            }
            CallVariadic { func, exprs } => {
                func.could_error() || exprs.iter().any(|e| e.could_error())
            }
            If { cond, then, els } => cond.could_error() || then.could_error() || els.could_error(),
        }
    }
}

impl OptimizableExpr for LirScalarExpr {
    fn is_literal(&self) -> bool {
        matches!(self, LirScalarExpr::Literal(_, _))
    }

    fn is_literal_err(&self) -> bool {
        matches!(self, LirScalarExpr::Literal(Err(_), _))
    }

    fn contains_temporal(&self) -> bool {
        false // LIR has no CallUnmaterializable, so no mz_now()
    }

    fn size(&self) -> usize {
        let mut size = 0;
        self.visit_pre(|_| size += 1);
        size
    }

    fn eager_children(&mut self) -> Option<Vec<&mut Self>> {
        // Do not eagerly memoize `if` branches that might not be taken.
        if let LirScalarExpr::If { cond, .. } = self {
            return Some(vec![cond]);
        }

        // Do not eagerly memoize `COALESCE` expressions after the first.
        if let LirScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce(_),
            exprs,
        } = self
        {
            return Some(exprs.iter_mut().take(1).collect());
        }

        // No temporal filters in LIR.
        None
    }

    fn equality_column_alias(predicate: &Self, expr: &Self, threshold: usize) -> Option<Self> {
        if let LirScalarExpr::CallBinary {
            func: BinaryFunc::Eq(_),
            expr1,
            expr2,
        } = predicate
        {
            if let LirScalarExpr::Column(c, name) = &**expr1 {
                if *c < threshold && &**expr2 == expr {
                    return Some(LirScalarExpr::Column(*c, name.clone()));
                }
            }
            if let LirScalarExpr::Column(c, name) = &**expr2 {
                if *c < threshold && &**expr1 == expr {
                    return Some(LirScalarExpr::Column(*c, name.clone()));
                }
            }
        }
        None
    }

    fn extract_temporal_bounds(temporal: Vec<Self>) -> Result<(Vec<Self>, Vec<Self>), String> {
        if temporal.is_empty() {
            Ok((Vec::new(), Vec::new()))
        } else {
            Err("LIR expressions do not support temporal predicates".into())
        }
    }
}

// We need a custom Debug because we don't want to show `None` for name information.
// Sadly, the `derivative` crate doesn't support this use case.
impl std::fmt::Debug for LirScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LirScalarExpr::Column(i, TreatAsEqual(Some(name))) => {
                write!(f, "Column({i}, {name:?})")
            }
            LirScalarExpr::Column(i, TreatAsEqual(None)) => write!(f, "Column({i})"),
            LirScalarExpr::Literal(lit, typ) => write!(f, "Literal({lit:?}, {typ:?})"),
            LirScalarExpr::CallUnary { func, expr } => {
                write!(f, "CallUnary({func:?}, {expr:?})")
            }
            LirScalarExpr::CallBinary { func, expr1, expr2 } => {
                write!(f, "CallBinary({func:?}, {expr1:?}, {expr2:?})")
            }
            LirScalarExpr::CallVariadic { func, exprs } => {
                write!(f, "CallVariadic({func:?}, {exprs:?})")
            }
            LirScalarExpr::If { cond, then, els } => {
                write!(f, "If({cond:?}, {then:?}, {els:?})")
            }
        }
    }
}

impl ScalarOps for LirScalarExpr {
    fn match_col_ref(&self) -> Option<usize> {
        match self {
            LirScalarExpr::Column(c, _name) => Some(*c),
            _ => None,
        }
    }

    fn references(&self, column: usize) -> bool {
        match self {
            LirScalarExpr::Column(c, _name) => *c == column,
            _ => false,
        }
    }
}

impl mz_expr::explain::HumanizeDisplay for LirScalarExpr {
    fn humanize<'a, M: HumanizerMode>(
        e: &HumanizedExpr<'a, Self, M>,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        use LirScalarExpr::*;

        match e.expr {
            Column(i, TreatAsEqual(None)) => {
                // Delegate to the `HumanizedExpr<'a, _>` implementation (plain column reference).
                e.child(i).fmt(f)
            }
            Column(i, TreatAsEqual(Some(name))) => {
                // Delegate to the `HumanizedExpr<'a, _>` implementation (with stored name information)
                e.child(&(i, name)).fmt(f)
            }
            Literal(row, _) => {
                // Delegate to the `HumanizedExpr<'a, _>` implementation.
                e.child(row).fmt(f)
            }
            CallUnary { func, expr } => {
                if let UnaryFunc::Not(_) = *func {
                    if let CallUnary { func, expr } = expr.as_ref() {
                        if let Some(is) = func.is() {
                            let expr = e.child::<LirScalarExpr>(&*expr);
                            return write!(f, "({}) IS NOT {}", expr, is);
                        }
                    }
                }
                if let Some(is) = func.is() {
                    let expr = e.child::<LirScalarExpr>(&*expr);
                    write!(f, "({}) IS {}", expr, is)
                } else {
                    let expr = e.child::<LirScalarExpr>(&*expr);
                    write!(f, "{}({})", func, expr)
                }
            }
            CallBinary { func, expr1, expr2 } => {
                let expr1 = e.child::<LirScalarExpr>(&*expr1);
                let expr2 = e.child::<LirScalarExpr>(&*expr2);
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)
                }
            }
            CallVariadic { func, exprs } => {
                use VariadicFunc::*;
                match func {
                    CaseLiteral(cl) => {
                        let input = e.child::<LirScalarExpr>(&exprs[0]);
                        write!(f, "case_lookup {}", input)?;
                        for entry in &cl.lookup {
                            let result = e.child::<LirScalarExpr>(&exprs[entry.expr_index]);
                            write!(f, " when ")?;
                            e.mode.humanize_datum(entry.literal.unpack_first(), f)?;
                            write!(f, " then {}", result)?;
                        }
                        let els = e.child::<LirScalarExpr>(exprs.last().unwrap());
                        write!(f, " else {} end", els)
                    }
                    ArrayCreate(..) => {
                        let exprs = exprs.iter().map(|expr| e.child(expr));
                        let exprs = separated(", ", exprs);
                        write!(f, "array[{}]", exprs)
                    }
                    ListCreate(..) => {
                        let exprs = exprs.iter().map(|expr| e.child(expr));
                        let exprs = separated(", ", exprs);
                        write!(f, "list[{}]", exprs)
                    }
                    RecordCreate(..) => {
                        let exprs = exprs.iter().map(|expr| e.child(expr));
                        let exprs = separated(", ", exprs);
                        write!(f, "row({})", exprs)
                    }
                    func if func.is_infix_op() && exprs.len() > 1 => {
                        let exprs = exprs.iter().map(|expr| e.child(expr));
                        let func = format!(" {} ", func);
                        let exprs = separated(&func, exprs);
                        write!(f, "({})", exprs)
                    }
                    func => {
                        let exprs = exprs.iter().map(|expr| e.child(expr));
                        let exprs = separated(", ", exprs);
                        write!(f, "{}({})", func, exprs)
                    }
                }
            }
            If { cond, then, els } => {
                let cond = e.child::<LirScalarExpr>(&*cond);
                let then = e.child::<LirScalarExpr>(&*then);
                let els = e.child::<LirScalarExpr>(&*els);
                write!(f, "case when {} then {} else {} end", cond, then, els)
            }
        }
    }
}

impl std::fmt::Display for LirScalarExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = HumanizedExplain::default();
        std::fmt::Display::fmt(&mode.expr(self, None), f)
    }
}

impl From<&LirScalarExpr> for MirScalarExpr {
    fn from(value: &LirScalarExpr) -> Self {
        use LirScalarExpr::*;
        match value {
            Column(c, treat_as_equal) => MirScalarExpr::Column(c.clone(), treat_as_equal.clone()),
            Literal(row, repr_column_type) => {
                MirScalarExpr::Literal(row.clone(), repr_column_type.clone())
            }
            CallUnary { func, expr } => MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(MirScalarExpr::from(expr.as_ref())),
            },
            CallBinary { func, expr1, expr2 } => MirScalarExpr::CallBinary {
                func: func.clone(),
                expr1: Box::new(MirScalarExpr::from(expr1.as_ref())),
                expr2: Box::new(MirScalarExpr::from(expr2.as_ref())),
            },
            CallVariadic { func, exprs } => MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: exprs.iter().map(MirScalarExpr::from).collect(),
            },
            If { cond, then, els } => MirScalarExpr::If {
                cond: Box::new(MirScalarExpr::from(cond.as_ref())),
                then: Box::new(MirScalarExpr::from(then.as_ref())),
                els: Box::new(MirScalarExpr::from(els.as_ref())),
            },
        }
    }
}

impl TryFrom<&MirScalarExpr> for LirScalarExpr {
    // MIR-to-LIR failures come from unmaterializable functions that haven't been dealt with yet.
    type Error = Vec<UnmaterializableFunc>;

    fn try_from(value: &MirScalarExpr) -> Result<Self, Self::Error> {
        use MirScalarExpr::*;
        match value {
            Column(c, treat_as_equal) => Ok(LirScalarExpr::Column(*c, treat_as_equal.clone())),
            Literal(row, repr_column_type) => Ok(LirScalarExpr::Literal(
                row.clone(),
                repr_column_type.clone(),
            )),
            CallUnary { func, expr } => Ok(LirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(LirScalarExpr::try_from(expr.as_ref())?),
            }),
            CallBinary { func, expr1, expr2 } => {
                match (
                    LirScalarExpr::try_from(expr1.as_ref()),
                    LirScalarExpr::try_from(expr2.as_ref()),
                ) {
                    (Ok(expr1), Ok(expr2)) => Ok(LirScalarExpr::CallBinary {
                        func: func.clone(),
                        expr1: Box::new(expr1),
                        expr2: Box::new(expr2),
                    }),
                    (Ok(_), Err(e)) | (Err(e), Ok(_)) => Err(e),
                    (Err(mut e1), Err(mut e2)) => {
                        e1.append(&mut e2);
                        Err(e1)
                    }
                }
            }
            CallVariadic { func, exprs } => {
                let (exprs, errors): (Vec<LirScalarExpr>, Vec<Vec<UnmaterializableFunc>>) = exprs
                    .into_iter()
                    .map(LirScalarExpr::try_from)
                    .partition_result();

                if errors.is_empty() {
                    Ok(LirScalarExpr::CallVariadic {
                        func: func.clone(),
                        exprs,
                    })
                } else {
                    Err(errors.concat())
                }
            }
            If { cond, then, els } => {
                let cond = LirScalarExpr::try_from(cond.as_ref());
                let then = LirScalarExpr::try_from(then.as_ref());
                let els = LirScalarExpr::try_from(els.as_ref());

                match (cond, then, els) {
                    (Ok(cond), Ok(then), Ok(els)) => Ok(LirScalarExpr::If {
                        cond: Box::new(cond),
                        then: Box::new(then),
                        els: Box::new(els),
                    }),
                    (Err(e), Ok(_), Ok(_)) | (Ok(_), Err(e), Ok(_)) | (Ok(_), Ok(_), Err(e)) => {
                        Err(e)
                    }
                    (Err(mut e1), Err(mut e2), Ok(_))
                    | (Err(mut e1), Ok(_), Err(mut e2))
                    | (Ok(_), Err(mut e1), Err(mut e2)) => {
                        e1.append(&mut e2);
                        Err(e1)
                    }
                    (Err(mut e1), Err(mut e2), Err(mut e3)) => {
                        e1.append(&mut e2);
                        e1.append(&mut e3);
                        Err(e1)
                    }
                }
            }
            CallUnmaterializable(f) => Err(vec![f.clone()]),
        }
    }
}

/// Convert a MIR `MapFilterProject` to LIR.
///
/// Panics if any expression contains unmaterializable functions.
pub fn mfp_mir_to_lir(mfp: MapFilterProject<MirScalarExpr>) -> MapFilterProject<LirScalarExpr> {
    let expressions = lses_from_mses(&mfp.expressions);
    let predicates = mfp
        .predicates
        .iter()
        .map(|(pos, pred)| {
            (
                *pos,
                LirScalarExpr::try_from(pred).expect("unmaterializable in MFP predicate"),
            )
        })
        .collect();
    MapFilterProject::<LirScalarExpr> {
        expressions,
        predicates,
        projection: mfp.projection,
        input_arity: mfp.input_arity,
    }
}

/// Convert a MIR `SafeMfpPlan` to LIR.
///
/// Panics if any expression contains unmaterializable functions.
pub fn safe_mfp_mir_to_lir(plan: SafeMfpPlan<MirScalarExpr>) -> SafeMfpPlan<LirScalarExpr> {
    SafeMfpPlan::from_mfp(mfp_mir_to_lir(plan.into_mfp()))
}

/// Convert a MIR `MapFilterProject` into an LIR `MfpPlan`.
///
/// The temporal bounds and the inner SafeMfpPlan are all `mz_now()`-free
/// after temporal extraction, so conversion always succeeds.
/// Panics if any expression unexpectedly contains unmaterializable functions.
pub fn mfp_mir_to_lir_plan(mfp: MapFilterProject<MirScalarExpr>) -> MfpPlan<LirScalarExpr> {
    let plan = mfp.into_plan().expect("MFP planning failed");
    mfp_plan_mir_to_lir(plan)
}

/// Convert a MIR `MfpPlan` to LIR.
///
/// The temporal bounds and the inner SafeMfpPlan are all `mz_now()`-free
/// after temporal extraction, so conversion always succeeds.
/// Panics if any expression unexpectedly contains unmaterializable functions.
pub fn mfp_plan_mir_to_lir(plan: MfpPlan<MirScalarExpr>) -> MfpPlan<LirScalarExpr> {
    let (safe, lower, upper) = plan.into_parts();
    MfpPlan::from_parts(
        safe_mfp_mir_to_lir(safe),
        lses_from_mses(&lower),
        lses_from_mses(&upper),
    )
}

/// Convert a LIR `MfpPlan` to MIR (always succeeds).
pub fn mfp_plan_lir_to_mir(plan: MfpPlan<LirScalarExpr>) -> MfpPlan<MirScalarExpr> {
    let (safe, lower, upper) = plan.into_parts();

    let mfp = safe.into_mfp();
    let expressions = mfp.expressions.iter().map(MirScalarExpr::from).collect();
    let predicates = mfp
        .predicates
        .iter()
        .map(|(pos, pred)| (*pos, MirScalarExpr::from(pred)))
        .collect();
    let mir_mfp = MapFilterProject::<MirScalarExpr> {
        expressions,
        predicates,
        projection: mfp.projection,
        input_arity: mfp.input_arity,
    };

    let lower = lower.iter().map(MirScalarExpr::from).collect();
    let upper = upper.iter().map(MirScalarExpr::from).collect();
    MfpPlan::from_parts(SafeMfpPlan::from_mfp(mir_mfp), lower, upper)
}

/// Translates a `&Vec<MirScalarExpr>` (or similar) to a `Vec<LirScalarExpr>`.
///
/// LIR-level expressions never contain unmaterializable functions, so this
/// conversion is total in practice. The function follows the convention that
/// the non-`try_` variant panics on failure: a panic here indicates a lowering
/// bug, not a recoverable condition.
pub(crate) fn lses_from_mses<'a>(
    exprs: impl IntoIterator<Item = &'a MirScalarExpr>,
) -> Vec<LirScalarExpr> {
    match exprs
        .into_iter()
        .map(LirScalarExpr::try_from)
        .collect::<Result<Vec<LirScalarExpr>, _>>()
    {
        Ok(exprs) => exprs,
        Err(funcs) => {
            panic!(
                "unmaterializable functions cannot be translated to LirScalarExpr: {}",
                separated(", ", &funcs)
            )
        }
    }
}
