// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Column-at-a-time evaluation of MFP batches.
//!
//! [`SafeMfpPlan::evaluate_batch`](crate::SafeMfpPlan::evaluate_batch)
//! evaluates a batch here when every map and predicate of the plan is built
//! from nodes with a typed kernel: column references, literals, `If`, and the
//! [`Int64Op`] functions, over input columns that hold only `Int64` or only
//! boolean values. Each expression then runs once per batch over typed
//! columns, under a selection of the rows still live. Any other plan stays on
//! the row-at-a-time loop, which costs no more than evaluating rows one at a
//! time; evaluating such a plan a column at a time, with per-row fallbacks,
//! measured slower than the loop.
//!
//! Every row gets the outcome per-row evaluation gives it. Rows leave the
//! selection at their first error or failed predicate, as
//! [`SafeMfpPlan::evaluate_inner`](crate::SafeMfpPlan::evaluate_inner) stops
//! there, `If` evaluates each branch only on the rows that take it, and a
//! function's error is its first argument's error, else its second's, else
//! its own, matching the eager functions' error precedence. Typed columns hold
//! no internal errors, so that precedence needs no other case.

use std::collections::BTreeSet;

use columnar::primitive::Bools;
use columnar::{Index, Push};
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_repr::Datum;

use crate::linear::plan::BatchInput;
use crate::{BinaryFunc, EvalError, MapFilterProject};

/// The structure of an expression node, as column-at-a-time evaluation sees
/// it.
#[derive(Debug)]
pub enum BatchNode<'e, E> {
    /// A reference to a column.
    Column(usize),
    /// A literal value.
    Literal(Datum<'e>),
    /// An [`Int64Op`] applied to two arguments.
    Int64(Int64Op, &'e E, &'e E),
    /// `If { cond, then, els }`.
    If(&'e E, &'e E, &'e E),
    /// Any other node, which keeps its plan on the row-at-a-time loop.
    Other,
}

/// Expressions that column-at-a-time evaluation can inspect.
pub trait BatchExpr: Sized {
    /// The structure of this node.
    fn batch_node(&self) -> BatchNode<'_, Self>;
}

/// The binary functions with `Int64` kernels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Int64Op {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl Int64Op {
    /// The kernel for `func` on `Int64` arguments, if it has one. The
    /// comparisons apply to any argument type; the kernel covers them only
    /// where [`evaluate`] has established both arguments are `Int64`.
    pub fn from_func(func: &BinaryFunc) -> Option<Self> {
        Some(match func {
            BinaryFunc::AddInt64(_) => Int64Op::Add,
            BinaryFunc::SubInt64(_) => Int64Op::Sub,
            BinaryFunc::MulInt64(_) => Int64Op::Mul,
            BinaryFunc::DivInt64(_) => Int64Op::Div,
            BinaryFunc::ModInt64(_) => Int64Op::Mod,
            BinaryFunc::Eq(_) => Int64Op::Eq,
            BinaryFunc::NotEq(_) => Int64Op::NotEq,
            BinaryFunc::Lt(_) => Int64Op::Lt,
            BinaryFunc::Lte(_) => Int64Op::Lte,
            BinaryFunc::Gt(_) => Int64Op::Gt,
            BinaryFunc::Gte(_) => Int64Op::Gte,
            _ => return None,
        })
    }

    fn output(self) -> Kind {
        match self {
            Int64Op::Add | Int64Op::Sub | Int64Op::Mul | Int64Op::Div | Int64Op::Mod => Kind::Int64,
            _ => Kind::Bool,
        }
    }

    /// The result of the function on non-null arguments, as its `Int64`
    /// implementation computes it.
    fn arith(self, l: i64, r: i64) -> Result<i64, EvalError> {
        match self {
            Int64Op::Add => l.checked_add(r).ok_or(EvalError::NumericFieldOverflow),
            Int64Op::Sub => l.checked_sub(r).ok_or(EvalError::NumericFieldOverflow),
            Int64Op::Mul => l.checked_mul(r).ok_or(EvalError::NumericFieldOverflow),
            Int64Op::Div if r == 0 => Err(EvalError::DivisionByZero),
            Int64Op::Div => l
                .checked_div(r)
                .ok_or_else(|| EvalError::Int64OutOfRange(format!("{l} / {r}").into())),
            Int64Op::Mod if r == 0 => Err(EvalError::DivisionByZero),
            Int64Op::Mod => Ok(l.checked_rem(r).unwrap_or(0)),
            _ => unreachable!("{self:?} is a comparison"),
        }
    }

    fn compare(self, l: i64, r: i64) -> bool {
        match self {
            Int64Op::Eq => l == r,
            Int64Op::NotEq => l != r,
            Int64Op::Lt => l < r,
            Int64Op::Lte => l <= r,
            Int64Op::Gt => l > r,
            Int64Op::Gte => l >= r,
            _ => unreachable!("{self:?} is arithmetic"),
        }
    }
}

/// The value type of a column or expression on the typed path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Int64,
    Bool,
    /// Only nulls, which either type accepts.
    Null,
}

impl Kind {
    fn unify(self, other: Kind) -> Option<Kind> {
        match (self, other) {
            (a, b) if a == b => Some(a),
            (Kind::Null, k) | (k, Kind::Null) => Some(k),
            _ => None,
        }
    }
}

/// A typed column, one value per row of a batch or per selected row.
#[derive(Clone, Debug)]
enum Column {
    Int64 {
        values: Vec<i64>,
        nulls: Option<Bools>,
    },
    Bool {
        values: Bools,
        nulls: Option<Bools>,
    },
}

impl Column {
    fn is_null(&self, index: usize) -> bool {
        let nulls = match self {
            Column::Int64 { nulls, .. } | Column::Bool { nulls, .. } => nulls,
        };
        nulls.as_ref().is_some_and(|nulls| nulls.get(index))
    }

    fn datum(&self, index: usize) -> Datum<'static> {
        if self.is_null(index) {
            return Datum::Null;
        }
        match self {
            Column::Int64 { values, .. } => Datum::Int64(values[index]),
            Column::Bool { values, .. } => Datum::from(values.get(index)),
        }
    }

    /// The column of `datums`, if they are all of one kind.
    fn from_datums(datums: &[Datum]) -> Option<Column> {
        let nulls = datums
            .iter()
            .any(|datum| datum.is_null())
            .then(|| bools(datums.iter().map(|datum| datum.is_null())));
        if datums
            .iter()
            .all(|datum| matches!(datum, Datum::Int64(_) | Datum::Null))
        {
            let values = datums
                .iter()
                .map(|datum| match datum {
                    Datum::Int64(value) => *value,
                    _ => 0,
                })
                .collect();
            Some(Column::Int64 { values, nulls })
        } else if datums
            .iter()
            .all(|datum| matches!(datum, Datum::True | Datum::False | Datum::Null))
        {
            let values = bools(datums.iter().map(|datum| *datum == Datum::True));
            Some(Column::Bool { values, nulls })
        } else {
            None
        }
    }

    fn kind(&self) -> Kind {
        match self {
            Column::Int64 { .. } => Kind::Int64,
            Column::Bool { .. } => Kind::Bool,
        }
    }

    /// The values at `sel`, in order.
    fn gather(&self, sel: &[u32]) -> Column {
        let at = |row: &u32| usize::cast_from(*row);
        let nulls = |nulls: &Option<Bools>| {
            nulls
                .as_ref()
                .map(|nulls| bools(sel.iter().map(|row| nulls.get(at(row)))))
        };
        match self {
            Column::Int64 { values, nulls: n } => Column::Int64 {
                values: sel.iter().map(|row| values[at(row)]).collect(),
                nulls: nulls(n),
            },
            Column::Bool { values, nulls: n } => Column::Bool {
                values: bools(sel.iter().map(|row| values.get(at(row)))),
                nulls: nulls(n),
            },
        }
    }
}

fn bools(bits: impl Iterator<Item = bool>) -> Bools {
    let mut bools = Bools::default();
    for bit in bits {
        bools.push(bit);
    }
    bools
}

/// An expression's values on the selected rows of a batch, and the errors of
/// the rows that failed, by position in the selection, in order. A failed
/// row's value is meaningless.
struct Evaluated {
    values: Values,
    errors: Vec<(usize, EvalError)>,
}

enum Values {
    /// The same value for every selected row: an `Int64`, a boolean, or null.
    Const(Option<Scalar>),
    Column(Column),
}

#[derive(Clone, Copy)]
enum Scalar {
    Int64(i64),
    Bool(bool),
}

impl Values {
    fn int64(&self, position: usize) -> Option<i64> {
        match self {
            Values::Const(Some(Scalar::Int64(value))) => Some(*value),
            Values::Const(_) => None,
            Values::Column(column @ Column::Int64 { values, .. }) => {
                (!column.is_null(position)).then(|| values[position])
            }
            Values::Column(Column::Bool { .. }) => unreachable!("typed as Int64"),
        }
    }

    fn bool(&self, position: usize) -> Option<bool> {
        match self {
            Values::Const(Some(Scalar::Bool(value))) => Some(*value),
            Values::Const(_) => None,
            Values::Column(column @ Column::Bool { values, .. }) => {
                (!column.is_null(position)).then(|| values.get(position))
            }
            Values::Column(Column::Int64 { .. }) => unreachable!("typed as Bool"),
        }
    }

    fn may_be_null(&self) -> bool {
        match self {
            Values::Const(scalar) => scalar.is_none(),
            Values::Column(Column::Int64 { nulls, .. } | Column::Bool { nulls, .. }) => {
                nulls.is_some()
            }
        }
    }
}

/// The error at `position` in `errors`, which is sorted by position.
fn error_at(errors: &[(usize, EvalError)], position: usize) -> Option<&EvalError> {
    errors
        .binary_search_by_key(&position, |(p, _)| *p)
        .ok()
        .map(|index| &errors[index].1)
}

/// The kind of `expr` given the kinds of the columns it reads, if every node
/// of it has a typed kernel.
fn kind<E: BatchExpr>(expr: &E, columns: &[Option<Kind>]) -> Option<Kind> {
    match expr.batch_node() {
        BatchNode::Column(index) => columns.get(index).copied().flatten(),
        BatchNode::Literal(Datum::Int64(_)) => Some(Kind::Int64),
        BatchNode::Literal(Datum::True | Datum::False) => Some(Kind::Bool),
        BatchNode::Literal(Datum::Null) => Some(Kind::Null),
        BatchNode::Literal(_) | BatchNode::Other => None,
        BatchNode::Int64(op, left, right) => {
            kind(left, columns)?.unify(Kind::Int64)?;
            kind(right, columns)?.unify(Kind::Int64)?;
            Some(op.output())
        }
        BatchNode::If(cond, then, els) => {
            kind(cond, columns)?.unify(Kind::Bool)?;
            kind(then, columns)?.unify(kind(els, columns)?)
        }
    }
}

/// Whether every node of `expr` has a typed kernel, before input types are
/// known.
fn has_kernels<E: BatchExpr>(expr: &E) -> bool {
    match expr.batch_node() {
        BatchNode::Column(_) => true,
        BatchNode::Literal(datum) => {
            matches!(
                datum,
                Datum::Int64(_) | Datum::True | Datum::False | Datum::Null
            )
        }
        BatchNode::Int64(_, left, right) => has_kernels(left) && has_kernels(right),
        BatchNode::If(cond, then, els) => {
            has_kernels(cond) && has_kernels(then) && has_kernels(els)
        }
        BatchNode::Other => false,
    }
}

/// The input columns `expr` reads.
fn input_columns<E: BatchExpr>(expr: &E, arity: usize, demand: &mut BTreeSet<usize>) {
    match expr.batch_node() {
        BatchNode::Column(index) if index < arity => {
            demand.insert(index);
        }
        BatchNode::Int64(_, left, right) => {
            input_columns(left, arity, demand);
            input_columns(right, arity, demand);
        }
        BatchNode::If(cond, then, els) => {
            input_columns(cond, arity, demand);
            input_columns(then, arity, demand);
            input_columns(els, arity, demand);
        }
        _ => {}
    }
}

/// Evaluates `expr` on the rows `sel` of `columns`. `expr` has a kind under
/// `columns`' kinds.
fn eval<E: BatchExpr>(expr: &E, columns: &[Option<Column>], sel: &[u32]) -> Evaluated {
    let values = match expr.batch_node() {
        BatchNode::Column(index) => {
            Values::Column(columns[index].as_ref().expect("read column").gather(sel))
        }
        BatchNode::Literal(Datum::Int64(value)) => Values::Const(Some(Scalar::Int64(value))),
        BatchNode::Literal(Datum::True) => Values::Const(Some(Scalar::Bool(true))),
        BatchNode::Literal(Datum::False) => Values::Const(Some(Scalar::Bool(false))),
        BatchNode::Literal(_) => Values::Const(None),
        BatchNode::Int64(op, left, right) => {
            return eval_int64(
                op,
                eval(left, columns, sel),
                eval(right, columns, sel),
                sel.len(),
            );
        }
        BatchNode::If(cond, then, els) => return eval_if(cond, then, els, columns, sel),
        BatchNode::Other => unreachable!("typed expressions have no other nodes"),
    };
    Evaluated {
        values,
        errors: Vec::new(),
    }
}

fn eval_int64(op: Int64Op, left: Evaluated, right: Evaluated, len: usize) -> Evaluated {
    let mut errors = Vec::new();
    let mut nulls =
        (left.values.may_be_null() || right.values.may_be_null()).then(|| Vec::with_capacity(len));
    let mut push_null = |null: bool| {
        if let Some(nulls) = nulls.as_mut() {
            nulls.push(null);
        }
    };
    let failed = |position: usize| {
        error_at(&left.errors, position)
            .or_else(|| error_at(&right.errors, position))
            .cloned()
    };
    let column = match op.output() {
        Kind::Int64 => {
            let mut values = Vec::with_capacity(len);
            for position in 0..len {
                if let Some(error) = failed(position) {
                    errors.push((position, error));
                    values.push(0);
                    push_null(false);
                    continue;
                }
                match (left.values.int64(position), right.values.int64(position)) {
                    (Some(l), Some(r)) => {
                        match op.arith(l, r) {
                            Ok(value) => values.push(value),
                            Err(error) => {
                                errors.push((position, error));
                                values.push(0);
                            }
                        }
                        push_null(false);
                    }
                    _ => {
                        values.push(0);
                        push_null(true);
                    }
                }
            }
            Column::Int64 {
                values,
                nulls: nulls.map(|nulls| bools(nulls.into_iter())),
            }
        }
        _ => {
            let mut values = Bools::default();
            for position in 0..len {
                if let Some(error) = failed(position) {
                    errors.push((position, error));
                    values.push(false);
                    push_null(false);
                    continue;
                }
                match (left.values.int64(position), right.values.int64(position)) {
                    (Some(l), Some(r)) => {
                        values.push(op.compare(l, r));
                        push_null(false);
                    }
                    _ => {
                        values.push(false);
                        push_null(true);
                    }
                }
            }
            Column::Bool {
                values,
                nulls: nulls.map(|nulls| bools(nulls.into_iter())),
            }
        }
    };
    Evaluated {
        values: Values::Column(column),
        errors,
    }
}

fn eval_if<E: BatchExpr>(
    cond: &E,
    then: &E,
    els: &E,
    columns: &[Option<Column>],
    sel: &[u32],
) -> Evaluated {
    let conds = eval(cond, columns, sel);
    let (mut then_sel, mut els_sel) = (Vec::new(), Vec::new());
    // Each selected row's source: its position in the `then` or `els`
    // results, or the condition's error.
    let mut sources = Vec::with_capacity(sel.len());
    for (position, row) in sel.iter().enumerate() {
        sources.push(match error_at(&conds.errors, position) {
            Some(error) => Err(error.clone()),
            None if conds.values.bool(position) == Some(true) => {
                then_sel.push(*row);
                Ok((true, then_sel.len() - 1))
            }
            None => {
                els_sel.push(*row);
                Ok((false, els_sel.len() - 1))
            }
        });
    }
    let then_out = eval(then, columns, &then_sel);
    let els_out = eval(els, columns, &els_sel);
    let mut errors = Vec::new();
    let mut datums = Vec::with_capacity(sel.len());
    for (position, source) in sources.into_iter().enumerate() {
        let (branch, index) = match source {
            Ok(source) => source,
            Err(error) => {
                errors.push((position, error));
                datums.push(Datum::Null);
                continue;
            }
        };
        let out = if branch { &then_out } else { &els_out };
        match error_at(&out.errors, index) {
            Some(error) => {
                errors.push((position, error.clone()));
                datums.push(Datum::Null);
            }
            None => datums.push(value_datum(&out.values, index)),
        }
    }
    let column = Column::from_datums(&datums).expect("the branches have one kind");
    Evaluated {
        values: Values::Column(column),
        errors,
    }
}

fn value_datum(values: &Values, position: usize) -> Datum<'static> {
    match values {
        Values::Const(None) => Datum::Null,
        Values::Const(Some(Scalar::Int64(value))) => Datum::Int64(*value),
        Values::Const(Some(Scalar::Bool(value))) => Datum::from(*value),
        Values::Column(column) => column.datum(position),
    }
}

/// Evaluates `mfp`'s maps and filters on `input` a column at a time, as
/// [`SafeMfpPlan::evaluate_batch`](crate::SafeMfpPlan::evaluate_batch)
/// specifies, if every map and predicate has typed kernels for the input's
/// values. Returns `each` unused otherwise.
pub(crate) fn evaluate<'a, E, I, F>(
    mfp: &'a MapFilterProject<E>,
    input: &I,
    mut each: F,
) -> Result<(), F>
where
    E: crate::OptimizableExpr + BatchExpr,
    I: BatchInput<'a> + ?Sized,
    F: FnMut(usize, Result<bool, EvalError>, &[Datum<'a>]),
{
    let exprs = || {
        mfp.expressions
            .iter()
            .chain(mfp.predicates.iter().map(|(_, p)| p))
    };
    if !exprs().all(has_kernels) {
        return Err(each);
    }
    let len = input.len();
    let mut demand = BTreeSet::new();
    for expr in exprs() {
        input_columns(expr, mfp.input_arity, &mut demand);
    }

    // Extract the input columns the expressions read.
    let mut extracted: Vec<Vec<Datum>> = demand.iter().map(|_| Vec::with_capacity(len)).collect();
    let mut datums = Vec::new();
    for index in 0..len {
        input.read(index, &mut datums);
        for (column, input_column) in extracted.iter_mut().zip_eq(demand.iter()) {
            column.push(datums[*input_column]);
        }
    }
    let mut columns: Vec<Option<Column>> = (0..mfp.input_arity).map(|_| None).collect();
    for (datums, index) in extracted.iter().zip_eq(demand.iter()) {
        let Some(column) = Column::from_datums(datums) else {
            return Err(each);
        };
        columns[*index] = Some(column);
    }

    // The maps and predicates in evaluation order, as `evaluate_inner` runs
    // them, each with its kind.
    let mut kinds: Vec<Option<Kind>> = columns
        .iter()
        .map(|c| c.as_ref().map(Column::kind))
        .collect();
    let mut steps: Vec<(bool, &E)> = Vec::new();
    let mut expression = 0;
    for (support, predicate) in mfp.predicates.iter() {
        while mfp.input_arity + expression < *support {
            steps.push((true, &mfp.expressions[expression]));
            expression += 1;
        }
        steps.push((false, predicate));
    }
    for expression in expression..mfp.expressions.len() {
        steps.push((true, &mfp.expressions[expression]));
    }
    for (map, expr) in &steps {
        let Some(kind) = kind(*expr, &kinds) else {
            return Err(each);
        };
        if *map {
            kinds.push(Some(kind));
        } else if kind.unify(Kind::Bool).is_none() {
            return Err(each);
        }
    }

    let mut outcomes: Vec<Result<bool, EvalError>> = vec![Ok(true); len];
    let mut sel: Vec<u32> = (0..u32::try_from(len).expect("batch fits u32 rows")).collect();
    for (map, expr) in steps {
        let evaluated = eval(expr, &columns, &sel);
        let mut errors = evaluated.errors.into_iter().peekable();
        let mut live = Vec::with_capacity(sel.len());
        let mut mapped = map.then(|| vec![Datum::Null; len]);
        for (position, row) in sel.iter().enumerate() {
            let index = usize::cast_from(*row);
            if let Some((_, error)) = errors.next_if(|(p, _)| *p == position) {
                outcomes[index] = Err(error);
                continue;
            }
            let datum = value_datum(&evaluated.values, position);
            match &mut mapped {
                Some(mapped) => {
                    mapped[index] = datum;
                    live.push(*row);
                }
                None if datum == Datum::True => live.push(*row),
                None => outcomes[index] = Ok(false),
            }
        }
        if let Some(mapped) = mapped {
            // Rows that left the selection hold nulls, which either kind
            // accepts.
            let column = Column::from_datums(&mapped).expect("the map has one kind");
            columns.push(Some(column));
        }
        sel = live;
    }

    for (index, outcome) in outcomes.into_iter().enumerate() {
        datums.clear();
        if matches!(outcome, Ok(true)) {
            input.read(index, &mut datums);
            datums.truncate(mfp.input_arity);
            Extend::extend(
                &mut datums,
                columns[mfp.input_arity..]
                    .iter()
                    .map(|column| column.as_ref().expect("mapped").datum(index)),
            );
        }
        each(index, outcome, &datums);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprScalarType, RowArena};

    use crate::func::{
        AddInt64, DivInt64, Eq, Gt, Gte, Lt, Lte, ModInt64, MulInt64, NotEq, SubInt64,
    };
    use crate::{BinaryFunc, MapFilterProject, MirScalarExpr, SafeMfpPlan};

    /// Deterministic pseudo-random choices.
    struct Choices(u64);

    impl Choices {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            usize::try_from((self.0 >> 33) % u64::try_from(n).expect("fits")).expect("fits")
        }
    }

    /// Values that reach every kernel's errors and nulls.
    const VALUES: [Option<i64>; 9] = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(2),
        Some(7),
        Some(-1000),
        Some(i64::MAX),
        Some(i64::MIN),
    ];

    fn int_literal(choices: &mut Choices) -> MirScalarExpr {
        match VALUES[choices.below(VALUES.len())] {
            Some(value) => MirScalarExpr::literal_ok(Datum::Int64(value), ReprScalarType::Int64),
            None => MirScalarExpr::literal_null(ReprScalarType::Int64),
        }
    }

    fn int_expr(choices: &mut Choices, columns: usize, depth: usize) -> MirScalarExpr {
        match if depth == 0 {
            choices.below(2)
        } else {
            choices.below(4)
        } {
            0 => MirScalarExpr::column(choices.below(columns)),
            1 => int_literal(choices),
            2 => {
                let func: BinaryFunc = match choices.below(5) {
                    0 => AddInt64.into(),
                    1 => SubInt64.into(),
                    2 => MulInt64.into(),
                    3 => DivInt64.into(),
                    _ => ModInt64.into(),
                };
                int_expr(choices, columns, depth - 1)
                    .call_binary(int_expr(choices, columns, depth - 1), func)
            }
            _ => bool_expr(choices, columns, depth - 1).if_then_else(
                int_expr(choices, columns, depth - 1),
                int_expr(choices, columns, depth - 1),
            ),
        }
    }

    fn bool_expr(choices: &mut Choices, columns: usize, depth: usize) -> MirScalarExpr {
        let func: BinaryFunc = match choices.below(6) {
            0 => Eq.into(),
            1 => NotEq.into(),
            2 => Lt.into(),
            3 => Lte.into(),
            4 => Gt.into(),
            _ => Gte.into(),
        };
        int_expr(choices, columns, depth).call_binary(int_expr(choices, columns, depth), func)
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn columns_match_per_row_evaluation() {
        const ARITY: usize = 3;
        let mut choices = Choices(1);
        let mut columns_taken = 0;
        for _ in 0..500 {
            let mut mfp = MapFilterProject::new(ARITY);
            let mut columns = ARITY;
            for _ in 0..choices.below(3) + 1 {
                if choices.below(3) == 0 {
                    mfp = mfp.filter([bool_expr(&mut choices, columns, 2)]);
                } else {
                    mfp = mfp.map([int_expr(&mut choices, columns, 2)]);
                    columns += 1;
                }
            }
            let plan = SafeMfpPlan::from_mfp(mfp);
            let rows: Vec<Vec<Datum>> = (0..64)
                .map(|_| {
                    (0..ARITY)
                        .map(|_| match VALUES[choices.below(VALUES.len())] {
                            Some(value) => Datum::Int64(value),
                            None => Datum::Null,
                        })
                        .collect()
                })
                .collect();

            let arena = RowArena::new();
            let expected: Vec<_> = rows
                .iter()
                .map(|row| {
                    let mut datums = row.clone();
                    let outcome = plan.evaluate_inner(&mut datums, &arena);
                    let datums = matches!(outcome, Ok(true)).then_some(datums);
                    (outcome, datums)
                })
                .collect();
            let mut actual = Vec::new();
            let taken = super::evaluate(&plan.mfp, &rows[..], |index, outcome, datums| {
                assert_eq!(index, actual.len(), "rows arrive in order");
                let datums = matches!(outcome, Ok(true)).then(|| datums.to_vec());
                actual.push((outcome, datums));
            });
            if taken.is_ok() {
                columns_taken += 1;
                assert_eq!(actual, expected, "{:?}", plan.mfp);
            }
        }
        // Plans whose input columns hold only nulls fall back to the row loop;
        // most plans take the column path.
        assert!(
            columns_taken > 400,
            "{columns_taken} plans took the column path"
        );
    }
}
