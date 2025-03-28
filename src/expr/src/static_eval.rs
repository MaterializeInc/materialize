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

#[derive(Debug, Clone)]
pub(crate) enum StaticMirScalarExprParams {
    /// A function call that takes one expression as an argument.
    CallUnary { func: UnaryFunc },
}

impl StaticMirScalarExprParams {
    pub(crate) fn unwrap_call_unary(&self) -> &UnaryFunc {
        match self {
            Self::CallUnary { func } => func,
        }
    }
}

type StaticUnaryFunc = for<'a> fn(
    this: &'a UnaryFunc,
    datum: Result<Datum<'a>, EvalError>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError>;

#[derive(Debug, Clone)]
enum Instruction {
    Column(usize),
    UnaryFn(StaticUnaryFunc),
    BinaryFn(BinaryFunc),
    VariadicFn(Box<(VariadicFunc, Box<[StaticMirScalarExprs]>)>),
    UnmaterializableFn(UnmaterializableFunc),
    Literal(usize),
    Skip(isize),
    SkipIfFalse(isize),
}

#[derive(Debug)]
enum LabeledInstruction {
    Column(usize),
    UnaryFn(StaticUnaryFunc),
    BinaryFn(BinaryFunc),
    VariadicFn(Box<(VariadicFunc, Box<[StaticMirScalarExprs]>)>),
    UnmaterializableFn(UnmaterializableFunc),
    Label(usize),
    Literal(usize),
    Skip(usize),
    SkipIfFalse(usize),
}

enum PendingInstruction<'a> {
    Expr(&'a MirScalarExpr),
    Label(usize),
    Skip(usize),
    SkipIfFalse(usize),
}

/// TODO
#[derive(Debug, Clone)]
pub struct StaticMirScalarExprs {
    instructions: Vec<Instruction>,
    params: Vec<Option<Box<StaticMirScalarExprParams>>>,
    arities: Vec<usize>,
    literals: Vec<Result<Row, EvalError>>,
}

impl From<&MirScalarExpr> for StaticMirScalarExprs {
    fn from(value: &MirScalarExpr) -> Self {
        let mut instructions: Vec<LabeledInstruction> = Vec::new();
        let mut params = Vec::new();
        let mut arities = Vec::new();

        let mut labels: usize = 0;
        let mut new_label = || {
            labels += 1;
            labels - 1
        };

        let mut literals: Vec<Result<Row, EvalError>> = Vec::new();

        let mut todo = vec![PendingInstruction::Expr(value)];

        while let Some(value) = todo.pop() {
            match value {
                PendingInstruction::Expr(value) => match value {
                    MirScalarExpr::Column(col, _name) => {
                        instructions.push(LabeledInstruction::Column(*col));
                        params.push(None);
                        arities.push(0);
                    }
                    MirScalarExpr::Literal(res, _) => {
                        instructions.push(LabeledInstruction::Literal(literals.len()));
                        params.push(None);
                        arities.push(0);
                        literals.push(res.clone())
                    }
                    MirScalarExpr::CallUnmaterializable(f) => {
                        instructions.push(LabeledInstruction::UnmaterializableFn(f.clone()));
                        params.push(None);
                        arities.push(0);
                    }
                    MirScalarExpr::CallUnary { func, expr } => {
                        instructions.push(LabeledInstruction::UnaryFn(func.static_unary_fn()));
                        params.push(Some(Box::new(StaticMirScalarExprParams::CallUnary {
                            func: func.clone(),
                        })));
                        arities.push(1);
                        todo.push(PendingInstruction::Expr(&*expr));
                    }
                    MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                        instructions.push(LabeledInstruction::BinaryFn(func.clone()));
                        params.push(None);
                        arities.push(2);
                        todo.push(PendingInstruction::Expr(&*expr1));
                        todo.push(PendingInstruction::Expr(&*expr2));
                    }
                    MirScalarExpr::CallVariadic { func, exprs } => {
                        let inputs = exprs.iter().map(Self::from).collect::<Vec<_>>();
                        instructions.push(LabeledInstruction::VariadicFn(Box::new((
                            func.clone(),
                            inputs.into_boxed_slice(),
                        ))));
                        params.push(None);
                        arities.push(exprs.len());
                    }
                    MirScalarExpr::If { cond, then, els } => {
                        let label_false = new_label();
                        let label_after = new_label();
                        todo.push(PendingInstruction::Expr(&*cond));
                        todo.push(PendingInstruction::SkipIfFalse(label_false));
                        todo.push(PendingInstruction::Expr(&*then));
                        todo.push(PendingInstruction::Skip(label_after));
                        todo.push(PendingInstruction::Label(label_false));
                        todo.push(PendingInstruction::Expr(&*els));
                        todo.push(PendingInstruction::Label(label_after));
                    }
                },
                PendingInstruction::Skip(skip) => {
                    instructions.push(LabeledInstruction::Skip(skip));
                    params.push(None);
                    arities.push(0);
                }
                PendingInstruction::SkipIfFalse(skip) => {
                    instructions.push(LabeledInstruction::SkipIfFalse(skip));
                    params.push(None);
                    arities.push(1);
                }
                PendingInstruction::Label(label) => {
                    instructions.push(LabeledInstruction::Label(label));
                }
            }
        }

        instructions.reverse();
        params.reverse();
        arities.reverse();

        let mut label_positions = vec![0; labels];
        let mut instruction_counter = 0;
        for instruction in &instructions {
            if let LabeledInstruction::Label(label) = instruction {
                label_positions[*label] = instruction_counter;
            } else {
                instruction_counter += 1;
            }
        }
        instructions.retain(|i| !matches!(i, LabeledInstruction::Label(_)));

        let instructions = instructions
            .into_iter()
            .enumerate()
            .map(|(i, instr)| {
                let relative_skip = |label| {
                    isize::try_from(label_positions[label]).expect("must_fit")
                        - isize::try_from(i).expect("must_fit")
                };
                match instr {
                    LabeledInstruction::BinaryFn(f) => Instruction::BinaryFn(f),
                    LabeledInstruction::Column(col) => Instruction::Column(col),
                    LabeledInstruction::Label(_) => unreachable!(),
                    LabeledInstruction::Literal(literal) => Instruction::Literal(literal),
                    LabeledInstruction::Skip(label) => Instruction::Skip(relative_skip(label)),
                    LabeledInstruction::SkipIfFalse(label) => {
                        Instruction::SkipIfFalse(relative_skip(label))
                    }
                    LabeledInstruction::UnaryFn(f) => Instruction::UnaryFn(f),
                    LabeledInstruction::UnmaterializableFn(f) => Instruction::UnmaterializableFn(f),
                    LabeledInstruction::VariadicFn(f) => Instruction::VariadicFn(f),
                }
            })
            .collect();
        Self {
            instructions,
            params,
            arities,
            literals,
        }
    }
}

impl StaticMirScalarExprs {
    pub fn eval<'a>(
        &'a self,
        columns: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        // TODO: have two registers for the topmost datums.
        let mut stack = Vec::new();
        let mut pointer = 0;
        while pointer < self.instructions.len() {
            let instruction = &self.instructions[pointer];

            match instruction {
                Instruction::Column(column) => {
                    let datum = columns[*column];
                    stack.push(Ok(datum));
                    pointer += 1;
                }
                Instruction::Literal(literal) => match &self.literals[*literal] {
                    Ok(row) => {
                        let datum = row.unpack_first();
                        stack.push(Ok(datum));
                        pointer += 1;
                    }
                    Err(err) => {
                        stack.push(Err(err.clone()));
                        pointer += 1;
                    }
                },
                Instruction::UnaryFn(func) => {
                    let param = self.params[pointer].as_deref().expect("param exist");
                    let unary_func = param.unwrap_call_unary();
                    let datum = stack.pop().expect("input exists");
                    stack.push(func(unary_func, datum, &temp_storage));
                    pointer += 1;
                }
                Instruction::BinaryFn(func) => {
                    let b = stack.pop().expect("input b exists");
                    let a = stack.pop().expect("input a exists");
                    match (a, b) {
                        (Ok(a), Ok(b)) => {
                            stack.push(func.eval_input(&temp_storage, a, b));
                        }
                        (Err(err), _) | (_, Err(err)) => {
                            stack.push(Err(err));
                        }
                    }
                    pointer += 1;
                }
                Instruction::VariadicFn(input) => {
                    let (variadic_fn, exprs) = input.as_ref();
                    debug_assert_eq!(exprs.len(), self.arities[pointer]);
                    stack.push(variadic_fn.eval_static_the_second(columns, &temp_storage, exprs));
                    pointer += 1;
                }
                Instruction::UnmaterializableFn(func) => {
                    stack.push(Err(EvalError::Internal(
                        format!("cannot evaluate unmaterializable function: {func:?}").into(),
                    )));
                    pointer += 1;
                }
                Instruction::Skip(offset) => {
                    pointer = pointer
                        .checked_add_signed(*offset)
                        .expect("correct instructions")
                }
                Instruction::SkipIfFalse(offset) => {
                    let datum = stack.pop().expect("should have one datum left");
                    if datum == Ok(Datum::False) {
                        pointer = pointer
                            .checked_add_signed(*offset)
                            .expect("correct instructions");
                    } else {
                        pointer += 1;
                    }
                }
            }
        }
        stack.pop().expect("should have one datum left")
    }
}

#[cfg(test)]
mod test {
    use crate::MirScalarExpr;
    use mz_repr::{ColumnType, ScalarType};

    use super::*;

    #[test]
    fn test_construct_static_mir_scalar_expr() {
        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(MirScalarExpr::Column(0)),
            expr2: Box::new(MirScalarExpr::CallUnary {
                func: UnaryFunc::Not(crate::func::Not),
                expr: Box::new(MirScalarExpr::Column(1)),
            }),
        };

        let expr = MirScalarExpr::If {
            cond: Box::new(expr),
            then: Box::new(MirScalarExpr::Literal(
                Ok(Row::pack_slice(&[Datum::String("yes!")])),
                ColumnType {
                    scalar_type: ScalarType::String,
                    nullable: false,
                },
            )),
            els: Box::new(MirScalarExpr::Column(3)),
        };

        let static_expr = StaticMirScalarExprs::from(&expr);

        println!("{:?}", static_expr.instructions);
        println!("{:?}", static_expr.params);
        println!("{:?}", static_expr.arities);

        let temp_storage = RowArena::new();
        let res = static_expr.eval(&[Datum::True, Datum::False], &temp_storage);
        println!("{:?}", res);
    }
}
