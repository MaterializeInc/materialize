// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compiles a [`MirScalarExpr`] tree into a flat [`CompiledMirScalarExpr`]
//! instruction sequence, evaluated by a stack machine with a program counter.
//!
//! # Execution model
//!
//! A value stack holds `Result<Datum, EvalError>` entries. Each instruction
//! pops its inputs, computes, and pushes its output. Branch instructions
//! (`Skip`, `SkipIfNotTrue`, and the variadic step instructions) modify the
//! program counter instead of always advancing by one.
//!
//! # Compilation
//!
//! The expression tree is lowered iteratively using a work stack of
//! `PendingItem`s (either sub-expressions to recurse into, or
//! `LabeledInstruction`s to emit). Items are pushed in **reverse execution
//! order** because the work stack is LIFO. A label-resolution pass then
//! converts symbolic labels to relative PC offsets.
//!
//! # Instruction categories
//!
//! - **Leaf**: `Column`, `Literal`, `CallUnmaterializable`.
//! - **Unary/Binary**: `CallUnary`, `CallBinary` — pop operands, push result.
//! - **Branching (If)**: `SkipIfNotTrue` (two offsets: false branch, error
//!   escape) + `Skip` (unconditional jump past else).
//! - **Inlined variadics**: And/Or use an accumulator on the stack with
//!   `AndStep`/`OrStep` (short-circuit + three-way null/error merge).
//!   `Coalesce` and `ErrorIfNull` use `SkipIfNotNull` + `RaiseIfNullError`.
//!   `Greatest`/`Least` use `GreatestStep`/`LeastStep` with a null
//!   accumulator. All other (eager) variadics use `CallEagerVariadic`.
//! - **Compound casts**: `MapListElements`, `MapArrayElements`, `MapRecord`,
//!   `MapListToJsonb`, `MapArrayToJsonb`, `ParseAndCast` — these contain
//!   compiled sub-programs applied per collection element.

use std::borrow::Cow;
use std::sync::LazyLock;

use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, Row, RowArena, SqlScalarType, strconv};

use crate::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc};

/// Static column-reference expressions for calling `BinaryFunc::eval` with
/// pre-evaluated datum values (avoids lifetime issues with local temporaries).
static COL_0: LazyLock<MirScalarExpr> =
    LazyLock::new(|| MirScalarExpr::Column(0, TreatAsEqual(None)));
static COL_1: LazyLock<MirScalarExpr> =
    LazyLock::new(|| MirScalarExpr::Column(1, TreatAsEqual(None)));

/// A compiled representation of a [`MirScalarExpr`] for efficient evaluation.
///
/// Instead of recursively matching on enum variants, evaluation walks a flat
/// instruction sequence using a value stack and a program counter.
#[derive(Debug, Clone)]
pub struct CompiledMirScalarExpr {
    instructions: Vec<Instruction>,
    /// Literal values referenced by [`Instruction::Literal`] instructions.
    literals: Vec<Result<Row, EvalError>>,
}

/// Instructions for the stack machine.
///
/// Most instructions pop their inputs from the stack and push their result.
/// Branch instructions (`Skip`, `SkipIfNotTrue`) modify the program counter.
#[derive(Debug, Clone)]
enum Instruction {
    /// Push `datums[column]` onto the stack.
    Column(usize),
    /// Push the literal at the given index onto the stack.
    Literal(usize),
    /// Pop 1 value, apply unary function, push result.
    CallUnary(UnaryFunc),
    /// Pop 2 values (first-pushed = left), apply binary function, push result.
    /// Uses `COL_0`/`COL_1` static references to call `BinaryFunc::eval`.
    CallBinary(BinaryFunc),
    /// Push an error for an unmaterializable function.
    CallUnmaterializable(UnmaterializableFunc),
    /// Unconditional relative jump. `pc += offset`.
    Skip(isize),
    /// Pop 1 value; if True, continue. If False/Null, jump by `false_offset`.
    /// If Error, push error back and jump by `error_offset` (skips entire If).
    SkipIfNotTrue {
        false_offset: isize,
        error_offset: isize,
    },

    // -- Inlined variadic instructions --

    /// Pop operand and accumulator for `And`. If operand is False, push False
    /// and jump to end. Otherwise merge into accumulator (error > null > true).
    AndStep(isize),
    /// Pop operand and accumulator for `Or`. If operand is True, push True
    /// and jump to end. Otherwise merge into accumulator (error > null > false).
    OrStep(isize),
    /// Pop value for `Coalesce`/`ErrorIfNull`. If non-null (including errors),
    /// push back and jump to end. If null, discard and continue.
    SkipIfNotNull(isize),
    /// Pop operand and accumulator for `Greatest`. If operand is an error,
    /// push error and jump to end. If null, keep accumulator. Otherwise push
    /// the greater of operand and accumulator.
    GreatestStep(isize),
    /// Pop operand and accumulator for `Least`. Same as GreatestStep but keeps
    /// the lesser value.
    LeastStep(isize),
    /// Pop message from stack (for `ErrorIfNull`). Push `Err(IfNullError(msg))`.
    RaiseIfNullError,
    /// Pop `arity` values, apply eager variadic function, push result.
    /// Handles null propagation if `func.propagates_nulls()`.
    CallEagerVariadic {
        func: VariadicFunc,
        arity: usize,
    },

    // -- Compound-cast instructions --
    // These pop 1 input from the stack, iterate over its elements, apply
    // a compiled sub-program per element, and push the result.

    /// Map a compiled sub-program over each element of a list.
    /// Used by `CastList1ToList2`.
    MapListElements(CompiledMirScalarExpr),
    /// Map a compiled sub-program over each element of an array, preserving
    /// dimensions. Used by `CastArrayToArray`.
    MapArrayElements(CompiledMirScalarExpr),
    /// Zip per-field sub-programs over a record's fields.
    /// Used by `CastRecord1ToRecord2`.
    MapRecord {
        cast_programs: Box<[CompiledMirScalarExpr]>,
        return_ty: SqlScalarType,
    },
    /// Map a sub-program over list elements, converting SQL Null to JsonNull.
    /// Used by `CastListToJsonb`.
    MapListToJsonb(CompiledMirScalarExpr),
    /// Map a sub-program over array elements respecting multi-dim structure,
    /// converting SQL Null to JsonNull. Used by `CastArrayToJsonb`.
    MapArrayToJsonb(CompiledMirScalarExpr),
    /// Parse a string into a compound value, applying a compiled cast
    /// sub-program to each parsed element.
    ParseAndCast(ParseAndCastKind),
}

/// Variants for parse-and-cast operations on string inputs.
#[derive(Debug, Clone)]
enum ParseAndCastKind {
    StringToArray {
        cast_program: CompiledMirScalarExpr,
    },
    StringToList {
        return_ty: SqlScalarType,
        cast_program: CompiledMirScalarExpr,
    },
    StringToMap {
        return_ty: SqlScalarType,
        cast_program: CompiledMirScalarExpr,
    },
    StringToRange {
        cast_program: CompiledMirScalarExpr,
    },
}

// ---------------------------------------------------------------------------
// Compilation: MirScalarExpr -> CompiledMirScalarExpr
// ---------------------------------------------------------------------------

/// An instruction with symbolic labels, used during compilation before
/// label resolution.
enum LabeledInstruction {
    Column(usize),
    Literal(usize),
    CallUnary(UnaryFunc),
    CallBinary(BinaryFunc),
    CallUnmaterializable(UnmaterializableFunc),
    AndStep(usize),
    OrStep(usize),
    SkipIfNotNull(usize),
    GreatestStep(usize),
    LeastStep(usize),
    RaiseIfNullError,
    CallEagerVariadic { func: VariadicFunc, arity: usize },
    MapListElements(CompiledMirScalarExpr),
    MapArrayElements(CompiledMirScalarExpr),
    MapRecord {
        cast_programs: Box<[CompiledMirScalarExpr]>,
        return_ty: SqlScalarType,
    },
    MapListToJsonb(CompiledMirScalarExpr),
    MapArrayToJsonb(CompiledMirScalarExpr),
    ParseAndCast(ParseAndCastKind),
    Label(usize),
    Skip(usize),
    SkipIfNotTrue { false_label: usize, error_label: usize },
}

/// Work items for the compilation loop.
enum PendingItem<'a> {
    Expr(&'a MirScalarExpr),
    Emit(LabeledInstruction),
}

impl From<&MirScalarExpr> for CompiledMirScalarExpr {
    fn from(expr: &MirScalarExpr) -> Self {
        let mut instructions: Vec<LabeledInstruction> = Vec::new();
        let mut literals: Vec<Result<Row, EvalError>> = Vec::new();
        let mut label_count: usize = 0;
        let mut new_label = || {
            let l = label_count;
            label_count += 1;
            l
        };

        // We process the expression tree iteratively using a work stack.
        // Items are pushed in REVERSE execution order because the stack is LIFO.
        let mut todo = vec![PendingItem::Expr(expr)];

        while let Some(item) = todo.pop() {
            match item {
                PendingItem::Expr(expr) => match expr {
                    MirScalarExpr::Column(col, _name) => {
                        instructions.push(LabeledInstruction::Column(*col));
                    }
                    MirScalarExpr::Literal(res, _) => {
                        let idx = literals.len();
                        literals.push(res.clone());
                        instructions.push(LabeledInstruction::Literal(idx));
                    }
                    MirScalarExpr::CallUnmaterializable(f) => {
                        instructions.push(LabeledInstruction::CallUnmaterializable(f.clone()));
                    }
                    MirScalarExpr::CallUnary { func, expr: child } => {
                        if !func.embedded_exprs().is_empty() {
                            // Compound-cast: compile cast sub-expr(s) into sub-programs,
                            // then emit the child inline followed by the compound instruction.
                            let compound_instr = compile_compound_instruction(func);
                            // Push in reverse: compound instr last (popped last = emitted last)
                            todo.push(PendingItem::Emit(compound_instr));
                            todo.push(PendingItem::Expr(child));
                        } else {
                            // Normal unary: child first (on stack), then apply func.
                            todo.push(PendingItem::Emit(
                                LabeledInstruction::CallUnary(func.clone()),
                            ));
                            todo.push(PendingItem::Expr(child));
                        }
                    }
                    MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                        // Result: [expr1_instrs, expr2_instrs, CallBinary]
                        // Push in reverse execution order:
                        todo.push(PendingItem::Emit(
                            LabeledInstruction::CallBinary(func.clone()),
                        ));
                        todo.push(PendingItem::Expr(expr2));
                        todo.push(PendingItem::Expr(expr1));
                    }
                    MirScalarExpr::CallVariadic { func, exprs } => {
                        match func {
                            VariadicFunc::And => {
                                // Literal(True), [eval expr, AndStep(end)]*, Label(end)
                                let end_label = new_label();
                                let lit_idx = literals.len();
                                literals.push(Ok(Row::pack_slice(&[Datum::True])));
                                todo.push(PendingItem::Emit(LabeledInstruction::Label(end_label)));
                                for expr in exprs.iter().rev() {
                                    todo.push(PendingItem::Emit(
                                        LabeledInstruction::AndStep(end_label),
                                    ));
                                    todo.push(PendingItem::Expr(expr));
                                }
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::Literal(lit_idx),
                                ));
                            }
                            VariadicFunc::Or => {
                                // Literal(False), [eval expr, OrStep(end)]*, Label(end)
                                let end_label = new_label();
                                let lit_idx = literals.len();
                                literals.push(Ok(Row::pack_slice(&[Datum::False])));
                                todo.push(PendingItem::Emit(LabeledInstruction::Label(end_label)));
                                for expr in exprs.iter().rev() {
                                    todo.push(PendingItem::Emit(
                                        LabeledInstruction::OrStep(end_label),
                                    ));
                                    todo.push(PendingItem::Expr(expr));
                                }
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::Literal(lit_idx),
                                ));
                            }
                            VariadicFunc::Coalesce => {
                                // [eval expr, SkipIfNotNull(end)]*, eval last, Label(end)
                                // If no exprs, just push Null.
                                if exprs.is_empty() {
                                    let lit_idx = literals.len();
                                    literals.push(Ok(Row::pack_slice(&[Datum::Null])));
                                    instructions.push(LabeledInstruction::Literal(lit_idx));
                                } else {
                                    let end_label = new_label();
                                    todo.push(PendingItem::Emit(
                                        LabeledInstruction::Label(end_label),
                                    ));
                                    // Last expr: just evaluate (no SkipIfNotNull)
                                    todo.push(PendingItem::Expr(exprs.last().unwrap()));
                                    // All but last: eval + SkipIfNotNull
                                    for expr in exprs[..exprs.len() - 1].iter().rev() {
                                        todo.push(PendingItem::Emit(
                                            LabeledInstruction::SkipIfNotNull(end_label),
                                        ));
                                        todo.push(PendingItem::Expr(expr));
                                    }
                                }
                            }
                            VariadicFunc::ErrorIfNull => {
                                // eval value, SkipIfNotNull(end), eval msg, RaiseIfNullError, Label(end)
                                assert_eq!(exprs.len(), 2);
                                let end_label = new_label();
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::Label(end_label),
                                ));
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::RaiseIfNullError,
                                ));
                                todo.push(PendingItem::Expr(&exprs[1]));
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::SkipIfNotNull(end_label),
                                ));
                                todo.push(PendingItem::Expr(&exprs[0]));
                            }
                            VariadicFunc::Greatest => {
                                // Literal(Null), [eval expr, GreatestStep(end)]*, Label(end)
                                let end_label = new_label();
                                let lit_idx = literals.len();
                                literals.push(Ok(Row::pack_slice(&[Datum::Null])));
                                todo.push(PendingItem::Emit(LabeledInstruction::Label(end_label)));
                                for expr in exprs.iter().rev() {
                                    todo.push(PendingItem::Emit(
                                        LabeledInstruction::GreatestStep(end_label),
                                    ));
                                    todo.push(PendingItem::Expr(expr));
                                }
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::Literal(lit_idx),
                                ));
                            }
                            VariadicFunc::Least => {
                                // Literal(Null), [eval expr, LeastStep(end)]*, Label(end)
                                let end_label = new_label();
                                let lit_idx = literals.len();
                                literals.push(Ok(Row::pack_slice(&[Datum::Null])));
                                todo.push(PendingItem::Emit(LabeledInstruction::Label(end_label)));
                                for expr in exprs.iter().rev() {
                                    todo.push(PendingItem::Emit(
                                        LabeledInstruction::LeastStep(end_label),
                                    ));
                                    todo.push(PendingItem::Expr(expr));
                                }
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::Literal(lit_idx),
                                ));
                            }
                            _ => {
                                // Eager variadics: eval all operands inline, then call func
                                let arity = exprs.len();
                                todo.push(PendingItem::Emit(
                                    LabeledInstruction::CallEagerVariadic {
                                        func: func.clone(),
                                        arity,
                                    },
                                ));
                                for expr in exprs.iter().rev() {
                                    todo.push(PendingItem::Expr(expr));
                                }
                            }
                        }
                    }
                    MirScalarExpr::If { cond, then, els } => {
                        let label_else = new_label();
                        let label_end = new_label();
                        // Desired instruction order:
                        //   [cond, SkipIfNotTrue, then, Skip(end), label_else, els, label_end]
                        // Push in reverse for LIFO:
                        todo.push(PendingItem::Emit(LabeledInstruction::Label(label_end)));
                        todo.push(PendingItem::Expr(els));
                        todo.push(PendingItem::Emit(LabeledInstruction::Label(label_else)));
                        todo.push(PendingItem::Emit(LabeledInstruction::Skip(label_end)));
                        todo.push(PendingItem::Expr(then));
                        todo.push(PendingItem::Emit(LabeledInstruction::SkipIfNotTrue {
                            false_label: label_else,
                            error_label: label_end,
                        }));
                        todo.push(PendingItem::Expr(cond));
                    }
                },
                PendingItem::Emit(instr) => {
                    instructions.push(instr);
                }
            }
        }

        // Resolve labels to relative offsets.
        // First, record position of each label (not counting labels themselves).
        let mut label_positions = vec![0usize; label_count];
        let mut pos = 0;
        for instr in &instructions {
            if let LabeledInstruction::Label(l) = instr {
                label_positions[*l] = pos;
            } else {
                pos += 1;
            }
        }

        // Remove labels and convert to final instructions.
        let mut final_instructions = Vec::with_capacity(pos);
        let mut current_pos = 0;
        for instr in instructions {
            match instr {
                LabeledInstruction::Label(_) => continue,
                LabeledInstruction::Column(c) => final_instructions.push(Instruction::Column(c)),
                LabeledInstruction::Literal(l) => final_instructions.push(Instruction::Literal(l)),
                LabeledInstruction::CallUnary(f) => {
                    final_instructions.push(Instruction::CallUnary(f))
                }
                LabeledInstruction::CallBinary(f) => {
                    final_instructions.push(Instruction::CallBinary(f))
                }
                LabeledInstruction::CallUnmaterializable(f) => {
                    final_instructions.push(Instruction::CallUnmaterializable(f))
                }
                LabeledInstruction::MapListElements(p) => {
                    final_instructions.push(Instruction::MapListElements(p))
                }
                LabeledInstruction::MapArrayElements(p) => {
                    final_instructions.push(Instruction::MapArrayElements(p))
                }
                LabeledInstruction::MapRecord {
                    cast_programs,
                    return_ty,
                } => final_instructions.push(Instruction::MapRecord {
                    cast_programs,
                    return_ty,
                }),
                LabeledInstruction::MapListToJsonb(p) => {
                    final_instructions.push(Instruction::MapListToJsonb(p))
                }
                LabeledInstruction::MapArrayToJsonb(p) => {
                    final_instructions.push(Instruction::MapArrayToJsonb(p))
                }
                LabeledInstruction::ParseAndCast(k) => {
                    final_instructions.push(Instruction::ParseAndCast(k))
                }
                LabeledInstruction::AndStep(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::AndStep(offset));
                }
                LabeledInstruction::OrStep(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::OrStep(offset));
                }
                LabeledInstruction::SkipIfNotNull(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::SkipIfNotNull(offset));
                }
                LabeledInstruction::GreatestStep(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::GreatestStep(offset));
                }
                LabeledInstruction::LeastStep(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::LeastStep(offset));
                }
                LabeledInstruction::RaiseIfNullError => {
                    final_instructions.push(Instruction::RaiseIfNullError)
                }
                LabeledInstruction::CallEagerVariadic { func, arity } => {
                    final_instructions.push(Instruction::CallEagerVariadic { func, arity })
                }
                LabeledInstruction::Skip(label) => {
                    let offset = label_positions[label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::Skip(offset));
                }
                LabeledInstruction::SkipIfNotTrue {
                    false_label,
                    error_label,
                } => {
                    let false_offset =
                        label_positions[false_label] as isize - current_pos as isize;
                    let error_offset =
                        label_positions[error_label] as isize - current_pos as isize;
                    final_instructions.push(Instruction::SkipIfNotTrue {
                        false_offset,
                        error_offset,
                    });
                }
            }
            current_pos += 1;
        }

        CompiledMirScalarExpr {
            instructions: final_instructions,
            literals,
        }
    }
}

/// Compile a compound-cast `UnaryFunc` into its corresponding instruction.
/// The embedded cast sub-expressions are compiled into sub-programs.
/// The child (input) expression is NOT handled here; the caller must ensure
/// it is evaluated onto the stack before this instruction executes.
fn compile_compound_instruction(func: &UnaryFunc) -> LabeledInstruction {
    match func {
        UnaryFunc::CastList1ToList2(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::MapListElements(cast_program)
        }
        UnaryFunc::CastArrayToArray(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::MapArrayElements(cast_program)
        }
        UnaryFunc::CastRecord1ToRecord2(inner) => {
            let cast_programs: Box<[CompiledMirScalarExpr]> = inner
                .cast_exprs
                .iter()
                .map(CompiledMirScalarExpr::from)
                .collect();
            LabeledInstruction::MapRecord {
                cast_programs,
                return_ty: inner.return_ty.clone(),
            }
        }
        UnaryFunc::CastListToJsonb(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_element.as_ref());
            LabeledInstruction::MapListToJsonb(cast_program)
        }
        UnaryFunc::CastArrayToJsonb(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_element.as_ref());
            LabeledInstruction::MapArrayToJsonb(cast_program)
        }
        UnaryFunc::CastStringToArray(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::ParseAndCast(ParseAndCastKind::StringToArray { cast_program })
        }
        UnaryFunc::CastStringToList(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::ParseAndCast(ParseAndCastKind::StringToList {
                return_ty: inner.return_ty.clone(),
                cast_program,
            })
        }
        UnaryFunc::CastStringToMap(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::ParseAndCast(ParseAndCastKind::StringToMap {
                return_ty: inner.return_ty.clone(),
                cast_program,
            })
        }
        UnaryFunc::CastStringToRange(inner) => {
            let cast_program = CompiledMirScalarExpr::from(inner.cast_expr.as_ref());
            LabeledInstruction::ParseAndCast(ParseAndCastKind::StringToRange { cast_program })
        }
        _ => unreachable!(
            "compile_compound_instruction called on non-compound function: {func}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

impl CompiledMirScalarExpr {
    /// Evaluate this compiled expression against the given datum columns.
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        let mut stack: Vec<Result<Datum<'a>, EvalError>> = Vec::new();
        let mut pc = 0usize;

        while pc < self.instructions.len() {
            match &self.instructions[pc] {
                Instruction::Column(col) => {
                    stack.push(Ok(datums[*col]));
                    pc += 1;
                }
                Instruction::Literal(idx) => {
                    stack.push(match &self.literals[*idx] {
                        Ok(row) => Ok(row.unpack_first()),
                        Err(e) => Err(e.clone()),
                    });
                    pc += 1;
                }
                Instruction::CallUnary(func) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(func.eval_input(temp_storage, input));
                    pc += 1;
                }
                Instruction::CallBinary(func) => {
                    let b = stack.pop().expect("stack underflow");
                    let a = stack.pop().expect("stack underflow");
                    match (a, b) {
                        (Ok(a), Ok(b)) => {
                            // Use static column references to call BinaryFunc::eval
                            // with pre-evaluated datums.
                            stack.push(func.eval(&[a, b], temp_storage, &COL_0, &COL_1));
                        }
                        (Err(e), _) | (_, Err(e)) => stack.push(Err(e)),
                    }
                    pc += 1;
                }
                Instruction::CallUnmaterializable(func) => {
                    stack.push(Err(EvalError::Internal(
                        format!("cannot evaluate unmaterializable function: {func:?}").into(),
                    )));
                    pc += 1;
                }
                Instruction::Skip(offset) => {
                    pc = (pc as isize + offset) as usize;
                }
                Instruction::SkipIfNotTrue {
                    false_offset,
                    error_offset,
                } => {
                    let val = stack.pop().expect("stack underflow");
                    match val {
                        Ok(Datum::True) => pc += 1,
                        Ok(_) => pc = (pc as isize + false_offset) as usize,
                        Err(e) => {
                            // Cond errored: push error and skip entire If
                            // (past both then and else branches).
                            stack.push(Err(e));
                            pc = (pc as isize + error_offset) as usize;
                        }
                    }
                }
                Instruction::AndStep(end_offset) => {
                    let operand = stack.pop().expect("stack underflow");
                    let accumulator = stack.pop().expect("stack underflow");
                    match operand {
                        Ok(Datum::False) => {
                            // False short-circuits And, even over accumulated errors
                            stack.push(Ok(Datum::False));
                            pc = (pc as isize + end_offset) as usize;
                        }
                        Ok(Datum::True) => {
                            // True doesn't change the accumulator
                            stack.push(accumulator);
                            pc += 1;
                        }
                        Ok(Datum::Null) => {
                            // Null: upgrade accumulator (error stays, true→null)
                            match accumulator {
                                Err(_) => stack.push(accumulator),
                                _ => stack.push(Ok(Datum::Null)),
                            }
                            pc += 1;
                        }
                        Err(e) => {
                            // Error: merge with accumulator (max of errors)
                            match accumulator {
                                Err(e2) => stack.push(Err(std::cmp::max(e, e2))),
                                _ => stack.push(Err(e)),
                            }
                            pc += 1;
                        }
                        _ => unreachable!(),
                    }
                }
                Instruction::OrStep(end_offset) => {
                    let operand = stack.pop().expect("stack underflow");
                    let accumulator = stack.pop().expect("stack underflow");
                    match operand {
                        Ok(Datum::True) => {
                            // True short-circuits Or, even over accumulated errors
                            stack.push(Ok(Datum::True));
                            pc = (pc as isize + end_offset) as usize;
                        }
                        Ok(Datum::False) => {
                            // False doesn't change the accumulator
                            stack.push(accumulator);
                            pc += 1;
                        }
                        Ok(Datum::Null) => {
                            match accumulator {
                                Err(_) => stack.push(accumulator),
                                _ => stack.push(Ok(Datum::Null)),
                            }
                            pc += 1;
                        }
                        Err(e) => {
                            match accumulator {
                                Err(e2) => stack.push(Err(std::cmp::max(e, e2))),
                                _ => stack.push(Err(e)),
                            }
                            pc += 1;
                        }
                        _ => unreachable!(),
                    }
                }
                Instruction::SkipIfNotNull(end_offset) => {
                    let value = stack.pop().expect("stack underflow");
                    match value {
                        Ok(Datum::Null) => {
                            // Null: discard and continue to next operand
                            pc += 1;
                        }
                        _ => {
                            // Non-null or error: push back and jump to end
                            stack.push(value);
                            pc = (pc as isize + end_offset) as usize;
                        }
                    }
                }
                Instruction::GreatestStep(end_offset) => {
                    let operand = stack.pop().expect("stack underflow");
                    let accumulator = stack.pop().expect("stack underflow");
                    match operand {
                        Err(e) => {
                            // Errors propagate immediately
                            stack.push(Err(e));
                            pc = (pc as isize + end_offset) as usize;
                        }
                        Ok(d) if d.is_null() => {
                            // Null operand: keep accumulator unchanged
                            stack.push(accumulator);
                            pc += 1;
                        }
                        Ok(d) => {
                            // Non-null: update max (accumulator is always Ok)
                            match &accumulator {
                                Ok(acc) if !acc.is_null() && *acc >= d => {
                                    stack.push(accumulator)
                                }
                                _ => stack.push(Ok(d)),
                            }
                            pc += 1;
                        }
                    }
                }
                Instruction::LeastStep(end_offset) => {
                    let operand = stack.pop().expect("stack underflow");
                    let accumulator = stack.pop().expect("stack underflow");
                    match operand {
                        Err(e) => {
                            stack.push(Err(e));
                            pc = (pc as isize + end_offset) as usize;
                        }
                        Ok(d) if d.is_null() => {
                            stack.push(accumulator);
                            pc += 1;
                        }
                        Ok(d) => {
                            match &accumulator {
                                Ok(acc) if !acc.is_null() && *acc <= d => {
                                    stack.push(accumulator)
                                }
                                _ => stack.push(Ok(d)),
                            }
                            pc += 1;
                        }
                    }
                }
                Instruction::RaiseIfNullError => {
                    let msg = stack.pop().expect("stack underflow");
                    match msg {
                        Err(e) => stack.push(Err(e)),
                        Ok(Datum::Null) => stack.push(Err(EvalError::Internal(
                            "unexpected NULL in error side of error_if_null".into(),
                        ))),
                        Ok(d) => {
                            stack.push(Err(EvalError::IfNullError(d.unwrap_str().into())))
                        }
                    }
                    pc += 1;
                }
                Instruction::CallEagerVariadic { func, arity } => {
                    let start = stack.len() - arity;
                    let args: Result<Vec<Datum<'a>>, EvalError> =
                        stack.drain(start..).collect();
                    match args {
                        Err(e) => stack.push(Err(e)),
                        Ok(ds) => {
                            if func.propagates_nulls() && ds.iter().any(|d| d.is_null()) {
                                stack.push(Ok(Datum::Null));
                            } else {
                                stack.push(func.eval_eager(&ds, temp_storage));
                            }
                        }
                    }
                    pc += 1;
                }
                Instruction::MapListElements(cast_program) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_map_list_elements(input, cast_program, temp_storage));
                    pc += 1;
                }
                Instruction::MapArrayElements(cast_program) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_map_array_elements(input, cast_program, temp_storage));
                    pc += 1;
                }
                Instruction::MapRecord {
                    cast_programs,
                    return_ty: _,
                } => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_map_record(input, cast_programs, temp_storage));
                    pc += 1;
                }
                Instruction::MapListToJsonb(cast_program) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_map_list_to_jsonb(input, cast_program, temp_storage));
                    pc += 1;
                }
                Instruction::MapArrayToJsonb(cast_program) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_map_array_to_jsonb(input, cast_program, temp_storage));
                    pc += 1;
                }
                Instruction::ParseAndCast(kind) => {
                    let input = stack.pop().expect("stack underflow");
                    stack.push(eval_parse_and_cast(input, kind, temp_storage));
                    pc += 1;
                }
            }
        }

        stack.pop().expect("stack should have one result")
    }
}

// ---------------------------------------------------------------------------
// Compound-cast evaluation helpers
// ---------------------------------------------------------------------------

fn eval_map_list_elements<'a>(
    input: Result<Datum<'a>, EvalError>,
    cast_program: &'a CompiledMirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    let cast_datums = a
        .unwrap_list()
        .iter()
        .map(|el| cast_program.eval(&[el], temp_storage))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
}

fn eval_map_array_elements<'a>(
    input: Result<Datum<'a>, EvalError>,
    cast_program: &'a CompiledMirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    let arr = a.unwrap_array();
    let dims: Vec<ArrayDimension> = arr.dims().into_iter().collect();
    let casted = arr
        .elements()
        .iter()
        .map(|el| cast_program.eval(&[el], temp_storage))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(temp_storage.try_make_datum(|packer| packer.try_push_array(&dims, casted))?)
}

fn eval_map_record<'a>(
    input: Result<Datum<'a>, EvalError>,
    cast_programs: &'a [CompiledMirScalarExpr],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    let mut cast_datums = Vec::new();
    for (el, prog) in a.unwrap_list().iter().zip(cast_programs.iter()) {
        cast_datums.push(prog.eval(&[el], temp_storage)?);
    }
    Ok(temp_storage.make_datum(|packer| packer.push_list(cast_datums)))
}

fn eval_map_list_to_jsonb<'a>(
    input: Result<Datum<'a>, EvalError>,
    cast_program: &'a CompiledMirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    let mut row = Row::default();
    row.packer().push_list_with(|packer| {
        for elem in a.unwrap_list().iter() {
            let elem = match cast_program.eval(&[elem], temp_storage)? {
                Datum::Null => Datum::JsonNull,
                d => d,
            };
            packer.push(elem);
        }
        Ok::<_, EvalError>(())
    })?;
    Ok(temp_storage.push_unary_row(row))
}

fn eval_map_array_to_jsonb<'a>(
    input: Result<Datum<'a>, EvalError>,
    cast_program: &'a CompiledMirScalarExpr,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    let arr = a.unwrap_array();
    let elements = arr.elements();
    let dims: Vec<ArrayDimension> = arr.dims().into_iter().collect();
    let mut row = Row::default();
    pack_array_to_jsonb(
        temp_storage,
        &mut elements.into_iter(),
        &dims,
        cast_program,
        &mut row.packer(),
    )?;
    Ok(temp_storage.push_unary_row(row))
}

/// Recursive helper for `MapArrayToJsonb` that mirrors the original
/// `CastArrayToJsonb::pack` function.
fn pack_array_to_jsonb<'a>(
    temp_storage: &RowArena,
    elems: &mut impl Iterator<Item = Datum<'a>>,
    dims: &[ArrayDimension],
    cast_program: &CompiledMirScalarExpr,
    packer: &mut mz_repr::RowPacker,
) -> Result<(), EvalError> {
    packer.push_list_with(|packer| match dims {
        [] => Ok(()),
        [dim] => {
            for _ in 0..dim.length {
                let elem = elems.next().unwrap();
                let elem = match cast_program.eval(&[elem], temp_storage)? {
                    Datum::Null => Datum::JsonNull,
                    d => d,
                };
                packer.push(elem);
            }
            Ok(())
        }
        [dim, rest @ ..] => {
            for _ in 0..dim.length {
                pack_array_to_jsonb(temp_storage, elems, rest, cast_program, packer)?;
            }
            Ok(())
        }
    })
}

// ---------------------------------------------------------------------------
// Parse-and-cast evaluation
// ---------------------------------------------------------------------------

fn eval_parse_and_cast<'a>(
    input: Result<Datum<'a>, EvalError>,
    kind: &'a ParseAndCastKind,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let a = input?;
    if a.is_null() {
        return Ok(Datum::Null);
    }
    match kind {
        ParseAndCastKind::StringToArray { cast_program } => {
            let (datums, dims) = strconv::parse_array(
                a.unwrap_str(),
                || Datum::Null,
                |elem_text| {
                    let elem_text = match elem_text {
                        Cow::Owned(s) => temp_storage.push_string(s),
                        Cow::Borrowed(s) => s,
                    };
                    cast_program.eval(&[Datum::String(elem_text)], temp_storage)
                },
            )?;
            Ok(temp_storage.try_make_datum(|packer| packer.try_push_array(&dims, datums))?)
        }
        ParseAndCastKind::StringToList {
            return_ty,
            cast_program,
        } => {
            let parsed_datums = strconv::parse_list(
                a.unwrap_str(),
                matches!(
                    return_ty.unwrap_list_element_type(),
                    SqlScalarType::List { .. }
                ),
                || Datum::Null,
                |elem_text| {
                    let elem_text = match elem_text {
                        Cow::Owned(s) => temp_storage.push_string(s),
                        Cow::Borrowed(s) => s,
                    };
                    cast_program.eval(&[Datum::String(elem_text)], temp_storage)
                },
            )?;
            Ok(temp_storage.make_datum(|packer| packer.push_list(parsed_datums)))
        }
        ParseAndCastKind::StringToMap {
            return_ty,
            cast_program,
        } => {
            let parsed_map = strconv::parse_map(
                a.unwrap_str(),
                matches!(
                    return_ty.unwrap_map_value_type(),
                    SqlScalarType::Map { .. }
                ),
                |value_text| -> Result<Datum, EvalError> {
                    let value_text = match value_text {
                        Some(Cow::Owned(s)) => Datum::String(temp_storage.push_string(s)),
                        Some(Cow::Borrowed(s)) => Datum::String(s),
                        None => Datum::Null,
                    };
                    cast_program.eval(&[value_text], temp_storage)
                },
            )?;
            let mut pairs: Vec<(String, Datum)> =
                parsed_map.into_iter().map(|(k, v)| (k, v)).collect();
            pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
            pairs.dedup_by(|(k1, _), (k2, _)| k1 == k2);
            Ok(temp_storage.make_datum(|packer| {
                packer.push_dict_with(|packer| {
                    for (k, v) in pairs {
                        packer.push(Datum::String(&k));
                        packer.push(v);
                    }
                })
            }))
        }
        ParseAndCastKind::StringToRange { cast_program } => {
            let mut range = strconv::parse_range(a.unwrap_str(), |elem_text| {
                let elem_text = match elem_text {
                    Cow::Owned(s) => temp_storage.push_string(s),
                    Cow::Borrowed(s) => s,
                };
                cast_program.eval(&[Datum::String(elem_text)], temp_storage)
            })?;
            range.canonicalize()?;
            Ok(temp_storage.make_datum(|packer| {
                packer
                    .push_range(range)
                    .expect("must have already handled errors")
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that compiled evaluation produces the same result as direct evaluation
    /// for basic expressions.
    #[mz_ore::test]
    fn test_column() {
        let arena = RowArena::new();
        let expr = MirScalarExpr::Column(1, TreatAsEqual(None));
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[Datum::Int32(10), Datum::Int32(42)];

        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
        );
    }

    #[mz_ore::test]
    fn test_literal() {
        let arena = RowArena::new();
        let expr = MirScalarExpr::literal_ok(Datum::String("hello"), SqlScalarType::String);
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[];

        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
        );
    }

    #[mz_ore::test]
    fn test_if_true_branch() {
        let arena = RowArena::new();
        // If(Column(0), Literal(1), Literal(2))
        let expr = MirScalarExpr::If {
            cond: Box::new(MirScalarExpr::literal_ok(
                Datum::True,
                SqlScalarType::Bool,
            )),
            then: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(1),
                SqlScalarType::Int32,
            )),
            els: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(2),
                SqlScalarType::Int32,
            )),
        };
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[];

        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
        );
    }

    #[mz_ore::test]
    fn test_if_false_branch() {
        let arena = RowArena::new();
        let expr = MirScalarExpr::If {
            cond: Box::new(MirScalarExpr::literal_ok(
                Datum::False,
                SqlScalarType::Bool,
            )),
            then: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(1),
                SqlScalarType::Int32,
            )),
            els: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(2),
                SqlScalarType::Int32,
            )),
        };
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[];

        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
        );
    }

    #[mz_ore::test]
    fn test_if_null_goes_to_else() {
        let arena = RowArena::new();
        let expr = MirScalarExpr::If {
            cond: Box::new(MirScalarExpr::literal_ok(
                Datum::Null,
                SqlScalarType::Bool,
            )),
            then: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(1),
                SqlScalarType::Int32,
            )),
            els: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(2),
                SqlScalarType::Int32,
            )),
        };
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[];

        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
        );
    }

    /// Helper: assert compiled and direct evaluation produce the same result.
    fn assert_eval_eq(expr: &MirScalarExpr, datums: &[Datum]) {
        let arena = RowArena::new();
        let compiled = CompiledMirScalarExpr::from(expr);
        assert_eq!(
            compiled.eval(datums, &arena),
            expr.eval(datums, &arena),
            "mismatch for expr: {expr:?} with datums: {datums:?}",
        );
    }

    #[mz_ore::test]
    fn test_call_unary_not() {
        use crate::func::Not;
        // NOT(Column(0))
        let expr = MirScalarExpr::column(0).call_unary(Not);
        assert_eval_eq(&expr, &[Datum::True]);
        assert_eval_eq(&expr, &[Datum::False]);
        assert_eval_eq(&expr, &[Datum::Null]);
    }

    #[mz_ore::test]
    fn test_call_unary_is_null() {
        use crate::func::IsNull;
        // IS_NULL(Column(0))
        let expr = MirScalarExpr::column(0).call_unary(IsNull);
        assert_eval_eq(&expr, &[Datum::Null]);
        assert_eval_eq(&expr, &[Datum::Int32(42)]);
        assert_eval_eq(&expr, &[Datum::String("hello")]);
    }

    #[mz_ore::test]
    fn test_call_unary_neg_int32() {
        use crate::func::NegInt32;
        // -Column(0)
        let expr = MirScalarExpr::column(0).call_unary(NegInt32);
        assert_eval_eq(&expr, &[Datum::Int32(42)]);
        assert_eval_eq(&expr, &[Datum::Int32(-1)]);
        assert_eval_eq(&expr, &[Datum::Int32(0)]);
        assert_eval_eq(&expr, &[Datum::Null]);
    }

    #[mz_ore::test]
    fn test_call_unary_nested() {
        use crate::func::Not;
        // NOT(NOT(Column(0)))
        let expr = MirScalarExpr::column(0).call_unary(Not).call_unary(Not);
        assert_eval_eq(&expr, &[Datum::True]);
        assert_eval_eq(&expr, &[Datum::False]);
    }

    #[mz_ore::test]
    fn test_call_binary_add_int32() {
        use crate::func::AddInt32;
        // Column(0) + Column(1)
        let expr = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), AddInt32);
        assert_eval_eq(&expr, &[Datum::Int32(10), Datum::Int32(32)]);
        assert_eval_eq(&expr, &[Datum::Int32(-5), Datum::Int32(5)]);
        assert_eval_eq(&expr, &[Datum::Null, Datum::Int32(1)]);
        assert_eval_eq(&expr, &[Datum::Int32(1), Datum::Null]);
    }

    #[mz_ore::test]
    fn test_call_binary_add_literal() {
        use crate::func::AddInt32;
        // Column(0) + Literal(100)
        let expr = MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal_ok(Datum::Int32(100), SqlScalarType::Int32),
            AddInt32,
        );
        assert_eval_eq(&expr, &[Datum::Int32(1)]);
        assert_eval_eq(&expr, &[Datum::Int32(-50)]);
    }

    #[mz_ore::test]
    fn test_call_binary_overflow() {
        use crate::func::AddInt32;
        // i32::MAX + 1 should produce an error in both paths
        let expr = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), AddInt32);
        let datums = &[Datum::Int32(i32::MAX), Datum::Int32(1)];
        let arena = RowArena::new();
        let compiled = CompiledMirScalarExpr::from(&expr);
        let compiled_result = compiled.eval(datums, &arena);
        let direct_result = expr.eval(datums, &arena);
        assert!(compiled_result.is_err());
        assert!(direct_result.is_err());
    }

    #[mz_ore::test]
    fn test_binary_with_unary() {
        use crate::func::{AddInt32, NegInt32};
        // (-Column(0)) + Column(1)
        let expr = MirScalarExpr::column(0)
            .call_unary(NegInt32)
            .call_binary(MirScalarExpr::column(1), AddInt32);
        assert_eval_eq(&expr, &[Datum::Int32(10), Datum::Int32(3)]);
        assert_eval_eq(&expr, &[Datum::Int32(0), Datum::Int32(7)]);
    }

    #[mz_ore::test]
    fn test_if_error_in_cond() {
        let arena = RowArena::new();
        let expr = MirScalarExpr::If {
            cond: Box::new(MirScalarExpr::literal(
                Err(EvalError::Internal("test error".into())),
                SqlScalarType::Bool,
            )),
            then: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(1),
                SqlScalarType::Int32,
            )),
            els: Box::new(MirScalarExpr::literal_ok(
                Datum::Int32(2),
                SqlScalarType::Int32,
            )),
        };
        let compiled = CompiledMirScalarExpr::from(&expr);
        let datums = &[];

        // Both should return the error
        let compiled_result = compiled.eval(datums, &arena);
        let direct_result = expr.eval(datums, &arena);
        assert!(compiled_result.is_err());
        assert!(direct_result.is_err());
    }

    // -- Variadic tests --

    /// Helper to make a variadic expression.
    fn variadic(func: VariadicFunc, exprs: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::CallVariadic { func, exprs }
    }

    fn lit_bool(b: bool) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::from(b), SqlScalarType::Bool)
    }

    fn lit_null_bool() -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Null, SqlScalarType::Bool)
    }

    fn lit_i32(v: i32) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int32(v), SqlScalarType::Int32)
    }

    fn lit_null_i32() -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Null, SqlScalarType::Int32)
    }

    fn lit_err() -> MirScalarExpr {
        MirScalarExpr::literal(
            Err(EvalError::Internal("test error".into())),
            SqlScalarType::Bool,
        )
    }

    #[mz_ore::test]
    fn test_and_basic() {
        // And(true, true) = true
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(true), lit_bool(true)]), &[]);
        // And(true, false) = false
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(true), lit_bool(false)]), &[]);
        // And(false, true) = false
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(false), lit_bool(true)]), &[]);
        // And(false, false) = false
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(false), lit_bool(false)]), &[]);
    }

    #[mz_ore::test]
    fn test_and_null() {
        // And(true, null) = null
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(true), lit_null_bool()]), &[]);
        // And(null, true) = null
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_null_bool(), lit_bool(true)]), &[]);
        // And(null, false) = false (false short-circuits)
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_null_bool(), lit_bool(false)]), &[]);
        // And(false, null) = false
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_bool(false), lit_null_bool()]), &[]);
    }

    #[mz_ore::test]
    fn test_and_error() {
        // And(error, false) = false (false short-circuits over errors)
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_err(), lit_bool(false)]), &[]);
        // And(error, true) = error
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_err(), lit_bool(true)]), &[]);
        // And(error, null) = error (error > null)
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_err(), lit_null_bool()]), &[]);
        // And(null, error) = error
        assert_eval_eq(&variadic(VariadicFunc::And, vec![lit_null_bool(), lit_err()]), &[]);
    }

    #[mz_ore::test]
    fn test_and_empty() {
        // And() = true (identity element)
        assert_eval_eq(&variadic(VariadicFunc::And, vec![]), &[]);
    }

    #[mz_ore::test]
    fn test_or_basic() {
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(false), lit_bool(false)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(false), lit_bool(true)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(true), lit_bool(false)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(true), lit_bool(true)]), &[]);
    }

    #[mz_ore::test]
    fn test_or_null() {
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(false), lit_null_bool()]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_null_bool(), lit_bool(false)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_null_bool(), lit_bool(true)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_bool(true), lit_null_bool()]), &[]);
    }

    #[mz_ore::test]
    fn test_or_error() {
        // Or(error, true) = true (true short-circuits over errors)
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_err(), lit_bool(true)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_err(), lit_bool(false)]), &[]);
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![lit_err(), lit_null_bool()]), &[]);
    }

    #[mz_ore::test]
    fn test_or_empty() {
        // Or() = false (identity element)
        assert_eval_eq(&variadic(VariadicFunc::Or, vec![]), &[]);
    }

    #[mz_ore::test]
    fn test_coalesce() {
        // Coalesce(null, null, 42) = 42
        assert_eval_eq(
            &variadic(VariadicFunc::Coalesce, vec![lit_null_i32(), lit_null_i32(), lit_i32(42)]),
            &[],
        );
        // Coalesce(1, 2) = 1
        assert_eval_eq(
            &variadic(VariadicFunc::Coalesce, vec![lit_i32(1), lit_i32(2)]),
            &[],
        );
        // Coalesce(null) = null
        assert_eval_eq(
            &variadic(VariadicFunc::Coalesce, vec![lit_null_i32()]),
            &[],
        );
        // Coalesce() = null
        assert_eval_eq(&variadic(VariadicFunc::Coalesce, vec![]), &[]);
    }

    #[mz_ore::test]
    fn test_coalesce_with_columns() {
        // Coalesce(Column(0), Column(1))
        let expr = variadic(
            VariadicFunc::Coalesce,
            vec![MirScalarExpr::column(0), MirScalarExpr::column(1)],
        );
        assert_eval_eq(&expr, &[Datum::Null, Datum::Int32(5)]);
        assert_eval_eq(&expr, &[Datum::Int32(3), Datum::Int32(5)]);
        assert_eval_eq(&expr, &[Datum::Null, Datum::Null]);
    }

    #[mz_ore::test]
    fn test_error_if_null_non_null() {
        // ErrorIfNull(42, "oops") = 42
        let expr = variadic(
            VariadicFunc::ErrorIfNull,
            vec![
                lit_i32(42),
                MirScalarExpr::literal_ok(Datum::String("oops"), SqlScalarType::String),
            ],
        );
        assert_eval_eq(&expr, &[]);
    }

    #[mz_ore::test]
    fn test_error_if_null_null() {
        // ErrorIfNull(null, "oops") = error
        let expr = variadic(
            VariadicFunc::ErrorIfNull,
            vec![
                lit_null_i32(),
                MirScalarExpr::literal_ok(Datum::String("oops"), SqlScalarType::String),
            ],
        );
        assert_eval_eq(&expr, &[]);
    }

    #[mz_ore::test]
    fn test_greatest() {
        assert_eval_eq(
            &variadic(VariadicFunc::Greatest, vec![lit_i32(1), lit_i32(3), lit_i32(2)]),
            &[],
        );
        assert_eval_eq(
            &variadic(VariadicFunc::Greatest, vec![lit_i32(5), lit_null_i32(), lit_i32(3)]),
            &[],
        );
        assert_eval_eq(
            &variadic(VariadicFunc::Greatest, vec![lit_null_i32(), lit_null_i32()]),
            &[],
        );
        assert_eval_eq(&variadic(VariadicFunc::Greatest, vec![]), &[]);
    }

    #[mz_ore::test]
    fn test_least() {
        assert_eval_eq(
            &variadic(VariadicFunc::Least, vec![lit_i32(3), lit_i32(1), lit_i32(2)]),
            &[],
        );
        assert_eval_eq(
            &variadic(VariadicFunc::Least, vec![lit_i32(5), lit_null_i32(), lit_i32(3)]),
            &[],
        );
        assert_eval_eq(
            &variadic(VariadicFunc::Least, vec![lit_null_i32(), lit_null_i32()]),
            &[],
        );
        assert_eval_eq(&variadic(VariadicFunc::Least, vec![]), &[]);
    }

    #[mz_ore::test]
    fn test_eager_variadic_array_create() {
        // ArrayCreate([1, 2, 3])
        let expr = MirScalarExpr::CallVariadic {
            func: VariadicFunc::ArrayCreate {
                elem_type: SqlScalarType::Int32,
            },
            exprs: vec![lit_i32(1), lit_i32(2), lit_i32(3)],
        };
        assert_eval_eq(&expr, &[]);
    }

    #[mz_ore::test]
    fn test_eager_variadic_list_create() {
        // ListCreate([1, null, 3])
        let expr = MirScalarExpr::CallVariadic {
            func: VariadicFunc::ListCreate {
                elem_type: SqlScalarType::Int32,
            },
            exprs: vec![lit_i32(1), lit_null_i32(), lit_i32(3)],
        };
        assert_eval_eq(&expr, &[]);
    }

    #[mz_ore::test]
    fn test_and_three_operands() {
        // And(true, true, true) = true
        assert_eval_eq(
            &variadic(VariadicFunc::And, vec![lit_bool(true), lit_bool(true), lit_bool(true)]),
            &[],
        );
        // And(true, null, false) = false
        assert_eval_eq(
            &variadic(VariadicFunc::And, vec![lit_bool(true), lit_null_bool(), lit_bool(false)]),
            &[],
        );
        // And(true, null, true) = null
        assert_eval_eq(
            &variadic(VariadicFunc::And, vec![lit_bool(true), lit_null_bool(), lit_bool(true)]),
            &[],
        );
    }
}
