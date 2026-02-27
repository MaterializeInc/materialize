// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WASM code generation for compiled scalar expressions.
//!
//! Uses `wasm-encoder` to produce a WASM module that evaluates a compiled expression
//! on columnar data in linear memory. The generated module imports a host-provided memory
//! and exports an `eval` function that loops over rows, reading input columns, computing
//! the expression, and writing the result column.

use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc};
use mz_repr::Datum;
use wasm_encoder::{
    CodeSection, EntityType, ExportKind, ExportSection, FunctionSection, ImportSection,
    Instruction, MemoryType, Module, TypeSection, ValType,
};

/// Metadata about the generated WASM module's memory layout expectations.
#[derive(Debug, Clone)]
pub struct WasmLayout {
    /// Number of input columns the eval function expects.
    pub num_input_cols: usize,
}

/// Generates a WASM module that evaluates the given expression on columnar Int64 data.
///
/// The generated module:
/// * Imports: `memory` from `"env"` (host-provided linear memory)
/// * Exports: `eval(num_rows: i32, col0_ptr: i32, col0_valid_ptr: i32, ...,
///            out_ptr: i32, out_valid_ptr: i32, err_ptr: i32)`
///
/// Memory layout for each column:
/// * Values: contiguous array of i64 (8 bytes per row)
/// * Validity: contiguous array of bytes (1 byte per row, 0=null, 1=valid)
/// * Errors: contiguous array of bytes (1 byte per row, 0=ok, 1=error)
///
/// Returns `None` if the expression is not compilable.
pub fn generate_wasm(
    expr: &MirScalarExpr,
    input_types: &[mz_repr::SqlScalarType],
) -> Option<(Vec<u8>, WasmLayout)> {
    if !crate::analyze::is_compilable(expr, input_types) {
        return None;
    }

    let input_cols = crate::analyze::referenced_columns(expr);
    let num_input_cols = input_cols.len();

    // Build a mapping from original column index to parameter position.
    let col_to_param: std::collections::BTreeMap<usize, usize> = input_cols
        .iter()
        .enumerate()
        .map(|(i, &c)| (c, i))
        .collect();

    let mut module = Module::new();

    // Type section: eval function signature.
    // Parameters: num_rows, then for each input col: (values_ptr, validity_ptr),
    //             then: out_ptr, out_validity_ptr, err_ptr.
    let num_params = 1 + num_input_cols * 2 + 3;
    let params: Vec<ValType> = vec![ValType::I32; num_params];
    let mut types = TypeSection::new();
    types.ty().function(params, vec![]);
    module.section(&types);

    // Import section: memory from "env".
    let mut imports = ImportSection::new();
    imports.import(
        "env",
        "memory",
        EntityType::Memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        }),
    );
    module.section(&imports);

    // Function section: one function (index 0).
    let mut functions = FunctionSection::new();
    functions.function(0); // type index 0
    module.section(&functions);

    // Export section: export the eval function.
    let mut exports = ExportSection::new();
    exports.export("eval", ExportKind::Func, 0);
    module.section(&exports);

    // Code section: the eval function body.
    let mut codes = CodeSection::new();
    let mut func = wasm_encoder::Function::new(vec![
        // Local 0..num_params-1: function params (implicit)
        // Additional locals for the loop:
        (1, ValType::I32), // loop counter `i`
        (1, ValType::I64), // temp result value
        (1, ValType::I32), // temp null flag (0=not_null, 1=null)
        (1, ValType::I32), // temp error flag (0=ok, 1=overflow, 2=div-by-zero, 3=out-of-range)
        (1, ValType::I64), // temp operand a (for overflow detection)
        (1, ValType::I64), // temp operand b (for overflow detection)
        (1, ValType::I32), // saved null flag (for restoring is_null across subtrees)
        (1, ValType::I32), // any_dominant flag (for And/Or three-valued logic)
        (1, ValType::I32), // any_null flag (for And/Or three-valued logic)
        (1, ValType::I32), // saved caller is_null for And/Or (survives nested And/Or)
    ]);

    // Local variable indices:
    // 0: num_rows
    // 1..(1+num_input_cols*2-1): col0_ptr, col0_valid_ptr, col1_ptr, col1_valid_ptr, ...
    // (1+num_input_cols*2): out_ptr
    // (1+num_input_cols*2+1): out_valid_ptr
    // (1+num_input_cols*2+2): err_ptr
    // (num_params): i (loop counter)
    // (num_params+1): result_val (i64)
    // (num_params+2): is_null (i32)
    // (num_params+3): is_error (i32)
    // (num_params+4): operand_a (i64)
    // (num_params+5): operand_b (i64)
    // (num_params+6): saved_null (i32)
    // (num_params+7): any_dominant (i32) — for And/Or
    // (num_params+8): any_null (i32) — for And/Or
    // (num_params+9): saved_caller_null (i32) — for And/Or nesting
    #[allow(clippy::as_conversions)]
    let local_i = num_params as u32;
    let local_result = local_i + 1;
    let local_is_null = local_i + 2;
    let local_is_error = local_i + 3;
    let local_a = local_i + 4;
    let local_b = local_i + 5;
    let local_saved_null = local_i + 6;
    let local_any_dominant = local_i + 7;
    let local_any_null = local_i + 8;
    let local_saved_caller_null = local_i + 9;

    #[allow(clippy::as_conversions)]
    let out_ptr_local = (1 + num_input_cols * 2) as u32;
    let out_valid_local = out_ptr_local + 1;
    let err_ptr_local = out_ptr_local + 2;

    // Initialize loop counter to 0.
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(local_i));

    // Block/Loop structure for the row loop.
    func.instruction(&Instruction::Block(wasm_encoder::BlockType::Empty));
    func.instruction(&Instruction::Loop(wasm_encoder::BlockType::Empty));

    // Break if i >= num_rows.
    func.instruction(&Instruction::LocalGet(local_i));
    func.instruction(&Instruction::LocalGet(0)); // num_rows
    func.instruction(&Instruction::I32GeU);
    func.instruction(&Instruction::BrIf(1)); // break outer block

    // Reset per-row state.
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::LocalSet(local_result));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(local_is_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(local_is_error));

    // Emit the expression evaluation. This pushes the result i64 on the WASM stack
    // and sets local_is_null / local_is_error as side effects.
    emit_expr(
        &mut func,
        expr,
        &col_to_param,
        local_i,
        local_result,
        local_is_null,
        local_is_error,
        local_a,
        local_b,
        local_saved_null,
        local_any_dominant,
        local_any_null,
        local_saved_caller_null,
    );
    func.instruction(&Instruction::LocalSet(local_result));

    // Store output value: mem[out_ptr + i*8] = result_val.
    func.instruction(&Instruction::LocalGet(out_ptr_local));
    func.instruction(&Instruction::LocalGet(local_i));
    func.instruction(&Instruction::I32Const(8));
    func.instruction(&Instruction::I32Mul);
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::LocalGet(local_result));
    func.instruction(&Instruction::I64Store(wasm_encoder::MemArg {
        offset: 0,
        align: 3, // 2^3 = 8 byte alignment
        memory_index: 0,
    }));

    // Store output validity: mem[out_valid_ptr + i] = is_null ? 0 : 1.
    func.instruction(&Instruction::LocalGet(out_valid_local));
    func.instruction(&Instruction::LocalGet(local_i));
    func.instruction(&Instruction::I32Add);
    // value = 1 - is_null (valid=1 when not null, valid=0 when null)
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::LocalGet(local_is_null));
    func.instruction(&Instruction::I32Sub);
    func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
        offset: 0,
        align: 0,
        memory_index: 0,
    }));

    // Store error flag: mem[err_ptr + i] = is_error.
    func.instruction(&Instruction::LocalGet(err_ptr_local));
    func.instruction(&Instruction::LocalGet(local_i));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::LocalGet(local_is_error));
    func.instruction(&Instruction::I32Store8(wasm_encoder::MemArg {
        offset: 0,
        align: 0,
        memory_index: 0,
    }));

    // Increment loop counter and continue.
    func.instruction(&Instruction::LocalGet(local_i));
    func.instruction(&Instruction::I32Const(1));
    func.instruction(&Instruction::I32Add);
    func.instruction(&Instruction::LocalSet(local_i));
    func.instruction(&Instruction::Br(0)); // continue loop

    func.instruction(&Instruction::End); // end loop
    func.instruction(&Instruction::End); // end block
    func.instruction(&Instruction::End); // end function

    codes.function(&func);
    module.section(&codes);

    let wasm_bytes = module.finish();
    Some((wasm_bytes, WasmLayout { num_input_cols }))
}

/// Common parameters for all emit functions, reducing argument count.
struct EmitLocals {
    local_i: u32,
    local_result: u32,
    local_is_null: u32,
    local_is_error: u32,
    local_a: u32,
    local_b: u32,
    local_saved_null: u32,
    local_any_dominant: u32,
    local_any_null: u32,
    local_saved_caller_null: u32,
}

/// Emits WASM instructions that evaluate `expr` and leave an i64 result on the stack.
///
/// Side effects: sets `local_is_null` to 1 if the result is null,
/// sets `local_is_error` to a nonzero code if an error occurred.
fn emit_expr(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    local_i: u32,
    local_result: u32,
    local_is_null: u32,
    local_is_error: u32,
    local_a: u32,
    local_b: u32,
    local_saved_null: u32,
    local_any_dominant: u32,
    local_any_null: u32,
    local_saved_caller_null: u32,
) {
    let l = EmitLocals {
        local_i,
        local_result,
        local_is_null,
        local_is_error,
        local_a,
        local_b,
        local_saved_null,
        local_any_dominant,
        local_any_null,
        local_saved_caller_null,
    };
    match expr {
        MirScalarExpr::Column(idx, _) => {
            let param_idx = col_to_param[idx];
            // Values pointer is at param position: 1 + param_idx * 2
            // Validity pointer is at: 1 + param_idx * 2 + 1
            #[allow(clippy::as_conversions)]
            let values_local = (1 + param_idx * 2) as u32;
            let validity_local = values_local + 1;

            // Check validity: if mem[validity_ptr + i] == 0, this is null.
            func.instruction(&Instruction::LocalGet(validity_local));
            func.instruction(&Instruction::LocalGet(l.local_i));
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I32Load8U(wasm_encoder::MemArg {
                offset: 0,
                align: 0,
                memory_index: 0,
            }));
            func.instruction(&Instruction::I32Eqz);
            func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            // Null path: set is_null = 1.
            func.instruction(&Instruction::I32Const(1));
            func.instruction(&Instruction::LocalSet(l.local_is_null));
            func.instruction(&Instruction::End);

            // Load value: mem[values_ptr + i * 8] as i64.
            func.instruction(&Instruction::LocalGet(values_local));
            func.instruction(&Instruction::LocalGet(l.local_i));
            func.instruction(&Instruction::I32Const(8));
            func.instruction(&Instruction::I32Mul);
            func.instruction(&Instruction::I32Add);
            func.instruction(&Instruction::I64Load(wasm_encoder::MemArg {
                offset: 0,
                align: 3,
                memory_index: 0,
            }));
        }

        MirScalarExpr::Literal(Ok(row), _) => {
            let datum = row.unpack_first();
            match datum {
                Datum::Int64(v) => {
                    func.instruction(&Instruction::I64Const(v));
                }
                Datum::Null => {
                    func.instruction(&Instruction::I32Const(1));
                    func.instruction(&Instruction::LocalSet(l.local_is_null));
                    func.instruction(&Instruction::I64Const(0));
                }
                Datum::True => {
                    func.instruction(&Instruction::I64Const(1));
                }
                Datum::False => {
                    func.instruction(&Instruction::I64Const(0));
                }
                _ => unreachable!("analyze ensures only Int64/Null/True/False literals"),
            }
        }

        MirScalarExpr::CallBinary {
            func: bf,
            expr1,
            expr2,
        } => match bf {
            BinaryFunc::AddInt64(_) => emit_add_int64(func, expr1, expr2, col_to_param, &l),
            BinaryFunc::SubInt64(_) => emit_sub_int64(func, expr1, expr2, col_to_param, &l),
            BinaryFunc::MulInt64(_) => emit_mul_int64(func, expr1, expr2, col_to_param, &l),
            BinaryFunc::DivInt64(_) => emit_div_int64(func, expr1, expr2, col_to_param, &l),
            BinaryFunc::ModInt64(_) => emit_mod_int64(func, expr1, expr2, col_to_param, &l),
            BinaryFunc::BitAndInt64(_) => {
                emit_bitwise_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64And)
            }
            BinaryFunc::BitOrInt64(_) => {
                emit_bitwise_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64Or)
            }
            BinaryFunc::BitXorInt64(_) => {
                emit_bitwise_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64Xor)
            }
            BinaryFunc::Eq(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64Eq)
            }
            BinaryFunc::NotEq(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64Ne)
            }
            BinaryFunc::Lt(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64LtS)
            }
            BinaryFunc::Lte(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64LeS)
            }
            BinaryFunc::Gt(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64GtS)
            }
            BinaryFunc::Gte(_) => {
                emit_comparison_int64(func, expr1, expr2, col_to_param, &l, &Instruction::I64GeS)
            }
            _ => unreachable!("analyze ensures only supported binary functions"),
        },

        MirScalarExpr::CallUnary {
            func: uf,
            expr: child,
        } => match uf {
            UnaryFunc::NegInt64(_) => emit_neg_int64(func, child, col_to_param, &l),
            UnaryFunc::BitNotInt64(_) => emit_bit_not_int64(func, child, col_to_param, &l),
            UnaryFunc::AbsInt64(_) => emit_abs_int64(func, child, col_to_param, &l),
            UnaryFunc::Not(_) => emit_not(func, child, col_to_param, &l),
            UnaryFunc::IsNull(_) => emit_is_null(func, child, col_to_param, &l),
            UnaryFunc::IsTrue(_) => emit_is_true(func, child, col_to_param, &l),
            UnaryFunc::IsFalse(_) => emit_is_false(func, child, col_to_param, &l),
            _ => unreachable!("analyze ensures only supported unary functions"),
        },

        MirScalarExpr::CallVariadic { func: vf, exprs } => match vf {
            VariadicFunc::And(_) => emit_and(func, exprs, col_to_param, &l),
            VariadicFunc::Or(_) => emit_or(func, exprs, col_to_param, &l),
            _ => unreachable!("analyze ensures only supported variadic functions"),
        },

        _ => unreachable!("analyze ensures only compilable expressions"),
    }
}

/// Emits the save/restore preamble for a fallible binary operation.
///
/// Pushes the current `is_null` onto the WASM value stack, resets `is_null` to 0,
/// evaluates both children (leaving `[saved_null(i32), a(i64), b(i64)]` on the stack),
/// then pops children into `local_a`/`local_b` and the saved null into `local_saved_null`.
///
/// After this, `is_null` reflects only the current operation's children's null status,
/// isolated from ancestors' or siblings' null states. The caller must merge `local_saved_null`
/// back into `is_null` after its error check.
fn emit_binary_preamble(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    // Save current is_null onto the WASM value stack and reset.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));
    // Stack: [saved_null(i32)]

    emit_expr(
        func,
        expr1,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [saved_null(i32), a(i64)]
    emit_expr(
        func,
        expr2,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [saved_null(i32), a(i64), b(i64)]

    func.instruction(&Instruction::LocalSet(l.local_b));
    func.instruction(&Instruction::LocalSet(l.local_a));
    // Stack: [saved_null(i32)]
    func.instruction(&Instruction::LocalSet(l.local_saved_null));
    // Stack: []
}

/// Emits the merge postamble: `is_null |= local_saved_null`, then pushes `local_result`.
fn emit_merge_null_and_push_result(func: &mut wasm_encoder::Function, l: &EmitLocals) {
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalGet(l.local_saved_null));
    func.instruction(&Instruction::I32Or);
    func.instruction(&Instruction::LocalSet(l.local_is_null));
    func.instruction(&Instruction::LocalGet(l.local_result));
}

/// Emits the save/restore preamble for a fallible unary operation.
///
/// Same principle as [`emit_binary_preamble`] but for a single child.
fn emit_unary_preamble(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    // Save current is_null onto the WASM value stack and reset.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));
    // Stack: [saved_null(i32)]

    emit_expr(
        func,
        expr,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [saved_null(i32), a(i64)]

    func.instruction(&Instruction::LocalSet(l.local_a));
    // Stack: [saved_null(i32)]
    func.instruction(&Instruction::LocalSet(l.local_saved_null));
    // Stack: []
}

/// Emits WASM for `a + b` with overflow detection and null propagation.
///
/// Overflow: `((a ^ result) & (b ^ result)) < 0` (sign bit set).
fn emit_add_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_binary_preamble(func, expr1, expr2, col_to_param, l);

    // Wrapping add: WASM i64.add wraps on overflow.
    func.instruction(&Instruction::LocalGet(l.local_a));
    func.instruction(&Instruction::LocalGet(l.local_b));
    func.instruction(&Instruction::I64Add);
    func.instruction(&Instruction::LocalSet(l.local_result));

    // Overflow detection: skip when is_null (operand values may be garbage).
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::LocalGet(l.local_result));
        func.instruction(&Instruction::I64Xor); // a ^ result
        func.instruction(&Instruction::LocalGet(l.local_b));
        func.instruction(&Instruction::LocalGet(l.local_result));
        func.instruction(&Instruction::I64Xor); // b ^ result
        func.instruction(&Instruction::I64And); // (a ^ result) & (b ^ result)
        func.instruction(&Instruction::I64Const(0));
        func.instruction(&Instruction::I64LtS); // true if sign bit set => overflow
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(1)); // NumericFieldOverflow
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::End);

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for `a - b` with overflow detection and null propagation.
///
/// Overflow: `((a ^ b) & (a ^ result)) < 0`.
fn emit_sub_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_binary_preamble(func, expr1, expr2, col_to_param, l);

    func.instruction(&Instruction::LocalGet(l.local_a));
    func.instruction(&Instruction::LocalGet(l.local_b));
    func.instruction(&Instruction::I64Sub);
    func.instruction(&Instruction::LocalSet(l.local_result));

    // Overflow: ((a ^ b) & (a ^ result)) < 0
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::LocalGet(l.local_b));
        func.instruction(&Instruction::I64Xor); // a ^ b
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::LocalGet(l.local_result));
        func.instruction(&Instruction::I64Xor); // a ^ result
        func.instruction(&Instruction::I64And); // (a ^ b) & (a ^ result)
        func.instruction(&Instruction::I64Const(0));
        func.instruction(&Instruction::I64LtS);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(1)); // NumericFieldOverflow
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::End);

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for `a * b` with overflow detection and null propagation.
///
/// Overflow detection via division-based verification:
/// * If a == 0 || b == 0: no overflow
/// * If a == -1: overflow iff b == i64::MIN
/// * If b == -1: overflow iff a == i64::MIN
/// * Else: overflow iff (result / a) != b
fn emit_mul_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_binary_preamble(func, expr1, expr2, col_to_param, l);

    func.instruction(&Instruction::LocalGet(l.local_a));
    func.instruction(&Instruction::LocalGet(l.local_b));
    func.instruction(&Instruction::I64Mul);
    func.instruction(&Instruction::LocalSet(l.local_result));

    // Overflow check (skip if null).
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        // if a == 0: no overflow (skip)
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::I64Eqz);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        // a == 0: no overflow, do nothing
        func.instruction(&Instruction::Else);
        {
            // if b == 0: no overflow (skip)
            func.instruction(&Instruction::LocalGet(l.local_b));
            func.instruction(&Instruction::I64Eqz);
            func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            // b == 0: no overflow, do nothing
            func.instruction(&Instruction::Else);
            {
                // if a == -1: overflow iff b == MIN
                func.instruction(&Instruction::LocalGet(l.local_a));
                func.instruction(&Instruction::I64Const(-1));
                func.instruction(&Instruction::I64Eq);
                func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
                {
                    func.instruction(&Instruction::LocalGet(l.local_b));
                    func.instruction(&Instruction::I64Const(i64::MIN));
                    func.instruction(&Instruction::I64Eq);
                    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
                    {
                        func.instruction(&Instruction::I32Const(1)); // NumericFieldOverflow
                        func.instruction(&Instruction::LocalSet(l.local_is_error));
                    }
                    func.instruction(&Instruction::End);
                }
                func.instruction(&Instruction::Else);
                {
                    // if b == -1: overflow iff a == MIN
                    func.instruction(&Instruction::LocalGet(l.local_b));
                    func.instruction(&Instruction::I64Const(-1));
                    func.instruction(&Instruction::I64Eq);
                    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
                    {
                        func.instruction(&Instruction::LocalGet(l.local_a));
                        func.instruction(&Instruction::I64Const(i64::MIN));
                        func.instruction(&Instruction::I64Eq);
                        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
                        {
                            func.instruction(&Instruction::I32Const(1)); // NumericFieldOverflow
                            func.instruction(&Instruction::LocalSet(l.local_is_error));
                        }
                        func.instruction(&Instruction::End);
                    }
                    func.instruction(&Instruction::Else);
                    {
                        // General case: overflow iff (result / a) != b
                        // Safe because a != 0 and a != -1.
                        func.instruction(&Instruction::LocalGet(l.local_result));
                        func.instruction(&Instruction::LocalGet(l.local_a));
                        func.instruction(&Instruction::I64DivS);
                        func.instruction(&Instruction::LocalGet(l.local_b));
                        func.instruction(&Instruction::I64Ne);
                        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
                        {
                            func.instruction(&Instruction::I32Const(1)); // NumericFieldOverflow
                            func.instruction(&Instruction::LocalSet(l.local_is_error));
                        }
                        func.instruction(&Instruction::End);
                    }
                    func.instruction(&Instruction::End); // end b == -1
                }
                func.instruction(&Instruction::End); // end a == -1
            }
            func.instruction(&Instruction::End); // end b == 0
        }
        func.instruction(&Instruction::End); // end a == 0
    }
    func.instruction(&Instruction::End); // end !is_null

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for `a / b` with division-by-zero and overflow detection.
///
/// Pre-checks before `i64.div_s` (which traps on div-by-zero and MIN/-1):
/// * b == 0 → error code 2 (DivisionByZero)
/// * a == MIN && b == -1 → error code 3 (Int64OutOfRange)
fn emit_div_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_binary_preamble(func, expr1, expr2, col_to_param, l);

    // Skip error checks if null.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        // Check b == 0.
        func.instruction(&Instruction::LocalGet(l.local_b));
        func.instruction(&Instruction::I64Eqz);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(2)); // DivisionByZero
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::Else);
        {
            // Check a == MIN && b == -1.
            func.instruction(&Instruction::LocalGet(l.local_a));
            func.instruction(&Instruction::I64Const(i64::MIN));
            func.instruction(&Instruction::I64Eq);
            func.instruction(&Instruction::LocalGet(l.local_b));
            func.instruction(&Instruction::I64Const(-1));
            func.instruction(&Instruction::I64Eq);
            func.instruction(&Instruction::I32And);
            func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            {
                func.instruction(&Instruction::I32Const(3)); // Int64OutOfRange
                func.instruction(&Instruction::LocalSet(l.local_is_error));
            }
            func.instruction(&Instruction::Else);
            {
                // Safe to divide.
                func.instruction(&Instruction::LocalGet(l.local_a));
                func.instruction(&Instruction::LocalGet(l.local_b));
                func.instruction(&Instruction::I64DivS);
                func.instruction(&Instruction::LocalSet(l.local_result));
            }
            func.instruction(&Instruction::End); // end MIN/-1 check
        }
        func.instruction(&Instruction::End); // end b == 0 check
    }
    func.instruction(&Instruction::End); // end !is_null

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for `a % b` with division-by-zero detection.
///
/// Pre-check (`i64.rem_s` traps on div-by-zero; MIN%-1 returns 0 per WASM spec):
/// * b == 0 → error code 2 (DivisionByZero)
/// * Otherwise: `i64.rem_s` (safe, including MIN % -1 = 0)
fn emit_mod_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_binary_preamble(func, expr1, expr2, col_to_param, l);

    // Skip if null.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        // Check b == 0.
        func.instruction(&Instruction::LocalGet(l.local_b));
        func.instruction(&Instruction::I64Eqz);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(2)); // DivisionByZero
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::Else);
        {
            // Safe: MIN % -1 = 0 in WASM's i64.rem_s.
            func.instruction(&Instruction::LocalGet(l.local_a));
            func.instruction(&Instruction::LocalGet(l.local_b));
            func.instruction(&Instruction::I64RemS);
            func.instruction(&Instruction::LocalSet(l.local_result));
        }
        func.instruction(&Instruction::End); // end b == 0 check
    }
    func.instruction(&Instruction::End); // end !is_null

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for an infallible bitwise binary operation (and, or, xor).
///
/// No overflow or error checks needed.
fn emit_bitwise_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
    op: &Instruction<'static>,
) {
    emit_expr(
        func,
        expr1,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    emit_expr(
        func,
        expr2,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [a, b]
    func.instruction(op);
    // Stack: [result]
}

/// Emits WASM for an infallible comparison of two Int64 operands.
///
/// The `cmp_instruction` should be one of the WASM i64 comparison instructions
/// (I64Eq, I64Ne, I64LtS, I64LeS, I64GtS, I64GeS). The result is an i32 (0 or 1),
/// which is widened to i64 via `i64.extend_i32_u` to maintain the stack convention.
fn emit_comparison_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
    cmp_instruction: &Instruction<'static>,
) {
    emit_expr(
        func,
        expr1,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    emit_expr(
        func,
        expr2,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [a(i64), b(i64)]
    func.instruction(cmp_instruction);
    // Stack: [result(i32)]  — WASM comparisons return i32
    func.instruction(&Instruction::I64ExtendI32U);
    // Stack: [result(i64)]  — widened to i64 for stack convention
}

/// Emits WASM for `NOT a` (boolean negation). Infallible.
///
/// Uses `i64.eqz` (returns 1 if input is 0, else 0) followed by `i64.extend_i32_u`.
fn emit_not(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_expr(
        func,
        expr,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [a(i64)]
    func.instruction(&Instruction::I64Eqz);
    // Stack: [result(i32)]  — 0→1, nonzero→0
    func.instruction(&Instruction::I64ExtendI32U);
    // Stack: [result(i64)]
}

/// Emits WASM for `-a` (negation) with overflow detection.
///
/// `i64::MIN` cannot be negated: error code 3 (Int64OutOfRange).
/// WASM has no `i64.neg`, so we use `0 - a`.
fn emit_neg_int64(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_unary_preamble(func, expr, col_to_param, l);

    // Skip if null.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        // Check a == MIN.
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::I64Const(i64::MIN));
        func.instruction(&Instruction::I64Eq);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(3)); // Int64OutOfRange
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::Else);
        {
            // 0 - a
            func.instruction(&Instruction::I64Const(0));
            func.instruction(&Instruction::LocalGet(l.local_a));
            func.instruction(&Instruction::I64Sub);
            func.instruction(&Instruction::LocalSet(l.local_result));
        }
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::End);

    emit_merge_null_and_push_result(func, l);
}

/// Emits WASM for `~a` (bitwise NOT). Infallible.
///
/// WASM has no `i64.not`, so we use `a ^ -1`.
fn emit_bit_not_int64(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_expr(
        func,
        expr,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
    // Stack: [a]
    func.instruction(&Instruction::I64Const(-1));
    func.instruction(&Instruction::I64Xor);
    // Stack: [~a]
}

/// Emits WASM for `abs(a)` with overflow detection.
///
/// `i64::MIN` has no positive counterpart: error code 3 (Int64OutOfRange).
/// For negative values, computes `0 - a`.
fn emit_abs_int64(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_unary_preamble(func, expr, col_to_param, l);

    // Skip if null.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz);
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        // Check a == MIN.
        func.instruction(&Instruction::LocalGet(l.local_a));
        func.instruction(&Instruction::I64Const(i64::MIN));
        func.instruction(&Instruction::I64Eq);
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            func.instruction(&Instruction::I32Const(3)); // Int64OutOfRange
            func.instruction(&Instruction::LocalSet(l.local_is_error));
        }
        func.instruction(&Instruction::Else);
        {
            // if a < 0: result = 0 - a; else: result = a
            func.instruction(&Instruction::LocalGet(l.local_a));
            func.instruction(&Instruction::I64Const(0));
            func.instruction(&Instruction::I64LtS);
            func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            {
                func.instruction(&Instruction::I64Const(0));
                func.instruction(&Instruction::LocalGet(l.local_a));
                func.instruction(&Instruction::I64Sub);
                func.instruction(&Instruction::LocalSet(l.local_result));
            }
            func.instruction(&Instruction::Else);
            {
                func.instruction(&Instruction::LocalGet(l.local_a));
                func.instruction(&Instruction::LocalSet(l.local_result));
            }
            func.instruction(&Instruction::End);
        }
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::End);

    emit_merge_null_and_push_result(func, l);
}

/// Helper to call `emit_expr` using `EmitLocals`.
fn emit_child(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_expr(
        func,
        expr,
        col_to_param,
        l.local_i,
        l.local_result,
        l.local_is_null,
        l.local_is_error,
        l.local_a,
        l.local_b,
        l.local_saved_null,
        l.local_any_dominant,
        l.local_any_null,
        l.local_saved_caller_null,
    );
}

/// Emits WASM for `IsNull(child)`. Null-consuming: output is never null.
///
/// `is_null(NULL) = TRUE (1)`, `is_null(non-NULL) = FALSE (0)`.
fn emit_is_null(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    // Save current is_null onto local_saved_null and reset.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalSet(l.local_saved_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    emit_child(func, expr, col_to_param, l);
    // Stack: [child_val(i64)]
    func.instruction(&Instruction::Drop);

    // Result = child's is_null flag, widened to i64.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I64ExtendI32U);
    // Stack: [result(i64)]

    // Restore original is_null (IsNull consumes child nullness).
    func.instruction(&Instruction::LocalGet(l.local_saved_null));
    func.instruction(&Instruction::LocalSet(l.local_is_null));
}

/// Emits WASM for `IsTrue(child)`. Null-consuming: output is never null.
///
/// `is_true(TRUE) = TRUE`, `is_true(FALSE) = FALSE`, `is_true(NULL) = FALSE`.
fn emit_is_true(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    // Save current is_null and reset.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalSet(l.local_saved_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    emit_child(func, expr, col_to_param, l);
    // Stack: [child_val(i64)]
    func.instruction(&Instruction::LocalSet(l.local_a));

    // Result = (not null) AND (value != 0)
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz); // not_null flag
    func.instruction(&Instruction::LocalGet(l.local_a));
    func.instruction(&Instruction::I64Const(0));
    func.instruction(&Instruction::I64Ne); // value != 0
    func.instruction(&Instruction::I32And); // both conditions
    func.instruction(&Instruction::I64ExtendI32U);
    // Stack: [result(i64)]

    // Restore original is_null.
    func.instruction(&Instruction::LocalGet(l.local_saved_null));
    func.instruction(&Instruction::LocalSet(l.local_is_null));
}

/// Emits WASM for `IsFalse(child)`. Null-consuming: output is never null.
///
/// `is_false(FALSE) = TRUE`, `is_false(TRUE) = FALSE`, `is_false(NULL) = FALSE`.
fn emit_is_false(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    // Save current is_null and reset.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalSet(l.local_saved_null));
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    emit_child(func, expr, col_to_param, l);
    // Stack: [child_val(i64)]
    func.instruction(&Instruction::LocalSet(l.local_a));

    // Result = (not null) AND (value == 0)
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::I32Eqz); // not_null flag
    func.instruction(&Instruction::LocalGet(l.local_a));
    func.instruction(&Instruction::I64Eqz); // value == 0 (returns i32)
    func.instruction(&Instruction::I32And); // both conditions
    func.instruction(&Instruction::I64ExtendI32U);
    // Stack: [result(i64)]

    // Restore original is_null.
    func.instruction(&Instruction::LocalGet(l.local_saved_null));
    func.instruction(&Instruction::LocalSet(l.local_is_null));
}

/// Emits WASM for `AND(children...)` with three-valued null logic.
///
/// FALSE dominates: if any child is FALSE, return FALSE even if others are NULL.
/// Result: if any FALSE → FALSE(0); else if any NULL → NULL; else TRUE(1).
fn emit_and(
    func: &mut wasm_encoder::Function,
    exprs: &[MirScalarExpr],
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_and_or(func, exprs, col_to_param, l, true);
}

/// Emits WASM for `OR(children...)` with three-valued null logic.
///
/// TRUE dominates: if any child is TRUE, return TRUE even if others are NULL.
/// Result: if any TRUE → TRUE(1); else if any NULL → NULL; else FALSE(0).
fn emit_or(
    func: &mut wasm_encoder::Function,
    exprs: &[MirScalarExpr],
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
) {
    emit_and_or(func, exprs, col_to_param, l, false);
}

/// Shared implementation for And/Or with three-valued null logic.
///
/// `is_and == true` → And (FALSE dominates), `is_and == false` → Or (TRUE dominates).
fn emit_and_or(
    func: &mut wasm_encoder::Function,
    exprs: &[MirScalarExpr],
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    l: &EmitLocals,
    is_and: bool,
) {
    // Save caller's is_null into local_saved_caller_null (local_saved_null would be
    // clobbered by nested And/Or or unary preamble calls).
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalSet(l.local_saved_caller_null));

    // Reset is_null for this And/Or.
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    // Initialize tracking locals.
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_any_dominant)); // no dominant value seen
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_any_null)); // no NULL seen

    for child in exprs {
        // Save any_dominant and any_null on the WASM stack before evaluating the child,
        // because a nested And/Or child will clobber these locals.
        func.instruction(&Instruction::LocalGet(l.local_any_dominant));
        func.instruction(&Instruction::LocalGet(l.local_any_null));
        // Stack: [saved_any_dominant(i32), saved_any_null(i32)]

        // Reset is_null for this child.
        func.instruction(&Instruction::I32Const(0));
        func.instruction(&Instruction::LocalSet(l.local_is_null));

        emit_child(func, child, col_to_param, l);
        // Stack: [saved_any_dominant, saved_any_null, child_val(i64)]
        func.instruction(&Instruction::LocalSet(l.local_result));
        // Stack: [saved_any_dominant, saved_any_null]

        // Restore any_null and any_dominant from WASM stack (reverse order).
        func.instruction(&Instruction::LocalSet(l.local_any_null));
        func.instruction(&Instruction::LocalSet(l.local_any_dominant));
        // Stack: []

        // Check if child is null.
        func.instruction(&Instruction::LocalGet(l.local_is_null));
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            // Child is null: mark any_null.
            func.instruction(&Instruction::I32Const(1));
            func.instruction(&Instruction::LocalSet(l.local_any_null));
        }
        func.instruction(&Instruction::Else);
        {
            // Child is not null: check for dominant value.
            if is_and {
                // AND: FALSE (0) is dominant.
                func.instruction(&Instruction::LocalGet(l.local_result));
                func.instruction(&Instruction::I64Eqz); // value == 0 → child is FALSE
            } else {
                // OR: TRUE (nonzero) is dominant.
                func.instruction(&Instruction::LocalGet(l.local_result));
                func.instruction(&Instruction::I64Const(0));
                func.instruction(&Instruction::I64Ne); // value != 0 → child is TRUE
            }
            func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
            {
                func.instruction(&Instruction::I32Const(1));
                func.instruction(&Instruction::LocalSet(l.local_any_dominant));
            }
            func.instruction(&Instruction::End);
        }
        func.instruction(&Instruction::End);
    }

    // After all children: produce result.
    // Reset is_null to 0 (And/Or control null explicitly).
    func.instruction(&Instruction::I32Const(0));
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    // If any_dominant: result is the dominant value, is_null stays 0.
    func.instruction(&Instruction::LocalGet(l.local_any_dominant));
    func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
    {
        if is_and {
            // AND dominant = FALSE → result = 0
            func.instruction(&Instruction::I64Const(0));
        } else {
            // OR dominant = TRUE → result = 1
            func.instruction(&Instruction::I64Const(1));
        }
        func.instruction(&Instruction::LocalSet(l.local_result));
    }
    func.instruction(&Instruction::Else);
    {
        // No dominant. Check any_null.
        func.instruction(&Instruction::LocalGet(l.local_any_null));
        func.instruction(&Instruction::If(wasm_encoder::BlockType::Empty));
        {
            // Some child was NULL, no dominant → result is NULL.
            func.instruction(&Instruction::I32Const(1));
            func.instruction(&Instruction::LocalSet(l.local_is_null));
            func.instruction(&Instruction::I64Const(0)); // garbage, masked by null
            func.instruction(&Instruction::LocalSet(l.local_result));
        }
        func.instruction(&Instruction::Else);
        {
            // No dominant, no null → all children had non-dominant value.
            if is_and {
                // AND: all TRUE → result = 1
                func.instruction(&Instruction::I64Const(1));
            } else {
                // OR: all FALSE → result = 0
                func.instruction(&Instruction::I64Const(0));
            }
            func.instruction(&Instruction::LocalSet(l.local_result));
        }
        func.instruction(&Instruction::End);
    }
    func.instruction(&Instruction::End);

    // Merge caller's is_null (saved in local_saved_caller_null) into is_null.
    func.instruction(&Instruction::LocalGet(l.local_is_null));
    func.instruction(&Instruction::LocalGet(l.local_saved_caller_null));
    func.instruction(&Instruction::I32Or);
    func.instruction(&Instruction::LocalSet(l.local_is_null));

    // Push result onto stack.
    func.instruction(&Instruction::LocalGet(l.local_result));
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row, SqlScalarType};

    fn col(idx: usize) -> MirScalarExpr {
        MirScalarExpr::Column(idx, Default::default())
    }

    fn lit_i64(v: i64) -> MirScalarExpr {
        MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(v)])),
            ReprColumnType {
                scalar_type: ReprScalarType::Int64,
                nullable: false,
            },
        )
    }

    fn add_i64(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        MirScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(mz_expr::func::AddInt64),
            expr1: Box::new(a),
            expr2: Box::new(b),
        }
    }

    #[mz_ore::test]
    fn test_generate_simple_add() {
        let expr = add_i64(col(0), col(1));
        let types = vec![SqlScalarType::Int64, SqlScalarType::Int64];
        let result = generate_wasm(&expr, &types);
        assert!(result.is_some());
        let (wasm_bytes, layout) = result.unwrap();
        assert_eq!(layout.num_input_cols, 2);
        // Validate it's a valid WASM module by checking the magic number.
        assert_eq!(&wasm_bytes[0..4], b"\0asm");
    }

    #[mz_ore::test]
    fn test_generate_add_with_literal() {
        let expr = add_i64(col(0), lit_i64(10));
        let types = vec![SqlScalarType::Int64];
        let result = generate_wasm(&expr, &types);
        assert!(result.is_some());
        let (_, layout) = result.unwrap();
        assert_eq!(layout.num_input_cols, 1);
    }

    #[mz_ore::test]
    fn test_generate_unsupported() {
        let expr = MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: vec![col(0)],
        };
        assert!(generate_wasm(&expr, &[]).is_none());
    }
}
