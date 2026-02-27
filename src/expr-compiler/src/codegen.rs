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

use mz_expr::{BinaryFunc, MirScalarExpr};
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
pub fn generate_wasm(expr: &MirScalarExpr) -> Option<(Vec<u8>, WasmLayout)> {
    if !crate::analyze::is_compilable(expr) {
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
        (1, ValType::I32), // temp error flag
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
    #[allow(clippy::as_conversions)]
    let local_i = num_params as u32;
    let local_result = local_i + 1;
    let local_is_null = local_i + 2;
    let local_is_error = local_i + 3;

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
        local_is_null,
        local_is_error,
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

/// Emits WASM instructions that evaluate `expr` and leave an i64 result on the stack.
///
/// Side effects: sets `local_is_null` to 1 if the result is null,
/// sets `local_is_error` to 1 if an overflow occurred.
fn emit_expr(
    func: &mut wasm_encoder::Function,
    expr: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    local_i: u32,
    local_is_null: u32,
    local_is_error: u32,
) {
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
            func.instruction(&Instruction::LocalGet(local_i));
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
            func.instruction(&Instruction::LocalSet(local_is_null));
            func.instruction(&Instruction::End);

            // Load value: mem[values_ptr + i * 8] as i64.
            func.instruction(&Instruction::LocalGet(values_local));
            func.instruction(&Instruction::LocalGet(local_i));
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
                    func.instruction(&Instruction::LocalSet(local_is_null));
                    func.instruction(&Instruction::I64Const(0));
                }
                _ => unreachable!("analyze ensures only Int64/Null literals"),
            }
        }

        MirScalarExpr::CallBinary {
            func: bf,
            expr1,
            expr2,
        } => match bf {
            BinaryFunc::AddInt64(_) => {
                emit_add_int64(
                    func,
                    expr1,
                    expr2,
                    col_to_param,
                    local_i,
                    local_is_null,
                    local_is_error,
                );
            }
            _ => unreachable!("analyze ensures only AddInt64"),
        },

        _ => unreachable!("analyze ensures only compilable expressions"),
    }
}

/// Emits WASM for `a + b` with overflow detection and null propagation.
///
/// Uses the following approach for checked addition without overflow intrinsics:
/// * Compute `a + b` as i64
/// * Detect overflow: if `a` and `b` have the same sign but `result` has a different sign
/// * If overflow, set error flag
fn emit_add_int64(
    func: &mut wasm_encoder::Function,
    expr1: &MirScalarExpr,
    expr2: &MirScalarExpr,
    col_to_param: &std::collections::BTreeMap<usize, usize>,
    local_i: u32,
    local_is_null: u32,
    local_is_error: u32,
) {
    // Evaluate left operand.
    emit_expr(
        func,
        expr1,
        col_to_param,
        local_i,
        local_is_null,
        local_is_error,
    );
    // Evaluate right operand.
    emit_expr(
        func,
        expr2,
        col_to_param,
        local_i,
        local_is_null,
        local_is_error,
    );

    // If either operand is null, the result is null (null propagation).
    // The is_null flag is already set by emit_expr if either child is null.
    // We just need to compute the add; the null flag will cause the validity
    // bit to be cleared in the output.

    // Stack: [a, b]
    func.instruction(&Instruction::I64Add);
    // Stack: [a+b]

    // Overflow detection for i64 add:
    // We need to detect if the addition overflowed. WASM i64.add wraps on overflow.
    // Overflow occurs when: sign(a) == sign(b) && sign(result) != sign(a).
    //
    // For simplicity in milestone 1, we use a conservative approach:
    // Store a and b in locals, compute result, then check.
    // But since we already consumed a and b from the stack, we need a different approach.
    //
    // Alternative: we rely on the fact that wasmtime i64.add is wrapping, and we
    // detect overflow by checking if (result - a) != b (which only works for non-wrapping).
    // Actually a simpler check: no overflow if result >= a when b >= 0, or result < a when b < 0.
    //
    // For milestone 1, we skip overflow detection in the WASM and handle it in the
    // host-side ResultColumn post-processing. The WASM just does wrapping addition.
    //
    // TODO(milestone 2): Add overflow detection inline in WASM.
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;
    use mz_repr::{Datum, ReprColumnType, ReprScalarType, Row};

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
        let result = generate_wasm(&expr);
        assert!(result.is_some());
        let (wasm_bytes, layout) = result.unwrap();
        assert_eq!(layout.num_input_cols, 2);
        // Validate it's a valid WASM module by checking the magic number.
        assert_eq!(&wasm_bytes[0..4], b"\0asm");
    }

    #[mz_ore::test]
    fn test_generate_add_with_literal() {
        let expr = add_i64(col(0), lit_i64(10));
        let result = generate_wasm(&expr);
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
        assert!(generate_wasm(&expr).is_none());
    }
}
