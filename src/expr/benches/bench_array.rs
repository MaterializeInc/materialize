// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for Array and List scalar functions (binary, unary, and variadic).

use bencher::{Bencher, benchmark_group, benchmark_main};
use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc, func};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, RowArena, SqlScalarType};

/// Build a 1-D Int32 array datum (borrows from `arena`).
fn make_int32_array<'a>(arena: &'a RowArena, elems: &[i32]) -> Datum<'a> {
    let datums: Vec<Datum> = elems.iter().map(|&v| Datum::Int32(v)).collect();
    arena.make_datum(|packer| {
        packer
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: elems.len(),
                }],
                &datums,
            )
            .unwrap()
    })
}

/// Build an Int32 list datum (borrows from `arena`).
fn make_int32_list<'a>(arena: &'a RowArena, elems: &[i32]) -> Datum<'a> {
    let datums: Vec<Datum> = elems.iter().map(|&v| Datum::Int32(v)).collect();
    arena.make_datum(|packer| packer.push_list(&datums))
}

// ===========================================================================
// Binary — Array functions
// ===========================================================================

fn bench_array_contains_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayContains(func::ArrayContains);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::Int32(30), arr];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_contains_array_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr_a = make_int32_array(&sa, &[1, 2, 3, 4, 5]);
    let arr_b = make_int32_array(&sa, &[1, 3, 5]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayContainsArray(func::ArrayContainsArray);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr_a, arr_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_contains_array_rev_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr_a = make_int32_array(&sa, &[1, 3, 5]);
    let arr_b = make_int32_array(&sa, &[1, 2, 3, 4, 5]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayContainsArrayRev(func::ArrayContainsArrayRev);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr_a, arr_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_length_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayLength(func::ArrayLength);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr, Datum::Int64(1)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_lower_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayLower(func::ArrayLower);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr, Datum::Int64(1)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_upper_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayUpper(func::ArrayUpper);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr, Datum::Int64(1)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_remove_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[1, 2, 3, 4, 5]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayRemove(func::ArrayRemove);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr, Datum::Int32(3)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_array_array_concat_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr_a = make_int32_array(&sa, &[1, 2, 3]);
    let arr_b = make_int32_array(&sa, &[4, 5, 6]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ArrayArrayConcat(func::ArrayArrayConcat);
    let arena = RowArena::new();
    let datums: &[Datum] = &[arr_a, arr_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// ===========================================================================
// Binary — List functions
// ===========================================================================

fn bench_list_list_concat_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list_a = make_int32_list(&sa, &[1, 2, 3]);
    let list_b = make_int32_list(&sa, &[4, 5, 6]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListListConcat(func::ListListConcat);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list_a, list_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_list_element_concat_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[1, 2, 3]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListElementConcat(func::ListElementConcat);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list, Datum::Int32(4)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_element_list_concat_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[2, 3, 4]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ElementListConcat(func::ElementListConcat);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::Int32(1), list];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_list_remove_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[1, 2, 3, 2, 4]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListRemove(func::ListRemove);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list, Datum::Int32(2)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_list_contains_list_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list_a = make_int32_list(&sa, &[1, 2, 3, 4, 5]);
    let list_b = make_int32_list(&sa, &[1, 3, 5]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListContainsList(func::ListContainsList);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list_a, list_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_list_contains_list_rev_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list_a = make_int32_list(&sa, &[1, 3, 5]);
    let list_b = make_int32_list(&sa, &[1, 2, 3, 4, 5]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListContainsListRev(func::ListContainsListRev);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list_a, list_b];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn bench_list_length_max_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[10, 20, 30, 40, 50]);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let f = BinaryFunc::ListLengthMax(func::ListLengthMax { max_layer: 1 });
    let arena = RowArena::new();
    let datums: &[Datum] = &[list, Datum::Int64(1)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// ===========================================================================
// Unary — List functions
// ===========================================================================

fn bench_list_length_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[10, 20, 30, 40, 50]);
    let f = UnaryFunc::ListLength(func::ListLength);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[Datum] = &[list];
    b.iter(|| f.eval(datums, &arena, &a));
}

// ===========================================================================
// Variadic — Array / List functions
// ===========================================================================

fn bench_array_create_happy(b: &mut Bencher) {
    let func = VariadicFunc::ArrayCreate {
        elem_type: SqlScalarType::Int32,
    };
    let datums: &[Datum] = &[
        Datum::Int32(1),
        Datum::Int32(2),
        Datum::Int32(3),
        Datum::Int32(4),
        Datum::Int32(5),
    ];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_list_create_happy(b: &mut Bencher) {
    let func = VariadicFunc::ListCreate {
        elem_type: SqlScalarType::Int32,
    };
    let datums: &[Datum] = &[
        Datum::Int32(1),
        Datum::Int32(2),
        Datum::Int32(3),
        Datum::Int32(4),
        Datum::Int32(5),
    ];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_array_to_string_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[1, 2, 3, 4, 5]);
    let func = VariadicFunc::ArrayToString {
        elem_type: SqlScalarType::Int32,
    };
    let datums: &[Datum] = &[arr, Datum::String(",")];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_array_index_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let func = VariadicFunc::ArrayIndex { offset: 0 };
    let datums: &[Datum] = &[arr, Datum::Int64(3)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_list_index_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[10, 20, 30, 40, 50]);
    let func = VariadicFunc::ListIndex;
    let datums: &[Datum] = &[list, Datum::Int64(3)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_list_slice_linear_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let list = make_int32_list(&sa, &[10, 20, 30, 40, 50]);
    let func = VariadicFunc::ListSliceLinear;
    let datums: &[Datum] = &[list, Datum::Int64(2), Datum::Int64(4)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_array_position_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let arr = make_int32_array(&sa, &[10, 20, 30, 40, 50]);
    let func = VariadicFunc::ArrayPosition;
    let datums: &[Datum] = &[arr, Datum::Int32(30)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_array_fill_happy(b: &mut Bencher) {
    let sa = RowArena::new();
    let dims = make_int32_array(&sa, &[5]);
    let func = VariadicFunc::ArrayFill {
        elem_type: SqlScalarType::Int32,
    };
    let datums: &[Datum] = &[Datum::Int32(42), dims];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

// ===========================================================================
// Benchmark registration
// ===========================================================================

benchmark_group!(
    binary_array_benches,
    bench_array_contains_happy,
    bench_array_contains_array_happy,
    bench_array_contains_array_rev_happy,
    bench_array_length_happy,
    bench_array_lower_happy,
    bench_array_upper_happy,
    bench_array_remove_happy,
    bench_array_array_concat_happy
);

benchmark_group!(
    binary_list_benches,
    bench_list_list_concat_happy,
    bench_list_element_concat_happy,
    bench_element_list_concat_happy,
    bench_list_remove_happy,
    bench_list_contains_list_happy,
    bench_list_contains_list_rev_happy,
    bench_list_length_max_happy,
    bench_list_length_happy
);

benchmark_group!(
    variadic_array_list_benches,
    bench_array_create_happy,
    bench_list_create_happy,
    bench_array_to_string_happy,
    bench_array_index_happy,
    bench_list_index_happy,
    bench_list_slice_linear_happy,
    bench_array_position_happy,
    bench_array_fill_happy
);

benchmark_main!(
    binary_array_benches,
    binary_list_benches,
    variadic_array_list_benches
);
