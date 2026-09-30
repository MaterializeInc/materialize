// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runs the functions in `fixtures/udfs` through the runtime.

use std::io::Read;
use std::sync::{Arc, LazyLock, Mutex};

use itertools::Itertools;
use mz_expr::EvalError;
use mz_expr::func::{
    InvokerCell, WasmErrorKind, WasmFunc, WasmInvoker, WasmLimits, WasmModuleHash,
};
use mz_repr::{Datum, RowArena, SqlScalarType};
use mz_wasm_udf::codec::ValueType;
use mz_wasm_udf::{BatchConfig, CallObserver, CompiledModule, Invoker, Runtime};
use mz_wasm_udf_abi::{ScalarSignature, UdfType};

static FIXTURE: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let gz: &[u8] = include_bytes!("fixtures/udfs.wasm.gz");
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(gz)
        .read_to_end(&mut bytes)
        .expect("valid gzip");
    bytes
});

static MODULE: LazyLock<Arc<CompiledModule>> =
    LazyLock::new(|| Arc::new(CompiledModule::compile(&FIXTURE).expect("fixture compiles")));

const LIMITS: WasmLimits = WasmLimits {
    fuel: 100_000_000,
    memory_bytes: 64 << 20,
};

fn invoker(name: &str, args: &[SqlScalarType], ret: SqlScalarType, limits: WasmLimits) -> Invoker {
    let arg_types: Vec<ValueType> = args
        .iter()
        .map(|t| ValueType::new(t.clone()).unwrap())
        .collect();
    let ret = ValueType::new(ret).unwrap();
    let sig = ScalarSignature {
        name: name.into(),
        args: arg_types.iter().map(|t| t.udf.clone()).collect(),
        ret: ret.udf.clone(),
    };
    Invoker::new(
        name.into(),
        Arc::clone(&MODULE),
        sig.export_name(),
        arg_types,
        ret,
        limits,
        Arc::new(BatchConfig::default()),
    )
}

fn call<'a>(
    invoker: &Invoker,
    rows: &[&[Datum<'a>]],
    arena: &'a RowArena,
) -> Vec<Result<Datum<'a>, EvalError>> {
    let mut out = Vec::new();
    invoker.call_batch(rows, arena, &mut out);
    assert_eq!(out.len(), rows.len());
    out
}

fn kind(result: &Result<Datum, EvalError>) -> Option<WasmErrorKind> {
    match result {
        Err(EvalError::WasmFunction { kind, .. }) => Some(*kind),
        _ => None,
    }
}

#[mz_ore::test]
fn gcd_batch() {
    let gcd = invoker(
        "gcd",
        &[SqlScalarType::Int32, SqlScalarType::Int32],
        SqlScalarType::Int32,
        LIMITS,
    );
    let arena = RowArena::new();
    let rows: Vec<[Datum; 2]> = vec![
        [Datum::Int32(12), Datum::Int32(18)],
        [Datum::Int32(7), Datum::Int32(3)],
        [Datum::Int32(0), Datum::Int32(-5)],
        [Datum::Null, Datum::Int32(1)],
    ];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    assert_eq!(
        call(&gcd, &rows, &arena),
        vec![
            Ok(Datum::Int32(6)),
            Ok(Datum::Int32(1)),
            Ok(Datum::Int32(5)),
            // The guest's non-`Option` parameters make it strict.
            Ok(Datum::Null),
        ]
    );
}

#[mz_ore::test]
fn guest_errors_are_per_row() {
    let safe_div = invoker(
        "safe_div",
        &[SqlScalarType::Int64, SqlScalarType::Int64],
        SqlScalarType::Int64,
        LIMITS,
    );
    let arena = RowArena::new();
    let rows = [
        [Datum::Int64(10), Datum::Int64(2)],
        [Datum::Int64(1), Datum::Int64(0)],
    ];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&safe_div, &rows, &arena);
    assert_eq!(out[0], Ok(Datum::Int64(5)));
    match &out[1] {
        Err(EvalError::WasmFunction {
            name,
            kind: WasmErrorKind::Guest,
            message,
        }) => {
            assert_eq!(&**name, "safe_div");
            assert!(message.contains("division by zero"), "{message}");
        }
        other => panic!("expected a guest error, got {other:?}"),
    }
}

#[mz_ore::test]
fn traps_are_attributed_to_rows() {
    let f = invoker(
        "trap_on_negative",
        &[SqlScalarType::Int32],
        SqlScalarType::Int32,
        LIMITS,
    );
    let arena = RowArena::new();
    let values = [1, 2, -1, 3, 4, -2, 5];
    let rows: Vec<[Datum; 1]> = values.iter().map(|i| [Datum::Int32(*i)]).collect();
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&f, &rows, &arena);
    for (value, result) in values.iter().zip_eq(&out) {
        if *value < 0 {
            assert_eq!(kind(result), Some(WasmErrorKind::Trap));
            let Err(EvalError::WasmFunction { message, .. }) = result else {
                unreachable!()
            };
            assert!(message.contains("negative input"), "{message}");
        } else {
            assert_eq!(*result, Ok(Datum::Int32(*value)));
        }
    }
}

#[mz_ore::test]
fn fuel_exhaustion_is_per_row() {
    let limits = WasmLimits {
        fuel: 5_000_000,
        ..LIMITS
    };
    let spin = invoker(
        "spin",
        &[SqlScalarType::Int64],
        SqlScalarType::Int64,
        limits,
    );
    let arena = RowArena::new();
    let rows = [
        [Datum::Int64(10)],
        [Datum::Int64(1_000_000_000)],
        [Datum::Int64(100)],
    ];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&spin, &rows, &arena);
    assert_eq!(out[0], Ok(Datum::Int64(45)));
    assert_eq!(kind(&out[1]), Some(WasmErrorKind::OutOfFuel));
    assert_eq!(out[2], Ok(Datum::Int64(4950)));
}

#[mz_ore::test]
fn memory_limit_is_per_row() {
    let limits = WasmLimits {
        memory_bytes: 16 << 20,
        ..LIMITS
    };
    let alloc = invoker(
        "alloc_mib",
        &[SqlScalarType::Int32],
        SqlScalarType::Int32,
        limits,
    );
    let arena = RowArena::new();
    let rows = [[Datum::Int32(1)], [Datum::Int32(64)], [Datum::Int32(2)]];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&alloc, &rows, &arena);
    assert_eq!(out[0], Ok(Datum::Int32(1)));
    assert_eq!(kind(&out[1]), Some(WasmErrorKind::MemoryLimit));
    assert_eq!(out[2], Ok(Datum::Int32(2)));
}

#[mz_ore::test]
fn wasi_is_deterministic() {
    let arena = RowArena::new();
    let distinct = invoker(
        "distinct_chars",
        &[SqlScalarType::String],
        SqlScalarType::Int32,
        LIMITS,
    );
    let row = [Datum::String("hello")];
    assert_eq!(call(&distinct, &[&row], &arena), vec![Ok(Datum::Int32(4))]);

    let now = invoker("now_secs", &[], SqlScalarType::Int64, LIMITS);
    assert_eq!(call(&now, &[&[]], &arena), vec![Ok(Datum::Int64(0))]);
}

#[derive(Default)]
struct Recorder(Mutex<Vec<(usize, String)>>);

impl CallObserver for Recorder {
    fn observe(&self, rows: usize, _fuel: u64, _memory: usize, output: &[u8], _failed: bool) {
        self.0
            .lock()
            .unwrap()
            .push((rows, String::from_utf8_lossy(output).into_owned()));
    }
}

#[mz_ore::test]
fn output_is_captured() {
    let recorder = Arc::new(Recorder::default());
    let shout = invoker(
        "shout",
        &[SqlScalarType::String],
        SqlScalarType::String,
        LIMITS,
    )
    .with_observer(Arc::clone(&recorder));
    let arena = RowArena::new();
    let rows = [[Datum::String("hi")], [Datum::String("there")]];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    assert_eq!(
        call(&shout, &rows, &arena),
        vec![Ok(Datum::String("HI")), Ok(Datum::String("THERE"))]
    );
    let calls = recorder.0.lock().unwrap();
    assert_eq!(calls.len(), 1, "both rows go in one call");
    assert_eq!(calls[0], (2, "shouting hi\nshouting there\n".to_string()));
}

#[mz_ore::test]
fn scalar_types_round_trip() {
    let arena = RowArena::new();
    let reverse = invoker(
        "reverse",
        &[SqlScalarType::String],
        SqlScalarType::String,
        LIMITS,
    );
    assert_eq!(
        call(&reverse, &[&[Datum::String("abc")]], &arena),
        vec![Ok(Datum::String("cba"))]
    );
    let byte_len = invoker(
        "byte_len",
        &[SqlScalarType::Bytes],
        SqlScalarType::Int32,
        LIMITS,
    );
    assert_eq!(
        call(&byte_len, &[&[Datum::Bytes(b"four")]], &arena),
        vec![Ok(Datum::Int32(4))]
    );
    let halve = invoker(
        "halve",
        &[SqlScalarType::Float64],
        SqlScalarType::Float64,
        LIMITS,
    );
    assert_eq!(
        call(&halve, &[&[Datum::from(3.0f64)]], &arena),
        vec![Ok(Datum::from(1.5f64))]
    );
    let is_even = invoker(
        "is_even",
        &[SqlScalarType::Int64],
        SqlScalarType::Bool,
        LIMITS,
    );
    assert_eq!(
        call(&is_even, &[&[Datum::Int64(4)], &[Datum::Int64(5)]], &arena),
        vec![Ok(Datum::True), Ok(Datum::False)]
    );
}

#[mz_ore::test]
fn jaro_winkler() {
    let jw = invoker(
        "jaro_winkler",
        &[SqlScalarType::String, SqlScalarType::String],
        SqlScalarType::Float64,
        LIMITS,
    );
    let arena = RowArena::new();
    let pairs = [
        ("MARTHA", "MARHTA", 0.961_111),
        ("DIXON", "DICKSONX", 0.813_333),
        ("same", "same", 1.0),
        ("", "", 1.0),
        ("abc", "xyz", 0.0),
    ];
    let rows: Vec<[Datum; 2]> = pairs
        .iter()
        .map(|(a, b, _)| [Datum::String(a), Datum::String(b)])
        .collect();
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    for ((a, b, expected), result) in pairs.iter().zip_eq(call(&jw, &rows, &arena)) {
        let actual = result.unwrap().unwrap_float64();
        assert!(
            (actual - expected).abs() < 1e-5,
            "jaro_winkler({a:?}, {b:?}) = {actual}, expected {expected}"
        );
    }
}

/// `saxpy` is a `batch_fn`: the guest computes the whole batch with Arrow
/// kernels. Ten thousand rows cross the boundary in one call.
#[mz_ore::test]
fn vectorized_guest_gets_one_call_per_batch() {
    let recorder = Arc::new(Recorder::default());
    let saxpy = invoker(
        "saxpy",
        &[
            SqlScalarType::Float64,
            SqlScalarType::Float64,
            SqlScalarType::Float64,
        ],
        SqlScalarType::Float64,
        LIMITS,
    )
    .with_observer(Arc::clone(&recorder));
    let arena = RowArena::new();
    let rows: Vec<[Datum; 3]> = (0..10_000)
        .map(|i| {
            let x = f64::from(i);
            [Datum::from(2.0f64), Datum::from(x), Datum::from(1.0f64)]
        })
        .collect();
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&saxpy, &rows, &arena);
    for (i, result) in out.iter().enumerate() {
        let expected = 2.0 * f64::from(u32::try_from(i).unwrap()) + 1.0;
        assert_eq!(*result, Ok(Datum::from(expected)));
    }
    let calls = recorder.0.lock().unwrap();
    assert_eq!(
        calls.iter().map(|(rows, _)| *rows).collect::<Vec<_>>(),
        vec![10_000]
    );
}

#[mz_ore::test]
fn batch_config_bounds_rows_per_call() {
    let recorder = Arc::new(Recorder::default());
    let batch = Arc::new(BatchConfig::default());
    batch.set(1000, usize::MAX);
    let is_even = Invoker::new(
        "is_even".into(),
        Arc::clone(&MODULE),
        ScalarSignature {
            name: "is_even".into(),
            args: vec![UdfType::Int64],
            ret: UdfType::Boolean,
        }
        .export_name(),
        vec![ValueType::new(SqlScalarType::Int64).unwrap()],
        ValueType::new(SqlScalarType::Bool).unwrap(),
        LIMITS,
        batch,
    )
    .with_observer(Arc::clone(&recorder));
    let arena = RowArena::new();
    let rows: Vec<[Datum; 1]> = (0..2500).map(|i| [Datum::Int64(i)]).collect();
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let out = call(&is_even, &rows, &arena);
    assert_eq!(out[2499], Ok(Datum::False));
    let calls = recorder.0.lock().unwrap();
    assert_eq!(
        calls.iter().map(|(rows, _)| *rows).collect::<Vec<_>>(),
        vec![1000, 1000, 500]
    );
}

#[mz_ore::test]
fn batching_does_not_change_pure_results() {
    let gcd = invoker(
        "gcd",
        &[SqlScalarType::Int32, SqlScalarType::Int32],
        SqlScalarType::Int32,
        LIMITS,
    );
    let arena = RowArena::new();
    let rows: Vec<[Datum; 2]> = (0..50)
        .map(|i| [Datum::Int32(i * 6), Datum::Int32(i * 4 + 2)])
        .collect();
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    let batched = call(&gcd, &rows, &arena);
    let single: Vec<_> = rows
        .iter()
        .flat_map(|row| call(&gcd, &[*row], &arena))
        .collect();
    assert_eq!(batched, single);
}

/// `stateful` breaks the per-row contract: within one call, its output
/// depends on how many rows came before. This is the behavior
/// `mz udf check --determinism` exists to catch.
#[mz_ore::test]
fn stateful_guests_observe_batches() {
    let f = invoker(
        "stateful",
        &[SqlScalarType::Int32],
        SqlScalarType::Int64,
        LIMITS,
    );
    let arena = RowArena::new();
    let rows = [[Datum::Int32(0)], [Datum::Int32(0)], [Datum::Int32(0)]];
    let rows: Vec<&[Datum]> = rows.iter().map(|r| r.as_slice()).collect();
    assert_eq!(
        call(&f, &rows, &arena),
        vec![
            Ok(Datum::Int64(0)),
            Ok(Datum::Int64(1)),
            Ok(Datum::Int64(2))
        ]
    );
    // A fresh instance per call resets the state.
    assert_eq!(call(&f, &rows[..1], &arena), vec![Ok(Datum::Int64(0))]);
}

#[mz_ore::test]
fn runtime_binds_installed_modules() {
    let runtime = Runtime::global();
    let hash = WasmModuleHash(MODULE.info().hash);
    runtime.install(hash, &FIXTURE).unwrap();
    assert!(runtime.contains(&hash));
    assert!(runtime.install(WasmModuleHash([1; 32]), &FIXTURE).is_err());

    let sig = ScalarSignature {
        name: "gcd".into(),
        args: vec![UdfType::Int32, UdfType::Int32],
        ret: UdfType::Int32,
    };
    let func = WasmFunc {
        name: "materialize.public.gcd".into(),
        module: hash,
        export: sig.export_name(),
        arg_types: vec![SqlScalarType::Int32, SqlScalarType::Int32],
        return_type: SqlScalarType::Int32,
        strict: true,
        limits: LIMITS,
        invoker: InvokerCell::default(),
    };
    let arena = RowArena::new();
    let mut out = Vec::new();
    func.call_batch(
        &[
            &[Datum::Int32(9), Datum::Int32(6)],
            &[Datum::Null, Datum::Int32(6)],
        ],
        &arena,
        &mut out,
    );
    assert_eq!(out, vec![Ok(Datum::Int32(3)), Ok(Datum::Null)]);

    let missing = WasmFunc {
        module: WasmModuleHash([2; 32]),
        invoker: InvokerCell::default(),
        ..func
    };
    out.clear();
    missing.call_batch(&[&[Datum::Int32(1), Datum::Int32(1)]], &arena, &mut out);
    assert!(matches!(out[0], Err(EvalError::Internal(_))), "{out:?}");
}
