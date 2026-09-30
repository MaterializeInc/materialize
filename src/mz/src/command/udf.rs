// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Building blocks for WebAssembly user-defined functions.
//!
//! Local commands run a module through `mz_wasm_udf`, the runtime clusters
//! use, so type conversion, determinism stubs, batching, fuel, and memory
//! limits all behave exactly as they would in a region. Region commands talk
//! to the region through `psql`, sending SQL on stdin because a module can be
//! larger than the operating system allows a command-line argument to be.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use base64::Engine;
use itertools::Itertools;
use mz_expr::EvalError;
use mz_expr::func::{WasmInvoker, WasmLimits};
use mz_ore::cast::CastFrom;
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_postgres_util::Sql;
use mz_repr::{Datum, Row, RowArena, SqlScalarType};
use mz_wasm_udf::codec::ValueType;
use mz_wasm_udf::{BatchConfig, CallObserver, CompiledModule, Invoker};
use mz_wasm_udf_abi::{ModuleInfo, ScalarSignature, UdfType};
use serde::{Deserialize, Serialize};
use tabled::Tabled;

use crate::context::{Context, RegionContext};
use crate::error::Error;

fn udf_error(e: impl ToString) -> Error {
    Error::Udf(e.to_string())
}

/// A module read from disk and compiled.
pub struct Module {
    bytes: Vec<u8>,
    compiled: Arc<CompiledModule>,
}

impl Module {
    /// Reads, validates, and compiles the module at `path`.
    pub fn load(path: &Path) -> Result<Self, Error> {
        Self::from_bytes(std::fs::read(path)?)
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Error> {
        let compiled = CompiledModule::compile(&bytes).map_err(udf_error)?;
        Ok(Module {
            bytes,
            compiled: Arc::new(compiled),
        })
    }

    fn info(&self) -> &ModuleInfo {
        self.compiled.info()
    }

    /// Finds the signature of `function`, which is either a guest function
    /// name or a full signature string.
    fn signature(&self, function: &str) -> Result<ScalarSignature, Error> {
        let functions = &self.info().functions;
        if functions.contains(function) {
            return ScalarSignature::parse(function).map_err(udf_error);
        }
        let prefix = format!("{function}(");
        let matching: Vec<_> = functions
            .iter()
            .filter(|sig| sig.starts_with(&prefix) && !sig.contains(")->>"))
            .collect();
        match matching.as_slice() {
            [sig] => ScalarSignature::parse(sig).map_err(udf_error),
            [] => Err(Error::Udf(format!(
                "module exports no scalar function {function}; it exports: {}",
                functions.iter().cloned().collect::<Vec<_>>().join(", ")
            ))),
            _ => Err(Error::Udf(format!(
                "{function} is overloaded; pass one of these signatures instead: {}",
                matching
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }
}

/// The SQL types a signature's arguments and result are presented as.
struct SqlSignature {
    sig: ScalarSignature,
    args: Vec<SqlScalarType>,
    ret: SqlScalarType,
}

impl SqlSignature {
    fn new(sig: ScalarSignature) -> Self {
        let args = sig.args.iter().map(|t| t.default_sql_type()).collect();
        let ret = sig.ret.default_sql_type();
        SqlSignature { sig, args, ret }
    }

    fn pg_arg_types(&self) -> Vec<mz_pgrepr::Type> {
        self.args.iter().map(mz_pgrepr::Type::from).collect()
    }
}

fn sql_type_name(typ: &SqlScalarType) -> String {
    match typ {
        SqlScalarType::Array(elem) => format!("{}[]", sql_type_name(elem)),
        SqlScalarType::List { element_type, .. } => format!("{} list", sql_type_name(element_type)),
        typ => mz_pgrepr::Type::from(typ).name().to_string(),
    }
}

/// The module name [`create`] and [`sql`] use when none is given: the file
/// name without its extension.
fn default_module_name(path: &Path) -> Result<String, Error> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::to_string)
        .ok_or_else(|| {
            Error::Udf(format!(
                "cannot derive a module name from {}; pass --module-name",
                path.display()
            ))
        })
}

/// A psql command that creates the WebAssembly module `module_name` from
/// `module`, unless a module with that name exists already.
///
/// The module is sent as a bind parameter (`\bind`, psql 16 and later),
/// because the statement text has a size limit that most modules exceed.
/// An existing module with the same name and different bytes is an error.
fn create_module_psql(module: &Module, module_name: &str) -> String {
    // Base64 has no quote or backslash characters, so it needs no escaping
    // inside a psql single-quoted argument.
    let encoded = base64::engine::general_purpose::STANDARD.encode(&module.bytes);
    format!(
        "CREATE WASM MODULE IF NOT EXISTS {} USING BASE64 $1 \\bind '{encoded}' \\g",
        Sql::ident(module_name).as_str()
    )
}

/// The `CREATE FUNCTION` statement binding `name` to `sig` in `module`, a
/// module reference already in SQL syntax, declared `STRICT` and with the
/// fuel and memory limits of `options` where it sets them.
fn create_function_sql(
    sig: &SqlSignature,
    name: &str,
    module: &str,
    options: &CallOptions,
) -> String {
    let args: Vec<_> = sig.args.iter().map(sql_type_name).collect();
    let mut sql = format!(
        "CREATE FUNCTION {}({}) RETURNS {} LANGUAGE wasm",
        Sql::ident(name).as_str(),
        args.join(", "),
        sql_type_name(&sig.ret),
    );
    if options.strict {
        sql.push_str(" STRICT");
    }
    sql.push_str(&format!(" USING MODULE {module}"));
    let mut with = Vec::new();
    if sig.sig.name != name {
        with.push(format!("EXPORT = {}", Sql::literal(&sig.sig.name).as_str()));
    }
    if let Some(fuel) = options.fuel {
        with.push(format!("FUEL = {fuel}"));
    }
    if let Some(memory) = options.memory {
        let memory = mz_repr::bytes::ByteSize::b(memory).to_string();
        with.push(format!("MEMORY = {}", Sql::literal(&memory).as_str()));
    }
    if !with.is_empty() {
        sql.push_str(&format!(" WITH ({})", with.join(", ")));
    }
    sql.push(';');
    sql
}

/// Accumulates what the guest calls of one command did.
#[derive(Default)]
struct Stats(Mutex<StatsInner>);

#[derive(Default)]
struct StatsInner {
    calls: usize,
    failed_calls: usize,
    rows: usize,
    fuel: u64,
    peak_memory: usize,
    output: String,
}

impl CallObserver for Stats {
    fn observe(&self, rows: usize, fuel: u64, peak_memory: usize, output: &[u8], failed: bool) {
        let mut stats = self.0.lock().expect("lock poisoned");
        stats.calls += 1;
        stats.failed_calls += usize::from(failed);
        stats.rows += rows;
        stats.fuel += fuel;
        stats.peak_memory = stats.peak_memory.max(peak_memory);
        stats.output.push_str(&String::from_utf8_lossy(output));
    }
}

impl Stats {
    /// Reports the calls on stderr, keeping stdout for results.
    fn report(&self) {
        let stats = self.0.lock().expect("lock poisoned");
        if !stats.output.is_empty() {
            eprintln!("guest output:\n{}", stats.output.trim_end());
        }
        eprintln!(
            "{} guest call(s) ({} failed and were bisected) over {} row(s), {} fuel, peak memory {} bytes",
            stats.calls, stats.failed_calls, stats.rows, stats.fuel, stats.peak_memory
        );
    }
}

/// Limits and batching for local calls.
#[derive(Default)]
pub struct CallOptions {
    /// The fuel each guest call may use, or the region default.
    pub fuel: Option<u64>,
    /// The memory limit of each guest call in bytes, or the region default.
    pub memory: Option<u64>,
    /// The most rows one guest call may carry, or the region default.
    pub batch_rows: Option<usize>,
    /// Whether a row with a `NULL` argument returns `NULL` without calling
    /// the guest, as for a `STRICT` function.
    pub strict: bool,
}

impl CallOptions {
    fn limits(&self) -> WasmLimits {
        WasmLimits {
            fuel: self.fuel.unwrap_or(mz_wasm_udf_abi::DEFAULT_FUEL),
            memory_bytes: self.memory.unwrap_or(mz_wasm_udf_abi::DEFAULT_MEMORY_BYTES),
        }
    }
}

fn invoker(
    module: &Module,
    sig: &SqlSignature,
    options: &CallOptions,
    batch_rows: usize,
    stats: &Arc<Stats>,
) -> Result<Invoker, Error> {
    let value_type = |t: &SqlScalarType| ValueType::new(t.clone()).map_err(udf_error);
    let batch = BatchConfig::default();
    batch.set(batch_rows, BatchConfig::DEFAULT_MAX_BYTES);
    Ok(Invoker::new(
        sig.sig.name.clone(),
        Arc::clone(&module.compiled),
        sig.sig.export_name(),
        sig.args.iter().map(value_type).collect::<Result<_, _>>()?,
        value_type(&sig.ret)?,
        options.limits(),
        Arc::new(batch),
    )
    .with_observer(Arc::clone(stats)))
}

fn format_datum(datum: Datum, typ: &SqlScalarType) -> String {
    match mz_pgrepr::Value::from_datum(datum, typ) {
        None => "NULL".into(),
        Some(value) => {
            let mut buf = bytes::BytesMut::new();
            value.encode_text(
                &mut buf,
                mz_pgrepr::TextEncodeSettings {
                    extra_float_digits: 1,
                },
            );
            String::from_utf8_lossy(&buf).into_owned()
        }
    }
}

fn format_result(result: &Result<Datum, EvalError>, typ: &SqlScalarType) -> String {
    match result {
        Ok(datum) => format_datum(*datum, typ),
        Err(e) => format!("error: {e}"),
    }
}

fn format_row(row: &Row, types: &[SqlScalarType]) -> String {
    let fields: Vec<_> = row
        .iter()
        .zip_eq(types)
        .map(|(d, t)| format_datum(d, t))
        .collect();
    format!("({})", fields.join(", "))
}

/// Parses SQL literals into a row of `sig`'s argument types. `NULL` in any
/// case is a null.
fn parse_literals(args: &[String], sig: &SqlSignature) -> Result<Row, Error> {
    if args.len() != sig.args.len() {
        return Err(Error::Udf(format!(
            "{} takes {} argument(s), got {}",
            sig.sig,
            sig.args.len(),
            args.len()
        )));
    }
    let arena = RowArena::new();
    let mut datums = Vec::with_capacity(args.len());
    for (arg, ty) in args.iter().zip_eq(sig.pg_arg_types()) {
        if arg.eq_ignore_ascii_case("null") {
            datums.push(Datum::Null);
            continue;
        }
        let value = mz_pgrepr::Value::decode_text(&ty, arg.as_bytes())
            .map_err(|e| Error::Udf(format!("invalid {} literal {arg:?}: {e}", ty.name())))?;
        let datum = value
            .into_datum(&arena, &ty)
            .map_err(|e| Error::Udf(format!("invalid literal {arg:?}: {e:?}")))?;
        datums.push(datum);
    }
    Ok(Row::pack(datums))
}

/// Reads argument rows from a headerless CSV file, where an unquoted empty
/// field is a null.
fn read_csv(path: &Path, sig: &SqlSignature) -> Result<Vec<Row>, Error> {
    let data = std::fs::read(path)?;
    mz_pgcopy::decode_copy_format(
        &data,
        &sig.pg_arg_types(),
        CopyFormatParams::Csv(CopyCsvFormatParams::default()),
    )
    .map_err(|e| Error::Udf(format!("reading {}: {e}", path.display())))
}

/// A seeded SplitMix64 stream, so that generated inputs are reproducible
/// from their seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[usize::cast_from(self.below(u64::cast_from(items.len())))]
    }

    /// An integer in `min..=max`, usually small and sometimes an endpoint.
    fn integer(&mut self, min: i128, max: i128) -> i128 {
        match self.below(8) {
            0 => [min, max, 0.max(min), 1.min(max)][usize::cast_from(self.below(4))],
            1 => min + i128::from(self.next()) % (max - min + 1),
            _ => {
                let (lo, hi) = (min.max(-1000), max.min(1000));
                lo + i128::from(self.below(u64::try_from(hi - lo + 1).expect("small range")))
            }
        }
    }
}

/// A SQL text literal of `typ` for [`parse_literals`], or `NULL` when
/// `nulls` is set.
///
/// Values are mostly small, with the type's edge cases mixed in. Small values
/// keep functions whose cost grows with their input, such as a loop bound,
/// within their fuel.
fn literal(rng: &mut Rng, typ: &UdfType, nulls: bool) -> String {
    if nulls && rng.below(16) == 0 {
        return "NULL".into();
    }
    let small = |rng: &mut Rng| rng.integer(-1000, 1000);
    // A sign kept apart from the integer part, so values in (-1, 0) occur.
    let sign = |rng: &mut Rng| if rng.below(2) == 0 { "-" } else { "" };
    match typ {
        UdfType::Boolean => rng.pick(&["true", "false"]).into(),
        UdfType::Int16 => rng.integer(i16::MIN.into(), i16::MAX.into()).to_string(),
        UdfType::Int32 => rng.integer(i32::MIN.into(), i32::MAX.into()).to_string(),
        UdfType::Int64 => rng.integer(i64::MIN.into(), i64::MAX.into()).to_string(),
        UdfType::UInt16 => rng.integer(0, u16::MAX.into()).to_string(),
        UdfType::UInt32 => rng.integer(0, u32::MAX.into()).to_string(),
        UdfType::UInt64 => rng.integer(0, u64::MAX.into()).to_string(),
        UdfType::Float32 | UdfType::Float64 => match rng.below(4) {
            0 => rng
                .pick(&["0", "-0", "NaN", "Infinity", "-Infinity", "1e-30"])
                .into(),
            _ => format!("{}{}.{}", sign(rng), rng.below(1000), rng.below(1000)),
        },
        UdfType::Decimal => match rng.below(8) {
            0 => rng
                .pick(&["0", "-0.001", "99999999999999999999.999999"])
                .into(),
            _ => format!("{}{}.{:03}", sign(rng), rng.below(1000), rng.below(1000)),
        },
        UdfType::String => {
            // No n, u or l, so a string is never the literal NULL.
            const CHARS: &[char] = &[
                'a', 'b', 'c', 'x', 'Y', 'Z', ' ', '0', '9', '-', 'é', '漢', '😀', '"', '\\', '\'',
                ',', '{', '}',
            ];
            let len = rng.below(12);
            (0..len)
                .map(|_| CHARS[usize::cast_from(rng.below(u64::cast_from(CHARS.len())))])
                .collect()
        }
        UdfType::Binary => {
            let len = rng.below(12);
            let hex: String = (0..len)
                .map(|_| format!("{:02x}", rng.below(256)))
                .collect();
            format!("\\x{hex}")
        }
        UdfType::Json => match rng.below(4) {
            0 => rng.pick(&["{}", "[]", "0", "true", "\"\""]).into(),
            _ => format!(
                r#"{{"a": {}, "b": [{}, "x{}"]}}"#,
                small(rng),
                small(rng),
                rng.below(100)
            ),
        },
        UdfType::Date32 => date(rng),
        UdfType::Time64 => time(rng),
        UdfType::Timestamp => format!("{} {}", date(rng), time(rng)),
        UdfType::Interval => format!(
            "{} months {} days {} microseconds",
            small(rng),
            small(rng),
            rng.integer(-86_400_000_000, 86_400_000_000)
        ),
        // Nested lists are SQL lists (see `UdfType::default_sql_type`), whose
        // text form nests inner lists unquoted.
        UdfType::List(elem) if matches!(**elem, UdfType::List(_)) => {
            let len = rng.below(4);
            let elems: Vec<String> = (0..len).map(|_| literal(rng, elem, true)).collect();
            format!("{{{}}}", elems.join(","))
        }
        UdfType::List(elem) => {
            let len = rng.below(5);
            let elems: Vec<String> = (0..len)
                .map(|_| match literal(rng, elem, true) {
                    null if null == "NULL" => null,
                    value => format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")),
                })
                .collect();
            format!("{{{}}}", elems.join(","))
        }
    }
}

fn date(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => rng
            .pick(&["0001-01-01", "1970-01-01", "2000-02-29", "9999-12-31"])
            .into(),
        _ => format!(
            "{}-{:02}-{:02}",
            1900 + rng.below(200),
            1 + rng.below(12),
            1 + rng.below(28)
        ),
    }
}

fn time(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => rng.pick(&["00:00:00", "23:59:59.999999"]).into(),
        _ => format!(
            "{:02}:{:02}:{:02}.{:06}",
            rng.below(24),
            rng.below(60),
            rng.below(60),
            rng.below(1_000_000)
        ),
    }
}

/// Generates `count` argument rows for `sig` from `seed`. Some rows repeat
/// earlier ones, since a guest that keeps state can treat a value it has
/// already seen differently.
fn generate_rows(
    sig: &SqlSignature,
    count: usize,
    seed: u64,
    nulls: bool,
) -> Result<Vec<Row>, Error> {
    let mut rng = Rng(seed);
    let mut rows: Vec<Row> = Vec::with_capacity(count);
    for _ in 0..count {
        if !rows.is_empty() && rng.below(8) == 0 {
            let earlier = rows[usize::cast_from(rng.below(u64::cast_from(rows.len())))].clone();
            rows.push(earlier);
            continue;
        }
        let literals: Vec<String> = sig
            .sig
            .args
            .iter()
            .map(|typ| literal(&mut rng, typ, nulls))
            .collect();
        rows.push(parse_literals(&literals, sig)?);
    }
    Ok(rows)
}

/// Calls the function on `rows` and returns one result per row. With
/// `strict`, rows with a `NULL` argument return `NULL` without a call, as
/// `WasmFunc` does for a `STRICT` function in a region.
fn call_rows<'a>(
    invoker: &Invoker,
    rows: &'a [Row],
    arena: &'a RowArena,
    strict: bool,
) -> Vec<Result<Datum<'a>, EvalError>> {
    let datums: Vec<Vec<Datum>> = rows.iter().map(|r| r.iter().collect()).collect();
    let calls = |row: &[Datum]| !(strict && row.iter().any(|d| d.is_null()));
    let args: Vec<&[Datum]> = datums
        .iter()
        .map(|d| d.as_slice())
        .filter(|row| calls(row))
        .collect();
    let mut called = Vec::with_capacity(args.len());
    invoker.call_batch(&args, arena, &mut called);
    let mut called = called.into_iter();
    datums
        .iter()
        .map(|row| match calls(row) {
            true => called.next().expect("one result per called row"),
            false => Ok(Datum::Null),
        })
        .collect()
}

/// One row of [`call`]'s output.
#[derive(Deserialize, Serialize, Tabled)]
pub struct CallResult {
    /// The arguments, as SQL literals.
    pub input: String,
    /// The result as a SQL literal, or the error.
    pub output: String,
}

/// Arguments for [`call`].
pub struct CallArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// The guest function, by name or by signature.
    pub function: &'a str,
    /// Arguments as SQL literals, used when `input` is `None`.
    pub args: &'a [String],
    /// A headerless CSV file of argument rows.
    pub input: Option<&'a Path>,
    /// Limits and batching.
    pub options: CallOptions,
}

/// Calls a function locally, on literal arguments or on the rows of a CSV
/// file, and prints the results.
pub fn call(cx: &Context, args: CallArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    let rows = match args.input {
        Some(path) => read_csv(path, &sig)?,
        None => vec![parse_literals(args.args, &sig)?],
    };
    let stats = Arc::new(Stats::default());
    let batch_rows = args
        .options
        .batch_rows
        .unwrap_or(BatchConfig::DEFAULT_MAX_ROWS);
    let invoker = invoker(&module, &sig, &args.options, batch_rows, &stats)?;
    let arena = RowArena::new();
    let results = call_rows(&invoker, &rows, &arena, args.options.strict);
    let table = rows
        .iter()
        .zip_eq(&results)
        .map(|(row, result)| CallResult {
            input: format_row(row, &sig.args),
            output: format_result(result, &sig.ret),
        });
    cx.output_formatter().output_table(table)?;
    stats.report();
    Ok(())
}

/// One exported function, as [`check`] lists it.
#[derive(Deserialize, Serialize, Tabled)]
pub struct ExportInfo {
    /// The arrow-udf signature.
    pub signature: String,
    /// The SQL signature it maps to, or why it cannot be called from SQL.
    pub sql: String,
}

/// Where a determinism check gets its argument rows.
pub enum CheckInput<'a> {
    /// A headerless CSV file.
    Csv(&'a Path),
    /// Rows generated from the argument types.
    Generated {
        /// How many rows to generate.
        rows: usize,
        /// The seed of the generator, so a failing check can be rerun.
        seed: u64,
        /// Whether to include `NULL` arguments, which a `STRICT` function
        /// never receives.
        nulls: bool,
    },
}

impl CheckInput<'_> {
    /// How many rows a generated check uses by default: the default
    /// `wasm_udf_batch_max_rows`, so the one-batch run reaches the largest
    /// batch a region sends, where a vectorized guest may take a different
    /// path than it does for a few rows.
    pub const DEFAULT_ROWS: usize = 16384;
}

/// Arguments for [`check`].
pub struct CheckArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// A function to check the per-row contract of, and its input.
    pub determinism: Option<(&'a str, CheckInput<'a>)>,
    /// Limits for the determinism check.
    pub options: CallOptions,
}

/// Validates a module and lists what it exports, optionally checking that a
/// function's results do not depend on how rows are batched.
pub fn check(cx: &Context, args: CheckArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let info = module.info();
    eprintln!(
        "arrow-udf ABI {}.{}, {} bytes, sha256 {}",
        info.abi_version.0,
        info.abi_version.1,
        info.size,
        mz_expr::func::WasmModuleHash(info.hash),
    );
    let exports = info.functions.iter().map(|sig| {
        let sql = match ScalarSignature::parse(sig) {
            Ok(parsed) => {
                let sql_sig = SqlSignature::new(parsed);
                let args: Vec<_> = sql_sig.args.iter().map(sql_type_name).collect();
                format!(
                    "{}({}) RETURNS {}",
                    sql_sig.sig.name,
                    args.join(", "),
                    sql_type_name(&sql_sig.ret)
                )
            }
            Err(e) => format!("not callable from SQL: {e}"),
        };
        ExportInfo {
            signature: sig.clone(),
            sql,
        }
    });
    cx.output_formatter().output_table(exports)?;
    for (import, behavior) in info.determinized_imports() {
        eprintln!(
            "warning: the module imports {}.{}, which {behavior}",
            import.module, import.name
        );
    }

    let Some((function, input)) = args.determinism else {
        return Ok(());
    };
    let sig = SqlSignature::new(module.signature(function)?);
    determinism(&module, &sig, &input, &args.options)
}

/// Evaluates the input rows in one batch, one row per call, in odd-sized
/// batches, and in reverse order, and fails if any row's result differs
/// between them.
fn determinism(
    module: &Module,
    sig: &SqlSignature,
    input: &CheckInput<'_>,
    options: &CallOptions,
) -> Result<(), Error> {
    const MAX_REPORTED: usize = 10;
    let rows = match input {
        CheckInput::Csv(path) => read_csv(path, sig)?,
        CheckInput::Generated { rows, seed, nulls } => generate_rows(sig, *rows, *seed, *nulls)?,
    };
    let rows = rows.as_slice();
    if rows.is_empty() {
        return Err(Error::Udf(
            "the input has no rows, so the check shows nothing".into(),
        ));
    }
    // A strict function's rows with a NULL argument never reach the guest.
    let reaches_guest = |row: &Row| !options.strict || !row.iter().any(|datum| datum.is_null());
    // Each run returns the outcome of every row, as an owned value or error
    // message so that a NULL result and a string reading "NULL" differ.
    let run = |batch_rows: usize, reverse: bool| -> Result<Vec<Result<Row, String>>, Error> {
        let stats = Arc::new(Stats::default());
        let invoker = invoker(module, sig, options, batch_rows, &stats)?;
        let arena = RowArena::new();
        let mut ordered: Vec<Row> = rows.to_vec();
        if reverse {
            ordered.reverse();
        }
        let mut results: Vec<Result<Row, String>> =
            call_rows(&invoker, &ordered, &arena, options.strict)
                .into_iter()
                .map(|r| {
                    r.map(|datum| Row::pack_slice(&[datum]))
                        .map_err(|e| e.to_string())
                })
                .collect();
        if reverse {
            results.reverse();
        }
        Ok(results)
    };
    let format = |result: &Result<Row, String>| match result {
        Ok(row) => format_datum(row.unpack_first(), &sig.ret),
        Err(e) => format!("error: {e}"),
    };

    let one_batch = options.batch_rows.unwrap_or(rows.len()).max(1);
    let baseline = run(one_batch, false)?;
    let succeeded = rows
        .iter()
        .zip_eq(&baseline)
        .filter(|(row, result)| reaches_guest(row) && result.is_ok())
        .count();
    let variants = [
        ("one row per call", run(1, false)?),
        ("batches of 7", run(7, false)?),
        ("reversed", run(one_batch, true)?),
    ];
    let mut mismatches = 0;
    for (name, results) in &variants {
        for (i, (expected, actual)) in baseline.iter().zip_eq(results).enumerate() {
            if expected != actual {
                mismatches += 1;
                if mismatches <= MAX_REPORTED {
                    eprintln!(
                        "row {} {}: {} in one batch, {} with {name}",
                        i + 1,
                        format_row(&rows[i], &sig.args),
                        format(expected),
                        format(actual),
                    );
                }
            }
        }
    }
    if mismatches > MAX_REPORTED {
        eprintln!("and {} more", mismatches - MAX_REPORTED);
    }
    if mismatches > 0 {
        let source = match input {
            CheckInput::Csv(path) => format!("the rows in {}", path.display()),
            CheckInput::Generated { rows, seed, .. } => {
                format!("{rows} generated rows (--rows {rows} --seed {seed})")
            }
        };
        return Err(Error::Udf(format!(
            "{} returns results that depend on batching for {source}; its output for a row \
             must depend only on that row",
            sig.sig
        )));
    }
    // Agreement shows something only for rows the guest answered: rows that
    // fail the same way everywhere, or never reach the guest, agree trivially.
    if succeeded == 0 {
        return Err(Error::Udf(format!(
            "no row of the input succeeded, so the check shows nothing about {}; check it on \
             inputs it accepts with `mz udf check --determinism --input`, or raise --fuel or \
             --memory",
            sig.sig
        )));
    }
    eprintln!(
        "{} row(s) agree across batch sizes and orders",
        baseline.len()
    );
    Ok(())
}

/// Arguments for [`sql`].
pub struct SqlArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// The guest function, by name or by signature.
    pub function: &'a str,
    /// The SQL name, defaulting to the guest name.
    pub name: Option<&'a str>,
    /// The SQL name of the module, defaulting to the file name without its
    /// extension.
    pub module_name: Option<&'a str>,
    /// Whether to declare the function `STRICT`, and the limits to declare.
    pub options: CallOptions,
}

/// Prints the psql commands that create a module and one of its functions.
pub fn sql(args: SqlArgs<'_>) -> Result<(), Error> {
    if args.options.batch_rows.is_some() {
        return Err(Error::Udf(
            "--batch-rows sets how calls are batched locally, which `sql` does not do".into(),
        ));
    }
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    let name = args.name.unwrap_or(&sig.sig.name);
    let module_name = match args.module_name {
        Some(module_name) => module_name.to_string(),
        None => default_module_name(args.module)?,
    };
    println!("{}", create_module_psql(&module, &module_name));
    println!(
        "{}",
        create_function_sql(&sig, name, Sql::ident(&module_name).as_str(), &args.options)
    );
    Ok(())
}

/// Runs `sql` in the region through `psql`, sending it on stdin, and returns
/// what psql wrote to stdout.
async fn run_sql(
    cx: &RegionContext,
    database: Option<&str>,
    psql_args: &[&str],
    sql: &str,
) -> Result<Vec<u8>, Error> {
    let claims = cx.admin_client().claims().await?;
    let region_info = cx.get_region_info().await?;
    let user = claims.user()?;
    let mut command = cx.sql_client().script(&region_info, user, database);
    command
        .args(["-v", "ON_ERROR_STOP=1", "-q"])
        .args(psql_args)
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| Error::CommandExecutionError(e.to_string()))?;
    // Writing from another thread lets psql's output drain while it reads a
    // script larger than the pipe buffer.
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let sql = sql.to_string();
    let writer = std::thread::spawn(move || stdin.write_all(sql.as_bytes()));
    let output = child
        .wait_with_output()
        .map_err(|e| Error::CommandExecutionError(e.to_string()))?;
    // A psql that stops early closes stdin; its own error explains why.
    if !output.status.success() {
        return Err(Error::CommandFailed(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    writer.join().expect("writer does not panic")?;
    Ok(output.stdout)
}

/// Arguments for [`create`].
pub struct CreateArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// The guest function, by name or by signature.
    pub function: &'a str,
    /// The SQL name, defaulting to the guest name.
    pub name: Option<&'a str>,
    /// The SQL name of the module, defaulting to the file name without its
    /// extension.
    pub module_name: Option<&'a str>,
    /// The database to create the function in.
    pub database: Option<&'a str>,
    /// The schema to create the function in.
    pub schema: Option<&'a str>,
    /// Whether to declare the function `STRICT`, the limits to declare, and
    /// how the determinism check calls it.
    pub options: CallOptions,
    /// Whether to skip the determinism check on generated rows.
    pub skip_check: bool,
}

/// Creates a function in the region from a local module, after checking on
/// generated rows that its results do not depend on batching. Creates the
/// module first unless the schema has a module of that name already.
pub async fn create(cx: &RegionContext, args: CreateArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    let module_name = match args.module_name {
        Some(module_name) => module_name.to_string(),
        None => default_module_name(args.module)?,
    };
    if !args.skip_check {
        let input = CheckInput::Generated {
            rows: CheckInput::DEFAULT_ROWS,
            seed: 0,
            nulls: !args.options.strict,
        };
        determinism(&module, &sig, &input, &args.options).map_err(|e| {
            Error::Udf(format!(
                "{e}\nnot creating the function; pass --skip-check to create it anyway"
            ))
        })?;
    }
    let name = args.name.unwrap_or(&sig.sig.name).to_string();
    let hash = mz_expr::func::WasmModuleHash(module.info().hash).to_string();

    let spinner = cx
        .output_formatter()
        .loading_spinner("Creating function...");
    let search_path = match args.schema {
        Some(schema) => format!("SET search_path TO {};\n", Sql::ident(schema).as_str()),
        None => String::new(),
    };
    // Reusing a module needs only USAGE on it, where creating one takes the
    // CREATEWASMMODULE privilege even when the module exists, so look first.
    let mut existing = find_module(cx, &args, &module_name).await?;
    let created = existing.is_none();
    if created {
        require_psql_bind()?;
        let sql = format!(
            "{search_path}{}\n",
            create_module_psql(&module, &module_name)
        );
        run_sql(cx, args.database, &[], &sql).await?;
        existing = find_module(cx, &args, &module_name).await?;
    }
    let Some(found) = existing else {
        return Err(Error::Udf(format!(
            "module {module_name} disappeared while the function was being created"
        )));
    };
    if found.hash != hash {
        return Err(Error::Udf(format!(
            "module {module_name} already exists with different bytes (sha256 {}); pass \
             --module-name to create this module under another name",
            found.hash
        )));
    }

    // Bound by id, so a module dropped and recreated under the same name since
    // the hash check fails the statement rather than taking its place.
    let sql = format!(
        "{search_path}{}\n",
        create_function_sql(&sig, &name, &found.reference, &args.options)
    );
    if let Err(e) = run_sql(cx, args.database, &[], &sql).await {
        if created {
            eprintln!(
                "note: module {module_name} was created for this function and remains; drop it \
                 with DROP WASM MODULE if it is unused"
            );
        }
        return Err(e);
    }
    spinner.finish_and_clear();
    eprintln!(
        "created {name} from {} in module {module_name} (sha256 {hash})",
        sig.sig,
    );
    Ok(())
}

/// Checks that the local psql has `\bind` (psql 16 and later), which
/// uploading a module needs.
fn require_psql_bind() -> Result<(), Error> {
    let output = std::process::Command::new("psql")
        .arg("--version")
        .output()
        .map_err(|e| Error::CommandExecutionError(e.to_string()))?;
    // For example `psql (PostgreSQL) 16.4`.
    let version = String::from_utf8_lossy(&output.stdout);
    let major = version
        .split_whitespace()
        .find_map(|word| word.split('.').next()?.parse::<u32>().ok());
    match major {
        Some(major) if major >= 16 => Ok(()),
        _ => Err(Error::Udf(format!(
            "uploading a module needs psql 16 or later, for \\bind; found {}",
            version.trim()
        ))),
    }
}

/// A module in a region, as [`find_module`] reports it.
struct FoundModule {
    /// The SQL reference that binds the module by id, `[id AS db.schema.name]`.
    reference: String,
    hash: String,
}

/// The module named `module_name` in the schema `create` targets, if there is
/// one.
async fn find_module(
    cx: &RegionContext,
    args: &CreateArgs<'_>,
    module_name: &str,
) -> Result<Option<FoundModule>, Error> {
    let schema = match args.schema {
        Some(schema) => Sql::literal(schema).as_str().to_string(),
        None => "current_schema()".to_string(),
    };
    let stdout = run_sql(
        cx,
        args.database,
        &["-A", "-t", "-z"],
        &format!(
            "SELECT m.id, m.hash, d.name, s.name FROM mz_catalog.mz_wasm_modules m \
             JOIN mz_catalog.mz_schemas s ON m.schema_id = s.id \
             JOIN mz_catalog.mz_databases d ON s.database_id = d.id \
             WHERE m.name = {} AND s.name = {schema} AND d.name = current_database();\n",
            Sql::literal(module_name).as_str(),
        ),
    )
    .await?;
    let stdout = String::from_utf8_lossy(&stdout);
    let stdout = stdout.trim_end_matches(['\n', '\0']);
    if stdout.is_empty() {
        return Ok(None);
    }
    let fields: Vec<_> = stdout.split('\0').collect();
    let [id, hash, database, schema] = fields.as_slice() else {
        return Err(Error::Udf(format!(
            "unexpected module lookup output: {stdout:?}"
        )));
    };
    Ok(Some(FoundModule {
        reference: format!(
            "[{id} AS {}.{}.{}]",
            Sql::ident(database).as_str(),
            Sql::ident(schema).as_str(),
            Sql::ident(module_name).as_str()
        ),
        hash: hash.to_string(),
    }))
}

/// Arguments for [`pull`].
pub struct PullArgs<'a> {
    /// The SQL name of the function, or of the module if `module` is set.
    pub name: &'a str,
    /// Whether `name` names a module rather than a function.
    pub module: bool,
    /// The database that contains the function or module.
    pub database: Option<&'a str>,
    /// Where to write the module.
    pub output: &'a Path,
}

/// Runs `SHOW CREATE <object_type> <name>` in the region and parses the
/// statement it returns.
async fn show_create(
    cx: &RegionContext,
    database: Option<&str>,
    object_type: &str,
    name: &str,
) -> Result<mz_sql_parser::ast::Statement<mz_sql_parser::ast::Raw>, Error> {
    // `name` goes into a psql script, so it must be exactly one SQL name, not
    // a statement or a psql command.
    let name = mz_sql_parser::parser::parse_item_name(name)
        .map_err(|e| Error::Udf(format!("invalid name {name:?}: {e}")))?;
    // Fields are separated by a zero byte (`-z`), which no name or statement
    // can contain.
    let stdout = run_sql(
        cx,
        database,
        &["-A", "-t", "-z"],
        &format!("SHOW CREATE {object_type} {name};\n"),
    )
    .await?;
    let stdout = String::from_utf8_lossy(&stdout);
    let (_, create_sql) = stdout
        .trim_end()
        .split_once('\0')
        .ok_or_else(|| Error::Udf(format!("unexpected SHOW CREATE output: {stdout}")))?;
    let mut statements = mz_sql_parser::parser::parse_statements(create_sql).map_err(udf_error)?;
    match statements.pop() {
        Some(statement) if statements.is_empty() => Ok(statement.ast),
        _ => Err(Error::Udf(format!(
            "SHOW CREATE {object_type} {name} returned more than one statement"
        ))),
    }
}

/// Downloads a module in the region, named directly or through a function
/// that calls into it.
pub async fn pull(cx: &RegionContext, args: PullArgs<'_>) -> Result<(), Error> {
    use mz_sql_parser::ast::{Statement, WasmModuleBody};

    let module_name = if args.module {
        args.name.to_string()
    } else {
        match show_create(cx, args.database, "FUNCTION", args.name).await? {
            Statement::CreateFunction(stmt) => stmt.module.name().to_string(),
            _ => {
                return Err(Error::Udf(format!(
                    "{} is not a WebAssembly function",
                    args.name
                )));
            }
        }
    };
    let encoded = match show_create(cx, args.database, "WASM MODULE", &module_name).await? {
        Statement::CreateWasmModule(stmt) => match stmt.body {
            WasmModuleBody::Base64(encoded) => encoded,
            WasmModuleBody::Base64Parameter(_) => {
                return Err(Error::Udf(format!(
                    "SHOW CREATE WASM MODULE {module_name} returned a parameter, not the module"
                )));
            }
        },
        _ => {
            return Err(Error::Udf(format!(
                "{module_name} is not a WebAssembly module"
            )));
        }
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(udf_error)?;
    // A module in a region was valid when it was created, but a later, stricter
    // validator may reject it. It is still the module that runs, so it is
    // written either way.
    std::fs::write(args.output, &bytes)?;
    eprintln!(
        "wrote {} bytes to {} (sha256 {})",
        bytes.len(),
        args.output.display(),
        mz_expr::func::WasmModuleHash(mz_wasm_udf_abi::module_hash(&bytes))
    );
    if let Err(e) = ModuleInfo::parse(&bytes) {
        eprintln!("warning: this version of mz does not accept the module: {e}");
    }
    Ok(())
}

/// Arguments for [`replay`].
pub struct ReplayArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// The guest function, by name or by signature.
    pub function: &'a str,
    /// A query whose columns are the function's arguments.
    pub query: &'a str,
    /// The database to run the query in.
    pub database: Option<&'a str>,
    /// Limits and batching.
    pub options: CallOptions,
}

/// A row whose call failed during [`replay`].
#[derive(Deserialize, Serialize, Tabled)]
pub struct FailedRow {
    /// The arguments, as SQL literals.
    pub input: String,
    /// The error.
    pub error: String,
}

/// Runs `query` in the region and calls the function locally on each row it
/// returns, reporting the rows whose calls fail.
pub async fn replay(cx: &RegionContext, args: ReplayArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    // The query goes into a psql script, so it must be exactly one SELECT,
    // not further statements or psql commands.
    let mut statements = mz_sql_parser::parser::parse_statements(args.query).map_err(udf_error)?;
    let query = match (statements.pop().map(|s| s.ast), statements.is_empty()) {
        (Some(query @ mz_sql_parser::ast::Statement::Select(_)), true) => query.to_string(),
        _ => {
            return Err(Error::Udf(
                "--query must be a single SELECT statement".into(),
            ));
        }
    };
    let stdout = run_sql(
        cx,
        args.database,
        &[],
        &format!("COPY ({query}) TO STDOUT;\n"),
    )
    .await?;
    let rows = mz_pgcopy::decode_copy_format(
        &stdout,
        &sig.pg_arg_types(),
        CopyFormatParams::Text(CopyTextFormatParams::default()),
    )
    .map_err(|e| {
        Error::Udf(format!(
            "the query's columns must match the arguments of {}: {e}",
            sig.sig
        ))
    })?;

    let stats = Arc::new(Stats::default());
    let batch_rows = args
        .options
        .batch_rows
        .unwrap_or(BatchConfig::DEFAULT_MAX_ROWS);
    let invoker = invoker(&module, &sig, &args.options, batch_rows, &stats)?;
    let arena = RowArena::new();
    let results = call_rows(&invoker, &rows, &arena, args.options.strict);
    let failures: Vec<_> = rows
        .iter()
        .zip_eq(&results)
        .filter_map(|(row, result)| {
            result.as_ref().err().map(|e| FailedRow {
                input: format_row(row, &sig.args),
                error: e.to_string(),
            })
        })
        .collect();
    let failed = failures.len();
    cx.output_formatter().output_table(failures)?;
    stats.report();
    eprintln!("{failed} of {} row(s) failed", rows.len());
    Ok(())
}

/// The default output path for [`pull`].
pub fn default_pull_path(name: &str) -> PathBuf {
    PathBuf::from(format!("{}.wasm", name.rsplit('.').next().unwrap_or(name)))
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::LazyLock;

    use super::*;

    static FIXTURE: LazyLock<Module> = LazyLock::new(|| {
        let gz: &[u8] = include_bytes!("../../../wasm-udf/tests/fixtures/udfs.wasm.gz");
        let mut bytes = Vec::new();
        flate2::read::GzDecoder::new(gz)
            .read_to_end(&mut bytes)
            .expect("valid gzip");
        Module::from_bytes(bytes).expect("fixture compiles")
    });

    fn sql_signature(args: Vec<UdfType>) -> SqlSignature {
        SqlSignature::new(ScalarSignature {
            name: "f".into(),
            args,
            ret: UdfType::Int32,
        })
    }

    fn generated(nulls: bool) -> CheckInput<'static> {
        CheckInput::Generated {
            rows: 256,
            seed: 0,
            nulls,
        }
    }

    fn check(function: &str) -> Result<(), Error> {
        let options = CallOptions::default();
        let sig = SqlSignature::new(FIXTURE.signature(function)?);
        determinism(&FIXTURE, &sig, &generated(true), &options)
    }

    #[mz_ore::test]
    fn generates_rows_of_every_type() {
        let scalars = [
            UdfType::Boolean,
            UdfType::Int16,
            UdfType::Int32,
            UdfType::Int64,
            UdfType::UInt16,
            UdfType::UInt32,
            UdfType::UInt64,
            UdfType::Float32,
            UdfType::Float64,
            UdfType::Decimal,
            UdfType::Json,
            UdfType::String,
            UdfType::Binary,
            UdfType::Date32,
            UdfType::Time64,
            UdfType::Timestamp,
            UdfType::Interval,
        ];
        let mut args = scalars.to_vec();
        args.extend(scalars.iter().map(|t| UdfType::List(Box::new(t.clone()))));
        args.push(UdfType::List(Box::new(UdfType::List(Box::new(
            UdfType::Int32,
        )))));
        let sig = sql_signature(args);
        for seed in 0..8 {
            let rows = generate_rows(&sig, 512, seed, true).unwrap();
            assert_eq!(rows.len(), 512);
        }
    }

    #[mz_ore::test]
    fn create_statements_parse() {
        use mz_sql_parser::ast::Statement;

        let module = create_module_psql(&FIXTURE, "my udfs");
        let (statement, _) = module.split_once(" \\bind ").expect("psql bind");
        match mz_sql_parser::parser::parse_statements(statement).unwrap()[0].ast {
            Statement::CreateWasmModule(ref stmt) => {
                assert_eq!(stmt.name.to_string(), "\"my udfs\"");
                assert!(stmt.if_not_exists);
            }
            ref other => panic!("not CREATE WASM MODULE: {other}"),
        }

        let sig = SqlSignature::new(FIXTURE.signature("gcd").unwrap());
        let options = CallOptions {
            strict: true,
            fuel: Some(1000),
            memory: Some(1 << 20),
            ..Default::default()
        };
        let function =
            create_function_sql(&sig, "my gcd", Sql::ident("my udfs").as_str(), &options);
        match mz_sql_parser::parser::parse_statements(&function).unwrap()[0].ast {
            Statement::CreateFunction(ref stmt) => {
                assert_eq!(stmt.module.name().to_string(), "\"my udfs\"");
                assert_eq!(stmt.with_options.len(), 3, "EXPORT, FUEL and MEMORY");
            }
            ref other => panic!("not CREATE FUNCTION: {other}"),
        }

        // `create` binds the module by id.
        let function = create_function_sql(
            &sig,
            "gcd",
            "[u5 AS materialize.public.\"my udfs\"]",
            &options,
        );
        match mz_sql_parser::parser::parse_statements(&function).unwrap()[0].ast {
            Statement::CreateFunction(ref stmt) => {
                assert!(
                    matches!(stmt.module, mz_sql_parser::ast::RawItemName::Id(ref id, _, _) if id == "u5"),
                    "{function}"
                );
            }
            ref other => panic!("not CREATE FUNCTION: {other}"),
        }
    }

    #[mz_ore::test]
    fn generation_is_reproducible() {
        let sig = sql_signature(vec![UdfType::String, UdfType::Float64]);
        let a = generate_rows(&sig, 64, 7, true).unwrap();
        let b = generate_rows(&sig, 64, 7, true).unwrap();
        let c = generate_rows(&sig, 64, 8, true).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[mz_ore::test]
    fn strict_generation_has_no_nulls() {
        let sig = sql_signature(vec![UdfType::Int32, UdfType::String]);
        let rows = generate_rows(&sig, 512, 0, false).unwrap();
        assert!(rows.iter().flat_map(|r| r.iter()).all(|d| !d.is_null()));
    }

    #[mz_ore::test]
    fn check_accepts_pure_functions() {
        for function in ["gcd", "reverse", "distinct_chars", "safe_div"] {
            check(function).unwrap_or_else(|e| panic!("{function}: {e}"));
        }
    }

    #[mz_ore::test]
    fn strict_calls_skip_null_rows() {
        let sig = SqlSignature::new(FIXTURE.signature("nullable_or_zero").unwrap());
        let options = CallOptions::default();
        let invoker = invoker(&FIXTURE, &sig, &options, 16, &Arc::default()).unwrap();
        let rows = [Row::pack([Datum::Int32(5)]), Row::pack([Datum::Null])];
        let arena = RowArena::new();
        let lenient = call_rows(&invoker, &rows, &arena, false);
        assert_eq!(lenient, vec![Ok(Datum::Int32(5)), Ok(Datum::Int32(0))]);
        let strict = call_rows(&invoker, &rows, &arena, true);
        assert_eq!(strict, vec![Ok(Datum::Int32(5)), Ok(Datum::Null)]);
    }

    #[mz_ore::test]
    fn check_requires_a_successful_row() {
        // Below the fixed cost of one arrow-udf call, so every call fails
        // the same way in every arrangement.
        let options = CallOptions {
            fuel: Some(1_000),
            ..Default::default()
        };
        let sig = SqlSignature::new(FIXTURE.signature("gcd").unwrap());
        let err = determinism(&FIXTURE, &sig, &generated(false), &options)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no row of the input succeeded"), "{err}");
    }

    #[mz_ore::test]
    fn check_requires_rows_the_guest_answers() {
        let sig = SqlSignature::new(FIXTURE.signature("gcd").unwrap());
        let empty = CheckInput::Generated {
            rows: 0,
            seed: 0,
            nulls: false,
        };
        let err = determinism(&FIXTURE, &sig, &empty, &CallOptions::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("no rows"), "{err}");

        // Every row has a NULL argument, so a strict function never calls the
        // guest and every row agrees without showing anything.
        let path = std::env::temp_dir().join(format!("udf-nulls-{}.csv", std::process::id()));
        std::fs::write(&path, ",1\n2,\n").unwrap();
        let options = CallOptions {
            strict: true,
            ..Default::default()
        };
        let err = determinism(&FIXTURE, &sig, &CheckInput::Csv(&path), &options)
            .unwrap_err()
            .to_string();
        std::fs::remove_file(&path).unwrap();
        assert!(err.contains("no row of the input succeeded"), "{err}");
    }

    #[mz_ore::test]
    fn nested_lists_are_sql_lists() {
        let nested = UdfType::List(Box::new(UdfType::List(Box::new(UdfType::Int32))));
        assert_eq!(
            sql_type_name(&nested.default_sql_type()),
            "integer list list"
        );
        let flat = UdfType::List(Box::new(UdfType::Int32));
        assert_eq!(sql_type_name(&flat.default_sql_type()), "integer[]");
    }

    #[mz_ore::test]
    fn check_rejects_batch_dependent_functions() {
        for function in ["stateful", "letters"] {
            let err = check(function).expect_err(function).to_string();
            assert!(err.contains("depend on batching"), "{function}: {err}");
        }
    }
}
