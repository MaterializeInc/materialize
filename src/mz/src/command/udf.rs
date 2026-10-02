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
use mz_pgcopy::{CopyCsvFormatParams, CopyFormatParams, CopyTextFormatParams};
use mz_postgres_util::Sql;
use mz_repr::{Datum, Row, RowArena, SqlScalarType};
use mz_wasm_udf::codec::ValueType;
use mz_wasm_udf::{BatchConfig, CallObserver, CompiledModule, Invoker};
use mz_wasm_udf_abi::{ModuleInfo, ScalarSignature};
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
        let bytes = std::fs::read(path)?;
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
        typ => mz_pgrepr::Type::from(typ).name().to_string(),
    }
}

/// A psql command that runs the `CREATE FUNCTION` statement binding `name` to
/// `sig` in `module`.
///
/// The module is sent as a bind parameter (`\bind`, psql 16 and later),
/// because the statement text has a size limit that most modules exceed.
fn create_function_psql(module: &Module, sig: &SqlSignature, name: &str, strict: bool) -> String {
    let args: Vec<_> = sig.args.iter().map(sql_type_name).collect();
    let mut sql = format!(
        "CREATE FUNCTION {}({}) RETURNS {} LANGUAGE wasm",
        Sql::ident(name).as_str(),
        args.join(", "),
        sql_type_name(&sig.ret),
    );
    if strict {
        sql.push_str(" STRICT");
    }
    sql.push_str(" USING BASE64 $1");
    if sig.sig.name != name {
        sql.push_str(&format!(
            " WITH (EXPORT = {})",
            Sql::literal(&sig.sig.name).as_str()
        ));
    }
    // Base64 has no quote or backslash characters, so it needs no escaping
    // inside a psql single-quoted argument.
    let encoded = base64::engine::general_purpose::STANDARD.encode(&module.bytes);
    sql.push_str(&format!(" \\bind '{encoded}' \\g"));
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
pub struct CallOptions {
    /// The fuel each guest call may use, or the region default.
    pub fuel: Option<u64>,
    /// The memory limit of each guest call in bytes, or the region default.
    pub memory: Option<u64>,
    /// The most rows one guest call may carry, or the region default.
    pub batch_rows: Option<usize>,
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

/// Calls the function on `rows` and returns one result per row.
fn call_rows<'a>(
    invoker: &Invoker,
    rows: &'a [Row],
    arena: &'a RowArena,
) -> Vec<Result<Datum<'a>, EvalError>> {
    let datums: Vec<Vec<Datum>> = rows.iter().map(|r| r.iter().collect()).collect();
    let args: Vec<&[Datum]> = datums.iter().map(|d| d.as_slice()).collect();
    let mut out = Vec::with_capacity(rows.len());
    invoker.call_batch(&args, arena, &mut out);
    out
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
    let results = call_rows(&invoker, &rows, &arena);
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

/// Arguments for [`check`].
pub struct CheckArgs<'a> {
    /// The module file.
    pub module: &'a Path,
    /// A function and a CSV file of argument rows, to check the function's
    /// per-row contract on.
    pub determinism: Option<(&'a str, &'a Path)>,
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
    let rows = read_csv(input, &sig)?;
    determinism(&module, &sig, &rows, &args.options)
}

/// Evaluates `rows` in one batch, one row per call, in odd-sized batches, and
/// in reverse order, and fails if any row's result differs between them.
fn determinism(
    module: &Module,
    sig: &SqlSignature,
    rows: &[Row],
    options: &CallOptions,
) -> Result<(), Error> {
    let run = |batch_rows: usize, reverse: bool| -> Result<Vec<String>, Error> {
        let stats = Arc::new(Stats::default());
        let invoker = invoker(module, sig, options, batch_rows, &stats)?;
        let arena = RowArena::new();
        let mut ordered: Vec<Row> = rows.to_vec();
        if reverse {
            ordered.reverse();
        }
        let mut results: Vec<String> = call_rows(&invoker, &ordered, &arena)
            .iter()
            .map(|r| format_result(r, &sig.ret))
            .collect();
        if reverse {
            results.reverse();
        }
        Ok(results)
    };

    let baseline = run(rows.len().max(1), false)?;
    let variants = [
        ("one row per call", run(1, false)?),
        ("batches of 7", run(7, false)?),
        ("reversed", run(rows.len().max(1), true)?),
    ];
    let mut mismatches = 0;
    for (name, results) in &variants {
        for (i, (expected, actual)) in baseline.iter().zip_eq(results).enumerate() {
            if expected != actual {
                mismatches += 1;
                eprintln!(
                    "row {} {}: {expected} in one batch, {actual} with {name}",
                    i + 1,
                    format_row(&rows[i], &sig.args),
                );
            }
        }
    }
    if mismatches > 0 {
        return Err(Error::Udf(format!(
            "{} returns results that depend on batching; its output for a row must depend \
             only on that row",
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
    /// Whether to declare the function `STRICT`.
    pub strict: bool,
}

/// Prints the psql command that creates a module's function.
pub fn sql(args: SqlArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    let name = args.name.unwrap_or(&sig.sig.name).to_string();
    println!(
        "{}",
        create_function_psql(&module, &sig, &name, args.strict)
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
    let mut command = cx.sql_client().shell(&region_info, user, None);
    if let Some(database) = database {
        command.args(["-d", database]);
    }
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
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(sql.as_bytes())?;
    let output = child
        .wait_with_output()
        .map_err(|e| Error::CommandExecutionError(e.to_string()))?;
    if !output.status.success() {
        return Err(Error::CommandFailed(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
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
    /// The database to create the function in.
    pub database: Option<&'a str>,
    /// The schema to create the function in.
    pub schema: Option<&'a str>,
    /// Whether to declare the function `STRICT`.
    pub strict: bool,
}

/// Creates a function in the region from a local module.
pub async fn create(cx: &RegionContext, args: CreateArgs<'_>) -> Result<(), Error> {
    let module = Module::load(args.module)?;
    let sig = SqlSignature::new(module.signature(args.function)?);
    let name = args.name.unwrap_or(&sig.sig.name).to_string();
    let mut sql = String::new();
    if let Some(schema) = args.schema {
        sql.push_str(&format!(
            "SET search_path TO {};\n",
            Sql::ident(schema).as_str()
        ));
    }
    sql.push_str(&create_function_psql(&module, &sig, &name, args.strict));
    sql.push('\n');

    let spinner = cx
        .output_formatter()
        .loading_spinner("Creating function...");
    run_sql(cx, args.database, &[], &sql).await?;
    spinner.finish_and_clear();
    eprintln!(
        "created {name} from {} (sha256 {})",
        sig.sig,
        mz_expr::func::WasmModuleHash(module.info().hash)
    );
    Ok(())
}

/// Arguments for [`pull`].
pub struct PullArgs<'a> {
    /// The function's SQL name.
    pub name: &'a str,
    /// The database that contains the function.
    pub database: Option<&'a str>,
    /// Where to write the module.
    pub output: &'a Path,
}

/// Downloads the module behind a function in the region.
pub async fn pull(cx: &RegionContext, args: PullArgs<'_>) -> Result<(), Error> {
    let stdout = run_sql(
        cx,
        args.database,
        &["-A", "-t"],
        &format!("SHOW CREATE FUNCTION {};\n", args.name),
    )
    .await?;
    let stdout = String::from_utf8_lossy(&stdout);
    let (_, rest) = stdout
        .split_once("USING BASE64 '")
        .ok_or_else(|| Error::Udf(format!("{} is not a WebAssembly function", args.name)))?;
    let (encoded, _) = rest
        .split_once('\'')
        .ok_or_else(|| Error::Udf("malformed CREATE FUNCTION statement".into()))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(udf_error)?;
    let info = ModuleInfo::parse(&bytes).map_err(udf_error)?;
    std::fs::write(args.output, &bytes)?;
    eprintln!(
        "wrote {} bytes to {} (sha256 {})",
        bytes.len(),
        args.output.display(),
        mz_expr::func::WasmModuleHash(info.hash)
    );
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
    let query = args.query.trim().trim_end_matches(';');
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
    let results = call_rows(&invoker, &rows, &arena);
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
