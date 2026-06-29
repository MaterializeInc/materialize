// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Executes a text command script against `clusterd`.
//!
//! Instead of recompiling a Rust scenario, a test (or an agent) writes a
//! [`crate::text`] script: a sequence of commands, each with an expected output
//! block (`----`) that is the assertion. The coarse orchestration verbs map almost
//! directly to [`Driver`] calls; `define` carries arbitrary MIR (as pretty-form
//! specs parsed by `mz-expr-parser`, the `.spec` test syntax) over the full
//! [`DataflowBuilder`] surface, including index imports, while `define_index`
//! stays as sugar for the common single-index shape. Explicit `write_rows`
//! payloads are typed against the schema token-by-token via `cell_from_token`
//! (reusing `mz_repr::strconv`) rather than `Row`'s opaque serde.
//!
//! # Execution
//!
//! [`run`] parses the script, executes each command, and compares its golden
//! output to the expected block — failing the run on a mismatch, or rewriting the
//! file when `REWRITE` is set. A command that fails renders as `error: <message>`,
//! so an expected failure is asserted by its golden block. Assertions are
//! level-triggered waits on monotonic frontiers, so a single sequential script is
//! deterministic regardless of how the dataflows interleave.
//!
//! Shards are referenced by a string alias; the first command naming an alias
//! allocates a fresh [`ShardId`] for it. Object ids are raw `u64`s mapped to
//! [`GlobalId::User`].

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use mz_compute_client::protocol::command::{ComputeCommand, PeekTarget};
use mz_dyncfg::{ConfigType, ConfigUpdates, ConfigVal};
use mz_expr::visit::Visit;
use mz_expr::{Id, MirRelationExpr};
use mz_expr_parser::{TestCatalog, try_parse_mir};
use mz_persist_client::PersistClient;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{
    GlobalId, RelationDesc, ReprRelationType, Row, SqlColumnType, SqlRelationType, SqlScalarType,
    Timestamp, strconv,
};
use mz_storage_types::controller::CollectionMetadata;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::data::{
    Cell, pack_cells, sample_desc, synth_rows, write_rows_single_ts, write_rows_spread,
};
use crate::dataflow::{
    DataflowBuilder, PersistSink, PersistSource, count_over_index, index_dataflow,
};
use crate::driver::Driver;

/// The default payload padding (bytes) for synthetic rows when a command omits it.
const DEFAULT_ROW_BYTES: usize = 64;
/// The default timeout (seconds) for `await_frontier` when a command omits it.
const DEFAULT_TIMEOUT_SECS: u64 = 600;

/// A column declaration in a `define_schema` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSpec {
    /// Column name.
    pub name: String,
    /// Scalar type name; see `scalar_type_from_str`.
    #[serde(rename = "type")]
    pub ty: String,
    /// Whether the column admits `NULL`.
    #[serde(default)]
    pub nullable: bool,
}

/// A single dyncfg update in an `update-configuration` command: a config name, a
/// type tag selecting how `value` is parsed (`bool`/`u32`/`usize`/`f64`/`string`/
/// `duration`), and the value. Typed against [`mz_dyncfg`] at execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSetting {
    /// The dyncfg name (sent to the replica by name; unknown names are ignored).
    pub name: String,
    /// The type tag selecting the [`ConfigVal`] variant.
    #[serde(rename = "type")]
    pub ty: String,
    /// The value, parsed against `ty`.
    pub value: String,
}

/// A collection to import in a `define` command: a persist source or an existing
/// index. Externally tagged: `{"source": {…}}` or `{"index": {…}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportSpec {
    /// Import a persist-backed storage collection, as `define_index` does.
    Source {
        /// The imported source's global id.
        id: u64,
        /// Shard alias to import; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The shard's exclusive write upper (see `PersistSource::upper`).
        upper: u64,
    },
    /// Import an existing index by its global id; its arranged collection, key,
    /// and type are taken from the registry, so it must have been defined first.
    Index {
        /// The index's global id.
        index_id: u64,
    },
}

/// A MIR object to build in a `define` command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildSpec {
    /// The built object's global id.
    pub id: u64,
    /// The computation, as a pretty-form MIR spec parsed by `mz-expr-parser`
    /// (e.g. `Reduce aggregates=[count(*)]` over `Get u1000`). It references
    /// imported or previously-built objects by their global-id name (`u<n>`); the
    /// leaf `Get`'s type is resolved from the import, not authored.
    pub expr: String,
}

/// An export in a `create-dataflow` command, mirroring the export kinds a real
/// dataflow produces (see [`mz_compute_types::sinks::ComputeSinkConnection`]).
/// `copy-to` is intentionally absent: the parser rejects it as unimplemented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExportSpec {
    /// An arrangement, peekable as an index and importable by later dataflows.
    Index {
        /// The exported index's global id.
        index_id: u64,
        /// The imported or built id the index arranges.
        on_id: u64,
        /// Columns to arrange by.
        key: Vec<usize>,
    },
    /// A persist sink writing the collection to a shard (a materialized view),
    /// verified by reading the shard back with a persist `peek` of the sink id.
    MaterializedView {
        /// The sink's global id (scheduled and frontier-tracked under this id).
        sink_id: u64,
        /// The imported or built id the sink writes.
        on_id: u64,
        /// Target shard alias; allocated on first use.
        shard: String,
        /// Output schema; defaults to the sample schema. Must match `on_id`'s type.
        schema: Option<String>,
    },
    /// A subscribe sink streaming changes back as responses, collected by
    /// `await-subscribe`.
    Subscribe {
        /// The sink's global id.
        sink_id: u64,
        /// The imported or built id the sink streams.
        on_id: u64,
        /// Output schema; defaults to the sample schema. Must match `on_id`'s type.
        schema: Option<String>,
        /// Exclusive upper at which the subscribe completes; unbounded if absent.
        up_to: Option<u64>,
    },
}

/// Map a JSON type name to a [`SqlScalarType`]. The supported set is intentionally
/// small and matches [`crate::data::Cell`]; extend both together.
fn scalar_type_from_str(s: &str) -> anyhow::Result<SqlScalarType> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "int16" | "smallint" => SqlScalarType::Int16,
        "int32" | "int" | "integer" => SqlScalarType::Int32,
        "int64" | "bigint" => SqlScalarType::Int64,
        "bool" | "boolean" => SqlScalarType::Bool,
        "string" | "text" => SqlScalarType::String,
        "bytes" | "bytea" => SqlScalarType::Bytes,
        other => anyhow::bail!("unsupported column type {other:?}"),
    })
}

/// Parse a configuration value string into a [`ConfigVal`], selecting the variant
/// by a type tag. Each numeric/bool/duration type reuses [`mz_dyncfg`]'s own
/// `ConfigType::parse` (so the accepted syntax — e.g. `on`/`off` for bool, humantime
/// for duration — matches the rest of the codebase); `string` is taken verbatim.
fn parse_config_val(ty: &str, value: &str) -> anyhow::Result<ConfigVal> {
    let err = |e: String| anyhow::anyhow!("config value {value:?} is not a valid {ty}: {e}");
    Ok(match ty {
        "bool" => <bool as ConfigType>::parse(value).map_err(err)?.into(),
        "u32" => <u32 as ConfigType>::parse(value).map_err(err)?.into(),
        "usize" => <usize as ConfigType>::parse(value).map_err(err)?.into(),
        "f64" => <f64 as ConfigType>::parse(value).map_err(err)?.into(),
        "duration" => <Duration as ConfigType>::parse(value).map_err(err)?.into(),
        "string" => ConfigVal::String(value.to_string()),
        other => anyhow::bail!(
            "unsupported config type {other:?}; use bool/u32/usize/f64/duration/string"
        ),
    })
}

/// Build a [`RelationDesc`] from column specs.
fn relation_desc(columns: &[ColumnSpec]) -> anyhow::Result<RelationDesc> {
    let mut builder = RelationDesc::builder();
    for col in columns {
        builder = builder.with_column(
            col.name.as_str(),
            SqlColumnType {
                scalar_type: scalar_type_from_str(&col.ty)?,
                nullable: col.nullable,
            },
        );
    }
    Ok(builder.finish())
}

/// Strip surrounding double quotes from a token, if present.
fn unquote(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

/// Type a raw row-value token against its column into an owned [`Cell`].
///
/// The bare token `null` is SQL `NULL` (only in a nullable column); quote it
/// (`"null"`) for the literal string. Numeric and boolean tokens go through
/// [`mz_repr::strconv`] — the canonical PostgreSQL-compatible text parser the rest
/// of the codebase uses — so the accepted syntax matches `mz_pgrepr`'s text decode.
/// `string`/`bytes` columns take the (unquoted) token verbatim; `bytes` is its
/// UTF-8 encoding.
fn cell_from_token(token: &str, col: &SqlColumnType) -> anyhow::Result<Cell> {
    if token == "null" {
        anyhow::ensure!(col.nullable, "null value in non-nullable column");
        return Ok(Cell::Null);
    }
    let parse =
        |kind: &str, e: strconv::ParseError| anyhow::anyhow!("parsing {token:?} as {kind}: {e}");
    let cell = match col.scalar_type {
        SqlScalarType::Int16 => {
            Cell::Int16(strconv::parse_int16(token).map_err(|e| parse("int16", e))?)
        }
        SqlScalarType::Int32 => {
            Cell::Int32(strconv::parse_int32(token).map_err(|e| parse("int32", e))?)
        }
        SqlScalarType::Int64 => {
            Cell::Int64(strconv::parse_int64(token).map_err(|e| parse("int64", e))?)
        }
        SqlScalarType::Bool => {
            Cell::Bool(strconv::parse_bool(token).map_err(|e| parse("bool", e))?)
        }
        SqlScalarType::String => Cell::Str(unquote(token).to_string()),
        SqlScalarType::Bytes => Cell::Bytes(unquote(token).as_bytes().to_vec()),
        ref other => anyhow::bail!("unsupported column type {other:?}"),
    };
    Ok(cell)
}

/// Pack explicit row tokens against `desc`, validating arity per row.
fn rows_from_tokens(desc: &RelationDesc, rows: &[Vec<String>]) -> anyhow::Result<Vec<Row>> {
    let cols: Vec<&SqlColumnType> = desc.iter_types().collect();
    let mut out = Vec::with_capacity(rows.len());
    for (r, row) in rows.iter().enumerate() {
        anyhow::ensure!(
            row.len() == cols.len(),
            "row {r} has {} values but schema has {} columns",
            row.len(),
            cols.len()
        );
        // Arity validated above, so indexing `cols` by position is in bounds.
        let cells = row
            .iter()
            .enumerate()
            .map(|(c, v)| cell_from_token(v, cols[c]))
            .collect::<anyhow::Result<Vec<Cell>>>()?;
        out.push(pack_cells(&cells));
    }
    Ok(out)
}

/// Register a referenceable object in the parser `catalog` under its global-id
/// name (e.g. `u1000`), recording the name-to-id mapping so the parsed `Get`s —
/// which carry the catalog's own assigned ids — can be remapped back to the
/// script's ids.
fn register_catalog_object(
    catalog: &mut TestCatalog,
    name_to_id: &mut BTreeMap<String, GlobalId>,
    id: GlobalId,
    sql_typ: SqlRelationType,
) -> anyhow::Result<()> {
    let name = id.to_string();
    // Column names are only used for display; `Get` references columns by `#n`,
    // so synthetic `c0..cN` names suffice.
    let cols = (0..sql_typ.column_types.len())
        .map(|i| format!("c{i}"))
        .collect();
    catalog
        .insert(&name, cols, sql_typ, false)
        .map_err(|e| anyhow::anyhow!("registering {name} in catalog: {e}"))?;
    name_to_id.insert(name, id);
    Ok(())
}

/// Rewrite every global `Get` in `expr` from the catalog's assigned id back to
/// the script's id, looked up by the object's name.
fn remap_gets(
    expr: &mut MirRelationExpr,
    catalog: &TestCatalog,
    name_to_id: &BTreeMap<String, GlobalId>,
) -> anyhow::Result<()> {
    expr.try_visit_mut_post::<_, anyhow::Error>(&mut |e| {
        if let MirRelationExpr::Get {
            id: Id::Global(g), ..
        } = e
        {
            let name = catalog
                .get_source_name(g)
                .ok_or_else(|| anyhow::anyhow!("get of unknown catalog object {g}"))?;
            let id = name_to_id
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("get of unregistered object {name}"))?;
            *g = *id;
        }
        Ok(())
    })
}

/// A command read from the script stream.
///
/// Tagged on `"cmd"`, snake_case, e.g.
/// `{"cmd":"write_single_ts","shard":"s1","ts":0,"rows":1000}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Declare a named relation schema for later `schema` references.
    DefineSchema {
        /// Schema name, referenced by `schema` fields on other commands.
        name: String,
        /// Ordered column declarations.
        columns: Vec<ColumnSpec>,
    },
    /// Write `count` synthetic rows to `shard` at a single timestamp `ts`.
    WriteSingleTs {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in `(bigint, text)` sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to write at.
        ts: u64,
        /// Number of synthetic rows to write.
        count: u64,
        /// First synthetic row index, so successive batches can use disjoint id
        /// ranges (`start..start + count`) that never consolidate. Defaults to 0.
        #[serde(default)]
        start: u64,
        /// Payload padding per row; defaults to `DEFAULT_ROW_BYTES`.
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Write `count` synthetic rows to `shard`, spread across `n_ts` timestamps in a
    /// single append.
    WriteSpread {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// Number of synthetic rows to write.
        count: u64,
        /// Number of distinct timestamps to spread the rows across.
        n_ts: u64,
        /// First synthetic row index (see [`Command::WriteSingleTs`]). Defaults to 0.
        #[serde(default)]
        start: u64,
        /// Payload padding per row; defaults to `DEFAULT_ROW_BYTES`.
        #[serde(default)]
        row_bytes: Option<usize>,
    },
    /// Write explicit rows to `shard` at a single timestamp `ts`. Each row is an
    /// array of JSON values matching the schema's columns in order.
    WriteRows {
        /// Shard alias; allocated on first use.
        shard: String,
        /// Schema name; defaults to the built-in sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to write at.
        ts: u64,
        /// Rows as arrays of raw value tokens, typed against the schema by
        /// `cell_from_token`.
        rows: Vec<Vec<String>>,
    },
    /// Submit (without scheduling) an index dataflow over `shard`.
    DefineIndex {
        /// The imported source's global id.
        source_id: u64,
        /// The exported index's global id.
        index_id: u64,
        /// Shard alias to import; must already exist.
        shard: String,
        /// Schema name; defaults to the built-in sample schema. Must match what was
        /// written to `shard`.
        #[serde(default)]
        schema: Option<String>,
        /// Columns to arrange by.
        key: Vec<usize>,
        /// The dataflow's `as_of`.
        as_of: u64,
        /// The shard's exclusive write upper (see `PersistSource::upper`).
        upper: u64,
    },
    /// Schedule a previously-submitted collection so it makes progress.
    Schedule {
        /// The collection's global id.
        id: u64,
    },
    /// Advance an index's read frontier (`since`) via `AllowCompaction`.
    AllowCompaction {
        /// The index's global id.
        id: u64,
        /// The new read frontier.
        frontier: u64,
    },
    /// Take a collection out of read-only mode via `AllowWrites`, letting its
    /// persist sink begin writing. Every dataflow starts read-only; indexes,
    /// subscribes, and peeks work regardless, but a materialized-view sink withholds
    /// all persist writes until this is sent for its sink id.
    AllowWrites {
        /// The sink's global id.
        id: u64,
    },
    /// Wait until `id`'s output frontier reaches `ts`, or fail after the timeout.
    AwaitFrontier {
        /// The collection's global id.
        id: u64,
        /// The target output-frontier timestamp.
        ts: u64,
        /// Timeout in seconds; defaults to `DEFAULT_TIMEOUT_SECS`.
        #[serde(default)]
        timeout_secs: Option<u64>,
        /// If true, a timeout is reported (`status: timeout`) without failing the
        /// run. Used by reproductions where not reaching the frontier is an
        /// expected outcome, not an assertion failure.
        #[serde(default)]
        allow_timeout: bool,
    },
    /// Count `id`'s rows at `ts` and emit the count.
    ///
    /// Sugar over a `Reduce`: builds an ephemeral dataflow that index-imports `id`,
    /// computes `count(*)` over it, and peeks the single-row result — so the count
    /// runs through a real reduce operator rather than being tallied in the driver.
    /// `id` must have been registered by a prior `define_index` (or `define`
    /// export). The golden output is the count; the script's `----` block asserts it.
    Count {
        /// The index's global id.
        id: u64,
        /// The timestamp to count at.
        ts: u64,
    },
    /// Submit (without scheduling) a dataflow built from generic MIR — the
    /// abstraction behind index / materialized-view / subscribe / copy-to.
    ///
    /// A projection of [`DataflowBuilder`]: import sources and/or existing indexes,
    /// build MIR objects (each a pretty-form MIR spec; see [`BuildSpec`]), and
    /// export over them. Exports are index, materialized-view, or subscribe (see
    /// [`ExportSpec`]); copy-to is not implemented. Exported indexes are registered
    /// for later import or count assertion; subscribe sinks register a response
    /// buffer for `await-subscribe`. `define-index` is sugar over this. The optional
    /// `optimize` flag runs the MIR optimizer before lowering (needed for joins).
    CreateDataflow {
        /// Debug name for the dataflow; defaults to `headless-create-dataflow`.
        #[serde(default)]
        name: Option<String>,
        /// Collections to import (persist sources and/or existing indexes).
        #[serde(default)]
        imports: Vec<ImportSpec>,
        /// MIR objects to compute, each bound to an id.
        #[serde(default)]
        builds: Vec<BuildSpec>,
        /// Exports over imported or built ids.
        #[serde(default)]
        exports: Vec<ExportSpec>,
        /// The dataflow's `as_of`.
        as_of: u64,
        /// Run the MIR optimizer before lowering (needed for e.g. joins). Off by
        /// default, so the caller's MIR is lowered faithfully.
        #[serde(default)]
        optimize: bool,
    },
    /// Peek `id` at `ts` and emit the returned rows (sorted, one per line). The
    /// generic output assertion: the script's `----` block holds the expected rows.
    Peek {
        /// The index's global id.
        id: u64,
        /// Schema name describing the peek's output; defaults to the sample schema.
        #[serde(default)]
        schema: Option<String>,
        /// The timestamp to peek at.
        ts: u64,
    },
    /// Wait for subscribe sink `id`'s upper to reach `up_to`, then emit its
    /// accumulated updates as `<ts> <diff> <datums>` lines (consolidated, sorted).
    /// The output assertion for a subscribe sink.
    AwaitSubscribe {
        /// The subscribe sink's global id.
        id: u64,
        /// The exclusive upper to wait for (typically the sink's `up_to`).
        up_to: u64,
        /// Timeout in seconds; defaults to `DEFAULT_TIMEOUT_SECS`.
        timeout_secs: Option<u64>,
    },
    /// Send `CreateInstance`, opening the compute instance (and the reconciliation
    /// window). The settable [`InstanceConfig`] knobs default to the values a plain
    /// `create-instance` supplies; the peek-stash location is always the host's, and
    /// the peek-response stash is force-disabled (see `Driver::create_instance`).
    ///
    /// [`InstanceConfig`]: mz_compute_client::protocol::command::InstanceConfig
    CreateInstance {
        /// Replica expiration offset (a duration like `30s`); none if absent.
        #[serde(default)]
        expiration_offset: Option<String>,
        /// Whether arrangements use dictionary compression.
        #[serde(default)]
        arrangement_dictionary_compression: bool,
        /// The create-time dyncfg snapshot the controller would supply (`name type value` rows).
        /// Applied to the replica's worker config before create-time setup, so a scenario can
        /// assert that create-time work observes synced values rather than dyncfg defaults.
        #[serde(default)]
        initial_config: Vec<ConfigSetting>,
    },
    /// Send `UpdateConfiguration` with a table of dyncfg updates (`name type value`
    /// rows). Generic over any configuration; the peek-response stash is not settable
    /// here (it is force-disabled at instance creation).
    UpdateConfiguration {
        /// The dyncfg updates to apply.
        #[serde(default)]
        updates: Vec<ConfigSetting>,
    },
    /// Drop the current connection and reconnect, sending only `Hello`. Re-issue
    /// `create_instance`, replay the dataflows the replica should keep, then
    /// `initialization_complete` to close the reconciliation window.
    Reconnect,
    /// Send `InitializationComplete`, closing the reconciliation window.
    InitializationComplete,
}

/// What the registry remembers about an exported index, so a later `define`
/// import or count assertion can reconstruct the import without re-declaring it.
struct IndexEntry {
    /// The id of the collection the index arranges.
    on_id: u64,
    /// The columns the index is arranged by.
    key: Vec<usize>,
    /// The arranged collection's relation type (for `import_index`).
    on_type: ReprRelationType,
}

/// The base for ephemeral global ids the count sugar allocates. Far above any
/// id a script would use, so its dataflows never collide with user objects.
const INTERNAL_ID_BASE: u64 = u64::MAX / 2;

/// Mutable state threaded through a script run.
pub struct ScriptState {
    driver: Driver,
    client: PersistClient,
    loc: PersistLocation,
    /// Named schemas declared via `define_schema`.
    schemas: BTreeMap<String, RelationDesc>,
    /// Alias-to-shard map; aliases are allocated lazily on first use.
    shards: BTreeMap<String, ShardId>,
    /// Exported indexes, by global id, for later import / count assertions.
    indexes: BTreeMap<u64, IndexEntry>,
    /// Materialized-view sink outputs, by sink global id: the target shard's
    /// metadata, so a `peek` of the sink id reads its shard via a persist peek.
    mv_outputs: BTreeMap<u64, CollectionMetadata>,
    /// Next ephemeral id for the count sugar's dataflows.
    next_internal: u64,
}

impl ScriptState {
    /// Build the state from a connected driver and its persist location, opening a
    /// persist client.
    pub async fn new(driver: Driver, loc: PersistLocation) -> anyhow::Result<Self> {
        let client = driver.host.client().await?;
        Ok(ScriptState {
            driver,
            client,
            loc,
            schemas: BTreeMap::new(),
            shards: BTreeMap::new(),
            indexes: BTreeMap::new(),
            mv_outputs: BTreeMap::new(),
            next_internal: INTERNAL_ID_BASE,
        })
    }

    /// Resolve a shard alias, allocating a fresh [`ShardId`] on first use.
    fn shard_id(&mut self, alias: &str) -> ShardId {
        *self
            .shards
            .entry(alias.to_string())
            .or_insert_with(ShardId::new)
    }

    /// Allocate a fresh ephemeral global id for an internally-built dataflow.
    fn alloc_internal(&mut self) -> GlobalId {
        let id = self.next_internal;
        self.next_internal += 1;
        GlobalId::User(id)
    }

    /// Count the rows of a registered index at `ts` by running a `count(*)`
    /// `Reduce` over it: build an ephemeral dataflow that index-imports `index_id`,
    /// schedule and hydrate it, then peek its single-row output. An empty result
    /// (the reduce emits no row over empty input) reads as a count of `0`.
    async fn count_via_reduce(&mut self, index_id: u64, ts: u64) -> anyhow::Result<u64> {
        let entry = self.indexes.get(&index_id).ok_or_else(|| {
            anyhow::anyhow!("unknown index {index_id}; define it with define_index first")
        })?;
        let on_id = entry.on_id;
        let key = entry.key.clone();
        let on_type = entry.on_type.clone();

        let reduce_id = self.alloc_internal();
        let out_index_id = self.alloc_internal();
        let df = count_over_index(
            GlobalId::User(index_id),
            GlobalId::User(on_id),
            on_type,
            key,
            reduce_id,
            out_index_id,
            Timestamp::from(ts),
        )?;
        self.driver.submit_dataflow(df)?;
        self.driver.schedule(out_index_id)?;
        // The count is final once the output frontier passes `ts`.
        self.driver
            .expect_frontier(
                out_index_id,
                Timestamp::from(ts).step_forward(),
                Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            )
            .await?;

        // The reduce output is a single non-null bigint column.
        let count_desc = RelationDesc::builder()
            .with_column(
                "count",
                SqlColumnType {
                    scalar_type: SqlScalarType::Int64,
                    nullable: false,
                },
            )
            .finish();
        let rows = self
            .driver
            .peek(
                PeekTarget::Index { id: out_index_id },
                count_desc,
                Timestamp::from(ts),
            )
            .await?;
        match rows.as_slice() {
            // No row over empty input: count is zero.
            [] => Ok(0),
            [row] => {
                let count = row.unpack_first().unwrap_int64();
                Ok(u64::try_from(count)?)
            }
            other => anyhow::bail!(
                "count reduce returned {} rows, expected 0 or 1",
                other.len()
            ),
        }
    }

    /// Resolve a schema name, defaulting to the built-in sample schema when absent.
    fn resolve_schema(&self, name: &Option<String>) -> anyhow::Result<RelationDesc> {
        match name {
            None => Ok(sample_desc()),
            Some(name) => self.schemas.get(name).cloned().ok_or_else(|| {
                anyhow::anyhow!("unknown schema {name:?}; declare it with define_schema first")
            }),
        }
    }

    /// Validate that a sink's declared output schema `desc` matches the column types
    /// of the object `on_id` it exports. Compares column types (not inferred keys),
    /// so a mismatched arity or type fails before the dataflow is submitted.
    fn check_sink_schema(
        &self,
        builder: &DataflowBuilder,
        on_id: u64,
        desc: &RelationDesc,
    ) -> anyhow::Result<()> {
        let on_type = builder.get(GlobalId::User(on_id))?.typ();
        let want = ReprRelationType::from(desc.typ());
        anyhow::ensure!(
            on_type.column_types == want.column_types,
            "sink output schema does not match object {on_id}: \
             declared {:?}, object is {:?}",
            want.column_types,
            on_type.column_types
        );
        Ok(())
    }

    /// Execute a single command, returning its golden output text.
    pub async fn execute(&mut self, cmd: Command) -> anyhow::Result<String> {
        match cmd {
            Command::DefineSchema { name, columns } => {
                let desc = relation_desc(&columns)?;
                self.schemas.insert(name, desc);
                Ok("ok".to_string())
            }
            Command::WriteSingleTs {
                shard,
                schema,
                ts,
                count,
                start,
                row_bytes,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = synth_rows(&desc, start, count, pad);
                write_rows_single_ts(&self.client, shard, &desc, &batch, Timestamp::from(ts))
                    .await?;
                Ok(format!("wrote {count}"))
            }
            Command::WriteSpread {
                shard,
                schema,
                count,
                n_ts,
                start,
                row_bytes,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let shard = self.shard_id(&shard);
                let pad = row_bytes.unwrap_or(DEFAULT_ROW_BYTES);
                let batch = synth_rows(&desc, start, count, pad);
                write_rows_spread(&self.client, shard, &desc, &batch, n_ts).await?;
                Ok(format!("wrote {count}"))
            }
            Command::WriteRows {
                shard,
                schema,
                ts,
                rows,
            } => {
                let desc = self.resolve_schema(&schema)?;
                let batch = rows_from_tokens(&desc, &rows)?;
                let written = batch.len();
                let shard = self.shard_id(&shard);
                write_rows_single_ts(&self.client, shard, &desc, &batch, Timestamp::from(ts))
                    .await?;
                Ok(format!("wrote {written}"))
            }
            Command::DefineIndex {
                source_id,
                index_id,
                shard,
                schema,
                key,
                as_of,
                upper,
            } => {
                let desc = self.resolve_schema(&schema)?;
                // Validate the key columns against the schema up front, so a bad
                // index (e.g. key past the last column) yields a clean error rather
                // than reaching the lowering with an out-of-range column reference.
                let arity = desc.arity();
                for &col in &key {
                    anyhow::ensure!(
                        col < arity,
                        "key column {col} out of range for a {arity}-column schema"
                    );
                }
                let shard = self.shard_id(&shard);
                let on_type = ReprRelationType::from(desc.typ());
                let df = index_dataflow(
                    GlobalId::User(source_id),
                    GlobalId::User(index_id),
                    shard,
                    self.loc.clone(),
                    desc,
                    key.clone(),
                    Timestamp::from(as_of),
                    Timestamp::from(upper),
                )?;
                self.driver.submit_dataflow(df)?;
                // Register only after a successful submit, so a rejected index is
                // not later importable or countable.
                self.indexes.insert(
                    index_id,
                    IndexEntry {
                        on_id: source_id,
                        key,
                        on_type,
                    },
                );
                Ok("ok".to_string())
            }
            Command::Schedule { id } => {
                self.driver.schedule(GlobalId::User(id))?;
                Ok("ok".to_string())
            }
            Command::AllowCompaction { id, frontier } => {
                self.driver.send(ComputeCommand::AllowCompaction {
                    id: GlobalId::User(id),
                    frontier: Antichain::from_elem(Timestamp::from(frontier)),
                })?;
                Ok("ok".to_string())
            }
            Command::AllowWrites { id } => {
                self.driver
                    .send(ComputeCommand::AllowWrites(GlobalId::User(id)))?;
                Ok("ok".to_string())
            }
            Command::AwaitFrontier {
                id,
                ts,
                timeout_secs,
                allow_timeout,
            } => {
                let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
                let result = self
                    .driver
                    .expect_frontier(GlobalId::User(id), Timestamp::from(ts), timeout)
                    .await;
                if allow_timeout {
                    // The outcome is intentionally unobserved: emit a fixed token so
                    // the golden output stays deterministic whether or not the
                    // frontier was reached (see `multi_dataflow`, whose hydration is
                    // nondeterministic).
                    Ok("awaited".to_string())
                } else {
                    result?;
                    Ok("ok".to_string())
                }
            }
            Command::Count { id, ts } => {
                let count = self.count_via_reduce(id, ts).await?;
                Ok(count.to_string())
            }
            Command::CreateDataflow {
                name,
                imports,
                builds,
                exports,
                as_of,
                optimize,
            } => {
                let mut builder = DataflowBuilder::new(
                    name.unwrap_or_else(|| "headless-create-dataflow".to_string()),
                );
                if optimize {
                    builder.optimize();
                }
                // The parser's catalog resolves `Get u<n>` leaves by name; it
                // assigns its own global ids, so we keep a name->our-id map and
                // remap the parsed `Get`s back to the script's ids afterwards.
                let mut catalog = TestCatalog::default();
                let mut name_to_id: BTreeMap<String, GlobalId> = BTreeMap::new();
                for import in imports {
                    match import {
                        ImportSpec::Source {
                            id,
                            shard,
                            schema,
                            upper,
                        } => {
                            let desc = self.resolve_schema(&schema)?;
                            let id = GlobalId::User(id);
                            register_catalog_object(
                                &mut catalog,
                                &mut name_to_id,
                                id,
                                desc.typ().clone(),
                            )?;
                            let shard = self.shard_id(&shard);
                            builder.import_persist(
                                id,
                                PersistSource {
                                    shard,
                                    location: self.loc.clone(),
                                    desc,
                                    upper: Timestamp::from(upper),
                                },
                            );
                        }
                        ImportSpec::Index { index_id } => {
                            let entry = self.indexes.get(&index_id).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "unknown index {index_id}; define it before importing it"
                                )
                            })?;
                            let on_id = GlobalId::User(entry.on_id);
                            let key = entry.key.clone();
                            let on_type = entry.on_type.clone();
                            register_catalog_object(
                                &mut catalog,
                                &mut name_to_id,
                                on_id,
                                SqlRelationType::from_repr(&on_type),
                            )?;
                            builder.import_index(
                                GlobalId::User(index_id),
                                on_id,
                                key,
                                on_type,
                                false,
                            );
                        }
                    }
                }
                for build in builds {
                    // Parse the pretty MIR spec against the catalog, then remap
                    // its catalog-assigned `Get` ids to the script's ids.
                    let mut expr = try_parse_mir(&catalog, &build.expr)
                        .map_err(|e| anyhow::anyhow!("parsing MIR for object {}: {e}", build.id))?;
                    remap_gets(&mut expr, &catalog, &name_to_id)?;
                    let id = GlobalId::User(build.id);
                    // Register the built object so later builds can `get` it.
                    register_catalog_object(
                        &mut catalog,
                        &mut name_to_id,
                        id,
                        SqlRelationType::from_repr(&expr.typ()),
                    )?;
                    builder.build(id, expr);
                }
                // Wire each export onto the builder. Index exports are captured for
                // later import / count assertions; sink exports route their output
                // either to a target shard (materialized view) or back as responses
                // (subscribe). Sink output schemas must match the exported object's
                // type, validated here so a mismatch fails before submission.
                let mut new_indexes = Vec::new();
                let mut new_subscribes = Vec::new();
                let mut new_mv_outputs = Vec::new();
                for export in exports {
                    match export {
                        ExportSpec::Index {
                            index_id,
                            on_id,
                            key,
                        } => {
                            let on_type = builder.get(GlobalId::User(on_id))?.typ();
                            builder.export_index(
                                GlobalId::User(index_id),
                                GlobalId::User(on_id),
                                key.clone(),
                            );
                            new_indexes.push((index_id, on_id, key, on_type));
                        }
                        ExportSpec::MaterializedView {
                            sink_id,
                            on_id,
                            shard,
                            schema,
                        } => {
                            let desc = self.resolve_schema(&schema)?;
                            self.check_sink_schema(&builder, on_id, &desc)?;
                            let shard = self.shard_id(&shard);
                            let location = self.loc.clone();
                            builder.export_materialized_view(
                                GlobalId::User(sink_id),
                                GlobalId::User(on_id),
                                desc.clone(),
                                PersistSink {
                                    shard,
                                    location: location.clone(),
                                },
                            );
                            // Record the target shard so a later `peek` of the sink
                            // id reads it via a persist peek (the `SELECT * FROM mv`
                            // path), with no separate read-back command.
                            new_mv_outputs.push((
                                sink_id,
                                CollectionMetadata {
                                    persist_location: location,
                                    data_shard: shard,
                                    relation_desc: desc,
                                    txns_shard: None,
                                },
                            ));
                        }
                        ExportSpec::Subscribe {
                            sink_id,
                            on_id,
                            schema,
                            up_to,
                        } => {
                            let desc = self.resolve_schema(&schema)?;
                            self.check_sink_schema(&builder, on_id, &desc)?;
                            builder.export_subscribe(
                                GlobalId::User(sink_id),
                                GlobalId::User(on_id),
                                desc,
                                up_to_antichain(up_to),
                            );
                            new_subscribes.push(sink_id);
                        }
                    }
                }
                builder.as_of(Timestamp::from(as_of));
                let df = builder.finish()?;
                self.driver.submit_dataflow(df)?;
                // Register only after a successful submit, so a rejected dataflow
                // leaves no dangling index entry or subscribe buffer.
                for (index_id, on_id, key, on_type) in new_indexes {
                    self.indexes.insert(
                        index_id,
                        IndexEntry {
                            on_id,
                            key,
                            on_type,
                        },
                    );
                }
                for sink_id in new_subscribes {
                    self.driver.register_subscribe(GlobalId::User(sink_id));
                }
                for (sink_id, metadata) in new_mv_outputs {
                    self.mv_outputs.insert(sink_id, metadata);
                }
                Ok("ok".to_string())
            }
            Command::Peek { id, schema, ts } => {
                let desc = self.resolve_schema(&schema)?;
                // A materialized-view sink id resolves to a persist peek of its
                // output shard; any other id is an index peek. The persist peek
                // blocks until the shard seals through `ts`, so it doubles as a wait
                // for the writing sink to catch up.
                let target = match self.mv_outputs.get(&id) {
                    Some(metadata) => PeekTarget::Persist {
                        id: GlobalId::User(id),
                        metadata: metadata.clone(),
                    },
                    None => PeekTarget::Index {
                        id: GlobalId::User(id),
                    },
                };
                let rows = self.driver.peek(target, desc, Timestamp::from(ts)).await?;
                Ok(render_rows(&rows))
            }
            Command::AwaitSubscribe {
                id,
                up_to,
                timeout_secs,
            } => {
                let timeout = Duration::from_secs(timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
                let updates = self
                    .driver
                    .await_subscribe(GlobalId::User(id), Timestamp::from(up_to), timeout)
                    .await?;
                Ok(render_updates(&updates))
            }
            Command::CreateInstance {
                expiration_offset,
                arrangement_dictionary_compression,
                initial_config,
            } => {
                let expiration_offset = expiration_offset
                    .as_deref()
                    .map(|s| {
                        <Duration as ConfigType>::parse(s).map_err(|e| {
                            anyhow::anyhow!("expiration-offset {s:?} is not a duration: {e}")
                        })
                    })
                    .transpose()?;
                let mut initial = ConfigUpdates::default();
                for setting in &initial_config {
                    initial.add_dynamic(
                        &setting.name,
                        parse_config_val(&setting.ty, &setting.value)?,
                    );
                }
                self.driver.create_instance(
                    expiration_offset,
                    arrangement_dictionary_compression,
                    initial,
                )?;
                Ok("ok".to_string())
            }
            Command::UpdateConfiguration { updates } => {
                let mut dyncfg_updates = ConfigUpdates::default();
                for setting in &updates {
                    dyncfg_updates.add_dynamic(
                        &setting.name,
                        parse_config_val(&setting.ty, &setting.value)?,
                    );
                }
                self.driver.update_configuration(dyncfg_updates)?;
                Ok("ok".to_string())
            }
            Command::Reconnect => {
                self.driver.reconnect().await?;
                Ok("ok".to_string())
            }
            Command::InitializationComplete => {
                self.driver.send(ComputeCommand::InitializationComplete)?;
                Ok("ok".to_string())
            }
        }
    }
}

/// Run a script: parse `content` into stanzas, execute each command, and either
/// compare its output to the stanza's expected block or — when `REWRITE` is set
/// and `path` is given — rewrite the file in place with the actual outputs.
///
/// Returns `Err` if any stanza's output differs from its expected block, so a
/// scripted run exits non-zero on a mismatch (and CI fails). A command that fails
/// renders as `error: <message>`, so an expected failure is asserted by its
/// golden block rather than a special command.
pub async fn run(
    driver: Driver,
    loc: PersistLocation,
    content: &str,
    path: Option<&Path>,
) -> anyhow::Result<()> {
    let items = crate::text::parse_file(content)?;
    let mut state = ScriptState::new(driver, loc).await?;
    let rewrite = std::env::var_os("REWRITE").is_some();

    let mut actuals = Vec::new();
    let mut mismatches = 0usize;
    for item in &items {
        let crate::text::Item::Stanza(stanza) = item else {
            continue;
        };
        let actual = match state.execute(stanza.command.clone()).await {
            Ok(output) => output,
            Err(e) => format!("error: {e}"),
        };
        let directive = stanza.input.lines().next().unwrap_or_default();
        if rewrite {
            println!("{directive} => {actual}");
        } else if actual == stanza.expected {
            println!("ok: {directive}");
        } else {
            mismatches += 1;
            println!(
                "MISMATCH: {directive}\n  expected: {:?}\n  actual:   {:?}",
                stanza.expected, actual
            );
        }
        actuals.push(actual);
    }

    if rewrite {
        let path = path.context("REWRITE is set but the script came from stdin")?;
        std::fs::write(path, crate::text::rewrite(&items, &actuals))
            .with_context(|| format!("rewriting {}", path.display()))?;
        return Ok(());
    }
    if mismatches > 0 {
        anyhow::bail!("{mismatches} stanza(s) did not match their expected output");
    }
    Ok(())
}

/// Render peeked rows as deterministic golden text: each row's datums joined by
/// spaces, with the rows sorted so the output is independent of arrangement order.
fn render_rows(rows: &[Row]) -> String {
    let mut lines: Vec<String> = rows
        .iter()
        .map(|row| {
            row.unpack()
                .iter()
                .map(|datum| datum.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    lines.sort();
    lines.join("\n")
}

/// Convert an optional `up_to` timestamp into a sink's exclusive upper antichain;
/// `None` is the empty antichain (no bound — the sink runs indefinitely).
fn up_to_antichain(up_to: Option<u64>) -> Antichain<Timestamp> {
    match up_to {
        Some(t) => Antichain::from_elem(Timestamp::from(t)),
        None => Antichain::new(),
    }
}

/// Render a subscribe's updates as golden text: `<ts> <diff> <datums>` per line.
/// Updates are consolidated by `(time, row)` — so split batches and retractions
/// collapse — net-zero rows dropped, and the lines sorted for determinism.
fn render_updates(updates: &[(Row, Timestamp, i64)]) -> String {
    let mut by_key: BTreeMap<(Timestamp, Row), i64> = BTreeMap::new();
    for (row, ts, diff) in updates {
        *by_key.entry((*ts, row.clone())).or_default() += diff;
    }
    let mut lines: Vec<String> = by_key
        .into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((ts, row), diff)| {
            let datums = row
                .unpack()
                .iter()
                .map(|datum| datum.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            format!("{ts} {diff} {datums}")
        })
        .collect();
    lines.sort();
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `define_schema` types map to a `RelationDesc` with matching arity and
    /// nullability, and `synth_rows` fills it.
    #[mz_ore::test]
    fn schema_parse_and_synth() {
        let columns = vec![
            ColumnSpec {
                name: "k".to_string(),
                ty: "bigint".to_string(),
                nullable: false,
            },
            ColumnSpec {
                name: "flag".to_string(),
                ty: "boolean".to_string(),
                nullable: false,
            },
            ColumnSpec {
                name: "v".to_string(),
                ty: "text".to_string(),
                nullable: true,
            },
        ];
        let desc = relation_desc(&columns).unwrap();
        assert_eq!(desc.arity(), 3);
        let types: Vec<_> = desc.iter_types().collect();
        assert_eq!(types[0].scalar_type, SqlScalarType::Int64);
        assert_eq!(types[1].scalar_type, SqlScalarType::Bool);
        assert!(types[2].nullable);

        let rows = synth_rows(&desc, 0, 4, 8);
        assert_eq!(rows.len(), 4);

        assert!(scalar_type_from_str("nope").is_err());
    }

    /// Tokens type into the right `Cell`s against their column, bare `null` is SQL
    /// null (rejected for a non-nullable column), and a bad numeric token errors.
    #[mz_ore::test]
    fn cell_from_token_maps_values() {
        let int_col = SqlColumnType {
            scalar_type: SqlScalarType::Int64,
            nullable: false,
        };
        let str_col = SqlColumnType {
            scalar_type: SqlScalarType::String,
            nullable: true,
        };
        assert_eq!(cell_from_token("7", &int_col).unwrap(), Cell::Int64(7));
        assert_eq!(
            cell_from_token("hi", &str_col).unwrap(),
            Cell::Str("hi".to_string())
        );
        // A quoted string keeps its contents; the quotes are stripped.
        assert_eq!(
            cell_from_token("\"a b\"", &str_col).unwrap(),
            Cell::Str("a b".to_string())
        );
        assert_eq!(cell_from_token("null", &str_col).unwrap(), Cell::Null);
        // null into a non-nullable column is an error.
        assert!(cell_from_token("null", &int_col).is_err());
        // a non-numeric token in an int column is an error.
        assert!(cell_from_token("x", &int_col).is_err());
    }
}
