// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mutation testing of the LIR plan validator against a real replica.
//!
//! The base plans in `lir_mutation/` are optimizer output, from `EXPLAIN PHYSICAL PLAN AS LIR
//! JSON FOR MATERIALIZED VIEW` over the views in [`BASES`]. Each mutant applies one random
//! mutation to the JSON of a base plan, from a fixed seed, and goes through the steps of
//! `CREATE MATERIALIZED VIEW ... FROM PLAN`: parse the envelope, bind the imports, export the
//! output to a materialized-view sink with the base plan's output type as the declared
//! schema, and validate in user plan mode. Every mutant must be handled without a panic.
//!
//! With `LIR_MUTATION_CLUSTERD` set to a `clusterd` binary and `COCKROACH_URL` to a
//! CockroachDB for persist consensus, the test also starts that replica, renders every
//! accepted mutant on it over a small input, and asserts that the replica is alive
//! afterwards. A replica that exits is restarted, and the mutant is reported. Without them,
//! only the validation runs. The replica runs with soft assertions enabled and overflows
//! panicking, as in CI, so that both surface as crashes.
//!
//! Further environment variables:
//!
//! * `LIR_MUTANTS_PER_BASE`: mutants per base plan, by default [`DEFAULT_MUTANTS_PER_BASE`].
//! * `LIR_MUTATION_SEED`: the seed, by default [`DEFAULT_SEED`].
//! * `LIR_MUTATION_OUT`: a directory for `results.jsonl`, one line per mutant, and the JSON
//!   of every accepted mutant that crashed or stalled the replica. `pending.json` holds the
//!   mutant being rendered, which identifies the culprit if the test itself dies.
//! * `LIR_MUTATION_CLUSTERD_WRAPPER`: a command, split at whitespace, that `clusterd` runs
//!   under, for example `memcap 4G` to bound its memory. A replica the wrapper kills counts
//!   as crashed.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use mz_clusterd_test_driver::data::pack_cells;
use mz_clusterd_test_driver::data::{Cell, write_rows_spread};
use mz_clusterd_test_driver::dataflow::{PersistSink, PersistSource, index_dataflow};
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::lir_plan::{
    LirCatalog, LirIndex, LirPlanDataflow, LirRelation, declared_desc,
};
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_compute_client::protocol::command::{ComputeCommand, PeekTarget};
use mz_compute_types::plan::envelope::parse_envelope;
use mz_dyncfg::ConfigUpdates;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, Row, SqlScalarType, Timestamp};
use mz_storage_types::dyncfgs::ORE_OVERFLOWING_BEHAVIOR;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng};
use serde_json::{Value, json};
use timely::progress::Antichain;

/// The seed unless `LIR_MUTATION_SEED` overrides it.
const DEFAULT_SEED: u64 = 0x5eed_0013;

/// The mutants per base unless `LIR_MUTANTS_PER_BASE` overrides it.
const DEFAULT_MUTANTS_PER_BASE: usize = 200;

/// The fewest mutants a run with the default mutants per base must generate.
const MIN_MUTANTS: usize = 1000;

/// The exclusive upper of the input shards. Rows are spread over the times before it.
const INPUT_UPPER: u64 = 5;

/// A base plan and the view it is the plan of.
struct Base {
    name: &'static str,
    json: &'static str,
    #[allow(dead_code)]
    sql: &'static str,
    /// Whether the persist imports are monotonic, as for append-only sources. The plans were
    /// produced over tables, so they use the variants for non-monotonic inputs, which the
    /// mutations can switch to the monotonic ones. The input data has no retractions.
    monotonic: bool,
}

const BASES: &[Base] = &[
    Base {
        name: "join",
        json: include_str!("lir_mutation/join.json"),
        sql: "SELECT pt.a, pt.b, pu.c FROM pt JOIN pu ON pt.a = pu.a",
        monotonic: false,
    },
    Base {
        name: "reduce",
        json: include_str!("lir_mutation/reduce.json"),
        sql: "SELECT a, count(*) AS n, sum(b) AS s, min(b) AS lo, max(b) AS hi FROM pt GROUP BY a",
        monotonic: false,
    },
    Base {
        name: "topk",
        json: include_str!("lir_mutation/topk.json"),
        sql: "SELECT a, b FROM (SELECT DISTINCT a FROM pt) g, LATERAL \
              (SELECT b FROM pt WHERE pt.a = g.a ORDER BY b DESC NULLS LAST LIMIT 2)",
        monotonic: false,
    },
    Base {
        name: "letrec",
        json: include_str!("lir_mutation/letrec.json"),
        sql: "WITH MUTUALLY RECURSIVE (RETURN AT RECURSION LIMIT 20) \
              reach (src int8, dst int8) AS (SELECT src, dst FROM edges UNION \
              SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) \
              SELECT src, dst FROM reach",
        monotonic: false,
    },
    Base {
        name: "temporal",
        json: include_str!("lir_mutation/temporal.json"),
        sql: "SELECT ts, v FROM ev WHERE mz_now() >= ts::mz_timestamp \
              AND mz_now() < (ts + 3)::mz_timestamp",
        monotonic: false,
    },
    Base {
        name: "constant",
        json: include_str!("lir_mutation/const.json"),
        sql: "(SELECT a, b FROM pt UNION ALL VALUES (100::int8, 7::int8), (101, NULL)) \
              EXCEPT ALL SELECT a, b FROM pt WHERE b IS NULL",
        monotonic: false,
    },
    Base {
        name: "reduce_monotonic",
        json: include_str!("lir_mutation/reduce.json"),
        sql: "SELECT a, count(*) AS n, sum(b) AS s, min(b) AS lo, max(b) AS hi FROM pt GROUP BY a",
        monotonic: true,
    },
    Base {
        name: "topk_monotonic",
        json: include_str!("lir_mutation/topk.json"),
        sql: "SELECT a, b FROM (SELECT DISTINCT a FROM pt) g, LATERAL \
              (SELECT b FROM pt WHERE pt.a = g.a ORDER BY b DESC NULLS LAST LIMIT 2)",
        monotonic: true,
    },
];

/// A table the base plans import: its name, id, columns, and rows.
struct Table {
    name: &'static str,
    id: GlobalId,
    columns: &'static [(&'static str, SqlScalarType, bool)],
    rows: Vec<Vec<Cell>>,
}

fn tables() -> Vec<Table> {
    use Cell::{Int64 as I, Null as N};
    let s = |s: &str| Cell::Str(s.to_string());
    vec![
        Table {
            name: "materialize.public.pt",
            id: GlobalId::User(1),
            columns: &[
                ("a", SqlScalarType::Int64, false),
                ("b", SqlScalarType::Int64, true),
            ],
            rows: vec![
                vec![I(1), I(10)],
                vec![I(2), N],
                vec![I(3), I(30)],
                vec![I(3), I(31)],
                vec![I(4), I(40)],
                vec![I(5), N],
                vec![I(6), I(-7)],
                vec![I(7), I(i64::MAX)],
                vec![I(7), I(i64::MAX)],
                vec![I(8), I(i64::MIN)],
            ],
        },
        Table {
            name: "materialize.public.pu",
            id: GlobalId::User(2),
            columns: &[
                ("a", SqlScalarType::Int64, false),
                ("c", SqlScalarType::String, true),
            ],
            rows: vec![
                vec![I(1), s("one")],
                vec![I(3), s("three")],
                vec![I(4), s("four")],
                vec![I(6), N],
            ],
        },
        Table {
            name: "materialize.public.edges",
            id: GlobalId::User(3),
            columns: &[
                ("src", SqlScalarType::Int64, false),
                ("dst", SqlScalarType::Int64, false),
            ],
            rows: vec![
                vec![I(1), I(2)],
                vec![I(2), I(3)],
                vec![I(3), I(4)],
                vec![I(4), I(1)],
                vec![I(5), I(6)],
            ],
        },
        Table {
            name: "materialize.public.ev",
            id: GlobalId::User(4),
            columns: &[
                ("ts", SqlScalarType::Int64, false),
                ("v", SqlScalarType::Int64, true),
            ],
            rows: vec![
                vec![I(0), I(1)],
                vec![I(1), I(2)],
                vec![I(2), N],
                vec![I(3), I(4)],
                vec![I(4), I(5)],
                vec![I(10), I(6)],
            ],
        },
    ]
}

/// The name, id, and key of the index on `pu`.
const PU_INDEX: (&str, GlobalId, usize) = ("materialize.public.pu_a", GlobalId::User(5), 0);

fn table_desc(table: &Table) -> RelationDesc {
    let mut builder = RelationDesc::builder();
    for (name, typ, nullable) in table.columns {
        builder = builder.with_column(*name, typ.clone().nullable(*nullable));
    }
    builder.finish()
}

/// The import bindings of the base plans.
fn catalog(
    location: &PersistLocation,
    shards: &BTreeMap<&'static str, ShardId>,
    monotonic: bool,
) -> LirCatalog {
    let mut catalog = LirCatalog::default();
    for table in tables() {
        let desc = table_desc(&table);
        catalog.relations.insert(
            table.name.to_string(),
            LirRelation {
                id: table.id,
                typ: ReprRelationType::from(desc.typ()),
                persist: Some(PersistSource {
                    shard: shards[table.name],
                    location: location.clone(),
                    desc,
                    upper: Timestamp::from(INPUT_UPPER),
                }),
                monotonic,
            },
        );
    }
    catalog.indexes.insert(
        PU_INDEX.0.to_string(),
        LirIndex {
            index_id: PU_INDEX.1,
            on_id: GlobalId::User(2),
            key: vec![PU_INDEX.2],
        },
    );
    catalog
}

// Mutation operators.

#[derive(Clone, Debug)]
enum Seg {
    Key(String),
    Idx(usize),
}

type JsonPath = Vec<Seg>;

fn path_string(path: &[Seg]) -> String {
    path.iter()
        .map(|s| match s {
            Seg::Key(k) => format!(".{k}"),
            Seg::Idx(i) => format!("[{i}]"),
        })
        .collect()
}

/// Collects the paths of all nodes of `v`, in pre-order.
fn collect_paths(v: &Value, path: &mut JsonPath, out: &mut Vec<JsonPath>) {
    out.push(path.clone());
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                path.push(Seg::Key(k.clone()));
                collect_paths(child, path, out);
                path.pop();
            }
        }
        Value::Array(items) => {
            for (i, child) in items.iter().enumerate() {
                path.push(Seg::Idx(i));
                collect_paths(child, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn at<'a>(v: &'a Value, path: &[Seg]) -> &'a Value {
    path.iter().fold(v, |v, s| match s {
        Seg::Key(k) => &v[k.as_str()],
        Seg::Idx(i) => &v[*i],
    })
}

fn at_mut<'a>(v: &'a mut Value, path: &[Seg]) -> &'a mut Value {
    path.iter().fold(v, |v, s| match s {
        Seg::Key(k) => &mut v[k.as_str()],
        Seg::Idx(i) => &mut v[*i],
    })
}

fn last_key(path: &[Seg]) -> Option<&str> {
    match path.last() {
        Some(Seg::Key(k)) => Some(k),
        _ => None,
    }
}

/// The key under which the nearest enclosing object field sits, skipping array indices.
fn enclosing_key(path: &[Seg]) -> Option<&str> {
    path.iter().rev().find_map(|s| match s {
        Seg::Key(k) => Some(k.as_str()),
        Seg::Idx(_) => None,
    })
}

fn has_key(v: &Value, key: &str) -> bool {
    v.as_object().is_some_and(|m| m.contains_key(key))
}

/// Facts about the whole plan that some operators draw from.
struct Ctx {
    lir_ids: Vec<u64>,
    global_ids: Vec<Value>,
}

impl Ctx {
    fn new(root: &Value) -> Self {
        let mut paths = Vec::new();
        collect_paths(root, &mut Vec::new(), &mut paths);
        let mut lir_ids = Vec::new();
        let mut global_ids = Vec::new();
        for path in &paths {
            let v = at(root, path);
            if last_key(path) == Some("lir_id") {
                lir_ids.extend(v.as_u64());
            }
            if let Some(id) = v.get("Global") {
                global_ids.push(id.clone());
            }
        }
        global_ids.push(json!({"Transient": 99}));
        global_ids.push(json!({"User": 2}));
        Ctx {
            lir_ids,
            global_ids,
        }
    }
}

/// A mutation operator: which nodes it applies to, and how it changes one.
struct Op {
    name: &'static str,
    applies: fn(&Value, &[Seg], &Value) -> bool,
    /// Changes the node, and describes the change, or returns `None` if it cannot.
    apply: fn(&mut Value, &Ctx, &mut StdRng) -> Option<String>,
}

fn column(c: u64) -> Value {
    json!({"Column": [c, null]})
}

fn random_index(rng: &mut StdRng, current: u64) -> u64 {
    let mut choices = vec![current + 1, current + 2, 0, 1, 1000];
    if current > 0 {
        choices.push(current - 1);
    }
    choices.retain(|c| *c != current);
    *choices.choose(rng).expect("non-empty")
}

fn random_datum(rng: &mut StdRng) -> Value {
    [
        json!("Null"),
        json!({"Bool": true}),
        json!({"Int16": 3}),
        json!({"Int32": -1}),
        json!({"Int64": i64::MAX}),
        json!({"Int64": -5}),
        json!({"UInt64": 7}),
        json!({"Float64": 1.5}),
        json!({"Numeric": "1.5"}),
        json!({"String": "x"}),
        json!({"MzTimestamp": "3"}),
    ]
    .choose(rng)
    .expect("non-empty")
    .clone()
}

fn random_scalar_type(rng: &mut StdRng) -> Value {
    [
        "Bool",
        "Int16",
        "Int32",
        "Int64",
        "UInt64",
        "Float64",
        "Numeric",
        "String",
        "MzTimestamp",
        "Bytes",
    ]
    .choose(rng)
    .map(|t| json!(t))
    .expect("non-empty")
}

/// Changes one element of an array of indices.
fn mutate_index_array(v: &mut Value, rng: &mut StdRng) -> Option<String> {
    let items = v.as_array_mut()?;
    match rng.random_range(0..4) {
        0 if !items.is_empty() => {
            let i = rng.random_range(0..items.len());
            let new = random_index(rng, items[i].as_u64()?);
            items[i] = json!(new);
            Some(format!("element {i} to {new}"))
        }
        1 if !items.is_empty() => {
            let i = rng.random_range(0..items.len());
            items.remove(i);
            Some(format!("remove element {i}"))
        }
        2 if !items.is_empty() => {
            let i = rng.random_range(0..items.len());
            let item = items[i].clone();
            items.push(item);
            Some(format!("duplicate element {i}"))
        }
        _ => {
            let new = rng.random_range(0..4u64);
            items.push(json!(new));
            Some(format!("append {new}"))
        }
    }
}

/// Changes one key expression of an arrangement.
fn mutate_key_exprs(v: &mut Value, rng: &mut StdRng) -> Option<String> {
    let items = v.as_array_mut()?;
    match rng.random_range(0..3) {
        0 if !items.is_empty() => {
            let i = rng.random_range(0..items.len());
            let new = rng.random_range(0..4u64);
            items[i] = column(new);
            Some(format!("key {i} to #{new}"))
        }
        1 if !items.is_empty() => {
            items.clear();
            Some("empty key".to_string())
        }
        _ => {
            let new = rng.random_range(0..4u64);
            items.push(column(new));
            Some(format!("append key #{new}"))
        }
    }
}

fn identity_mfp(arity: u64) -> Value {
    json!({
        "mfp": {
            "mfp": {
                "expressions": [],
                "predicates": [],
                "projection": (0..arity).collect::<Vec<_>>(),
                "input_arity": arity,
            },
        },
        "lower_bounds": [],
        "upper_bounds": [],
    })
}

const UNARY_FUNCS: &[&str] = &[
    "Not",
    "IsNull",
    "CastInt64ToMzTimestamp",
    "StepMzTimestamp",
    "CastInt32ToInt64",
    "NegInt64",
    "CastInt64ToInt32",
    "CastBoolToInt32",
    "CastInt64ToString",
];

const BINARY_FUNCS: &[&str] = &[
    "Eq", "NotEq", "Lt", "Lte", "Gt", "Gte", "AddInt64", "SubInt64", "MulInt64", "DivInt64",
    "AddInt32", "ModInt64",
];

const AGGREGATE_FUNCS: &[&str] = &[
    "Count",
    "SumInt64",
    "MaxInt64",
    "MinInt64",
    "Any",
    "All",
    "SumInt32",
    "MaxInt32",
    "MaxString",
    "MinBool",
];

/// The aggregate functions user plans admit.
const ALLOWED_AGGREGATE_FUNCS: &[&str] =
    &["Count", "SumInt64", "MaxInt64", "MinInt64", "Any", "All"];

fn ops() -> Vec<Op> {
    vec![
        Op {
            name: "column_ref",
            applies: |_, _, v| v.get("Column").is_some_and(|c| c[0].is_u64()),
            apply: |v, _, rng| {
                let c = v["Column"][0].as_u64()?;
                let new = random_index(rng, c);
                v["Column"][0] = json!(new);
                Some(format!("#{c} to #{new}"))
            },
        },
        Op {
            name: "mfp_projection",
            applies: |_, p, v| last_key(p) == Some("projection") && v.is_array(),
            apply: |v, _, rng| mutate_index_array(v, rng),
        },
        Op {
            name: "mfp_input_arity",
            applies: |_, p, v| last_key(p) == Some("input_arity") && v.is_u64(),
            apply: |v, _, rng| {
                let n = v.as_u64()?;
                let new = random_index(rng, n);
                *v = json!(new);
                Some(format!("{n} to {new}"))
            },
        },
        Op {
            name: "drop_arrangement",
            applies: |_, p, v| {
                last_key(p) == Some("arranged") && v.as_array().is_some_and(|a| !a.is_empty())
            },
            apply: |v, _, rng| {
                let items = v.as_array_mut()?;
                let i = rng.random_range(0..items.len());
                items.remove(i);
                Some(format!("remove arrangement {i}"))
            },
        },
        Op {
            name: "arrangement_layout",
            applies: |_, p, v| {
                let in_arranged = matches!(p.last(), Some(Seg::Idx(_)))
                    && matches!(p.iter().rev().nth(1), Some(Seg::Key(k)) if k == "arranged");
                (in_arranged || last_key(p) == Some("ensure_arrangement"))
                    && v.as_array().is_some_and(|a| a.len() == 3)
            },
            apply: |v, _, rng| match rng.random_range(0..3) {
                0 => mutate_key_exprs(&mut v[0], rng).map(|d| format!("key: {d}")),
                1 => mutate_index_array(&mut v[1], rng).map(|d| format!("permutation: {d}")),
                _ => mutate_index_array(&mut v[2], rng).map(|d| format!("thinning: {d}")),
            },
        },
        Op {
            name: "get_plan",
            applies: |root, p, _| {
                last_key(p) == Some("plan") && {
                    let parent = at(root, &p[..p.len() - 1]);
                    has_key(parent, "keys") && has_key(parent, "id")
                }
            },
            apply: |v, _, rng| {
                let arity = rng.random_range(1..5u64);
                match v {
                    Value::Object(m) if m.contains_key("Arrangement") => {
                        let plan = &mut m["Arrangement"];
                        match rng.random_range(0..3) {
                            0 => mutate_key_exprs(&mut plan[0], rng).map(|d| format!("key: {d}")),
                            1 => {
                                plan[1] = json!({"$row": [random_datum(rng)]});
                                Some(format!("seek {}", plan[1]))
                            }
                            _ => {
                                *v = json!("PassArrangements");
                                Some("Arrangement to PassArrangements".to_string())
                            }
                        }
                    }
                    _ => {
                        let key = rng.random_range(0..arity);
                        let (new, desc) = match rng.random_range(0..3) {
                            0 => (json!("PassArrangements"), "PassArrangements".to_string()),
                            1 => (
                                json!({"Collection": identity_mfp(arity)}),
                                format!("Collection of arity {arity}"),
                            ),
                            _ => (
                                json!({"Arrangement": [[column(key)], null, identity_mfp(arity)]}),
                                format!("Arrangement by #{key} of arity {arity}"),
                            ),
                        };
                        if *v == new {
                            return None;
                        }
                        *v = new;
                        Some(desc)
                    }
                }
            },
        },
        Op {
            name: "get_keys",
            applies: |_, p, v| last_key(p) == Some("keys") && has_key(v, "raw"),
            apply: |v, _, rng| {
                if rng.random_bool(0.5) {
                    let raw = v["raw"].as_bool()?;
                    v["raw"] = json!(!raw);
                    Some(format!("raw to {}", !raw))
                } else {
                    let key = rng.random_range(0..3u64);
                    let arrangement = json!([[column(key)], [0, 1], [1]]);
                    v["arranged"].as_array_mut()?.push(arrangement);
                    Some(format!("add arrangement by #{key}"))
                }
            },
        },
        Op {
            name: "join_stage",
            applies: |_, _, v| has_key(v, "lookup_key") && has_key(v, "stream_key"),
            apply: |v, _, rng| match rng.random_range(0..5) {
                0 => {
                    let m = v.as_object_mut()?;
                    let lookup = m.remove("lookup_key")?;
                    let stream = m.remove("stream_key")?;
                    m.insert("lookup_key".to_string(), stream);
                    m.insert("stream_key".to_string(), lookup);
                    Some("swap lookup_key and stream_key".to_string())
                }
                1 => {
                    let r = v["lookup_relation"].as_u64()?;
                    let new = random_index(rng, r);
                    v["lookup_relation"] = json!(new);
                    Some(format!("lookup_relation {r} to {new}"))
                }
                2 => mutate_index_array(&mut v["stream_thinning"], rng)
                    .map(|d| format!("stream_thinning: {d}")),
                3 => {
                    mutate_key_exprs(&mut v["lookup_key"], rng).map(|d| format!("lookup_key: {d}"))
                }
                _ => {
                    mutate_key_exprs(&mut v["stream_key"], rng).map(|d| format!("stream_key: {d}"))
                }
            },
        },
        Op {
            name: "join_source",
            applies: |_, _, v| has_key(v, "source_relation") && has_key(v, "source_key"),
            apply: |v, _, rng| {
                if rng.random_bool(0.5) {
                    let r = v["source_relation"].as_u64()?;
                    let new = random_index(rng, r);
                    v["source_relation"] = json!(new);
                    Some(format!("source_relation {r} to {new}"))
                } else {
                    mutate_key_exprs(&mut v["source_key"], rng).map(|d| format!("source_key: {d}"))
                }
            },
        },
        Op {
            name: "aggregate_func",
            applies: |root, p, v| {
                v.is_string()
                    && ((last_key(p) == Some("func")
                        && has_key(at(root, &p[..p.len() - 1]), "distinct"))
                        || enclosing_key(p) == Some("aggr_funcs"))
            },
            apply: |v, _, rng| {
                let old = v.as_str()?.to_string();
                // Mostly swap between admitted functions, which only the types tell apart.
                let pool = if rng.random_bool(0.75) {
                    ALLOWED_AGGREGATE_FUNCS
                } else {
                    AGGREGATE_FUNCS
                };
                let new = *pool.choose(rng)?;
                if new == old {
                    return None;
                }
                *v = json!(new);
                Some(format!("{old} to {new}"))
            },
        },
        Op {
            name: "monotonic_variant",
            applies: |_, p, v| {
                (last_key(p) == Some("top_k_plan") && v.is_object())
                    || has_key(v, "Bucketed")
                    || has_key(v, "Monotonic")
            },
            apply: |v, _, rng| {
                let must_consolidate = rng.random_bool(0.5);
                let m = v.as_object_mut()?;
                if let Some(basic) = m.remove("Basic") {
                    let mut plan = json!({
                        "group_key": basic["group_key"],
                        "order_key": basic["order_key"],
                        "arity": basic["arity"],
                        "must_consolidate": must_consolidate,
                    });
                    let variant = if rng.random_bool(0.5) {
                        plan["limit"] = basic["limit"].clone();
                        "MonotonicTopK"
                    } else {
                        "MonotonicTop1"
                    };
                    m.insert(variant.to_string(), plan);
                    Some(format!(
                        "Basic to {variant}, must_consolidate {must_consolidate}"
                    ))
                } else if let Some(bucketed) = m.remove("Bucketed") {
                    m.insert(
                        "Monotonic".to_string(),
                        json!({
                            "aggr_funcs": bucketed["aggr_funcs"],
                            "must_consolidate": must_consolidate,
                        }),
                    );
                    Some(format!(
                        "Bucketed to Monotonic, must_consolidate {must_consolidate}"
                    ))
                } else if let Some(monotonic) = m.remove("Monotonic") {
                    m.insert(
                        "Bucketed".to_string(),
                        json!({"aggr_funcs": monotonic["aggr_funcs"], "buckets": [16]}),
                    );
                    Some("Monotonic to Bucketed".to_string())
                } else {
                    None
                }
            },
        },
        Op {
            name: "flag",
            applies: |_, p, v| {
                v.is_boolean()
                    && matches!(
                        last_key(p),
                        Some(
                            "must_consolidate"
                                | "consolidate_output"
                                | "distinct"
                                | "return_at_limit"
                                | "raw"
                                | "nullable"
                                | "fused_unnest_list"
                                | "desc"
                                | "nulls_last"
                        )
                    )
            },
            apply: |v, _, _| {
                let b = v.as_bool()?;
                *v = json!(!b);
                Some(format!("{b} to {}", !b))
            },
        },
        Op {
            name: "literal",
            applies: |_, _, v| v.get("Literal").is_some_and(|l| l.is_array()),
            apply: |v, _, rng| {
                let lit = &mut v["Literal"];
                match rng.random_range(0..3) {
                    0 => {
                        let t = random_scalar_type(rng);
                        lit[1]["scalar_type"] = t.clone();
                        Some(format!("type to {t}"))
                    }
                    1 => {
                        let d = random_datum(rng);
                        lit[0] = json!({"Ok": {"$row": [d.clone()]}});
                        Some(format!("datum to {d}"))
                    }
                    _ => {
                        let n = lit[1]["nullable"].as_bool()?;
                        lit[1]["nullable"] = json!(!n);
                        Some(format!("nullable to {}", !n))
                    }
                }
            },
        },
        Op {
            name: "constant_row",
            applies: |_, p, v| {
                p.iter()
                    .any(|s| matches!(s, Seg::Key(k) if k == "Constant"))
                    && v.as_array()
                        .is_some_and(|a| a.len() == 3 && has_key(&a[0], "$row"))
            },
            apply: |v, _, rng| {
                let row = v[0]["$row"].as_array_mut()?;
                match rng.random_range(0..5) {
                    0 if !row.is_empty() => {
                        let i = rng.random_range(0..row.len());
                        let d = random_datum(rng);
                        row[i] = d.clone();
                        Some(format!("datum {i} to {d}"))
                    }
                    1 if !row.is_empty() => {
                        let i = rng.random_range(0..row.len());
                        row.remove(i);
                        Some(format!("remove datum {i}"))
                    }
                    2 => {
                        let d = random_datum(rng);
                        row.push(d.clone());
                        Some(format!("append datum {d}"))
                    }
                    3 => {
                        let diff = *[0i64, -1, -3, i64::MAX, i64::MIN].choose(rng)?;
                        v[2] = json!(diff);
                        Some(format!("diff to {diff}"))
                    }
                    _ => {
                        let time = *[1u64, 3, 1000, u64::MAX].choose(rng)?;
                        v[1] = json!(time);
                        Some(format!("time to {time}"))
                    }
                }
            },
        },
        Op {
            name: "scalar_func",
            applies: |root, p, v| {
                last_key(p) == Some("func") && v.is_object() && {
                    let parent = at(root, &p[..p.len() - 1]);
                    has_key(parent, "expr") || has_key(parent, "expr1")
                }
            },
            apply: |v, _, rng| {
                let old = v.as_object()?.keys().next()?.clone();
                let pool = if BINARY_FUNCS.contains(&old.as_str()) {
                    BINARY_FUNCS
                } else {
                    UNARY_FUNCS
                };
                let new = *pool.choose(rng)?;
                if new == old {
                    return None;
                }
                *v = json!({ new: null });
                Some(format!("{old} to {new}"))
            },
        },
        Op {
            name: "letrec_limit",
            applies: |_, p, v| last_key(p) == Some("limits") && v.is_array(),
            apply: |v, _, rng| {
                let limits = v.as_array_mut()?;
                if limits.is_empty() {
                    return None;
                }
                let i = rng.random_range(0..limits.len());
                match rng.random_range(0..3) {
                    0 => {
                        limits[i] = Value::Null;
                        Some(format!("limit {i} to None"))
                    }
                    1 => {
                        let n = *[0u64, 1, u64::MAX].choose(rng)?;
                        limits[i]["max_iters"] = json!(n);
                        Some(format!("limit {i} max_iters to {n}"))
                    }
                    _ => {
                        limits.remove(i);
                        Some(format!("remove limit {i}"))
                    }
                }
            },
        },
        Op {
            name: "local_id",
            applies: |_, _, v| v.get("Local").is_some_and(|l| l.is_u64()),
            apply: |v, _, rng| {
                let l = v["Local"].as_u64()?;
                let new = random_index(rng, l);
                v["Local"] = json!(new);
                Some(format!("l{l} to l{new}"))
            },
        },
        Op {
            name: "global_id",
            applies: |_, _, v| v.get("Global").is_some_and(|g| g.is_object()),
            apply: |v, ctx, rng| {
                let new = ctx.global_ids.choose(rng)?.clone();
                if v["Global"] == new {
                    return None;
                }
                v["Global"] = new.clone();
                Some(format!("to {new}"))
            },
        },
        Op {
            name: "lir_id",
            applies: |_, p, v| last_key(p) == Some("lir_id") && v.is_u64(),
            apply: |v, ctx, rng| {
                let old = v.as_u64()?;
                let new = if rng.random_bool(0.7) {
                    *ctx.lir_ids.choose(rng)?
                } else {
                    *[0u64, 1000, u64::MAX].choose(rng)?
                };
                if new == old {
                    return None;
                }
                *v = json!(new);
                Some(format!("{old} to {new}"))
            },
        },
        Op {
            name: "topk_fields",
            applies: |_, _, v| has_key(v, "group_key") && has_key(v, "order_key"),
            apply: |v, _, rng| match rng.random_range(0..6) {
                0 => {
                    mutate_index_array(&mut v["group_key"], rng).map(|d| format!("group_key: {d}"))
                }
                1 => {
                    let order = v["order_key"].as_array_mut()?;
                    if order.is_empty() {
                        order.push(json!({"column": 0, "desc": false, "nulls_last": false}));
                        return Some("add order by #0".to_string());
                    }
                    let c = order[0]["column"].as_u64()?;
                    let new = random_index(rng, c);
                    order[0]["column"] = json!(new);
                    Some(format!("order column {c} to {new}"))
                }
                2 => {
                    let limit = [
                        json!(null),
                        json!({"Literal": [{"Ok": {"$row": [{"Int64": -1}]}}, {"scalar_type": "Int64", "nullable": false}]}),
                        json!({"Literal": [{"Ok": {"$row": ["Null"]}}, {"scalar_type": "Int64", "nullable": true}]}),
                        json!({"Literal": [{"Ok": {"$row": [{"Int64": 0}]}}, {"scalar_type": "Int64", "nullable": false}]}),
                        json!({"Literal": [{"Ok": {"$row": [{"String": "2"}]}}, {"scalar_type": "String", "nullable": false}]}),
                        column(0),
                        column(1),
                    ]
                    .choose(rng)?
                    .clone();
                    v["limit"] = limit.clone();
                    Some(format!("limit to {limit}"))
                }
                3 => {
                    let offset = *[1u64, 5, u64::MAX].choose(rng)?;
                    v.as_object_mut()?
                        .insert("offset".to_string(), json!(offset));
                    Some(format!("offset to {offset}"))
                }
                4 => {
                    let a = v["arity"].as_u64()?;
                    let new = random_index(rng, a);
                    v["arity"] = json!(new);
                    Some(format!("arity {a} to {new}"))
                }
                _ => {
                    let order = v["order_key"].as_array_mut()?;
                    order.clear();
                    Some("empty order_key".to_string())
                }
            },
        },
        Op {
            name: "buckets",
            applies: |_, p, v| last_key(p) == Some("buckets") && v.is_array(),
            apply: |v, _, rng| {
                let new = [
                    json!([]),
                    json!([0]),
                    json!([1]),
                    json!([16, 256]),
                    json!([u64::MAX]),
                ]
                .choose(rng)?
                .clone();
                if *v == new {
                    return None;
                }
                *v = new.clone();
                Some(format!("to {new}"))
            },
        },
        Op {
            name: "union_inputs",
            applies: |_, _, v| has_key(v, "inputs") && has_key(v, "consolidate_output"),
            apply: |v, _, rng| {
                if rng.random_bool(0.5) {
                    let inputs = v["inputs"].as_array_mut()?;
                    let i = rng.random_range(0..inputs.len());
                    inputs.remove(i);
                    Some(format!("remove input {i}"))
                } else {
                    let strategies = v["temporal_bucketing_strategies"].as_array_mut()?;
                    strategies.pop()?;
                    Some("drop a bucketing strategy".to_string())
                }
            },
        },
        Op {
            name: "mfp_bounds",
            applies: |_, _, v| has_key(v, "lower_bounds") && has_key(v, "upper_bounds"),
            apply: |v, _, rng| {
                let m = v.as_object_mut()?;
                match rng.random_range(0..3) {
                    0 => {
                        let lower = m.remove("lower_bounds")?;
                        let upper = m.remove("upper_bounds")?;
                        m.insert("lower_bounds".to_string(), upper);
                        m.insert("upper_bounds".to_string(), lower);
                        Some("swap bounds".to_string())
                    }
                    1 => {
                        m["lower_bounds"].as_array_mut()?.push(column(0));
                        Some("add lower bound #0".to_string())
                    }
                    _ => {
                        let bound = json!({"CallUnary": {"func": {"CastInt64ToMzTimestamp": null}, "expr": column(1)}});
                        m["upper_bounds"].as_array_mut()?.push(bound);
                        Some("add upper bound #1::mz_timestamp".to_string())
                    }
                }
            },
        },
        Op {
            name: "predicate",
            applies: |_, p, v| {
                enclosing_key(p) == Some("predicates")
                    && v.as_array().is_some_and(|a| a.len() == 2 && a[0].is_u64())
            },
            apply: |v, _, rng| {
                let pos = v[0].as_u64()?;
                let new = random_index(rng, pos);
                v[0] = json!(new);
                Some(format!("position {pos} to {new}"))
            },
        },
        Op {
            name: "input_key",
            applies: |_, p, _| last_key(p) == Some("input_key"),
            apply: |v, _, rng| {
                if v.is_null() {
                    let key = rng.random_range(0..3u64);
                    *v = json!([column(key)]);
                    Some(format!("None to [#{key}]"))
                } else if rng.random_bool(0.5) {
                    *v = Value::Null;
                    Some("to None".to_string())
                } else {
                    mutate_key_exprs(v, rng)
                }
            },
        },
        Op {
            name: "generic_int",
            applies: |_, _, v| v.is_u64(),
            apply: |v, _, rng| {
                let n = v.as_u64()?;
                let new = random_index(rng, n);
                *v = json!(new);
                Some(format!("{n} to {new}"))
            },
        },
        Op {
            name: "generic_array",
            applies: |_, _, v| v.as_array().is_some_and(|a| !a.is_empty()),
            apply: |v, _, rng| {
                let items = v.as_array_mut()?;
                let i = rng.random_range(0..items.len());
                match rng.random_range(0..3) {
                    0 => {
                        items.remove(i);
                        Some(format!("remove element {i}"))
                    }
                    1 => {
                        let item = items[i].clone();
                        items.insert(i, item);
                        Some(format!("duplicate element {i}"))
                    }
                    _ => {
                        let j = rng.random_range(0..items.len());
                        if i == j {
                            return None;
                        }
                        items.swap(i, j);
                        Some(format!("swap elements {i} and {j}"))
                    }
                }
            },
        },
        Op {
            name: "generic_bool",
            applies: |_, _, v| v.is_boolean(),
            apply: |v, _, _| {
                let b = v.as_bool()?;
                *v = json!(!b);
                Some(format!("{b} to {}", !b))
            },
        },
    ]
}

/// A mutant: the mutated JSON, and the mutation that produced it.
struct Mutant {
    op: &'static str,
    description: String,
    json: String,
}

/// Applies one random mutation to `base`.
fn mutate(base: &Value, ops: &[Op], rng: &mut StdRng) -> Mutant {
    let ctx = Ctx::new(base);
    let mut paths = Vec::new();
    collect_paths(base, &mut Vec::new(), &mut paths);
    let applicable: Vec<(&Op, Vec<&JsonPath>)> = ops
        .iter()
        .map(|op| {
            let sites = paths
                .iter()
                .filter(|p| (op.applies)(base, p, at(base, p)))
                .collect::<Vec<_>>();
            (op, sites)
        })
        .filter(|(_, sites)| !sites.is_empty())
        .collect();
    loop {
        let (op, sites) = applicable.choose(rng).expect("some operator applies");
        let path = *sites.choose(rng).expect("non-empty");
        let mut mutant = base.clone();
        if let Some(description) = (op.apply)(at_mut(&mut mutant, path), &ctx, rng) {
            return Mutant {
                op: op.name,
                description: format!("{} @ {}: {description}", op.name, path_string(path)),
                json: serde_json::to_string(&mutant).expect("serializable"),
            };
        }
    }
}

// Validation.

/// What validating a mutant found.
enum Verdict {
    Unparseable(String),
    Unbound(String),
    Rejected(String),
    Accepted(Box<LirPlanDataflow>),
}

/// Runs the steps of `CREATE MATERIALIZED VIEW ... FROM PLAN` up to validation.
///
/// Returns an error if a step panics.
fn validate(
    json: &str,
    catalog: &LirCatalog,
    declared: &RelationDesc,
    sink: (GlobalId, PersistSink),
) -> Result<Verdict, String> {
    catch_unwind(AssertUnwindSafe(|| {
        let envelope = match parse_envelope(json) {
            Ok(envelope) => envelope,
            Err(e) => return Verdict::Unparseable(e.to_string()),
        };
        let mut dataflow = match LirPlanDataflow::new("lir-mutant", &envelope, catalog) {
            Ok(dataflow) => dataflow,
            Err(e) => return Verdict::Unbound(e.to_string()),
        };
        dataflow.as_of(Timestamp::from(0)).export_materialized_view(
            sink.0,
            declared.clone(),
            sink.1,
        );
        match dataflow.validate() {
            Ok(()) => Verdict::Accepted(Box::new(dataflow)),
            Err(errors) => Verdict::Rejected(
                errors
                    .iter()
                    .map(|e| format!("{:?}", e.rule))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        }
    }))
    .map_err(|e| {
        e.downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "panic".to_string())
    })
}

// The replica.

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("free port")
        .port()
}

/// A `clusterd` process and the driver connected to it.
struct Replica {
    binary: PathBuf,
    dir: PathBuf,
    compute_addr: String,
    pubsub_url: String,
    child: Child,
    log: PathBuf,
    log_offset: u64,
    driver: Driver,
    restarts: usize,
}

impl Replica {
    fn spawn_clusterd(
        binary: &Path,
        dir: &Path,
        compute_addr: &str,
        pubsub_url: &str,
        log: &Path,
    ) -> Child {
        let timely = |port: u16| {
            json!({
                "workers": 2,
                "process": 0,
                "addresses": [format!("127.0.0.1:{port}")],
                "arrangement_exert_proportionality": 16,
                "enable_zero_copy": false,
                "enable_zero_copy_lgalloc": false,
                "zero_copy_limit": null,
            })
            .to_string()
        };
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .expect("log file");
        for sub in ["scratch", "secrets"] {
            std::fs::create_dir_all(dir.join(sub)).expect("dir");
        }
        let wrapper = std::env::var("LIR_MUTATION_CLUSTERD_WRAPPER").unwrap_or_default();
        let mut wrapper = wrapper.split_whitespace();
        let mut command = match wrapper.next() {
            Some(program) => {
                let mut command = Command::new(program);
                command.args(wrapper).arg(binary);
                command
            }
            None => Command::new(binary),
        };
        // The wrapper and `clusterd` share a process group of their own, which `kill` ends as
        // a whole. Killing only the wrapper would leave `clusterd` holding its ports.
        command
            .process_group(0)
            .args([
                "--compute-controller-listen-addr",
                compute_addr,
                "--storage-controller-listen-addr",
                &format!("127.0.0.1:{}", free_port()),
                "--compute-timely-config",
                &timely(free_port()),
                "--storage-timely-config",
                &timely(free_port()),
                "--process",
                "0",
                "--environment-id",
                "mzcompose-us-east-1-00000000-0000-0000-0000-000000000000-0",
                "--secrets-reader",
                "local-file",
                "--secrets-reader-local-file-dir",
                &dir.join("secrets").display().to_string(),
                "--scratch-directory",
                &dir.join("scratch").display().to_string(),
            ])
            .env("PERSIST_PUBSUB_URL", pubsub_url)
            .env("MZ_SOFT_ASSERTIONS", "1")
            .env("MZ_LOG_FILTER", "warn")
            .stdout(Stdio::from(log_file.try_clone().expect("log file")))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawn clusterd")
    }

    /// Waits until clusterd accepts connections on `addr`.
    async fn wait_for_port(addr: &str) {
        for _ in 0..240 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("clusterd does not listen on {addr}");
    }

    async fn start(
        binary: PathBuf,
        dir: PathBuf,
        host: PersistHost,
        index: &mz_compute_types::dataflows::RenderDataflowDescription<
            mz_storage_types::controller::CollectionMetadata,
        >,
    ) -> Self {
        let compute_addr = format!("127.0.0.1:{}", free_port());
        let pubsub_url = host.pubsub_url();
        let log = dir.join("clusterd.log");
        let child = Self::spawn_clusterd(&binary, &dir, &compute_addr, &pubsub_url, &log);
        Self::wait_for_port(&compute_addr).await;
        let driver = Driver::connect(host, &compute_addr).await.expect("connect");
        let mut replica = Replica {
            binary,
            dir,
            compute_addr,
            pubsub_url,
            child,
            log,
            log_offset: 0,
            driver,
            restarts: 0,
        };
        replica.initialize(index).await;
        replica
    }

    /// Opens the instance and installs the index the plans import.
    async fn initialize(
        &mut self,
        index: &mz_compute_types::dataflows::RenderDataflowDescription<
            mz_storage_types::controller::CollectionMetadata,
        >,
    ) {
        let mut config = ConfigUpdates::default();
        config.add(&ORE_OVERFLOWING_BEHAVIOR, "panic");
        self.driver
            .create_instance(None, false, config)
            .expect("create instance");
        self.driver
            .send(ComputeCommand::InitializationComplete)
            .expect("initialization complete");
        self.driver.submit_dataflow(index.clone()).expect("index");
        self.driver.schedule(PU_INDEX.1).expect("schedule index");
        self.driver
            .expect_frontier(
                PU_INDEX.1,
                Timestamp::from(INPUT_UPPER),
                Duration::from_secs(120),
            )
            .await
            .expect("index hydrates");
        self.log_offset = self.log_len();
    }

    async fn restart(
        &mut self,
        index: &mz_compute_types::dataflows::RenderDataflowDescription<
            mz_storage_types::controller::CollectionMetadata,
        >,
    ) {
        kill(&mut self.child);
        self.child = Self::spawn_clusterd(
            &self.binary,
            &self.dir,
            &self.compute_addr,
            &self.pubsub_url,
            &self.log,
        );
        Self::wait_for_port(&self.compute_addr).await;
        self.driver.reconnect().await.expect("reconnect");
        self.restarts += 1;
        self.initialize(index).await;
    }

    fn exited(&mut self) -> Option<String> {
        exit_status(&mut self.child)
    }

    fn log_len(&self) -> u64 {
        std::fs::metadata(&self.log).map(|m| m.len()).unwrap_or(0)
    }

    /// The log lines written since the last call that report a panic or a failed soft
    /// assertion.
    fn new_problems(&mut self) -> Vec<String> {
        let mut file = std::fs::File::open(&self.log).expect("log");
        file.seek(SeekFrom::Start(self.log_offset)).expect("seek");
        let mut text = String::new();
        file.read_to_string(&mut text).expect("read log");
        self.log_offset += u64::try_from(text.len()).expect("fits");
        text.lines()
            .filter(|l| {
                l.contains("panicked") || l.contains("soft assertion") || l.contains("halting")
            })
            .map(|l| l.chars().take(500).collect())
            .collect()
    }

    /// Renders `dataflow` and checks that the replica survives.
    async fn run(
        &mut self,
        dataflow: mz_compute_types::dataflows::RenderDataflowDescription<
            mz_storage_types::controller::CollectionMetadata,
        >,
        sink_id: GlobalId,
        pu_desc: &RelationDesc,
    ) -> Outcome {
        let mz_compute_types::sinks::ComputeSinkConnection::MaterializedView(sink) =
            &dataflow.sink_exports[&sink_id].connection
        else {
            panic!("not a materialized view sink");
        };
        let metadata = sink.storage_metadata.clone();
        self.driver.submit_dataflow(dataflow).expect("submit");
        self.driver.schedule(sink_id).expect("schedule");
        self.driver
            .send(ComputeCommand::AllowWrites(sink_id))
            .expect("allow writes");

        let hydrated = {
            let child = &mut self.child;
            let wait = self.driver.expect_frontier(
                sink_id,
                Timestamp::from(INPUT_UPPER),
                Duration::from_secs(10),
            );
            tokio::pin!(wait);
            loop {
                tokio::select! {
                    result = &mut wait => break result.is_ok(),
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        if let Some(status) = exit_status(child) {
                            return Outcome::Crashed(status);
                        }
                    }
                }
            }
        };

        // The replica must still serve a peek of the long-lived index.
        let probe = tokio::time::timeout(
            Duration::from_secs(30),
            self.driver.peek(
                PeekTarget::Index { id: PU_INDEX.1 },
                pu_desc.clone(),
                Timestamp::from(0),
            ),
        )
        .await;
        if let Some(status) = self.exited() {
            return Outcome::Crashed(status);
        }
        match probe {
            Ok(Ok(rows)) if !rows.is_empty() => {}
            Ok(Ok(_)) => return Outcome::Crashed("probe peek returned no rows".to_string()),
            Ok(Err(e)) => return Outcome::Crashed(format!("probe peek failed: {e}")),
            Err(_) => return Outcome::Crashed("probe peek timed out".to_string()),
        }

        // The sink's output, read back from its shard. Errors in the output are not replica
        // failures.
        let output = if hydrated {
            let desc = metadata.relation_desc.clone();
            let peek = self.driver.peek(
                PeekTarget::Persist {
                    id: sink_id,
                    metadata,
                },
                desc,
                Timestamp::from(INPUT_UPPER - 1),
            );
            match tokio::time::timeout(Duration::from_secs(30), peek).await {
                Ok(Ok(rows)) => format!("{} rows", rows.len()),
                Ok(Err(e)) => format!("output peek failed: {e}"),
                Err(_) => "output peek timed out".to_string(),
            }
        } else {
            String::new()
        };
        if let Some(status) = self.exited() {
            return Outcome::Crashed(status);
        }

        self.driver
            .send(ComputeCommand::AllowCompaction {
                id: sink_id,
                frontier: Antichain::new(),
            })
            .expect("drop");
        if hydrated {
            Outcome::Hydrated(output)
        } else {
            Outcome::Stalled
        }
    }
}

/// Kills the process group of `child`, which `Replica::spawn_clusterd` creates, and reaps
/// `child`.
fn kill(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-KILL", &format!("-{}", child.id())])
        .status();
    let _ = child.wait();
}

fn exit_status(child: &mut Child) -> Option<String> {
    child
        .try_wait()
        .expect("wait")
        .map(|status| status.to_string())
}

enum Outcome {
    /// The sink caught up with the input, and wrote the output described.
    Hydrated(String),
    Stalled,
    Crashed(String),
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Renders the join base plan with a projection past its last column, which the validator
/// rejects, and checks that the harness sees the replica crash.
async fn check_crash_detection(
    replica: &mut Replica,
    catalog: &LirCatalog,
    location: &PersistLocation,
    pu_desc: &RelationDesc,
    index: &mz_compute_types::dataflows::RenderDataflowDescription<
        mz_storage_types::controller::CollectionMetadata,
    >,
) {
    let base = BASES.iter().find(|b| b.name == "join").expect("join base");
    let mut json: Value = serde_json::from_str(base.json).expect("base JSON");
    let projection = &mut json["objects"][0]["plan"]["node"]["Join"]["plan"]["Linear"]["stage_plans"]
        [0]["closure"]["before"]["mfp"]["projection"];
    assert_eq!(*projection, json!([0, 2, 1]), "join base changed");
    *projection = json!([0, 9, 1]);
    let envelope = parse_envelope(&json.to_string()).expect("parses");
    let mut dataflow = LirPlanDataflow::new("crash-check", &envelope, catalog).expect("binds");
    let sink_id = GlobalId::User(999);
    let declared = declared_desc(
        &LirPlanDataflow::new(
            "crash-check",
            &parse_envelope(base.json).expect("parses"),
            catalog,
        )
        .expect("binds")
        .output_type()
        .expect("valid"),
    );
    let target = PersistSink {
        shard: ShardId::new(),
        location: location.clone(),
    };
    dataflow
        .as_of(Timestamp::from(0))
        .export_materialized_view(sink_id, declared, target);
    assert!(
        dataflow.validate().is_err(),
        "the validator rejects the plan"
    );
    let dataflow = dataflow.finish().expect("converts");
    match replica.run(dataflow, sink_id, pu_desc).await {
        Outcome::Crashed(status) => {
            println!(
                "crash detection works: {status}: {:?}",
                replica.new_problems()
            );
            replica.restart(index).await;
        }
        Outcome::Hydrated(_) | Outcome::Stalled => panic!("the replica survived the bad plan"),
    }
}

#[derive(Default, Debug)]
struct Counts {
    generated: usize,
    duplicates: usize,
    unparseable: usize,
    unbound: usize,
    rejected: usize,
    accepted: usize,
    validator_panics: usize,
    conversion_failures: usize,
    hydrated: usize,
    stalled: usize,
    crashed: usize,
    replica_problems: usize,
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)]
async fn lir_validator_mutations() {
    let per_base: usize = env_or("LIR_MUTANTS_PER_BASE", DEFAULT_MUTANTS_PER_BASE);
    let seed: u64 = env_or("LIR_MUTATION_SEED", DEFAULT_SEED);
    let clusterd = std::env::var_os("LIR_MUTATION_CLUSTERD").map(PathBuf::from);
    let out_dir = std::env::var_os("LIR_MUTATION_OUT").map(PathBuf::from);
    let mut results = out_dir.as_ref().map(|dir| {
        std::fs::create_dir_all(dir).expect("out dir");
        std::fs::File::create(dir.join("results.jsonl")).expect("results")
    });

    let work = tempfile::tempdir().expect("tempdir");
    let location = match &clusterd {
        Some(_) => PersistLocation {
            blob_uri: format!("file://{}", work.path().join("blob").display())
                .parse()
                .expect("blob uri"),
            consensus_uri: std::env::var("COCKROACH_URL")
                .expect("COCKROACH_URL for persist consensus")
                .parse()
                .expect("consensus uri"),
        },
        None => PersistLocation::new_in_mem(),
    };
    let host = PersistHost::start(location.clone()).await.expect("host");

    let tables = tables();
    let shards: BTreeMap<_, _> = tables.iter().map(|t| (t.name, ShardId::new())).collect();
    let catalogs = [false, true].map(|monotonic| catalog(&location, &shards, monotonic));
    let pu_desc = table_desc(&tables[1]);

    let index = index_dataflow(
        GlobalId::User(2),
        PU_INDEX.1,
        shards[tables[1].name],
        location.clone(),
        pu_desc.clone(),
        vec![PU_INDEX.2],
        Timestamp::from(0),
        Timestamp::from(INPUT_UPPER),
    )
    .expect("index dataflow");

    let mut replica = match clusterd {
        Some(binary) => {
            let client = host.client().await.expect("client");
            for table in &tables {
                let rows: Vec<Row> = table.rows.iter().map(|r| pack_cells(r)).collect();
                write_rows_spread(
                    &client,
                    shards[table.name],
                    &table_desc(table),
                    &rows,
                    INPUT_UPPER,
                )
                .await
                .expect("write input");
            }
            let dir = out_dir.clone().unwrap_or_else(|| work.path().to_path_buf());
            Some(Replica::start(binary, dir, host, &index).await)
        }
        None => None,
    };

    if let Some(replica) = replica.as_mut() {
        check_crash_detection(replica, &catalogs[0], &location, &pu_desc, &index).await;
    }

    let ops = ops();
    let mut rng = StdRng::seed_from_u64(seed);
    let mut counts = Counts::default();
    let mut op_counts: BTreeMap<&str, [usize; 3]> = BTreeMap::new();
    let mut failures = Vec::new();
    let mut next_sink = 1000u64;

    for base in BASES {
        let catalog = &catalogs[usize::from(base.monotonic)];
        let base_json: Value = serde_json::from_str(base.json).expect("base JSON");
        let envelope = parse_envelope(base.json).expect("base parses");
        let output = LirPlanDataflow::new(base.name, &envelope, catalog)
            .expect("base binds")
            .output_type()
            .unwrap_or_else(|e| panic!("base {} is invalid: {e:?}", base.name));
        let declared = declared_desc(&output);

        // The base plan itself is accepted and rendered.
        let mut mutants = vec![Mutant {
            op: "none",
            description: "base".to_string(),
            json: base.json.to_string(),
        }];
        let mut seen = std::collections::BTreeSet::new();
        seen.insert(serde_json::to_string(&base_json).expect("serializable"));
        // Mutations whose result another mutation produced already are drawn again.
        let mut attempts = 0;
        while mutants.len() <= per_base && attempts < per_base * 20 {
            attempts += 1;
            let mutant = mutate(&base_json, &ops, &mut rng);
            if seen.insert(mutant.json.clone()) {
                mutants.push(mutant);
            } else {
                counts.duplicates += 1;
            }
        }

        for mutant in mutants {
            counts.generated += 1;
            let sink_id = GlobalId::User(next_sink);
            next_sink += 1;
            let target = PersistSink {
                shard: ShardId::new(),
                location: location.clone(),
            };
            let stats = op_counts.entry(mutant.op).or_default();
            let (verdict, detail, accepted) =
                match validate(&mutant.json, catalog, &declared, (sink_id, target)) {
                    Err(panic) => {
                        counts.validator_panics += 1;
                        failures.push(format!("validator panic: {}: {panic}", mutant.description));
                        ("validator_panic", panic, None)
                    }
                    Ok(Verdict::Unparseable(e)) => {
                        counts.unparseable += 1;
                        ("unparseable", e, None)
                    }
                    Ok(Verdict::Unbound(e)) => {
                        counts.unbound += 1;
                        ("unbound", e, None)
                    }
                    Ok(Verdict::Rejected(rules)) => {
                        counts.rejected += 1;
                        stats[1] += 1;
                        ("rejected", rules, None)
                    }
                    Ok(Verdict::Accepted(dataflow)) => {
                        counts.accepted += 1;
                        stats[2] += 1;
                        ("accepted", String::new(), Some(dataflow))
                    }
                };
            stats[0] += 1;
            if mutant.op == "none" && verdict != "accepted" {
                panic!("base {} not accepted: {verdict}: {detail}", base.name);
            }

            let mut outcome = None;
            if let Some(dataflow) = accepted {
                let dataflow = match catch_unwind(AssertUnwindSafe(|| dataflow.finish())) {
                    Ok(Ok(dataflow)) => Some(dataflow),
                    Ok(Err(e)) => {
                        counts.conversion_failures += 1;
                        failures.push(format!(
                            "accepted but not convertible: {}: {e}",
                            mutant.description
                        ));
                        outcome = Some(("conversion_failure".to_string(), e.to_string()));
                        None
                    }
                    Err(_) => {
                        counts.conversion_failures += 1;
                        failures.push(format!(
                            "accepted but conversion panicked: {}",
                            mutant.description
                        ));
                        outcome = Some(("conversion_panic".to_string(), String::new()));
                        None
                    }
                };
                if let (Some(replica), Some(dataflow)) = (replica.as_mut(), dataflow) {
                    if let Some(dir) = &out_dir {
                        std::fs::write(dir.join("pending.json"), &mutant.json)
                            .expect("write pending mutant");
                    }
                    let result = replica.run(dataflow, sink_id, &pu_desc).await;
                    let problems = replica.new_problems();
                    if !problems.is_empty() {
                        counts.replica_problems += 1;
                        failures.push(format!(
                            "replica reported problems: {}: {problems:?}",
                            mutant.description
                        ));
                    }
                    let problems = problems.join("\n");
                    outcome = Some(match result {
                        Outcome::Hydrated(output) => {
                            counts.hydrated += 1;
                            if mutant.op == "none" {
                                println!("base {}: {output}", base.name);
                            }
                            ("hydrated".to_string(), format!("{output}\n{problems}"))
                        }
                        Outcome::Stalled => {
                            counts.stalled += 1;
                            ("stalled".to_string(), problems)
                        }
                        Outcome::Crashed(status) => {
                            counts.crashed += 1;
                            failures.push(format!(
                                "replica crashed: {}: {status}: {problems}",
                                mutant.description
                            ));
                            replica.restart(&index).await;
                            ("crashed".to_string(), format!("{status}\n{problems}"))
                        }
                    });
                }
            }

            if let Some(file) = results.as_mut() {
                let line = json!({
                    "base": base.name,
                    "op": mutant.op,
                    "mutation": mutant.description,
                    "verdict": verdict,
                    "detail": detail,
                    "outcome": outcome.as_ref().map(|o| &o.0),
                    "outcome_detail": outcome.as_ref().map(|o| &o.1),
                });
                writeln!(file, "{line}").expect("write result");
                let interesting = outcome.as_ref().is_some_and(|o| o.0 != "hydrated");
                if interesting {
                    let dir = out_dir.as_ref().expect("out dir");
                    std::fs::write(
                        dir.join(format!("mutant-{}-{}.json", base.name, sink_id)),
                        &mutant.json,
                    )
                    .expect("write mutant");
                }
            }
        }
    }

    println!("seed {seed:#x}, {per_base} mutants per base");
    println!("{counts:#?}");
    println!("per operator [validated, rejected, accepted]:");
    for (op, [n, rejected, accepted]) in &op_counts {
        println!("  {op:20} {n:5} {rejected:5} {accepted:5}");
    }
    if let Some(replica) = &replica {
        println!("replica restarts: {}", replica.restarts);
    }
    for failure in &failures {
        println!("FAILURE: {failure}");
    }
    if let Some(mut replica) = replica {
        kill(&mut replica.child);
    }
    assert!(failures.is_empty(), "{} failures", failures.len());
    if per_base >= DEFAULT_MUTANTS_PER_BASE {
        assert!(
            counts.generated >= MIN_MUTANTS,
            "only {} mutants generated",
            counts.generated
        );
    }
}
