// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated compute workloads: a JSON description of a dataflow plus the
//! oracles that decide whether rendering it was correct.
//!
//! A [`Workload`] is the unit a corpus file holds and the runner executes. It
//! carries the MIR to render, the input data to render it over (as timestamped
//! batches, so incremental maintenance is exercised), the exports to build, the
//! dyncfg configurations to repeat under, the surface cells it claims to cover,
//! and which oracles to apply.
//!
//! # Why MIR travels as JSON
//!
//! [`MirRelationExpr`] is `Serialize`/`Deserialize`, so a workload embeds the plan
//! directly. The alternative, printing MIR to the `mz-expr-parser` text form and
//! reparsing it, would need a relation printer that does not exist (only
//! `print_scalar` does) and would risk silent fidelity loss on the round trip.
//!
//! # Oracles
//!
//! The oracles are deliberately layered, because each covers where the previous
//! one goes blind:
//!
//!  * [`Oracle::FoldConstants`] is the strongest and the primary one: substitute
//!    each input's actual updates back into the plan as literal `Constant`s and
//!    evaluate with the optimizer's constant folder, an entirely separate
//!    implementation from the renderer. It cannot see through `LetRec`, which is
//!    exactly why the others exist.
//!  * [`Oracle::ExportInvariance`] compares an index peek, a materialized-view
//!    shard read-back, and a subscribe accumulated to the same timestamp. It needs
//!    no reference implementation, so it still works where folding does not, and
//!    it is the only oracle that can catch a sink-specific bug.
//!  * [`Oracle::Incremental`] compares the maintained collection against a
//!    dataflow freshly created at the current `as_of`. Where `FoldConstants` is
//!    live this is a redundant cross-check, since both agreeing with an
//!    independent reference implies agreeing with each other. Its real value is
//!    the `LetRec` case, where it is the only thing that distinguishes a correct
//!    incremental update from a wrong one.
//!  * [`Oracle::StrategyInvariance`] is not a per-run oracle but a property of the
//!    `configs` list: the same workload must produce the same output under every
//!    configuration. This is what tests the compute strategy dyncfgs (join core,
//!    batcher spill, correction v2, dictionary compression, temporal bucketing)
//!    without needing to know what the right answer is.
//!
//! Every run additionally asserts the realized plan's surface cells against
//! `claims`, so a generator that drifts away from a cell fails loudly instead of
//! quietly testing less.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::MirRelationExpr;
use mz_repr::{Datum, Diff, Row, SqlColumnType, SqlScalarType, Timestamp};
use mz_transform::mirgen::Ty;
use serde::{Deserialize, Serialize};

use crate::script::ConfigSetting;
use crate::surface::SurfaceCell;

/// A generated compute workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workload {
    /// A stable name, used for the corpus filename and in failure messages.
    pub name: String,
    /// The seed the generator drew this workload from, so a failure is replayable
    /// from the seed alone.
    #[serde(default)]
    pub seed: Option<u64>,
    /// The input collections, each backed by its own persist shard.
    pub inputs: Vec<InputSpec>,
    /// The computation, referencing input `i` as `Get(GlobalId::User(input_id(i)))`.
    pub plan: MirRelationExpr,
    /// The exports to build over the computation.
    pub exports: Vec<WorkloadExport>,
    /// Dyncfg configurations to run the workload under. An empty list means one
    /// run at the replica's defaults. More than one turns on strategy invariance.
    #[serde(default)]
    pub configs: Vec<NamedConfig>,
    /// The surface cells this workload claims to exercise. Asserted against the
    /// realized plan (see the module docs).
    #[serde(default)]
    pub claims: BTreeSet<SurfaceCell>,
    /// Which oracles to apply.
    pub oracles: Vec<Oracle>,
    /// Whether the MIR optimizer runs before lowering. Required for plans holding
    /// a `Join`, whose implementation is `Unimplemented` until
    /// `JoinImplementation` fills it in.
    #[serde(default)]
    pub optimize: bool,
}

/// A named dyncfg configuration, so a failure report can say which one diverged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedConfig {
    /// A short label, e.g. `mz-join-core` or `defaults`.
    pub name: String,
    /// The dyncfg settings to apply before creating the dataflow.
    pub settings: Vec<ConfigSetting>,
}

/// One input collection: a schema and a sequence of timestamped update batches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputSpec {
    /// Column types, matching the `Get`'s relation type in `plan`. All columns are
    /// nullable, as the generators declare them.
    pub schema: Vec<ColumnTy>,
    /// Update batches in timestamp order. Batch `i` seals `[i, i+1)`.
    pub batches: Vec<Batch>,
}

/// The scalar types a workload's inputs can carry.
///
/// Mirrors [`Ty`], which is what the generators produce. Kept as its own type so
/// the JSON has a stable spelling independent of the generator's internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnTy {
    /// `int4`.
    Int32,
    /// `int8`.
    Int64,
    /// `boolean`.
    Bool,
}

impl From<Ty> for ColumnTy {
    fn from(ty: Ty) -> Self {
        match ty {
            Ty::Int32 => ColumnTy::Int32,
            Ty::Int64 => ColumnTy::Int64,
            Ty::Bool => ColumnTy::Bool,
        }
    }
}

impl ColumnTy {
    /// The SQL scalar type, for the persist `RelationDesc`.
    pub fn sql_scalar_type(self) -> SqlScalarType {
        match self {
            ColumnTy::Int32 => SqlScalarType::Int32,
            ColumnTy::Int64 => SqlScalarType::Int64,
            ColumnTy::Bool => SqlScalarType::Bool,
        }
    }
}

/// One batch of updates written at a single timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    /// The updates, as `(values, diff)`. A negative diff retracts, which is what
    /// drives the correction and consolidation paths.
    pub updates: Vec<Update>,
}

/// A single update: one row's values plus its multiplicity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    /// Column values, in schema order.
    pub values: Vec<Value>,
    /// The multiplicity. Negative retracts.
    pub diff: i64,
}

/// A scalar value in an input update.
///
/// Spelled as a tagged enum rather than a bare JSON scalar so `null` is
/// unambiguous and so an `int8` value outside `f64`'s exact range survives the
/// round trip through JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// An `int4` value.
    Int32(i32),
    /// An `int8` value, spelled as a string so JSON's number precision never
    /// truncates it.
    Int64(String),
    /// A `boolean` value.
    Bool(bool),
}

impl Value {
    /// Borrow this value as a [`Datum`], validating it against `ty`.
    pub fn datum(&self) -> anyhow::Result<Datum<'_>> {
        Ok(match self {
            Value::Null => Datum::Null,
            Value::Int32(v) => Datum::Int32(*v),
            Value::Int64(v) => Datum::Int64(v.parse()?),
            Value::Bool(true) => Datum::True,
            Value::Bool(false) => Datum::False,
        })
    }

    /// Build a value from a [`Datum`], for the generator side.
    pub fn from_datum(datum: Datum<'_>) -> anyhow::Result<Self> {
        Ok(match datum {
            Datum::Null => Value::Null,
            Datum::Int32(v) => Value::Int32(v),
            Datum::Int64(v) => Value::Int64(v.to_string()),
            Datum::True => Value::Bool(true),
            Datum::False => Value::Bool(false),
            other => anyhow::bail!("unsupported datum in a workload input: {other:?}"),
        })
    }
}

/// An export to build over the workload's computation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadExport {
    /// An arrangement, read back by an index peek.
    Index,
    /// A persist sink, read back from its output shard.
    MaterializedView,
    /// A subscribe, read back from its accumulated response batches.
    Subscribe,
}

/// Which correctness check to apply. See the module docs for why each exists.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
#[serde(rename_all = "kebab-case")]
pub enum Oracle {
    /// Compare against the optimizer's constant folder over the same plan.
    FoldConstants,
    /// Compare the workload's exports against each other.
    ExportInvariance,
    /// Compare the maintained collection against a freshly created dataflow.
    Incremental,
    /// Compare across `configs`. A property of the config list, checked by the
    /// runner rather than within a single run.
    StrategyInvariance,
}

/// Global ids a workload occupies, laid out so a workload's ids never collide
/// with another's within a run and never collide with the `script` module's
/// ephemeral range.
///
/// The layout is fixed rather than allocated so a JSON plan can name its inputs
/// by id without the generator and the runner having to agree on an allocator.
///
/// # Per-configuration ids
///
/// Everything the runner *creates* is offset by the configuration index, so no
/// two configurations of the same workload ever name the same collection. Only
/// the input ids are shared, and those are imports rather than creations, which
/// any number of dataflows may hold at once.
///
/// This is not tidiness. Each configuration reconnects to drop the previous one's
/// dataflows, and the replica's teardown of the old session is not synchronous
/// with the new session's commands. Reusing ids across configurations puts a
/// newly created collection and a dying one of the same name in the same window,
/// which showed up as a dataflow that never reported a frontier, intermittently
/// and more often the further into the configuration matrix a workload got.
/// Offsetting the ids removes the window rather than trying to time it.
pub mod ids {
    use super::WorkloadExport;

    /// The first id of a workload's input sources. Not offset per configuration:
    /// inputs are imported, not created, and their shards are written once for
    /// the whole workload.
    pub const INPUT_BASE: u64 = 100;

    /// How far apart successive configurations' id ranges sit. Larger than the
    /// number of ids any one configuration uses, with room to spare.
    pub const CONFIG_STRIDE: u64 = 1000;

    /// The base of configuration `config`'s id range.
    pub fn config_base(config: usize) -> u64 {
        1000 + u64::try_from(config).expect("config count fits u64") * CONFIG_STRIDE
    }

    /// The exported index's id, within configuration `config`.
    pub fn index(config: usize) -> u64 {
        config_base(config) + 1
    }

    /// The materialized-view sink's id, within configuration `config`.
    pub fn mv_sink(config: usize) -> u64 {
        config_base(config) + 2
    }

    /// The subscribe sink's id, within configuration `config`.
    pub fn subscribe_sink(config: usize) -> u64 {
        config_base(config) + 3
    }

    /// The recompute dataflow's computation id, for the incremental oracle.
    pub fn recompute_plan(config: usize) -> u64 {
        config_base(config) + 4
    }

    /// The recompute dataflow's index id, for the incremental oracle.
    pub fn recompute_index(config: usize) -> u64 {
        config_base(config) + 5
    }

    /// The id the computation is bound to inside `export`'s own dataflow, within
    /// configuration `config`.
    ///
    /// Each export gets its own dataflow, hence its own binding for the same
    /// computation. Sharing one id across dataflows is not an option: a global id
    /// names one collection in the instance.
    pub fn plan(config: usize, export: WorkloadExport) -> u64 {
        let offset = match export {
            WorkloadExport::Index => 10,
            WorkloadExport::MaterializedView => 11,
            WorkloadExport::Subscribe => 12,
        };
        config_base(config) + offset
    }

    /// The global id of input `i`.
    pub fn input(i: usize) -> u64 {
        INPUT_BASE + u64::try_from(i).expect("input count fits u64")
    }
}

impl InputSpec {
    /// The relation description this input's shard is written and read with.
    pub fn relation_desc(&self) -> mz_repr::RelationDesc {
        let mut builder = mz_repr::RelationDesc::builder();
        for (i, ty) in self.schema.iter().enumerate() {
            builder = builder.with_column(
                format!("c{i}"),
                SqlColumnType {
                    scalar_type: ty.sql_scalar_type(),
                    // The generators declare every column nullable, and the plan's
                    // `Get` relation type must agree or the lowering rejects it.
                    nullable: true,
                },
            );
        }
        builder.finish()
    }

    /// This input's updates as persist-ready triples, with batch `i` at timestamp
    /// `i`.
    pub fn updates(&self) -> anyhow::Result<Vec<(Row, Timestamp, i64)>> {
        let arity = self.schema.len();
        let mut out = Vec::new();
        for (ts, batch) in self.batches.iter().enumerate() {
            let ts = Timestamp::from(u64::try_from(ts).expect("batch count fits u64"));
            for update in &batch.updates {
                anyhow::ensure!(
                    update.values.len() == arity,
                    "update has {} values but the schema has {arity} columns",
                    update.values.len()
                );
                let mut row = Row::default();
                let mut packer = row.packer();
                for value in &update.values {
                    packer.push(value.datum()?);
                }
                out.push((row, ts, update.diff));
            }
        }
        Ok(out)
    }

    /// The number of batches, which is also the exclusive upper the shard is
    /// sealed to.
    pub fn upper(&self) -> u64 {
        u64::try_from(self.batches.len()).expect("batch count fits u64")
    }

    /// This input's contents as of `ts`, as a consolidated `(row, diff)` multiset.
    ///
    /// This is what the fold oracle substitutes into the plan: everything written
    /// at timestamps `<= ts`, accumulated. Rows whose diffs cancel are dropped, so
    /// the substituted constant matches what a dataflow reading `as_of = ts`
    /// observes.
    pub fn contents_at(&self, ts: u64) -> anyhow::Result<Vec<(Row, Diff)>> {
        let mut acc: BTreeMap<Row, Diff> = BTreeMap::new();
        for (row, update_ts, diff) in self.updates()? {
            if u64::from(update_ts) <= ts {
                *acc.entry(row).or_insert(Diff::ZERO) += Diff::from(diff);
            }
        }
        acc.retain(|_, d| *d != Diff::ZERO);
        Ok(acc.into_iter().collect())
    }
}

impl Workload {
    /// The timestamp the workload's results are asserted at: the last batch
    /// timestamp across all inputs, so every write is visible.
    ///
    /// A workload with no batches asserts at `0` over empty inputs, which is a
    /// legitimate case (an empty collection is a real answer, and several render
    /// paths only differ on empty input).
    pub fn assert_ts(&self) -> u64 {
        self.inputs
            .iter()
            .map(|i| i.batches.len().saturating_sub(1))
            .max()
            .unwrap_or(0)
            .try_into()
            .expect("batch count fits u64")
    }

    /// The exclusive upper every input shard is sealed to. Uniform across inputs
    /// so the dataflow's inputs all become available at the same time, which keeps
    /// the assertion timestamp meaningful.
    pub fn upper(&self) -> u64 {
        self.inputs
            .iter()
            .map(|i| i.upper())
            .max()
            .unwrap_or(1)
            .max(1)
    }

    /// The plan with each input `Get` replaced by a literal `Constant` of that
    /// input's contents as of `ts`.
    ///
    /// This is the fold oracle's reference input. Substituting the data the
    /// dataflow actually read, rather than re-deriving it, is what makes the
    /// comparison meaningful: both sides see identical input, so a divergence is a
    /// rendering bug and not a data-setup difference.
    pub fn plan_with_constants(&self, ts: u64) -> anyhow::Result<MirRelationExpr> {
        use mz_expr::Id;
        use mz_expr::visit::Visit;
        use mz_repr::GlobalId;

        // Precompute each input's constant form, keyed by its global id.
        let mut by_id: BTreeMap<GlobalId, MirRelationExpr> = BTreeMap::new();
        for (i, input) in self.inputs.iter().enumerate() {
            let contents = input.contents_at(ts)?;
            let typ = mz_transform::mirgen::nullable_relation_type(
                &input
                    .schema
                    .iter()
                    .map(|t| match t {
                        ColumnTy::Int32 => Ty::Int32,
                        ColumnTy::Int64 => Ty::Int64,
                        ColumnTy::Bool => Ty::Bool,
                    })
                    .collect::<Vec<_>>(),
            );
            by_id.insert(
                GlobalId::User(ids::input(i)),
                MirRelationExpr::Constant {
                    rows: Ok(contents),
                    typ,
                },
            );
        }

        let mut plan = self.plan.clone();
        plan.try_visit_mut_post::<_, anyhow::Error>(&mut |e| {
            if let MirRelationExpr::Get {
                id: Id::Global(g), ..
            } = e
            {
                let replacement = by_id.get(g).ok_or_else(|| {
                    anyhow::anyhow!("plan gets unknown global id {g}; not one of its inputs")
                })?;
                *e = replacement.clone();
            }
            Ok(())
        })?;
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int64(v: i64) -> Value {
        Value::Int64(v.to_string())
    }

    fn input_with_batches() -> InputSpec {
        InputSpec {
            schema: vec![ColumnTy::Int64],
            batches: vec![
                Batch {
                    updates: vec![
                        Update {
                            values: vec![int64(1)],
                            diff: 1,
                        },
                        Update {
                            values: vec![int64(2)],
                            diff: 1,
                        },
                    ],
                },
                Batch {
                    // Retract 1, add 3.
                    updates: vec![
                        Update {
                            values: vec![int64(1)],
                            diff: -1,
                        },
                        Update {
                            values: vec![int64(3)],
                            diff: 1,
                        },
                    ],
                },
            ],
        }
    }

    /// `contents_at` accumulates through the requested timestamp and drops rows
    /// whose diffs cancel, which is what makes it match a dataflow's view.
    #[mz_ore::test]
    fn contents_accumulate_and_cancel() {
        let input = input_with_batches();

        let at0 = input.contents_at(0).unwrap();
        assert_eq!(at0.len(), 2, "both rows of batch 0 present at ts 0");

        let at1 = input.contents_at(1).unwrap();
        // Row 1 was retracted, so only 2 and 3 survive.
        assert_eq!(at1.len(), 2);
        let vals: Vec<i64> = at1
            .iter()
            .map(|(r, _)| r.unpack_first().unwrap_int64())
            .collect();
        assert_eq!(vals, vec![2, 3]);
        assert!(at1.iter().all(|(_, d)| *d == Diff::from(1)));
    }

    /// `int8` values survive the JSON round trip at full precision. A bare JSON
    /// number would lose the low bits of a large `i64` through `f64`.
    #[mz_ore::test]
    fn int64_round_trips_at_full_precision() {
        let v = int64(i64::MAX);
        let json = serde_json::to_string(&v).unwrap();
        let back: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
        assert_eq!(back.datum().unwrap(), Datum::Int64(i64::MAX));
    }

    /// A workload's assertion timestamp is the last batch timestamp, and its
    /// upper is one past it, so every write is visible at the assertion.
    #[mz_ore::test]
    fn assert_ts_and_upper() {
        let w = Workload {
            name: "t".into(),
            seed: None,
            inputs: vec![input_with_batches()],
            plan: MirRelationExpr::constant(vec![], mz_repr::ReprRelationType::new(vec![])),
            exports: vec![WorkloadExport::Index],
            configs: vec![],
            claims: BTreeSet::new(),
            oracles: vec![Oracle::FoldConstants],
            optimize: false,
        };
        assert_eq!(w.assert_ts(), 1);
        assert_eq!(w.upper(), 2);
    }
}
