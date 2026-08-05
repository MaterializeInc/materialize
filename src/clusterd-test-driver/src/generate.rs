// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generation of a workload corpus that covers the compute rendering surface.
//!
//! # Why generate-and-filter rather than one workload per cell
//!
//! A cell is a property of the *lowered* plan, and lowering is where the
//! interesting decisions happen: which reduce strategy a set of aggregates
//! selects, whether a join comes out linear or delta, when an arrangement gets a
//! temporal bucket. Hand-writing a plan per cell would mean encoding those
//! decisions in the generator, where they would drift out of step with the
//! lowering that actually makes them.
//!
//! So instead: draw random MIR with [`mz_transform::mirgen::gen_rel`], lower it,
//! ask [`crate::surface`] which cells came out, and keep the workload only if it
//! covers a cell nothing kept so far covers. That is greedy set cover, and it has
//! the property the whole exercise needs, which is that coverage is *measured*
//! against real lowering rather than asserted by construction. It also reports
//! which cells the generator never reached, and those are the interesting output:
//! either a gap in the generator or a plan shape MIR cannot express.
//!
//! # Sharing the generator with the fuzz targets
//!
//! The MIR generator and the fold oracle live in `mz_transform::mirgen` and are
//! shared with the `mz-transform` fuzz targets, which check MIR-to-MIR
//! equivalence under the optimizer over the same plan distribution. This module
//! adds only what rendering needs and fuzzing does not: persist-backed leaves
//! instead of literal constants, timestamped input batches with retractions, the
//! choice of exports, and the dyncfg matrix.

use std::collections::BTreeSet;

use mz_expr::{MirRelationExpr, visit::Visit};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Datum, GlobalId, Timestamp};
use mz_transform::mirgen::{self, SeededEntropy, Ty};

use crate::dataflow::{DataflowBuilder, PersistSource};
use crate::surface::{SurfaceCell, cells_of_plan};
use crate::workload::{
    Batch, ColumnTy, InputSpec, NamedConfig, Oracle, Update, Value, Workload, WorkloadExport, ids,
};

/// The corpus seed. Fixed, so a run is reproducible from the code alone: the
/// corpus is generated on demand rather than committed, and this is what makes
/// two runs of the same commit compare the same plans.
pub const DEFAULT_SEED: u64 = 0x5EED;

/// How many candidates to draw before giving up on finding new coverage.
pub const DEFAULT_MAX_DRAWS: usize = 6000;

/// How many consecutive draws may add nothing before generation stops.
///
/// Set generously: greedy set cover's tail is long, and a small patience stops
/// while cells are still being found. The loop is cheap (lowering only, no
/// rendering), so over-drawing costs little.
pub const DEFAULT_PATIENCE: usize = 1500;

/// The corpus a run executes: the targeted shapes plus set-covering draws, under
/// the pairwise strategy-flag matrix.
///
/// Generated rather than read from disk. A committed corpus has to be kept in step
/// with the generator by a lint, and the two drift the moment someone changes
/// generation without regenerating; deriving it from a fixed seed removes the
/// second source of truth entirely. Reproducibility is unaffected, and is pinned
/// by `generation_is_reproducible`.
pub fn default_corpus(seed: u64) -> anyhow::Result<Corpus> {
    let configs = pairwise_configs(STRATEGY_FLAGS);
    generate(seed, DEFAULT_MAX_DRAWS, DEFAULT_PATIENCE, &configs)
}

/// How deep a generated MIR plan may nest.
///
/// Matches what the fuzz targets use, so both consumers see the same plan
/// distribution and a plan that fails here is reproducible there.
const PLAN_DEPTH: u32 = 4;

/// The strategy dyncfgs the config matrix varies.
///
/// These are the compute-owned switches that change *how* a result is computed
/// without changing *what* it is, which is exactly the precondition for the
/// strategy-invariance oracle. Resource knobs (buffer sizes, intervals, memory
/// budgets) are deliberately excluded: they do not select a different algorithm,
/// so varying them buys coverage of nothing while multiplying runtime.
pub const STRATEGY_FLAGS: &[&str] = &[
    "enable_mz_join_core",
    "enable_compute_half_join2",
    "enable_column_paged_batcher",
    "enable_compute_correction_v2",
    "enable_compute_temporal_bucketing",
    "enable_arrangement_dictionary_compression_alpha",
    "compute_apply_column_demands",
    "enable_compute_sync_mv_sink",
];

/// A pairwise covering array over the strategy flags.
///
/// Every pair of flags takes every combination of values in at least one row.
/// Full enumeration would be `2^8` rows per workload, which multiplies the corpus
/// runtime by 256 to cover interactions that essentially never involve more than
/// two flags at once. Pairwise is the standard trade and keeps the matrix to a
/// handful of rows.
///
/// Built greedily rather than tabulated, so adding a flag to [`STRATEGY_FLAGS`]
/// extends the array instead of silently leaving the new flag untested in
/// combination.
pub fn pairwise_configs(flags: &[&str]) -> Vec<NamedConfig> {
    let n = flags.len();
    if n == 0 {
        return Vec::new();
    }

    // Every (flag_i, flag_j, value_i, value_j) pair that still needs covering.
    let mut needed: BTreeSet<(usize, usize, bool, bool)> = BTreeSet::new();
    for i in 0..n {
        for j in (i + 1)..n {
            for vi in [false, true] {
                for vj in [false, true] {
                    needed.insert((i, j, vi, vj));
                }
            }
        }
    }

    let mut rows: Vec<Vec<bool>> = Vec::new();
    // Candidate rows are drawn from a deterministic sweep rather than at random,
    // so the corpus is reproducible without carrying a seed for the matrix.
    let mut candidate_seed = 0u64;
    while !needed.is_empty() {
        // Pick the candidate row covering the most outstanding pairs.
        let mut best: Option<(usize, Vec<bool>)> = None;
        for _ in 0..256 {
            candidate_seed = candidate_seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1);
            let row: Vec<bool> = (0..n)
                .map(|k| (candidate_seed >> (k % 64)) & 1 == 1)
                .collect();
            let covered = needed
                .iter()
                .filter(|(i, j, vi, vj)| row[*i] == *vi && row[*j] == *vj)
                .count();
            if best.as_ref().is_none_or(|(b, _)| covered > *b) {
                best = Some((covered, row));
            }
        }
        // `needed` is non-empty, so some assignment covers at least one pair and
        // the loop always makes progress.
        let (covered, row) = best.expect("at least one candidate row");
        assert!(
            covered > 0,
            "a candidate row must cover an outstanding pair"
        );
        needed.retain(|(i, j, vi, vj)| !(row[*i] == *vi && row[*j] == *vj));
        rows.push(row);
    }

    rows.into_iter()
        .enumerate()
        .map(|(idx, row)| NamedConfig {
            name: format!("matrix-{idx}"),
            settings: flags
                .iter()
                .zip(&row)
                .map(|(flag, on)| crate::script::ConfigSetting {
                    name: (*flag).to_string(),
                    ty: "bool".to_string(),
                    value: on.to_string(),
                })
                .collect(),
        })
        .collect()
}

/// Whether `plan` must go through the MIR optimizer before it can be lowered.
///
/// Two shapes the LIR lowering will not accept as drawn, both because a transform
/// is expected to have normalized them first:
///
///  * A `Join` carries `JoinImplementation::Unimplemented` until
///    `JoinImplementation` fills it in, and the lowering rejects that.
///  * A `Reduce` mixing reduction types (say `min` and `sum`) hits an
///    `assert_eq!("Multiple reduction types detected")` inside
///    `ReducePlan::create_from`. `ReduceReduction` is what splits such a reduce
///    into one per type, joined back together, so lowering assumes it ran.
///
/// Deciding this from the plan rather than always optimizing matters: an
/// unoptimized lowering reaches LIR shapes the optimizer would rewrite away, and
/// those are exactly the render paths a corpus built only from optimized plans
/// would never touch.
///
/// NOTE: the mixed-reduce case is a panic, not an error, so it escapes
/// `DataflowBuilder::finish`'s contract of reporting malformed plans as errors.
/// Anything driving the builder from external input has to screen for it here
/// rather than catching it downstream.
fn needs_optimizer(plan: &MirRelationExpr) -> anyhow::Result<bool> {
    use mz_compute_types::plan::reduce::reduction_type;

    let mut needs = false;
    plan.visit_post(&mut |e| match e {
        MirRelationExpr::Join { .. } => needs = true,
        MirRelationExpr::Reduce { aggregates, .. } => {
            let mut types = aggregates.iter().map(|a| reduction_type(&a.func));
            if let Some(first) = types.next() {
                if types.any(|t| t != first) {
                    needs = true;
                }
            }
        }
        _ => {}
    });
    Ok(needs)
}

/// One input collection the generator built for a plan's leaves.
struct GeneratedInput {
    schema: Vec<Ty>,
    rows: Vec<Vec<Datum<'static>>>,
}

/// Draw one candidate workload from `entropy`.
///
/// Returns `None` when the drawn plan cannot be lowered. That is expected and not
/// an error: not every well-typed MIR shape has a rendering (a `Join` whose
/// implementation the optimizer declines to fill, for instance), and the caller
/// simply draws again.
fn draw_candidate(
    entropy: &mut SeededEntropy,
    seed: u64,
    configs: &[NamedConfig],
) -> anyhow::Result<Option<(Workload, BTreeSet<SurfaceCell>)>> {
    let mut inputs: Vec<GeneratedInput> = Vec::new();

    // The leaf closure is where this generator diverges from the fuzz targets:
    // theirs root plans at literal `Constant`s, this one roots them at `Get`s of
    // persist-backed sources, so the plan reads real data through the storage
    // import path.
    let plan_result = mirgen::gen_rel(entropy, PLAN_DEPTH, &mut |u| {
        let schema = mirgen::gen_schema(u)?;
        let rows = mirgen::gen_rows(u, &schema)?;
        let index = inputs.len();
        inputs.push(GeneratedInput {
            schema: schema.clone(),
            rows,
        });
        let typ = mirgen::nullable_relation_type(&schema);
        Ok((
            MirRelationExpr::global_get(GlobalId::User(ids::input(index)), typ),
            schema,
        ))
    });
    // `SeededEntropy` cannot fail, so a draw always succeeds.
    let (plan, _schema, non_negative) = plan_result.expect("seeded entropy never fails");

    // An exported collection must be non-negative; see the runner's contract.
    // `Threshold` is the operator that enforces it, and it is what the real
    // planner would insert for the same reason.
    let plan = if non_negative { plan } else { plan.threshold() };

    let needs_optimizer = needs_optimizer(&plan)?;

    let inputs: Vec<InputSpec> = inputs
        .iter()
        .map(|input| into_input_spec(input, seed, false))
        .collect::<anyhow::Result<_>>()?;

    // Export invariance and the incremental check apply to every plan; the fold
    // oracle is added below, only once the plan is known to fold.
    let mut oracles = vec![Oracle::ExportInvariance, Oracle::Incremental];
    if configs.len() > 1 {
        oracles.push(Oracle::StrategyInvariance);
    }

    let mut workload = Workload {
        name: format!("gen-{seed:08x}"),
        seed: Some(seed),
        inputs,
        plan,
        exports: vec![
            WorkloadExport::Index,
            WorkloadExport::MaterializedView,
            WorkloadExport::Subscribe,
        ],
        configs: configs.to_vec(),
        claims: BTreeSet::new(),
        oracles,
        optimize: needs_optimizer,
    };

    // Lower the workload exactly as the runner will, to learn which cells it
    // realizes. Dummy shard ids and an in-memory location suffice: lowering does
    // not read the data, it only needs the metadata slots filled.
    let Some(cells) = realized_cells(&workload)? else {
        return Ok(None);
    };
    workload.claims = cells.clone();

    // Request the fold oracle only when folding actually reaches a verdict.
    // Attaching it unconditionally would make it inert on the plans it cannot
    // reduce, and an inert oracle is indistinguishable from a passing one, so the
    // runner treats inertness as a failure. Deciding it here keeps that guard
    // meaningful rather than turning it into noise the corpus has to tolerate.
    let reference = workload.plan_with_constants(workload.assert_ts())?;
    match mz_transform::mirgen::fold_outcome(reference) {
        // Rows and errors are both verdicts the runner can check: it reads a
        // collection error as a value, so an erroring plan is required to produce
        // that same error rather than rows.
        mirgen::FoldOutcome::Rows(_) | mirgen::FoldOutcome::Error(_) => {
            workload.oracles.push(Oracle::FoldConstants)
        }
        mirgen::FoldOutcome::Unfoldable => {}
    }
    Ok(Some((workload, cells)))
}

/// The surface cells a workload's plan lowers to, or `None` if it does not lower.
///
/// Mirrors the runner's dataflow assembly so the claim recorded at generation time
/// is the one the runner will check. The two must stay in step: if this diverges
/// from `WorkloadRunner::build_dataflow`, every generated claim becomes wrong at
/// once, which the runner's claim check would then report as a failure on every
/// workload.
fn realized_cells(workload: &Workload) -> anyhow::Result<Option<BTreeSet<SurfaceCell>>> {
    let loc = PersistLocation {
        blob_uri: "mem://".parse()?,
        consensus_uri: "mem://".parse()?,
    };
    let mut cells = BTreeSet::new();
    // The runner renders one dataflow per export and unions their cells, so mirror
    // that here. An index export contributes an `ArrangeBy` that a sink export
    // does not, so building only one of them would record a claim the runner's
    // union does not match.
    for export in &workload.exports {
        let mut builder = DataflowBuilder::new(format!("workload-{}-{export:?}", workload.name));
        if workload.optimize {
            builder.optimize();
        }
        for (i, input) in workload.inputs.iter().enumerate() {
            builder.import_persist(
                GlobalId::User(ids::input(i)),
                PersistSource {
                    shard: ShardId::new(),
                    location: loc.clone(),
                    desc: input.relation_desc(),
                    upper: Timestamp::from(workload.upper()),
                },
            );
        }
        let plan_id = GlobalId::User(ids::plan(0, *export));
        let plan_type = workload.plan.typ();
        builder.build(plan_id, workload.plan.clone());
        let result_desc = desc_from_repr(&plan_type);
        match export {
            WorkloadExport::Index => {
                builder.export_index(GlobalId::User(ids::index(0)), plan_id, vec![]);
            }
            WorkloadExport::MaterializedView => {
                builder.export_materialized_view(
                    GlobalId::User(ids::mv_sink(0)),
                    plan_id,
                    result_desc,
                    crate::dataflow::PersistSink {
                        shard: ShardId::new(),
                        location: loc.clone(),
                    },
                );
            }
            WorkloadExport::Subscribe => {
                builder.export_subscribe(
                    GlobalId::User(ids::subscribe_sink(0)),
                    plan_id,
                    result_desc,
                    timely::progress::Antichain::from_elem(Timestamp::from(
                        workload.assert_ts() + 1,
                    )),
                );
            }
        }
        builder.as_of(Timestamp::from(workload.assert_ts()));
        match builder.finish() {
            Ok(df) => cells.extend(
                df.objects_to_build
                    .iter()
                    .flat_map(|o| cells_of_plan(&o.plan)),
            ),
            // A plan that does not lower is not a finding here, just a draw to skip.
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(cells))
}

/// A `RelationDesc` for a computed relation type, matching what the runner builds
/// for a sink's output schema.
fn desc_from_repr(typ: &mz_repr::ReprRelationType) -> mz_repr::RelationDesc {
    let sql = mz_repr::SqlRelationType::from_repr(typ);
    let mut builder = mz_repr::RelationDesc::builder();
    for (i, ct) in sql.column_types.iter().enumerate() {
        builder = builder.with_column(format!("c{i}"), ct.clone());
    }
    builder.finish()
}

/// How many timestamped batches an input's rows are spread across.
///
/// Two batches is the minimum that exercises incremental maintenance at all, and
/// it is not enough: with a single update step, an arrangement never compacts
/// mid-stream and the correction buffers never see more than one round. Four
/// gives the maintained collection somewhere to go wrong between the first update
/// and the assertion.
pub const BATCHES_PER_INPUT: usize = 4;

/// How many times each generated row is repeated across the batches.
///
/// The generator draws 0-4 distinct rows, which is the right size for finding
/// logic bugs and far too small for anything volume-sensitive: the spilling
/// batcher never spills, dictionary compression has nothing to compress, and the
/// peek stash never fills. Repeating rows raises the update count without
/// widening the value space, so results stay small enough to compare exactly
/// while the operators see real batches.
///
/// Deliberately modest. This is a correctness suite that runs a large
/// configuration matrix, so per-workload cost multiplies by 8; genuinely large
/// data belongs in a load-oriented test rather than here.
pub const ROW_REPEATS: usize = 24;

/// Turn generated rows into timestamped batches.
///
/// Rows are spread across [`BATCHES_PER_INPUT`] timestamps, each repeated
/// [`ROW_REPEATS`] times, and (unless `append_only`) a share of them is retracted
/// in later batches. Retractions are the point of the later batches: an
/// all-positive sequence never reaches the correction, consolidation, or
/// negative-diff paths, which is where incremental bugs live.
///
/// `append_only` exists for the monotonic shapes. A monotonic operator over a
/// retracting collection is incorrect, so declaring one requires insert-only
/// input; see [`crate::shapes`].
fn into_input_spec(
    input: &GeneratedInput,
    seed: u64,
    append_only: bool,
) -> anyhow::Result<InputSpec> {
    let schema: Vec<ColumnTy> = input.schema.iter().copied().map(ColumnTy::from).collect();

    let to_update = |row: &Vec<Datum<'static>>, diff: i64| -> anyhow::Result<Update> {
        Ok(Update {
            values: row
                .iter()
                .map(|d| Value::from_datum(*d))
                .collect::<anyhow::Result<_>>()?,
            diff,
        })
    };

    let mut batches = Vec::with_capacity(BATCHES_PER_INPUT);
    for batch in 0..BATCHES_PER_INPUT {
        let mut updates = Vec::new();
        for (i, row) in input.rows.iter().enumerate() {
            // Retract a rotating third of the rows in every batch after the
            // first. The rotation is derived from the seed so it varies across
            // the corpus while staying reproducible, and skipping the first batch
            // means there is always something present to retract.
            let retract = !append_only && batch > 0 && (seed as usize + i + batch) % 3 == 0;
            let diff = if retract { -1 } else { 1 };
            for _ in 0..ROW_REPEATS {
                updates.push(to_update(row, diff)?);
            }
        }
        batches.push(Batch { updates });
    }

    Ok(InputSpec { schema, batches })
}

/// Surface cells the corpus does not reach, as a matchable cell-name prefix and
/// the reason.
///
/// This is the honest half of the coverage report. A suite that reports only what
/// it covered reads exactly like one that covered everything, so the gaps are
/// enumerated with their causes.
///
/// The prefixes are matched against realized cell names by
/// `known_gaps_are_still_gaps`, which fails if any entry is actually covered. That
/// matters more than it looks: as `shapes` closes gaps, a hand-maintained list
/// silently becomes a list of lies, and a stale gap list is worse than none
/// because it argues against work already done.
pub const KNOWN_GAPS: &[(&str, &str)] = &[
    (
        "Get/ArrangementLookup",
        "needs literal constraints over an imported index key. The workload format \
         has no index imports: every input is a persist source, so no Get carries \
         a key to seek into",
    ),
    (
        "Mfp/Plain/Lookup",
        "same as Get/ArrangementLookup, no keyed input to seek into",
    ),
    (
        "Mfp/Temporal",
        "needs an mz_now() predicate. gen_scalar has no unmaterializable functions, \
         and adding one makes the result depend on wall-clock time, which breaks \
         the export-invariance and strategy-invariance oracles unless the workload \
         pins mz_now through the dataflow's `until`",
    ),
    (
        "Reduce/BasicSingle",
        "needs a non-accumulable, non-hierarchical aggregate. Every Basic aggregate \
         (jsonb_agg, string_agg, the window functions) takes jsonb, text, or a \
         record argument, and the workload format's column types are int4/int8/bool",
    ),
    (
        "Reduce/BasicMultiple",
        "as Reduce/BasicSingle: no Basic aggregate is expressible over the \
         supported column types",
    ),
    (
        "Reduce/MonotonicConsolidating",
        "the consolidating variant of a monotonic hierarchical reduce. Lowering \
         sets must_consolidate from its own analysis, and the monotonic shape does \
         not land on the branch that asks for it",
    ),
    (
        "TopK/MonotonicTopK/",
        "the unlimited monotonic Top-K. A TopK with no limit and a monotonic input \
         is not a shape SQL produces, since LIMIT is what creates a TopK",
    ),
    (
        "FlatMap/Arranged",
        "needs a table function reading an arrangement rather than a stream, which \
         requires an index import (see Get/ArrangementLookup)",
    ),
    (
        "FlatMap/Lookup",
        "as FlatMap/Arranged, plus a literal constraint to seek with",
    ),
    (
        "ArrangeBy/Several",
        "needs one collection carrying several arrangements at once. The multi-key \
         join shape asks for it, but the optimizer decides the arrangements and \
         currently plans that join without it",
    ),
    (
        "Bucketed",
        "every ArrangementStrategy::TemporalBucketing cell, across Reduce, TopK, \
         Union, and ArrangeBy. Lowering picks it only for a plan carrying \
         future-stamped updates, which means mz_now(); see Mfp/Temporal",
    ),
];

/// Whether `cell` is covered by one of the [`KNOWN_GAPS`] prefixes.
fn matches_gap(cell: &str, prefix: &str) -> bool {
    // `Bucketed` appears mid-name (`Reduce/Bucketed/...` is a different thing:
    // the bucketed *hierarchical* reduce, not temporal bucketing), so match the
    // strategy position rather than a bare substring.
    if prefix == "Bucketed" {
        return cell.contains("/Bucketed/") && !cell.starts_with("Reduce/Bucketed");
    }
    cell.starts_with(prefix)
}

/// The outcome of a corpus generation run.
#[derive(Debug)]
pub struct Corpus {
    /// The kept workloads, each covering at least one cell no earlier one covers.
    pub workloads: Vec<Workload>,
    /// Every cell the corpus covers.
    pub covered: BTreeSet<SurfaceCell>,
    /// How many candidates were drawn to get here, including the discarded ones.
    pub drawn: usize,
}

/// Build the workload for a targeted shape.
///
/// Mirrors `draw_candidate`'s assembly, but over a written-out plan and its
/// declared input schemas rather than a drawn one. The oracle selection is the
/// same logic: fold when the plan reaches a verdict, which a `LetRec` shape will
/// not, leaving it the export-invariance and incremental checks.
fn shape_workload(
    shape: &crate::shapes::Shape,
    configs: &[NamedConfig],
) -> anyhow::Result<Option<(Workload, BTreeSet<SurfaceCell>)>> {
    use crate::shapes::ShapeInputs;

    let append_only = shape.input_mode == ShapeInputs::AppendOnly;
    // Deterministic per shape, so the corpus is reproducible: seed the row values
    // off the shape's name rather than a counter, which would shift every shape's
    // data when one is added.
    let seed = shape.name.bytes().fold(0u64, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(u64::from(b))
    });

    let inputs: Vec<InputSpec> = shape
        .inputs
        .iter()
        .enumerate()
        .map(|(i, schema)| {
            let mut entropy = SeededEntropy::new(seed.wrapping_add(i as u64));
            let rows = mirgen::gen_rows(&mut entropy, schema).expect("seeded entropy never fails");
            let generated = GeneratedInput {
                schema: schema.clone(),
                rows,
            };
            into_input_spec(&generated, seed, append_only)
        })
        .collect::<anyhow::Result<_>>()?;

    let mut oracles = vec![Oracle::ExportInvariance, Oracle::Incremental];
    if configs.len() > 1 {
        oracles.push(Oracle::StrategyInvariance);
    }

    let mut workload = Workload {
        name: shape.name.to_string(),
        seed: None,
        inputs,
        plan: shape.plan.clone(),
        exports: vec![
            WorkloadExport::Index,
            WorkloadExport::MaterializedView,
            WorkloadExport::Subscribe,
        ],
        configs: configs.to_vec(),
        claims: BTreeSet::new(),
        oracles,
        optimize: shape.optimize,
    };

    let Some(cells) = realized_cells(&workload)? else {
        // A shape that does not lower is a defect in the shape, not a draw to
        // skip, so say so rather than dropping it silently.
        anyhow::bail!(
            "shape {:?} does not lower; it targets {} but cannot be rendered",
            shape.name,
            shape.targets
        );
    };
    workload.claims = cells.clone();

    let reference = workload.plan_with_constants(workload.assert_ts())?;
    match mz_transform::mirgen::fold_outcome(reference) {
        mirgen::FoldOutcome::Rows(_) | mirgen::FoldOutcome::Error(_) => {
            workload.oracles.push(Oracle::FoldConstants)
        }
        mirgen::FoldOutcome::Unfoldable => {}
    }
    Ok(Some((workload, cells)))
}

/// Generate a corpus by greedy set cover over the surface.
///
/// Draws up to `max_draws` candidates and keeps each one that covers a cell no
/// kept workload covers. Stops early once `patience` consecutive draws add
/// nothing, on the same reasoning as the loop-until-dry pattern: a fixed draw
/// count either stops before the tail or wastes the budget after it.
///
/// The result is intentionally *not* claimed to be complete. Which cells were
/// missed is reported in [`Corpus::covered`] against the caller's expectation,
/// because a coverage tool that cannot say what it missed is indistinguishable
/// from one that covered everything.
pub fn generate(
    start_seed: u64,
    max_draws: usize,
    patience: usize,
    configs: &[NamedConfig],
) -> anyhow::Result<Corpus> {
    let mut workloads = Vec::new();
    let mut covered: BTreeSet<SurfaceCell> = BTreeSet::new();
    let mut since_progress = 0usize;
    let mut drawn = 0usize;

    // The targeted shapes come first and are always kept. They exist precisely
    // because random draws do not reach their cells, so subjecting them to the
    // set-cover filter would be circular: whether they are kept must not depend
    // on what the draws happen to find.
    for shape in crate::shapes::all() {
        let (workload, cells) = shape_workload(&shape, configs)?
            .ok_or_else(|| anyhow::anyhow!("shape {:?} produced no workload", shape.name))?;
        covered.extend(cells);
        workloads.push(workload);
    }

    for i in 0..max_draws {
        let seed = start_seed.wrapping_add(u64::try_from(i).expect("draw index fits u64"));
        let mut entropy = SeededEntropy::new(seed);
        drawn += 1;
        let Some((workload, cells)) = draw_candidate(&mut entropy, seed, configs)? else {
            since_progress += 1;
            if since_progress >= patience {
                break;
            }
            continue;
        };
        if cells.is_subset(&covered) {
            since_progress += 1;
            if since_progress >= patience {
                break;
            }
            continue;
        }
        covered.extend(cells);
        workloads.push(workload);
        since_progress = 0;
    }

    Ok(Corpus {
        workloads,
        covered,
        drawn,
    })
}

/// A human-readable coverage report: what the corpus reached, and the documented
/// gaps it did not.
///
/// Printed by the corpus generator so the gaps travel with the corpus. Reporting
/// only the covered set would let coverage silently shrink, since a shorter list
/// reads the same as a complete one.
pub fn coverage_report(corpus: &Corpus) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "corpus: {} workloads kept from {} draws, covering {} surface cells\n\n",
        corpus.workloads.len(),
        corpus.drawn,
        corpus.covered.len()
    ));
    out.push_str("covered cells:\n");
    for cell in &corpus.covered {
        out.push_str(&format!("  {cell}\n"));
    }
    out.push_str("\nknown gaps (cells the corpus does not reach, with cause):\n");
    for (cell, why) in KNOWN_GAPS {
        // A gap the corpus actually covers would make this report misleading.
        // `known_gaps_are_still_gaps` fails the build on it; say so here too, so a
        // report generated from a stale list cannot be read as authoritative.
        let stale = corpus
            .covered
            .iter()
            .any(|c| matches_gap(&c.to_string(), cell));
        let mark = if stale { "  [STALE: now covered]" } else { "" };
        out.push_str(&format!("  {cell}{mark}\n      {why}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pairwise array covers every flag pair in both directions, and does so
    /// in far fewer rows than full enumeration.
    #[mz_ore::test]
    fn pairwise_array_covers_every_pair() {
        let configs = pairwise_configs(STRATEGY_FLAGS);
        let n = STRATEGY_FLAGS.len();

        // Recover the boolean rows from the emitted settings.
        let rows: Vec<Vec<bool>> = configs
            .iter()
            .map(|c| {
                c.settings
                    .iter()
                    .map(|s| s.value == "true")
                    .collect::<Vec<_>>()
            })
            .collect();
        for row in &rows {
            assert_eq!(row.len(), n, "each row assigns every flag");
        }

        for i in 0..n {
            for j in (i + 1)..n {
                for vi in [false, true] {
                    for vj in [false, true] {
                        assert!(
                            rows.iter().any(|r| r[i] == vi && r[j] == vj),
                            "pair ({}={vi}, {}={vj}) is not covered",
                            STRATEGY_FLAGS[i],
                            STRATEGY_FLAGS[j]
                        );
                    }
                }
            }
        }
        // Pairwise must be a real saving over the 256-row full enumeration,
        // otherwise the trade this function exists to make has not been made.
        assert!(
            rows.len() < 20,
            "pairwise array grew to {} rows, expected well under 2^{n}",
            rows.len()
        );
    }

    /// Generation is reproducible from its seed: the same seed yields the same
    /// corpus. This is what makes a committed corpus regenerable and a failure
    /// replayable from the seed alone.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn generation_is_reproducible() {
        let a = generate(42, 40, 40, &[]).expect("generate");
        let b = generate(42, 40, 40, &[]).expect("generate");
        assert_eq!(a.workloads.len(), b.workloads.len());
        assert_eq!(a.covered, b.covered);
        for (x, y) in a.workloads.iter().zip(&b.workloads) {
            assert_eq!(x, y, "same seed must yield the same workload");
        }
    }

    /// Greedy set cover keeps only workloads that add coverage, and reaches a
    /// non-trivial slice of the surface. The floor is deliberately loose: this
    /// asserts the generator works at all, not what the corpus achieves, which
    /// the coverage report measures.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn generation_covers_and_dedupes() {
        let corpus = generate(1, 400, 120, &[]).expect("generate");
        assert!(
            corpus.covered.len() >= 10,
            "expected a non-trivial slice of the surface, got {}:\n{}",
            corpus.covered.len(),
            crate::surface::render_cells(&corpus.covered)
        );
        assert!(
            corpus.workloads.len() <= corpus.covered.len(),
            "set cover must not keep more workloads than the cells they cover"
        );
        // Every kept workload's claim must be exactly what it lowers to, since
        // the runner will hold it to that.
        for workload in &corpus.workloads {
            let cells = realized_cells(workload)
                .expect("lowering a kept workload")
                .expect("a kept workload lowers");
            assert_eq!(workload.claims, cells, "claim must match the lowered plan");
        }
    }
}

#[cfg(test)]
mod corpus_tests {
    use super::*;

    /// The corpus a run will execute is self-consistent: every workload's claimed
    /// cells match what its plan lowers to, and its oracle selection matches what
    /// those oracles can actually check.
    ///
    /// The corpus is generated on demand rather than committed, so this checks the
    /// thing a run will really use. The oracle check is the point: a workload
    /// requesting the fold oracle over a plan that does not fold would make that
    /// oracle inert, and the runner turns inertness into a failure, so catching it
    /// here is both cheaper and clearer than at run time.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn default_corpus_is_self_consistent() {
        let corpus = default_corpus(DEFAULT_SEED).expect("generate");
        assert!(
            !corpus.workloads.is_empty(),
            "the corpus is empty; a run over it would check nothing"
        );

        for workload in &corpus.workloads {
            let cells = realized_cells(workload)
                .expect("lowering")
                .unwrap_or_else(|| panic!("{} does not lower", workload.name));
            assert_eq!(
                workload.claims, cells,
                "{}: claimed cells do not match the lowered plan",
                workload.name
            );

            if workload.oracles.contains(&Oracle::FoldConstants) {
                let reference = workload
                    .plan_with_constants(workload.assert_ts())
                    .expect("substituting constants");
                assert!(
                    !matches!(
                        mz_transform::mirgen::fold_outcome(reference),
                        mz_transform::mirgen::FoldOutcome::Unfoldable
                    ),
                    "{}: requests the fold oracle but its plan does not fold to \
                     rows or an error, so the oracle would be inert",
                    workload.name
                );
            }

            if workload.oracles.contains(&Oracle::StrategyInvariance) {
                assert!(
                    workload.configs.len() >= 2,
                    "{}: requests strategy invariance with {} config(s)",
                    workload.name,
                    workload.configs.len()
                );
            }
        }
    }

    /// Every workload survives a JSON round trip.
    ///
    /// Nothing in a run serializes a workload any more, but the format is still
    /// how `gen-workloads` dumps one for inspection and how a hand-written repro
    /// is fed back in. A type that stops round-tripping would break both, quietly,
    /// at the moment somebody needs them most.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn workloads_round_trip_through_json() {
        let corpus = default_corpus(DEFAULT_SEED).expect("generate");
        for workload in &corpus.workloads {
            let json = serde_json::to_string(workload).expect("serialize");
            let back: Workload = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(&back, workload, "{} did not round trip", workload.name);
        }
    }

    /// No `KNOWN_GAPS` entry may name a cell the corpus actually covers.
    ///
    /// The gap list is the suite's own account of what it does not test, and it
    /// is hand-maintained. As `shapes` closes gaps, an unrevised entry turns the
    /// coverage report into a list of lies, arguing against work already done.
    /// Failing here is what keeps the report worth reading.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn known_gaps_are_still_gaps() {
        let corpus = default_corpus(DEFAULT_SEED).expect("generate");
        let covered: Vec<String> = corpus.covered.iter().map(|c| c.to_string()).collect();

        let mut stale = Vec::new();
        for (prefix, _) in KNOWN_GAPS {
            let hits: Vec<&String> = covered.iter().filter(|c| matches_gap(c, prefix)).collect();
            if !hits.is_empty() {
                stale.push(format!("{prefix} is covered by {hits:?}"));
            }
        }
        assert!(
            stale.is_empty(),
            "KNOWN_GAPS lists cells the corpus now covers; remove them:\n{}",
            stale.join("\n")
        );
    }
}
