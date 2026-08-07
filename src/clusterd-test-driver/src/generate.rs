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

use itertools::Itertools;
use mz_expr::{MirRelationExpr, MirScalarExpr, visit::Visit};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Datum, Row};
use mz_transform::mirgen::{self, SeededEntropy, Ty};

use crate::surface::{SurfaceCell, cells_of_plan};
use crate::workload::{
    Batch, ColumnTy, InputSpec, NamedConfig, Oracle, Update, Value, Workload, WorkloadExport,
    WorkloadShards,
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

/// A soak corpus: `count` consecutive draws, every one kept, at the replica's
/// default configuration.
///
/// [`default_corpus`] selects by greedy set cover, which is what makes it a fast
/// deterministic regression suite: one workload per new surface cell, and the same
/// workloads on every run. That is also its ceiling as a bug finder. A rendering
/// bug is a cell interacting with particular data, not a cell being present, so
/// once the set-cover corpus is green at a fixed seed it stays green and stops
/// telling anybody anything.
///
/// This mode makes the other trade: keep every draw, spend the run's budget on
/// plan variety instead of on the configuration matrix, and take a fresh seed each
/// run. The shapes come along because they cover what random MIR cannot reach.
/// Failures stay replayable: each workload carries the seed it was drawn from.
///
/// Each workload still gets two configurations rather than one, drawn from the same
/// pairwise matrix, so strategy invariance keeps working and the flags are checked
/// against a different plan every time instead of against the same 27. Two rows
/// costs a quarter of the eight the regression corpus runs, which is what pays for
/// the plan count.
pub fn soak_corpus(seed: u64, count: usize) -> anyhow::Result<Corpus> {
    let matrix = pairwise_configs(STRATEGY_FLAGS);
    // A pair of matrix rows, chosen by `index`. Successive workloads get different
    // pairs, so a soak of any length walks the whole matrix.
    let configs_for = |index: usize| -> Vec<NamedConfig> {
        if matrix.len() < 2 {
            return matrix.clone();
        }
        let first = index % matrix.len();
        // The stride only has to differ from `first` and vary as `index` does.
        // Nothing about it is otherwise significant.
        let second = (index * 3 + 1) % matrix.len();
        if first == second {
            return vec![matrix[first].clone()];
        }
        vec![matrix[first].clone(), matrix[second].clone()]
    };

    let mut workloads = Vec::new();
    let mut covered = BTreeSet::new();
    for (i, shape) in crate::shapes::all().iter().enumerate() {
        let (workload, cells) = shape_workload(shape, &configs_for(i))?;
        covered.extend(cells);
        workloads.push(workload);
    }
    let mut drawn = 0;
    for i in 0..count {
        let seed = seed.wrapping_add(u64::try_from(i).expect("draw index fits u64"));
        let mut entropy = SeededEntropy::new(seed);
        drawn += 1;
        // A plan that does not lower, or that computes nothing, is skipped exactly as
        // in set-cover mode.
        if let Some((workload, cells)) =
            draw_candidate(&mut entropy, seed, &configs_for(workloads.len()))?
        {
            if is_live(&workload) {
                covered.extend(cells);
                workloads.push(workload);
            }
        }
    }
    Ok(Corpus {
        workloads,
        covered,
        drawn,
    })
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
                .zip_eq(&row)
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
fn needs_optimizer(plan: &MirRelationExpr) -> bool {
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
    needs
}

/// The values generated data and plan literals are folded into.
///
/// `gen_datum` draws across the full width of each integer type, and two such
/// draws never coincide. Left alone, an equi-join between two inputs finds no
/// partners, a group key never groups two rows together, and a `col = literal`
/// predicate is never true. The operators still appear in the plan, so the surface
/// looks covered, while the collections flowing through them cannot exercise
/// anything: the oracles compare empty against empty and agree. Measured on the
/// corpus this replaces, every join workload but one computed an empty result.
///
/// Folding every value into a handful of candidates is what makes keys meet.
/// `0..=4` are fixed points, so a small integer keeps the meaning it was drawn with
/// (a `TopK` limit stays that limit), and the last entry keeps arithmetic overflow,
/// hence error propagation, reachable, which a purely small domain would lose.
const DOMAIN: [i64; 6] = [0, 1, 2, 3, 4, i64::MAX];

/// [`DOMAIN`] for `int4`. Spelled separately so the extreme is the *type's*
/// extreme: `i64::MAX` narrowed to `int4` is an error, not a large `int4`.
const DOMAIN_I32: [i32; 6] = [0, 1, 2, 3, 4, i32::MAX];

/// Fold an `int8` into [`DOMAIN`].
fn narrow_i64(v: i64) -> i64 {
    DOMAIN[usize::try_from(v.rem_euclid(6)).expect("rem_euclid is in 0..6")]
}

/// Fold an `int4` into [`DOMAIN_I32`].
fn narrow_i32(v: i32) -> i32 {
    DOMAIN_I32[usize::try_from(v.rem_euclid(6)).expect("rem_euclid is in 0..6")]
}

/// Fold every integer literal in `plan` into the value domain.
///
/// The data and the literals have to be narrowed together. Narrowing only the data
/// would leave `col = 7823...` unsatisfiable, which keeps the filters that guard
/// the interesting operators permanently false.
fn narrow_literals(plan: &mut MirRelationExpr) {
    plan.visit_scalars_mut(&mut |top| {
        // `visit_scalars_mut` hands over each scalar a relation node owns, not each
        // subexpression of it, so recurse.
        top.visit_mut_post(&mut |e| {
            let MirScalarExpr::Literal(Ok(row), _) = e else {
                return;
            };
            let narrowed = match row.unpack_first() {
                Datum::Int32(v) => Datum::Int32(narrow_i32(v)),
                Datum::Int64(v) => Datum::Int64(narrow_i64(v)),
                // Errors, nulls and booleans are already in a small domain.
                _ => return,
            };
            *row = Row::pack_slice(&[narrowed]);
        });
    });
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
        // Redraw an empty leaf. `gen_rows` draws `0..=4` rows, so a fifth of all
        // leaves come out empty, and an empty input annihilates everything above
        // it: a join emits nothing, a reduce sees no groups. Those workloads pass
        // by computing nothing on both sides. The empty case still gets tested,
        // deliberately, by `shape-empty-join`.
        let mut rows = mirgen::gen_rows(u, &schema)?;
        while rows.is_empty() {
            rows = mirgen::gen_rows(u, &schema)?;
        }
        let index = inputs.len();
        inputs.push(GeneratedInput {
            schema: schema.clone(),
            rows,
        });
        let typ = mirgen::nullable_relation_type(&schema);
        Ok((
            MirRelationExpr::global_get(
                mz_repr::GlobalId::User(crate::workload::ids::input(index)),
                typ,
            ),
            schema,
        ))
    });
    // `SeededEntropy` cannot fail, so a draw always succeeds.
    let (mut plan, _schema, non_negative) = plan_result.expect("seeded entropy never fails");

    narrow_literals(&mut plan);
    for input in &mut inputs {
        for row in &mut input.rows {
            for datum in row.iter_mut() {
                *datum = match *datum {
                    Datum::Int32(v) => Datum::Int32(narrow_i32(v)),
                    Datum::Int64(v) => Datum::Int64(narrow_i64(v)),
                    other => other,
                };
            }
        }
    }

    // `Threshold(Negate(x))` holds nothing whatever `x` holds, so a draw that ends
    // in a negation exports an empty collection and the oracles agree about
    // nothing. Peel the negations rather than discarding the draw: `Negate` is still
    // covered from the union and join arms, where a negated input contributes to a
    // result that survives the threshold.
    let mut plan = plan;
    while let MirRelationExpr::Negate { input } = plan {
        plan = *input;
    }

    // An exported collection must be non-negative; see the runner's contract.
    // `Threshold` is the operator that enforces it, and it is what the real planner
    // would insert for the same reason.
    let plan = if non_negative { plan } else { plan.threshold() };

    let inputs: Vec<InputSpec> = inputs
        .iter()
        .map(|input| into_input_spec(input, seed, false))
        .collect::<anyhow::Result<_>>()?;

    assemble_workload(
        format!("gen-{seed:08x}"),
        Some(seed),
        inputs,
        // Drawn plans are judged on shape, not size. Volume belongs to the shapes
        // written for it, where the result is kept small enough to read back.
        0,
        plan,
        configs,
        // A draw that does not lower is expected, not a defect: not every
        // well-typed MIR shape has a rendering. The caller draws again.
        false,
    )
}

/// Finish a workload: attach the exports, the configuration matrix, the oracles it
/// can actually answer, and the surface cells it realizes.
///
/// Shared by the random draws and the targeted shapes, which differ only in where
/// the plan and the input data come from. Two copies of this diverged on oracle
/// selection, which is the part that must not differ: an oracle attached to a plan
/// it cannot judge is inert, and the runner treats inertness as a failure.
///
/// `strict` decides what a plan that does not lower means. A shape that will not
/// lower is a defect in the shape and says so. A draw that will not lower is
/// skipped.
fn assemble_workload(
    name: String,
    seed: Option<u64>,
    inputs: Vec<InputSpec>,
    volume: usize,
    plan: MirRelationExpr,
    configs: &[NamedConfig],
    strict: bool,
) -> anyhow::Result<Option<(Workload, BTreeSet<SurfaceCell>)>> {
    // Every differential oracle applies to every plan: they compare render paths
    // against each other, so none of them needs the plan to have any particular
    // property.
    let mut oracles = vec![
        Oracle::ExportInvariance,
        Oracle::Incremental,
        Oracle::Reconciliation,
    ];
    if configs.len() > 1 {
        oracles.push(Oracle::StrategyInvariance);
    }

    let optimize = needs_optimizer(&plan);
    let mut workload = Workload {
        name,
        seed,
        inputs,
        volume,
        plan,
        exports: vec![
            WorkloadExport::Index,
            WorkloadExport::MaterializedView,
            WorkloadExport::Subscribe,
        ],
        configs: configs.to_vec(),
        claims: BTreeSet::new(),
        oracles,
        optimize,
    };

    // Lower the workload exactly as the runner will, to learn which cells it
    // realizes.
    let cells = match realized_cells(&workload) {
        Ok(Some(cells)) => cells,
        Ok(None) | Err(_) if !strict => return Ok(None),
        Ok(None) => anyhow::bail!("{} does not lower", workload.name),
        Err(e) => return Err(e),
    };
    workload.claims = cells.clone();

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
    // Dummy shard ids and an in-memory location suffice: lowering does not read the
    // data, it only needs the metadata slots filled.
    let location = PersistLocation {
        blob_uri: "mem://".parse()?,
        consensus_uri: "mem://".parse()?,
    };
    let input_shards: Vec<ShardId> = workload.inputs.iter().map(|_| ShardId::new()).collect();
    let shards = WorkloadShards {
        location: &location,
        inputs: &input_shards,
        sink: ShardId::new(),
    };

    let mut cells = BTreeSet::new();
    // The runner renders one dataflow per export and unions their cells, so mirror
    // that here. An index export contributes an `ArrangeBy` that a sink export does
    // not, so building only one of them would record a claim the runner's union
    // does not match.
    for export in &workload.exports {
        match workload.export_dataflow(0, *export, workload.assert_ts(), &shards) {
            Ok((df, _)) => cells.extend(
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

/// How many timestamped batches an input's rows are spread across.
///
/// Two batches is the minimum that exercises incremental maintenance at all, and
/// it is not enough: with a single update step, an arrangement never compacts
/// mid-stream and the correction buffers never see more than one round. Four
/// gives the maintained collection somewhere to go wrong between the first update
/// and the assertion.
pub const BATCHES_PER_INPUT: usize = 4;

/// The multiplicity each row is inserted and retracted at.
///
/// Above one so the diff arithmetic is not all `±1`: an accumulable reduce, a
/// `count`, and the consolidation paths all treat a multiplicity of three
/// differently from a multiplicity of one. Small, because a peek expands
/// multiplicities into repeated rows and a failure message has to stay readable.
///
/// It deliberately does not try to create volume. Identical rows at one timestamp
/// are one logical update by the time anything downstream sees them, so repeating a
/// row cannot make the spilling batcher spill or give dictionary compression
/// something to compress. Genuine volume belongs in a load-oriented test.
pub const ROW_REPEATS: i64 = 3;

/// Turn generated rows into timestamped batches.
///
/// Batch 0 inserts every row. Later batches delete a rotating share of what is
/// present and reinsert what is absent, so across [`BATCHES_PER_INPUT`] timestamps
/// rows genuinely leave the collection and some come back.
///
/// Reaching multiplicity zero is the point. A schedule that only ever lowers a
/// multiplicity (insert three, retract one) exercises diff arithmetic but never the
/// case that matters most, which is a row disappearing: consolidation dropping a
/// key, a reduce losing its last row for a group, a correction buffer cancelling to
/// nothing. Those are where incremental bugs live.
///
/// `append_only` exists for the monotonic shapes. A monotonic operator over a
/// retracting collection is incorrect, so declaring one requires insert-only input;
/// see [`crate::shapes`].
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

    // Which rows the collection currently holds, so a retraction only ever removes
    // something that is there and the collection stays non-negative.
    let mut present = vec![false; input.rows.len()];
    let mut batches = Vec::with_capacity(BATCHES_PER_INPUT);
    for batch in 0..BATCHES_PER_INPUT {
        let mut updates = Vec::new();
        for (i, row) in input.rows.iter().enumerate() {
            // The rotation is derived from the seed so it varies across the corpus
            // while staying reproducible.
            let phase =
                (usize::try_from(seed % 3).expect("seed % 3 fits usize") + i * 2 + batch) % 3;
            let diff = match () {
                // Batch 0 populates the collection.
                _ if batch == 0 => 1,
                // A monotonic input only ever grows.
                _ if append_only => 1,
                _ if present[i] && phase == 0 => -1,
                _ if !present[i] => 1,
                _ => continue,
            };
            present[i] = diff > 0;
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
        "Mfp/Plain/Lookup",
        "a leftover MFP that seeks an arrangement. `shape-arrangement-lookup` \
         reaches the equivalent `Get` cell, but there the filter fuses into the \
         Get rather than surviving as its own Mfp node, and a plan that forces the \
         split has not been found",
    ),
    (
        "Mfp/Temporal",
        "needs an mz_now() predicate. gen_scalar has no unmaterializable functions, \
         and adding one makes the result depend on wall-clock time, which breaks \
         the export-invariance and strategy-invariance oracles unless the workload \
         pins mz_now through the dataflow's `until`",
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
        "a table function reading an arrangement rather than a stream. Putting a \
         FlatMap over a Let-bound ArrangeBy does not do it: lowering still hands \
         the table function the raw collection, so the arrangement goes to the Get \
         and the FlatMap stays streamed",
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

/// Whether a candidate workload would test anything at all.
///
/// Every oracle here compares a rendered result against a reference, and two empty
/// collections agree. A plan that computes nothing therefore passes every oracle
/// while exercising none of them, and the surface-cell count reports its operators
/// as covered either way. Draws are cheap and plentiful, so a candidate that cannot
/// compute anything is skipped rather than kept and reported.
///
/// Two ways to compute nothing, and both are common in drawn MIR:
///
///  * The result is empty at every asserted timestamp, for instance a `TopK` whose
///    drawn limit is `0`.
///  * A `Join`'s inputs never match, so the join emits nothing however much data
///    flows into it. This is checked on the join subtree rather than on the plan's
///    output, because an operator above the join may legitimately eliminate rows;
///    what matters is that the join itself had partners to find.
///
/// A plan the folder cannot reduce (`LetRec`) gets no verdict, so it is judged live
/// on its inputs instead and left to the export-invariance and incremental oracles.
///
/// A folded *error* usually counts as computing something, since the renderer is
/// required to produce that same error. The exception is an error the folder hoists
/// out of a collection with no rows: `FoldConstants` propagates a literal error
/// through the whole relation however many rows it would have been evaluated on,
/// while the renderer evaluates per row and so produces an empty collection and no
/// error at all. Both are right, the comparison reaches no verdict, and the
/// workload spends a run saying nothing. [`without_error_literals`] is what tells
/// the two cases apart.
fn is_live(workload: &Workload) -> bool {
    use mz_transform::mirgen::FoldOutcome;

    // A volume workload's rows are synthesized at setup, so folding its declared
    // inputs sees an empty collection and would call it dead. Its size is the whole
    // point of it, and `InputSpec::volume_updates` guarantees the rows exist.
    if workload.volume > 0 {
        return true;
    }

    let mut any_rows = false;
    let mut foldable = false;
    // Only a join the folder can evaluate can be judged here. The optimizer
    // introduces joins of its own (`ReduceReduction` splits a mixed reduce and joins
    // the parts back together) and a join inside a `LetRec` reads a recursive
    // binding, and neither has a subtree that folds.
    let mut join_foldable = false;
    let mut join_matched = false;

    for ts in workload.timestamps() {
        let Ok(plan) = workload.plan_with_constants(ts) else {
            return false;
        };
        match mz_transform::mirgen::fold_outcome(plan.clone()) {
            FoldOutcome::Rows(m) => {
                foldable = true;
                any_rows |= !m.is_empty();
            }
            // An error is a verdict the oracles check, so it counts as computing
            // something, unless it is one the renderer will not raise because the
            // collection has no rows to raise it on.
            FoldOutcome::Error(_) => {
                foldable = true;
                any_rows |= !matches!(
                    mz_transform::mirgen::fold_outcome(without_error_literals(&plan)),
                    FoldOutcome::Rows(m) if m.is_empty()
                );
            }
            FoldOutcome::Unfoldable => {}
        }

        let mut joins = Vec::new();
        plan.visit_post(&mut |e| {
            if matches!(e, MirRelationExpr::Join { .. }) {
                joins.push(e.clone());
            }
        });
        for join in joins {
            match mz_transform::mirgen::fold_outcome(join) {
                FoldOutcome::Rows(m) => {
                    join_foldable = true;
                    join_matched |= !m.is_empty();
                }
                FoldOutcome::Error(_) => {
                    join_foldable = true;
                    join_matched = true;
                }
                FoldOutcome::Unfoldable => {}
            }
        }
    }

    if join_foldable && !join_matched {
        return false;
    }
    if !foldable {
        // Judged on its inputs: nothing can decide its output offline.
        return workload
            .inputs
            .iter()
            .flat_map(|i| &i.batches)
            .any(|b| !b.updates.is_empty());
    }
    any_rows
}

/// `plan` with every literal error replaced by a literal value of the same type.
///
/// Used to answer one question for [`is_live`]: does this plan have any rows at
/// all, setting its errors aside? A plan carrying a poison literal always folds to
/// an error, which hides whether the collection underneath it was empty.
///
/// A heuristic, and only ever used to decide whether to keep a draw. Substituting a
/// value can change which rows survive a filter, so it answers "are there rows"
/// approximately. Getting it wrong keeps or drops one candidate.
fn without_error_literals(plan: &MirRelationExpr) -> MirRelationExpr {
    use mz_repr::ReprScalarType;

    let mut plan = plan.clone();
    plan.visit_scalars_mut(&mut |top| {
        top.visit_mut_post(&mut |e| {
            let MirScalarExpr::Literal(lit @ Err(_), typ) = e else {
                return;
            };
            let datum = match typ.scalar_type {
                ReprScalarType::Int32 => Datum::Int32(0),
                ReprScalarType::Int64 => Datum::Int64(0),
                ReprScalarType::Bool => Datum::False,
                // Nothing else is generated. A null is valid for any type, and a
                // wrong guess only mis-keeps one candidate.
                _ => Datum::Null,
            };
            *lit = Ok(Row::pack_slice(&[datum]));
        });
    });
    plan
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
/// The plan and the input data are written out rather than drawn. Everything past
/// that is [`assemble_workload`], so a shape gets the same oracle selection and the
/// same claim check as a draw.
fn shape_workload(
    shape: &crate::shapes::Shape,
    configs: &[NamedConfig],
) -> anyhow::Result<(Workload, BTreeSet<SurfaceCell>)> {
    use crate::shapes::ShapeData;

    // Deterministic per shape, so the corpus is reproducible: seed the row values
    // off the shape's name rather than a counter, which would shift every shape's
    // data when one is added.
    let seed = shape.name.bytes().fold(0u64, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(u64::from(b))
    });

    // A volume shape declares no data of its own: the runner synthesizes it at
    // setup, so only the schemas and the row count travel.
    let volume = match shape.data {
        ShapeData::Volume { rows } => rows,
        _ => 0,
    };
    let inputs: Vec<InputSpec> = match &shape.data {
        ShapeData::Volume { .. } => shape
            .inputs
            .iter()
            .map(|schema| InputSpec {
                schema: schema.iter().copied().map(ColumnTy::from).collect(),
                batches: Vec::new(),
            })
            .collect(),
        ShapeData::Explicit(per_input) => {
            anyhow::ensure!(
                per_input.len() == shape.inputs.len(),
                "shape {:?} declares {} input schema(s) but {} batch list(s)",
                shape.name,
                shape.inputs.len(),
                per_input.len()
            );
            shape
                .inputs
                .iter()
                .zip_eq(per_input)
                .map(|(schema, batches)| InputSpec {
                    schema: schema.iter().copied().map(ColumnTy::from).collect(),
                    batches: batches.clone(),
                })
                .collect()
        }
        mode => shape
            .inputs
            .iter()
            .enumerate()
            .map(|(i, schema)| {
                let mut entropy =
                    SeededEntropy::new(seed.wrapping_add(u64::try_from(i).expect("input index")));
                let mut rows =
                    mirgen::gen_rows(&mut entropy, schema).expect("seeded entropy never fails");
                while rows.is_empty() {
                    rows =
                        mirgen::gen_rows(&mut entropy, schema).expect("seeded entropy never fails");
                }
                for row in &mut rows {
                    for datum in row.iter_mut() {
                        *datum = match *datum {
                            Datum::Int32(v) => Datum::Int32(narrow_i32(v)),
                            Datum::Int64(v) => Datum::Int64(narrow_i64(v)),
                            other => other,
                        };
                    }
                }
                let generated = GeneratedInput {
                    schema: schema.clone(),
                    rows,
                };
                into_input_spec(&generated, seed, *mode == ShapeData::GeneratedAppendOnly)
            })
            .collect::<anyhow::Result<_>>()?,
    };

    let plan = shape.plan.clone();
    if shape.optimize {
        // A shape asking for the optimizer needs it for lowering (a `Join`'s
        // implementation), and `assemble_workload` decides that from the plan. Keep
        // the shape's declaration honest rather than silently ignoring it.
        anyhow::ensure!(
            needs_optimizer(&plan),
            "shape {:?} sets optimize but its plan does not need the optimizer",
            shape.name
        );
    }
    // A shape's literals are deliberate, so they are left alone. Narrowing them
    // would rewrite a `generate_series` bound or a comparison the shape was built
    // around. Only its *data* is narrowed, above, so a filter written against a
    // small literal can match.

    assemble_workload(
        shape.name.to_string(),
        None,
        inputs,
        volume,
        plan,
        configs,
        // A shape that does not lower is a defect in the shape, not a draw to skip.
        true,
    )?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "shape {:?} produced no workload; it targets {}",
            shape.name,
            shape.targets
        )
    })
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
        let (workload, cells) = shape_workload(&shape, configs)?;
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
        if !is_live(&workload) || cells.is_subset(&covered) {
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
        for (x, y) in a.workloads.iter().zip_eq(&b.workloads) {
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
    /// thing a run will really use. The oracle check is the point: strategy
    /// invariance over a single configuration compares nothing, and catching that
    /// here is cheaper and clearer than at run time.
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

    /// Every flag the configuration matrix varies must still exist.
    ///
    /// `ConfigUpdates::apply` skips names the target `ConfigSet` does not know, and
    /// logs from inside the replica rather than failing. A renamed or retired flag
    /// would therefore leave the whole matrix in place while every row set nothing:
    /// eight identical runs, strategy invariance passing vacuously, and no failure
    /// anywhere. This is the check that turns that into a build break.
    #[mz_ore::test]
    fn strategy_flags_still_exist() {
        let set = mz_compute_types::dyncfgs::all_dyncfgs(mz_dyncfg::ConfigSet::default());
        let missing: Vec<&str> = STRATEGY_FLAGS
            .iter()
            .copied()
            .filter(|name| set.entry(name).is_none())
            .collect();
        assert!(
            missing.is_empty(),
            "STRATEGY_FLAGS names no longer in the compute dyncfg set: {missing:?}. \
             A run would silently apply nothing for these."
        );
    }

    /// The corpus computes something.
    ///
    /// Every oracle here compares a rendered result against a reference, and two
    /// empty collections agree. A corpus whose plans mostly produce nothing
    /// therefore passes while testing almost nothing, and the surface-cell count
    /// reports it as covered either way. That is exactly what the full-width value
    /// domain used to produce: a fifth of all inputs were empty, and every join
    /// workload but one folded to `<empty>`.
    ///
    /// So generation filters candidates through [`is_live`], and this pins that the
    /// corpus a run executes really came out that way. Only the shapes are exempt,
    /// and only one of them is deliberately empty.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn default_corpus_is_not_vacuous() {
        let corpus = default_corpus(DEFAULT_SEED).expect("generate");
        let dead: Vec<&String> = corpus
            .workloads
            .iter()
            // `shape-empty-join` owns the empty case on purpose.
            .filter(|w| w.name != "shape-empty-join" && !is_live(w))
            .map(|w| &w.name)
            .collect();
        assert!(
            dead.is_empty(),
            "these workloads compare empty against empty, so they test nothing: {dead:?}"
        );
    }

    /// An input a plan reads is never empty unless a shape asked for it.
    ///
    /// The complement of the check above, on the input side: a leaf that draws zero
    /// rows annihilates every operator above it, and `gen_rows` draws `0..=4`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn drawn_inputs_are_never_empty() {
        let corpus = default_corpus(DEFAULT_SEED).expect("generate");
        for workload in &corpus.workloads {
            if workload.seed.is_none() {
                // A shape controls its own data, including deliberately empty data.
                continue;
            }
            for (i, input) in workload.inputs.iter().enumerate() {
                let updates: usize = input.batches.iter().map(|b| b.updates.len()).sum();
                assert!(
                    updates > 0,
                    "{}: input {i} is empty, so everything above it computes nothing",
                    workload.name
                );
            }
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
