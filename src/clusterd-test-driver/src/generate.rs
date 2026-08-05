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
        .map(|input| into_input_spec(input, seed))
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
        let plan_id = GlobalId::User(ids::plan(*export));
        let plan_type = workload.plan.typ();
        builder.build(plan_id, workload.plan.clone());
        let result_desc = desc_from_repr(&plan_type);
        match export {
            WorkloadExport::Index => {
                builder.export_index(GlobalId::User(ids::INDEX), plan_id, vec![]);
            }
            WorkloadExport::MaterializedView => {
                builder.export_materialized_view(
                    GlobalId::User(ids::MV_SINK),
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
                    GlobalId::User(ids::SUBSCRIBE_SINK),
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

/// Turn generated rows into timestamped batches, with retractions.
///
/// The rows are split across two timestamps and a fraction of the first batch is
/// retracted in the second. Retractions are the point: a single all-positive batch
/// never reaches the correction, consolidation, or negative-diff paths, which is
/// where the incremental bugs live. The split is derived from `seed` so it varies
/// across the corpus while staying reproducible.
fn into_input_spec(input: &GeneratedInput, seed: u64) -> anyhow::Result<InputSpec> {
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

    // Insert everything at time 0.
    let first: Vec<Update> = input
        .rows
        .iter()
        .map(|r| to_update(r, 1))
        .collect::<anyhow::Result<_>>()?;

    // At time 1, retract every k-th row and re-insert the rest with an extra
    // copy, so the second batch carries both signs.
    let mut second = Vec::new();
    for (i, row) in input.rows.iter().enumerate() {
        let retract = (seed as usize + i) % 3 == 0;
        second.push(to_update(row, if retract { -1 } else { 1 })?);
    }

    Ok(InputSpec {
        schema,
        batches: vec![Batch { updates: first }, Batch { updates: second }],
    })
}

/// A surface cell the shared MIR generator provably cannot reach, and why.
///
/// This list is the honest half of the coverage report. A generated suite that
/// only reports what it covered is indistinguishable from one that covered
/// everything, so the gaps are enumerated here with their cause. Each entry is a
/// claim about *why* random MIR cannot produce the cell, which makes it reviewable
/// and gives whoever closes the gap a starting point.
///
/// The overwhelming reason is that [`mz_transform::mirgen::gen_rel`] is shared
/// with the `mz-transform` fuzz targets, and its draw sequence is load-bearing:
/// the release-qualification corpus is carried between runs and is keyed to how
/// many bytes each draw consumes. Adding an operator arm to `gen_rel` would remap
/// every stored corpus entry to a different plan. So these gaps are closed by
/// adding targeted plan shapes *here*, alongside the random draws, not by widening
/// the shared generator.
pub const KNOWN_GAPS: &[(&str, &str)] = &[
    (
        "Constant/Error",
        "gen_scalar emits error literals inside expressions, but gen_rel never \
         roots a collection at an error Constant",
    ),
    (
        "Get/ArrangementLookup",
        "needs literal constraints over an imported index key; gen_rel imports \
         nothing and its Gets carry no key",
    ),
    (
        "Mfp/Temporal/*",
        "needs an mz_now() predicate; gen_scalar has no unmaterializable functions",
    ),
    (
        "Mfp/*/Lookup",
        "same as Get/ArrangementLookup: no keyed input to seek into",
    ),
    (
        "FlatMap/*",
        "gen_rel has no FlatMap arm, so no table function is ever planned",
    ),
    (
        "Reduce/Monotonic*, TopK/Monotonic*",
        "needs a monotonic input; gen_rel marks every leaf non-monotonic and \
         nothing in the plan establishes monotonicity",
    ),
    (
        "Reduce/BasicSingle, Reduce/BasicMultiple",
        "needs a non-accumulable, non-hierarchical aggregate (jsonb_agg, \
         string_agg); gen_aggregate's set is all accumulable or hierarchical",
    ),
    (
        "*/Bucketed (ArrangementStrategy::TemporalBucketing)",
        "lowering only chooses it for plans with mz_now() temporal filters, which \
         gen_scalar cannot express",
    ),
    (
        "ArrangeBy/Several",
        "needs one collection arranged by several keys at once, which the \
         optimizer forms for a join over multiple keys; not reached by the drawn \
         join shapes",
    ),
    (
        "LetRec/*",
        "gen_rel has no LetRec arm, so no recursive binding is ever planned. Note \
         this is also where the fold oracle goes blind, so these cells need the \
         incremental oracle instead",
    ),
];

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
    out.push_str("\nknown gaps (cells random MIR cannot reach, with cause):\n");
    for (cell, why) in KNOWN_GAPS {
        out.push_str(&format!("  {cell}\n      {why}\n"));
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

    /// The directory the committed corpus lives in, relative to the repo root.
    const CORPUS_DIR: &str = "test/clusterd-test-driver/workloads";

    /// The repo root, derived from this crate's manifest directory.
    fn repo_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("repo root")
    }

    /// Regenerating the corpus must reproduce the committed files exactly.
    ///
    /// The corpus is committed so nightly runs are deterministic and a failure is
    /// bisectable, but it is generated, not maintained by hand. Without this check
    /// the two drift apart invisibly: the committed workloads keep passing while
    /// no longer reflecting what the generator produces, so a coverage regression
    /// in the generator never surfaces.
    ///
    /// On failure, regenerate with:
    /// `cargo run -p mz-clusterd-test-driver --bin gen-workloads -- --out test/clusterd-test-driver/workloads`
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn corpus_matches_committed_files() {
        // Must match `gen-workloads`'s defaults, else this compares against a
        // corpus nobody generates.
        let configs = pairwise_configs(STRATEGY_FLAGS);
        let corpus = generate(0x5EED, 6000, 1500, &configs).expect("generate");

        let dir = repo_root().join(CORPUS_DIR);
        let mut committed: Vec<String> = std::fs::read_dir(&dir)
            .expect("corpus directory exists")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|n| n.ends_with(".json"))
            .collect();
        committed.sort();

        let mut expected: Vec<String> = corpus
            .workloads
            .iter()
            .map(|w| format!("{}.json", w.name))
            .collect();
        expected.sort();
        assert_eq!(
            committed, expected,
            "the committed corpus does not match a fresh generation; regenerate it"
        );

        for workload in &corpus.workloads {
            let path = dir.join(format!("{}.json", workload.name));
            let json = std::fs::read_to_string(&path).expect("read committed workload");
            let parsed: Workload = serde_json::from_str(&json).expect("committed workload parses");
            assert_eq!(
                &parsed,
                workload,
                "committed {} differs from a fresh generation; regenerate the corpus",
                path.display()
            );
        }
    }

    /// Every committed workload's claimed cells match what its plan lowers to, and
    /// its oracle selection is consistent with what those oracles can check.
    ///
    /// This catches a corpus edited by hand, and it catches the specific footgun
    /// of a workload requesting the fold oracle over a plan the folder cannot
    /// reduce, which would make that workload silently check nothing.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn committed_workloads_are_self_consistent() {
        let dir = repo_root().join(CORPUS_DIR);
        let mut checked = 0usize;
        for entry in std::fs::read_dir(&dir).expect("corpus directory exists") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let json = std::fs::read_to_string(&path).expect("read workload");
            let workload: Workload = serde_json::from_str(&json).expect("parse workload");

            let cells = realized_cells(&workload)
                .expect("lowering")
                .unwrap_or_else(|| panic!("{} does not lower", path.display()));
            assert_eq!(
                workload.claims,
                cells,
                "{}: claimed cells do not match the lowered plan",
                path.display()
            );

            // A workload asking for the fold oracle must reach a verdict, which
            // means rows *or* an error. Only `Unfoldable` leaves the oracle inert,
            // and the runner turns that into a run-time failure rather than a
            // silent pass, so catching it here is the cheaper place.
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
                    path.display()
                );
            }

            // Strategy invariance needs at least two configurations to compare.
            if workload.oracles.contains(&Oracle::StrategyInvariance) {
                assert!(
                    workload.configs.len() >= 2,
                    "{}: requests strategy invariance with {} config(s)",
                    path.display(),
                    workload.configs.len()
                );
            }
            checked += 1;
        }
        assert!(
            checked > 0,
            "no committed workloads were checked; a vacuous pass here would hide an \
             empty or misplaced corpus"
        );
    }
}
