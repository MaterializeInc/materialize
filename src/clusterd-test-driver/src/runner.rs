// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Executes a [`Workload`] against `clusterd` and applies its oracles.
//!
//! # Per-configuration isolation
//!
//! A workload can name several dyncfg configurations, and the results must agree
//! across all of them (the strategy-invariance oracle). Each configuration runs
//! against a freshly reconciled compute state: the runner reconnects, re-sends
//! `CreateInstance`, and closes the reconciliation window *without* replaying any
//! dataflow, so the replica drops everything from the previous configuration. That
//! buys two things. The global ids can be reused, keeping the JSON's fixed id
//! layout workable, and a configuration can vary create-time settings (which
//! `InstanceConfig` carries and a later `UpdateConfiguration` cannot change), not
//! just render-time ones.
//!
//! Each configuration's settings are sent twice, as the `CreateInstance`
//! `initial_config` snapshot and again as an `UpdateConfiguration`, mirroring a
//! real controller: it syncs its dyncfgs before creating the instance and then
//! keeps them updated. Sending both means a setting read at create time and one
//! read at render time are both honoured, so a workload does not have to know
//! which kind it is toggling.
//!
//! Input shards live in persist, not in compute state, so they survive
//! reconciliation and are written once for the whole workload rather than per
//! configuration.
//!
//! # Non-negativity contract
//!
//! An exported collection must have non-negative multiplicities. This is not a
//! limitation of the runner but of what an index or a persist sink can represent,
//! and it is the same contract the real optimizer upholds. The generator is
//! responsible for it (`gen_rel` returns the flag needed to decide); a workload
//! that violates it fails as a result mismatch rather than being silently
//! tolerated.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_compute_client::protocol::command::{ComputeCommand, PeekTarget};
use mz_dyncfg::ConfigUpdates;
use mz_persist_client::PersistClient;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Diff, GlobalId, RelationDesc, ReprRelationType, Row, SqlRelationType, Timestamp};
use mz_storage_types::controller::CollectionMetadata;

use crate::data::write_updates;
use crate::dataflow::{DataflowBuilder, PersistSink, PersistSource};
use crate::driver::Driver;
use crate::script::parse_config_val;
use crate::surface::{SurfaceCell, cells_of_plan, render_cells};
use crate::workload::{NamedConfig, Oracle, Workload, WorkloadExport, ids};

/// How long to wait for a dataflow to hydrate through the assertion timestamp.
const FRONTIER_TIMEOUT: Duration = Duration::from_secs(120);

/// A consolidated `(row, diff)` multiset.
pub type Multiset = BTreeMap<Row, Diff>;

/// What reading an export produced.
///
/// The common currency every oracle compares in, so an index peek, a shard
/// read-back, a subscribe, and the fold reference are all reduced to the same
/// shape before comparison.
///
/// An error is a *result*, not a failure. A computation over erroring input
/// should produce that error, and the renderer routes it through the `err`
/// collection to exactly that end. Collapsing it into the failure channel would
/// drop every error-propagating plan out of the oracles' reach while looking like
/// coverage. Genuine failures (timeouts, dropped connections, a cancelled peek)
/// stay in `anyhow::Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadResult {
    /// The collection's contents.
    Rows(Multiset),
    /// The collection holds an error, reported verbatim.
    Error(String),
}

impl ReadResult {
    /// Render for a failure message.
    pub fn render(&self) -> String {
        match self {
            ReadResult::Rows(m) => render_multiset(m),
            ReadResult::Error(e) => format!("<error: {e}>"),
        }
    }
}

/// The outcome of running one workload.
#[derive(Debug, Clone)]
pub struct WorkloadOutcome {
    /// The workload's name.
    pub name: String,
    /// The surface cells the realized plan actually exercised.
    pub realized_cells: BTreeSet<SurfaceCell>,
    /// One result multiset per configuration, in `configs` order.
    pub per_config: Vec<(String, ReadResult)>,
    /// Oracle comparisons that could not render a verdict, with the reason.
    ///
    /// Surfaced rather than dropped: a check that quietly stops answering looks
    /// exactly like one that passes, and the count is how a reader knows how much
    /// of the run actually concluded anything.
    pub inconclusive: Vec<String>,
}

/// Render a multiset as deterministic text, for failure messages and goldens.
pub fn render_multiset(m: &Multiset) -> String {
    if m.is_empty() {
        return "<empty>".to_string();
    }
    m.iter()
        .map(|(row, diff)| {
            let datums = row
                .unpack()
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            format!("{diff} {datums}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A `RelationDesc` for a computed relation type, with synthetic column names.
///
/// Peeks and sink exports need a `RelationDesc`, but a built object only carries a
/// `ReprRelationType`. Column names are display-only here, so `c0..cN` suffices.
fn desc_from_repr(typ: &ReprRelationType) -> RelationDesc {
    let sql = SqlRelationType::from_repr(typ);
    let mut builder = RelationDesc::builder();
    for (i, ct) in sql.column_types.iter().enumerate() {
        builder = builder.with_column(format!("c{i}"), ct.clone());
    }
    builder.finish()
}

/// Lift a read that may have surfaced a collection error into a [`ReadResult`].
fn read_result(read: Result<Multiset, String>) -> ReadResult {
    match read {
        Ok(m) => ReadResult::Rows(m),
        Err(e) => ReadResult::Error(e),
    }
}

/// Collapse peeked rows into a multiset.
///
/// A peek expands multiplicities into repeated rows, so counting occurrences
/// recovers the diffs.
fn multiset_from_rows(rows: Vec<Row>) -> Multiset {
    let mut m: Multiset = BTreeMap::new();
    for row in rows {
        *m.entry(row).or_insert(Diff::ZERO) += Diff::from(1);
    }
    m.retain(|_, d| *d != Diff::ZERO);
    m
}

/// Accumulate a subscribe's updates through `ts` into a multiset.
///
/// A subscribe streams the change log, so its contents *at* `ts` are the sum of
/// every update at times `<= ts`. Updates past `ts` are ignored rather than
/// treated as an error: with `up_to = ts + 1` there should be none, but a replica
/// is free to batch, and the comparison is about contents at `ts`.
fn multiset_from_subscribe(updates: Vec<(Row, Timestamp, i64)>, ts: u64) -> Multiset {
    let mut m: Multiset = BTreeMap::new();
    for (row, update_ts, diff) in updates {
        if u64::from(update_ts) <= ts {
            *m.entry(row).or_insert(Diff::ZERO) += Diff::from(diff);
        }
    }
    m.retain(|_, d| *d != Diff::ZERO);
    m
}

/// The runner's state for one workload: a driver, a persist client, and the
/// shards its inputs were written to.
pub struct WorkloadRunner {
    driver: Driver,
    client: PersistClient,
    loc: PersistLocation,
    /// One shard per input, allocated once and reused across configurations.
    input_shards: Vec<ShardId>,
    /// The materialized-view sink's output shard, if the workload exports one.
    ///
    /// Allocated per configuration: a persist sink appends to its shard, so
    /// reusing one across configurations would make the second run read the
    /// first's output.
    mv_shard: Option<ShardId>,
    /// Whether an instance has been opened on the current connection yet.
    ///
    /// The first configuration reuses the connection `Driver::connect` already
    /// opened; only later ones reconnect to drop the previous configuration's
    /// dataflows. Reconnecting immediately after connecting churns the connection
    /// for no reason and leaves the replica tearing down one session while the
    /// next session's commands arrive.
    instance_open: bool,
}

impl WorkloadRunner {
    /// Build a runner over a connected driver.
    pub async fn new(driver: Driver, loc: PersistLocation) -> anyhow::Result<Self> {
        let client = driver.host.client().await?;
        Ok(WorkloadRunner {
            driver,
            client,
            loc,
            input_shards: Vec::new(),
            mv_shard: None,
            instance_open: false,
        })
    }

    /// Run `workload`: write its inputs, then render and check it under each
    /// configuration, then compare across configurations.
    pub async fn run(&mut self, workload: &Workload) -> anyhow::Result<WorkloadOutcome> {
        self.write_inputs(workload).await?;

        // An empty `configs` means one run at the replica's defaults.
        let configs: Vec<NamedConfig> = if workload.configs.is_empty() {
            vec![NamedConfig {
                name: "defaults".to_string(),
                settings: vec![],
            }]
        } else {
            workload.configs.clone()
        };

        let mut per_config = Vec::new();
        let mut realized_cells = BTreeSet::new();
        let mut inconclusive: Vec<String> = Vec::new();
        for (config_index, config) in configs.iter().enumerate() {
            let (result, cells, mut config_inconclusive) = self
                .run_one_config(workload, config_index, config)
                .await
                .map_err(|e| anyhow::anyhow!("config {:?}: {e}", config.name))?;
            // The plan is the same under every configuration, so the realized
            // cells must be too. A configuration that changes them would mean a
            // dyncfg altered lowering, which happens at create time in the
            // controller and not here; assert rather than merge, so such a change
            // surfaces instead of being averaged away.
            if realized_cells.is_empty() {
                realized_cells = cells;
            } else if realized_cells != cells {
                anyhow::bail!(
                    "config {:?} realized different surface cells than an earlier config:\n\
                     earlier:\n{}\nthis:\n{}",
                    config.name,
                    render_cells(&realized_cells),
                    render_cells(&cells)
                );
            }
            per_config.push((config.name.clone(), result));
            inconclusive.append(&mut config_inconclusive);
        }

        self.check_claims(workload, &realized_cells)?;
        if workload.oracles.contains(&Oracle::StrategyInvariance) {
            check_strategy_invariance(workload, &per_config)?;
        }

        Ok(WorkloadOutcome {
            name: workload.name.clone(),
            realized_cells,
            per_config,
            inconclusive,
        })
    }

    /// Write every input's updates to a fresh shard, sealing all of them to the
    /// same upper so the dataflow's inputs become available together.
    async fn write_inputs(&mut self, workload: &Workload) -> anyhow::Result<()> {
        let upper = workload.upper();
        self.input_shards = (0..workload.inputs.len()).map(|_| ShardId::new()).collect();
        for (i, input) in workload.inputs.iter().enumerate() {
            let desc = input.relation_desc();
            let updates = input.updates()?;
            // Seal to the workload-wide upper even when this input has fewer
            // batches, so a shorter input does not hold the dataflow's frontier
            // back below the assertion timestamp.
            write_updates(
                &self.client,
                self.input_shards[i],
                &desc,
                &updates,
                Timestamp::from(0),
                Timestamp::from(upper),
            )
            .await
            .map_err(|e| anyhow::anyhow!("writing input {i}: {e}"))?;
        }
        Ok(())
    }

    /// Reconcile to an empty compute state under `config`, then render the
    /// workload and read back its exports.
    async fn run_one_config(
        &mut self,
        workload: &Workload,
        config_index: usize,
        config: &NamedConfig,
    ) -> anyhow::Result<(ReadResult, BTreeSet<SurfaceCell>, Vec<String>)> {
        self.reset_instance(config).await?;
        if std::env::var_os("DRIVER_DEBUG_RESPONSES").is_some() {
            self.driver
                .log_raw_responses(&format!("{}/{}", workload.name, config.name));
        }

        let ts = workload.assert_ts();
        let upper = workload.upper();

        // One dataflow per export, which is how the real system renders these: an
        // index, a materialized view, and a subscribe over the same view are three
        // separate dataflows, never three exports of one. Building them as one
        // dataflow is not just unfaithful, it does not work. A subscribe carries a
        // finite `up_to` while a persist sink requires an empty one, and a
        // `DataflowDescription` holding both stalls the subscribe.
        //
        // Rendering each export separately also strengthens export invariance:
        // it then compares three independently rendered dataflows rather than
        // three read paths out of one shared computation.
        let mut cells = BTreeSet::new();
        let mut plan_type = None;
        for export in &workload.exports {
            let (df, typ) = self.build_dataflow(workload, config_index, *export, ts, upper)?;
            cells.extend(
                df.objects_to_build
                    .iter()
                    .flat_map(|o| cells_of_plan(&o.plan)),
            );
            plan_type = Some(typ);

            let id = GlobalId::User(export_id(config_index, *export));
            self.driver.submit_dataflow(df)?;
            if *export == WorkloadExport::Subscribe {
                // Register before scheduling, so the response pump is accumulating
                // by the time the sink emits its first batch.
                self.driver.register_subscribe(id);
            }
            self.driver.schedule(id)?;
        }

        // Close the reconciliation window only now, with this configuration's
        // dataflows already declared inside it.
        //
        // This ordering is what a controller does on reconnect: re-create the
        // instance, replay the dataflows it wants the replica to be running, then
        // send `InitializationComplete`. Closing the window first and creating
        // dataflows afterwards looks equivalent, since creating a dataflow in the
        // steady state is perfectly normal, but it is not: after a reconnect that
        // reconciled to zero collections, dataflows created afterwards were
        // installed and then never reported a frontier. The first configuration
        // (which does not reconnect) was always fine, and every later one could
        // hang. `scripts/reconciliation.spec` is the worked example of the correct
        // order.
        self.driver.send(ComputeCommand::InitializationComplete)?;

        // Take the materialized-view sinks out of read-only mode. After the
        // window, so the command applies to a live collection rather than being
        // folded into reconciliation.
        for export in &workload.exports {
            if *export == WorkloadExport::MaterializedView {
                let id = GlobalId::User(export_id(config_index, *export));
                self.driver.send(ComputeCommand::AllowWrites(id))?;
            }
        }
        let plan_type =
            plan_type.ok_or_else(|| anyhow::anyhow!("workload has no exports to render"))?;

        let result_desc = desc_from_repr(&plan_type);
        let mut by_export: BTreeMap<WorkloadExport, ReadResult> = BTreeMap::new();
        // Read the subscribe first. It is the one export with a finite `up_to`, so
        // it completes on its own and then stops being observable; the index and
        // materialized-view reads block until their collection catches up, and
        // doing them first can leave the subscribe waiting past its timeout.
        let mut read_order: Vec<WorkloadExport> = workload.exports.clone();
        read_order.sort_by_key(|e| match e {
            WorkloadExport::Subscribe => 0,
            WorkloadExport::Index => 1,
            WorkloadExport::MaterializedView => 2,
        });
        for export in &read_order {
            let m = self
                .read_export(config_index, *export, &result_desc, ts)
                .await
                .map_err(|e| anyhow::anyhow!("reading {export:?} export: {e}"))?;
            by_export.insert(*export, m);
        }

        // Every oracle compares against one canonical result. Prefer the index
        // (the cheapest and most direct read of the maintained arrangement) and
        // fall back to whichever export the workload does have.
        let canonical = by_export
            .get(&WorkloadExport::Index)
            .or_else(|| by_export.get(&WorkloadExport::MaterializedView))
            .or_else(|| by_export.get(&WorkloadExport::Subscribe))
            .ok_or_else(|| anyhow::anyhow!("workload has no exports to read"))?
            .clone();

        if workload.oracles.contains(&Oracle::ExportInvariance) {
            check_export_invariance(&by_export)?;
        }
        let mut inconclusive = Vec::new();
        if workload.oracles.contains(&Oracle::FoldConstants) {
            if let Some(reason) = check_fold_constants(workload, ts, &canonical)? {
                inconclusive.push(format!("{}: {reason}", config.name));
            }
        }
        if workload.oracles.contains(&Oracle::Incremental) {
            self.check_incremental(workload, config_index, ts, upper, &canonical)
                .await?;
        }

        Ok((canonical, cells, inconclusive))
    }

    /// Drop the previous configuration's dataflows and open a fresh instance
    /// under `config`.
    ///
    /// Reconnecting and closing the reconciliation window with no replayed
    /// dataflow is what clears the compute state (see the module docs).
    async fn reset_instance(&mut self, config: &NamedConfig) -> anyhow::Result<()> {
        // Reconnect only to discard a previous configuration's dataflows. The
        // first configuration runs on the connection `Driver::connect` opened,
        // which is already fresh and has no instance on it.
        if self.instance_open {
            self.driver.reconnect().await?;
        }
        self.instance_open = true;
        let mut updates = ConfigUpdates::default();
        for setting in &config.settings {
            updates.add_dynamic(
                &setting.name,
                parse_config_val(&setting.ty, &setting.value)?,
            );
        }
        // The same settings at create time and at render time; see the module docs.
        self.driver.create_instance(None, false, updates.clone())?;
        self.driver.update_configuration(updates)?;
        // The reconciliation window stays OPEN. The caller creates this
        // configuration's dataflows inside it and closes it afterwards; see
        // `run_one_config`.
        Ok(())
    }

    /// Assemble the dataflow for one export, returning it and the computation's
    /// output relation type.
    fn build_dataflow(
        &mut self,
        workload: &Workload,
        config_index: usize,
        export: WorkloadExport,
        as_of: u64,
        upper: u64,
    ) -> anyhow::Result<(
        mz_compute_types::dataflows::DataflowDescription<
            mz_compute_types::plan::render_plan::RenderPlan,
            CollectionMetadata,
        >,
        ReprRelationType,
    )> {
        let mut builder = DataflowBuilder::new(format!("workload-{}-{export:?}", workload.name));
        if workload.optimize {
            builder.optimize();
        }
        for (i, input) in workload.inputs.iter().enumerate() {
            builder.import_persist(
                GlobalId::User(ids::input(i)),
                PersistSource {
                    shard: self.input_shards[i],
                    location: self.loc.clone(),
                    desc: input.relation_desc(),
                    upper: Timestamp::from(upper),
                },
            );
        }
        let plan_id = GlobalId::User(ids::plan(config_index, export));
        let plan_type = workload.plan.typ();
        builder.build(plan_id, workload.plan.clone());

        let result_desc = desc_from_repr(&plan_type);
        match export {
            WorkloadExport::Index => {
                // Key the index by no columns. An empty key always exists
                // regardless of the plan's arity, so the same workload shape
                // works for any output width, and it keeps the peek a full
                // scan rather than a lookup.
                builder.export_index(GlobalId::User(ids::index(config_index)), plan_id, vec![]);
            }
            WorkloadExport::MaterializedView => {
                let shard = ShardId::new();
                self.mv_shard = Some(shard);
                builder.export_materialized_view(
                    GlobalId::User(ids::mv_sink(config_index)),
                    plan_id,
                    result_desc,
                    PersistSink {
                        shard,
                        location: self.loc.clone(),
                    },
                );
            }
            WorkloadExport::Subscribe => {
                builder.export_subscribe(
                    GlobalId::User(ids::subscribe_sink(config_index)),
                    plan_id,
                    result_desc,
                    // Complete one past the assertion timestamp, so the
                    // subscribe's contents at `as_of` are final.
                    timely::progress::Antichain::from_elem(Timestamp::from(as_of + 1)),
                );
            }
        }
        builder.as_of(Timestamp::from(as_of));
        let df = builder.finish()?;
        Ok((df, plan_type))
    }

    /// Read one export's contents at `ts`.
    async fn read_export(
        &self,
        config_index: usize,
        export: WorkloadExport,
        result_desc: &RelationDesc,
        ts: u64,
    ) -> anyhow::Result<ReadResult> {
        match export {
            WorkloadExport::Index => {
                let id = GlobalId::User(ids::index(config_index));
                self.driver
                    .expect_frontier(id, Timestamp::from(ts).step_forward(), FRONTIER_TIMEOUT)
                    .await?;
                let read = self
                    .driver
                    .peek_result(
                        PeekTarget::Index { id },
                        result_desc.clone(),
                        Timestamp::from(ts),
                    )
                    .await?;
                Ok(read_result(read.map(multiset_from_rows)))
            }
            WorkloadExport::MaterializedView => {
                let shard = self
                    .mv_shard
                    .ok_or_else(|| anyhow::anyhow!("no materialized-view shard allocated"))?;
                // A persist peek blocks until the shard seals through `ts`, so it
                // doubles as the wait for the sink to catch up.
                let read = self
                    .driver
                    .peek_result(
                        PeekTarget::Persist {
                            id: GlobalId::User(ids::mv_sink(config_index)),
                            metadata: CollectionMetadata {
                                persist_location: self.loc.clone(),
                                data_shard: shard,
                                relation_desc: result_desc.clone(),
                                txns_shard: None,
                            },
                        },
                        result_desc.clone(),
                        Timestamp::from(ts),
                    )
                    .await?;
                Ok(read_result(read.map(multiset_from_rows)))
            }
            WorkloadExport::Subscribe => {
                let read = self
                    .driver
                    .await_subscribe_result(
                        GlobalId::User(ids::subscribe_sink(config_index)),
                        Timestamp::from(ts + 1),
                        FRONTIER_TIMEOUT,
                    )
                    .await?;
                Ok(read_result(read.map(|u| multiset_from_subscribe(u, ts))))
            }
        }
    }

    /// The incremental oracle: build a second dataflow whose `as_of` is already
    /// `ts`, so it computes the answer in one shot from the snapshot rather than
    /// by maintaining it forward from `0`, and require the two to agree.
    ///
    /// Where the fold oracle is live this is a cross-check. Where it is not
    /// (`LetRec`, which the constant folder cannot see through) this is the only
    /// oracle that distinguishes a correct incremental update from a wrong one.
    async fn check_incremental(
        &mut self,
        workload: &Workload,
        config_index: usize,
        ts: u64,
        upper: u64,
        maintained: &ReadResult,
    ) -> anyhow::Result<()> {
        let mut builder = DataflowBuilder::new(format!("workload-{}-recompute", workload.name));
        if workload.optimize {
            builder.optimize();
        }
        for (i, input) in workload.inputs.iter().enumerate() {
            builder.import_persist(
                GlobalId::User(ids::input(i)),
                PersistSource {
                    shard: self.input_shards[i],
                    location: self.loc.clone(),
                    desc: input.relation_desc(),
                    upper: Timestamp::from(upper),
                },
            );
        }
        let plan_id = GlobalId::User(ids::recompute_plan(config_index));
        let plan_type = workload.plan.typ();
        builder.build(plan_id, workload.plan.clone());
        let index_id = GlobalId::User(ids::recompute_index(config_index));
        builder.export_index(index_id, plan_id, vec![]);
        builder.as_of(Timestamp::from(ts));
        let df = builder.finish()?;

        self.driver.submit_dataflow(df)?;
        self.driver.schedule(index_id)?;
        self.driver
            .expect_frontier(
                index_id,
                Timestamp::from(ts).step_forward(),
                FRONTIER_TIMEOUT,
            )
            .await?;
        let read = self
            .driver
            .peek_result(
                PeekTarget::Index { id: index_id },
                desc_from_repr(&plan_type),
                Timestamp::from(ts),
            )
            .await?;
        let recomputed = read_result(read.map(multiset_from_rows));
        if &recomputed != maintained {
            anyhow::bail!(
                "incremental oracle: the maintained collection differs from a fresh \
                 computation at the same as_of\nmaintained:\n{}\nrecomputed:\n{}",
                maintained.render(),
                recomputed.render()
            );
        }
        Ok(())
    }

    /// Require the realized cells to match what the workload claimed.
    ///
    /// A claim is a promise about what the workload tests. Checking it is what
    /// keeps a corpus from silently decaying: a generator change that stops
    /// producing a plan shape fails here instead of continuing to pass while
    /// covering less. An empty `claims` skips the check, for a hand-written
    /// workload that is not making a coverage claim.
    fn check_claims(
        &self,
        workload: &Workload,
        realized: &BTreeSet<SurfaceCell>,
    ) -> anyhow::Result<()> {
        if workload.claims.is_empty() {
            return Ok(());
        }
        if &workload.claims != realized {
            let missing: Vec<_> = workload.claims.difference(realized).collect();
            let extra: Vec<_> = realized.difference(&workload.claims).collect();
            anyhow::bail!(
                "surface claims do not match the realized plan\n\
                 claimed but not realized: {missing:?}\n\
                 realized but not claimed: {extra:?}\n\
                 realized cells:\n{}",
                render_cells(realized)
            );
        }
        Ok(())
    }
}

/// The global id an export is built under.
fn export_id(config_index: usize, export: WorkloadExport) -> u64 {
    match export {
        WorkloadExport::Index => ids::index(config_index),
        WorkloadExport::MaterializedView => ids::mv_sink(config_index),
        WorkloadExport::Subscribe => ids::subscribe_sink(config_index),
    }
}

/// The fold oracle: evaluate the same plan with its inputs substituted as literal
/// constants, using the optimizer's constant folder, and require agreement.
///
/// The folder is an independent implementation of the same semantics, which is
/// what makes this a real oracle rather than a self-consistency check.
///
/// A plan the folder cannot reduce to a constant yields no verdict. That is the
/// dangerous case: an oracle that silently declines is indistinguishable from one
/// that passes, and the whole check would rot unnoticed. So declining is an error
/// here, and a workload whose plan cannot fold must not request this oracle. The
/// generator knows which plans those are (any containing a `LetRec`) and selects
/// oracles accordingly.
fn check_fold_constants(
    workload: &Workload,
    ts: u64,
    actual: &ReadResult,
) -> anyhow::Result<Option<String>> {
    use mz_transform::mirgen::FoldOutcome;

    let reference_plan = workload.plan_with_constants(ts)?;
    let expected = match mz_transform::mirgen::fold_outcome(reference_plan) {
        FoldOutcome::Rows(m) => ReadResult::Rows(m),
        // Spell the error the way the renderer will. The renderer surfaces an
        // `EvalError` wrapped in a `DataflowError`, whose `Display` prepends
        // "Evaluation error: ", so wrapping it here makes the comparison exact
        // instead of a substring match that would pass on a genuinely different
        // error that merely shares a prefix.
        FoldOutcome::Error(err) => ReadResult::Error(
            mz_storage_types::errors::DataflowError::EvalError(Box::new(err)).to_string(),
        ),
        FoldOutcome::Unfoldable => anyhow::bail!(
            "fold oracle is inert: the plan did not reduce to a constant, so this \
             workload must not request the fold-constants oracle"
        ),
    };
    match (&expected, actual) {
        (ReadResult::Rows(e), ReadResult::Rows(a)) if e == a => Ok(None),
        // The finding this oracle exists for: both sides computed a result and
        // they disagree.
        (ReadResult::Rows(e), ReadResult::Rows(a)) => anyhow::bail!(
            "fold oracle: rendered output differs from the constant-folded \
             reference\nexpected:\n{}\nactual:\n{}",
            render_multiset(e),
            render_multiset(a)
        ),
        (ReadResult::Error(e), ReadResult::Error(a)) if e == a => Ok(None),
        (ReadResult::Error(e), ReadResult::Error(a)) => anyhow::bail!(
            "fold oracle: both sides errored, but with different errors\n\
             expected: {e}\nactual:   {a}"
        ),
        // Rows on one side and an error on the other is not a verdict this oracle
        // can render, so it is reported rather than judged.
        //
        // Errors travel in a dataflow's `err` collection, which is unioned through
        // operators independently of the `ok` collection. A join with an empty
        // input therefore still forwards its inputs' errors, while constant
        // folding computes the join, gets no rows, and drops the error with them.
        // Neither side is obviously wrong: Materialize does not promise that
        // optimization preserves errors exactly, so a difference here is expected
        // behaviour often enough that failing on it would bury the real
        // divergences this oracle exists to catch.
        //
        // Counted and named rather than silently skipped. A skip is
        // indistinguishable from a pass, and an oracle that quietly stops
        // answering is the failure mode this suite is built to avoid.
        (ReadResult::Rows(_), ReadResult::Error(a)) => Ok(Some(format!(
            "folder produced rows, renderer produced an error ({a}); errors survive \
             row elimination in dataflow but not in constant folding"
        ))),
        (ReadResult::Error(e), ReadResult::Rows(_)) => Ok(Some(format!(
            "folder produced an error ({e}), renderer produced rows"
        ))),
    }
}

/// The export-invariance oracle: every export of the same collection must hold
/// the same contents.
///
/// Needs at least two exports to say anything. With fewer it yields no verdict,
/// which is fine here (unlike the fold oracle) because the condition is visible
/// in the workload itself rather than depending on plan structure.
fn check_export_invariance(by_export: &BTreeMap<WorkloadExport, ReadResult>) -> anyhow::Result<()> {
    let mut iter = by_export.iter();
    let Some((first_kind, first)) = iter.next() else {
        return Ok(());
    };
    for (kind, m) in iter {
        if m != first {
            anyhow::bail!(
                "export invariance: {kind:?} differs from {first_kind:?}\n\
                 {first_kind:?}:\n{}\n{kind:?}:\n{}",
                first.render(),
                m.render()
            );
        }
    }
    Ok(())
}

/// The strategy-invariance oracle: the same workload under different dyncfg
/// configurations must produce the same result.
///
/// This is what tests the compute strategy flags (join core, batcher spill,
/// correction v2, dictionary compression, temporal bucketing) without needing a
/// reference implementation: whatever the right answer is, it cannot depend on
/// which strategy computed it.
fn check_strategy_invariance(
    workload: &Workload,
    per_config: &[(String, ReadResult)],
) -> anyhow::Result<()> {
    let Some((base_name, base)) = per_config.first() else {
        return Ok(());
    };
    if per_config.len() < 2 {
        anyhow::bail!(
            "workload {:?} requests the strategy-invariance oracle but names \
             {} configuration(s); it needs at least two to compare",
            workload.name,
            per_config.len()
        );
    }
    for (name, m) in &per_config[1..] {
        if m != base {
            anyhow::bail!(
                "strategy invariance: config {name:?} differs from {base_name:?}\n\
                 {base_name:?}:\n{}\n{name:?}:\n{}",
                base.render(),
                m.render()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::workload::{Batch, ColumnTy, InputSpec, Update, Value};

    fn int64(v: i64) -> Value {
        Value::Int64(v.to_string())
    }

    fn one_input_workload(oracles: Vec<Oracle>, configs: Vec<NamedConfig>) -> Workload {
        use mz_expr::MirRelationExpr;
        let input = InputSpec {
            schema: vec![ColumnTy::Int64],
            batches: vec![Batch {
                updates: vec![Update {
                    values: vec![int64(7)],
                    diff: 1,
                }],
            }],
        };
        let typ = mz_transform::mirgen::nullable_relation_type(&[mz_transform::mirgen::Ty::Int64]);
        Workload {
            name: "t".into(),
            seed: None,
            inputs: vec![input],
            plan: MirRelationExpr::global_get(GlobalId::User(ids::input(0)), typ),
            exports: vec![WorkloadExport::Index],
            configs,
            claims: BTreeSet::new(),
            oracles,
            optimize: false,
        }
    }

    /// The fold oracle refuses to be inert: a plan it cannot reduce yields an
    /// error, not a silent pass. Without this, an unfoldable plan would look
    /// exactly like a correct one.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn fold_oracle_refuses_to_be_inert() {
        use mz_expr::{Id, LocalId, MirRelationExpr};

        // A `LetRec` the constant folder cannot see through.
        let typ = mz_transform::mirgen::nullable_relation_type(&[mz_transform::mirgen::Ty::Int64]);
        let mut workload = one_input_workload(vec![Oracle::FoldConstants], vec![]);
        workload.plan = MirRelationExpr::LetRec {
            ids: vec![LocalId::new(0)],
            values: vec![MirRelationExpr::global_get(
                GlobalId::User(ids::input(0)),
                typ.clone(),
            )],
            limits: vec![None],
            body: Box::new(MirRelationExpr::Get {
                id: Id::Local(LocalId::new(0)),
                typ,
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            }),
        };

        let err = check_fold_constants(&workload, 0, &ReadResult::Rows(Multiset::new()))
            .expect_err("an unfoldable plan must not silently pass");
        assert!(
            err.to_string().contains("inert"),
            "expected an inertness error, got: {err}"
        );
    }

    /// The fold oracle agrees with a correct result and rejects a wrong one, so
    /// it is live in both directions.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer`
    fn fold_oracle_is_live() {
        let workload = one_input_workload(vec![Oracle::FoldConstants], vec![]);

        // The correct answer: the one row that was written.
        let mut correct: Multiset = BTreeMap::new();
        let mut row = Row::default();
        row.packer().push(mz_repr::Datum::Int64(7));
        correct.insert(row.clone(), Diff::from(1));
        assert_eq!(
            check_fold_constants(&workload, 0, &ReadResult::Rows(correct.clone()))
                .expect("the correct result must pass"),
            None,
            "an agreeing comparison reaches a verdict, so reports no reason"
        );

        // A wrong multiplicity must be caught.
        let mut wrong = correct.clone();
        wrong.insert(row, Diff::from(2));
        let err = check_fold_constants(&workload, 0, &ReadResult::Rows(wrong))
            .expect_err("a wrong result must be rejected");
        assert!(err.to_string().contains("fold oracle"));

        // Rows on one side and an error on the other yields no verdict, and says
        // so. It must not read as a pass: an oracle that quietly stops answering
        // is indistinguishable from one that agrees, which is the failure mode
        // this suite exists to avoid.
        let verdict = check_fold_constants(&workload, 0, &ReadResult::Error("boom".into()))
            .expect("a mixed rows/error comparison is reported, not an error");
        let reason = verdict.expect("the mixed case must report a reason");
        assert!(
            reason.contains("renderer produced an error"),
            "expected a named reason, got: {reason}"
        );
    }

    /// Strategy invariance catches a divergence between configurations, and
    /// refuses to pass vacuously when only one configuration was run.
    #[mz_ore::test]
    fn strategy_invariance_is_live() {
        let workload = one_input_workload(
            vec![Oracle::StrategyInvariance],
            vec![
                NamedConfig {
                    name: "a".into(),
                    settings: vec![],
                },
                NamedConfig {
                    name: "b".into(),
                    settings: vec![],
                },
            ],
        );

        let mut row = Row::default();
        row.packer().push(mz_repr::Datum::Int64(7));
        let a = ReadResult::Rows([(row.clone(), Diff::from(1))].into_iter().collect());
        let b = ReadResult::Rows([(row, Diff::from(2))].into_iter().collect());

        check_strategy_invariance(
            &workload,
            &[("a".into(), a.clone()), ("b".into(), a.clone())],
        )
        .expect("identical results must pass");

        let err = check_strategy_invariance(&workload, &[("a".into(), a.clone()), ("b".into(), b)])
            .expect_err("diverging results must be rejected");
        assert!(err.to_string().contains("strategy invariance"));

        // One configuration cannot demonstrate invariance, so it is an error
        // rather than a pass.
        let err = check_strategy_invariance(&workload, &[("a".into(), a)])
            .expect_err("a single config must not satisfy the oracle");
        assert!(err.to_string().contains("at least two"));
    }

    /// Export invariance catches a divergence between two exports of the same
    /// collection.
    #[mz_ore::test]
    fn export_invariance_is_live() {
        let mut row = Row::default();
        row.packer().push(mz_repr::Datum::Int64(7));
        let a = ReadResult::Rows([(row.clone(), Diff::from(1))].into_iter().collect());
        let b = ReadResult::Rows([(row, Diff::from(3))].into_iter().collect());

        let same: BTreeMap<WorkloadExport, ReadResult> = [
            (WorkloadExport::Index, a.clone()),
            (WorkloadExport::Subscribe, a.clone()),
        ]
        .into_iter()
        .collect();
        check_export_invariance(&same).expect("identical exports must pass");

        let differing: BTreeMap<WorkloadExport, ReadResult> =
            [(WorkloadExport::Index, a), (WorkloadExport::Subscribe, b)]
                .into_iter()
                .collect();
        let err =
            check_export_invariance(&differing).expect_err("diverging exports must be rejected");
        assert!(err.to_string().contains("export invariance"));
    }
}
