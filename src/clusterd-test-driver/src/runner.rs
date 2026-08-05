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
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

use crate::data::write_updates;
use crate::driver::Driver;
use crate::responses::SubscribePoison;
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

    /// Whether the collection held an error rather than rows.
    pub fn is_error(&self) -> bool {
        matches!(self, ReadResult::Error(_))
    }
}

/// What one export held at each asserted timestamp.
pub type Timeline = BTreeMap<u64, ReadResult>;

/// The outcome of running one workload.
#[derive(Debug, Clone)]
pub struct WorkloadOutcome {
    /// The workload's name.
    pub name: String,
    /// The surface cells the realized plan actually exercised.
    pub realized_cells: BTreeSet<SurfaceCell>,
    /// One result timeline per configuration, in `configs` order.
    pub per_config: Vec<(String, Timeline)>,
    /// Oracle comparisons that could not render a verdict, with the reason.
    ///
    /// Surfaced rather than dropped: a check that quietly stops answering looks
    /// exactly like one that passes, and the count is how a reader knows how much
    /// of the run actually concluded anything.
    pub inconclusive: Vec<String>,
    /// Whether any asserted timestamp produced something to compare: rows, or an
    /// error.
    ///
    /// A workload empty at every timestamp passes every oracle without comparing
    /// anything, since an empty reference and an empty read agree. That is not a
    /// failure (an empty collection is a legitimate answer, and some shapes exist to
    /// test it), but a run made mostly of them tests far less than its cell count
    /// suggests, so it is reported rather than left invisible. An error counts: it
    /// is a verdict the oracles check against.
    pub produced_output: bool,
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
    /// One materialized-view output shard per configuration.
    ///
    /// Per configuration because a persist sink appends to its shard, so reusing one
    /// would make the second configuration read the first's output.
    mv_shard: Vec<ShardId>,
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
            mv_shard: Vec::new(),
            instance_open: false,
        })
    }

    /// Run `workload`: write its inputs, then render and check it under each
    /// configuration, then compare across configurations.
    pub async fn run(&mut self, workload: &Workload) -> anyhow::Result<WorkloadOutcome> {
        // The incremental oracle builds one recompute dataflow per asserted
        // timestamp, striding its ids by the timestamp, so a workload with enough
        // batches would run into the next configuration's id range. Nothing
        // generated comes close, and a collision would show up as a dataflow
        // mysteriously standing in for another rather than as a clean failure.
        anyhow::ensure!(
            ids::recompute_index(0, workload.assert_ts()) < ids::config_base(1),
            "{} asserts at {} timestamps, which overruns the per-configuration id \
             range; raise ids::CONFIG_STRIDE",
            workload.name,
            workload.assert_ts() + 1
        );
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

        // One output shard per configuration, allocated up front so the dataflow
        // builders can be handed a complete shard set.
        self.mv_shard = configs.iter().map(|_| ShardId::new()).collect();

        let mut per_config: Vec<(String, Timeline)> = Vec::new();
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

        let produced_output = per_config.iter().any(|(_, timeline)| {
            timeline
                .values()
                .any(|r| !matches!(r, ReadResult::Rows(m) if m.is_empty()))
        });
        Ok(WorkloadOutcome {
            name: workload.name.clone(),
            realized_cells,
            per_config,
            inconclusive,
            produced_output,
        })
    }

    /// Write every input's updates to a fresh shard, sealing all of them to the
    /// same upper so the dataflow's inputs become available together.
    async fn write_inputs(&mut self, workload: &Workload) -> anyhow::Result<()> {
        let upper = workload.upper();
        self.input_shards = (0..workload.inputs.len()).map(|_| ShardId::new()).collect();
        for (i, input) in workload.inputs.iter().enumerate() {
            let desc = input.relation_desc();
            let mut updates = input.updates()?;
            // The synthesized rows go in the same append as the declared batches,
            // so a volume input is one `compare_and_append` rather than one per
            // thousand rows.
            updates.extend(input.volume_updates(workload.volume)?);
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
    ) -> anyhow::Result<(Timeline, BTreeSet<SurfaceCell>, Vec<String>)> {
        self.reset_instance(config).await?;
        if std::env::var_os("DRIVER_DEBUG_RESPONSES").is_some() {
            self.driver
                .log_raw_responses(&format!("{}/{}", workload.name, config.name));
        }

        let ts = workload.assert_ts();
        let timestamps: Vec<u64> = workload.timestamps().collect();

        // Every export starts at `as_of = 0` and is maintained forward through the
        // input's batches. That is what makes this an incremental test at all: an
        // export created at the assertion timestamp reads one snapshot and stops,
        // and then every later timestamp the oracles look at is a state no dataflow
        // ever passed through.
        const EXPORT_AS_OF: u64 = 0;

        // One dataflow per export. Rendering each export separately also
        // strengthens export invariance: it compares three independently rendered
        // dataflows rather than three read paths out of one shared computation. See
        // `Workload::export_dataflow` for why one dataflow cannot serve all three.
        let mut cells = BTreeSet::new();
        let mut plan_type = None;
        for export in &workload.exports {
            let (df, typ) = workload.export_dataflow(
                config_index,
                *export,
                EXPORT_AS_OF,
                &self.shards(config_index),
            )?;
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

        let result_desc = crate::workload::desc_from_repr(&plan_type);
        let mut by_export: BTreeMap<WorkloadExport, Timeline> = BTreeMap::new();
        let mut subscribe_poison = None;
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
            let (timeline, poison) = self
                .read_export(config_index, *export, &result_desc, &timestamps)
                .await
                .map_err(|e| anyhow::anyhow!("reading {export:?} export: {e}"))?;
            subscribe_poison = subscribe_poison.or(poison);
            by_export.insert(*export, timeline);
        }

        // Every oracle compares against one canonical timeline. Prefer the index
        // (the cheapest and most direct read of the maintained arrangement) and
        // fall back to whichever export the workload does have.
        let canonical = by_export
            .get(&WorkloadExport::Index)
            .or_else(|| by_export.get(&WorkloadExport::MaterializedView))
            .or_else(|| by_export.get(&WorkloadExport::Subscribe))
            .ok_or_else(|| anyhow::anyhow!("workload has no exports to read"))?
            .clone();

        let mut inconclusive = Vec::new();
        if workload.oracles.contains(&Oracle::ExportInvariance) {
            for reason in check_export_invariance(&by_export, subscribe_poison.as_ref())? {
                inconclusive.push(format!("{}: {reason}", config.name));
            }
        }
        if workload.oracles.contains(&Oracle::Incremental) {
            self.check_incremental(workload, config_index, &canonical)
                .await?;
        }
        self.check_compaction(workload, config_index, &result_desc, ts, &canonical)
            .await?;
        if workload.oracles.contains(&Oracle::Reconciliation) {
            self.check_reconciliation(workload, config_index, config, &result_desc, &canonical)
                .await?;
        }

        Ok((canonical, cells, inconclusive))
    }

    /// Where this workload's collections live, for the dataflow builders.
    fn shards(&self, config_index: usize) -> crate::workload::WorkloadShards<'_> {
        crate::workload::WorkloadShards {
            location: &self.loc,
            inputs: &self.input_shards,
            sink: self.mv_shard[config_index],
        }
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

    /// Read one export's contents at every timestamp in `tss`.
    ///
    /// The exports are maintained from `as_of = 0`, so every one of these
    /// timestamps is a state the dataflow passed through rather than a snapshot it
    /// started at. The arrangement is never allowed to compact below `0` until
    /// [`Self::check_compaction`], which is what keeps the historical peeks legal.
    async fn read_export(
        &self,
        config_index: usize,
        export: WorkloadExport,
        result_desc: &RelationDesc,
        tss: &[u64],
    ) -> anyhow::Result<(Timeline, Option<SubscribePoison>)> {
        let last = tss.last().copied().unwrap_or(0);
        let mut timeline = Timeline::new();
        let mut subscribe_poison = None;
        match export {
            WorkloadExport::Index => {
                let id = GlobalId::User(ids::index(config_index));
                // One wait, for the last timestamp: an arrangement that has caught
                // up to the last is readable at all of them.
                self.driver
                    .expect_frontier(id, Timestamp::from(last).step_forward(), FRONTIER_TIMEOUT)
                    .await?;
                for ts in tss {
                    let read = self
                        .driver
                        .peek_result(
                            PeekTarget::Index { id },
                            result_desc.clone(),
                            Timestamp::from(*ts),
                        )
                        .await?;
                    timeline.insert(*ts, read_result(read.map(multiset_from_rows)));
                }
            }
            WorkloadExport::MaterializedView => {
                let metadata = CollectionMetadata {
                    persist_location: self.loc.clone(),
                    data_shard: self.mv_shard[config_index],
                    relation_desc: result_desc.clone(),
                    txns_shard: None,
                };
                for ts in tss {
                    // A persist peek blocks until the shard seals through `ts`, so
                    // it doubles as the wait for the sink to catch up.
                    let read = self
                        .driver
                        .peek_result(
                            PeekTarget::Persist {
                                id: GlobalId::User(ids::mv_sink(config_index)),
                                metadata: metadata.clone(),
                            },
                            result_desc.clone(),
                            Timestamp::from(*ts),
                        )
                        .await?;
                    timeline.insert(*ts, read_result(read.map(multiset_from_rows)));
                }
            }
            WorkloadExport::Subscribe => {
                // One await: the change log through the last timestamp accumulates
                // to every earlier one.
                let outcome = self
                    .driver
                    .await_subscribe_result(
                        GlobalId::User(ids::subscribe_sink(config_index)),
                        Timestamp::from(last + 1),
                        FRONTIER_TIMEOUT,
                    )
                    .await?;
                // An error poisons the subscribe from the batch it arrived in
                // onwards, so it is a property of a suffix of the timeline rather
                // than of one timestamp. Before that suffix the subscribe reported
                // ordinary updates and they are compared as usual.
                let poisoned_from = outcome
                    .poison
                    .as_ref()
                    .map_or(u64::MAX, |p| p.lower.map_or(0, u64::from));
                for ts in tss {
                    let result = match &outcome.poison {
                        Some(poison) if *ts >= poisoned_from => {
                            ReadResult::Error(poison.message.clone())
                        }
                        _ => {
                            ReadResult::Rows(multiset_from_subscribe(outcome.updates.clone(), *ts))
                        }
                    };
                    timeline.insert(*ts, result);
                }
                subscribe_poison = outcome.poison;
            }
        }
        Ok((timeline, subscribe_poison))
    }

    /// The incremental oracle: at each asserted timestamp, build a dataflow whose
    /// `as_of` is already that timestamp, so it computes the answer in one shot from
    /// the snapshot rather than by maintaining it forward from `0`, and require the
    /// two to agree.
    ///
    /// Where the fold oracle is live this is a cross-check. Where it is not
    /// (`LetRec`, which the constant folder cannot see through) this is the only
    /// oracle that distinguishes a correct incremental update from a wrong one.
    async fn check_incremental(
        &self,
        workload: &Workload,
        config_index: usize,
        maintained: &Timeline,
    ) -> anyhow::Result<()> {
        for (ts, expected) in maintained {
            let (df, plan_type) =
                workload.recompute_dataflow(config_index, *ts, &self.shards(config_index))?;
            let index_id = GlobalId::User(ids::recompute_index(config_index, *ts));
            self.driver.submit_dataflow(df)?;
            self.driver.schedule(index_id)?;
            self.driver
                .expect_frontier(
                    index_id,
                    Timestamp::from(*ts).step_forward(),
                    FRONTIER_TIMEOUT,
                )
                .await?;
            let read = self
                .driver
                .peek_result(
                    PeekTarget::Index { id: index_id },
                    crate::workload::desc_from_repr(&plan_type),
                    Timestamp::from(*ts),
                )
                .await?;
            let recomputed = read_result(read.map(multiset_from_rows));
            if &recomputed != expected {
                anyhow::bail!(
                    "incremental oracle at ts {ts}: the maintained collection differs \
                     from a fresh computation at the same as_of\nmaintained:\n{}\n\
                     recomputed:\n{}",
                    expected.render(),
                    recomputed.render()
                );
            }
        }
        Ok(())
    }

    /// Reconnect and replay the dataflows that are already installed, as a
    /// controller does when a replica reconnects, then read again and require the
    /// answer to be unchanged.
    ///
    /// Reconciliation matches the replayed descriptions against what the replica is
    /// running and keeps the ones that match, rather than rebuilding them. Nothing
    /// else in the suite exercises that: `reset_instance` reconciles to *zero*
    /// dataflows between configurations, which takes the drop-everything path. The
    /// keep path is where a mismatched description silently substitutes one
    /// collection for another, and where a kept dataflow has to resume reporting
    /// frontiers to a connection that never saw it start.
    ///
    /// The replay is byte-identical to what was installed, which is the case a
    /// controller actually produces and the case that must keep the dataflow.
    async fn check_reconciliation(
        &mut self,
        workload: &Workload,
        config_index: usize,
        config: &NamedConfig,
        result_desc: &RelationDesc,
        before: &Timeline,
    ) -> anyhow::Result<()> {
        if !workload.exports.contains(&WorkloadExport::Index) {
            return Ok(());
        }
        let ts = workload.assert_ts();
        let Some(expected) = before.get(&ts) else {
            return Ok(());
        };

        self.driver.reconnect().await?;
        let mut updates = ConfigUpdates::default();
        for setting in &config.settings {
            updates.add_dynamic(
                &setting.name,
                parse_config_val(&setting.ty, &setting.value)?,
            );
        }
        self.driver.create_instance(None, false, updates.clone())?;
        self.driver.update_configuration(updates)?;

        // Replay every export inside the reconciliation window, then close it. A
        // subscribe is not replayed: its `up_to` has already been reached, so the
        // replica has dropped it and a replay would install a second one.
        //
        // Each replayed dataflow is scheduled as well as created. A controller
        // replays both commands for everything it expects to be running, and the
        // replica holds a collection back until it is scheduled, whether or not
        // reconciliation matched it to a live dataflow. Omitting it leaves the
        // index installed and silent, which surfaces as a frontier that never
        // arrives rather than as anything naming the missing command.
        for export in &workload.exports {
            if *export == WorkloadExport::Subscribe {
                continue;
            }
            let (df, _) =
                workload.export_dataflow(config_index, *export, 0, &self.shards(config_index))?;
            self.driver.submit_dataflow(df)?;
            self.driver
                .schedule(GlobalId::User(export_id(config_index, *export)))?;
        }
        self.driver.send(ComputeCommand::InitializationComplete)?;

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
        let after = read_result(read.map(multiset_from_rows));
        if &after != expected {
            anyhow::bail!(
                "reconciliation oracle: the index read differently at ts {ts} after \
                 replaying the installed dataflows\nbefore:\n{}\nafter:\n{}",
                expected.render(),
                after.render()
            );
        }
        Ok(())
    }

    /// Allow the index to compact up to the last asserted timestamp, then read it
    /// there again and require the answer not to have changed.
    ///
    /// Compaction is the one thing a maintained arrangement does that no reference
    /// implementation models: batches merge, historical detail is discarded, and the
    /// result at the remaining frontier has to survive it. Nothing else in the suite
    /// advances a `since`, so without this the merge path is never entered.
    ///
    /// `AllowCompaction` only relaxes the read capability, so the replica is free to
    /// do the work whenever it likes. This is therefore a check that compaction does
    /// not corrupt what remains readable, not a guarantee that any compaction
    /// happened.
    async fn check_compaction(
        &self,
        workload: &Workload,
        config_index: usize,
        result_desc: &RelationDesc,
        ts: u64,
        before: &Timeline,
    ) -> anyhow::Result<()> {
        if !workload.exports.contains(&WorkloadExport::Index) {
            return Ok(());
        }
        let Some(expected) = before.get(&ts) else {
            return Ok(());
        };
        let id = GlobalId::User(ids::index(config_index));
        self.driver.send(ComputeCommand::AllowCompaction {
            id,
            frontier: Antichain::from_elem(Timestamp::from(ts)),
        })?;
        let read = self
            .driver
            .peek_result(
                PeekTarget::Index { id },
                result_desc.clone(),
                Timestamp::from(ts),
            )
            .await?;
        let after = read_result(read.map(multiset_from_rows));
        if &after != expected {
            anyhow::bail!(
                "compaction oracle: the index read differently at ts {ts} after \
                 AllowCompaction\nbefore:\n{}\nafter:\n{}",
                expected.render(),
                after.render()
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

/// The export-invariance oracle: every export of the same collection must hold the
/// same contents at every timestamp.
///
/// Needs at least two exports to say anything. With fewer it yields no verdict,
/// which is fine here (unlike the fold oracle) because the condition is visible in
/// the workload itself rather than depending on plan structure.
///
/// # The subscribe does not compare like the others
///
/// A subscribe is poisoned by the first error it observes and reports that same
/// error in every later batch, even after the error is retracted, while an index
/// and a persist sink both stop reporting an error the moment it goes away. That is
/// the protocol working as designed: a client already told the collection is in
/// error cannot be told to forget it.
///
/// So the subscribe is held to the reference *made sticky*: from the first
/// timestamp at which the reference errors, an error is expected forever after.
/// Comparing it directly would report intended behaviour as a divergence, and
/// skipping the subscribe once it errors would stop checking the export whose error
/// behaviour is the most intricate of the three.
///
/// Returns the comparisons that reached no verdict, which is only ever the
/// timestamps inside the batch that carried the error: the protocol's `Err` variant
/// has no timestamps, so when a replica batches several together, which of them
/// first errored cannot be recovered from the response.
fn check_export_invariance(
    by_export: &BTreeMap<WorkloadExport, Timeline>,
    poison: Option<&SubscribePoison>,
) -> anyhow::Result<Vec<String>> {
    // Prefer a non-subscribe reference, since the subscribe is the one carrying the
    // sticky rule.
    let Some((ref_kind, reference)) = by_export
        .iter()
        .find(|(kind, _)| **kind != WorkloadExport::Subscribe)
        .or_else(|| by_export.iter().next())
    else {
        return Ok(Vec::new());
    };

    let sticky = sticky_errors(reference);
    let mut inconclusive = Vec::new();
    for (kind, timeline) in by_export {
        if kind == ref_kind {
            continue;
        }
        let subscribe = *kind == WorkloadExport::Subscribe;
        for (ts, result) in timeline {
            let expected = if subscribe { &sticky } else { reference }
                .get(ts)
                .ok_or_else(|| {
                    anyhow::anyhow!("{ref_kind:?} has no read at ts {ts} but {kind:?} does")
                })?;
            if result == expected {
                continue;
            }
            // Inside the batch that carried the error, the subscribe cannot say
            // which timestamp the error belonged to. Tolerated only if the reference
            // really does error somewhere in that batch: a subscribe erroring where
            // the collection never does is a divergence whatever the batching.
            if subscribe
                && poison.is_some_and(|p| {
                    covers(p, *ts) && reference.iter().any(|(t, r)| covers(p, *t) && r.is_error())
                })
            {
                inconclusive.push(format!(
                    "subscribe at ts {ts}: the error arrived in a batch spanning \
                     several timestamps, so which one first errored cannot be \
                     recovered from the response"
                ));
                continue;
            }
            anyhow::bail!(
                "export invariance at ts {ts}: {kind:?} differs from {ref_kind:?}\n\
                 {ref_kind:?}:\n{}\n{kind:?}:\n{}",
                expected.render(),
                result.render()
            );
        }
    }
    Ok(inconclusive)
}

/// Whether the batch that poisoned a subscribe covers `ts`.
fn covers(poison: &SubscribePoison, ts: u64) -> bool {
    let lower = poison.lower.map_or(0, u64::from);
    let upper = poison.upper.map_or(u64::MAX, u64::from);
    lower <= ts && ts < upper
}

/// `timeline` with the first error carried forward through every later timestamp.
///
/// What a subscribe is expected to report, given that its first error poisons it.
fn sticky_errors(timeline: &Timeline) -> Timeline {
    let mut out = Timeline::new();
    let mut poison: Option<ReadResult> = None;
    for (ts, result) in timeline {
        if poison.is_none() && result.is_error() {
            poison = Some(result.clone());
        }
        out.insert(*ts, poison.clone().unwrap_or_else(|| result.clone()));
    }
    out
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
    per_config: &[(String, Timeline)],
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
    for (name, timeline) in &per_config[1..] {
        for (ts, result) in timeline {
            let expected = base.get(ts).ok_or_else(|| {
                anyhow::anyhow!("config {base_name:?} has no read at ts {ts} but {name:?} does")
            })?;
            if result != expected {
                anyhow::bail!(
                    "strategy invariance at ts {ts}: config {name:?} differs from \
                     {base_name:?}\n{base_name:?}:\n{}\n{name:?}:\n{}",
                    expected.render(),
                    result.render()
                );
            }
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
            volume: 0,
            plan: MirRelationExpr::global_get(GlobalId::User(ids::input(0)), typ),
            exports: vec![WorkloadExport::Index],
            configs,
            claims: BTreeSet::new(),
            oracles,
            optimize: false,
        }
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

        let (a, b) = (timeline_at(1), timeline_at(2));

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

    /// A single-timestamp timeline holding one row at multiplicity `diff`.
    fn timeline_at(diff: i64) -> Timeline {
        let mut row = Row::default();
        row.packer().push(mz_repr::Datum::Int64(7));
        [(
            0,
            ReadResult::Rows([(row, Diff::from(diff))].into_iter().collect()),
        )]
        .into_iter()
        .collect()
    }

    /// Export invariance catches a divergence between two exports of the same
    /// collection.
    #[mz_ore::test]
    fn export_invariance_is_live() {
        let (a, b) = (timeline_at(1), timeline_at(3));

        let same: BTreeMap<WorkloadExport, Timeline> = [
            (WorkloadExport::Index, a.clone()),
            (WorkloadExport::Subscribe, a.clone()),
        ]
        .into_iter()
        .collect();
        check_export_invariance(&same, None).expect("identical exports must pass");

        let differing: BTreeMap<WorkloadExport, Timeline> =
            [(WorkloadExport::Index, a), (WorkloadExport::Subscribe, b)]
                .into_iter()
                .collect();
        let err = check_export_invariance(&differing, None)
            .expect_err("diverging exports must be rejected");
        assert!(err.to_string().contains("export invariance"));
    }

    /// A divergence at an intermediate timestamp is caught even when the two
    /// timelines agree at the last one.
    ///
    /// This is the case the suite could not see when it read only the final state: a
    /// collection that passes through a wrong intermediate value and then converges
    /// looks identical to a correct one.
    #[mz_ore::test]
    fn invariance_catches_an_intermediate_divergence() {
        let mut row = Row::default();
        row.packer().push(mz_repr::Datum::Int64(7));
        let rows =
            |diff: i64| ReadResult::Rows([(row.clone(), Diff::from(diff))].into_iter().collect());

        let good: Timeline = [(0, rows(1)), (1, rows(2))].into_iter().collect();
        // Same answer at ts 1, wrong on the way there.
        let bad: Timeline = [(0, rows(5)), (1, rows(2))].into_iter().collect();

        let by_export: BTreeMap<WorkloadExport, Timeline> = [
            (WorkloadExport::Index, good),
            (WorkloadExport::Subscribe, bad),
        ]
        .into_iter()
        .collect();
        let err = check_export_invariance(&by_export, None)
            .expect_err("a divergence at ts 0 must be rejected");
        assert!(
            err.to_string().contains("at ts 0"),
            "the failure must name the timestamp, got: {err}"
        );
    }
}
