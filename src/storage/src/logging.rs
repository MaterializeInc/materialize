// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection events that map storage dataflows and their operators to catalog objects.
//!
//! Storage logs to the Timely logger named [`LOGGER_NAME`] on the worker it renders into. The
//! host of the Timely workers registers that logger and turns the events into introspection
//! collections. When no logger is registered, [`StageLogger`] is a no-op.
//!
//! A stage is a range of operator ids, bracketed with [`Worker::peek_identifier`] around the
//! rendering code that creates the stage's operators. A stage rendered in several places logs
//! one [`StageMapping`] per range, all with the same `stage_id`. Operators rendered outside any
//! bracket belong to no stage.
//!
//! Stage ids are positions in a fixed pre-order layout of the dataflow's stage tree: the shared
//! stages, then for each export, in export id order, its [`Stage::Export`] stage followed by its
//! child stages. Ordering by `stage_id` yields the tree top-down with every parent before its
//! children, and every worker assigns the same ids. Positions of stages a dataflow does not
//! render are skipped, so ids have gaps.
//!
//! The tree has depth two. Shared stages and [`Stage::Export`] stages have nesting 1 and no
//! parent, and every other per-export stage has nesting 2 and its export's `Export` stage as
//! parent. The `EXPLAIN ANALYZE` SQL rewrite for storage objects relies on this shape, on
//! [`Stage::Remap`] having `stage_id` 0, and on the `Export` stage name.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;

use columnar::Columnar;
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_repr::GlobalId;
use mz_timely_util::columnar::builder::ColumnBuilder;
use timely::worker::Worker;

/// The name of the Timely logger that receives [`StorageEvent`]s.
pub const LOGGER_NAME: &str = "materialize/storage";

/// Container builder for the storage logger.
pub type StorageEventBuilder = ColumnBuilder<(Duration, StorageEvent)>;

/// A logger of storage events.
pub type Logger = timely::logging_core::Logger<StorageEventBuilder>;

/// Announces that a dataflow renders a `GlobalId`.
#[derive(Debug, Clone, PartialOrd, PartialEq, Columnar)]
pub struct DataflowGlobal {
    /// Timely worker index of the dataflow.
    pub dataflow_index: usize,
    /// A `GlobalId` that is rendered as part of this dataflow.
    pub global_id: GlobalId,
}

/// Announces that a range of a dataflow's operators belongs to a rendering stage of an object.
#[derive(Debug, Clone, PartialOrd, PartialEq, Columnar)]
pub struct StageMapping {
    /// Timely worker index of the dataflow.
    pub dataflow_index: usize,
    /// The object the stage belongs to. Shared stages belong to the dataflow's primary id,
    /// per-export stages to the export's id.
    pub global_id: GlobalId,
    /// The stage's position in the dataflow's stage tree, unique within the dataflow.
    pub stage_id: u64,
    /// The `stage_id` of the enclosing stage, if any.
    pub parent_stage_id: Option<u64>,
    /// Depth in the stage tree, 1 for top-level stages.
    pub nesting: u8,
    /// Human-readable stage name.
    pub stage: String,
    /// First operator id of the range, inclusive.
    pub operator_id_start: usize,
    /// End of the range, exclusive. Equal to `operator_id_start` for a stage that only
    /// aggregates its children.
    pub operator_id_end: usize,
}

/// All operators of a dataflow have shut down.
#[derive(Debug, Clone, PartialOrd, PartialEq, Columnar)]
pub struct DataflowShutdown {
    /// Timely worker index of the dataflow.
    pub dataflow_index: usize,
}

/// A logged storage event.
///
/// Storage never retracts what it logs. A consumer retracts every [`DataflowGlobal`] and
/// [`StageMapping`] of a dataflow when it observes [`StorageEvent::DataflowShutdown`] for that
/// dataflow's index. Keying retractions by dataflow index keeps them correct when a restarted
/// ingestion renders the same ids in a new dataflow before the old one has shut down.
#[derive(Debug, Clone, PartialOrd, PartialEq, Columnar)]
pub enum StorageEvent {
    /// A dataflow renders a `GlobalId`.
    DataflowGlobal(DataflowGlobal),
    /// A range of operators belongs to a stage.
    StageMapping(StageMapping),
    /// A dataflow has shut down. Logged by the host's Timely logging, which is the component
    /// that observes operator shutdown.
    DataflowShutdown(DataflowShutdown),
}

/// A rendering stage of a storage dataflow.
///
/// Ingestions use the shared stages under their primary id, and [`Stage::Export`] with the
/// ingestion child stages under each export id. Sinks use [`Stage::Export`] with the sink child
/// stages under the sink id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Shared: minting and following the remap bindings.
    Remap,
    /// Shared: reading upstream, for connectors that do not separate snapshot and replication.
    Reader,
    /// Shared: reading the upstream snapshot.
    ReaderSnapshot,
    /// Shared: reading the upstream replication stream.
    ReaderReplication,
    /// Shared: demultiplexing reader output into exports.
    Partition,
    /// Shared: collecting and reporting health status.
    Healthcheck,
    /// The root of an export's stages.
    Export,
    /// Ingestion child: reclocking into the `mz_repr::Timestamp` domain.
    Reclock,
    /// Ingestion child: decoding keys and values.
    Decode,
    /// Ingestion child: applying the envelope.
    Envelope(EnvelopeKind),
    /// Ingestion child: writing the export's shard.
    PersistSink,
    /// Sink child: arranging the sink's input by key.
    Arrange,
    /// Sink child: encoding updates for the sink's destination.
    Encode,
    /// Sink child: writing to the sink's destination.
    Sink,
}

/// The envelope applied by a [`Stage::Envelope`] stage, named after its SQL keyword.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeKind {
    /// `ENVELOPE NONE`.
    None,
    /// `ENVELOPE UPSERT`.
    Upsert,
    /// `ENVELOPE DEBEZIUM`.
    Debezium,
    /// `ENVELOPE MATERIALIZE`.
    Materialize,
}

/// Number of shared stage positions preceding the first export.
const SHARED_SLOTS: u64 = 6;
/// Number of stage positions per export.
const EXPORT_SLOTS: u64 = 5;

/// The position of a stage in its dataflow's stage tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StagePosition {
    stage_id: u64,
    parent_stage_id: Option<u64>,
    nesting: u8,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Stage::Remap => "Remap",
            Stage::Reader => "Reader",
            Stage::ReaderSnapshot => "Reader::Snapshot",
            Stage::ReaderReplication => "Reader::Replication",
            Stage::Partition => "Partition",
            Stage::Healthcheck => "Healthcheck",
            Stage::Export => "Export",
            Stage::Reclock => "Reclock",
            Stage::Decode => "Decode",
            Stage::Envelope(EnvelopeKind::None) => "Envelope NONE",
            Stage::Envelope(EnvelopeKind::Upsert) => "Envelope UPSERT",
            Stage::Envelope(EnvelopeKind::Debezium) => "Envelope DEBEZIUM",
            Stage::Envelope(EnvelopeKind::Materialize) => "Envelope MATERIALIZE",
            Stage::PersistSink => "PersistSink",
            Stage::Arrange => "Arrange",
            Stage::Encode => "Encode",
            Stage::Sink => "Sink",
        }
    }

    /// The position of this stage as a shared stage, when `export_index` is `None`, or as a
    /// stage of the export at `export_index` in export id order. Returns `None` when the stage
    /// is not of that kind.
    fn position(self, export_index: Option<usize>) -> Option<StagePosition> {
        match export_index {
            None => self.shared_slot().map(|stage_id| StagePosition {
                stage_id,
                parent_stage_id: None,
                nesting: 1,
            }),
            Some(index) => {
                let export_stage_id = SHARED_SLOTS + u64::cast_from(index) * EXPORT_SLOTS;
                let slot = self.export_slot()?;
                let (parent_stage_id, nesting) = match self {
                    Stage::Export => (None, 1),
                    _ => (Some(export_stage_id), 2),
                };
                Some(StagePosition {
                    stage_id: export_stage_id + slot,
                    parent_stage_id,
                    nesting,
                })
            }
        }
    }

    /// Position among the shared stages, or `None` for a per-export stage.
    fn shared_slot(self) -> Option<u64> {
        match self {
            Stage::Remap => Some(0),
            Stage::Reader => Some(1),
            Stage::ReaderSnapshot => Some(2),
            Stage::ReaderReplication => Some(3),
            Stage::Partition => Some(4),
            Stage::Healthcheck => Some(5),
            Stage::Export
            | Stage::Reclock
            | Stage::Decode
            | Stage::Envelope(_)
            | Stage::PersistSink
            | Stage::Arrange
            | Stage::Encode
            | Stage::Sink => None,
        }
    }

    /// Position within an export's stages, or `None` for a shared stage. Ingestion and sink
    /// children reuse positions because a dataflow is only ever one of the two.
    fn export_slot(self) -> Option<u64> {
        match self {
            Stage::Export => Some(0),
            Stage::Reclock | Stage::Arrange => Some(1),
            Stage::Decode | Stage::Encode => Some(2),
            Stage::Envelope(_) | Stage::Sink => Some(3),
            Stage::PersistSink => Some(4),
            Stage::Remap
            | Stage::Reader
            | Stage::ReaderSnapshot
            | Stage::ReaderReplication
            | Stage::Partition
            | Stage::Healthcheck => None,
        }
    }
}

/// Handle for logging the objects and stages of one storage dataflow.
///
/// Cloning is cheap and clones log to the same dataflow. The default value logs nothing.
#[derive(Clone, Default)]
pub struct StageLogger {
    inner: Option<Rc<StageLoggerInner>>,
}

struct StageLoggerInner {
    logger: Logger,
    dataflow_index: usize,
    primary_id: GlobalId,
    /// The index of each export in export id order.
    export_indexes: BTreeMap<GlobalId, usize>,
    /// Whether a bracket is open. Brackets must not nest, or the inner range would be attributed
    /// to both stages.
    bracket_open: Cell<bool>,
}

impl StageLogger {
    /// Logs a [`DataflowGlobal`] for `primary_id` and every export, and the [`Stage::Export`]
    /// stage of every export, if `worker` has a storage logger.
    ///
    /// Must be called on every worker with the same arguments, before rendering the dataflow's
    /// operators.
    pub fn new(
        worker: &Worker,
        dataflow_index: usize,
        primary_id: GlobalId,
        export_ids: impl IntoIterator<Item = GlobalId>,
    ) -> Self {
        let Some(logger) = worker.logger_for::<StorageEventBuilder>(LOGGER_NAME) else {
            return Self::default();
        };

        let export_ids: BTreeSet<_> = export_ids.into_iter().collect();
        let global_ids: BTreeSet<_> = export_ids.iter().copied().chain([primary_id]).collect();
        for global_id in global_ids {
            logger.log(&StorageEvent::DataflowGlobal(DataflowGlobal {
                dataflow_index,
                global_id,
            }));
        }

        let export_indexes: BTreeMap<_, _> = export_ids
            .into_iter()
            .enumerate()
            .map(|(index, id)| (id, index))
            .collect();

        // `Export` stages have operators of their own only where rendering brackets them. The
        // empty-range rows make every export's root present regardless.
        let operator_id = worker.peek_identifier();
        for (&global_id, &index) in &export_indexes {
            let position = Stage::Export
                .position(Some(index))
                .expect("Export is a per-export stage");
            logger.log(&StorageEvent::StageMapping(StageMapping {
                dataflow_index,
                global_id,
                stage_id: position.stage_id,
                parent_stage_id: position.parent_stage_id,
                nesting: position.nesting,
                stage: Stage::Export.name().to_owned(),
                operator_id_start: operator_id,
                operator_id_end: operator_id,
            }));
        }

        Self {
            inner: Some(Rc::new(StageLoggerInner {
                logger,
                dataflow_index,
                primary_id,
                export_indexes,
                bracket_open: Cell::new(false),
            })),
        }
    }

    /// Runs `render` and attributes the operators it creates to the shared `stage`.
    pub fn shared<R>(&self, worker: &Worker, stage: Stage, render: impl FnOnce() -> R) -> R {
        let Some(inner) = &self.inner else {
            return render();
        };
        let Some(position) = stage.position(None) else {
            soft_panic_or_log!("{stage:?} is not a shared storage stage");
            return render();
        };
        inner.bracket(worker, inner.primary_id, position, stage, render)
    }

    /// Runs `render` and attributes the operators it creates to `stage` of `export_id`.
    pub fn export<R>(
        &self,
        worker: &Worker,
        export_id: GlobalId,
        stage: Stage,
        render: impl FnOnce() -> R,
    ) -> R {
        let Some(inner) = &self.inner else {
            return render();
        };
        let Some(&index) = inner.export_indexes.get(&export_id) else {
            soft_panic_or_log!("{export_id} is not an export of this storage dataflow");
            return render();
        };
        let Some(position) = stage.position(Some(index)) else {
            soft_panic_or_log!("{stage:?} is not a per-export storage stage");
            return render();
        };
        inner.bracket(worker, export_id, position, stage, render)
    }
}

impl StageLoggerInner {
    fn bracket<R>(
        &self,
        worker: &Worker,
        global_id: GlobalId,
        position: StagePosition,
        stage: Stage,
        render: impl FnOnce() -> R,
    ) -> R {
        let was_open = self.bracket_open.replace(true);
        if was_open {
            soft_panic_or_log!("storage stage bracket for {stage:?} nested in another bracket");
        }
        let operator_id_start = worker.peek_identifier();
        let result = render();
        let operator_id_end = worker.peek_identifier();
        self.bracket_open.set(was_open);

        if operator_id_start != operator_id_end {
            self.logger.log(&StorageEvent::StageMapping(StageMapping {
                dataflow_index: self.dataflow_index,
                global_id,
                stage_id: position.stage_id,
                parent_stage_id: position.parent_stage_id,
                nesting: position.nesting,
                stage: stage.name().to_owned(),
                operator_id_start,
                operator_id_end,
            }));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const SHARED: [Stage; 6] = [
        Stage::Remap,
        Stage::Reader,
        Stage::ReaderSnapshot,
        Stage::ReaderReplication,
        Stage::Partition,
        Stage::Healthcheck,
    ];
    const INGESTION_EXPORT: [Stage; 8] = [
        Stage::Export,
        Stage::Reclock,
        Stage::Decode,
        Stage::Envelope(EnvelopeKind::None),
        Stage::Envelope(EnvelopeKind::Upsert),
        Stage::Envelope(EnvelopeKind::Debezium),
        Stage::Envelope(EnvelopeKind::Materialize),
        Stage::PersistSink,
    ];
    const SINK_EXPORT: [Stage; 4] = [Stage::Export, Stage::Arrange, Stage::Encode, Stage::Sink];

    /// Checks the stage tree of a dataflow with `exports` exports whose per-export stages are
    /// `export_stages`, and returns the position of every stage by `stage_id`.
    fn check_layout(exports: usize, export_stages: &[Stage]) -> BTreeMap<u64, StagePosition> {
        let mut by_id: BTreeMap<u64, (Stage, Option<usize>, StagePosition)> = BTreeMap::new();
        let mut insert = |stage: Stage, index: Option<usize>, position: StagePosition| {
            if let Some((other, other_index, _)) = by_id.get(&position.stage_id) {
                // Envelope kinds are alternatives for the same position.
                let alternatives =
                    matches!((stage, *other), (Stage::Envelope(_), Stage::Envelope(_)))
                        && index == *other_index;
                assert!(alternatives, "{stage:?} and {other:?} share a stage id");
            }
            by_id.insert(position.stage_id, (stage, index, position));
        };

        for stage in SHARED {
            let position = stage.position(None).expect("shared stage");
            assert_eq!(stage.position(Some(0)), None, "{stage:?} is not per-export");
            assert_eq!((position.parent_stage_id, position.nesting), (None, 1));
            insert(stage, None, position);
        }
        for index in 0..exports {
            for &stage in export_stages {
                let position = stage.position(Some(index)).expect("per-export stage");
                assert_eq!(stage.position(None), None, "{stage:?} is not shared");
                insert(stage, Some(index), position);
            }
        }

        let export_ids: BTreeMap<usize, u64> = by_id
            .values()
            .filter(|(stage, _, _)| *stage == Stage::Export)
            .map(|(_, index, position)| (index.expect("export index"), position.stage_id))
            .collect();
        let mut previous_export = None;
        for (stage, index, position) in by_id.values() {
            match index {
                None => assert!(export_ids.values().all(|&e| e > position.stage_id)),
                Some(index) => {
                    assert!(previous_export <= Some(*index), "export stages interleave");
                    previous_export = Some(*index);
                    let export = export_ids[index];
                    if *stage == Stage::Export {
                        assert_eq!((position.parent_stage_id, position.nesting), (None, 1));
                    } else {
                        assert_eq!(position.parent_stage_id, Some(export));
                        assert_eq!(position.nesting, 2);
                        assert!(export < position.stage_id, "parent precedes child");
                    }
                }
            }
        }
        by_id.into_iter().map(|(id, (_, _, p))| (id, p)).collect()
    }

    #[mz_ore::test]
    fn stage_layout_is_a_preorder_tree_of_depth_two() {
        for exports in [1, 2, 5] {
            check_layout(exports, &INGESTION_EXPORT);
            check_layout(exports, &SINK_EXPORT);
        }
        assert_eq!(Stage::Remap.position(None).expect("shared").stage_id, 0);
        assert_eq!(Stage::Export.name(), "Export");
    }
}
