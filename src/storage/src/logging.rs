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

impl Stage {
    fn name(self) -> String {
        match self {
            Stage::Remap => "Remap".into(),
            Stage::Reader => "Reader".into(),
            Stage::ReaderSnapshot => "Reader::Snapshot".into(),
            Stage::ReaderReplication => "Reader::Replication".into(),
            Stage::Partition => "Partition".into(),
            Stage::Healthcheck => "Healthcheck".into(),
            Stage::Export => "Export".into(),
            Stage::Reclock => "Reclock".into(),
            Stage::Decode => "Decode".into(),
            Stage::Envelope(kind) => {
                let kind = match kind {
                    EnvelopeKind::None => "NONE",
                    EnvelopeKind::Upsert => "UPSERT",
                    EnvelopeKind::Debezium => "DEBEZIUM",
                    EnvelopeKind::Materialize => "MATERIALIZE",
                };
                format!("Envelope {kind}")
            }
            Stage::PersistSink => "PersistSink".into(),
            Stage::Arrange => "Arrange".into(),
            Stage::Encode => "Encode".into(),
            Stage::Sink => "Sink".into(),
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
    /// The `stage_id` of each export's [`Stage::Export`] stage.
    export_stage_ids: BTreeMap<GlobalId, u64>,
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

        let export_stage_ids: BTreeMap<_, _> = export_ids
            .into_iter()
            .enumerate()
            .map(|(idx, id)| (id, SHARED_SLOTS + u64::cast_from(idx) * EXPORT_SLOTS))
            .collect();

        // `Export` stages have operators of their own only where rendering brackets them. The
        // empty-range rows make every export's root present regardless.
        let operator_id = worker.peek_identifier();
        for (&global_id, &stage_id) in &export_stage_ids {
            logger.log(&StorageEvent::StageMapping(StageMapping {
                dataflow_index,
                global_id,
                stage_id,
                parent_stage_id: None,
                nesting: 1,
                stage: Stage::Export.name(),
                operator_id_start: operator_id,
                operator_id_end: operator_id,
            }));
        }

        Self {
            inner: Some(Rc::new(StageLoggerInner {
                logger,
                dataflow_index,
                primary_id,
                export_stage_ids,
                bracket_open: Cell::new(false),
            })),
        }
    }

    /// Runs `render` and attributes the operators it creates to the shared `stage`.
    pub fn shared<R>(&self, worker: &Worker, stage: Stage, render: impl FnOnce() -> R) -> R {
        let Some(inner) = &self.inner else {
            return render();
        };
        let Some(slot) = stage.shared_slot() else {
            soft_panic_or_log!("{stage:?} is not a shared storage stage");
            return render();
        };
        inner.bracket(worker, inner.primary_id, slot, None, 1, stage, render)
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
        let Some(slot) = stage.export_slot() else {
            soft_panic_or_log!("{stage:?} is not a per-export storage stage");
            return render();
        };
        let Some(&export_stage_id) = inner.export_stage_ids.get(&export_id) else {
            soft_panic_or_log!("{export_id} is not an export of this storage dataflow");
            return render();
        };
        let (parent_stage_id, nesting) = match stage {
            Stage::Export => (None, 1),
            _ => (Some(export_stage_id), 2),
        };
        inner.bracket(
            worker,
            export_id,
            export_stage_id + slot,
            parent_stage_id,
            nesting,
            stage,
            render,
        )
    }
}

impl StageLoggerInner {
    fn bracket<R>(
        &self,
        worker: &Worker,
        global_id: GlobalId,
        stage_id: u64,
        parent_stage_id: Option<u64>,
        nesting: u8,
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
                stage_id,
                parent_stage_id,
                nesting,
                stage: stage.name(),
                operator_id_start,
                operator_id_end,
            }));
        }
        result
    }
}
