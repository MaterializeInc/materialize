// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::hashable::Hashable;
use mz_ore::cast::CastFrom;
use mz_repr::{GlobalId, Timestamp};
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::logging::compute::{ComputeEvent, Lifecycle, LifecycleStage, Logger as ComputeLogger};

mod copy_to_s3_oneshot;
#[cfg(feature = "bench")]
pub mod correction;
#[cfg(not(feature = "bench"))]
mod correction;
#[cfg(feature = "bench")]
pub mod correction_v2;
#[cfg(not(feature = "bench"))]
mod correction_v2;
mod materialized_view;
mod materialized_view_v2;
mod metric_sink;
mod refresh;
mod subscribe;

/// The worker that maintains a persist sink's shared write frontier.
///
/// The `mint` operator tracks the output shard's upper on this worker alone and clears the shared
/// frontier on all the others, so only this worker's copy carries write progress. The election is
/// private to the sink: the frontier leaves it as an input to the controller-visible meet, which
/// needs no owner, and the stages that do need one are reported from `mint` itself.
fn frontier_owner(sink_id: GlobalId, peers: usize) -> usize {
    usize::cast_from(sink_id.hashed()) % peers
}

/// Reports the write lifecycle stages of a persist sink.
///
/// Only the worker that mints batch descriptions tracks the output shard's upper, so it is the
/// only worker that can report these stages. That is what makes them one report per sink rather
/// than one per worker, and it is why this is held by `mint` rather than by the collection.
///
/// Each stage is reported at most once. The demux deduplicates as well, so this only keeps
/// repeated observations off the logging channel.
struct WriteStageLogger {
    export_id: GlobalId,
    /// The as-of `written` is measured against.
    as_of: Antichain<Timestamp>,
    /// `None` when compute logging is disabled.
    logger: Option<ComputeLogger>,
    blocked: bool,
    written: bool,
}

impl WriteStageLogger {
    fn new(
        export_id: GlobalId,
        as_of: Antichain<Timestamp>,
        logger: Option<ComputeLogger>,
    ) -> Self {
        Self {
            export_id,
            as_of,
            logger,
            blocked: false,
            written: false,
        }
    }

    /// Report that there is a batch to mint and read-only mode forbids writing it.
    ///
    /// The caller decides what makes a block worth reporting. Reporting every read-only
    /// observation would put a block on essentially every materialized view, since collections
    /// start read-only and the controller releases them.
    fn blocked(&mut self) {
        if !self.blocked {
            self.blocked = true;
            self.log(LifecycleStage::WriteBlockedReadOnly);
        }
    }

    /// Report that writing is now permitted, if we reported it blocked. A sink that was never
    /// seen to wait has nothing to report here.
    fn unblocked(&mut self) {
        if self.blocked {
            self.log(LifecycleStage::WriteUnblocked);
        }
    }

    /// Report `written` once the shard's upper passes the as-of.
    ///
    /// NOTE: this says the output is durable through the as-of, not that this replica wrote it.
    /// Every replica reads the same upper back from persist, so it advances on all of them when
    /// any one wins the append.
    fn observe_persist_frontier(&mut self, frontier: &Antichain<Timestamp>) {
        if !self.written && PartialOrder::less_than(&self.as_of, frontier) {
            self.written = true;
            self.log(LifecycleStage::Written);
        }
    }

    fn log(&self, stage: LifecycleStage) {
        if let Some(logger) = &self.logger {
            logger.log(&ComputeEvent::Lifecycle(Lifecycle {
                export_id: self.export_id,
                stage,
            }));
        }
    }
}
