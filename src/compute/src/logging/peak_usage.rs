// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflow for peak resource usage of the replica's processes.
//!
//! The peaks themselves are measured by `mz_metrics::usage`, on a task that keeps sampling while
//! the timely workers are busy. This dataflow only reports them, so a worker that stalls delays
//! the report but cannot cause a peak to go unrecorded.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use mz_metrics::usage::{PeakUsage, peak_usage};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, Diff, Timestamp};
use mz_row_spine::RowRowBuilder;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, batcher, columnar_exchange};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::{ComputeLog, LogCollection, LogVariant, PermutedRowPacker};
use crate::typedefs::RowRowSpine;

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

/// Constructs the logging dataflow fragment for peak resource usage.
pub(super) fn construct(
    scope: Scope<'_, Timestamp>,
    config: &mz_compute_client::logging::LoggingConfig,
    now: Instant,
    start_offset: Duration,
    workers_per_process: usize,
) -> Return {
    let variant = LogVariant::Compute(ComputeLog::PeakUsage);
    let mut collections = BTreeMap::new();
    let interval_ms = std::cmp::max(1, config.interval.as_millis());

    if !config.index_logs.contains_key(&variant) {
        return Return { collections };
    }

    let process_id = scope.index() / workers_per_process;
    let enable = scope.index() % workers_per_process == 0;

    let mut builder = OperatorBuilder::new("PeakUsage".to_string(), scope.clone());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::<_, ColumnBuilder<_>>::from(output);

    let operator_info = builder.operator_info();
    builder.build(move |capabilities| {
        // Peaks are per-process, so only one worker per process reports them. Drop the capability
        // for disabled workers so the frontier can advance without this operator holding it back.
        let mut cap = enable.then_some(capabilities.into_element());
        let activator = scope.activator_for(operator_info.address);

        let mut prev: Option<PeakUsage> = None;
        let mut packer = PermutedRowPacker::new(ComputeLog::PeakUsage);

        move |_frontiers| {
            let Some(cap) = &mut cap else { return };

            // Advance the capability to the next logging interval boundary, and schedule the next
            // activation there, so the output frontier progresses at the logging rate without
            // drifting from wall-clock elapsed time.
            let elapsed = now.elapsed().as_millis();
            let time_ms: u128 =
                ((elapsed + start_offset.as_millis()) / interval_ms + 1) * interval_ms;
            let ts: Timestamp = time_ms.try_into().expect("must fit");
            cap.downgrade(&ts);

            let next_boundary_ms = time_ms - start_offset.as_millis();
            let next_activation =
                now + Duration::from_millis(next_boundary_ms.try_into().expect("must fit"));
            activator.activate_after(next_activation.saturating_duration_since(Instant::now()));

            let current = peak_usage();
            if prev == Some(current) {
                return;
            }

            let mut output = output.activate();
            let mut session = output.session_with_builder(&cap);

            if let Some(prev) = prev {
                let (key, val) = pack_row(&mut packer, process_id, prev);
                session.give(((key, val), ts, Diff::MINUS_ONE));
            }
            let (key, val) = pack_row(&mut packer, process_id, current);
            session.give(((key, val), ts, Diff::ONE));

            prev = Some(current);
        }
    });

    let exchange = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
        columnar_exchange::<mz_repr::Row, mz_repr::Row, Timestamp, mz_repr::Diff>,
    );
    let trace = stream
        .mz_arrange_core::<
            _,
            batcher::Chunker<_>,
            Col2ValBatcher<_, _, _, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >(exchange, "Arrange PeakUsage")
        .trace;
    let token: Rc<dyn std::any::Any> = Rc::new(());
    let collection = LogCollection { trace, token };
    collections.insert(variant, collection);

    Return { collections }
}

/// Pack a peak usage row into key/value row pairs.
fn pack_row(
    packer: &mut PermutedRowPacker,
    process_id: usize,
    usage: PeakUsage,
) -> (&mz_repr::RowRef, &mz_repr::RowRef) {
    let datum = |bytes: Option<u64>| match bytes {
        Some(bytes) => Datum::UInt64(bytes),
        None => Datum::Null,
    };

    packer.pack_by_index(|row_packer, index| match index {
        0 => row_packer.push(Datum::UInt64(u64::cast_from(process_id))),
        1 => row_packer.push(datum(usage.memory_bytes)),
        2 => row_packer.push(datum(usage.heap_bytes)),
        3 => row_packer.push(datum(usage.disk_bytes)),
        _ => unreachable!("unexpected column index {index}"),
    })
}
