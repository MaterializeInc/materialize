// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflow for the resource usage of the replica's processes.
//!
//! The observations are read by `mz_metrics::usage`, on a task that keeps sampling while the
//! timely workers are busy. This dataflow only reports them, so a worker that stalls delays the
//! report without losing an observation: the values it reads are either kernel-maintained
//! high-water marks or the sampler's own folded peaks, neither of which a late read can miss.
//!
//! One row per `(process_id, source, metric)`, so a metric that moves every sample does not drag
//! the stable ones through a retraction with it.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use mz_metrics::usage::{MetricKey, observations};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::{Datum, Timestamp};
use mz_row_spine::RowRowBuilder;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, batcher, columnar_exchange};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

use crate::extensions::arrange::MzArrangeCore;
use crate::logging::{
    ComputeLog, LogCollection, LogVariant, PermutedRowPacker, downgrade_to_interval_boundary,
    emit_snapshot_diff,
};
use crate::typedefs::RowRowSpine;

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

/// Constructs the logging dataflow fragment for process resource usage.
pub(super) fn construct(
    scope: Scope<'_, Timestamp>,
    config: &mz_compute_client::logging::LoggingConfig,
    now: Instant,
    start_offset: Duration,
    workers_per_process: usize,
) -> Return {
    let variant = LogVariant::Compute(ComputeLog::ResourceUsage);
    let mut collections = BTreeMap::new();
    let interval_ms = std::cmp::max(1, config.interval.as_millis());

    if !config.index_logs.contains_key(&variant) {
        return Return { collections };
    }

    let process_id = scope.index() / workers_per_process;
    let enable = scope.index() % workers_per_process == 0;

    let mut builder = OperatorBuilder::new("ResourceUsage".to_string(), scope.clone());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::<_, ColumnBuilder<_>>::from(output);

    let operator_info = builder.operator_info();
    builder.build(move |capabilities| {
        // Usage is per-process, so only one worker per process reports it. Drop the capability for
        // disabled workers so the frontier can advance without this operator holding it back.
        let mut cap = enable.then_some(capabilities.into_element());
        let activator = scope.activator_for(operator_info.address);

        let mut prev: BTreeMap<MetricKey, u64> = BTreeMap::new();
        let mut packer = PermutedRowPacker::new(ComputeLog::ResourceUsage);

        move |_frontiers| {
            let Some(cap) = &mut cap else { return };

            // The capability is downgraded on this operator's own timer rather than on the
            // sampler's, so a sampler that stops ticking cannot freeze this collection's frontier.
            let ts =
                downgrade_to_interval_boundary(cap, &activator, now, start_offset, interval_ms);

            let current = observations().unwrap_or_default();
            if prev == current {
                return;
            }

            let mut output = output.activate();
            let mut session = output.session_with_builder(&cap);
            emit_snapshot_diff(
                &mut session,
                &mut packer,
                &prev,
                &current,
                ts,
                |packer, key, value| pack_row(packer, process_id, *key, *value),
            );

            prev = current;
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
        >(exchange, "Arrange ResourceUsage")
        .trace;
    let token: Rc<dyn std::any::Any> = Rc::new(());
    let collection = LogCollection { trace, token };
    collections.insert(variant, collection);

    Return { collections }
}

/// Pack one observation into key/value row pairs.
fn pack_row(
    packer: &mut PermutedRowPacker,
    process_id: usize,
    (source, metric): MetricKey,
    value: u64,
) -> (&mz_repr::RowRef, &mz_repr::RowRef) {
    packer.pack_by_index(|row_packer, index| match index {
        0 => row_packer.push(Datum::UInt64(u64::cast_from(process_id))),
        1 => row_packer.push(Datum::String(source)),
        2 => row_packer.push(Datum::String(metric)),
        3 => row_packer.push(Datum::UInt64(value)),
        _ => unreachable!("unexpected column index {index}"),
    })
}
