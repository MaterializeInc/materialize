// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPIKE: logging dataflow for the sampled live heap, attributed to timely operators.
//!
//! Every sampled allocation carries a tag naming the worker and the operator that the allocating
//! thread was scheduling, read from timely's current-operator thread-local. One worker per process
//! snapshots the process-wide tracker on a timer, and reports the change since the last snapshot.
//! Stacks are symbolized once per process and identified by a hash of their symbol names, so the
//! same stack has the same identifier in every process despite address space randomization.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::time::{Duration, Instant};

use mz_compute_client::logging::LoggingConfig;
use mz_ore::cast::{CastFrom, CastLossy};
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

/// How often the reporting worker snapshots the tracker.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(10);

thread_local! {
    // Read from inside the allocator, so const-initialized and without `Drop`.
    static WORKER: Cell<u64> = const { Cell::new(0) };
}

/// Tags a sample with `(worker + 1) << 32 | (operator + 1)`, or zero outside an operator.
fn tag() -> u64 {
    let worker = WORKER.with(Cell::get);
    match timely::scheduling::current::operator_id() {
        Some(op) if worker > 0 => (worker << 32) | (u64::cast_from(op) + 1),
        _ => 0,
    }
}

/// Splits a tag into the worker and operator identifiers it names.
fn untag(tag: u64) -> (Option<u64>, Option<u64>) {
    let worker = (tag >> 32).checked_sub(1);
    let op = (tag & 0xffff_ffff).checked_sub(1);
    match (worker, op) {
        (Some(w), Some(o)) => (Some(w), Some(o)),
        _ => (None, None),
    }
}

/// The return type of [`construct`].
pub(super) struct Return {
    /// Collections to export.
    pub collections: BTreeMap<LogVariant, LogCollection>,
}

type LiveKey = (Option<u64>, Option<u64>, &'static str, u64);

/// Constructs the logging dataflow fragment for the sampled heap.
pub(super) fn construct(
    scope: Scope<'_, Timestamp>,
    config: &LoggingConfig,
    now: Instant,
    start_offset: Duration,
    workers_per_process: usize,
) -> Return {
    let live_variant = LogVariant::Compute(ComputeLog::HeapLive);
    let stacks_variant = LogVariant::Compute(ComputeLog::HeapStacks);
    let mut collections = BTreeMap::new();
    let interval_ms = std::cmp::max(1, config.interval.as_millis());

    // Every worker tags its allocations, whether or not it reports.
    WORKER.with(|w| w.set(u64::cast_from(scope.index()) + 1));
    mz_alloc_track::set_tag_source(tag);

    if !config.index_logs.contains_key(&live_variant) {
        return Return { collections };
    }

    let process_id = u64::cast_from(scope.index() / workers_per_process);
    let enable = scope.index() % workers_per_process == 0;

    let mut builder = OperatorBuilder::new("HeapLive".to_string(), scope.clone());
    let (live_output, live_stream) = builder.new_output();
    let mut live_output = OutputBuilder::<_, ColumnBuilder<_>>::from(live_output);
    let (stacks_output, stacks_stream) = builder.new_output();
    let mut stacks_output = OutputBuilder::<_, ColumnBuilder<_>>::from(stacks_output);

    let operator_info = builder.operator_info();
    builder.build(move |capabilities| {
        let mut caps = enable.then_some(capabilities);
        let activator = scope.activator_for(operator_info.address);

        let mut prev_live: BTreeMap<LiveKey, (u64, u64)> = BTreeMap::new();
        let mut prev_stacks: BTreeMap<u64, Rc<str>> = BTreeMap::new();
        // Symbolized stacks by frames. Never pruned, which a spike can afford.
        let mut symbols: BTreeMap<Box<[usize]>, (u64, Rc<str>)> = BTreeMap::new();
        let mut last_snapshot: Option<Instant> = None;
        let mut live_packer = PermutedRowPacker::new(ComputeLog::HeapLive);
        let mut stacks_packer = PermutedRowPacker::new(ComputeLog::HeapStacks);

        move |_frontiers| {
            let Some(caps) = &mut caps else { return };
            let mut ts = Timestamp::default();
            for cap in caps.iter_mut() {
                ts =
                    downgrade_to_interval_boundary(cap, &activator, now, start_offset, interval_ms);
            }
            if last_snapshot.is_some_and(|t| t.elapsed() < SNAPSHOT_INTERVAL) {
                return;
            }
            last_snapshot = Some(Instant::now());

            let snapshot = mz_alloc_track::snapshot();
            let ids: Vec<(u64, Rc<str>)> = snapshot
                .stacks
                .iter()
                .map(|frames| {
                    symbols
                        .entry(frames.clone())
                        .or_insert_with(|| symbolize(frames))
                        .clone()
                })
                .collect();

            let mut live: BTreeMap<LiveKey, (u64, u64)> = BTreeMap::new();
            let mut stacks: BTreeMap<u64, Rc<str>> = BTreeMap::new();
            for site in &snapshot.live {
                let (stack_id, frames) = &ids[usize::cast_from(site.stack)];
                let (worker, op) = untag(site.tag);
                let entry = live
                    .entry((worker, op, site.space.name(), *stack_id))
                    .or_default();
                entry.0 += u64::cast_lossy(site.bytes.round());
                entry.1 += u64::cast_lossy(site.count.round());
                stacks.insert(*stack_id, Rc::clone(frames));
            }

            let [live_cap, stacks_cap] = &caps[..] else {
                unreachable!("two outputs")
            };
            let mut live_output = live_output.activate();
            let mut session = live_output.session_with_builder(live_cap);
            emit_snapshot_diff(
                &mut session,
                &mut live_packer,
                &prev_live,
                &live,
                ts,
                |packer, &(worker, op, space, stack_id), &(bytes, count)| {
                    packer.pack_slice(&[
                        Datum::UInt64(process_id),
                        worker.map_or(Datum::Null, Datum::UInt64),
                        op.map_or(Datum::Null, Datum::UInt64),
                        Datum::String(space),
                        Datum::UInt64(stack_id),
                        Datum::UInt64(bytes),
                        Datum::UInt64(count),
                    ])
                },
            );
            drop(session);
            let mut stacks_output = stacks_output.activate();
            let mut session = stacks_output.session_with_builder(stacks_cap);
            emit_snapshot_diff(
                &mut session,
                &mut stacks_packer,
                &prev_stacks,
                &stacks,
                ts,
                |packer, &stack_id, frames| {
                    packer.pack_slice(&[
                        Datum::UInt64(process_id),
                        Datum::UInt64(stack_id),
                        Datum::String(frames),
                    ])
                },
            );

            prev_live = live;
            prev_stacks = stacks;
        }
    });

    for (variant, stream, name) in [
        (live_variant, live_stream, "Arrange HeapLive"),
        (stacks_variant, stacks_stream, "Arrange HeapStacks"),
    ] {
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
            >(exchange, name)
            .trace;
        let token: Rc<dyn std::any::Any> = Rc::new(());
        collections.insert(variant, LogCollection { trace, token });
    }

    Return { collections }
}

/// Renders `frames` (return addresses, root first) as folded symbol names, root first, with the
/// tracker and allocator frames at the leaf removed, and hashes the result.
fn symbolize(frames: &[usize]) -> (u64, Rc<str>) {
    let mut names = Vec::new();
    for &addr in frames {
        // A return address points past the call, so resolve the call instruction instead.
        let pc = std::ptr::without_provenance_mut::<std::ffi::c_void>(addr.saturating_sub(1));
        let mut inlined = Vec::new();
        backtrace::resolve(pc, |sym| {
            inlined.push(
                sym.name()
                    .map_or_else(|| format!("{addr:#x}"), |n| format!("{n:#}")),
            );
        });
        if inlined.is_empty() {
            inlined.push(format!("{addr:#x}"));
        }
        // `resolve` reports the innermost inlined frame first.
        names.extend(inlined.into_iter().rev());
    }
    while names.last().is_some_and(|n| is_allocator_frame(n)) {
        names.pop();
    }
    let folded = names.join(";");
    let mut hasher = std::hash::DefaultHasher::new();
    folded.hash(&mut hasher);
    (hasher.finish(), folded.into())
}

fn is_allocator_frame(name: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "mz_alloc",
        "<mz_alloc",
        "__rust",
        "alloc::alloc::",
        "<alloc::alloc::",
        "std::alloc::",
        "backtrace::",
        "malloc",
        "calloc",
        "realloc",
    ];
    PREFIXES.iter().any(|p| name.starts_with(p))
}
