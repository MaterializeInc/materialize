// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection logging for the MV sink's correction buffer.
//!
//! The correction buffer lives on a Tokio task, while the introspection loggers are owned by the
//! Timely thread and are not `Send`. [`ChannelLogging`] bridges the two: the buffer reports size
//! and chain changes as [`LoggingEvent`]s over a channel, and [`CorrectionLogger`] drains them on
//! the Timely thread.

use std::fmt;
use std::num::NonZeroIsize;
use std::ops::AddAssign;

use differential_dataflow::logging::{BatchEvent, DropEvent};
use tokio::sync::mpsc;

use crate::logging::compute::{
    ArrangementHeapAllocations, ArrangementHeapCapacity, ArrangementHeapSize,
    ArrangementHeapSizeOperator, ArrangementHeapSizeOperatorDrop, ComputeEvent,
    Logger as ComputeLogger,
};

/// Helper type for convenient tracking of various size metrics together.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SizeMetrics {
    pub size: usize,
    pub capacity: usize,
    pub allocations: usize,
}

impl AddAssign<Self> for SizeMetrics {
    fn add_assign(&mut self, other: Self) {
        self.size += other.size;
        self.capacity += other.capacity;
        self.allocations += other.allocations;
    }
}

/// A logging event sent from the Tokio task back to the Timely thread.
#[derive(Debug)]
pub enum LoggingEvent {
    /// A chain with the given number of updates was created.
    ChainCreated(usize),
    /// A chain with the given number of updates was dropped.
    ChainDropped(usize),
    /// The heap size of the correction buffer changed by the given amount.
    SizeDiff(NonZeroIsize),
    /// The heap capacity of the correction buffer changed by the given amount.
    CapacityDiff(NonZeroIsize),
    /// The number of allocations of the correction buffer changed by the given amount.
    AllocationsDiff(NonZeroIsize),
}

/// Channel-based logging for corrections on a Tokio task. `Send`-safe.
///
/// Sends logging events to the Timely thread, where they are applied to the real `Logging`
/// instance. This allows corrections on the Tokio task to participate in introspection logging
/// without holding `Rc<RefCell<..>>`.
#[derive(Clone, Debug)]
pub struct ChannelLogging(mpsc::UnboundedSender<LoggingEvent>);

impl ChannelLogging {
    /// Construct a new `ChannelLogging` sending events on the given channel.
    pub fn new(tx: mpsc::UnboundedSender<LoggingEvent>) -> Self {
        Self(tx)
    }

    /// Report the creation of a chain with the given number of updates.
    pub fn chain_created(&self, updates: usize) {
        let _ = self.0.send(LoggingEvent::ChainCreated(updates));
    }

    /// Report the dropping of a chain with the given number of updates.
    pub fn chain_dropped(&self, updates: usize) {
        let _ = self.0.send(LoggingEvent::ChainDropped(updates));
    }

    /// Report a change in heap size by the given amount.
    pub fn report_size_diff(&self, diff: isize) {
        if let Some(diff) = NonZeroIsize::new(diff) {
            let _ = self.0.send(LoggingEvent::SizeDiff(diff));
        }
    }

    /// Report a change in heap capacity by the given amount.
    pub fn report_capacity_diff(&self, diff: isize) {
        if let Some(diff) = NonZeroIsize::new(diff) {
            let _ = self.0.send(LoggingEvent::CapacityDiff(diff));
        }
    }

    /// Report a change in the number of allocations by the given amount.
    pub fn report_allocations_diff(&self, diff: isize) {
        if let Some(diff) = NonZeroIsize::new(diff) {
            let _ = self.0.send(LoggingEvent::AllocationsDiff(diff));
        }
    }
}

/// State for correction buffer logging on the Timely thread.
///
/// Drains [`LoggingEvent`]s sent by [`ChannelLogging`] from the Tokio task and applies them
/// to the compute and differential loggers. Emits `ArrangementHeapSizeOperator` on construction
/// and `ArrangementHeapSizeOperatorDrop` on drop.
// TODO: Correction buffer logging currently reuses the arrangement batch and size logging. This
// isn't strictly correct as a correction buffer is not an arrangement. Consider refactoring this
// to be about "operator sizes" instead.
pub(crate) struct CorrectionLogger {
    compute_logger: ComputeLogger,
    differential_logger: differential_dataflow::logging::Logger,
    operator_id: usize,
    rx: mpsc::UnboundedReceiver<LoggingEvent>,
    /// Net number of batches logged (BatchEvent - DropEvent).
    net_batches: isize,
    /// Net number of records logged across all batch/drop/merge events.
    net_records: isize,
    /// Cumulative heap size delta, for retraction on drop.
    net_size: isize,
    /// Cumulative heap capacity delta, for retraction on drop.
    net_capacity: isize,
    /// Cumulative heap allocations delta, for retraction on drop.
    net_allocations: isize,
}

impl fmt::Debug for CorrectionLogger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrectionLogger")
            .field("operator_id", &self.operator_id)
            .finish_non_exhaustive()
    }
}

impl CorrectionLogger {
    pub fn new(
        compute_logger: ComputeLogger,
        differential_logger: differential_dataflow::logging::Logger,
        operator_id: usize,
        address: Vec<usize>,
        rx: mpsc::UnboundedReceiver<LoggingEvent>,
    ) -> Self {
        compute_logger.log(&ComputeEvent::ArrangementHeapSizeOperator(
            ArrangementHeapSizeOperator {
                operator_id,
                address,
            },
        ));

        Self {
            compute_logger,
            differential_logger,
            operator_id,
            rx,
            net_batches: 0,
            net_records: 0,
            net_size: 0,
            net_capacity: 0,
            net_allocations: 0,
        }
    }

    /// Drain logging events from the channel and apply them locally.
    pub fn apply_events(&mut self) {
        use LoggingEvent::*;

        while let Ok(event) = self.rx.try_recv() {
            match event {
                ChainCreated(length) => {
                    self.net_batches += 1;
                    self.net_records += isize::try_from(length).expect("must fit");
                    self.differential_logger.log(BatchEvent {
                        operator: self.operator_id,
                        length,
                    });
                }
                ChainDropped(length) => {
                    self.net_batches -= 1;
                    self.net_records -= isize::try_from(length).expect("must fit");
                    self.differential_logger.log(DropEvent {
                        operator: self.operator_id,
                        length,
                    });
                }
                SizeDiff(delta_size) => {
                    self.net_size += delta_size.get();
                    self.compute_logger.log(&ComputeEvent::ArrangementHeapSize(
                        ArrangementHeapSize {
                            operator_id: self.operator_id,
                            delta_size: delta_size.get(),
                        },
                    ));
                }
                CapacityDiff(delta_capacity) => {
                    self.net_capacity += delta_capacity.get();
                    self.compute_logger
                        .log(&ComputeEvent::ArrangementHeapCapacity(
                            ArrangementHeapCapacity {
                                operator_id: self.operator_id,
                                delta_capacity: delta_capacity.get(),
                            },
                        ));
                }
                AllocationsDiff(delta_allocations) => {
                    self.net_allocations += delta_allocations.get();
                    self.compute_logger
                        .log(&ComputeEvent::ArrangementHeapAllocations(
                            ArrangementHeapAllocations {
                                operator_id: self.operator_id,
                                delta_allocations: delta_allocations.get(),
                            },
                        ));
                }
            }
        }
    }
}

impl Drop for CorrectionLogger {
    fn drop(&mut self) {
        // Drain any events that arrived before the drop. Note that the Tokio task
        // may still be running (abort is async), so some events may not have arrived
        // yet. We retract any remaining batch/record counts below.
        self.apply_events();

        // Retract any outstanding batch and record counts that weren't balanced by
        // ChainDropped events. This handles the case where the Tokio task is aborted
        // and its Correction destructors haven't run yet (abort is async).
        //
        // Each DropEvent retracts one batch and `length` records, so we emit one per
        // outstanding batch, with the first carrying all outstanding records.
        for i in 0..self.net_batches {
            let length = if i == 0 {
                usize::try_from(self.net_records).unwrap_or(0)
            } else {
                0
            };
            self.differential_logger.log(DropEvent {
                operator: self.operator_id,
                length,
            });
        }

        // Retract any outstanding heap size/capacity/allocations deltas.
        if self.net_size != 0 {
            self.compute_logger
                .log(&ComputeEvent::ArrangementHeapSize(ArrangementHeapSize {
                    operator_id: self.operator_id,
                    delta_size: -self.net_size,
                }));
        }
        if self.net_capacity != 0 {
            self.compute_logger
                .log(&ComputeEvent::ArrangementHeapCapacity(
                    ArrangementHeapCapacity {
                        operator_id: self.operator_id,
                        delta_capacity: -self.net_capacity,
                    },
                ));
        }
        if self.net_allocations != 0 {
            self.compute_logger
                .log(&ComputeEvent::ArrangementHeapAllocations(
                    ArrangementHeapAllocations {
                        operator_id: self.operator_id,
                        delta_allocations: -self.net_allocations,
                    },
                ));
        }

        self.compute_logger
            .log(&ComputeEvent::ArrangementHeapSizeOperatorDrop(
                ArrangementHeapSizeOperatorDrop {
                    operator_id: self.operator_id,
                },
            ));
    }
}
