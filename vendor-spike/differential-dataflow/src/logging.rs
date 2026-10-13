//! Loggers and logging events for differential dataflow.

use columnar::Columnar;
use serde::{Deserialize, Serialize};

/// Container builder for differential log events.
pub type DifferentialEventBuilder = timely::container::CapacityContainerBuilder<Vec<(std::time::Duration, DifferentialEvent)>>;

/// Logger for differential dataflow events.
pub type Logger = ::timely::logging_core::TypedLogger<DifferentialEventBuilder, DifferentialEvent>;

/// Enables logging of differential dataflow events.
pub fn enable<W>(worker: &mut timely::worker::Worker, writer: W) -> Option<Box<dyn std::any::Any+'static>>
where
    W: std::io::Write + 'static,
{
    worker.log_register().and_then(|mut log_register| {
        let writer = ::timely::dataflow::operators::capture::EventWriter::new(writer);
        let mut logger = ::timely::logging::BatchLogger::new(writer);
        log_register.insert::<DifferentialEventBuilder,_>("differential/arrange", move |time, data| logger.publish_batch(time, data))
    })
}

/// Possible different differential events.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub enum DifferentialEvent {
    /// Batch creation.
    Batch(BatchEvent),
    /// Merge start and stop events.
    Merge(MergeEvent),
    /// Batch dropped when trace dropped.
    Drop(DropEvent),
    /// A merge failed to complete in time.
    MergeShortfall(MergeShortfall),
    /// Trace sharing event.
    TraceShare(TraceShare),
    /// Batcher size event
    Batcher(BatcherEvent),
}

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct BatchEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub length: usize,
}

impl From<BatchEvent> for DifferentialEvent { fn from(e: BatchEvent) -> Self { DifferentialEvent::Batch(e) } }


/// Either the start or end of a merge event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct BatcherEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Change in records.
    pub records_diff: isize,
    /// Change in used size.
    pub size_diff: isize,
    /// Change in capacity.
    pub capacity_diff: isize,
    /// Change in number of allocations.
    pub allocations_diff: isize,
}

impl From<BatcherEvent> for DifferentialEvent { fn from(e: BatcherEvent) -> Self { DifferentialEvent::Batcher(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct DropEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub length: usize,
}

impl From<DropEvent> for DifferentialEvent { fn from(e: DropEvent) -> Self { DifferentialEvent::Drop(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct MergeEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// Length of first trace.
    pub length1: usize,
    /// Length of second trace.
    pub length2: usize,
    /// None implies a start.
    pub complete: Option<usize>,
}

impl From<MergeEvent> for DifferentialEvent { fn from(e: MergeEvent) -> Self { DifferentialEvent::Merge(e) } }

/// A merge failed to complete in time.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct MergeShortfall {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// By how much were we short.
    pub shortfall: usize,
}

impl From<MergeShortfall> for DifferentialEvent { fn from(e: MergeShortfall) -> Self { DifferentialEvent::MergeShortfall(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Columnar)]
pub struct TraceShare {
    /// Operator identifier.
    pub operator: usize,
    /// Change in number of shares.
    pub diff: isize,
}

impl From<TraceShare> for DifferentialEvent { fn from(e: TraceShare) -> Self { DifferentialEvent::TraceShare(e) } }
