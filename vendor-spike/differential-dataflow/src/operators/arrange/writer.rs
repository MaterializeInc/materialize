//! Write endpoint for a sequence of batches.
//!
//! A `TraceWriter` accepts a sequence of batches and distributes them
//! to both a shared trace and to a sequence of private queues.

use std::rc::{Rc, Weak};
use std::cell::RefCell;

use timely::progress::Antichain;

use crate::trace::{Trace, Batch, BatchReader};

use super::TraceAgentQueueWriter;
use super::TraceReplayInstruction;
use super::agent::trace_box::TraceBox;

/// Write endpoint for a sequence of batches.
///
/// A `TraceWriter` accepts a sequence of batches and distributes them
/// to both a shared trace and to a sequence of private queues.
pub struct TraceWriter<Tr: Trace> {
    /// Current upper limit.
    upper: Antichain<Tr::Time>,
    /// Shared trace, possibly absent (due to weakness).
    trace: Weak<RefCell<TraceBox<Tr>>>,
    /// A sequence of private queues into which batches are written.
    queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
}

impl<Tr: Trace> TraceWriter<Tr> {
    /// Creates a new `TraceWriter`.
    pub fn new(
        upper: Vec<Tr::Time>,
        trace: Weak<RefCell<TraceBox<Tr>>>,
        queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>
    ) -> Self
    {
        let mut temp = Antichain::new();
        temp.extend(upper);
        Self { upper: temp, trace, queues }
    }

    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.exert();
        }
    }

    /// Advances the trace by `batch`.
    ///
    /// The `hint` argument is either `None` in the case of an empty batch,
    /// or is `Some(time)` for a time less or equal to all updates in the
    /// batch and which is suitable for use as a capability.
    pub fn insert(&mut self, batch: Tr::Batch, hint: Option<Tr::Time>) {

        // Something is wrong if not a sequence.
        if !(&self.upper == batch.lower()) {
            println!("{:?} vs {:?}", self.upper, batch.lower());
        }
        assert!(&self.upper == batch.lower());
        assert!(batch.lower() != batch.upper());

        self.upper.clone_from(batch.upper());

        // push information to each listener that still exists.
        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            if let Some(pair) = queue.upgrade() {
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Batch(batch.clone(), hint.clone()));
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Frontier(batch.upper().clone()));
                pair.0.activate();
            }
        }
        borrow.retain(|w| w.upgrade().is_some());

        // push data to the trace, if it still exists.
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.insert(batch);
        }

    }

    /// Inserts an empty batch up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            self.insert(Tr::Batch::empty(self.upper.clone(), upper), None);
        }
    }
}

impl<Tr: Trace> Drop for TraceWriter<Tr> {
    fn drop(&mut self) {
        self.seal(Antichain::new())
    }
}
