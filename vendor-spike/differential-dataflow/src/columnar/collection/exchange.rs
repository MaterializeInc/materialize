//! Exchange / parallelization contract for `RecordedUpdates`.
//!
//! `ValPact` is the PACT used when shuffling columnar updates across workers;
//! `ValDistributor` is the per-worker partitioner it constructs.

use std::rc::Rc;

use columnar::{Index, Len};
use timely::logging::TimelyLogger;
use timely::dataflow::channels::pushers::{Exchange, exchange::Distributor};
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
use timely::progress::Timestamp;
use timely::worker::Worker;

use crate::columnar::layout::ColumnarUpdate as Update;
use crate::columnar::updates::UpdatesTyped;
use super::RecordedUpdates;

/// Distributor that routes `RecordedUpdates` records to workers by hashing keys.
pub struct ValDistributor<U: Update, H> {
    marker: std::marker::PhantomData<U>,
    hashfunc: H,
    pre_lens: Vec<usize>,
}

impl<U: Update, H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64> Distributor<RecordedUpdates<U>> for ValDistributor<U, H> {
    // TODO: For unsorted UpdatesTyped (stride-1 outer keys), each key is its own outer group,
    // so the per-group pre_lens snapshot and seal check costs O(keys × workers). Should
    // either batch keys by destination first, or detect stride-1 outer bounds and use a
    // simpler single-pass partitioning that seals once at the end.
    fn partition<T: Clone, P: timely::communication::Push<Message<T, RecordedUpdates<U>>>>(&mut self, container: &mut RecordedUpdates<U>, time: &T, pushers: &mut [P]) {
        use crate::columnar::updates::child_range;

        let view = container.updates.view();
        let keys_b = view.keys;
        let mut outputs: Vec<UpdatesTyped<U>> = (0..pushers.len()).map(|_| UpdatesTyped::default()).collect();

        // Each outer key group becomes a separate run in the destination.
        for outer in 0..Len::len(&keys_b) {
            self.pre_lens.clear();
            self.pre_lens.extend(outputs.iter().map(|o| o.keys.values.len()));
            if pushers.len().is_power_of_two() {
                let mask = (pushers.len() - 1) as u64;
                for k in child_range(keys_b.bounds, outer) {
                    let key = keys_b.values.get(k);
                    let h = (self.hashfunc)(key);
                    let idx = (h & mask) as usize;
                    outputs[idx].extend_from_keys(view, k..k+1);
                }
            }
            else {
                let pushers_len = pushers.len() as u64;
                for k in child_range(keys_b.bounds, outer) {
                    let key = keys_b.values.get(k);
                    let h = (self.hashfunc)(key);
                    let idx = (h % pushers_len) as usize;
                    outputs[idx].extend_from_keys(view, k..k+1);
                }
            }
            for (output, &pre) in outputs.iter_mut().zip(self.pre_lens.iter()) {
                if output.keys.values.len() > pre {
                    output.keys.bounds.push(output.keys.values.len() as u64);
                }
            }
        }

        // Distribute the input's record count across non-empty outputs.
        let total_records = container.records;
        let non_empty: usize = outputs.iter().filter(|o| !o.keys.values.is_empty()).count();
        let mut first_records = total_records.saturating_sub(non_empty.saturating_sub(1));
        for (pusher, output) in pushers.iter_mut().zip(outputs) {
            if !output.keys.values.is_empty() {
                let recorded = RecordedUpdates { updates: output.into(), records: first_records, consolidated: container.consolidated };
                first_records = 1;
                let mut recorded = recorded;
                Message::push_at(&mut recorded, time.clone(), pusher);
            }
        }
    }
    fn flush<T: Clone, P: timely::communication::Push<Message<T, RecordedUpdates<U>>>>(&mut self, _time: &T, _pushers: &mut [P]) { }
    fn relax(&mut self) { }
}

/// PACT for shuffling `RecordedUpdates` containers by hashing keys.
pub struct ValPact<H> {
    /// Hash function applied to each key reference.
    pub hashfunc: H,
}

impl<T, U, H> ParallelizationContract<T, RecordedUpdates<U>> for ValPact<H>
where
    T: Timestamp,
    U: Update,
    H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64 + 'static,
{
    type Pusher = Exchange<
        T,
        LogPusher<Box<dyn timely::communication::Push<Message<T, RecordedUpdates<U>>>>>,
        ValDistributor<U, H>
    >;
    type Puller = LogPuller<Box<dyn timely::communication::Pull<Message<T, RecordedUpdates<U>>>>>;

    fn connect(self, worker: &Worker, identifier: usize, address: Rc<[usize]>, logging: Option<TimelyLogger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = worker.allocate::<Message<T, RecordedUpdates<U>>>(identifier, address);
        let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, worker.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
        let distributor = ValDistributor {
            marker: std::marker::PhantomData,
            hashfunc: self.hashfunc,
            pre_lens: Vec::new(),
        };
        (Exchange::new(senders, distributor), LogPuller::new(receiver, worker.index(), identifier, logging.clone()))
    }
}
