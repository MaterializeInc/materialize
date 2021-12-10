// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use differential_dataflow::consolidation::Consolidation;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::layers::BatchContainer;
use differential_dataflow::ExchangeData;
use timely::communication::Push;
use timely::dataflow::channels::BundleCore;
use timely::dataflow::operators::generic::OutputHandleCore;
use timely::dataflow::operators::{Capability, CapabilityRef};
use timely::progress::Timestamp;
use timely::Container;

/// A buffer that consolidates updates
///
/// The buffer implements a wrapper around [OutputHandleCore] consolidating elements pushed to it. It is
/// backed by a capacity-limited buffer, which means that compaction only occurs within the
/// dimensions of the buffer, i.e. the number of unique keys is less than half of the buffer's
/// capacity.
///
/// A cap is retained whenever the current time changes to be able to flush on drop or when the time
/// changes again.
///
/// The buffer is filled with updates until it reaches its capacity. At this point, the updates are
/// consolidated to free up space. This process repeats until the consolidation recovered less than
/// half of the buffer's capacity, at which point the buffer will be shipped.
///
/// The buffer retains a capability to send data on flush. It will flush all data once dropped, if
/// time changes, or if the buffer capacity is reached.
pub struct ConsolidateBuffer<'a, T, D, R, P, C = Vec<(D, T, R)>>
where
    P: Push<BundleCore<T, C>> + 'a,
    T: Clone + Lattice + Ord + Timestamp + 'a,
    D: 'a,
    D: ExchangeData,
    R: Semigroup,
    C: Container<Item = (D, T, R)> + Consolidation + BatchContainer<Item = (D, T, R)>,
{
    // a buffer for records, to send at self.cap
    // Invariant: Buffer only contains data if cap is Some.
    buffer: C,
    output_handle: OutputHandleCore<'a, T, C, P>,
    cap: Option<Capability<T>>,
    port: usize,
}

impl<'a, T, D: ExchangeData, R: Semigroup, P, C> ConsolidateBuffer<'a, T, D, R, P, C>
where
    T: Clone + Lattice + Ord + Timestamp + 'a,
    P: Push<BundleCore<T, C>> + 'a,
    C: Container<Item = (D, T, R)> + Consolidation + BatchContainer<Item = (D, T, R)>,
{
    /// Create a new [ConsolidateBuffer], wrapping the provided session.
    ///
    /// * `output_handle`: The output to send data to.
    /// * 'port': The output port to retain capabilities for.
    pub fn new(output_handle: OutputHandleCore<'a, T, C, P>, port: usize) -> Self {
        Self {
            output_handle,
            port,
            cap: None,
            buffer: C::with_capacity(::timely::container::buffer::default_capacity::<(D, T, R)>()),
        }
    }

    /// Give an element to the buffer
    pub fn give(&mut self, cap: &CapabilityRef<T>, data: (D, T, R)) {
        // Retain a cap for the current time, which will be used on flush.
        if self.cap.as_ref().map_or(true, |t| t.time() != cap.time()) {
            // Flush on capability change
            self.flush();
            // Retain capability for the specified output port.
            self.cap = Some(cap.delayed_for_output(cap.time(), self.port));
        }
        self.buffer.push(data);
        if self.buffer.len() == self.buffer.capacity() {
            // Consolidate while the consolidation frees at least half the buffer
            self.buffer.consolidate();
            if self.buffer.len() > self.buffer.capacity() / 2 {
                self.flush();
            }
        }
    }

    /// Flush the internal buffer to the underlying session
    pub fn flush(&mut self) {
        if let Some(cap) = &self.cap {
            self.output_handle
                .session(cap)
                .give_container(&mut self.buffer);
            self.buffer =
                C::with_capacity(::timely::container::buffer::default_capacity::<(D, T, R)>());
        }
    }
}

impl<'a, T, D, R, P, C> Drop for ConsolidateBuffer<'a, T, D, R, P, C>
where
    P: Push<BundleCore<T, C>> + 'a,
    T: Clone + Lattice + Ord + Timestamp + 'a,
    D: ExchangeData + 'a,
    R: Semigroup,
    C: Container<Item = (D, T, R)> + Consolidation + BatchContainer<Item = (D, T, R)>,
{
    fn drop(&mut self) {
        self.flush();
    }
}
