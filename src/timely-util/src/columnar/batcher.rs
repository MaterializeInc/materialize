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

//! Types for consolidating, merging, and extracting columnar update collections.

use std::collections::VecDeque;

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use timely::Container;
use timely::container::{ContainerBuilder, PushInto};

use crate::columnar::Column;

/// A chunker to transform input data into sorted columns.
#[derive(Default)]
pub struct Chunker<C> {
    /// Buffer into which we'll consolidate.
    ///
    /// Also the buffer where we'll stage responses to `extract` and `finish`.
    /// When these calls return, the buffer is available for reuse.
    target: C,
    /// Consolidated buffers ready to go.
    ready: VecDeque<C>,
}

impl<C: Container + Clone + 'static> ContainerBuilder for Chunker<C> {
    type Container = C;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.target = ready;
            Some(&mut self.target)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

impl<'a, D, T, R, C2> PushInto<&'a mut Column<(D, T, R)>> for Chunker<C2>
where
    D: Columnar,
    for<'b> columnar::Ref<'b, D>: Ord + Copy,
    T: Columnar,
    for<'b> columnar::Ref<'b, T>: Ord + Copy,
    R: Columnar + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
    for<'b> columnar::Ref<'b, R>: Ord,
    C2: Container + for<'b> PushInto<&'b (D, T, R)>,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        // Sort input data
        // TODO: consider `Vec<usize>` that we retain, containing indexes.
        let mut permutation = Vec::with_capacity(container.len());
        permutation.extend(container.drain());
        permutation.sort();

        self.target.clear();
        // Iterate over the data, accumulating diffs for like keys.
        let mut iter = permutation.drain(..);
        if let Some((data, time, diff)) = iter.next() {
            let mut owned_data = D::into_owned(data);
            let mut owned_time = T::into_owned(time);

            let mut prev_data = data;
            let mut prev_time = time;
            let mut prev_diff = <R as Columnar>::into_owned(diff);

            for (data, time, diff) in iter {
                if (&prev_data, &prev_time) == (&data, &time) {
                    prev_diff.plus_equals(&diff);
                } else {
                    if !prev_diff.is_zero() {
                        D::copy_from(&mut owned_data, prev_data);
                        T::copy_from(&mut owned_time, prev_time);
                        let tuple = (owned_data, owned_time, prev_diff);
                        self.target.push_into(&tuple);
                        (owned_data, owned_time, prev_diff) = tuple;
                    }
                    prev_data = data;
                    prev_time = time;
                    R::copy_from(&mut prev_diff, diff);
                }
            }

            if !prev_diff.is_zero() {
                D::copy_from(&mut owned_data, prev_data);
                T::copy_from(&mut owned_time, prev_time);
                let tuple = (owned_data, owned_time, prev_diff);
                self.target.push_into(&tuple);
            }
        }

        if !self.target.is_empty() {
            self.ready.push_back(std::mem::take(&mut self.target));
        }
    }
}
