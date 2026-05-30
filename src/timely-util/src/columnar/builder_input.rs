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

//! `BuilderInput` impl for [`Column`] so DD `Builder`s can drain our paged
//! batcher's output without an extra container conversion.
//!
//! Mirrors the impl on [`ColumnationStack`](crate::columnation::ColumnationStack)
//! at `columnation.rs`, but the `Item<'a>` here is a columnar `Ref` tuple
//! rather than a borrowed owned tuple, so:
//!
//! - `Key<'a>` / `Val<'a>` are `Ref<'a, K>` / `Ref<'a, V>` — no `Owned`
//!   round-trip on the read side.
//! - `Time` / `Diff` materialize as owned on `into_parts` (the trait
//!   contract requires owned for these).
//!
//! Distinct-counts (`key_val_upd_counts`) tally per chunk and sum, accepting
//! at most `chain.len()` over-counts at chunk boundaries. The downstream
//! consumer uses these as capacity hints, so a small over-estimate is
//! cheaper than the alternative (snapshotting `K::Owned` / `V::Owned`
//! across chunk boundaries).

use columnar::{Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::{BatchContainer, BuilderInput};
use timely::progress::Timestamp;

use crate::columnar::Column;

impl<KBC, VBC, K, V, T, R> BuilderInput<KBC, VBC> for Column<((K, V), T, R)>
where
    K: Columnar,
    V: Columnar,
    T: Columnar + Timestamp + Lattice + Clone,
    R: Columnar + Ord + Semigroup + Clone,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    KBC: BatchContainer,
    VBC: BatchContainer,
    for<'a, 'b> KBC::ReadItem<'a>: PartialEq<columnar::Ref<'b, K>>,
    for<'a, 'b> VBC::ReadItem<'a>: PartialEq<columnar::Ref<'b, V>>,
{
    type Key<'a> = columnar::Ref<'a, K>;
    type Val<'a> = columnar::Ref<'a, V>;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(
        item: Self::Item<'a>,
    ) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        let ((key, val), time, diff) = item;
        (key, val, T::into_owned(time), R::into_owned(diff))
    }

    fn key_eq(this: &columnar::Ref<'_, K>, other: KBC::ReadItem<'_>) -> bool {
        KBC::reborrow(other) == *this
    }

    fn val_eq(this: &columnar::Ref<'_, V>, other: VBC::ReadItem<'_>) -> bool {
        VBC::reborrow(other) == *this
    }

    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
        // Per-chunk dedup, summed. Skips cross-chunk equality checks; the
        // counts may over-count by up to `chain.len()` (one boundary per
        // chunk). Capacity-hint consumers tolerate over-estimates.
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        for col in chain.iter() {
            let view = col.borrow();
            let len = view.len();
            if len == 0 {
                continue;
            }
            let mut prev: Option<(columnar::Ref<'_, K>, columnar::Ref<'_, V>)> = None;
            for i in 0..len {
                let ((k, v), _, _) = view.get(i);
                match prev {
                    None => {
                        keys += 1;
                        vals += 1;
                    }
                    Some((pk, pv)) => {
                        if pk != k {
                            keys += 1;
                            vals += 1;
                        } else if pv != v {
                            vals += 1;
                        }
                    }
                }
                upds += 1;
                prev = Some((k, v));
            }
        }
        (keys, vals, upds)
    }
}
