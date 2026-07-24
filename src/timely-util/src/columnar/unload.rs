// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`UnloadChunk`]: the bulk-read capability for [`Chunk`] families, and the
//! batch-level driver over it.
//!
//! Ported from differential's unmerged bulk-read surface
//! (TimelyDataflow/differential-dataflow#782) so that consumers here depend
//! on a local definition rather than a git pin. One deliberate difference
//! from the upstream shape: the batch-level driver is the [`UnloadBatch`]
//! extension trait rather than inherent methods on differential's
//! [`ChunkBatch`] (an inherent impl on a foreign type is not available to
//! this crate); the method names and signatures match, so call sites survive
//! a future switch to upstream re-exports unchanged.

use std::cmp::Ordering;

use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

/// Look up a sorted set of keys in a chunk, copying the matching updates out
/// into caller-owned staging.
///
/// This is the bulk, copy-out way to read a chunk: extraction is finished
/// with a chunk's body when the call returns, so a spilled body is read for
/// the scope of one call and no reference into pool memory ever exists
/// outside it. That contract is what lets a buffer pool evict with no reader
/// accounting, where navigation-style access would need the body kept
/// readable for as long as the reader holds it.
///
/// Callers usually read whole batches, not single chunks: the [`UnloadBatch`]
/// extension on [`ChunkBatch`] looks up a probe set across the batch's chunk
/// sequence, and its `fetch_into` stages the full contents (the scan path).
/// Results land in a [`Staging`](UnloadChunk::Staging) the caller owns and
/// consumes at leisure. Staged times are copied verbatim — advancement by a
/// compaction frontier stays with the consumer.
///
/// Like [`Chunk`] itself, the trait has no key, val, time, or diff opinions:
/// `Staging` and `Probes` are opaque types the implementing family chooses,
/// and the one key comparison the batch driver needs is delegated to the
/// chunk via [`locate`](UnloadChunk::locate) — which, like
/// [`len`](Chunk::len), must be answerable from resident metadata even when
/// the body is spilled.
///
/// # The consume-index protocol (implementors)
///
/// A batch's chunks are one globally-sorted sequence cut at arbitrary points,
/// so a key's updates may straddle consecutive chunks.
/// [`extract_into`](UnloadChunk::extract_into) carries that invariant as a
/// protocol: a chunk consumes (advances `*probe_index` past) every probe
/// strictly below its last key — extracting hits, silently passing over
/// misses — and *extracts but does not consume* a probe equal to its last
/// key, so the driver re-offers that probe to the next chunk, whose
/// continuation lands in staging as a legal straddle. Consumers detect
/// misses as keys absent from staging.
pub trait UnloadChunk: Chunk {
    /// Where extracted updates land: an owned accumulation of resident data,
    /// chosen by the implementing family.
    ///
    /// Appends arrive in global `(key, val, time)` order; a group continuing
    /// across appends may remain split, exactly as chunk sequences elsewhere
    /// carry the straddle invariant.
    type Staging: Default;

    /// A sorted, deduplicated key column, borrowed from the same family —
    /// another chunk's key column, or a synthesized probe set.
    type Probes<'a>: Copy;

    /// The number of probe keys.
    fn probe_count(probes: Self::Probes<'_>) -> usize;

    /// Where `probes[probe_index]` falls relative to this chunk's key span:
    /// `Less` before the first key, `Equal` within `[first, last]`, `Greater`
    /// past the last key.
    ///
    /// Resident metadata only — must never fetch a spilled body.
    fn locate(&self, probes: Self::Probes<'_>, probe_index: usize) -> Ordering;

    /// Append this chunk's updates for probes at and after `*probe_index`
    /// into `staging`, advancing `*probe_index` past every probe strictly
    /// below this chunk's last key.
    ///
    /// A probe *equal* to the last key is extracted but not consumed: its
    /// group may continue in the next chunk (see the protocol above). Any
    /// read of a spilled body is scoped to this call.
    fn extract_into(
        &self,
        probes: Self::Probes<'_>,
        probe_index: &mut usize,
        staging: &mut Self::Staging,
    );

    /// Append the whole chunk into `staging` (the scan path).
    fn fetch_into(&self, staging: &mut Self::Staging);
}

/// The batch-level driver over [`UnloadChunk`], as an extension of
/// [`ChunkBatch`].
pub trait UnloadBatch<C: UnloadChunk> {
    /// Extract every probe hit in this batch into `staging`.
    ///
    /// Gallops the chunk list by [`locate`](UnloadChunk::locate) — resident
    /// metadata only — and opens (via
    /// [`extract_into`](UnloadChunk::extract_into)) only the chunks a probe
    /// touches. A probe left unconsumed at a chunk's last key is re-offered
    /// to the next chunk, whose continuation follows in staging.
    fn extract_into(&self, probes: C::Probes<'_>, staging: &mut C::Staging);

    /// Materialize the batch's full contents into `staging` (the scan path).
    fn fetch_into(&self, staging: &mut C::Staging);
}

impl<C: UnloadChunk> UnloadBatch<C> for ChunkBatch<C> {
    fn extract_into(&self, probes: C::Probes<'_>, staging: &mut C::Staging) {
        let count = C::probe_count(probes);
        let chunks = &self.chunks[..];
        let (mut probe_index, mut chunk) = (0usize, 0usize);
        while probe_index < count && chunk < chunks.len() {
            // Whether chunk `c` lies entirely below `probes[probe_index]`
            // (its last key is smaller), read from resident metadata.
            let below = |c: usize| chunks[c].locate(probes, probe_index) == Ordering::Greater;
            // Gallop to the first chunk not below the probe: exponential
            // search from the current chunk, then binary within the bracket.
            if below(chunk) {
                let (mut prev, mut step) = (chunk, 1usize);
                while prev + step < chunks.len() && below(prev + step) {
                    prev += step;
                    step <<= 1;
                }
                let (mut a, mut b) = (prev + 1, (prev + step).min(chunks.len()));
                while a < b {
                    let m = a + (b - a) / 2;
                    if below(m) { a = m + 1 } else { b = m }
                }
                chunk = a;
            }
            if chunk >= chunks.len() {
                return;
            }
            chunks[chunk].extract_into(probes, &mut probe_index, staging);
            // Everything strictly below this chunk's last key is consumed; a
            // probe equal to it was extracted but left for the next chunk
            // (the straddle re-offer).
            chunk += 1;
        }
    }

    fn fetch_into(&self, staging: &mut C::Staging) {
        for chunk in &self.chunks {
            chunk.fetch_into(staging);
        }
    }
}

#[cfg(test)]
mod tests {
    //! Contract tests for the batch driver over a miniature row family:
    //! extraction over arbitrary chunk cuts and probe placements — straddled
    //! keys included — equals the reference filter of the raw rows.

    use std::collections::VecDeque;

    use differential_dataflow::trace::Description;
    use timely::progress::Antichain;
    use timely::progress::frontier::AntichainRef;

    use super::*;

    /// A sorted, consolidated run of `(key, val)` rows; the minimal family.
    /// Only the read surface is exercised: the [`Chunk`] transducers are the
    /// maintenance half, which the driver never invokes.
    #[derive(Clone)]
    struct Rows(Vec<(u64, u64)>);

    impl Chunk for Rows {
        type Time = u64;
        const TARGET: usize = 8;
        fn len(&self) -> usize {
            self.0.len()
        }
        fn merge(_: &mut VecDeque<Self>, _: &mut VecDeque<Self>, _: &mut VecDeque<Self>) {
            unimplemented!("maintenance is not under test")
        }
        fn extract(
            _: &mut VecDeque<Self>,
            _: AntichainRef<u64>,
            _: &mut Antichain<u64>,
            _: &mut VecDeque<Self>,
            _: &mut VecDeque<Self>,
        ) {
            unimplemented!("maintenance is not under test")
        }
        fn advance(_: &mut VecDeque<Self>, _: AntichainRef<u64>, _: bool, _: &mut VecDeque<Self>) {
            unimplemented!("maintenance is not under test")
        }
        fn settle(_: &mut VecDeque<Self>, _: bool, _: &mut VecDeque<Self>) {
            unimplemented!("maintenance is not under test")
        }
    }

    impl UnloadChunk for Rows {
        type Staging = Vec<(u64, u64)>;
        type Probes<'a> = &'a [u64];

        fn probe_count(probes: &[u64]) -> usize {
            probes.len()
        }

        fn locate(&self, probes: &[u64], probe_index: usize) -> Ordering {
            let probe = probes[probe_index];
            if probe < self.0[0].0 {
                Ordering::Less
            } else if probe > self.0[self.0.len() - 1].0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }

        fn extract_into(
            &self,
            probes: &[u64],
            probe_index: &mut usize,
            staging: &mut Self::Staging,
        ) {
            let rows = &self.0[..];
            let last = rows[rows.len() - 1].0;
            let mut pos = 0;
            while *probe_index < probes.len() {
                let probe = probes[*probe_index];
                if probe > last {
                    return;
                }
                while pos < rows.len() && rows[pos].0 < probe {
                    pos += 1;
                }
                while pos < rows.len() && rows[pos].0 == probe {
                    staging.push(rows[pos]);
                    pos += 1;
                }
                if probe == last {
                    return;
                }
                *probe_index += 1;
            }
        }

        fn fetch_into(&self, staging: &mut Self::Staging) {
            staging.extend_from_slice(&self.0);
        }
    }

    fn batch_of(rows: &[(u64, u64)], cut: usize) -> ChunkBatch<Rows> {
        let chunks: Vec<Rows> = rows.chunks(cut).map(|c| Rows(c.to_vec())).collect();
        let description = Description::new(
            Antichain::from_elem(0u64),
            Antichain::new(),
            Antichain::from_elem(0u64),
        );
        ChunkBatch::new(chunks, description)
    }

    /// Every (chunk cut, contiguous probe range) placement over rows with a
    /// multi-chunk-spanning key equals the reference filter, and the scan
    /// path reproduces the batch exactly.
    #[mz_ore::test]
    fn extract_matches_filter() {
        // Even keys 0..=16; key 8 carries 6 rows so it spans chunks at every
        // cut size below 6.
        let mut rows: Vec<(u64, u64)> = Vec::new();
        for k in (0..=16u64).step_by(2) {
            let copies = if k == 8 { 6 } else { 1 };
            for c in 0..copies {
                rows.push((k, c));
            }
        }
        for cut in 1..=5 {
            let batch = batch_of(&rows, cut);
            for lo in 0..=18u64 {
                for hi in lo..=18u64 {
                    let probes: Vec<u64> = (lo..=hi).collect();
                    let mut staging = Vec::new();
                    batch.extract_into(&probes[..], &mut staging);
                    let want: Vec<_> = rows
                        .iter()
                        .filter(|r| lo <= r.0 && r.0 <= hi)
                        .copied()
                        .collect();
                    assert_eq!(staging, want, "cut={cut} probes={lo}..={hi}");
                }
            }
            let mut staging = Vec::new();
            batch.fetch_into(&mut staging);
            assert_eq!(staging, rows, "cut={cut}");
        }
    }
}
