// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Local, deterministic comparison of logical payload consolidation and a
//! canonical-handle control. The control receives identities from the fixture,
//! excluding interning costs. Compare merge times, not end-to-end interner cost.
//!
//! Each sample builds fresh inputs, times merge/advance/settle, then verifies
//! exact output bytes and diffs outside the timer. Compressed mode forces pool
//! eviction but keeps compressed extents warm. It does not model cold device I/O.
//! Decode counters cover only payload decompression, not metadata or resident
//! copies. Pool residency is a sampled storage ledger, not process RSS.
//!
//! Run `cargo bench -p mz-timely-util --bench payload_consolidation --profile optimized`.
//! Environment: `MZ_PAYLOAD_BENCH_ROWS` (8192), `MZ_PAYLOAD_BENCH_WIDTHS` (64,1900),
//! `MZ_PAYLOAD_BENCH_SAMPLES` (3), `MZ_PAYLOAD_BENCH_CASES`
//! (unique,cancel,churn,collision,advance), `MZ_PAYLOAD_BENCH_BACKENDS`
//! (resident,compressed), and `MZ_PAYLOAD_BENCH_BLOCK_BYTES` (2097152).
//! CSV goes to stdout. Pair order alternates between samples.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;

use columnar::{Columnar, Index, Len};
use differential_dataflow::trace::chunk::Chunk;
use mz_ore::cast::CastFrom;
use mz_ore::pool::{ExtentCodec, Pool};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::chunk::{LZ4_CODEC, with_spill_override};
use mz_timely_util::columnar::payload::{PayloadChunk, PayloadLayout};
use mz_timely_util::out_of_core::{Manifest, PayloadComparator, PayloadRef, RowHandle, Store};
use timely::container::PushInto;
use timely::progress::Antichain;

static PAYLOAD_DECODES: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_DECODED_BYTES: AtomicU64 = AtomicU64::new(0);
static PAYLOAD_WRITTEN_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct CountingCodec;
impl ExtentCodec for CountingCodec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        LZ4_CODEC.encode(body, out);
        PAYLOAD_WRITTEN_BYTES.fetch_add(u64::cast_from(out.len()), AtomicOrdering::Relaxed);
    }
    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        PAYLOAD_DECODES.fetch_add(1, AtomicOrdering::Relaxed);
        PAYLOAD_DECODED_BYTES.fetch_add(u64::cast_from(body.len()), AtomicOrdering::Relaxed);
        LZ4_CODEC.decode(stored, body);
    }
}
static CODEC: CountingCodec = CountingCodec;

struct Layout<const LOGICAL: bool>;
impl<const LOGICAL: bool> PayloadLayout for Layout<LOGICAL> {
    type Data = (u64, (u64, RowHandle));
    type Diff = i64;
    const COMPARE_PAYLOADS: bool = LOGICAL;
    fn compare<'a>(
        a: columnar::Ref<'a, Self::Data>,
        b: columnar::Ref<'a, Self::Data>,
        owners: &[&Manifest],
        cache: &mut PayloadComparator,
    ) -> Ordering {
        if !LOGICAL {
            return a.cmp(&b);
        }
        a.0.cmp(b.0).then_with(|| a.1.0.cmp(b.1.0)).then_with(|| {
            cache
                .compare(
                    PayloadRef::External(RowHandle::into_owned(a.1.1)),
                    PayloadRef::External(RowHandle::into_owned(b.1.1)),
                    owners,
                )
                .unwrap()
        })
    }
    fn visit((_, (_, row)): columnar::Ref<'_, Self::Data>, _: &i64, rows: &mut Vec<RowHandle>) {
        rows.push(RowHandle::into_owned(row));
    }
}
type Part<const LOGICAL: bool> = PayloadChunk<Layout<LOGICAL>, u64>;

#[derive(Clone)]
struct Update {
    key: u64,
    value: u64,
    hash: u64,
    time: u64,
    diff: i64,
}

fn payload(value: u64, width: usize) -> Vec<u8> {
    let mut state = value.wrapping_add(0x9e3779b97f4a7c15);
    let pattern: Vec<_> = (0..(width / 4).max(8))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    let mut bytes: Vec<_> = pattern.iter().copied().cycle().take(width).collect();
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    bytes
}

fn fixture(case: &str, n: usize, width: usize) -> [Vec<Update>; 2] {
    let make = |key, value, time, diff| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        payload(value, width).hash(&mut hasher);
        Update {
            key,
            value,
            time,
            diff,
            hash: if case == "collision" {
                0
            } else {
                hasher.finish()
            },
        }
    };
    let mut left = Vec::new();
    let mut right = Vec::new();
    for i in 0..u64::cast_from(n) {
        match case {
            "unique" => {
                left.push(make(2 * i, 2 * i, 0, 1));
                right.push(make(2 * i + 1, 2 * i + 1, 0, 1));
            }
            "cancel" | "advance" => {
                left.push(make(i, i, 0, 1));
                right.push(make(i, i, u64::from(case == "advance"), -1));
            }
            "churn" => {
                left.push(make(i, 2 * i, 0, 1));
                right.push(make(i, 2 * i, 0, -1));
                right.push(make(i, 2 * i + 1, 0, 1));
            }
            "collision" => {
                left.push(make(0, 2 * i, 0, 1));
                right.push(if i % 2 == 0 {
                    make(0, 2 * i, 0, -1)
                } else {
                    make(0, 2 * i + 1, 0, 1)
                });
            }
            other => panic!("unknown case {other}"),
        }
    }
    for updates in [&mut left, &mut right] {
        updates.sort_by_key(|u| (u.key, u.hash, u.value, u.time));
    }
    [left, right]
}

fn inputs<const LOGICAL: bool>(
    store: &Store,
    fixture: &[Vec<Update>; 2],
    width: usize,
) -> [VecDeque<Part<LOGICAL>>; 2] {
    // This fixture-only dictionary is dropped before the merge timer starts.
    // It provides an oracle control, not an implementation of the removed interner.
    let mut canonical = BTreeMap::new();
    let mut builder = store.builder();
    if !LOGICAL {
        let values: BTreeSet<_> = fixture
            .iter()
            .flatten()
            .map(|update| update.value)
            .collect();
        for value in values {
            canonical.insert(value, builder.push(&payload(value, width)).unwrap());
        }
    }
    let canonical_owner = builder.finish();
    std::array::from_fn(|side| {
        let mut chunks = VecDeque::new();
        for batch in fixture[side].chunks(512) {
            let mut builder = store.builder();
            let mut column = Column::default();
            for update in batch {
                let row = if LOGICAL {
                    builder.push(&payload(update.value, width)).unwrap()
                } else {
                    canonical[&update.value]
                };
                column.push_into(&((update.key, (update.hash, row)), update.time, update.diff));
            }
            let owner = builder.finish();
            let owners = [&owner, &canonical_owner];
            // A collision bucket's physical order can differ in the control.
            let column = Part::<LOGICAL>::consolidate(column, &owners);
            chunks.push_back(Part::new(column, &owners));
        }
        let mut settled = VecDeque::new();
        Part::settle(&mut chunks, true, &mut settled);
        settled
    })
}

struct Measurement {
    setup_ms: f64,
    merge_ms: f64,
    merge_only_ms: f64,
    advance_ms: f64,
    merge_decodes: u64,
    advance_decodes: u64,
    decodes: u64,
    decoded_bytes: u64,
    payload_written_bytes: u64,
    merge_extent_bytes: u64,
    peak_pool_bytes: u64,
    final_chunks: u64,
    output_records: usize,
}

fn measure<const LOGICAL: bool>(
    fixture: &[Vec<Update>; 2],
    width: usize,
    block_bytes: usize,
    compressed: bool,
    runtime: &tokio::runtime::Runtime,
) -> Measurement {
    let pool = Pool::new().unwrap();
    pool.set_budget(if compressed { 0 } else { usize::MAX });
    // Keep compressed extents warm. OS pageout is deliberately outside this microbench.
    pool.set_rss_target(usize::MAX);
    let store = Store::new(pool.clone(), block_bytes, block_bytes * 2, 1, &CODEC).unwrap();
    with_spill_override(pool.clone(), || {
        PAYLOAD_WRITTEN_BYTES.store(0, AtomicOrdering::Relaxed);
        let start = Instant::now();
        let [mut left, mut right] = inputs::<LOGICAL>(&store, fixture, width);
        let setup_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut peak_pool_bytes = pool.stats().resident_bytes;
        let extent_start = pool.stats().extent_bytes_written;
        PAYLOAD_DECODES.store(0, AtomicOrdering::Relaxed);
        PAYLOAD_DECODED_BYTES.store(0, AtomicOrdering::Relaxed);
        let start = Instant::now();
        let mut merged = VecDeque::new();
        let mut settled = VecDeque::new();
        while !left.is_empty() && !right.is_empty() {
            Part::merge(&mut left, &mut right, &mut merged);
            Part::settle(&mut merged, false, &mut settled);
            peak_pool_bytes = peak_pool_bytes.max(pool.stats().resident_bytes);
        }
        merged.extend(left);
        merged.extend(right);
        Part::settle(&mut merged, true, &mut settled);
        let merge_only_ms = start.elapsed().as_secs_f64() * 1000.0;
        let merge_decodes = PAYLOAD_DECODES.load(AtomicOrdering::Relaxed);
        let advance_start = Instant::now();
        let mut carry = VecDeque::new();
        let mut advanced = VecDeque::new();
        let mut output = VecDeque::new();
        let frontier = Antichain::from_elem(2);
        for chunk in settled {
            carry.push_back(chunk);
            Part::advance(&mut carry, frontier.borrow(), false, &mut advanced);
            Part::settle(&mut advanced, false, &mut output);
            peak_pool_bytes = peak_pool_bytes.max(pool.stats().resident_bytes);
        }
        Part::advance(&mut carry, frontier.borrow(), true, &mut advanced);
        Part::settle(&mut advanced, true, &mut output);
        let merge_ms = start.elapsed().as_secs_f64() * 1000.0;
        let measurement = Measurement {
            setup_ms,
            merge_ms,
            merge_only_ms,
            advance_ms: advance_start.elapsed().as_secs_f64() * 1000.0,
            merge_decodes,
            advance_decodes: PAYLOAD_DECODES.load(AtomicOrdering::Relaxed) - merge_decodes,
            decodes: PAYLOAD_DECODES.load(AtomicOrdering::Relaxed),
            decoded_bytes: PAYLOAD_DECODED_BYTES.load(AtomicOrdering::Relaxed),
            payload_written_bytes: PAYLOAD_WRITTEN_BYTES.load(AtomicOrdering::Relaxed),
            merge_extent_bytes: pool.stats().extent_bytes_written - extent_start,
            peak_pool_bytes: peak_pool_bytes.max(pool.stats().resident_bytes),
            final_chunks: pool.stats().live_chunks,
            output_records: output.iter().map(Chunk::len).sum(),
        };
        runtime.block_on(verify(&store, fixture, width, &output));
        drop(output);
        assert_eq!(
            pool.stats().live_chunks,
            0,
            "all payload and metadata blocks must retire"
        );
        measurement
    })
}

async fn verify<const LOGICAL: bool>(
    store: &Store,
    fixture: &[Vec<Update>; 2],
    width: usize,
    output: &VecDeque<Part<LOGICAL>>,
) {
    let mut expected = BTreeMap::<(u64, u64, u64), i64>::new();
    for update in fixture.iter().flatten() {
        *expected
            .entry((update.key, update.value, update.time.max(2)))
            .or_default() += update.diff;
    }
    expected.retain(|_, diff| *diff != 0);
    let mut actual = BTreeMap::new();
    let mut lease: Option<mz_timely_util::out_of_core::ReadLease> = None;
    for chunk in output {
        let column = chunk.metadata();
        let view = column.borrow();
        for index in 0..view.len() {
            let ((key, (_, row)), time, diff) = view.get(index);
            let row = RowHandle::into_owned(row);
            if lease.as_ref().is_none_or(|lease| lease.get(row).is_err()) {
                drop(lease.take());
                lease = Some(
                    store
                        .prepare_read([(chunk.manifest(), row)])
                        .unwrap()
                        .read()
                        .await,
                );
            }
            let bytes = lease.as_ref().unwrap().get(row).unwrap();
            let value = u64::from_be_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(bytes, payload(value, width));
            assert!(
                actual.insert((*key, value, *time), *diff).is_none(),
                "output must be consolidated"
            );
        }
    }
    assert_eq!(actual, expected);
}

fn number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .map(|s| s.parse().expect("numeric benchmark option"))
        .unwrap_or(default)
}
fn list(name: &str, default: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(str::to_owned)
        .collect()
}

fn main() {
    let n = number("MZ_PAYLOAD_BENCH_ROWS", 8192);
    let samples = number("MZ_PAYLOAD_BENCH_SAMPLES", 3);
    let block = number("MZ_PAYLOAD_BENCH_BLOCK_BYTES", 2 << 20);
    assert!(n > 0 && samples > 0);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    println!(
        "case,backend,mode,base_rows,input_records,width,sample,setup_ms,merge_ms,payload_decodes,payload_decoded_bytes,payload_written_bytes,merge_extent_bytes,peak_pool_resident_bytes,live_chunks_after_merge,output_records,merge_only_ms,advance_ms,merge_decodes,advance_decodes"
    );
    for width in list("MZ_PAYLOAD_BENCH_WIDTHS", "64,1900") {
        let width: usize = width.parse().unwrap();
        assert!(width >= 8 && width + 8 <= block);
        for case in list(
            "MZ_PAYLOAD_BENCH_CASES",
            "unique,cancel,churn,collision,advance",
        ) {
            let fixture = fixture(&case, n, width);
            let input_records: usize = fixture.iter().map(Vec::len).sum();
            for backend in list("MZ_PAYLOAD_BENCH_BACKENDS", "resident,compressed") {
                assert!(backend == "resident" || backend == "compressed");
                for sample in 0..samples {
                    for logical in if sample % 2 == 0 {
                        [false, true]
                    } else {
                        [true, false]
                    } {
                        let m = if logical {
                            measure::<true>(
                                &fixture,
                                width,
                                block,
                                backend == "compressed",
                                &runtime,
                            )
                        } else {
                            measure::<false>(
                                &fixture,
                                width,
                                block,
                                backend == "compressed",
                                &runtime,
                            )
                        };
                        let mode = if logical {
                            "logical"
                        } else {
                            "canonical_control"
                        };
                        println!(
                            "{case},{backend},{mode},{n},{input_records},{width},{sample},{:.3},{:.3},{},{},{},{},{},{},{},{:.3},{:.3},{},{}",
                            m.setup_ms,
                            m.merge_ms,
                            m.decodes,
                            m.decoded_bytes,
                            m.payload_written_bytes,
                            m.merge_extent_bytes,
                            m.peak_pool_bytes,
                            m.final_chunks,
                            m.output_records,
                            m.merge_only_ms,
                            m.advance_ms,
                            m.merge_decodes,
                            m.advance_decodes
                        );
                    }
                }
            }
        }
    }
}
