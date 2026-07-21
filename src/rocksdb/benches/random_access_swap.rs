// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Random point lookups over one keyed dataset, three backends, meant to run
//! under a cgroup memory limit on a swap-provisioned Linux box:
//!
//! * `mem`: a pure in-memory sorted columnar structure (binary-searched key
//!   column plus a value column), the arrangement-like baseline. Under a
//!   memory limit the kernel decides what swaps, 4 KiB faults at a time.
//! * `pool`: sorted 64 KiB chunks in the `mz_ore::pool` buffer pool with a
//!   resident fence index, copy-out reads, and an engine-held budget. The
//!   uniform pass uses plain reads, the skewed pass admitting reads.
//! * `pool-sub`: as `pool`, with a sub-blocked stored form ([`SubBlockCodec`],
//!   8 KiB interior lz4 blocks behind an offset header) and ranged reads, so
//!   a point probe decompresses one sub-block instead of the whole chunk.
//! * `rocksdb`: RocksDB exactly in production's scratch-less "in memory
//!   mode": `Env::mem_env()` (the SSTs live in anonymous process memory)
//!   with the WAL disabled, lz4-compressed. Under a memory limit those
//!   pages kernel-swap like any anonymous memory.
//!
//! One backend per process (`BACKEND=mem|pool|rocksdb`), so a cgroup scope
//! measures exactly one:
//!
//! ```text
//! cargo bench -p mz-rocksdb --bench random_access_swap --no-run
//! sudo systemd-run --scope --wait -p MemoryMax=512M -p MemorySwapMax=infinity \
//!     env BACKEND=pool <bench binary>
//! ```
//!
//! `RA_RECORDS` (default 16M, ~1.1 GiB logical at 72 B/record) sizes the
//! dataset; `RA_QUERIES` (default 100k) the measured passes. The final
//! checksum must agree across backends, which keeps the three lookup paths
//! honest. Phases report wall time, throughput, mean/p50/p99 latency, and
//! `VmRSS`/`VmHWM`.

use std::time::Instant;

use mz_ore::pool::{ChunkHandle, ChunkHints, ExtentCodec, Pool};

/// Records are `(u64 key, 8 u64 value words)`: 72 bytes, in the range of
/// upsert row sizes.
const VAL_WORDS: usize = 8;
const REC_WORDS: usize = 1 + VAL_WORDS;
/// 64 KiB chunks hold 910 records (8190 of 8192 words; the tail pads).
const CHUNK_WORDS: usize = (64 << 10) / 8;
const RECS_PER_CHUNK: usize = CHUNK_WORDS / REC_WORDS;

fn value_word(key: u64, j: usize) -> u64 {
    key.wrapping_mul(0x9E3779B97F4A7C15)
        .rotate_left(j as u32)
        .wrapping_add(j as u64)
}

fn rng(mut seed: u64) -> impl FnMut() -> u64 {
    move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// `VmRSS` and `VmHWM` in MiB from `/proc/self/status` (zeros off Linux).
fn rss_mib() -> (u64, u64) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            / 1024
    };
    (field("VmRSS"), field("VmHWM"))
}

/// One backend: load sorted data, then serve point lookups.
trait Backend {
    /// Loads keys `0..n` with their derived values, in key order.
    fn load(n: u64) -> Self;
    /// Returns the first value word for `key`, or panics if absent.
    fn get(&mut self, key: u64) -> u64;
    /// Batched probe, the operator calling convention: `keys` arrive sorted
    /// and deduplicated (the drain sorts its window), and the backend
    /// fetches them in one pass, amortizing whatever unit it reads. Returns
    /// the wrapping sum of the keys' first value words.
    fn get_batch(&mut self, keys: &[u64]) -> u64 {
        keys.iter()
            .fold(0u64, |acc, &k| acc.wrapping_add(self.get(k)))
    }
    /// Appends `count` new records starting at `first` (sorted, beyond all
    /// existing keys), in the backend's natural batch-write form: hydration
    /// writing new state while probes are served.
    fn append(&mut self, first: u64, count: usize);
}

/// Pure in-memory sorted columns: the arrangement-like baseline.
struct MemBackend {
    keys: Vec<u64>,
    vals: Vec<u64>,
    /// Appended batches, one segment per append, like a spine of new
    /// batches. Probes target only the original range.
    appended: Vec<(Vec<u64>, Vec<u64>)>,
}

impl Backend for MemBackend {
    fn load(n: u64) -> Self {
        let keys: Vec<u64> = (0..n).collect();
        let mut vals = Vec::with_capacity((n as usize) * VAL_WORDS);
        for key in 0..n {
            for j in 0..VAL_WORDS {
                vals.push(value_word(key, j));
            }
        }
        MemBackend {
            keys,
            vals,
            appended: Vec::new(),
        }
    }

    fn get(&mut self, key: u64) -> u64 {
        let i = self.keys.binary_search(&key).expect("key present");
        self.vals[i * VAL_WORDS]
    }

    fn append(&mut self, first: u64, count: usize) {
        let mut keys = Vec::with_capacity(count);
        let mut vals = Vec::with_capacity(count * VAL_WORDS);
        for key in first..first + count as u64 {
            keys.push(key);
            for j in 0..VAL_WORDS {
                vals.push(value_word(key, j));
            }
        }
        self.appended.push((keys, vals));
    }
}

/// Bench-local lz4 codec with the production framing.
#[derive(Debug)]
struct Lz4Codec;

static CODEC: Lz4Codec = Lz4Codec;

impl ExtentCodec for Lz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("bounded payloads");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..]).expect("sized");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits");
        assert_eq!(len, body.len(), "length mismatch");
        let written =
            lz4_flex::block::decompress_into(&stored[4..], body).expect("valid lz4 block");
        assert_eq!(written, body.len());
    }
}

/// Uncompressed bytes per interior sub-block of [`SubBlockCodec`].
const SUB_BYTES: usize = 8 << 10;
const SUB_WORDS: usize = SUB_BYTES / 8;

/// A sub-blocked stored form: the body cut into fixed 8 KiB pieces, each an
/// independent lz4 block, behind a header of `u32` cumulative compressed
/// end offsets. `decode_range` decompresses only the pieces the range
/// touches, so a point probe reads one sub-block instead of the body.
#[derive(Debug)]
struct SubBlockCodec;

static SUB_CODEC: SubBlockCodec = SubBlockCodec;

impl SubBlockCodec {
    fn pieces(body_len: usize) -> usize {
        body_len.div_ceil(SUB_BYTES)
    }

    /// (header length, piece compressed ranges) from the stored header.
    fn layout(stored: &[u8]) -> (usize, Vec<(usize, usize)>) {
        let n = u32::from_le_bytes(stored[..4].try_into().expect("count")) as usize;
        let header = 4 + 4 * n;
        let mut ranges = Vec::with_capacity(n);
        let mut start = 0usize;
        for i in 0..n {
            let end =
                u32::from_le_bytes(stored[4 + 4 * i..8 + 4 * i].try_into().expect("end")) as usize;
            ranges.push((start, end));
            start = end;
        }
        (header, ranges)
    }
}

impl ExtentCodec for SubBlockCodec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let n = Self::pieces(body.len());
        out.clear();
        out.extend_from_slice(&u32::try_from(n).expect("count fits").to_le_bytes());
        out.resize(4 + 4 * n, 0);
        let mut end = 0usize;
        for i in 0..n {
            let piece = &body[i * SUB_BYTES..((i + 1) * SUB_BYTES).min(body.len())];
            let compressed = lz4_flex::block::compress_prepend_size(piece);
            end += compressed.len();
            out[4 + 4 * i..8 + 4 * i]
                .copy_from_slice(&u32::try_from(end).expect("offset fits").to_le_bytes());
            out.extend_from_slice(&compressed);
        }
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let body_len = body.len();
        let (header, ranges) = Self::layout(stored);
        assert_eq!(ranges.len(), Self::pieces(body_len), "piece count");
        for (i, (start, end)) in ranges.iter().enumerate() {
            let dst = &mut body[i * SUB_BYTES..((i + 1) * SUB_BYTES).min(body_len)];
            let written =
                lz4_flex::block::decompress_into(&stored[header + start + 4..header + end], dst)
                    .expect("valid lz4 piece");
            assert_eq!(written, dst.len(), "piece length");
        }
    }

    fn decode_range(&self, stored: &[u8], body_len: usize, offset: usize, dst: &mut [u8]) {
        let end = offset + dst.len();
        assert!(end <= body_len, "range within body");
        let (header, ranges) = Self::layout(stored);
        use std::cell::RefCell;
        thread_local! {
            static PIECE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
        }
        PIECE.with(|cell| {
            let mut piece_buf = cell.borrow_mut();
            for p in offset / SUB_BYTES..=(end - 1) / SUB_BYTES {
                let piece_start = p * SUB_BYTES;
                let piece_len = SUB_BYTES.min(body_len - piece_start);
                let (start, stop) = ranges[p];
                piece_buf.resize(piece_len, 0);
                let written = lz4_flex::block::decompress_into(
                    &stored[header + start + 4..header + stop],
                    &mut piece_buf,
                )
                .expect("valid lz4 piece");
                assert_eq!(written, piece_len, "piece length");
                let copy_from = offset.max(piece_start);
                let copy_to = end.min(piece_start + piece_len);
                dst[copy_from - offset..copy_to - offset]
                    .copy_from_slice(&piece_buf[copy_from - piece_start..copy_to - piece_start]);
            }
        });
    }
}

/// Inserts one chunk of `count` records starting at `first`, padded sorted.
fn insert_chunk(
    pool: &Pool,
    codec: &'static dyn ExtentCodec,
    depth: u8,
    first: u64,
    count: usize,
) -> ChunkHandle {
    pool.insert_with(CHUNK_WORDS, ChunkHints { depth }, codec, |dst| {
        for r in 0..count {
            let k = first + r as u64;
            dst[r * REC_WORDS] = k;
            for j in 0..VAL_WORDS {
                dst[r * REC_WORDS + 1 + j] = value_word(k, j);
            }
        }
        // Pad keys at `u64::MAX` keep the chunk's key order sorted, so the
        // in-chunk binary search stays valid in partially-filled chunks.
        dst[count * REC_WORDS..].fill(u64::MAX);
    })
}

/// Budget-bounded pool chunks with a resident fence index.
struct PoolBackend {
    pool: Pool,
    codec: &'static dyn ExtentCodec,
    fences: Vec<u64>,
    chunks: Vec<ChunkHandle>,
    /// Chunks appended by the hydration pass, held live like new batches.
    appended: Vec<ChunkHandle>,
    buf: Vec<u64>,
    /// Whether lookups use the admitting read (the skewed pass does).
    admit: bool,
    /// Whether the stored form is sub-blocked and lookups read only the
    /// containing sub-block's word range (`BACKEND=pool-sub`).
    sub: bool,
}

impl PoolBackend {
    /// Binary-search `key` in the record words of `buf`, returning its
    /// first value word.
    fn search(buf: &[u64], key: u64) -> u64 {
        let recs = buf.len() / REC_WORDS;
        let (mut lo, mut hi) = (0usize, recs);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if buf[mid * REC_WORDS] < key {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        assert!(lo < recs && buf[lo * REC_WORDS] == key, "key present");
        buf[lo * REC_WORDS + 1]
    }

    /// The record-aligned, sub-block-sized word window containing `key`'s
    /// record, derived from the chunk's fence, the resident-index lookup a
    /// real sub-blocked chunk performs. Record alignment keeps the fetched
    /// buffer searchable at record stride; the codec's interior pieces need
    /// no alignment, `decode_range` serves any byte range.
    fn sub_range(&self, chunk: usize, key: u64) -> std::ops::Range<usize> {
        const RECS_PER_SUB: usize = SUB_WORDS / REC_WORDS;
        let rec = usize::try_from(key - self.fences[chunk]).expect("record index");
        let first = (rec / RECS_PER_SUB) * RECS_PER_SUB;
        (first * REC_WORDS)..(((first + RECS_PER_SUB) * REC_WORDS).min(CHUNK_WORDS))
    }
}

impl Backend for PoolBackend {
    fn load(n: u64) -> Self {
        let sub = std::env::var("BACKEND").as_deref() == Ok("pool-sub");
        let codec: &'static dyn mz_ore::pool::ExtentCodec = if sub { &SUB_CODEC } else { &CODEC };
        let pool = Pool::new().expect("pool reservation");
        pool.set_budget(env_usize("RA_POOL_BUDGET_MIB", 128) << 20);
        pool.set_rss_target(env_usize("RA_POOL_RSS_TARGET_MIB", 400) << 20);
        pool.set_spill_threads(2);
        pool.set_eager_backing(true);

        let mut fences = Vec::new();
        let mut chunks = Vec::new();
        let mut key = 0u64;
        while key < n {
            let count = RECS_PER_CHUNK.min(usize::try_from(n - key).expect("fits"));
            fences.push(key);
            chunks.push(insert_chunk(&pool, codec, 3, key, count));
            key += count as u64;
        }
        PoolBackend {
            pool,
            codec,
            fences,
            chunks,
            appended: Vec::new(),
            buf: Vec::new(),
            admit: false,
            sub,
        }
    }

    fn append(&mut self, first: u64, count: usize) {
        let (mut key, end) = (first, first + count as u64);
        while key < end {
            let n = RECS_PER_CHUNK.min(usize::try_from(end - key).expect("fits"));
            // Depth zero: fresh young batches, exactly what hydration mints.
            self.appended
                .push(insert_chunk(&self.pool, self.codec, 0, key, n));
            key += n as u64;
        }
    }

    fn get(&mut self, key: u64) -> u64 {
        let c = self.fences.partition_point(|&f| f <= key) - 1;
        if self.sub {
            let range = self.sub_range(c, key);
            if self.admit {
                self.chunks[c].read_range_into_admit(range, &mut self.buf);
            } else {
                self.chunks[c].read_range_into(range, &mut self.buf);
            }
        } else if self.admit {
            self.chunks[c].read_into_admit(&mut self.buf);
        } else {
            self.chunks[c].read_into(&mut self.buf);
        }
        Self::search(&self.buf, key)
    }

    /// Sorted keys visit chunks (and sub-blocks) in order, so each touched
    /// unit is fetched exactly once per batch, the `extract_into`
    /// amortization at the respective granularity.
    fn get_batch(&mut self, keys: &[u64]) -> u64 {
        let mut sum = 0u64;
        let mut current: Option<(usize, usize)> = None;
        for &key in keys {
            let c = self.fences.partition_point(|&f| f <= key) - 1;
            let unit = if self.sub {
                (c, self.sub_range(c, key).start)
            } else {
                (c, 0)
            };
            if current != Some(unit) {
                if self.sub {
                    let range = self.sub_range(c, key);
                    if self.admit {
                        self.chunks[c].read_range_into_admit(range, &mut self.buf);
                    } else {
                        self.chunks[c].read_range_into(range, &mut self.buf);
                    }
                } else if self.admit {
                    self.chunks[c].read_into_admit(&mut self.buf);
                } else {
                    self.chunks[c].read_into(&mut self.buf);
                }
                current = Some(unit);
            }
            sum = sum.wrapping_add(Self::search(&self.buf, key));
        }
        sum
    }
}

/// RocksDB in production's scratch-less in-memory mode.
struct RocksBackend {
    db: rocksdb::DB,
    /// Keeps the in-memory `Env` alive for the DB's lifetime.
    _env: rocksdb::Env,
}

impl Backend for RocksBackend {
    fn load(n: u64) -> Self {
        let env = rocksdb::Env::mem_env().expect("mem env");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_env(&env);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let db = rocksdb::DB::open(&opts, "/random-access-bench").expect("open");
        let mut wo = rocksdb::WriteOptions::default();
        wo.disable_wal(true);
        let mut batch = rocksdb::WriteBatch::default();
        let mut val = [0u8; VAL_WORDS * 8];
        for key in 0..n {
            for j in 0..VAL_WORDS {
                val[j * 8..(j + 1) * 8].copy_from_slice(&value_word(key, j).to_le_bytes());
            }
            // Big-endian keys keep RocksDB's order equal to numeric order.
            batch.put(key.to_be_bytes(), val);
            if batch.len() >= 10_000 {
                db.write_opt(std::mem::take(&mut batch), &wo)
                    .expect("write");
            }
        }
        db.write_opt(batch, &wo).expect("write");
        db.flush().expect("flush");
        RocksBackend { db, _env: env }
    }

    fn get(&mut self, key: u64) -> u64 {
        let v = self
            .db
            .get(key.to_be_bytes())
            .expect("get")
            .expect("key present");
        u64::from_le_bytes(v[..8].try_into().expect("value width"))
    }

    fn append(&mut self, first: u64, count: usize) {
        let mut wo = rocksdb::WriteOptions::default();
        wo.disable_wal(true);
        let mut batch = rocksdb::WriteBatch::default();
        let mut val = [0u8; VAL_WORDS * 8];
        for key in first..first + count as u64 {
            for j in 0..VAL_WORDS {
                val[j * 8..(j + 1) * 8].copy_from_slice(&value_word(key, j).to_le_bytes());
            }
            batch.put(key.to_be_bytes(), val);
        }
        self.db.write_opt(batch, &wo).expect("write");
    }

    /// The upsert operator's calling convention: one `multi_get` per drained
    /// window.
    fn get_batch(&mut self, keys: &[u64]) -> u64 {
        let key_bytes: Vec<[u8; 8]> = keys.iter().map(|k| k.to_be_bytes()).collect();
        self.db
            .multi_get(&key_bytes)
            .into_iter()
            .map(|r| {
                let v = r.expect("get").expect("key present");
                u64::from_le_bytes(v[..8].try_into().expect("value width"))
            })
            .fold(0u64, u64::wrapping_add)
    }
}

/// Runs `queries` lookups from `pick`, reporting latency and throughput.
fn pass<B: Backend>(
    label: &str,
    backend: &mut B,
    queries: usize,
    mut pick: impl FnMut() -> u64,
) -> u64 {
    let mut lat_ns: Vec<u64> = Vec::with_capacity(queries);
    let mut checksum = 0u64;
    let start = Instant::now();
    for _ in 0..queries {
        let key = pick();
        let t = Instant::now();
        checksum = checksum.wrapping_add(std::hint::black_box(backend.get(key)));
        lat_ns.push(u64::try_from(t.elapsed().as_nanos()).expect("fits"));
    }
    let wall = start.elapsed().as_secs_f64();
    lat_ns.sort_unstable();
    let mean = lat_ns.iter().sum::<u64>() as f64 / lat_ns.len() as f64 / 1000.0;
    let p = |q: f64| lat_ns[(lat_ns.len() as f64 * q) as usize] as f64 / 1000.0;
    let (rss, hwm) = rss_mib();
    println!(
        "  {label:<10} {:>9.0} gets/s  mean {mean:>8.1} us  p50 {:>8.1} us  p99 {:>9.1} us  rss {rss} MiB (hwm {hwm})",
        queries as f64 / wall,
        p(0.5),
        p(0.99),
    );
    checksum
}

/// Runs `total_keys` lookups in sorted, deduplicated batches of `batch`
/// drained keys, reporting per-batch latency and per-key throughput.
fn pass_batched<B: Backend>(
    label: &str,
    backend: &mut B,
    total_keys: usize,
    batch: usize,
    mut pick: impl FnMut() -> u64,
) -> u64 {
    let batches = (total_keys / batch).max(1);
    let mut lat_ns: Vec<u64> = Vec::with_capacity(batches);
    let mut checksum = 0u64;
    let mut served = 0usize;
    let mut keys = Vec::with_capacity(batch);
    let start = Instant::now();
    for _ in 0..batches {
        keys.clear();
        for _ in 0..batch {
            keys.push(pick());
        }
        keys.sort_unstable();
        keys.dedup();
        served += keys.len();
        let t = Instant::now();
        checksum = checksum.wrapping_add(std::hint::black_box(backend.get_batch(&keys)));
        lat_ns.push(u64::try_from(t.elapsed().as_nanos()).expect("fits"));
    }
    let wall = start.elapsed().as_secs_f64();
    lat_ns.sort_unstable();
    let mean = lat_ns.iter().sum::<u64>() as f64 / lat_ns.len() as f64 / 1000.0;
    let p = |q: f64| lat_ns[(lat_ns.len() as f64 * q) as usize] as f64 / 1000.0;
    let (rss, hwm) = rss_mib();
    println!(
        "  {label:<10} {:>9.0} keys/s  batch mean {mean:>8.1} us  p50 {:>8.1} us  p99 {:>9.1} us  rss {rss} MiB (hwm {hwm})",
        served as f64 / wall,
        p(0.5),
        p(0.99),
    );
    checksum
}

/// Hydration while serving: each round appends a step of new sorted records
/// (the batches hydration writes) and then serves one sorted probe window
/// against the existing data. This is the mixed read/write phase where
/// kernel-managed memory thrashes: write pressure evicts the read set, and
/// both sides fault against each other.
fn pass_mixed<B: Backend>(
    label: &str,
    backend: &mut B,
    queries: usize,
    batch: usize,
    append_start: u64,
    append_total: usize,
    mut pick: impl FnMut() -> u64,
) -> u64 {
    let windows = (queries / batch).max(1);
    let per_step = append_total / windows;
    let mut lat_ns: Vec<u64> = Vec::with_capacity(windows);
    let mut append_ns = 0u128;
    let mut checksum = 0u64;
    let mut served = 0usize;
    let mut next = append_start;
    let mut keys = Vec::with_capacity(batch);
    let start = Instant::now();
    for _ in 0..windows {
        let t = Instant::now();
        backend.append(next, per_step);
        next += per_step as u64;
        append_ns += t.elapsed().as_nanos();

        keys.clear();
        for _ in 0..batch {
            keys.push(pick());
        }
        keys.sort_unstable();
        keys.dedup();
        served += keys.len();
        let t = Instant::now();
        checksum = checksum.wrapping_add(std::hint::black_box(backend.get_batch(&keys)));
        lat_ns.push(u64::try_from(t.elapsed().as_nanos()).expect("fits"));
    }
    let wall = start.elapsed().as_secs_f64();
    lat_ns.sort_unstable();
    let p = |q: f64| lat_ns[(lat_ns.len() as f64 * q) as usize] as f64 / 1000.0;
    let appended_mib = (windows * per_step * REC_WORDS * 8) as f64 / (1 << 20) as f64;
    let (rss, hwm) = rss_mib();
    println!(
        "  {label:<10} append {:>6.0} MiB/s  probe {:>9.0} keys/s  window p50 {:>8.1} us  p99 {:>9.1} us  rss {rss} MiB (hwm {hwm})  ({:.1}s wall)",
        appended_mib / (append_ns as f64 / 1e9),
        served as f64 / (lat_ns.iter().sum::<u64>() as f64 / 1e9),
        p(0.5),
        p(0.99),
        wall,
    );
    checksum
}

fn run<B: Backend>(records: u64, queries: usize, set_admit: impl Fn(&mut B, bool)) {
    let t = Instant::now();
    let mut b = B::load(records);
    let (rss, hwm) = rss_mib();
    println!(
        "  load       {:>9.1} s                                                    rss {rss} MiB (hwm {hwm})",
        t.elapsed().as_secs_f64()
    );

    // Uniform: every key equally likely.
    let mut r = rng(0xDEC0DE);
    let uniform = pass("uniform", &mut b, queries, || r() % records);

    // Skewed: 99% of lookups in the hottest 1% of the key space.
    set_admit(&mut b, true);
    let mut r = rng(0xDEC0DE);
    let hot = (records / 100).max(1);
    let skewed = pass("skewed", &mut b, queries, || {
        if r() % 100 == 0 {
            r() % records
        } else {
            r() % hot
        }
    });

    // Batched probes: the operator drains a window of keys and fetches them
    // in one pass (a join's delta batch, the upsert drain's sorted window).
    // Real windows arrive at whatever fill the input produced, so sweep the
    // window size; the single-get passes above are the size-1 endpoint.
    let sizes: Vec<usize> = std::env::var("RA_BATCHES")
        .unwrap_or_else(|_| "16,64,256,1024".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let mut sums = vec![
        ("uniform".to_string(), uniform),
        ("skewed".to_string(), skewed),
    ];
    for &bsz in &sizes {
        set_admit(&mut b, false);
        let mut r = rng(0xBA7C4 ^ bsz as u64);
        let sum = pass_batched(&format!("b-uni-{bsz}"), &mut b, queries, bsz, || {
            r() % records
        });
        sums.push((format!("b-uni-{bsz}"), sum));
    }
    for &bsz in &sizes {
        set_admit(&mut b, true);
        let mut r = rng(0xBA7C4 ^ bsz as u64);
        let sum = pass_batched(&format!("b-skew-{bsz}"), &mut b, queries, bsz, || {
            if r() % 100 == 0 {
                r() % records
            } else {
                r() % hot
            }
        });
        sums.push((format!("b-skew-{bsz}"), sum));
    }
    // Hydration while serving: skewed probe windows against write pressure.
    set_admit(&mut b, true);
    let mut r = rng(0x1D8A7E);
    let mixed = pass_mixed(
        "mix-hydr",
        &mut b,
        queries,
        1024,
        records,
        usize::try_from(records / 4).expect("fits"),
        || {
            if r() % 100 == 0 {
                r() % records
            } else {
                r() % hot
            }
        },
    );
    sums.push(("mix-hydr".to_string(), mixed));

    let rendered: Vec<String> = sums.iter().map(|(l, s)| format!("{l} {s:#x}")).collect();
    println!("  checksums  {}", rendered.join(" "));
}

fn main() {
    let backend = std::env::var("BACKEND").unwrap_or_else(|_| "mem".into());
    let records = env_usize("RA_RECORDS", 16_000_000) as u64;
    let queries = env_usize("RA_QUERIES", 100_000);
    println!(
        "== backend {backend}: {records} records (~{} MiB logical), {queries} queries per pass ==",
        records * (REC_WORDS as u64) * 8 >> 20,
    );
    match backend.as_str() {
        "mem" => run::<MemBackend>(records, queries, |_, _| ()),
        "pool" | "pool-sub" => run::<PoolBackend>(records, queries, |b, admit| b.admit = admit),
        "rocksdb" => run::<RocksBackend>(records, queries, |_, _| ()),
        other => panic!("unknown BACKEND {other:?}"),
    }
}
