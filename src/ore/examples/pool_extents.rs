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

//! Single-process driver for the buffer pool's extent backends.
//!
//! Fills a pool with chunks, waits for the compressed tier to drain to its
//! cap, then reads random live chunks from concurrent threads. Prints per
//! phase wall and CPU time, and at the end every `PoolStats` field, the file
//! store's read-latency histogram, and derived rates. Run it once per backend
//! with identical arguments to compare swap and file mode. The memory and CPU
//! figures come from `/proc`, so they are Linux only. Spill threads are named
//! `pool-spill-N`, so `pidstat -t` attributes their CPU. A sampler polls
//! `spill_in_flight` every millisecond during the fill, drain and churn phases.
//!
//! ```text
//! cargo run --release -p mz-ore --features pool --example pool_extents -- \
//!     --backend file --dir target/pool-extents --budget-mib 256 \
//!     --rss-target-mib 512 --chunks 4096 --die-young 0.3 --readers 4 --reads 1000
//! ```

#![cfg(all(feature = "pool", unix))]

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::pool::{
    BackendKind, ChunkHandle, ChunkHints, ExtentBackend, ExtentCodec, IDENTITY_CODEC, Pool,
    READ_LATENCY_BUCKETS, SPILL_IN_FLIGHT_MAX,
};

const USAGE: &str = "\
usage: pool_extents [options]
  --backend swap|file        extent backend (default swap)
  --dir PATH                 file store directory, required for --backend file
  --file-capacity-mib N      cap the file store at N MiB (default: derived from the volume)
  --budget-mib N             resident-bytes budget (default 256)
  --rss-target-mib N         ceiling on pool RSS (default 512)
  --spill-threads N          spill threads (default 2)
  --chunks N                 chunks to insert (default 4096)
  --chunk-kib N              chunk body size in KiB, a multiple of 8 (default 2048)
  --compressibility F        fraction 0..1 of each body that is a repeating pattern,
                             the rest is random (default 0.5)
  --die-young F              fraction 0..1 of chunks dropped after insert (default 0)
  --die-lag N                a die-young chunk is dropped N inserts after its own
                             (default 0, right after insert). To die in the compressed
                             tier, die_lag * chunk bytes must exceed the budget
  --churn N                  after the first drain, replace a random live chunk with a
                             fresh insert N times, then drain again (default 0)
  --readers R                concurrent reader threads (default 0). Read latency is
                             closed-loop service time at concurrency R, and the caller
                             side percentiles include resident hits. The pool histogram
                             counts file reads only
  --reads N                  reads per reader thread (default 1000)
  --identity-fraction F      fraction 0..1 of chunks inserted with the identity codec
                             (default 0)
  --drain-timeout-secs N     longest drain wait (default 30)
  --seed N                   random seed (default 1)
  --help                     print this message";

/// A local codec matching `Lz4Codec` in `mz_timely_util::columnar::chunk`.
#[derive(Debug)]
struct Lz4Codec;

static LZ4_CODEC: Lz4Codec = Lz4Codec;

impl ExtentCodec for Lz4Codec {
    fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
        let max_out = lz4_flex::block::get_maximum_output_size(body.len());
        out.resize(4 + max_out, 0);
        let len = u32::try_from(body.len()).expect("chunk bodies are bounded by the size classes");
        out[..4].copy_from_slice(&len.to_le_bytes());
        let compressed = lz4_flex::block::compress_into(body, &mut out[4..])
            .expect("output sized to the maximum");
        out.truncate(4 + compressed);
    }

    fn decode(&self, stored: &[u8], body: &mut [u8]) {
        let prefix: [u8; 4] = stored[..4].try_into().expect("prefix length");
        let len = usize::try_from(u32::from_le_bytes(prefix)).expect("length fits usize");
        assert_eq!(len, body.len(), "destination must match the body length");
        let written = lz4_flex::block::decompress_into(&stored[4..], body)
            .expect("stored bytes hold a valid lz4 block");
        assert_eq!(written, body.len(), "decoded length mismatch");
    }
}

/// xorshift64*, enough for fill data and sampling.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // The state must be nonzero.
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A uniform value in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        f64::cast_lossy(self.next_u64() >> 11) / f64::cast_lossy(1u64 << 53)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::cast_from(self.next_u64() % u64::cast_from(n))
    }
}

struct Args {
    backend: String,
    dir: Option<PathBuf>,
    file_capacity_mib: Option<u64>,
    budget_mib: usize,
    rss_target_mib: usize,
    spill_threads: usize,
    chunks: usize,
    chunk_kib: usize,
    compressibility: f64,
    die_young: f64,
    die_lag: usize,
    churn: usize,
    readers: usize,
    reads: usize,
    identity_fraction: f64,
    drain_timeout_secs: u64,
    seed: u64,
}

fn parse_num<T: std::str::FromStr>(flag: &str, value: &str) -> T {
    value
        .parse()
        .unwrap_or_else(|_| panic!("bad value {value:?} for {flag}\n{USAGE}"))
}

impl Args {
    fn parse() -> Args {
        let mut raw = std::env::args().skip(1);
        let mut a = Args {
            backend: "swap".into(),
            dir: None,
            file_capacity_mib: None,
            budget_mib: 256,
            rss_target_mib: 512,
            spill_threads: 2,
            chunks: 4096,
            chunk_kib: 2048,
            compressibility: 0.5,
            die_young: 0.0,
            die_lag: 0,
            churn: 0,
            readers: 0,
            reads: 1000,
            identity_fraction: 0.0,
            drain_timeout_secs: 30,
            seed: 1,
        };
        while let Some(flag) = raw.next() {
            if flag == "--help" || flag == "-h" {
                println!("{USAGE}");
                std::process::exit(0);
            }
            let value = raw
                .next()
                .unwrap_or_else(|| panic!("{flag} needs a value\n{USAGE}"));
            match flag.as_str() {
                "--backend" => a.backend = value,
                "--dir" => a.dir = Some(PathBuf::from(value)),
                "--file-capacity-mib" => a.file_capacity_mib = Some(parse_num(&flag, &value)),
                "--budget-mib" => a.budget_mib = parse_num(&flag, &value),
                "--rss-target-mib" => a.rss_target_mib = parse_num(&flag, &value),
                "--spill-threads" => a.spill_threads = parse_num(&flag, &value),
                "--chunks" => a.chunks = parse_num(&flag, &value),
                "--chunk-kib" => a.chunk_kib = parse_num(&flag, &value),
                "--compressibility" => a.compressibility = parse_num(&flag, &value),
                "--die-young" => a.die_young = parse_num(&flag, &value),
                "--die-lag" => a.die_lag = parse_num(&flag, &value),
                "--churn" => a.churn = parse_num(&flag, &value),
                "--readers" => a.readers = parse_num(&flag, &value),
                "--reads" => a.reads = parse_num(&flag, &value),
                "--identity-fraction" => a.identity_fraction = parse_num(&flag, &value),
                "--drain-timeout-secs" => a.drain_timeout_secs = parse_num(&flag, &value),
                "--seed" => a.seed = parse_num(&flag, &value),
                other => panic!("unknown flag {other}\n{USAGE}"),
            }
        }
        for (name, f) in [
            ("--compressibility", a.compressibility),
            ("--die-young", a.die_young),
            ("--identity-fraction", a.identity_fraction),
        ] {
            assert!((0.0..=1.0).contains(&f), "{name} must be in 0..1\n{USAGE}");
        }
        assert!(
            a.chunk_kib > 0 && a.chunk_kib % 8 == 0,
            "--chunk-kib must be a positive multiple of 8"
        );
        a
    }
}

/// Process resident set sizes in MiB, as `(VmRSS, VmHWM)`.
fn rss_mib() -> (f64, f64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|rest| rest.trim().strip_suffix("kB"))
            .and_then(|kb| kb.trim().parse::<f64>().ok())
            .map_or(f64::NAN, |kb| kb / 1024.0)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

/// Process user and system CPU seconds, from the clock ticks
/// `/proc/self/stat` reports.
fn cpu_secs() -> (f64, f64) {
    // SAFETY: `sysconf` reads a system constant and takes no pointers.
    let ticks_per_sec = f64::cast_lossy(unsafe { libc::sysconf(libc::_SC_CLK_TCK) });
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // The command name may contain spaces, so count fields after its closing
    // parenthesis. `utime` and `stime` are fields 14 and 15, indexes 11 and
    // 12 after the state field.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks = |i: usize| {
        fields
            .get(i)
            .and_then(|f| f.parse::<f64>().ok())
            .map_or(f64::NAN, |t| t / ticks_per_sec)
    };
    (ticks(11), ticks(12))
}

/// Polls `spill_in_flight` every millisecond until stopped. Returns the peak,
/// the samples at `SPILL_IN_FLIGHT_MAX`, and the sample count.
struct Sampler {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<(u64, u64, u64)>,
}

impl Sampler {
    fn start(pool: &Pool) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (stop, pool) = (Arc::clone(&stop), pool.clone());
            std::thread::spawn(move || {
                let max = u64::cast_from(SPILL_IN_FLIGHT_MAX);
                let (mut peak, mut at_max, mut samples) = (0, 0, 0);
                while !stop.load(Ordering::Relaxed) {
                    let in_flight = pool.stats().spill_in_flight;
                    peak = peak.max(in_flight);
                    at_max += u64::from(in_flight >= max);
                    samples += 1;
                    std::thread::sleep(Duration::from_millis(1));
                }
                (peak, at_max, samples)
            })
        };
        Sampler { stop, thread }
    }

    fn stop(self) -> (u64, u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().expect("sampler")
    }
}

struct PhaseTimer {
    name: &'static str,
    start: Instant,
    cpu: (f64, f64),
    sampler: Option<Sampler>,
}

impl PhaseTimer {
    /// Starts a phase. Phases that queue spill work also sample the queue.
    fn start(name: &'static str, pool: &Pool, sample: bool) -> Self {
        PhaseTimer {
            name,
            start: Instant::now(),
            cpu: cpu_secs(),
            sampler: sample.then(|| Sampler::start(pool)),
        }
    }

    /// Prints the phase line and returns its wall time.
    fn finish(self, pool: &Pool) -> Duration {
        let wall = self.start.elapsed();
        let (user, sys) = cpu_secs();
        let (rss, hwm) = rss_mib();
        let stats = pool.stats();
        println!(
            "phase {:<5} wall={:.3}s user={:.2}s sys={:.2}s VmRSS={rss:.1}MiB VmHWM={hwm:.1}MiB \
             spill_in_flight={} extent_resident_mib={:.1} extent_file_mib={:.1}",
            self.name,
            wall.as_secs_f64(),
            user - self.cpu.0,
            sys - self.cpu.1,
            stats.spill_in_flight,
            mib(stats.extent_resident_bytes),
            mib(stats.extent_file_bytes),
        );
        if let Some(sampler) = self.sampler {
            let (peak, at_max, samples) = sampler.stop();
            println!(
                "  spill_in_flight peak={peak} (max {SPILL_IN_FLIGHT_MAX}) at_max_share={:.3} \
                 over {samples} samples",
                f64::cast_lossy(at_max) / f64::cast_lossy(samples.max(1)),
            );
        }
        wall
    }
}

/// Prints the bytes the file store holds on the filesystem against the
/// pool's live slot bytes.
fn print_file_space(label: &str, pool: &Pool) {
    if pool.backend_kind() == BackendKind::Swap {
        return;
    }
    let stats = pool.stats();
    println!(
        "  file space {label}: allocated={:.1}MiB live_slots={:.1}MiB punched={:.1}MiB",
        mib(stats.extent_file_allocated_bytes),
        mib(stats.extent_file_bytes),
        mib(stats.extent_file_holes_punched_bytes),
    );
}

fn mib(bytes: u64) -> f64 {
    f64::cast_lossy(bytes) / (1024.0 * 1024.0)
}

fn gib_per_sec(bytes: u64, wall: Duration) -> f64 {
    f64::cast_lossy(bytes) / (1024.0 * 1024.0 * 1024.0) / wall.as_secs_f64().max(1e-9)
}

/// Waits until the spill queue is empty and the pool reports the tier at or
/// below its cap, or until the tier stops moving. Returns why it stopped.
fn drain(pool: &Pool, timeout: Duration) -> &'static str {
    let start = Instant::now();
    let mut last = (u64::MAX, Instant::now());
    loop {
        let stats = pool.stats();
        if stats.spill_in_flight == 0 && !pool.compressed_tier_above_cap() {
            return "under cap";
        }
        if stats.extent_resident_bytes != last.0 {
            last = (stats.extent_resident_bytes, Instant::now());
        } else if stats.spill_in_flight == 0 && last.1.elapsed() > Duration::from_secs(2) {
            return "stalled above cap";
        }
        if start.elapsed() > timeout {
            return "timed out";
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Fills `body` with a repeating pattern over the first `pattern_words`
/// words and a rotation of `random` over the rest. The first and last words
/// carry `index`, which `check_body` verifies after a read.
fn fill_body(body: &mut [u64], index: u64, pattern_words: usize, random: &[u64]) {
    let (pattern, rest) = body.split_at_mut(pattern_words);
    for (i, w) in pattern.iter_mut().enumerate() {
        *w = 0xA5A5_0000 + u64::cast_from(i % 8);
    }
    let offset = usize::cast_from(index) % random.len();
    for (i, w) in rest.iter_mut().enumerate() {
        *w = random[(offset + i) % random.len()];
    }
    body[0] = index;
    *body.last_mut().expect("nonempty") = !index;
}

fn check_body(body: &[u64], index: u64, len: usize) {
    assert_eq!(body.len(), len, "read returned a short chunk");
    assert_eq!(body[0], index, "chunk {index} head corrupted");
    assert_eq!(body[len - 1], !index, "chunk {index} tail corrupted");
}

fn percentile_ms(sorted: &[Duration], p: f64) -> f64 {
    let i = usize::cast_lossy(f64::cast_lossy(sorted.len() - 1) * p);
    sorted[i].as_secs_f64() * 1e3
}

fn print_histogram(hist: &[u64; READ_LATENCY_BUCKETS]) {
    let total: u64 = hist.iter().sum();
    println!("extent_file_read_latency histogram ({total} file reads):");
    for (i, &count) in hist.iter().enumerate() {
        let label = if i == 0 {
            "< 32us".to_string()
        } else if i == READ_LATENCY_BUCKETS - 1 {
            format!(">= {}us", 16u64 << i)
        } else {
            format!("[{}us, {}us)", 16u64 << i, 32u64 << i)
        };
        println!("  {label:>18}  {count}");
    }
    // Upper bound of the bucket holding each percentile.
    let upper = |p: f64| {
        let rank = u64::cast_lossy(f64::cast_lossy(total) * p).max(1);
        let mut seen = 0;
        for (i, &count) in hist.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return if i == READ_LATENCY_BUCKETS - 1 {
                    "65.536ms+".to_string()
                } else {
                    format!("{}us", 32u64 << i)
                };
            }
        }
        "n/a".to_string()
    };
    if total > 0 {
        println!(
            "  p50 < {}  p99 < {} (bucket upper bounds)",
            upper(0.5),
            upper(0.99)
        );
    }
}

fn main() {
    let args = Args::parse();
    let budget = args.budget_mib * 1024 * 1024;
    let rss_target = args.rss_target_mib * 1024 * 1024;
    let chunk_words = args.chunk_kib * 1024 / 8;
    let chunk_bytes = u64::cast_from(args.chunk_kib * 1024);

    let backend = match args.backend.as_str() {
        "swap" => ExtentBackend::Swap,
        "file" => {
            let dir = args.dir.clone().expect("--backend file needs --dir");
            std::fs::create_dir_all(&dir).expect("create --dir");
            ExtentBackend::File {
                dir,
                capacity_bytes: args.file_capacity_mib.map(|m| m * 1024 * 1024),
            }
        }
        other => panic!("unknown backend {other:?}, use 'swap' or 'file'"),
    };
    let pool = Pool::with_backend(backend).expect("create pool");
    let kind = pool.backend_kind();
    println!("backend requested={} actual={kind:?}", args.backend);
    // A file store that cannot be used degrades to swap, which would make a
    // file-mode measurement silently measure swap.
    if args.backend == "file" && kind == BackendKind::Swap {
        eprintln!("--backend file ran on the swap backend, refusing to measure");
        std::process::exit(1);
    }
    pool.set_budget(budget);
    pool.set_rss_target(rss_target);
    pool.set_spill_threads(args.spill_threads);
    println!("compressed tier cap={:.1}MiB", mib(pool.compressed_cap()));

    let mut rng = Rng::new(args.seed);
    let pattern_words = usize::cast_lossy(f64::cast_lossy(chunk_words) * args.compressibility);
    // NOTE: every chunk's random part is a rotation of this one array, so a
    // deduplicating or compressing filesystem would see unrepresentative data.
    let random: Vec<u64> = (0..chunk_words - pattern_words + 1)
        .map(|_| rng.next_u64())
        .collect();
    let drain_timeout = Duration::from_secs(args.drain_timeout_secs);

    let inserted_bytes = std::cell::Cell::new(0u64);
    let insert = |rng: &mut Rng, index: u64| {
        let codec: &'static dyn ExtentCodec = if rng.unit() < args.identity_fraction {
            &IDENTITY_CODEC
        } else {
            &LZ4_CODEC
        };
        inserted_bytes.set(inserted_bytes.get() + chunk_bytes);
        pool.insert_with(chunk_words, ChunkHints::default(), codec, |body| {
            fill_body(body, index, pattern_words, &random)
        })
    };

    // Chunks selected to die young are dropped `die_lag` inserts after their
    // own, in insert order.
    let phase = PhaseTimer::start("fill", &pool, true);
    let mut live: Vec<(u64, ChunkHandle)> = Vec::new();
    let mut doomed: VecDeque<(usize, ChunkHandle)> = VecDeque::new();
    for i in 0..args.chunks {
        while doomed.front().is_some_and(|(due, _)| *due <= i) {
            doomed.pop_front();
        }
        let index = u64::cast_from(i);
        let handle = insert(&mut rng, index);
        if rng.unit() < args.die_young {
            doomed.push_back((i + args.die_lag + 1, handle));
        } else {
            live.push((index, handle));
        }
    }
    drop(doomed);
    let fill_wall = phase.finish(&pool);
    let fill_bytes = inserted_bytes.get();
    println!(
        "  inserted {} chunks, {} live after fill",
        args.chunks,
        live.len()
    );

    let phase = PhaseTimer::start("drain", &pool, true);
    let why = drain(&pool, drain_timeout);
    // Wall time of every phase that writes extents, the denominator of the
    // write rates. The final stats count the churn phase's writes too.
    let mut write_wall = fill_wall + phase.finish(&pool);
    println!("  drain ended: {why}");
    print_file_space("after drain", &pool);

    // Steady state: each replacement frees a chunk that may sit in the tier or
    // on file, and inserts a fresh one that pushes the tier over its cap again.
    if args.churn > 0 && !live.is_empty() {
        let phase = PhaseTimer::start("churn", &pool, true);
        for k in 0..args.churn {
            let index = u64::cast_from(args.chunks + k);
            let handle = insert(&mut rng, index);
            let slot = rng.below(live.len());
            live[slot] = (index, handle);
        }
        write_wall += phase.finish(&pool);
        let phase = PhaseTimer::start("drain2", &pool, true);
        let why = drain(&pool, drain_timeout);
        write_wall += phase.finish(&pool);
        println!("  drain ended: {why}");
        print_file_space("after churn", &pool);
    }

    let mut latencies: Vec<Duration> = Vec::new();
    let mut read_wall = Duration::ZERO;
    if args.readers > 0 && !live.is_empty() {
        let phase = PhaseTimer::start("read", &pool, false);
        let live = &live;
        let per_thread: Vec<Vec<Duration>> = std::thread::scope(|s| {
            let threads: Vec<_> = (0..args.readers)
                .map(|r| {
                    let seed = args.seed.wrapping_add(u64::cast_from(r) + 1);
                    let reads = args.reads;
                    s.spawn(move || {
                        let mut rng = Rng::new(seed);
                        let mut dst = Vec::new();
                        let mut lat = Vec::with_capacity(reads);
                        for _ in 0..reads {
                            let (index, handle) = &live[rng.below(live.len())];
                            let t = Instant::now();
                            handle.read_into(&mut dst);
                            lat.push(t.elapsed());
                            check_body(&dst, *index, chunk_words);
                        }
                        lat
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|t| t.join().expect("reader"))
                .collect()
        });
        latencies = per_thread.into_iter().flatten().collect();
        read_wall = phase.finish(&pool);
        print_file_space("after reads", &pool);
    }

    let final_stats = pool.stats();
    let (rss, hwm) = rss_mib();

    // Dropping every chunk shows how much file space the store keeps after
    // all slots are free.
    let phase = PhaseTimer::start("free", &pool, false);
    drop(live);
    phase.finish(&pool);
    print_file_space("after free", &pool);

    println!("\nfinal stats (before the free phase):\n{final_stats:#?}");
    print_histogram(&final_stats.extent_file_read_latency);
    println!("VmRSS={rss:.1}MiB VmHWM={hwm:.1}MiB");

    let elided = final_stats.extent_demotions_elided;
    let file_write_bytes = final_stats.extent_file_write_bytes_identity
        + final_stats.extent_file_write_bytes_compressed;
    // Counts every extent write, identity-coded ones included, so it only
    // approximates the codec's ratio when `--identity-fraction` is 0.
    let extents_written = final_stats.evictions_compress + final_stats.eager_backs;
    println!("\nderived:");
    println!(
        "  insert rate           {:.3} GiB/s ({:.1} MiB in {:.3}s)",
        gib_per_sec(fill_bytes, fill_wall),
        mib(fill_bytes),
        fill_wall.as_secs_f64()
    );
    println!(
        "  extent write rate     {:.3} GiB/s over the writing phases ({:.1} MiB stored)",
        gib_per_sec(final_stats.extent_bytes_written, write_wall),
        mib(final_stats.extent_bytes_written)
    );
    println!(
        "  stored/body ratio     {:.3} ({} extents written, compressibility {})",
        f64::cast_lossy(final_stats.extent_bytes_written)
            / f64::cast_lossy((extents_written * chunk_bytes).max(1)),
        extents_written,
        args.compressibility
    );
    // Every file write counter counts the same writes, so the bytes and the
    // counts divide into each other.
    println!(
        "  demotion rate         {:.3} GiB/s over the writing phases ({:.1} MiB written, {:.1}% identity, {:.1} KiB per write)",
        gib_per_sec(file_write_bytes, write_wall),
        mib(file_write_bytes),
        100.0 * f64::cast_lossy(final_stats.extent_file_write_bytes_identity)
            / f64::cast_lossy(file_write_bytes.max(1)),
        f64::cast_lossy(file_write_bytes)
            / f64::cast_lossy(final_stats.extent_file_writes.max(1))
            / 1024.0
    );
    println!(
        "  inline share          {:.3} ({} of {} file writes ran in an inline pass)",
        f64::cast_lossy(final_stats.extent_file_writes_inline)
            / f64::cast_lossy(final_stats.extent_file_writes.max(1)),
        final_stats.extent_file_writes_inline,
        final_stats.extent_file_writes
    );
    println!(
        "  demotion elision rate {:.3} ({elided} elided, {} pageouts)",
        f64::cast_lossy(elided) / f64::cast_lossy((elided + final_stats.extent_pageouts).max(1)),
        final_stats.extent_pageouts
    );
    println!(
        "  repeat read share     {:.3} ({} of {} file reads)",
        f64::cast_lossy(final_stats.extent_file_repeat_reads)
            / f64::cast_lossy(final_stats.extent_file_reads.max(1)),
        final_stats.extent_file_repeat_reads,
        final_stats.extent_file_reads
    );
    if !latencies.is_empty() {
        latencies.sort_unstable();
        println!(
            "  reads/s               {:.0} ({} reads in {:.3}s, {:.3} GiB/s decoded)",
            f64::cast_lossy(latencies.len()) / read_wall.as_secs_f64().max(1e-9),
            latencies.len(),
            read_wall.as_secs_f64(),
            gib_per_sec(u64::cast_from(latencies.len()) * chunk_bytes, read_wall)
        );
        println!(
            "  read_into service time p50={:.3}ms p99={:.3}ms max={:.3}ms \
             (closed loop at {} readers, includes resident hits)",
            percentile_ms(&latencies, 0.5),
            percentile_ms(&latencies, 0.99),
            percentile_ms(&latencies, 1.0),
            args.readers
        );
    }
}
