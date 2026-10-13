//! Example: file-backed spill for the columnar `Chunk` trace.
//!
//! The columnar trace runs on the `Chunk` abstraction (`ColChunk`), which is
//! either resident or paged. `Chunk::settle` pages committed chunks out via
//! `differential_dataflow::trace::chunk::col::spill` once a per-worker record budget is
//! exceeded; reads fetch them back. The library serializes the trie and tracks
//! the budget — this example supplies the *backing store*: a [`BytesStore`] that
//! lz4-compresses each blob into a rotating tempfile, and a [`BytesSource`] that
//! reads it back.
//!
//! Run with: `cargo run --release --example columnar_spill -- --mode both`

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Static spill config, read by each worker when it installs its spiller (we
// can't thread parameters through `timely::execute`'s closure otherwise).
static ENABLE_SPILL: AtomicBool = AtomicBool::new(true);
static BUDGET_RECORDS: AtomicUsize = AtomicUsize::new(10_000_000);

/// Compressed bytes actually written to disk, summed across `FileStore`s. Paired
/// with `SpillStats::bytes_written` (the pre-compression size) for a ratio.
static BYTES_COMPRESSED: AtomicUsize = AtomicUsize::new(0);

/// Cross-worker registry of each worker's `SpillStats`, summed after a run.
static SHARED_STATS: OnceLock<Mutex<Vec<Arc<SpillStats>>>> = OnceLock::new();

fn register_stats(stats: Arc<SpillStats>) {
    SHARED_STATS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap().push(stats);
}

fn collect_stats() -> (usize, usize, usize) {
    if let Some(m) = SHARED_STATS.get() {
        let v = m.lock().unwrap();
        let chunks: usize = v.iter().map(|s| s.spilled_chunks.load(Ordering::Relaxed)).sum();
        let records: usize = v.iter().map(|s| s.spilled_records.load(Ordering::Relaxed)).sum();
        let decompressed: usize = v.iter().map(|s| s.bytes_written.load(Ordering::Relaxed)).sum();
        (chunks, records, decompressed)
    } else { (0, 0, 0) }
}

fn reset_stats() {
    if let Some(m) = SHARED_STATS.get() { m.lock().unwrap().clear(); }
    BYTES_COMPRESSED.store(0, Ordering::Relaxed);
}

use differential_dataflow::columnar::trace::{Batcher as ValBatcher, Builder as ValBuilder, Chunker as ValChunker, Spine as ValSpine};
use differential_dataflow::columnar::collection::Builder as ValColBuilder;
use differential_dataflow::columnar::trace::spill::{self, BytesSource, BytesStore, SpillStats};
use differential_dataflow::columnar::updates::{Updates, UpdatesTyped};
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::probe::{Handle as ProbeHandle, Probe};
use timely::dataflow::operators::Input;
use timely::dataflow::InputHandle;

/// A file-backed [`BytesStore`]: lz4-compresses each blob and appends it to a
/// rotating tempfile, handing back a [`FileSource`] that reads it back. Rotating
/// lets disk space reclaim as old `FileSource`s drop (the `Arc<File>` hits zero,
/// the unlinked tempfile closes, the OS frees the space).
struct FileStore {
    current: Option<Arc<Mutex<std::fs::File>>>,
    offset: u64,
}

impl FileStore {
    const ROTATE_AFTER_BYTES: u64 = 1 << 30; // 1 GiB

    fn new() -> Self { Self { current: None, offset: 0 } }

    fn file(&mut self) -> Arc<Mutex<std::fs::File>> {
        if self.current.is_none() || self.offset >= Self::ROTATE_AFTER_BYTES {
            self.current = Some(Arc::new(Mutex::new(tempfile::tempfile().expect("tempfile"))));
            self.offset = 0;
        }
        self.current.as_ref().unwrap().clone()
    }
}

impl BytesStore for FileStore {
    fn store(&mut self, bytes: &[u8]) -> Box<dyn BytesSource> {
        let decompressed_len = bytes.len();
        let compressed = lz4_flex::block::compress(bytes);
        BYTES_COMPRESSED.fetch_add(compressed.len(), Ordering::Relaxed);

        let file = self.file();
        let offset = self.offset;
        {
            let mut f = file.lock().unwrap();
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.write_all(&compressed).unwrap();
        }
        self.offset += compressed.len() as u64;

        Box::new(FileSource { file, offset, compressed_len: compressed.len(), decompressed_len })
    }
}

/// Reads back one blob written by [`FileStore`].
struct FileSource {
    file: Arc<Mutex<std::fs::File>>,
    offset: u64,
    compressed_len: usize,
    decompressed_len: usize,
}

impl BytesSource for FileSource {
    fn load(&self) -> Vec<u8> {
        let mut compressed = vec![0u8; self.compressed_len];
        {
            let mut f = self.file.lock().unwrap();
            f.seek(SeekFrom::Start(self.offset)).unwrap();
            f.read_exact(&mut compressed).unwrap();
        }
        lz4_flex::block::decompress(&compressed, self.decompressed_len).expect("lz4 decompress")
    }
}

type TestUpdate = (u64, u64, u64, i64);

fn make_chunk(updates: &[TestUpdate]) -> UpdatesTyped<TestUpdate> {
    use columnar::Push;
    let mut out = UpdatesTyped::<TestUpdate>::default();
    for (k, v, t, d) in updates { out.push((k, v, t, d)); }
    out.consolidate()
}

fn collect(chunk: &UpdatesTyped<TestUpdate>) -> Vec<TestUpdate> {
    chunk.iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect()
}

fn main() {
    // Direct codec + store round-trip: serialize a trie the way the library does,
    // persist it through the file store, read it back, and compare.
    {
        let chunk = make_chunk(&[(1, 10, 100, 1), (1, 10, 200, 2), (1, 20, 100, 3), (2, 20, 200, 5)]);
        let mut buf = Vec::new();
        Updates::<TestUpdate>::from(chunk.clone()).write_to(&mut buf);
        let mut store = FileStore::new();
        let source = store.store(&buf);
        let back = Updates::<TestUpdate>::read_from(
            timely::bytes::arc::BytesMut::from(source.load()).freeze()).into_typed();
        assert_eq!(collect(&chunk), collect(&back), "store round-trip mismatch");
        println!("ok: trie codec + file store round-tripped");
    }

    // End-to-end: a real timely dataflow whose columnar batcher pages to disk.
    let cfg = match parse_args() { Some(c) => c, None => return };

    let total_records = (cfg.times * cfg.keys_per_time) as usize * 2;
    let bytes_per_record = std::mem::size_of::<TestUpdate>();
    let raw_gb = (total_records * bytes_per_record) as f64 / (1u64 << 30) as f64;
    let per_worker_budget = cfg.budget / cfg.workers.max(1);
    println!(
        "config: times={} keys={} workers={} budget={} ({} per worker) mode={:?} sample_secs={}",
        cfg.times, cfg.keys_per_time, cfg.workers, cfg.budget, per_worker_budget, cfg.mode, cfg.sample_secs,
    );
    println!("workload: {} records ({:.2} GB raw, {} bytes/record)", total_records, raw_gb, bytes_per_record);

    if cfg.mode != Mode::Baseline {
        ENABLE_SPILL.store(true, Ordering::Relaxed);
        BUDGET_RECORDS.store(per_worker_budget, Ordering::Relaxed);
        reset_stats();
        let elapsed = run_timely_dataflow(cfg.times, cfg.keys_per_time, cfg.workers, cfg.sample_secs);
        let (chunks, records, decompressed) = collect_stats();
        let compressed = BYTES_COMPRESSED.load(Ordering::Relaxed);
        println!(
            "spill: {:.2}s | {:.2} M records/s | spilled {} chunks ({} records)",
            elapsed.as_secs_f64(), total_records as f64 / elapsed.as_secs_f64() / 1e6, chunks, records,
        );
        if decompressed > 0 {
            println!(
                "compression: {:.2} GB → {:.2} GB ({:.2}× ratio, lz4)",
                decompressed as f64 / (1u64 << 30) as f64,
                compressed as f64 / (1u64 << 30) as f64,
                decompressed as f64 / compressed.max(1) as f64,
            );
        }
    }

    if cfg.mode != Mode::Spill {
        ENABLE_SPILL.store(false, Ordering::Relaxed);
        reset_stats();
        let elapsed = run_timely_dataflow(cfg.times, cfg.keys_per_time, cfg.workers, cfg.sample_secs);
        println!(
            "baseline: {:.2}s | {:.2} M records/s",
            elapsed.as_secs_f64(), total_records as f64 / elapsed.as_secs_f64() / 1e6,
        );
    }
}

/// Run a single timely dataflow with `workers` worker threads. Each worker
/// installs a file-backed spiller (when `ENABLE_SPILL`), generates its share of
/// a cancelling workload, arranges it through the columnar `Chunk` trace, and
/// waits for the probe. Returns elapsed wall time.
fn run_timely_dataflow(times: u64, keys_per_time: u64, workers: usize, sample_secs: u64) -> std::time::Duration {
    let stop = Arc::new(AtomicBool::new(false));

    // RSS sampler thread.
    let stop_clone = stop.clone();
    let label = if ENABLE_SPILL.load(Ordering::Relaxed) { "spill" } else { "baseline" };
    let sampler = (sample_secs > 0).then(|| {
        std::thread::spawn(move || {
            let start = std::time::Instant::now();
            while !stop_clone.load(Ordering::Relaxed) {
                if let Some(rss) = rss_kb() {
                    println!("  [{}] +{:>5.0}s   RSS {:>9} kB", label, start.elapsed().as_secs_f64(), rss);
                }
                std::thread::sleep(std::time::Duration::from_secs(sample_secs));
            }
        })
    });

    let timer = std::time::Instant::now();

    timely::execute(timely::Config::process(workers), move |worker| {
        // Install this worker's spiller (per-worker budget, own tempfile + stats).
        if ENABLE_SPILL.load(Ordering::Relaxed) {
            let stats = Arc::new(SpillStats::default());
            register_stats(stats.clone());
            spill::install(BUDGET_RECORDS.load(Ordering::Relaxed), Box::new(FileStore::new()), stats);
        } else {
            spill::uninstall();
        }

        let index = worker.index();
        let peers = worker.peers();
        let mut input = <InputHandle<u64, ValColBuilder<TestUpdate>>>::new_with_builder();
        let mut probe: ProbeHandle<u64> = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {
            let stream = scope.input_from(&mut input);
            let arranged = arrange_core::<
                _, _,
                ValChunker<(u64, u64, u64, i64)>,
                ValBatcher<u64, u64, u64, i64>,
                ValBuilder<u64, u64, u64, i64>,
                ValSpine<u64, u64, u64, i64>,
            >(stream, Pipeline, "ColumnarSpillArrange");
            arranged.stream.probe_with(&mut probe);
        });

        // Push positives then negatives at the same timely time so they cancel
        // during the batcher's own merges. `mix` is a reversible bijection so the
        // post-sort columnar bytes look ~random (incompressible), keeping the
        // baseline-vs-spill comparison fair; it pairs positives with negatives.
        fn mix(k: u64) -> u64 { let x = k.wrapping_mul(0x9E3779B97F4A7C15); x ^ (x >> 32) }

        const STEP_EVERY: usize = 1 << 16;
        let mut sent_since_step = 0usize;
        for sign in [1i64, -1] {
            for t in 0..times {
                let mut k = index as u64;
                while k < keys_per_time {
                    let kh = mix(k);
                    let d = ((kh as i64) >> 1) | 1; // half-range, odd: nonzero, `-d` never overflows
                    input.send((kh, kh & 0x3, t, sign * d));
                    k += peers as u64;
                    sent_since_step += 1;
                    if sent_since_step >= STEP_EVERY { worker.step(); sent_since_step = 0; }
                }
            }
        }
        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) { worker.step(); }

        spill::uninstall();
    }).expect("timely::execute failed");

    let elapsed = timer.elapsed();
    stop.store(true, Ordering::Relaxed);
    if let Some(s) = sampler { let _ = s.join(); }
    elapsed
}

#[derive(Debug, PartialEq)]
enum Mode { Both, Spill, Baseline }

struct Config {
    times: u64,
    keys_per_time: u64,
    budget: usize,
    workers: usize,
    sample_secs: u64,
    mode: Mode,
}

fn parse_args() -> Option<Config> {
    let mut cfg = Config {
        times: 8,
        keys_per_time: 500_000,
        budget: 10_000_000,
        workers: 1,
        sample_secs: 0,
        mode: Mode::Both,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let take = |it: &mut dyn Iterator<Item = String>, name: &str| -> String {
            it.next().unwrap_or_else(|| { print_usage(); panic!("--{} requires a value", name) })
        };
        match a.as_str() {
            "-h" | "--help" => { print_usage(); return None; }
            "--times"   => { cfg.times = take(&mut it, "times").parse().expect("times: integer"); }
            "--keys"    => { cfg.keys_per_time = take(&mut it, "keys").parse().expect("keys: integer"); }
            "--budget"  => { cfg.budget = take(&mut it, "budget").parse().expect("budget: integer"); }
            "--workers" => { cfg.workers = take(&mut it, "workers").parse().expect("workers: integer"); }
            "--sample-secs" => { cfg.sample_secs = take(&mut it, "sample-secs").parse().expect("sample-secs: integer"); }
            "--mode"    => {
                cfg.mode = match take(&mut it, "mode").as_str() {
                    "both" => Mode::Both,
                    "spill" => Mode::Spill,
                    "baseline" => Mode::Baseline,
                    other => { print_usage(); panic!("unknown mode: {}", other); }
                };
            }
            other => { print_usage(); panic!("unknown arg: {}", other); }
        }
    }
    Some(cfg)
}

fn print_usage() {
    eprintln!("Usage: columnar_spill [OPTIONS]");
    eprintln!();
    eprintln!("  --times N            distinct data timestamps         (default 8)");
    eprintln!("  --keys N             keys per timestamp               (default 500000)");
    eprintln!("  --budget N           resident-record budget (split across workers)  (default 10000000)");
    eprintln!("  --workers N          timely worker threads            (default 1)");
    eprintln!("  --sample-secs N      print RSS every N seconds        (default 0 = off)");
    eprintln!("  --mode MODE          spill | baseline | both          (default both)");
    eprintln!();
    eprintln!("Total records pushed = 2 * times * keys (positives + negatives that cancel).");
}

/// Current process RSS in kB via `ps` (portable, no dep). `None` if unavailable.
fn rss_kb() -> Option<usize> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()]).output().ok()?;
    std::str::from_utf8(&output.stdout).ok()?.trim().parse::<usize>().ok()
}
