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

//! Merge-batcher-style workload for the pager.
//!
//! Builds two chains of 2 MiB chunks (`--chain-gib` each), then performs a
//! merge pass that takes one chunk from each input, reads every cache line,
//! and emits two output chunks. Reports build/merge throughput.
//!
//! Run with constrained memory via `systemd-run --user --scope -p MemoryMax=...`.
//!
//! ```bash
//! cargo build --release --features pager --example pager_merge
//! systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=64G --quiet \
//!   --setenv=MZ_PAGER_SCRATCH=/path/to/scratch \
//!   -- target/release/examples/pager_merge --chain-gib 16 --backend swap
//! ```

#![cfg(feature = "pager")]

use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::pager::{self, Backend, Handle};

const CHUNK_BYTES: usize = 2 * 1024 * 1024;
const CHUNK_U64: usize = CHUNK_BYTES / 8;
const CACHE_LINE_BYTES: usize = 64;
const CACHE_LINE_U64: usize = CACHE_LINE_BYTES / 8;

fn main() {
    let args: Vec<String> = env::args().collect();
    let chain_gib: usize = parse_arg(&args, "--chain-gib", 16);
    let prefetch_depth: usize = parse_arg(&args, "--prefetch-depth", 1);
    let backend = parse_backend(&args);
    let scratch: PathBuf = env::var_os("MZ_PAGER_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);

    pager::set_scratch_dir(scratch);
    pager::set_backend(backend);

    let chain_bytes = chain_gib * 1024 * 1024 * 1024;
    let chunks_per_chain = chain_bytes / CHUNK_BYTES;

    println!(
        "backend={backend:?} chain={chain_gib}GiB chunks_per_chain={chunks_per_chain} chunk={CHUNK_BYTES}B prefetch_depth={prefetch_depth}"
    );

    let (chain_a, build_a) = time(|| build_chain(chunks_per_chain));
    println!(
        "build A: {:.2?} ({:.2} GiB/s)",
        build_a,
        gib_per_sec(chain_bytes, build_a)
    );

    let (chain_b, build_b) = time(|| build_chain(chunks_per_chain));
    println!(
        "build B: {:.2?} ({:.2} GiB/s)",
        build_b,
        gib_per_sec(chain_bytes, build_b)
    );

    let (chain_c, merge_dur) = time(|| merge_pass(chain_a, chain_b, prefetch_depth));
    let merged_bytes = chunks_per_chain * 2 * CHUNK_BYTES;
    println!(
        "merge: {:.2?} ({:.2} GiB/s through, output_chunks={})",
        merge_dur,
        gib_per_sec(merged_bytes, merge_dur),
        chain_c.len()
    );

    let (_, drop_dur) = time(|| drop(chain_c));
    println!("drop output chain: {:.2?}", drop_dur);
}

fn build_chain(n_chunks: usize) -> Vec<Handle> {
    let mut chain = Vec::with_capacity(n_chunks);
    let mut buf: Vec<u64> = vec![0; CHUNK_U64];
    for i in 0..n_chunks {
        // Fill with non-zero, position-dependent data so the kernel cannot
        // share zero pages.
        for (j, w) in buf.iter_mut().enumerate() {
            *w = u64::cast_from(i) ^ u64::cast_from(j);
        }
        let mut chunks = [std::mem::take(&mut buf)];
        chain.push(pager::pageout(&mut chunks));
        // Reallocate; the swap backend stole the buffer, the file backend
        // left an empty Vec with original capacity, but we don't keep it.
        buf = vec![0; CHUNK_U64];
    }
    chain
}

fn merge_pass(a: Vec<Handle>, b: Vec<Handle>, prefetch_depth: usize) -> Vec<Handle> {
    let n = a.len().min(b.len());
    let mut a: Vec<Option<Handle>> = a.into_iter().map(Some).collect();
    let mut b: Vec<Option<Handle>> = b.into_iter().map(Some).collect();
    let mut out = Vec::with_capacity(2 * n);
    let mut tmp_a: Vec<u64> = Vec::with_capacity(CHUNK_U64);
    let mut tmp_b: Vec<u64> = Vec::with_capacity(CHUNK_U64);
    let mut sink: u64 = 0;
    // Maintain a rolling window of `prefetch_depth` outstanding prefetches.
    // Issue the initial wave for indices [0, prefetch_depth).
    let initial = prefetch_depth.min(n);
    for j in 0..initial {
        if let Some(h) = a[j].as_ref() {
            pager::prefetch(h);
        }
        if let Some(h) = b[j].as_ref() {
            pager::prefetch(h);
        }
    }
    for i in 0..n {
        // Each iteration extends the window by one: prefetch index `i +
        // prefetch_depth` so that by the time we consume it the kernel has
        // had `prefetch_depth` chunks worth of compute time to make pages
        // available.
        let pf = i + prefetch_depth;
        if pf < n {
            if let Some(h) = a[pf].as_ref() {
                pager::prefetch(h);
            }
            if let Some(h) = b[pf].as_ref() {
                pager::prefetch(h);
            }
        }
        let ha = a[i].take().expect("handle a present");
        let hb = b[i].take().expect("handle b present");
        pager::take(ha, &mut tmp_a);
        pager::take(hb, &mut tmp_b);
        // Touch every cache line of both inputs (1 u64 per 64-byte line).
        sink = touch_cache_lines(&tmp_a, sink);
        sink = touch_cache_lines(&tmp_b, sink);
        // Emit two output chunks, simulating a merged run that doubles the
        // chunk count. Each output is 2 MiB; we hand the original buffers
        // straight to `pageout`, which transfers ownership cleanly on the
        // swap backend.
        {
            let mut chunks = [std::mem::take(&mut tmp_a)];
            out.push(pager::pageout(&mut chunks));
            tmp_a = Vec::with_capacity(CHUNK_U64);
        }
        {
            let mut chunks = [std::mem::take(&mut tmp_b)];
            out.push(pager::pageout(&mut chunks));
            tmp_b = Vec::with_capacity(CHUNK_U64);
        }
    }
    std::hint::black_box(sink);
    out
}

#[inline]
fn touch_cache_lines(buf: &[u64], mut sink: u64) -> u64 {
    let mut i = 0;
    while i < buf.len() {
        sink = sink.wrapping_add(buf[i]);
        i += CACHE_LINE_U64;
    }
    sink
}

fn time<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let v = f();
    (v, start.elapsed())
}

fn gib_per_sec(bytes: usize, d: Duration) -> f64 {
    let secs = d.as_secs_f64();
    if secs == 0.0 {
        return 0.0;
    }
    let gib = f64::cast_lossy(bytes) / (1024.0 * 1024.0 * 1024.0);
    gib / secs
}

fn parse_arg(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_backend(args: &[String]) -> Backend {
    let pos = args
        .iter()
        .position(|a| a == "--backend")
        .and_then(|i| args.get(i + 1));
    match pos.map(String::as_str) {
        Some("file") => Backend::File,
        Some("swap") => Backend::Swap,
        Some(other) => panic!("unknown backend {other:?}; use 'swap' or 'file'"),
        None => Backend::Swap,
    }
}
