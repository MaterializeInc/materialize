"""Regenerate blob-store-benchmark-report.md from the four-store round.

Every table comes from blob-store-benchmark-seaweedfs.csv, which holds one
round with all four stores measured back to back. Findings from the two
earlier rounds (garage tuning, the noise band, the rustfs bug) are narrative,
since those CSVs no longer exist.

Not committed: benchmark reporting, not repository material.
"""

import csv
from collections import defaultdict

CSV = "blob-store-benchmark-seaweedfs.csv"
OUT = "blob-store-benchmark-report.md"
STORES = ["minio", "rustfs", "garage", "seaweedfs"]
CONC = 32
FILL8 = 8 * 1024**3

rows = list(csv.DictReader(open(CSV)))
by_key = {}
for r in rows:
    by_key[
        (
            r["store"],
            r["op"],
            int(r["size_bytes"]),
            int(r["fill_bytes"]),
            int(r["concurrency"]),
        )
    ] = r
sizes = sorted({int(r["size_bytes"]) for r in rows})
concs = sorted({int(r["concurrency"]) for r in rows})


def fb(n):
    n = float(n)
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if n < 1024 or unit == "GiB":
            s = f"{n:.0f}" if n == int(n) else f"{n:.1f}"
            return f"{s} {unit}"
        n /= 1024


def cell(store, op, size, fill, conc=CONC):
    return by_key.get((store, op, size, fill, conc))


def matrix(op, fill):
    unit = "ops/s" if op in ("delete", "list") else "MiB/s, ops/s"
    head = "| object size | " + " | ".join(
        f"{s} {unit} (p50 / p99 ms)" for s in STORES
    )
    lines = [head + " |", "|---|" + "---|" * len(STORES)]
    for size in sizes:
        out = []
        for store in STORES:
            r = cell(store, op, size, fill)
            if r is None:
                out.append("not measured")
                continue
            if op in ("delete", "list"):
                v = f"{float(r['ops_per_sec']):,.0f}"
            else:
                v = f"{float(r['mib_per_sec']):,.0f}, {float(r['ops_per_sec']):,.0f}"
            v += f" ({float(r['p50_ms']):.1f} / {float(r['p99_ms']):.1f})"
            if int(r["retries"]):
                v += f", {r['retries']} retries"
            out.append(v)
        lines.append(f"| {fb(size)} | " + " | ".join(out) + " |")
    return "\n".join(lines)


def scaling(op, size, fill=0):
    lines = [
        "| store | " + " | ".join(f"{c} in flight" for c in concs) + " |",
        "|---|" + "---|" * len(concs),
    ]
    for store in STORES:
        vals = []
        for c in concs:
            r = cell(store, op, size, fill, c)
            vals.append(f"{float(r['ops_per_sec']):,.0f}" if r else "n/a")
        lines.append(f"| {store} | " + " | ".join(vals) + " |")
    return "\n".join(lines)


def multiples():
    """Each store's throughput against minio's, at the empty store."""
    lines = ["| operation | rustfs | garage | seaweedfs |", "|---|---|---|---|"]
    for op in ["set", "get", "delete"]:
        ratios = defaultdict(list)
        for size in sizes:
            m = cell("minio", op, size, 0)
            if not m or float(m["ops_per_sec"]) == 0:
                continue
            for store in ["rustfs", "garage", "seaweedfs"]:
                r = cell(store, op, size, 0)
                if r and float(r["ops_per_sec"]) > 0:
                    ratios[store].append(
                        float(m["ops_per_sec"]) / float(r["ops_per_sec"])
                    )
        fmt = lambda v: f"{min(v):.1f}x to {max(v):.1f}x" if v else "n/a"
        lines.append(
            f"| {op} | {fmt(ratios['rustfs'])} | {fmt(ratios['garage'])} | {fmt(ratios['seaweedfs'])} |"
        )
    return "\n".join(lines)


def resources():
    lines = [
        "| store | peak RSS | peak CPU (cores) | RSS over the 4 KiB and 64 KiB cells | retries |",
        "|---|---|---|---|---|",
    ]
    for store in STORES:
        rs = [r for r in rows if r["store"] == store]
        mem = max(int(r["mem_bytes_max"]) for r in rs)
        cpu = max(float(r["cpu_pct_max"]) for r in rs) / 100
        small = [int(r["mem_bytes_max"]) for r in rs if int(r["size_bytes"]) <= 65536]
        ret = sum(int(r["retries"]) for r in rs)
        lines.append(
            f"| {store} | {fb(mem)} | {cpu:.1f} | {fb(min(small))} to {fb(max(small))} | {ret} |"
        )
    return "\n".join(lines)


def retry_cells():
    lines = []
    for r in rows:
        if int(r["retries"]):
            lines.append(
                f"- {r['store']} {r['op']} {fb(int(r['size_bytes']))} at {r['concurrency']} in flight, "
                f"{fb(int(r['fill_bytes']))} stored: {r['retries']} retries, "
                f"{float(r['ops_per_sec']):,.1f} ops/s, p50 {float(r['p50_ms']):,.0f} ms"
            )
    return "\n".join(lines)


sw_worst = cell("seaweedfs", "set", 67108864, 0, 128)
minio_same = cell("minio", "set", 67108864, 0, 128)

report = f"""# Blob store benchmark: minio, garage, rustfs, seaweedfs as persist blob stores

One macOS machine under OrbStack Docker (18 CPUs, 47 GiB), one store at a time,
no resource limits on any container. Materialize `main` at deb034b6ff plus the
changes listed at the end.

Every table here comes from a **single round with all four stores measured back
to back**, which is the only way these numbers are comparable (see the
confidence note below). minio, garage and rustfs completed the full sweep at
both stored volumes. seaweedfs completed the empty-store sweep and part of the
8 GiB one before the run was stopped, for reasons the seaweedfs section
explains.

## Read this first: how much confidence the numbers carry

An earlier pair of rounds measured minio twice with an identical configuration.
Its per-cell throughput still moved by a median of -6%, with a 10th-to-90th
range of -44% to +43% and extremes of -72% to +313%.

**A single-run cell in this harness is not interpretable below roughly 50%
difference.** The large, consistent gaps below clear that bar. Differences in
the tens of percent do not, and should be treated as unmeasured rather than
small. Repeating cells and taking medians is the fix, and the harness does not
do it yet.

## Summary

- **minio** is the reference: fastest or tied on most operations, and second
  hungriest at {fb(max(int(r['mem_bytes_max']) for r in rows if r['store'] == 'minio'))} peak RSS. Its large reads run at page-cache
  speed here, which real disks will not reproduce.
- **garage** is roughly 4x behind minio on reads at every size and saturates by
  32 operations in flight, but does it on a fraction of the memory. It is the
  only store whose ceiling sits below what a busy environment would generate.
- **rustfs** tracks minio on reads and writes, is 3 to 4x behind on deletes and
  slower on listing, and throttles concurrent large multipart uploads with
  `SlowDown`, which persist absorbs as retries.
- **seaweedfs** is the fastest of the four on small objects and on deletes at
  every size, and the worst by a wide margin on large writes. Under concurrent
  64 MiB writes it stops making progress altogether, and it peaked at
  {fb(max(int(r['mem_bytes_max']) for r in rows if r['store'] == 'seaweedfs'))} resident.

For persist specifically, which writes batch parts from a few hundred KiB up to
a 128 MiB target, seaweedfs is fastest where persist writes least and slowest
where it writes most. garage has the opposite shape: acceptable large writes,
slow reads everywhere.

## Method

Two instruments, both in this tree:

1. `bin/mzcompose --find parallel-benchmark run default --blob-store <store>
   --other-blob-store minio`: Materialize end to end, with persist's blob
   operation counts and latencies scraped from environmentd and every clusterd
   after each scenario. Answers "does swapping the store change what CI and
   users see". Too indirect to characterize a store, since persist picks the
   object sizes and Materialize's own compute dominates the latencies.
2. `bin/mzcompose --find blob-store-benchmark run default --fill 0 --fill 8GiB`:
   drives each store through persist's own S3 client (`persistcli bench blob`)
   over object sizes {", ".join(fb(s) for s in sizes)}; {", ".join(str(c) for c in concs)}
   operations in flight; at 0 and 8 GiB already stored. Each cell writes its
   objects, lists the store, reads at random for 10 s, then deletes, reporting
   throughput, p50/p90/p99/max, retries persist's retry loop needed, and the
   store container's peak CPU and memory from `docker stats`. Reads verify
   object length. Data: `{CSV}`.

### Store configuration

The minio image carries years of Materialize-specific tuning, which the other
stores had to be brought level with before any comparison meant much:

| | minio | garage | rustfs | seaweedfs |
|---|---|---|---|---|
| fsync | patched out of the source | off (default) | `RUSTFS_DURABILITY_MODE=none` | off (default) |
| redundancy | `EC:0`, single drive | `replication_factor = 1` | single volume, zero parity | single node |
| compression | none | `compression_level = "none"` | none | none |
| background repair | `MINIO_HEAL_DISABLE=on` | `disable_scrub = true` | attempted, see below | n/a |
| chunking | n/a | `block_size = "10M"` | n/a | 1 GiB volumes |
| read concurrency | unlimited | `block_max_concurrent_reads = 256` | n/a | unlimited |
| write concurrency | unlimited | `block_max_concurrent_writes_per_request = 30` | internally capped | internally capped |
| auth | static key | static key | static key | static key |

Notes on the choices that are not obvious:

- Garage stores every object as a chain of `block_size` files. At the 1 MiB
  default, one large-blob read becomes dozens of file opens contending for
  `block_max_concurrent_reads`, whose default of 16 exists as backpressure for
  spinning disks. Upstream recommends 10 MiB blocks for large files.
- rustfs runs one volume because that matches minio's single drive and zero
  parity. It refuses several volumes that share a device, so more drives would
  need more devices.
- rustfs's `RUSTFS_HEAL_ENABLED=false` and `RUSTFS_SCANNER_ENABLED=false` are
  set but appear inert, so its background work runs where minio's is disabled.
- seaweedfs is given an S3 key deliberately. With no S3 config it serves every
  request unauthenticated and skips the signature verification the other three
  perform, which would flatter it. Its volume size limit is raised from the
  128 MiB `mini` default to the 1 GiB its own `server` mode uses.

## Results

32 operations in flight. Throughput first, then p50 / p99 latency in
milliseconds. Only objects above 8 MiB are multipart uploads in persist.

### Writes, empty store

{matrix("set", 0)}

### Writes, {fb(FILL8)} already stored

{matrix("set", FILL8)}

### Reads, empty store

{matrix("get", 0)}

### Reads, {fb(FILL8)} already stored

{matrix("get", FILL8)}

### Deletes, empty store

{matrix("delete", 0)}

### Deletes, {fb(FILL8)} already stored

{matrix("delete", FILL8)}

### Listing

One `ListObjectsV2` walk of the whole prefix after the cell's objects are
written.

Empty store:

{matrix("list", 0)}

{fb(FILL8)} already stored:

{matrix("list", FILL8)}

### Scaling with concurrency (empty store, ops/s)

4 KiB writes:

{scaling("set", 4096)}

4 KiB reads:

{scaling("get", 4096)}

1 MiB writes:

{scaling("set", 1048576)}

64 MiB writes, where seaweedfs comes apart:

{scaling("set", 67108864)}

minio and rustfs keep gaining on reads through 128 operations in flight and
saturate writes around 32. Garage saturates both by 32. seaweedfs goes
backwards on large writes as concurrency rises.

### Standing relative to minio

Range across all five object sizes at the empty store, as a multiple of minio's
throughput. Above 1.0 is slower than minio, below 1.0 is faster.

{multiples()}

The consistent, noise-clearing results are garage's read gap at about 4x every
size, rustfs's delete gap, and seaweedfs's split between small objects, where
it beats minio, and large writes, where it is far behind.

### Retries

Persist's retry loop wraps every set, get and delete, so a throttling store
appears as latency plus a retry count rather than an error. Two stores needed
them:

{retry_cells()}

rustfs's are `SlowDown` from its write admission limit, returned promptly.
seaweedfs's are a different thing entirely, described next.

### Resources

Peak over the round, sampled about once a second per cell.

{resources()}

## seaweedfs comes apart under concurrent large writes

The worst cell in the whole exercise. 64 MiB objects with 128 operations in
flight, empty store:

| | seaweedfs | minio |
|---|---|---|
| throughput | {float(sw_worst['ops_per_sec']):.1f} ops/s, {float(sw_worst['mib_per_sec']):.1f} MiB/s | {float(minio_same['ops_per_sec']):.0f} ops/s, {float(minio_same['mib_per_sec']):,.0f} MiB/s |
| p50 per write | {float(sw_worst['p50_ms']):,.0f} ms, about {float(sw_worst['p50_ms']) / 60000:.0f} minutes | {float(minio_same['p50_ms']):,.0f} ms |
| retries | {sw_worst['retries']} | {minio_same['retries']} |

Reads in that same cell still completed at {float(cell("seaweedfs", "get", 67108864, 0, 128)['ops_per_sec']):.0f} ops/s, so writes
specifically fell over rather than the server dying. At 32 operations in flight
the same object size was slow but functional, so the failure is
concurrency-dependent, not purely size-dependent.

This is a harsher failure than rustfs's. rustfs refuses work promptly with
`SlowDown` and persist rides it out. seaweedfs accepts the work and then takes
roughly an hour and a half per write, while its resident memory climbs to
{fb(max(int(r['mem_bytes_max']) for r in rows if r['store'] == 'seaweedfs'))}, more than half the Docker VM. That single cell is why the
8 GiB sweep for seaweedfs is incomplete: the run was stopped rather than spend
hours more on a result already established.

On the other hand, seaweedfs returns `x-amz-mp-parts-count` correctly on
part-number reads, with correct `Content-Range` and byte-exact round-trips, so
it does not have the defect described next.

## Findings from earlier rounds

**rustfs omits `x-amz-mp-parts-count`.** `GetObject?partNumber=1` returns 206
with a correct `Content-Range` but no parts count, and `HeadObject` ignores
`partNumber`. minio, garage, seaweedfs and S3 all return the header. Persist
read multipart blobs part by part and took the missing header to mean one part,
so any cold read of a blob above the 8 MiB multipart threshold, which is every
large compaction output, returned the first 8 MiB and panicked with
`Invalid Parquet file. Corrupt footer`. The blob cache hid this for freshly
written blobs. Fixed in `S3Blob::get` (`src/persist/src/s3.rs`): the total size
is read from `Content-Range`, the remainder fetched by byte range when the
count is missing, and the reassembled length checked against the total, so any
store returning a short object fails the read rather than corrupting data.
Worth reporting upstream, since clients without such a fallback truncate
silently.

**Garage's defaults were badly wrong for persist's object sizes.** With 10 MiB
blocks and a raised read-concurrency cap, its 8 MiB reads went from 70 to 470
ops/s and their p50 from 454 ms to 66 ms, and an apparent "cold start" effect
disappeared: empty-store and warmed cells now agree. The configuration in
`test/garage/garage.toml` carries that reasoning. The garage numbers in this
report are the tuned ones.

## Caveats

- One machine, Docker under macOS, each store alone. Relative comparisons only,
  and see the confidence note for how large a difference has to be to mean
  anything.
- Large-object reads are served from page cache and would look different on
  real disks.
- No resource limits on any store, so the memory and CPU figures are what each
  takes unconstrained. Under a cgroup limit the ranking could change, and
  seaweedfs most of all.
- fsync is off everywhere. rustfs's default `strict` mode, which fsyncs every
  write and is what anyone would run in production, was not measured.
- seaweedfs's 8 GiB sweep covers only the smallest object sizes.
- garage ran at v2.4.1, rustfs at 1.0.0-rc.5, seaweedfs at 4.47.

## Reproducing

```
# the full matrix, all four stores (hours, because of seaweedfs)
bin/mzcompose --find blob-store-benchmark run default --fill 0 --fill 8GiB

# one store, one cell
bin/mzcompose --find blob-store-benchmark run default \\
  --blob-store seaweedfs --size 64MiB --concurrency 32 --read-secs 10

# end to end, one store against minio
bin/mzcompose --find parallel-benchmark run default \\
  --scenario BlobStoreReadsWrites --blob-store garage --other-blob-store minio
```

## Changes in this tree

On `jubrad/blob-store-benchmark` (draft PR #38800):

- `src/persist/src/s3.rs`: `Content-Range` fallback and length check in
  `S3Blob::get`, with unit tests.
- `src/persist-client/src/cli/bench.rs`: `persistcli bench blob`.
- `misc/python/materialize/mzcompose/services/garage.py`, `rustfs.py`,
  `blob_store.py`; `test/garage/` (mzbuild image, since the upstream garage
  image ships without a shell), including the tuning in `test/garage/garage.toml`.
- `misc/python/materialize/mzcompose/services/materialized.py`, `testdrive.py`:
  `external_blob_store` accepts a store name.
- `test/parallel-benchmark/mzcompose.py`: `--blob-store`, `--other-blob-store`,
  and a persist blob operation report after each scenario.
- `misc/python/materialize/parallel_benchmark/scenarios.py`:
  `BlobStoreReadsWrites`.
- `test/blob-store-benchmark/`: the direct benchmark composition.

Stacked on `jubrad/seaweedfs`, not pushed:

- `misc/python/materialize/mzcompose/services/seaweedfs.py` and its
  registration in the two compositions.

## Open work

- Repeat cells and report medians, so differences under 50% become readable.
- Finish seaweedfs's 8 GiB sweep, or drop the 128-in-flight large-object cells
  from the matrix, since one of them costs hours.
- Measure rustfs in its default `strict` durability mode.
- Report the `x-amz-mp-parts-count` omission to rustfs upstream.
"""

open(OUT, "w").write(report)
print(f"wrote {OUT}: {len(report.splitlines())} lines")
