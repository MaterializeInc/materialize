# Blob store benchmark: minio, garage, rustfs, seaweedfs as persist blob stores

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
  hungriest at 5.6 GiB peak RSS. Its large reads run at page-cache
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
  25.8 GiB resident.

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
   over object sizes 4 KiB, 64 KiB, 1 MiB, 8 MiB, 64 MiB; 1, 8, 32, 128
   operations in flight; at 0 and 8 GiB already stored. Each cell writes its
   objects, lists the store, reads at random for 10 s, then deletes, reporting
   throughput, p50/p90/p99/max, retries persist's retry loop needed, and the
   store container's peak CPU and memory from `docker stats`. Reads verify
   object length. Data: `blob-store-benchmark-seaweedfs.csv`.

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

| object size | minio MiB/s, ops/s (p50 / p99 ms) | rustfs MiB/s, ops/s (p50 / p99 ms) | garage MiB/s, ops/s (p50 / p99 ms) | seaweedfs MiB/s, ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 34, 8,706 (3.4 / 11.0) | 20, 5,215 (5.9 / 11.4) | 12, 3,166 (9.4 / 22.5) | 46, 11,708 (2.4 / 7.8) |
| 64 KiB | 419, 6,699 (4.4 / 14.6) | 321, 5,131 (6.0 / 12.6) | 124, 1,977 (15.3 / 34.8) | 536, 8,576 (3.4 / 9.5) |
| 1 MiB | 924, 924 (26.5 / 66.7) | 1,567, 1,567 (19.6 / 34.3) | 906, 906 (31.7 / 67.0) | 251, 251 (17.5 / 1020.5) |
| 8 MiB | 2,348, 294 (107.3 / 108.9) | 1,879, 235 (132.7 / 136.0) | 1,182, 148 (196.6 / 215.5) | 244, 30 (270.3 / 309.3) |
| 64 MiB | 1,924, 30 (1060.6 / 1064.1) | 1,308, 20 (1307.2 / 1509.7), 17 retries | 1,289, 20 (1556.8 / 1583.1) | 586, 9 (3412.0 / 3495.0) |

### Writes, 8 GiB already stored

| object size | minio MiB/s, ops/s (p50 / p99 ms) | rustfs MiB/s, ops/s (p50 / p99 ms) | garage MiB/s, ops/s (p50 / p99 ms) | seaweedfs MiB/s, ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 30, 7,781 (3.8 / 11.9) | 22, 5,576 (5.2 / 11.2) | 6, 1,413 (21.5 / 46.2) | 36, 9,220 (2.7 / 14.7) |
| 64 KiB | 399, 6,379 (4.6 / 14.9) | 291, 4,662 (6.5 / 12.3) | 82, 1,311 (23.1 / 54.4) | not measured |
| 1 MiB | 804, 804 (36.1 / 82.2) | 1,667, 1,667 (17.1 / 40.7) | 930, 930 (31.6 / 58.5) | not measured |
| 8 MiB | 2,360, 295 (106.5 / 108.4) | 1,764, 220 (137.6 / 144.5) | 1,133, 142 (217.5 / 224.9) | not measured |
| 64 MiB | 1,877, 29 (1085.9 / 1091.2) | 1,285, 20 (1146.3 / 1532.9), 8 retries | 1,355, 21 (1491.2 / 1504.4) | not measured |

### Reads, empty store

| object size | minio MiB/s, ops/s (p50 / p99 ms) | rustfs MiB/s, ops/s (p50 / p99 ms) | garage MiB/s, ops/s (p50 / p99 ms) | seaweedfs MiB/s, ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 106, 27,149 (1.0 / 3.8) | 110, 28,166 (1.0 / 2.8) | 21, 5,353 (5.9 / 10.2) | 83, 21,355 (1.3 / 4.2) |
| 64 KiB | 1,461, 23,376 (1.2 / 5.2) | 1,594, 25,501 (1.2 / 2.9) | 261, 4,171 (7.5 / 12.7) | 1,124, 17,985 (1.6 / 5.0) |
| 1 MiB | 8,879, 8,879 (3.1 / 10.1) | 7,763, 7,763 (3.9 / 8.4) | 1,860, 1,860 (16.8 / 27.2) | 10,631, 10,631 (2.7 / 7.5) |
| 8 MiB | 17,994, 2,249 (13.4 / 30.7) | 11,729, 1,466 (20.8 / 43.3) | 3,919, 490 (64.8 / 89.8) | 19,578, 2,447 (12.2 / 28.9) |
| 64 MiB | 16,330, 255 (105.6 / 636.7) | 11,795, 184 (168.3 / 312.4) | 3,841, 60 (518.9 / 717.2) | 3,160, 49 (529.7 / 1864.1) |

### Reads, 8 GiB already stored

| object size | minio MiB/s, ops/s (p50 / p99 ms) | rustfs MiB/s, ops/s (p50 / p99 ms) | garage MiB/s, ops/s (p50 / p99 ms) | seaweedfs MiB/s, ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 83, 21,214 (1.4 / 4.1) | 102, 26,062 (1.1 / 3.2) | 3, 764 (5.9 / 11.4) | 80, 20,548 (1.4 / 4.5) |
| 64 KiB | 1,181, 18,893 (1.5 / 4.8) | 1,413, 22,612 (1.3 / 3.4) | 245, 3,926 (8.0 / 13.5) | not measured |
| 1 MiB | 8,500, 8,500 (3.4 / 10.2) | 7,081, 7,081 (4.3 / 9.2) | 2,148, 2,148 (14.5 / 24.6) | not measured |
| 8 MiB | 16,991, 2,124 (14.2 / 31.8) | 10,351, 1,294 (23.7 / 47.0) | 4,424, 553 (57.2 / 80.4) | not measured |
| 64 MiB | 16,849, 263 (105.4 / 579.8) | 11,126, 174 (175.7 / 414.1) | 4,448, 70 (447.6 / 628.6) | not measured |

### Deletes, empty store

| object size | minio ops/s (p50 / p99 ms) | rustfs ops/s (p50 / p99 ms) | garage ops/s (p50 / p99 ms) | seaweedfs ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 13,076 (2.1 / 7.2) | 3,170 (9.7 / 15.7) | 5,761 (5.1 / 13.2) | 13,111 (2.3 / 5.8) |
| 64 KiB | 11,394 (2.5 / 7.8) | 3,008 (10.2 / 16.1) | 5,818 (5.0 / 14.1) | 13,049 (2.3 / 5.2) |
| 1 MiB | 5,548 (5.8 / 12.6) | 1,773 (17.0 / 26.6) | 5,598 (4.7 / 13.3) | 11,660 (2.5 / 5.2) |
| 8 MiB | 3,669 (7.1 / 8.6) | 1,245 (22.1 / 25.4) | 3,671 (7.0 / 8.6) | 5,157 (5.0 / 6.2) |
| 64 MiB | 2,080 (10.1 / 15.0) | 1,101 (23.0 / 29.0) | 3,112 (8.6 / 10.2) | 5,686 (4.9 / 5.3) |

### Deletes, 8 GiB already stored

| object size | minio ops/s (p50 / p99 ms) | rustfs ops/s (p50 / p99 ms) | garage ops/s (p50 / p99 ms) | seaweedfs ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 8,526 (3.2 / 11.1) | 3,212 (9.6 / 16.1) | 2,736 (9.9 / 37.9) | 11,408 (2.6 / 6.3) |
| 64 KiB | 9,363 (3.0 / 8.9) | 3,281 (9.3 / 16.0) | 5,013 (5.7 / 16.8) | not measured |
| 1 MiB | 4,773 (6.0 / 19.6) | 1,494 (20.2 / 33.3) | 5,515 (4.9 / 14.5) | not measured |
| 8 MiB | 3,094 (6.9 / 10.2) | 1,050 (25.9 / 29.9) | 4,915 (4.8 / 6.4) | not measured |
| 64 MiB | 2,654 (7.8 / 11.0) | 935 (30.8 / 33.9) | 3,261 (8.4 / 9.7) | not measured |

### Listing

One `ListObjectsV2` walk of the whole prefix after the cell's objects are
written.

Empty store:

| object size | minio ops/s (p50 / p99 ms) | rustfs ops/s (p50 / p99 ms) | garage ops/s (p50 / p99 ms) | seaweedfs ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 46,840 (87.5 / 87.5) | 14,592 (280.7 / 280.7) | 56,875 (72.0 / 72.0) | 151,640 (27.0 / 27.0) |
| 64 KiB | 45,413 (90.2 / 90.2) | 4,093 (1000.8 / 1000.8) | 52,464 (78.1 / 78.1) | 126,496 (32.4 / 32.4) |
| 1 MiB | 62,599 (4.1 / 4.1) | 10,575 (24.2 / 24.2) | 13,436 (19.1 / 19.1) | 36,365 (7.0 / 7.0) |
| 8 MiB | 31,228 (1.0 / 1.0) | 12,901 (2.5 / 2.5) | 3,100 (10.3 / 10.3) | 5,843 (5.5 / 5.5) |
| 64 MiB | 18,280 (1.8 / 1.8) | 7,690 (4.2 / 4.2) | 1,728 (18.5 / 18.5) | 4,480 (7.1 / 7.1) |

8 GiB already stored:

| object size | minio ops/s (p50 / p99 ms) | rustfs ops/s (p50 / p99 ms) | garage ops/s (p50 / p99 ms) | seaweedfs ops/s (p50 / p99 ms) |
|---|---|---|---|---|
| 4 KiB | 52,411 (97.7 / 97.7) | 10,436 (490.6 / 490.6) | 39,628 (129.2 / 129.2) | 105,375 (48.6 / 48.6) |
| 64 KiB | 53,998 (94.8 / 94.8) | 4,786 (1069.7 / 1069.7) | 30,630 (167.2 / 167.2) | not measured |
| 1 MiB | 80,038 (16.0 / 16.0) | 15,282 (83.8 / 83.8) | 18,137 (70.6 / 70.6) | not measured |
| 8 MiB | 76,025 (13.9 / 13.9) | 18,102 (58.3 / 58.3) | 15,909 (66.4 / 66.4) | not measured |
| 64 MiB | 50,045 (21.1 / 21.1) | 9,089 (116.2 / 116.2) | 11,792 (89.5 / 89.5) | not measured |

### Scaling with concurrency (empty store, ops/s)

4 KiB writes:

| store | 1 in flight | 8 in flight | 32 in flight | 128 in flight |
|---|---|---|---|---|
| minio | 3,249 | 7,980 | 8,706 | 7,369 |
| rustfs | 1,295 | 4,476 | 5,215 | 5,015 |
| garage | 1,438 | 2,457 | 3,166 | 2,994 |
| seaweedfs | 1,844 | 7,168 | 11,708 | 15,410 |

4 KiB reads:

| store | 1 in flight | 8 in flight | 32 in flight | 128 in flight |
|---|---|---|---|---|
| minio | 4,479 | 14,456 | 27,149 | 37,080 |
| rustfs | 4,329 | 16,243 | 28,166 | 34,357 |
| garage | 3,062 | 7,829 | 5,353 | 5,154 |
| seaweedfs | 3,505 | 12,507 | 21,355 | 26,160 |

1 MiB writes:

| store | 1 in flight | 8 in flight | 32 in flight | 128 in flight |
|---|---|---|---|---|
| minio | 185 | 927 | 924 | 1,297 |
| rustfs | 207 | 972 | 1,567 | 1,231 |
| garage | 195 | 826 | 906 | 898 |
| seaweedfs | 137 | 705 | 251 | 884 |

64 MiB writes, where seaweedfs comes apart:

| store | 1 in flight | 8 in flight | 32 in flight | 128 in flight |
|---|---|---|---|---|
| minio | 21 | 39 | 30 | 28 |
| rustfs | 20 | 31 | 20 | 20 |
| garage | 14 | 19 | 20 | 21 |
| seaweedfs | 14 | 12 | 9 | 0 |

minio and rustfs keep gaining on reads through 128 operations in flight and
saturate writes around 32. Garage saturates both by 32. seaweedfs goes
backwards on large writes as concurrency rises.

### Standing relative to minio

Range across all five object sizes at the empty store, as a multiple of minio's
throughput. Above 1.0 is slower than minio, below 1.0 is faster.

| operation | rustfs | garage | seaweedfs |
|---|---|---|---|
| set | 0.6x to 1.7x | 1.0x to 3.4x | 0.7x to 9.6x |
| get | 0.9x to 1.5x | 4.3x to 5.6x | 0.8x to 5.2x |
| delete | 1.9x to 4.1x | 0.7x to 2.3x | 0.4x to 1.0x |

The consistent, noise-clearing results are garage's read gap at about 4x every
size, rustfs's delete gap, and seaweedfs's split between small objects, where
it beats minio, and large writes, where it is far behind.

### Retries

Persist's retry loop wraps every set, get and delete, so a throttling store
appears as latency plus a retry count rather than an error. Two stores needed
them:

- rustfs set 64 MiB at 32 in flight, 0 B stored: 17 retries, 20.4 ops/s, p50 1,307 ms
- rustfs set 64 MiB at 128 in flight, 0 B stored: 15 retries, 19.7 ops/s, p50 1,354 ms
- rustfs set 64 MiB at 32 in flight, 8 GiB stored: 8 retries, 20.1 ops/s, p50 1,146 ms
- rustfs set 64 MiB at 128 in flight, 8 GiB stored: 16 retries, 20.1 ops/s, p50 1,168 ms
- seaweedfs set 64 MiB at 128 in flight, 0 B stored: 62 retries, 0.0 ops/s, p50 5,595,715 ms

rustfs's are `SlowDown` from its write admission limit, returned promptly.
seaweedfs's are a different thing entirely, described next.

### Resources

Peak over the round, sampled about once a second per cell.

| store | peak RSS | peak CPU (cores) | RSS over the 4 KiB and 64 KiB cells | retries |
|---|---|---|---|---|
| minio | 5.6 GiB | 13.1 | 456.3 MiB to 3.8 GiB | 0 |
| rustfs | 2.3 GiB | 13.6 | 188 MiB to 1.1 GiB | 56 |
| garage | 705.6 MiB | 9.0 | 10.8 MiB to 60.0 MiB | 0 |
| seaweedfs | 25.8 GiB | 18.1 | 336.8 MiB to 3.1 GiB | 62 |

## seaweedfs comes apart under concurrent large writes

The worst cell in the whole exercise. 64 MiB objects with 128 operations in
flight, empty store:

| | seaweedfs | minio |
|---|---|---|
| throughput | 0.0 ops/s, 0.4 MiB/s | 28 ops/s, 1,796 MiB/s |
| p50 per write | 5,595,715 ms, about 93 minutes | 1,133 ms |
| retries | 62 | 0 |

Reads in that same cell still completed at 31 ops/s, so writes
specifically fell over rather than the server dying. At 32 operations in flight
the same object size was slow but functional, so the failure is
concurrency-dependent, not purely size-dependent.

This is a harsher failure than rustfs's. rustfs refuses work promptly with
`SlowDown` and persist rides it out. seaweedfs accepts the work and then takes
roughly an hour and a half per write, while its resident memory climbs to
25.8 GiB, more than half the Docker VM. That single cell is why the
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
bin/mzcompose --find blob-store-benchmark run default \
  --blob-store seaweedfs --size 64MiB --concurrency 32 --read-secs 10

# end to end, one store against minio
bin/mzcompose --find parallel-benchmark run default \
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
