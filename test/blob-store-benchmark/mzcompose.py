# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Measures the blob stores persist can run against by driving each through
persist's own blob client (`persistcli bench blob`) over a matrix of object
sizes, concurrency levels and stored volumes, and compares throughput, latency
and the store's CPU and memory use side by side.
"""

import argparse
import csv
import re
import subprocess
import threading
import time
from collections import defaultdict

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.blob_store import BLOB_STORES, blob_store_uri
from materialize.mzcompose.services.garage import Garage
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.rustfs import RustFs
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)

SERVICES = [
    Minio(setup_materialize=True),
    Garage(setup_materialize=True),
    RustFs(setup_materialize=True),
    Azurite(),
    Persistcli(),
]

# Columns `persistcli bench blob` prints, in order.
PERSISTCLI_FIELDS = [
    "op",
    "size_bytes",
    "concurrency",
    "ops",
    "bytes",
    "elapsed_secs",
    "ops_per_sec",
    "mib_per_sec",
    "p50_ms",
    "p90_ms",
    "p99_ms",
    "max_ms",
    "retries",
]
CSV_FIELDS = ["store", "fill_bytes"] + PERSISTCLI_FIELDS + ["cpu_pct_max", "mem_bytes_max"]

UNITS = {"": 1, "k": 1024, "kib": 1024, "m": 1024**2, "mib": 1024**2, "g": 1024**3, "gib": 1024**3}


def parse_bytes(text: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([a-zA-Z]*)\s*", text)
    if not match or match.group(2).lower() not in UNITS:
        raise argparse.ArgumentTypeError(f"not a size: {text!r}")
    return int(match.group(1)) * UNITS[match.group(2).lower()]


def format_bytes(n: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if n < 1024 or unit == "GiB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    raise AssertionError("unreachable")


class ContainerStats(threading.Thread):
    """Samples `docker stats` for one container until stopped, keeping the
    peak CPU percentage and memory use.

    `docker stats --no-stream` blocks for about a second per sample to compute
    the CPU rate, so this is a thread rather than a poll inside the cell loop.
    """

    def __init__(self, container: str):
        super().__init__(daemon=True)
        self.container = container
        self.cpu_pct_max = 0.0
        self.mem_bytes_max = 0
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                out = subprocess.check_output(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.CPUPerc}}\t{{.MemUsage}}",
                        self.container,
                    ],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
            except subprocess.CalledProcessError:
                time.sleep(1)
                continue
            cpu, mem = out.strip().split("\t")
            self.cpu_pct_max = max(self.cpu_pct_max, float(cpu.rstrip("%")))
            self.mem_bytes_max = max(self.mem_bytes_max, parse_docker_mem(mem.split("/")[0]))

    def stop(self) -> None:
        self._stop.set()
        self.join()


def parse_docker_mem(text: str) -> int:
    """`docker stats` prints memory like `1.234GiB` or `512MiB`."""
    match = re.fullmatch(r"\s*([\d.]+)\s*([A-Za-z]+)\s*", text)
    assert match, f"unexpected docker stats memory: {text!r}"
    return int(float(match.group(1)) * UNITS[match.group(2).lower()])


def bench(
    c: Composition,
    store: str,
    args: argparse.Namespace,
    prefix: str,
    size: int,
    count: int,
    concurrency: int,
    fill_bytes: int,
    read_secs: int,
    keep: bool,
    skip_list: bool,
) -> list[dict[str, str]]:
    """Runs one `persistcli bench blob` invocation and returns its rows.

    A negative `read_secs` skips the read phase.
    """
    stats = ContainerStats(f"{c.project_name}-{store}-1")
    stats.start()
    try:
        output = c.run(
            "persistcli",
            "bench",
            "blob",
            f"--blob-uri={blob_store_uri(store)}",
            f"--prefix={prefix}",
            # Everything under the store's prefix, so that objects kept from
            # the fill phase count towards the listing.
            "--list-prefix=",
            f"--size-bytes={size}",
            f"--count={count}",
            f"--concurrency={concurrency}",
            *([f"--read-secs={read_secs}"] if read_secs >= 0 else ["--skip-read"]),
            *(["--keep"] if keep else []),
            *(["--skip-list"] if skip_list else []),
            "--no-header",
            capture=True,
            rm=True,
        ).stdout
    finally:
        stats.stop()
    rows = []
    for record in csv.DictReader(output.splitlines(), fieldnames=PERSISTCLI_FIELDS):
        record["store"] = store
        record["fill_bytes"] = str(fill_bytes)
        record["cpu_pct_max"] = f"{stats.cpu_pct_max:.1f}"
        record["mem_bytes_max"] = str(stats.mem_bytes_max)
        rows.append(record)
    return rows


def objects_per_cell(args: argparse.Namespace, size: int, concurrency: int) -> int:
    """How many objects a cell writes: enough bytes to be representative and
    enough objects to keep `concurrency` busy, within the caps."""
    count = max(concurrency, args.bytes_per_cell // size)
    count = min(count, args.max_objects_per_cell, max(1, args.max_bytes_per_cell // size))
    return count


def print_report(rows: list[dict[str, str]]) -> None:
    stores = list(dict.fromkeys(row["store"] for row in rows))
    by_cell: dict[tuple[str, str, str], dict[tuple[str, str], dict[str, str]]] = defaultdict(dict)
    for row in rows:
        by_cell[(row["fill_bytes"], row["op"], row["size_bytes"])][
            (row["store"], row["concurrency"])
        ] = row
    for (fill_bytes, op, size_bytes), cells in by_cell.items():
        title = f"{op} {format_bytes(int(size_bytes))} objects"
        if int(fill_bytes):
            title += f", {format_bytes(int(fill_bytes))} already stored"
        print(f"\n=== {title}")
        header = f"{'CONC':>5} {'STORE':<8} {'OPS/S':>9} {'MiB/S':>8} {'P50 ms':>9} {'P90 ms':>9} {'P99 ms':>9} {'MAX ms':>9} {'RETRIES':>7} {'CPU%':>7} {'MEM':>9}"
        print(header)
        concurrencies = sorted({int(conc) for (_, conc) in cells}, key=int)
        for concurrency in concurrencies:
            for store in stores:
                row = cells.get((store, str(concurrency)))
                if row is None:
                    continue
                print(
                    f"{concurrency:>5} {store:<8} {float(row['ops_per_sec']):>9.1f} {float(row['mib_per_sec']):>8.1f} "
                    f"{float(row['p50_ms']):>9.2f} {float(row['p90_ms']):>9.2f} {float(row['p99_ms']):>9.2f} {float(row['max_ms']):>9.2f} "
                    f"{int(row['retries']):>7} {float(row['cpu_pct_max']):>7.1f} {format_bytes(int(row['mem_bytes_max'])):>9}"
                )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--blob-store",
        action="append",
        choices=BLOB_STORES,
        help="Blob stores to benchmark (default: minio, garage, rustfs)",
    )
    parser.add_argument(
        "--size",
        action="append",
        type=parse_bytes,
        help="Object sizes, like 4KiB or 8MiB (default: 4KiB 64KiB 1MiB 8MiB 64MiB)",
    )
    parser.add_argument(
        "--concurrency",
        action="append",
        type=int,
        help="Operations in flight (default: 1 8 32 128)",
    )
    parser.add_argument(
        "--fill",
        action="append",
        type=parse_bytes,
        help="Stored volumes to run the matrix at, ascending (default: 0). Fill objects are 8MiB and stay for the rest of the store's run.",
    )
    parser.add_argument(
        "--bytes-per-cell",
        type=parse_bytes,
        default=256 * 1024**2,
        help="Bytes each cell writes, before the caps (default: 256MiB)",
    )
    parser.add_argument(
        "--max-objects-per-cell",
        type=int,
        default=4096,
        help="Cap on objects per cell, which bounds the small-object cells (default: 4096)",
    )
    parser.add_argument(
        "--max-bytes-per-cell",
        type=parse_bytes,
        default=2 * 1024**3,
        help="Cap on bytes per cell, which bounds the large-object cells (default: 2GiB)",
    )
    parser.add_argument(
        "--read-secs",
        type=int,
        default=10,
        help="Seconds of random reads per cell (default: 10)",
    )
    parser.add_argument(
        "--csv",
        default="blob-store-benchmark.csv",
        help="Where to write every row, relative to the repository root",
    )
    args = parser.parse_args()

    stores = args.blob_store or ["minio", "garage", "rustfs"]
    sizes = args.size or [4 * 1024, 64 * 1024, 1024**2, 8 * 1024**2, 64 * 1024**2]
    concurrencies = args.concurrency or [1, 8, 32, 128]
    fills = sorted(set(args.fill or [0]))
    fill_object_bytes = 8 * 1024**2

    # Written as cells complete, so a run that dies keeps what it measured.
    csv_path = MZ_ROOT / args.csv
    csv_file = open(csv_path, "w", newline="")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
    writer.writeheader()
    rows: list[dict[str, str]] = []
    failures: list[str] = []
    for store in stores:
        print(f"+++ Benchmarking {store}")
        c.up(store)
        filled = 0
        for fill_bytes in fills:
            if fill_bytes > filled:
                print(f"--- Filling {store} to {format_bytes(fill_bytes)}")
                try:
                    bench(
                        c,
                        store,
                        args,
                        prefix=f"fill/{fill_bytes}",
                        size=fill_object_bytes,
                        count=(fill_bytes - filled) // fill_object_bytes,
                        concurrency=32,
                        fill_bytes=fill_bytes,
                        read_secs=-1,
                        keep=True,
                        skip_list=True,
                    )
                except Exception as e:
                    # The cells at this fill level would measure a store
                    # holding less than they claim, so skip the rest of them.
                    failures.append(f"{store}: fill to {format_bytes(fill_bytes)}: {e}")
                    print(f"Fill failed, skipping the remaining fill levels for {store}: {e}")
                    break
                filled = fill_bytes
            for size in sizes:
                for concurrency in concurrencies:
                    count = objects_per_cell(args, size, concurrency)
                    cell = f"{store}: {count} x {format_bytes(size)} at concurrency {concurrency}, {format_bytes(filled)} stored"
                    print(f"--- {cell}")
                    try:
                        cell_rows = bench(
                            c,
                            store,
                            args,
                            prefix=f"cell/{fill_bytes}/{size}/{concurrency}",
                            size=size,
                            count=count,
                            concurrency=concurrency,
                            fill_bytes=fill_bytes,
                            read_secs=args.read_secs,
                            keep=False,
                            skip_list=False,
                        )
                    except Exception as e:
                        # One failing cell should not cost the rest of the
                        # matrix. The objects it wrote stay behind, which is
                        # noise for later list cells of this store only.
                        failures.append(f"{cell}: {e}")
                        print(f"Cell failed, continuing: {e}")
                        continue
                    rows.extend(cell_rows)
                    writer.writerows(cell_rows)
                    csv_file.flush()
                    for row in cell_rows:
                        print(
                            f"    {row['op']:<7} {float(row['ops_per_sec']):>9.1f} ops/s {float(row['mib_per_sec']):>8.1f} MiB/s "
                            f"p50 {float(row['p50_ms']):>8.2f} ms  p99 {float(row['p99_ms']):>8.2f} ms  retries {row['retries']}"
                        )
        c.kill(store)
        c.rm(store, destroy_volumes=True)
    csv_file.close()

    print(f"+++ Results ({csv_path})")
    print_report(rows)
    if failures:
        print("+++ Failed cells")
        for failure in failures:
            print(f"  {failure}")
        raise FailedTestExecutionError(
            errors=[
                TestFailureDetails(message=failure, details=None) for failure in failures
            ]
        )
