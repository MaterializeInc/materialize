# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import base64
import os
import platform
import shlex
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

from materialize import MZ_ROOT, buildkite, ui
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser

SERVICES = []

# Buildkite artifact that carries the minimized corpus between release-qualification runs so
# coverage accumulates instead of restarting from the seeds every time.
CORPUS_ARTIFACT = "fuzz-corpus.tar.zst"

# Every fuzz crate. Keep in sync with the crates that have a `fuzz/`
# subdirectory. `bin/ci-builder` and the release-qualification pipeline build the same set.
FUZZ_CRATES = [
    "src/sql-parser/fuzz",
    "src/expr/fuzz",
    "src/transform/fuzz",
    "src/repr/fuzz",
    "src/storage-types/fuzz",
    "src/catalog-protos/fuzz",
    "src/avro/fuzz",
    "src/mysql-util/fuzz",
    "src/postgres-util/fuzz",
    "src/sql-server-util/fuzz",
    "src/pgwire/fuzz",
    "src/pgrepr/fuzz",
    "src/pgcopy/fuzz",
    "src/pgtz/fuzz",
    "src/interchange/fuzz",
    "src/persist-client/fuzz",
    "src/storage/fuzz",
]

# The highest-yield targets, the ones that keep surfacing bugs deep into a run,
# or that guard a bug-prone path / actively-developed subsystem where a find
# would be catastrophic. `--profile fruitful` restricts the run to these, which
# is the right focus for the long (24h) release-qualification run that should
# spend its cores where bugs still hide. Substring-matched against
# `crate::target`, like the positional `filters`.
#
# This set is pruned by productivity. Targets over well-tested, stable code that
# fuzz clean round after round (the arithmetic/range oracles, internal
# encode/decode round-trips, simple jsonb access) are dropped, since they've
# saturated, and their cores are better spent elsewhere. They still exist and can
# be run by name or re-added if the code under them changes. Two buckets stay
# despite no recent finds: the untrusted-input parsers/decoders (unbounded
# adversarial input) and the optimizer/upsert/persist targets (regression
# insurance on code that changes often). Revisit as the productive set shifts.
FRUITFUL = [
    # SQL-parser round-trip oracle, ~60% of all findings. A single target:
    # structured statement generation plus a full-vocabulary soup minority,
    # one `parse -> print -> reparse` AST-equality oracle.
    "sql_roundtrip",
    # Hand-written parsers/decoders of untrusted text/bytes (COPY, wire params,
    # Kafka payloads). Every decoder bug so far came from this bucket.
    "strconv_parse_timestamp",
    "strconv_parse_timestamptz",
    "strconv_parse_date",
    "strconv_parse_time",
    "strconv_parse_bytes",
    "strconv_parse_uuid",
    "rollup_proto_roundtrip",
    "copy_decode",
    "protobuf_decode_fuzzed_schema",
    "json_encode",
    "avro_decode_fuzzed_schema",
    "csv_decode",
    "schema_resolve",
    "avro_schema_parse",
    "timezone_parse",
    "like_pattern_compile",
    "like_pattern_escape",
    "build_regex",
    "cast_string",
    # Clean so far, but kept as regression insurance on actively-developed code
    # where a miscompilation / corruption would be catastrophic.
    "mir_scalar_reduce",
    "mir_relation_transforms",
    "full_optimizer_equiv",
    "optimizer_symbolic_equiv",
    "mfp_optimize",
    "upsert_consolidate",
    "upsert_value_roundtrip_v2",
    "upsert_state_consolidate",
    "upsert_runtime",
    "row_arrow_roundtrip",
]

say = ui.speaker("")


@dataclass
class Job:
    crate: str  # e.g. "src/avro/fuzz"
    target: str  # e.g. "reader_decode"
    log_path: Path
    proc: subprocess.Popen | None = None
    started_at: float = 0.0
    finished_at: float = 0.0
    returncode: int | None = None

    @property
    def name(self) -> str:
        # e.g. "avro::reader_decode", short crate name + target.
        crate_name = Path(self.crate).parent.name
        return f"{crate_name}::{self.target}"

    @property
    def artifact_dir(self) -> Path:
        return MZ_ROOT / self.crate / "artifacts" / self.target

    def elapsed(self) -> float:
        end = self.finished_at or time.time()
        return end - self.started_at


@dataclass
class _CminProc:
    """An in-flight `-merge=1` minimization for one target."""

    job: Job
    proc: subprocess.Popen
    log: IO[str]
    before: int  # corpus size before the merge
    merged: Path  # scratch dir the merge writes into, swapped in on success


def fuzz_env(target_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    # One shared target dir for every fuzz crate (see module docstring).
    env["CARGO_TARGET_DIR"] = str(target_dir)
    # Use every core for compilation. Force it explicitly: a `CARGO_BUILD_JOBS=1`
    # inherited from the environment (handy when running many fuzzers at once)
    # would otherwise throttle the build phase to a single thread.
    env["CARGO_BUILD_JOBS"] = str(os.cpu_count() or 1)
    # cargo-fuzz requires a nightly toolchain. Default to nightly unless the
    # caller already pinned one (the nightly ci-builder flavor sets it).
    env.setdefault("RUSTUP_TOOLCHAIN", "nightly")
    if platform.system() == "Darwin":
        # cargo-fuzz instruments every crate with SanCov coverage, but some
        # dependencies build a `cdylib` (e.g. crc-fast, pulled in via
        # aws-smithy-checksums). A cdylib links on its own without libFuzzer's
        # coverage runtime, so it fails with undefined `__sanitizer_cov_*`
        # symbols. Use the classic linker (the new one rejects these flags) and
        # defer those symbols to runtime via `-undefined dynamic_lookup`, where
        # the fuzz binary's own runtime supplies them. Disabling the nano malloc
        # zone keeps ASan-style allocators happy.
        extra = "-C link-arg=-Wl,-ld_classic -C link-arg=-Wl,-undefined,dynamic_lookup"
        rustflags = env.get("RUSTFLAGS", "")
        if "dynamic_lookup" not in rustflags:
            env["RUSTFLAGS"] = f"{rustflags} {extra}".strip()
        env.setdefault("MallocNanoZone", "0")
    return env


def host_triple(env: dict[str, str]) -> str:
    """The Rust host target triple, e.g. `x86_64-unknown-linux-gnu`.

    cargo-fuzz builds each target to `<CARGO_TARGET_DIR>/<triple>/release/<name>`,
    so we need the triple to locate the binaries we exec directly.
    """
    out = subprocess.run(
        ["rustc", "-vV"], env=env, check=True, capture_output=True, text=True
    )
    for line in out.stdout.splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ").strip()
    raise ui.UIError("could not determine host target triple from `rustc -vV`")


def list_targets(crate: str, env: dict[str, str]) -> list[str]:
    # Listing is cheap and doesn't need the corpus, so it runs for every crate.
    # `prepare_corpus` is then called only for the crates this shard owns.
    crate_dir = MZ_ROOT / crate
    out = subprocess.run(
        ["cargo", "fuzz", "list"],
        cwd=crate_dir,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


def prepare_corpus(crate: str, env: dict[str, str]) -> None:
    """Seed a crate's corpus by running its `prepare-corpus.sh`, if it has one."""
    crate_dir = MZ_ROOT / crate
    prepare = crate_dir / "prepare-corpus.sh"
    if prepare.is_file() and os.access(prepare, os.X_OK):
        subprocess.run([str(prepare)], cwd=crate_dir, env=env, check=True)


def dict_for(job: "Job") -> str | None:
    """Resolve a libFuzzer dictionary (`-dict`) for a target.

    Dictionaries inject "interesting" tokens (magic bytes, keywords, protobuf
    field tags) so the mutator builds structurally-valid input faster. Lookup
    order: a per-target `<target>.dict`, then a per-crate `corpus.dict`, then a
    shared `proto.dict` for the protobuf round-trip targets.
    """
    crate_dir = MZ_ROOT / job.crate
    for candidate in (crate_dir / f"{job.target}.dict", crate_dir / "corpus.dict"):
        if candidate.is_file():
            return str(candidate)
    if "proto" in job.target:
        shared = MZ_ROOT / "test" / "cargo-fuzz" / "proto.dict"
        if shared.is_file():
            return str(shared)
    return None


def tail(path: Path, lines: int = 1) -> str:
    try:
        content = path.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        return "(no output)"
    nonblank = [ln for ln in content if ln.strip()]
    if not nonblank:
        return "(no output)"
    return "\n".join(nonblank[-lines:])


# libFuzzer's end-of-run `stat::` keys, in display order, mapped to short labels.
_FINAL_STAT_LABELS = {
    "number_of_executed_units": "execs",
    "average_exec_per_sec": "exec/s",
    "new_units_added": "new_units",
    "slowest_unit_time_sec": "slowest_s",
    "peak_rss_mb": "rss_mb",
}


def final_stats(path: Path) -> str:
    """Compact one-line summary of libFuzzer's end-of-run `stat::` lines.

    libFuzzer prints e.g. `stat::peak_rss_mb: 71` for a handful of keys when a
    run ends cleanly (max_total_time reached). Surface them all, not just the
    last line. Fall back to the last log line if no stats were emitted.
    """
    try:
        content = path.read_text(errors="replace")
    except FileNotFoundError:
        return "(no output)"
    found: dict[str, str] = {}
    for line in content.splitlines():
        line = line.strip()
        if not line.startswith("stat::") or ":" not in line[6:]:
            continue
        key, _, value = line[6:].partition(":")
        label = _FINAL_STAT_LABELS.get(key.strip())
        if label:
            found[label] = value.strip()
    if not found:
        return tail(path)
    return "  ".join(
        f"{label}={found[label]}"
        for label in _FINAL_STAT_LABELS.values()
        if label in found
    )


def _escape_bytes(data: bytes) -> str:
    """Render bytes readable: printable ASCII as-is, everything else escaped."""
    special = {0x09: "\\t", 0x0A: "\\n", 0x0D: "\\r", 0x5C: "\\\\"}
    out = []
    for b in data:
        if b in special:
            out.append(special[b])
        elif 0x20 <= b < 0x7F:
            out.append(chr(b))
        else:
            out.append(f"\\x{b:02x}")
    return "".join(out)


def crash_input_lines(path: Path, max_bytes: int = 4096) -> list[str]:
    """Render a crashing input for the failure annotation: an escaped, readable
    form plus base64 (the authoritative, copy-pasteable form). libFuzzer doesn't
    always echo the input for `deadly signal` crashes, and the artifact lives on
    whichever machine ran the fuzzer, so inline it here for easy reproduction:
    `echo <base64> | base64 -d > input && cargo fuzz run <target> input`.
    """
    try:
        data = path.read_bytes()
    except OSError:
        return []
    shown = data[:max_bytes]
    lines = [
        f"input ({len(data)} bytes): {_escape_bytes(shown)}",
        # Base64 the full input, not just the escaped preview, so the reproduce
        # recipe is exact even when the preview above is truncated.
        f"input base64: {base64.b64encode(data).decode('ascii')}",
    ]
    if len(data) > max_bytes:
        lines.append(
            f"  (escaped preview shows the first {max_bytes} bytes; the base64 is "
            f"the full input, also saved as the artifact)"
        )
    return lines


@dataclass
class FuzzRunner:
    jobs: list[Job]
    env: dict[str, str]
    max_seconds: int
    rss_limit_mb: int
    jobs_per_target: int
    max_parallel: int
    fail_fast: bool
    triple: str = ""
    # None => don't pass --sanitizer (use cargo-fuzz's default, i.e. ASan).
    # The CLI defaults this to "none" (see below): our targets find panics /
    # round-trip drifts, not memory-corruption bugs, so ASan adds no detection
    # power here but ~2-3x slowdown. Pass --sanitizer=address to opt back in.
    sanitizer: str | None = None
    wall_budget: int = 0
    minimize: bool = True
    # Cap the post-fuzz minimize phase in seconds (0 = unbounded) so it and the
    # corpus upload after it finish inside the gap between `--wall-budget` and
    # the step's hard timeout. See `_minimize`.
    minimize_timeout: int = 0
    start: float = field(default_factory=time.time)
    pending: list[Job] = field(default_factory=list)
    running: list[Job] = field(default_factory=list)
    failed: list[Job] = field(default_factory=list)
    succeeded: list[Job] = field(default_factory=list)

    def _spawn(self, job: Job) -> None:
        # Exec the libFuzzer binary directly rather than going through
        # `cargo fuzz run`. The binary was already produced by `build()`, and
        # invoking cargo again for each of the ~25 targets just makes them
        # queue on cargo's per-target-dir build lock ("Blocking waiting for
        # file lock"), serializing what should run in parallel.
        target_dir = Path(self.env["CARGO_TARGET_DIR"])
        binary = target_dir / self.triple / "release" / job.target
        corpus = MZ_ROOT / job.crate / "corpus" / job.target
        corpus.mkdir(parents=True, exist_ok=True)
        job.artifact_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            str(binary),
            # Trailing slash: libFuzzer writes <prefix><kind>-<hash>.
            f"-artifact_prefix={job.artifact_dir}/",
            f"-rss_limit_mb={self.rss_limit_mb}",
            "-print_final_stats=1",
        ]
        if (dict_path := dict_for(job)) is not None:
            cmd.append(f"-dict={dict_path}")
        if self.jobs_per_target > 1:
            # libFuzzer fork mode: N worker processes for this one target.
            cmd.append(f"-fork={self.jobs_per_target}")
        if self.max_seconds > 0:
            cmd.append(f"-max_total_time={self.max_seconds}")
            cmd.append("-timeout=300")
        cmd.append(str(corpus))
        log = job.log_path.open("w")
        job.started_at = time.time()
        # start_new_session => own process group, so Ctrl-C teardown can kill
        # the libFuzzer process even if it forked workers.
        job.proc = subprocess.Popen(
            cmd,
            cwd=MZ_ROOT / job.crate,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.running.append(job)
        say(f"start {job.name}  (pid {job.proc.pid}, log {job.log_path})")

    def _fill_slots(self) -> None:
        limit = self.max_parallel or len(self.jobs)
        while self.pending and len(self.running) < limit:
            self._spawn(self.pending.pop(0))

    def _new_artifacts(self, job: Job) -> list[str]:
        """Crash/oom/timeout/leak artifacts this run produced (mtime >= start).

        Checked alongside the exit code because the two cover different run
        modes. A single libFuzzer process exits non-zero on a crash. Fork mode
        (`-fork=N`, used only when `--jobs` resolves to more than one worker per
        target) is crash-resilient instead: it writes the artifact and keeps
        fuzzing, exiting 0 at the time limit. `_reap` fails the job on a non-zero
        exit OR a new artifact, so neither mode reports a crash as green. Stale
        artifacts from earlier runs are excluded by the mtime check.
        """
        if not job.artifact_dir.is_dir():
            return []
        return sorted(
            p.name
            for p in job.artifact_dir.iterdir()
            # `slow-unit-*` is deliberately excluded: libFuzzer writes it for any
            # input slower than `-report_slow_units` (10s default) while exiting
            # 0, so a bounded-but-slow input is not a crash and must not fail the
            # run. Only genuine defect artifacts count.
            if p.name.startswith(("crash-", "oom-", "timeout-", "leak-"))
            and not p.name.endswith(".repro.txt")
            and p.stat().st_mtime >= job.started_at
        )

    def _reap(self, job: Job) -> None:
        assert job.proc is not None
        job.returncode = job.proc.returncode
        job.finished_at = time.time()
        self.running.remove(job)
        secs = int(job.elapsed())
        # A zero exit does NOT mean "no crash": fork mode writes the crash
        # artifact and keeps fuzzing, exiting 0 at the time limit. Fail the job if
        # it exited non-zero OR produced any new crash artifact.
        if job.returncode == 0 and not self._new_artifacts(job):
            self.succeeded.append(job)
            say(f"✓ {job.name}  [{secs}s]  {final_stats(job.log_path)}")
        else:
            self.failed.append(job)
            say(self._failure_block(job, secs))

    def _repro_env_prefix(self) -> str:
        """The `fuzz_env` vars that shape the build, as a shell `VAR=val …`
        prefix. Prepended to the reproduce command so `cargo fuzz run` reuses the
        binary we already built in the shared target dir instead of recompiling
        into cargo-fuzz's default `<crate>/fuzz/target`. Matching CARGO_TARGET_DIR,
        the toolchain, and RUSTFLAGS keeps the build fingerprint identical (the
        --sanitizer flag, the other build input, is added on the command itself).
        """
        parts = [f"CARGO_TARGET_DIR={shlex.quote(self.env['CARGO_TARGET_DIR'])}"]
        for var in ("RUSTUP_TOOLCHAIN", "RUSTFLAGS", "MallocNanoZone"):
            if (val := self.env.get(var)) is not None:
                parts.append(f"{var}={shlex.quote(val)}")
        return " ".join(parts)

    def _failure_block(self, job: Job, secs: int) -> str:
        # Emit a predictable, self-contained block. `bin/ci-annotate-errors`
        # matches the START/END markers (see CARGO_FUZZ_FAILURE in
        # ci_annotate_errors.py) and turns the whole block into a Buildkite
        # annotation, so we deliberately do NOT call `buildkite-agent` here.
        #
        # Only list artifacts this run produced. Stale crash-* files from earlier
        # runs would otherwise pollute every annotation.
        artifacts = self._new_artifacts(job)
        repro = artifacts[0] if artifacts else "<crash-file>"
        lines = [
            "---------- CARGO-FUZZ FAILURE START ----------",
            f"target: {job.name}  ({job.crate})",
            f"exit code: {job.returncode}  (after {secs}s)",
        ]
        if artifacts:
            lines.append(f"new artifacts: {', '.join(artifacts)}")
        san = f"--sanitizer={self.sanitizer} " if self.sanitizer else ""
        lines.append(
            f"reproduce: cd {job.crate} && {self._repro_env_prefix()} "
            f"cargo fuzz run {san}{job.target} artifacts/{job.target}/{repro}"
        )
        # Inline the crashing input so it can be reproduced straight from the
        # annotation, without fetching the artifact from the machine that ran it.
        if artifacts:
            lines += crash_input_lines(job.artifact_dir / repro)
        lines.append("last output:")
        lines += [f"  {ln}" for ln in tail(job.log_path, 8).splitlines()]
        lines.append("---------- CARGO-FUZZ FAILURE END ----------")
        block = "\n".join(lines)
        # Persist the same record next to the crashing artifact. stdout and the
        # CI annotation are ephemeral, so without this a later triage only has
        # the bare input file and must re-run the target to recover the cause.
        # The `.repro.txt` sidecar keeps the artifacts dir self-describing.
        if artifacts:
            try:
                (job.artifact_dir / f"{repro}.repro.txt").write_text(block + "\n")
            except OSError as e:
                say(f"  (could not write repro sidecar for {repro}: {e})")
        return block

    def _terminate_all(self, sig: int) -> None:
        for job in self.running:
            if job.proc and job.proc.poll() is None:
                try:
                    os.killpg(os.getpgid(job.proc.pid), sig)
                except ProcessLookupError:
                    pass

    def build(self) -> None:
        # Compile every fuzz crate up front, one at a time, not just the crates
        # this run will fuzz. A fuzz target that won't compile is a broken
        # build: building only the crates the active --profile/filters select
        # would let a compile break in a skipped crate pass as a green run,
        # since that crate is never compiled. Building all of them makes a
        # broken fuzzer fail the run immediately, whatever the profile.
        # Sequential, one crate at a time, so the concurrent fuzzing phase
        # doesn't have 20+ `cargo fuzz run` invocations fighting over cargo's
        # per-target-dir build lock; crates share the target dir, so common
        # dependencies compile once.
        # Must match _spawn's exec: we build with this sanitizer and run that
        # exact binary. Defaults to `none` everywhere (see the CLI flag): on
        # macOS ASan's interceptors segfault on startup (e.g. in flockfile), and
        # on Linux ASan only adds ~2-3x slowdown without catching our bug class
        # (panics / round-trip drifts, not memory corruption). Pass
        # --sanitizer=address to opt back into ASan's memory coverage.
        cmd = ["cargo", "fuzz", "build"]
        if self.sanitizer:
            cmd.append(f"--sanitizer={self.sanitizer}")
        for i, crate in enumerate(FUZZ_CRATES, 1):
            say(f"building [{i}/{len(FUZZ_CRATES)}] {crate}")
            if subprocess.run(cmd, cwd=MZ_ROOT / crate, env=self.env).returncode != 0:
                raise ui.UIError(f"build FAILED for {crate}")

    def run(self) -> list[Job]:
        # Clamp the per-target budget so a slow build can't blow the step
        # timeout: whatever wall time the build already consumed is subtracted.
        if self.max_seconds > 0 and self.wall_budget > 0:
            remaining = self.wall_budget - (time.time() - self.start)
            if remaining < 60:
                say(
                    f"build consumed the wall budget ({self.wall_budget}s); "
                    f"only {int(remaining)}s left, skipping the fuzz phase"
                )
                return []
            self.max_seconds = min(self.max_seconds, int(remaining))
        self.pending = list(self.jobs)
        say(
            f"launching {len(self.pending)} target(s); "
            f"{'continuous (Ctrl-C to stop)' if self.max_seconds == 0 else f'{self.max_seconds}s each'}; "
            f"target dir {self.env['CARGO_TARGET_DIR']}"
        )
        try:
            self._fill_slots()
            while self.running:
                for job in list(self.running):
                    assert job.proc is not None
                    if job.proc.poll() is not None:
                        self._reap(job)
                if self.fail_fast and self.failed:
                    say("--fail-fast: a target crashed, stopping the rest")
                    break
                self._fill_slots()
                time.sleep(1.0)
        except KeyboardInterrupt:
            say("\nCtrl-C: stopping all running targets…")
        finally:
            self._shutdown()
            # Minimize after fuzzing stops (on normal completion, on the
            # per-target timeout, and on Ctrl-C) so the corpus we keep around
            # for the next run holds only coverage-increasing inputs.
            if self.minimize:
                self._minimize()
            # Fold this run's crash reproducers into the corpus (after minimize,
            # so the merge never sees them) so the synced corpus replays them
            # next run and the qualification gate keeps enforcing the find.
            self._persist_crash_artifacts()
        self._summary()
        return self.failed

    def _minimize(self) -> None:
        # Minimize every corpus concurrently, with the same slot cap the fuzz
        # phase uses. Each `-merge=1` is a separate process over its own corpus,
        # so running them one at a time wastes ~N cores for the whole phase.
        #
        # Minimize and the corpus upload that follows it run after the fuzz
        # phase and outside `--wall-budget`, in the gap between it and the step's
        # hard timeout. A parallel `-merge` over corpora grown for the whole run
        # can be slow, so `--minimize-timeout` caps this phase: past the deadline
        # we stop launching merges and kill the running ones, leaving the rest of
        # the gap for the upload. Killing mid-merge never loses data, since
        # `_cmin_finish` swaps the minimized corpus in only on a clean exit.
        target_dir = Path(self.env["CARGO_TARGET_DIR"])
        limit = self.max_parallel or len(self.jobs)
        deadline = (
            time.time() + self.minimize_timeout if self.minimize_timeout > 0 else None
        )
        cap = f", ≤{self.minimize_timeout}s" if deadline else ""
        say(
            f"minimizing {len(self.jobs)} corpora in parallel "
            f"(cargo fuzz cmin, up to {limit} at once{cap})…"
        )
        pending = list(self.jobs)
        running: list[_CminProc] = []

        def fill() -> None:
            while pending and len(running) < limit:
                if deadline is not None and time.time() > deadline:
                    pending.clear()
                    return
                job = pending.pop(0)
                try:
                    entry = self._cmin_spawn(job, target_dir)
                except Exception as e:  # don't let one target abort the rest
                    say(f"  cmin {job.name}: failed to start ({e})")
                    continue
                if entry is not None:
                    running.append(entry)

        try:
            fill()
            while running:
                if deadline is not None and time.time() > deadline:
                    say(
                        f"minimize hit its {self.minimize_timeout}s cap, stopping "
                        f"(corpora left intact, {len(pending)} not minimized)"
                    )
                    pending.clear()
                    self._kill_cmins(running)
                    break
                for entry in list(running):
                    if entry.proc.poll() is not None:
                        running.remove(entry)
                        try:
                            self._cmin_finish(entry)
                        except Exception as e:
                            say(f"  cmin {entry.job.name}: failed ({e})")
                fill()
                time.sleep(0.5)
        except KeyboardInterrupt:
            say("Ctrl-C: stopping minimization (corpora left intact)")
            self._kill_cmins(running)

    def _kill_cmins(self, running: list["_CminProc"]) -> None:
        # Kill whatever's still merging and drop their scratch dirs. The original
        # corpora are untouched until a merge succeeds (see `_cmin_finish`), so
        # this is always safe.
        for entry in running:
            if entry.proc.poll() is None:
                try:
                    os.killpg(os.getpgid(entry.proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            entry.log.close()
            shutil.rmtree(entry.merged, ignore_errors=True)

    def _cmin_spawn(self, job: Job, target_dir: Path) -> _CminProc | None:
        binary = target_dir / self.triple / "release" / job.target
        corpus = MZ_ROOT / job.crate / "corpus" / job.target
        if not binary.is_file() or not corpus.is_dir():
            return None
        before = sum(1 for _ in corpus.iterdir())
        if before == 0:
            return None
        # libFuzzer `-merge=1 <dst> <src>` copies into the (empty) dst only the
        # inputs from src that add coverage, exactly what `cargo fuzz cmin`
        # does, minus the cargo invocation. Swap it in only on success (in
        # `_cmin_finish`) so an interrupted merge never loses the corpus.
        merged = corpus.with_name(f".{job.target}.cmin")
        shutil.rmtree(merged, ignore_errors=True)
        merged.mkdir(parents=True)
        log = job.log_path.open("a")
        proc = subprocess.Popen(
            [
                str(binary),
                "-merge=1",
                f"-rss_limit_mb={self.rss_limit_mb}",
                "-timeout=300",
                str(merged),
                str(corpus),
            ],
            cwd=MZ_ROOT / job.crate,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
            # Own process group, so Ctrl-C teardown can kill the merge.
            start_new_session=True,
        )
        return _CminProc(job=job, proc=proc, log=log, before=before, merged=merged)

    def _cmin_finish(self, entry: _CminProc) -> None:
        entry.log.close()
        job, before, merged = entry.job, entry.before, entry.merged
        corpus = MZ_ROOT / job.crate / "corpus" / job.target
        if entry.proc.returncode != 0:
            shutil.rmtree(merged, ignore_errors=True)
            say(
                f"  cmin {job.name}: merge exited {entry.proc.returncode}, kept {before}"
            )
            return
        after = sum(1 for _ in merged.iterdir())
        shutil.rmtree(corpus)
        merged.rename(corpus)
        say(f"  cmin {job.name}: {before} -> {after}")

    def _persist_crash_artifacts(self) -> None:
        """Fold this run's crash reproducers into the corpus.

        libFuzzer writes crash/oom/timeout/leak reproducers to the `artifacts/`
        dir (`-artifact_prefix`), which the corpus sync does not carry between
        runs. A reproducer that lives only there is lost once the build's
        Buildkite artifacts age out, so a later qualification run can miss the
        same bug and pass even though it is unfixed. Copying each one into the
        corpus makes the synced corpus carry it forward, and libFuzzer replays
        every corpus input on startup, so the crash re-triggers until the
        underlying bug is fixed. Run after minimize: `-merge=1` executes each
        input and would crash on a reproducer, so it must not see them.
        """
        for job in self.failed:
            corpus = MZ_ROOT / job.crate / "corpus" / job.target
            corpus.mkdir(parents=True, exist_ok=True)
            for name in self._new_artifacts(job):
                try:
                    shutil.copy2(job.artifact_dir / name, corpus / name)
                    say(f"  persisted reproducer {name} into {job.name} corpus")
                except OSError as e:
                    say(f"  (could not persist {name} into corpus: {e})")

    def _shutdown(self) -> None:
        if not self.running:
            return
        self._terminate_all(signal.SIGTERM)
        deadline = time.time() + 10
        while self.running and time.time() < deadline:
            for job in list(self.running):
                assert job.proc is not None
                if job.proc.poll() is not None:
                    job.returncode = job.proc.returncode
                    job.finished_at = time.time()
                    self.running.remove(job)
            time.sleep(0.2)
        if self.running:
            self._terminate_all(signal.SIGKILL)
            for job in self.running:
                assert job.proc is not None
                job.proc.wait()

    def _summary(self) -> None:
        say(
            f"summary: {len(self.succeeded)} ok, {len(self.failed)} failed, "
            f"{len(self.pending) + len(self.running)} not finished"
        )
        for job in self.failed:
            say(f"  ✗ {job.name}  ({job.artifact_dir})")


def _corpus_dirs(crates: list[str]) -> list[str]:
    """Existing `src/<crate>/fuzz/corpus` dirs for `crates`, relative to MZ_ROOT."""
    return [
        str(Path(c) / "corpus") for c in crates if (MZ_ROOT / c / "corpus").is_dir()
    ]


def upload_corpus(env: dict[str, str], crates: list[str]) -> None:
    """Tar this shard's (minimized) corpora and upload them as a Buildkite artifact.

    Only `crates` (the ones this shard actually fuzzed) are uploaded, so the
    artifact stays the single shard's corpus. The artifact name is fixed. Shards
    are told apart by their Buildkite job, not the filename (see
    `download_previous_corpus`).
    """
    if not buildkite.is_in_buildkite():
        return
    dirs = _corpus_dirs(crates)
    if not dirs:
        return
    try:
        subprocess.run(
            ["tar", "-caf", CORPUS_ARTIFACT, "-C", str(MZ_ROOT), *dirs],
            cwd=MZ_ROOT,
            env=env,
            check=True,
        )
        buildkite.upload_artifact(CORPUS_ARTIFACT, cwd=MZ_ROOT)
        say(f"uploaded corpus artifact {CORPUS_ARTIFACT} ({len(dirs)} crate(s))")
    except Exception as e:  # never fail the run over corpus housekeeping
        say(f"corpus upload failed (non-fatal): {e}")


def download_previous_corpus(env: dict[str, str]) -> None:
    """Seed corpora from the newest prior build that uploaded one.

    Best effort: any failure (missing token, no prior artifact, API hiccup)
    just leaves the seed corpora in place and logs a note.
    """
    if not buildkite.is_in_buildkite():
        return
    try:
        from materialize.buildkite_insights.buildkite_api import (
            artifacts_api,
            builds_api,
        )

        pipeline = os.environ["BUILDKITE_PIPELINE_SLUG"]
        branch = os.environ.get("BUILDKITE_BRANCH") or "main"
        builds = builds_api.get_builds(
            pipeline,
            max_fetches=1,
            branch=branch,
            build_states=["passed", "failed"],
            items_per_page=20,
        )

        # This shard always fuzzes the same crates, so its corpus lives on the
        # job for the same part. Parallel jobs carry a 0-based
        # `parallel_group_index` (mkpipeline's `%N` label suffix is its human
        # form). A non-parallel run has none, so match `None` then.
        want_index = (
            buildkite.get_parallelism_index()
            if buildkite.get_parallelism_count() > 1
            else None
        )

        def is_my_fuzz_job(j: dict) -> bool:
            fields = (j.get("step_key"), j.get("name"), j.get("label"))
            return (
                any("cargo-fuzz" in (f or "") for f in fields)
                and j.get("parallel_group_index") == want_index
            )

        for build in builds:
            job = next((j for j in build.get("jobs", []) if is_my_fuzz_job(j)), None)
            if job is None:
                continue
            artifacts = artifacts_api.get_build_job_artifact_list(
                pipeline, build["number"], job["id"]
            )
            art = next(
                (a for a in artifacts if a.get("filename") == CORPUS_ARTIFACT), None
            )
            if art is None:
                continue
            dest = MZ_ROOT / CORPUS_ARTIFACT
            artifacts_api.download_artifact_to_file(
                pipeline, build["number"], job["id"], art["id"], str(dest)
            )
            subprocess.run(
                ["tar", "-xaf", str(dest), "-C", str(MZ_ROOT)], env=env, check=True
            )
            say(f"seeded corpus from build #{build['number']}")
            return
        say("no previous corpus artifact found; starting from seeds")
    except Exception as e:
        say(f"corpus download failed (non-fatal): {e}")


def shard_jobs_by_crate(jobs: list[Job], index: int, count: int) -> list[Job]:
    """Select this shard's jobs, assigning whole crates to shards.

    Sharding by target would split a multi-target crate across machines, so its
    instrumented crate build would run on every shard owning one of its targets.
    Keeping a crate's targets together means each fuzz crate is built on exactly
    one machine. Crates are packed onto the least-loaded shard, heaviest first,
    so per-shard target counts stay even.

    The shared workspace dependency closure (mz-ore, mz-repr, …) is still built
    on every machine, since separate machines don't share a CARGO_TARGET_DIR. The
    build is halved down to that common base.
    """
    by_crate: dict[str, list[Job]] = {}
    for j in jobs:
        by_crate.setdefault(j.crate, []).append(j)
    # Heaviest crate first. The name is a deterministic tiebreaker so every shard
    # computes the identical assignment and they partition the work.
    ordered_crates = sorted(by_crate, key=lambda c: (-len(by_crate[c]), c))
    load = [0] * count
    owner: dict[str, int] = {}
    for crate in ordered_crates:
        lightest = min(range(count), key=lambda i: (load[i], i))
        owner[crate] = lightest
        load[lightest] += len(by_crate[crate])
    return [j for j in jobs if owner[j.crate] == index]


def _jobs_arg(value: str) -> "int | str":
    """Parse --jobs: a positive integer, or the literal 'auto'."""
    if value == "auto":
        return "auto"
    n = int(value)
    if n < 1:
        raise ValueError("--jobs must be >= 1 or 'auto'")
    return n


def autosize_jobs(
    requested: "int | str", num_targets: int, max_parallel: int
) -> tuple[int, int]:
    """Resolve (--jobs, --max-parallel) into concrete (forks, max_parallel).

    For an explicit integer this is a no-op. For 'auto', size fork workers to
    this host's core count so every core stays busy: run all targets at once
    (or the user's --max-parallel cap) and give each ``round(cores /
    concurrent)`` forks. Rounding (rather than flooring) keeps the cores filled
    when targets don't divide evenly, at the cost of mild oversubscription that
    fork workers tolerate.

    Crucially we launch *every* target at once even when targets outnumber
    cores (one fork each, oversubscribing the CPU) rather than running only
    `cores` of them and queueing the rest. A single run gives each target a
    long ``--max-seconds``. If the first `cores` targets each ran for that whole
    budget, the queued targets would never start before the wall clock ran out.
    Oversubscription just time-slices the cores, every target still makes
    progress, which beats some targets never running at all. A caller who
    really wants to bound concurrency can still pass --max-parallel.
    """
    if isinstance(requested, int):
        return requested, max_parallel
    cores = os.cpu_count() or 1
    concurrent = max_parallel or num_targets
    # round-half-up: cores/concurrent, biased to keep every core busy. Floors at
    # one fork once targets reach/exceed cores.
    forks = max(1, (cores + concurrent // 2) // concurrent)
    # The runner treats max_parallel=0 as "launch them all".
    resolved_parallel = concurrent if concurrent < num_targets else 0
    return forks, resolved_parallel


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=0,
        help="-max_total_time per target; 0 = fuzz until crash/Ctrl-C (default)",
    )
    parser.add_argument("--rss-limit-mb", type=int, default=4096)
    parser.add_argument(
        "--sanitizer",
        # Default to none on every platform: macOS arm64 ASan segfaults on
        # startup, and on Linux ASan only adds ~2-3x slowdown without catching
        # our bug class (panics / round-trip drifts, not memory corruption).
        # Pass --sanitizer=address to opt back into ASan's memory coverage.
        default="none",
        help="cargo-fuzz sanitizer to build with (default: none; "
        "pass 'address' for ASan memory coverage at ~2-3x slowdown)",
    )
    parser.add_argument(
        "--jobs",
        type=_jobs_arg,
        default="auto",
        help="libFuzzer fork-mode workers per target (-fork=N): N processes that "
        "share the target's corpus. Pass an integer, or 'auto' (the default) to "
        "fill the machine: forks are sized to this host's core count (cores / "
        "concurrent targets), capping concurrency at the core count when targets "
        "outnumber cores. Pass '1' for the old one-process-per-target behavior. "
        "--max-parallel still caps concurrent targets.",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=0,
        help="cap simultaneously-running targets; 0 = launch them all",
    )
    parser.add_argument(
        "--target-dir",
        default=str(MZ_ROOT / "target"),
        help="shared CARGO_TARGET_DIR for every fuzz crate (default: target/)",
    )
    parser.add_argument(
        "--wall-budget",
        type=int,
        default=0,
        help="overall build+fuzz wall budget in seconds; clamps --max-seconds so "
        "the build phase can't blow the step timeout (0 = no cap)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop every target as soon as one crashes",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="skip the up-front sequential build phase",
    )
    parser.add_argument(
        "--no-minimize",
        action="store_true",
        help="skip the `cargo fuzz cmin` corpus minimization after fuzzing",
    )
    parser.add_argument(
        "--minimize-timeout",
        type=int,
        default=0,
        help="cap post-fuzz corpus minimization at N seconds so it and the "
        "corpus upload finish before the step's hard timeout; 0 = unbounded "
        "(default). Set it below the step timeout minus --wall-budget.",
    )
    parser.add_argument(
        "--corpus-sync",
        action="store_true",
        help="in Buildkite, seed corpora from the last build's artifact and "
        "upload the minimized corpora when done (no-op locally)",
    )
    parser.add_argument(
        "--profile",
        choices=["all", "fruitful"],
        default="all",
        help="`fruitful` restricts the run to the historically high-yield "
        "targets (see FRUITFUL): the SQL-parser round-trip oracles and the rich "
        "hand-written PG parsers/decoders that keep finding bugs, ideal for a "
        "long local run. `all` (default) runs every target. A `filters` list "
        "narrows further within the profile.",
    )
    parser.add_argument(
        "filters",
        nargs="*",
        help="only run crate::target identifiers containing one of these substrings",
    )
    args = parser.parse_args()

    target_dir = Path(args.target_dir)
    env = fuzz_env(target_dir)
    log_dir = target_dir / "fuzz-logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    jobs: list[Job] = []
    for crate in FUZZ_CRATES:
        for target in list_targets(crate, env):
            job = Job(
                crate=crate,
                target=target,
                log_path=log_dir / f"{Path(crate).parent.name}_{target}.log",
            )
            matches_filters = not args.filters or any(
                f in job.name for f in args.filters
            )
            matches_profile = args.profile == "all" or any(
                f in job.name for f in FRUITFUL
            )
            if matches_filters and matches_profile:
                jobs.append(job)

    if not jobs:
        raise ui.UIError(
            f"no fuzz targets matched profile={args.profile} filters={args.filters}"
        )

    # Split the work across machines by crate (see shard_jobs_by_crate). The
    # shard identity is Buildkite's standard parallelism, which `get_parallelism_*`
    # read from BUILDKITE_PARALLEL_JOB[_COUNT]. Set those by hand to shard a local
    # run (e.g. `BUILDKITE_PARALLEL_JOB=0 BUILDKITE_PARALLEL_JOB_COUNT=2`).
    shard_index = buildkite.get_parallelism_index()
    shard_count = buildkite.get_parallelism_count()
    total = len(jobs)
    if shard_count > 1:
        jobs = shard_jobs_by_crate(jobs, shard_index, shard_count)
    if not jobs:
        say("this shard has no targets; nothing to do")
        return
    # The crates this shard owns (declaration order). Only these get their
    # corpus prepared, fuzzed, and uploaded.
    shard_crates = [c for c in FUZZ_CRATES if any(j.crate == c for j in jobs)]
    if shard_count > 1:
        say(
            f"shard {shard_index}/{shard_count}: {len(jobs)} of {total} targets "
            f"across {len(shard_crates)} crate(s): {sorted(j.name for j in jobs)}"
        )

    jobs_per_target, max_parallel = autosize_jobs(
        args.jobs, len(jobs), args.max_parallel
    )
    if args.jobs == "auto":
        say(
            f"--jobs auto: {os.cpu_count() or 1} core(s), {len(jobs)} target(s) "
            f"this shard -> -fork={jobs_per_target}"
            + (f", max-parallel={max_parallel}" if max_parallel else " (all at once)")
        )

    runner = FuzzRunner(
        jobs=jobs,
        env=env,
        max_seconds=args.max_seconds,
        rss_limit_mb=args.rss_limit_mb,
        jobs_per_target=jobs_per_target,
        max_parallel=max_parallel,
        fail_fast=args.fail_fast,
        wall_budget=args.wall_budget,
        minimize=not args.no_minimize,
        minimize_timeout=args.minimize_timeout,
        sanitizer=args.sanitizer,
        triple=host_triple(env),
    )
    if args.corpus_sync:
        download_previous_corpus(env)
    # Seed only this shard's corpora. The other crates are never fuzzed here.
    for crate in shard_crates:
        prepare_corpus(crate, env)
    if not args.no_build:
        runner.build()
    failed = runner.run()
    if args.corpus_sync:
        # After run() (which has minimized) so we upload the lean corpus, and
        # before the raise below so it persists even when a target crashed.
        upload_corpus(env, shard_crates)
    if failed:
        raise ui.UIError(
            f"{len(failed)} fuzz target(s) crashed: {[j.name for j in failed]}"
        )
