# A Workflow for Claude Code on Performance Investigations

I have been using Claude Code for performance investigation and optimization
work on the Materialize coordinator. Over two projects — a long DDL performance
saga (24 sessions, 10+ optimizations) and a focused storage-usage fix (4
sessions in one day, 25x speedup) — a workflow emerged that makes Claude more
useful for this kind of work. The key ingredients are a structured prompt file,
a persistent session log, and a disposable VM where Claude can operate without
permission babysitting.

**Note:** The actual prompt files, session logs, and commit history for both
projects are linked in the [References](#references) section at the end.

## The problem

Performance investigation is iterative. You measure, form a hypothesis,
instrument or fix something, measure again. Each step produces data that
informs the next. Claude Code is good at this kind of work — it can read code,
write instrumentation, run builds, capture metrics, and analyze results — but
it has two problems out of the box:

1. **Context loss.** Claude compresses old conversation history as it
   approaches its context limit. A long investigation session can lose earlier
   measurements, hypotheses, and findings. If you start a new session,
   everything is gone.

2. **Permission fatigue.** Claude asks for permission before commands not on
   your allow list. For everyday coding, most engineers have a good allow list
   — builds, tests, formatters. But performance investigations need commands
   you'd never permanently allow: `pkill` to stop processes, `perf` for CPU
   sampling, installing system packages, setting up databases from scratch,
   bulk-creating thousands of test objects via `psql`. You either approve each
   one manually or pollute your allow list with things you don't want there by
   default.

## PROMPT.md: a file that survives context loss

The core of the workflow is a `PROMPT.md` file at the repository root. This
is the first thing Claude reads when you start a session (you point it at the
file with `@PROMPT.md`), and the file itself tells Claude to re-read it after
every context compaction. It is the persistent brain of the investigation.

The structure below is what I arrived at after two projects.

- **Problem statement.** What is broken and why. References to a design doc (if
  any) and session log. This is what Claude reads to orient itself.

- **Workflow rules.** One commit per milestone. Reread the prompt after
  context compaction. Update the log after each milestone. Update the design
  doc if findings contradict it. Minimal changes — clean up to the minimum
  required fix at the end.

- **Setup.** Exact build commands, connection strings, how to create test data.

- **Key metrics table.** Which Prometheus histograms to capture and what they
  measure. For the storage-usage investigation:

  | Metric | What it measures |
  |--------|-----------------|
  | `mz_slow_message_handling{message_kind="storage_usage_update"}` | Wall-clock stall on coord thread |
  | `mz_storage_usage_collection_time_seconds` | Off-thread shard scan (not the problem) |

- **Cost breakdown table.** Initially predicted from reading the code, then
  updated with actual measurements. For storage-usage, the prediction was that
  persist I/O and oracle calls would dominate. The measurement showed that the
  transact_inner op loop was 91% of the cost. Having both the prediction and
  the measurement in the prompt helps Claude (and me) reason about what to fix.

- **Current status and completed steps.** Claude updates these after each
  milestone. When it re-reads the prompt after a context compaction or at the
  start of a new session, it immediately knows what has been done and what
  remains.

## The session log

Alongside `PROMPT.md`, there is a log file (`storage-usage-log.md`,
`ddl-perf-log.md`) that records per-session findings. Claude appends to it
after each milestone.

The storage-usage log had four sessions in one day:

1. **Baseline measurements.** ~51ms at 2.4k shards, ~150ms at 5k, ~499ms at
   10k. Linear scaling.
2. **Instrumentation.** Added timing to each phase. Found the op loop at 91%
   of total cost (~430ms out of 475ms).
3. **Fix.** Bypassed `catalog_transact_inner`, packed rows directly, replaced
   durable ID allocator with local counter. Result: 499ms to 20ms.
4. **Cleanup.** Removed dead code (the old op variant, the durable allocator,
   instrumentation logging).

The DDL-perf log grew to 2,500 lines over 24 sessions. It recorded things like:
"Session 19 showed table_register as 95% of cost in debug builds but only 20%
in optimized builds — debug proportions are misleading for I/O-bound work."
Findings like this, persisted in the log, prevented the agent from repeating
mistakes in later sessions.

The log is what makes multi-session investigations work. Without it, each
session would start from scratch.

## Two examples compared

**DDL performance** was a long saga. 24 sessions over multiple days. The prompt
was updated continually as we completed optimizations and discovered new
bottlenecks. The work produced 10+ distinct optimizations — caching, batching,
bypassing unnecessary catalog codepaths, and replacing eager validation with
lazy alternatives. Single-statement DDL latency at 100k objects went from
multiple seconds to ~264ms median.

**Storage-usage collection** was a focused, single-day fix. I already knew the
pattern from DDL-perf, so the setup was fast: write the prompt, measure the
baseline, instrument, fix, clean up. Four sessions, four commits (plus the
prompt/log bookkeeping commits). The coordinator stall at 10k shards dropped
from ~499ms to ~20ms.

The same workflow pattern scales from "long saga with many bottleneck layers"
to "quick focused investigation."

## Why `--dangerously-skip-permissions` matters

For everyday coding, Claude's permission model works well — you have an allow
list for common commands and approve the rest. Performance investigations break
this model. A single measure-diagnose-fix cycle might involve `pkill`, `perf
record`, bulk SQL via `psql`, `curl` to a Prometheus endpoint, editing code,
rebuilding, and restarting — commands that don't belong on a permanent allow
list. With `--dangerously-skip-permissions`, Claude runs the full loop
autonomously. You set it loose with a prompt, check in occasionally to see what
it found, and review the commits it produces. This is where the structured
prompt and log become essential — they are Claude's instructions and its
notebook. You review its work through the git history, not by watching it type.

The tradeoff is obvious: Claude can run arbitrary commands on your machine. So
you need a safe environment.

## Ember: cheap disposable VMs for unsandboxed Claude

In the past, I used Materialize scratch instances on EC2 for this. Those work
but they are heavyweight: they take minutes to provision and they cost real
money while running.

I now have my own microVM manager called
[ember](https://github.com/aljoscha/ember). Ember wraps Firecracker (Amazon's
lightweight hypervisor) with ZFS-backed storage. The key feature for our
workflow is instant VM forking: because ZFS uses copy-on-write, `ember vm fork
base task-1` creates a full clone of a VM in milliseconds, regardless of disk
size. Each forked VM is fully isolated.

### One-time: create a base VM

```bash
sudo ember image build ubuntu-dev
sudo ember vm create base --image ubuntu-dev --cpus 4 --memory 8G --disk-size 32G
sudo ember vm start base
```

The base VM ships with a Rust toolchain, Docker, and basic development tools
already installed in the image. For Materialize-specific setup (cloning the
repo, setting up CockroachDB, configuring Claude Code), I have an idempotent
provisioning script:

```bash
# Provisions the VM: SSH key, git config, repo clone, CockroachDB in Docker,
# Claude Code with --dangerously-skip-permissions aliased, and more
./mz-provision.py --ember base
```

The script is idempotent — each step checks whether it's already been done. It
abstracts over both ember VMs and cloud scratch instances, so the same command
works for either.

After provisioning, stop the base VM:
```bash
sudo ember vm stop base
```

### Per-task: fork and go

```bash
# Fork a fresh VM from the base (instant, copy-on-write)
sudo ember vm fork base storage-usage-fix
ember ssh storage-usage-fix

# Inside the VM:
cd ~/materialize
git pull                        # get the branch with PROMPT.md
claude "follow @PROMPT.md"      # aliased to claude --dangerously-skip-permissions
# ... Claude works autonomously, following PROMPT.md ...
# ... commits results to git as it goes ...
git push                        # push results out

# Back on host:
git pull                        # pull Claude's commits
# Review, give feedback, push new instructions via PROMPT.md updates
# When done:
sudo ember vm delete storage-usage-fix
```

The git push/pull loop is how you communicate with Claude across the VM
boundary. Push an updated PROMPT.md to adjust instructions, pull to review what
Claude produced. The VM is disposable — if something goes wrong, delete it and
fork a fresh one. This takes seconds, not minutes.

You can also run multiple investigations in parallel by forking multiple VMs
from the same base. Each gets its own isolated copy of the repo and its own
running Materialize instance.

## Closing thoughts

The key insight is that PROMPT.md is not just instructions — it is a living
document that Claude updates as it works. The current status, the cost
breakdown tables, the completed steps — these are all maintained by Claude as
part of the workflow. When you come back to review, the prompt and the log tell
you exactly what happened and where things stand.

## References

The branches below contain the actual prompt files, session logs, design docs,
and full commit history for each project. You can see exactly what Claude was
given and what it produced.

- **Storage-usage fix:**
  [`design-storage-usage-blocking`](https://github.com/aljoscha/materialize/tree/design-storage-usage-blocking)
  — `PROMPT.md`, `storage-usage-log.md`, `misc/storage-usage-coord-blocking.md`,
  and 9 commits from baseline through cleanup.

- **DDL performance:**
  [`spike-ddl-perf`](https://github.com/aljoscha/materialize/tree/spike-ddl-perf)
  — `PROMPT.md` (called `ddl-perf-prompt.md`), `ddl-perf-log.md` (2,500 lines),
  and 24 sessions of optimizations.

- **Ember (microVM manager):**
  [github.com/aljoscha/ember](https://github.com/aljoscha/ember)
