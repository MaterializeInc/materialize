# Invariants

A correctness-under-chaos test framework: multi-threaded scenarios whose
invariants hold no matter which concurrent operations succeed, fail, or end up
in an unknown state, checked *continuously* while toxiproxy cuts connections and
processes are killed, and strictly again after healing.

## Why another workload

The trick is picking invariants that are **outcome-independent**. A bank
transfer debits one account and credits another, so the grand total is
conserved whether the transfer committed, was rejected, or timed out with an
unknown outcome. That is what makes it safe to assert exact results *during* a
disruption, rather than waiting for quiescence and comparing against an oracle
that chaos has invalidated.

| | catches | verifies results |
|---|---|---|
| parallel-workload | panics, unexpected errors | no result oracle |
| zippy | wrong results, single-threaded | after quiescing |
| **invariants** | **wrong results, lost/duplicated writes** | **continuously, under disruption** |

## How a run works

1. **setup**: create the objects, record the oracle's starting point.
2. **chaos** (`--runtime`, default 600s): worker threads issue writes, checker
   threads verify invariants, the disruptor injects/heals, the agitator flips
   feature flags and cancels connections. Any checker failure fails the run
   immediately.
3. **heal**: every disruption is removed, with retries.
4. **converge**: wait for all data paths to catch up.
5. **final check**: strict assertions, plus a vacuity check: every checker and
   the workers must have made progress in *both halves* of the chaos phase, and
   the disruptor and agitator must have acted. A thread that wedges midway, or a
   run where nothing was ever disrupted, fails instead of passing silently.

Each thread draws its own seeded RNG in a fixed order, so `--seed` reproduces
the whole action/disruption sequence.

## What gets disrupted

Toxiproxy fronts each *leg*: persist consensus (metadata store), persist blob,
`envd`<->`clusterd` storagectl/computectl, and the source/sink/registry
connections. Disruptions are `disable`, `latency`, `timeout`, `limit_data` and
`bandwidth`, applied to one direction only so half are asymmetric. On top of
that the disruptor SIGKILLs and SIGSTOPs `environmentd` and the `clusterd`
processes, deliberately overlapping process kills with leg cuts and following
some heals with an immediate kill, since the post-heal window is where recovery
bugs live. Outages are capped per leg (a metadata cut must stay well under the
15-minute persist lease expiry). Coverage is reported at the end, and a
deterministic first sweep guarantees no leg goes untouched by RNG accident.

`--upgrade-from=<image>` starts on an older release and swaps in the current
build at the chaos midpoint: an upgrade under concurrent load *and* disruptions,
with the invariants never pausing.

## Scenarios

| `--scenario` | invariant |
|---|---|
| `table-bank` | conserved total across tables, MVs, indexes, temporal filters, `REFRESH EVERY`, schema swaps, replacement MVs, `COPY TO`/`FROM` |
| `pg-cdc-bank`, `mysql-cdc-bank`, `sqlserver-cdc-bank` | the same conserved total, written upstream and replicated in, plus upstream DDL and real-time recency |
| `kafka-ledger` | append-only Kafka ledger sums to the produced total |
| `kafka-upsert` | last value per key wins under retractions |
| `sink-roundtrip` | sink out, source back in, must equal the original |
| `webhook-set` | every accepted webhook body appears exactly once |
| `avro-loopback` | Avro/CSR encode-decode preserves every row |

`--complexity=low|medium|high` scales workers, disruption frequency and
concurrency. `--no-disruptions` runs the same workload and checkers clean, which
is how you tell a product bug from a chaos artifact.

## Read-path coverage

The same invariant is verified through deliberately different plans and
protocols, because a wrong answer usually only shows up in one of them: one-shot
peeks (maintained MV, ad-hoc over base tables, and result-equivalent joins,
window functions, recursive CTEs and LATERAL subqueries), `SUBSCRIBE` with
`PROGRESS` (both fresh and resumed from a durable timestamp), read-only
transaction snapshots, `AS OF` time travel into retained history, `COPY TO`
export, and a post-run audit that replays the entire retained history and
validates every progress boundary, retroactively closing the rounds live
checkers had to skip during outages. Reads rotate over isolation levels, since a
timestamp-free invariant must hold under all of them.

## Findings so far

Each finding has a concentrated reproducer, selectable via `--scenario`:

- [PER-10](https://linear.app/materializeinc/issue/PER-10): persist GC panic,
  earliest state without rollup (`repro-per10`)
- [PER-31](https://linear.app/materializeinc/issue/PER-31): unbounded clusterd
  memory while the blob store is cut (`repro-blob-memory`)
- [PER-32](https://linear.app/materializeinc/issue/PER-32): writes stalled long
  after a metadata cut healed (`repro-postheal-stall`)
- [PER-49](https://linear.app/materializeinc/issue/PER-49): compute halts
  hydrating a dataflow past its `as_of` (`repro-compute-asof`)
- not yet filed: a resumed `SUBSCRIBE` loses its carried state
  (`repro-durable-resume`)

## Running it

```shell
bin/mzcompose --find invariants run default --scenario=table-bank --runtime=120
bin/mzcompose --find invariants run default --scenario=repro-postheal-stall
```

Wired into Nightly as 11 steps (one per scenario, plus a large `table-bank` and
the upgrade-under-load variant). Every failure annotation carries the exact
reproducer command, and the run log opens with the random seed that replays it.
