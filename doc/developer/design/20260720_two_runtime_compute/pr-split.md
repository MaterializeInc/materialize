# Cutting the branch into landable pieces

The branch is about 12,100 inserted lines across 51 files. This is how it splits, what each piece depends on, and why the order is what it is.

## Already extracted

These are open separately and the split below layers on top of them.

| PR | Contents | State |
|---|---|---|
| #37884 | `ComputeRuntimeRole` and role-labeled metrics | ready |
| #37881 | `row-spine` spines backed by `Arc` for cross-thread sharing | ready |
| #37880 | `clusterd-test-driver` s/u/t-prefixed global ids | ready |
| #37747 | Design doc | draft |

## What the measurements imply about order

E2 found that the walk substrate, not the second runtime, is what removes head-of-line blocking between peeks: V1 equals V3 at every scan cost. E11 found that the substrate is also what stops one skewed key stalling every lookup behind it, and that shape was always reachable because `ORDER BY` finishings never qualified for the peek response stash.

So the peek offload carries essentially all of the measured peek benefit and depends on none of the two-runtime machinery. It should land first and separately. The second runtime's own justification is E7, temporary dataflows under maintenance load, and E9, a replica that stays introspectable while it hydrates. Those are real but they are a different claim and a much larger diff.

## Stack A: the peek walk substrate

No dependency on the second runtime or on arrangement sharing. Lands behind a default-off dyncfg.

| PR | Contents | Size |
|---|---|---|
| A1 | Extract the index peek walk into reusable pieces: `drain_ok_iterator`, `scan_errs_for_error`, `peek_stash_config`, `IndexPeekMetrics`. No behaviour change. | ~300 |
| A2 | Walk a peek's cursor off the serving worker: `local_snapshot.rs`, `OffloadSnapshot`, `spawn_offloaded_walk`, `offloaded_response`, `PendingPeek::IndexOffload`, `ENABLE_INDEX_PEEK_OFFLOAD` and `INDEX_PEEK_OFFLOAD_MAX_INFLIGHT`, both default off | ~500 |
| A3 | `mz_index_peek_walks_total{substrate}` | ~40 |
| A4 | Let an offloaded walk divert to the peek response stash: `upload_blocking`, remove the stash gate, spare cursor | ~200 |
| A5 | Parallel benchmark for the skewed point lookup, as a regression guard | ~150 |

A1 is a mechanical refactor and should be reviewed as one. A2 is the substance. A3 exists because without it "the offload changed nothing" and "the offload never ran" are indistinguishable, which cost a full round of staging measurement. A4 is what makes the feature reachable in a production configuration, since production runs the stash on.

## Stack B: command multiplexing and the protocol invariant

`src/compute-client/src/multiplex.rs` imports only compute-client protocol types, so it is independent of everything on the compute side and is inert with a single runtime.

| PR | Contents | Size |
|---|---|---|
| B1 | Multiplex compute commands across two client streams, plus capping `AllowCompaction` at interactive read holds and the `compaction_floor` that stops a cap regressing | ~1200 |

Worth splitting the capping out only if review finds it contentious. The mechanism exists because the split loses command ordering between the two streams, so an index can be told to compact past the `as_of` of an interactive dataflow that imports it. `protocol.tla` models exactly that and belongs with this PR rather than with the design doc.

## Stack C: arrangement sharing and the second runtime

The bulk, about 5,200 lines, and the riskiest.

| PR | Contents | Size |
|---|---|---|
| C1 | Per-process arrangement sharing registry: `sharing.rs`, `shared_trace.rs`, with unit tests | ~3,700 |
| C2 | Render index imports from the registry: `render.rs`, `typedefs.rs`, arrange/threshold/join edges | ~900 |
| C3 | Launch the second interactive timely runtime: `server.rs` role plumbing, `clusterd`, `cluster/client.rs`, the controller's `interactive` port and its length guard | ~500 |
| C4 | Scope `enable_two_runtime_compute` to replicas | ~120 |
| C5 | Serve peeks from the registry: the shared peek path and `shared_snapshot_for_offload` | ~400 |
| C6 | Tests and harness: clusterd-test-driver specs, `two_runtime_shared_fate`, mzcompose and parallel-workload flag registration | ~650 |
| C7 | Fold `evaluation.md` and the stash plan into #37747 | docs |

C1 lands code nothing calls yet, which is the usual objection. The alternative is to merge C1 and C2, at about 4,600 lines. Given this repo squash-merges and lands large branches sequentially, C1 plus C2 as one PR is probably the better trade, with the registry's tests carrying the review.

C5 depends on both A2 and C1, and is the only place the two stacks meet.

## Dependency order

```
#37884, #37881, #37880  (already open)
        |
        A1 -> A2 -> A3 -> A4 -> A5        independently mergeable, ships the peek win
        |
        B1                                 inert with one runtime
        |
        C1+C2 -> C3 -> C4 -> C5 -> C6 -> C7
```

A is worth landing on its own schedule regardless of what happens to C, because it carries the peek results and none of the risk. C should not be gated on A except for C5.

## What is not yet resolved

* The in-file test harness in `compute_state.rs` (`make_peek`, `publish_kv_index`, `reduce_count_dataflow`, `test_metrics` and friends) is shared by A and C tests. Whichever lands first carries it, so A1 should take it.
* `evaluation.md` records several experiments whose conclusions argue for A over C. That belongs in review, not buried in a doc commit.
