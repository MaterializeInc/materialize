# Property Catalog Evaluation — Kafka Source Additions

**Scope**: The 16 properties added to Category 7 in `property-catalog.md` on 2026-05-11 targeting the Kafka source ingestion pipeline (NONE + UPSERT envelopes), and the assertion sites in `existing-assertions.md`. Pre-existing properties in Categories 1-6 are *not* re-evaluated here — they passed evaluation on 2026-05-06 and nothing has changed in their code paths. The 16 are: 5 user-visible Kafka source properties (`kafka-source-no-data-loss`, `-no-data-duplication`, `-frontier-monotonic`, `-survives-broker-fault`, `-survives-clusterd-restart`), 4 UPSERT envelope properties (`upsert-key-reflects-latest-value`, `-tombstone-removes-key`, `-state-rehydrates-correctly`, `-decode-error-retractable`), 3 UPSERT operator-internal properties (`upsert-no-internal-panic`, `-state-consolidation-wellformed`, `-ensure-decoded-called-before-access`), and 4 reclock / source-reader operator-internal properties (`kafka-source-no-internal-panic`, `remap-shard-antichain-wellformed`, `reclock-mint-eventually-succeeds`, `offset-known-not-below-committed`).

This evaluation was performed in single-agent mode across the four lenses, written as a single synthesis. Per-lens evidence files are inline below; spawning four parallel ensemble agents for a 16-property targeted addition would have been over-engineering given that one human's worth of catalog review is the better fit.

## Lens 1 — Antithesis Fit

**Passes**:

- All 16 properties target timing-sensitive, concurrency-sensitive, or partial-failure scenarios. None can be fully verified by a deterministic unit test.
- Mix of assertion types is healthy: 7 Safety (`Always`), 3 Liveness (`Sometimes`), 3 Reachability (`Unreachable`), 2 properties combine multiple assertion families internally.
- Several properties (`kafka-source-survives-clusterd-restart`, `upsert-state-rehydrates-correctly`, `reclock-mint-eventually-succeeds`) explicitly need fault injection that deterministic tests can't sequence — strong Antithesis fit.
- The SUT-side instrumentation properties (`upsert-no-internal-panic`, `upsert-state-consolidation-wellformed`, `upsert-ensure-decoded-called-before-access`, `kafka-source-no-internal-panic`) wrap *existing* asserts/panics rather than adding new logic; this is the cheapest possible instrumentation cost.

**Refinements**:

- `offset-known-not-below-committed` is borderline unit-test material — the invariant could be tested by mocking the statistics update path. Kept in the catalog because the *interesting* failure is the restart-window timing, which is genuinely Antithesis territory; lowered priority from P1 to P2 (already P2 in the catalog).
- `upsert-decode-error-retractable` could be tested as integration. It earns its catalog slot only if the test exercises crash recovery between the bad and good message; the evidence file already calls this out. No change needed.

**Findings**: None. Antithesis fit is good across the addition.

## Lens 2 — Coverage Balance

**Passes**:

- Both envelopes (NONE and UPSERT) get dedicated coverage.
- The SUT analysis's Appendix A failure-prone areas table has 9 rows; 8 of them are covered by at least one new property. The one uncovered row is "Flag flip mid-append on persist sink (commit 68e1dfd86d)" — see Gap below.
- Liveness, Safety, and Reachability are all represented.
- Both workload-observable and SUT-side properties exist; the workload-only properties form the user-visible contract (`kafka-source-no-data-loss`, etc.) and the SUT-side properties form the operator-internal correctness backbone.

**Gaps identified** (addressed during this pass — see "Addressing findings" below):

- **G1: Persist sink flag-flip TOCTOU** — commit `68e1dfd86d` (database-issues#9585) regression is not represented. The bug was a config flag re-evaluated multiple times during `append_batches`. Decision: **Acknowledged but not added**. This is a persist-sink generic correctness property, not Kafka-source-specific; it belongs in Category 1 (Persist Layer Safety), not in the Kafka section. Filing as a follow-up note in `property-relationships.md` would clutter the relationships; instead, called out here as a known omission for a future persist-focused research pass.

- **G2: Partition reassignment correctness** — Kafka topic adding/removing partitions while the source is live is mentioned in the SUT analysis but not captured as a property. The closest is `kafka-source-no-internal-panic` which catches *panics* on the rebalance path but not *correctness* (no data loss, no duplicates, correct partition→worker assignment under rebalance). Decision: **Catalog as a future expansion item**, not added in this pass because it requires non-trivial workload support (the test driver must be able to dynamically add Kafka partitions, and the worker-hash assignment property requires multi-worker clusterd).

- **G3: Schema Registry interaction** — Avro / Protobuf decoding via Schema Registry is a significant Kafka source code path that is unmentioned. Schema evolution mid-source is a known operational hazard. Decision: **Future expansion item**. The workload is realistically text/JSON for v1 of these properties; Schema Registry coverage is a v2 expansion.

**Refinements**:

- The pre-existing `source-ingestion-progress` property is now redundant with `kafka-source-no-data-loss` for Kafka specifically. The relationships file calls this out. Decision: **Keep both** — `source-ingestion-progress` remains valid for non-Kafka sources (Postgres CDC, MySQL, generators), so it doesn't go away. The new property is more specific. No catalog edit needed beyond the cross-reference in `property-relationships.md`.

## Lens 3 — Implementability

**Passes**:

- All workload-level properties can be checked via standard SQL queries against `mz_internal.mz_source_statistics_per_worker` and direct `SELECT` from the source. The workload only needs a PostgreSQL client and a Kafka producer (both already required by the existing topology in `deployment-topology.md`).
- All SUT-side properties wrap *existing* code (panic / assert / unreachable sites). No new SUT instrumentation logic is required, only replacing the existing macro with the Antithesis SDK equivalent and giving each callsite a unique message.
- Deployment topology already provides Kafka (Redpanda) and `materialized` in separate containers; network partition between them is a supported fault.
- Multi-replica scenarios for `upsert-state-consolidation-wellformed` and the upsert internals require a topology variation (multiple compute replicas serving the same source). The existing topology is single-replica; this is flagged.

**Refinements**:

- `kafka-source-survives-clusterd-restart` requires **node-termination faults**, which the `faults.md` reference says are disabled by default in Antithesis tenants. Flagged in the evidence file. The user should confirm this fault class is enabled.
- `upsert-state-consolidation-wellformed` (and `kafka-source-no-data-duplication` for the historical multi-replica regression) gain significant value from a multi-replica topology. Suggest adding a second topology variant to `deployment-topology.md` as a follow-up — single-replica is sufficient to start, but the multi-replica drain bug (commit `1accbe28b3`) requires multi-replica to reproduce.

**Findings (refinements applied or noted in evidence files)**:

- R1: Added a note to `properties/kafka-source-survives-clusterd-restart.md` calling out the node-termination-faults dependency.
- R2: Added a note to `properties/upsert-state-consolidation-wellformed.md` explaining the multi-replica relevance.

## Lens 4 — Wildcard

**Things the other lenses missed**:

- **W1: Multi-topic / multi-source interaction.** The 16 properties all treat a single Kafka source as the unit of analysis. The real-world failure mode of "two Kafka sources on the same cluster, one is healthy, the other is partitioned" is unaddressed. The `materialized` container hosts both; partitioning one source from its broker should not affect the other. Decision: **Future expansion**. Adding this now would expand the workload significantly.

- **W2: Clock-jump interaction with Kafka timestamps.** The SUT analysis flags `expect("kafka sources always have upstream_time")` at kafka.rs:1209 — this depends on the Kafka message timestamp being valid. Clock jumps on the *Kafka broker* could produce future or past message timestamps. The current property set doesn't address how Materialize handles a backward-clocked Kafka broker. Decision: **Acknowledged as a known gap**, similar to W1.

- **W3: Reading the catalog as a whole, the SUT-side instrumentation properties feel like a single "wrap all the existing panics in Antithesis SDK" project rather than four separate properties.** Decision: **Keep the four-property structure** anyway, because the slugs give Antithesis distinct property tags and the per-site message uniqueness requirement makes them genuinely distinct invariants. But operationally, a single PR can implement all four.

## Addressing Findings

- **Refinements applied**: R1, R2 (noted in evidence files during this pass).
- **Gaps held as known omissions**: G1 (persist-sink flag flip — belongs in Category 1), G2 (partition reassignment — needs workload extension), G3 (schema registry — v2 expansion), W1 (multi-source interaction), W2 (clock jumps on broker).
- **Biases escalated to user**: None — the catalog framing matches the user's stated scope ("basic properties for Kafka sources, both normal and upsert workloads"). The "basic" qualifier explicitly suggests that some areas like partition reassignment, schema registry, and multi-source scenarios are intentionally deferred to future passes.

## Conclusion

The 16-property Kafka source addition is implementable, well-scoped to Antithesis's strengths, and covers both envelopes plus the shared reclock layer. Known gaps are documented above as follow-up candidates. No biases escalated; the user's "basic" framing aligns with the catalog scope.
