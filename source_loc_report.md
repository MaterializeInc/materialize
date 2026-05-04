# Code LOC Tree

Generated: 2026-05-02 11:12:51 EDT
Root: `/Users/jc/worktrees/materialize-arch-review`
Commands: `uv run --no-project loc_tree.py --analysis` and `uv run --no-project loc_tree.py`

Counts `.rs`, `.py`, and `.ts` files. In the tree, each line shows cumulative LOC for the subtree; `(N own)` is the LOC of code files directly in that directory when both direct and nested code exist.

## Threshold analysis

Pick a threshold *M*; each subtree whose cumulative LOC > *M* becomes one analysis unit. Counts are *non-overlapping*: when a subtree exceeds *M* and a child subtree also exceeds *M*, the child stands in for the parent.

| M (LOC) | # units | sample of largest units |
|---:|---:|---|
| 2,000 | 88 | `src/catalog-protos/src`, `src/persist-client/src/internal`, `src/transform/src` |
| 5,000 | 40 | `src/catalog-protos/src`, `src/persist-client/src/internal`, `src/transform/src` |
| 10,000 | 23 | `test`, `src/catalog-protos/src`, `src/compute/src` |
| 15,000 | 18 | `test`, `src/sql/src/plan`, `src/catalog/src` |
| 20,000 | 15 | `misc/python/materialize`, `test`, `src/expr/src` |
| 30,000 | 11 | `misc/python/materialize`, `src/adapter/src`, `test` |
| 50,000 | 5 | `misc/python/materialize`, `src/adapter/src`, `src/sql/src` |
| 100,000 | 2 | `src`, `misc` |

### Partition at M = 10,000 (23 units)

| LOC | path |
|---:|---|
| 53,017 | `test` |
| 31,194 | `src/catalog-protos/src` |
| 24,538 | `src/compute/src` |
| 23,526 | `src/persist-client/src/internal` |
| 23,397 | `src/transform/src` |
| 18,721 | `console/src/api/materialize` |
| 16,866 | `src/storage/src/source` |
| 16,458 | `src/adapter/src/catalog` |
| 16,303 | `src/environmentd/tests` |
| 15,248 | `misc/python/materialize/output_consistency` |
| 15,121 | `src/ore/src` |
| 14,401 | `src/storage-types/src` |
| 13,917 | `src/sql/src/plan/statement` |
| 13,781 | `src/catalog/src/durable` |
| 13,050 | `misc/python/materialize/checks/all_checks` |
| 12,630 | `console/src/platform` |
| 12,392 | `src/expr/src/scalar/func` |
| 12,106 | `src/environmentd/src` |
| 12,030 | `src/adapter/src/coord/sequencer` |
| 11,937 | `src/repr/src/adt` |
| 11,465 | `src/avro` |
| 10,256 | `src/sql-parser/src/ast/defs` |
| 10,077 | `src/compute-client/src` |

### Recommendation

**M = 10,000** is the sweet spot for "modules and sub-modules" analysis:

- **23 non-overlapping units**, each 10K–30K LOC — large enough to deserve a dedicated pass, small enough to hold in working memory.
- Captures both top-level crates (`sql`, `adapter`, `persist-client`, `storage`, `expr`, `catalog`, `repr`, `transform`, `compute`, `sql-parser`, `compute-client`) and the natural sub-modules inside the largest crates (`adapter/src/coord` 28K, `sql/src/plan` 36K, `persist-client/src/internal` 23K, `expr/src/scalar` 16K, `catalog/src/durable` 14K).
- Plus the three non-Rust giants: `misc/python/materialize` (95K), `console/src` (48K), `test/` (53K).

Alternatives:
- **M = 15,000** → 18 units. Drops some intra-crate sub-modules but keeps every crate worth its own pass.
- **M = 5,000** → 40 units. Exhaustive; no important sub-module gets missed, at 2× the work.

## Tree

```
.  908,628 LOC (242 own)
├── src/  695,350 LOC
│   ├── adapter/  71,323 LOC
│   │   ├── src/  70,668 LOC (21,166 own)
│   │   │   ├── coord/  28,576 LOC (16,334 own)
│   │   │   │   ├── sequencer/  12,030 LOC (4,897 own)
│   │   │   │   │   └── inner/  7,133 LOC
│   │   │   │   └── catalog_implications/  212 LOC
│   │   │   ├── catalog/  16,458 LOC (14,645 own)
│   │   │   │   ├── open/  1,619 LOC
│   │   │   │   └── builtin_table_updates/  194 LOC
│   │   │   ├── optimize/  2,731 LOC
│   │   │   ├── explain/  1,116 LOC
│   │   │   └── config/  621 LOC
│   │   ├── tests/  604 LOC
│   │   │   └── testdata/  0 LOC
│   │   └── benches/  51 LOC
│   ├── sql/  65,741 LOC
│   │   └── src/  65,741 LOC (19,061 own)
│   │       ├── plan/  36,204 LOC (21,314 own)
│   │       │   ├── statement/  13,917 LOC (12,988 own)
│   │       │   │   └── ddl/  929 LOC
│   │       │   ├── explain/  507 LOC
│   │       │   └── lowering/  466 LOC
│   │       ├── session/  6,919 LOC (2,831 own)
│   │       │   └── vars/  4,088 LOC
│   │       ├── pure/  3,055 LOC
│   │       └── ast/  502 LOC
│   ├── persist-client/  44,363 LOC (94 own)
│   │   ├── src/  43,390 LOC (16,962 own)
│   │   │   ├── internal/  23,526 LOC
│   │   │   ├── cli/  2,023 LOC
│   │   │   └── operators/  879 LOC
│   │   ├── benches/  879 LOC
│   │   ├── proptest-regressions/  0 LOC
│   │   │   └── internal/  0 LOC
│   │   └── tests/  0 LOC
│   │       ├── machine/  0 LOC
│   │       │   └── rewrite/  0 LOC
│   │       └── trace/  0 LOC
│   ├── storage/  38,273 LOC
│   │   ├── src/  36,651 LOC (9,020 own)
│   │   │   ├── source/  16,866 LOC (5,144 own)
│   │   │   │   ├── generator/  6,155 LOC
│   │   │   │   ├── postgres/  2,165 LOC
│   │   │   │   ├── mysql/  2,065 LOC (1,458 own)
│   │   │   │   │   └── replication/  607 LOC
│   │   │   │   ├── sql_server/  1,024 LOC
│   │   │   │   └── reclock/  313 LOC
│   │   │   ├── sink/  4,047 LOC
│   │   │   ├── render/  2,365 LOC
│   │   │   ├── metrics/  1,790 LOC (804 own)
│   │   │   │   ├── source/  579 LOC
│   │   │   │   └── sink/  407 LOC
│   │   │   ├── upsert/  1,738 LOC
│   │   │   ├── storage_state/  542 LOC
│   │   │   └── decode/  283 LOC
│   │   └── examples/  1,622 LOC (1,264 own)
│   │       └── upsert_open_loop/  358 LOC
│   ├── expr/  37,466 LOC (26 own)
│   │   ├── src/  36,891 LOC (13,408 own)
│   │   │   ├── scalar/  16,078 LOC (3,686 own)
│   │   │   │   └── func/  12,392 LOC (4,947 own)
│   │   │   │       ├── impls/  7,445 LOC
│   │   │   │       └── snapshots/  0 LOC
│   │   │   ├── relation/  5,061 LOC
│   │   │   ├── explain/  1,560 LOC
│   │   │   └── row/  784 LOC
│   │   ├── benches/  326 LOC
│   │   ├── tests/  223 LOC
│   │   │   └── testdata/  0 LOC
│   │   └── proptest-regressions/  0 LOC
│   ├── catalog/  37,222 LOC
│   │   ├── src/  35,038 LOC (16,701 own)
│   │   │   ├── durable/  13,781 LOC (9,892 own)
│   │   │   │   ├── objects/  1,871 LOC
│   │   │   │   ├── upgrade/  1,865 LOC
│   │   │   │   │   └── snapshots/  0 LOC
│   │   │   │   └── persist/  153 LOC
│   │   │   ├── memory/  4,213 LOC
│   │   │   └── builtin/  343 LOC
│   │   └── tests/  2,184 LOC
│   │       └── snapshots/  0 LOC
│   ├── repr/  36,800 LOC (39 own)
│   │   ├── src/  35,001 LOC (19,187 own)
│   │   │   ├── adt/  11,937 LOC
│   │   │   │   └── snapshots/  0 LOC
│   │   │   ├── row/  2,987 LOC
│   │   │   └── explain/  890 LOC
│   │   ├── benches/  988 LOC
│   │   │   └── testdata/  0 LOC
│   │   ├── tests/  772 LOC
│   │   └── proptest-regressions/  0 LOC
│   │       └── row/  0 LOC
│   ├── catalog-protos/  31,453 LOC (130 own)
│   │   ├── src/  31,194 LOC
│   │   └── tests/  129 LOC
│   ├── environmentd/  28,430 LOC (21 own)
│   │   ├── tests/  16,303 LOC
│   │   │   └── testdata/  0 LOC
│   │   │       ├── http/  0 LOC
│   │   │       ├── mcp/  0 LOC
│   │   │       └── timezones/  0 LOC
│   │   ├── src/  12,106 LOC (4,425 own)
│   │   │   ├── http/  5,716 LOC
│   │   │   │   └── static/  0 LOC
│   │   │   │       ├── css/  0 LOC
│   │   │   │       └── js/  0 LOC
│   │   │   ├── environmentd/  1,386 LOC
│   │   │   ├── deployment/  567 LOC
│   │   │   └── bin/  12 LOC
│   │   ├── ci/  0 LOC
│   │   └── templates/  0 LOC
│   ├── compute/  24,538 LOC
│   │   └── src/  24,538 LOC (6,895 own)
│   │       ├── render/  8,425 LOC (6,256 own)
│   │       │   └── join/  2,169 LOC
│   │       ├── sink/  4,043 LOC
│   │       ├── logging/  3,514 LOC
│   │       ├── extensions/  792 LOC
│   │       ├── compute_state/  558 LOC
│   │       └── arrangement/  311 LOC
│   ├── transform/  24,378 LOC
│   │   ├── src/  23,397 LOC (19,022 own)
│   │   │   ├── analysis/  1,349 LOC
│   │   │   ├── movement/  956 LOC
│   │   │   ├── fusion/  881 LOC
│   │   │   ├── notice/  408 LOC
│   │   │   ├── cse/  402 LOC
│   │   │   ├── canonicalization/  284 LOC
│   │   │   └── compound/  95 LOC
│   │   └── tests/  981 LOC
│   │       ├── test_transforms/  0 LOC
│   │       │   └── fusion/  0 LOC
│   │       └── testdata/  0 LOC
│   ├── sql-parser/  22,226 LOC (126 own)
│   │   ├── src/  21,861 LOC (10,558 own)
│   │   │   └── ast/  11,303 LOC (1,047 own)
│   │   │       └── defs/  10,256 LOC
│   │   └── tests/  239 LOC
│   │       └── testdata/  0 LOC
│   ├── ore/  15,390 LOC
│   │   ├── src/  15,121 LOC (12,987 own)
│   │   │   ├── netio/  1,063 LOC
│   │   │   ├── collections/  523 LOC
│   │   │   ├── metrics/  414 LOC
│   │   │   └── channel/  134 LOC
│   │   ├── benches/  156 LOC
│   │   └── tests/  113 LOC
│   ├── storage-types/  14,600 LOC (34 own)
│   │   ├── src/  14,401 LOC (9,210 own)
│   │   │   ├── sources/  4,389 LOC
│   │   │   ├── connections/  649 LOC
│   │   │   ├── sinks/  153 LOC
│   │   │   └── snapshots/  0 LOC
│   │   ├── benches/  165 LOC
│   │   └── proptest-regressions/  0 LOC
│   ├── avro/  11,465 LOC
│   │   ├── src/  9,022 LOC
│   │   ├── tests/  1,711 LOC
│   │   ├── benches/  583 LOC
│   │   └── examples/  149 LOC
│   ├── compute-client/  10,077 LOC
│   │   └── src/  10,077 LOC (4,421 own)
│   │       ├── controller/  4,558 LOC
│   │       └── protocol/  1,098 LOC
│   ├── testdrive/  9,278 LOC (59 own)
│   │   ├── src/  9,219 LOC (2,514 own)
│   │   │   ├── action/  5,617 LOC (3,196 own)
│   │   │   │   ├── kafka/  1,849 LOC
│   │   │   │   ├── sql_server/  202 LOC
│   │   │   │   ├── postgres/  160 LOC
│   │   │   │   ├── duckdb/  136 LOC
│   │   │   │   └── mysql/  74 LOC
│   │   │   ├── bin/  608 LOC
│   │   │   ├── format/  355 LOC
│   │   │   │   └── protobuf/  0 LOC
│   │   │   └── util/  125 LOC
│   │   └── ci/  0 LOC
│   ├── compute-types/  9,214 LOC
│   │   └── src/  9,214 LOC (2,302 own)
│   │       ├── plan/  5,198 LOC (3,516 own)
│   │       │   ├── interpret/  1,028 LOC
│   │       │   ├── join/  473 LOC
│   │       │   └── transform/  181 LOC
│   │       └── explain/  1,714 LOC
│   ├── storage-controller/  8,960 LOC
│   │   └── src/  8,960 LOC (8,668 own)
│   │       └── persist_handles/  292 LOC
│   ├── persist/  8,875 LOC (27 own)
│   │   └── src/  8,848 LOC (6,729 own)
│   │       └── indexed/  2,119 LOC (1,640 own)
│   │           └── columnar/  479 LOC
│   ├── storage-client/  7,176 LOC
│   │   └── src/  7,176 LOC (7,019 own)
│   │       ├── storage_collections/  103 LOC
│   │       └── util/  54 LOC
│   ├── txn-wal/  6,988 LOC (22 own)
│   │   └── src/  6,966 LOC
│   ├── cloud-resources/  6,470 LOC
│   │   └── src/  6,470 LOC (350 own)
│   │       ├── crd/  5,761 LOC (1,388 own)
│   │       │   └── generated/  4,373 LOC (11 own)
│   │       │       └── cert_manager/  4,362 LOC
│   │       └── bin/  359 LOC
│   ├── persist-types/  6,324 LOC (27 own)
│   │   ├── src/  6,297 LOC (4,462 own)
│   │   │   └── stats/  1,835 LOC
│   │   └── proptest-regressions/  0 LOC
│   ├── timely-util/  5,947 LOC
│   │   ├── src/  5,820 LOC (5,530 own)
│   │   │   ├── columnar/  228 LOC
│   │   │   └── containers/  62 LOC
│   │   └── benches/  127 LOC
│   ├── frontegg-mock/  5,281 LOC
│   │   ├── src/  2,711 LOC (638 own)
│   │   │   ├── handlers/  1,301 LOC
│   │   │   ├── models/  682 LOC
│   │   │   └── middleware/  90 LOC
│   │   └── tests/  2,570 LOC
│   ├── orchestratord/  5,052 LOC
│   │   ├── src/  5,052 LOC (526 own)
│   │   │   ├── controller/  3,977 LOC (2,070 own)
│   │   │   │   └── materialize/  1,907 LOC
│   │   │   └── bin/  549 LOC
│   │   └── ci/  0 LOC
│   ├── pgwire/  4,978 LOC
│   │   └── src/  4,978 LOC
│   ├── storage-operators/  4,954 LOC
│   │   └── src/  4,954 LOC (3,290 own)
│   │       ├── oneshot_source/  974 LOC
│   │       └── s3_oneshot_sink/  690 LOC
│   ├── sql-server-util/  4,916 LOC (22 own)
│   │   ├── src/  4,745 LOC
│   │   └── examples/  149 LOC
│   ├── sqllogictest/  4,254 LOC
│   │   ├── src/  4,122 LOC (3,698 own)
│   │   │   └── bin/  424 LOC
│   │   ├── tests/  132 LOC
│   │   └── ci/  0 LOC
│   ├── mz/  3,824 LOC
│   │   ├── src/  3,408 LOC (1,518 own)
│   │   │   ├── command/  979 LOC
│   │   │   └── bin/  911 LOC
│   │   │       └── mz/  911 LOC (428 own)
│   │   │           └── command/  483 LOC
│   │   └── tests/  416 LOC
│   ├── persist-cli/  3,647 LOC
│   │   ├── src/  3,647 LOC (996 own)
│   │   │   └── maelstrom/  2,651 LOC
│   │   ├── ci/  0 LOC
│   │   └── ci-base/  0 LOC
│   ├── interchange/  3,469 LOC (22 own)
│   │   ├── src/  2,933 LOC (1,403 own)
│   │   │   ├── avro/  1,468 LOC
│   │   │   └── bin/  62 LOC
│   │   ├── benches/  514 LOC
│   │   └── testdata/  0 LOC
│   ├── mz-debug/  3,166 LOC
│   │   ├── src/  3,166 LOC
│   │   └── ci/  0 LOC
│   ├── pgrepr/  2,963 LOC
│   │   └── src/  2,963 LOC (2,295 own)
│   │       └── value/  668 LOC
│   ├── balancerd/  2,728 LOC
│   │   ├── src/  2,344 LOC (2,010 own)
│   │   │   └── bin/  334 LOC
│   │   ├── tests/  384 LOC
│   │   └── ci/  0 LOC
│   ├── fivetran-destination/  2,691 LOC (53 own)
│   │   ├── src/  2,638 LOC (1,442 own)
│   │   │   ├── destination/  1,108 LOC
│   │   │   └── bin/  88 LOC
│   │   ├── ci/  0 LOC
│   │   └── proto/  0 LOC
│   ├── timestamp-oracle/  2,574 LOC
│   │   └── src/  2,574 LOC
│   ├── mysql-util/  2,323 LOC (22 own)
│   │   └── src/  2,301 LOC
│   ├── kafka-util/  2,278 LOC
│   │   ├── src/  2,278 LOC (1,504 own)
│   │   │   └── bin/  774 LOC
│   │   └── ci/  0 LOC
│   ├── service/  2,227 LOC
│   │   ├── src/  1,574 LOC (1,441 own)
│   │   │   └── transport/  133 LOC
│   │   └── tests/  653 LOC
│   ├── orchestrator-kubernetes/  2,199 LOC
│   │   └── src/  2,199 LOC
│   ├── ccsr/  2,137 LOC
│   │   ├── src/  1,643 LOC
│   │   └── tests/  494 LOC
│   ├── arrow-util/  1,980 LOC
│   │   └── src/  1,980 LOC
│   ├── expr-parser/  1,892 LOC
│   │   ├── src/  1,866 LOC
│   │   └── tests/  26 LOC
│   │       └── test_mir_parser/  0 LOC
│   ├── expr-derive-impl/  1,876 LOC
│   │   └── src/  1,876 LOC
│   │       └── snapshots/  0 LOC
│   ├── rocksdb/  1,788 LOC
│   │   ├── src/  1,367 LOC
│   │   └── tests/  421 LOC
│   ├── controller/  1,754 LOC
│   │   └── src/  1,754 LOC
│   ├── sql-pretty/  1,597 LOC
│   │   ├── src/  1,480 LOC
│   │   └── tests/  117 LOC
│   ├── frontegg-auth/  1,403 LOC
│   │   └── src/  1,403 LOC (1,207 own)
│   │       └── client/  196 LOC
│   ├── lsp-server/  1,392 LOC
│   │   ├── src/  783 LOC
│   │   └── tests/  609 LOC
│   ├── audit-log/  1,384 LOC
│   │   └── src/  1,384 LOC
│   ├── orchestrator-process/  1,312 LOC
│   │   └── src/  1,312 LOC
│   ├── postgres-util/  1,185 LOC (21 own)
│   │   └── src/  1,164 LOC
│   ├── pgcopy/  1,156 LOC
│   │   └── src/  1,156 LOC
│   ├── lowertest/  1,097 LOC
│   │   ├── src/  860 LOC
│   │   └── tests/  237 LOC
│   │       └── testdata/  0 LOC
│   ├── cluster/  1,080 LOC
│   │   └── src/  1,080 LOC
│   ├── ssh-util/  1,048 LOC
│   │   └── src/  1,048 LOC
│   ├── pgwire-common/  1,040 LOC
│   │   └── src/  1,040 LOC
│   ├── dyncfg/  1,006 LOC
│   │   └── src/  1,006 LOC
│   ├── walkabout/  958 LOC
│   │   ├── src/  927 LOC
│   │   └── tests/  31 LOC
│   │       └── testdata/  0 LOC
│   ├── expr-test-util/  904 LOC
│   │   ├── src/  771 LOC
│   │   └── tests/  133 LOC
│   │       └── testdata/  0 LOC
│   ├── proto/  858 LOC (25 own)
│   │   └── src/  833 LOC
│   ├── frontegg-client/  854 LOC
│   │   └── src/  854 LOC (562 own)
│   │       └── client/  292 LOC
│   ├── catalog-debug/  824 LOC
│   │   └── src/  824 LOC
│   ├── pgrepr-consts/  806 LOC
│   │   └── src/  806 LOC
│   ├── cloud-api/  798 LOC
│   │   └── src/  798 LOC (400 own)
│   │       └── client/  398 LOC
│   ├── server-core/  785 LOC
│   │   └── src/  785 LOC
│   ├── sql-lexer/  770 LOC (93 own)
│   │   ├── src/  635 LOC
│   │   └── tests/  42 LOC
│   │       └── testdata/  0 LOC
│   ├── pgtz/  738 LOC (155 own)
│   │   ├── src/  583 LOC
│   │   └── tznames/  0 LOC
│   ├── clusterd/  725 LOC (13 own)
│   │   ├── src/  712 LOC (700 own)
│   │   │   └── bin/  12 LOC
│   │   └── ci/  0 LOC
│   ├── pgtest/  692 LOC
│   │   └── src/  692 LOC
│   ├── aws-util/  683 LOC
│   │   └── src/  683 LOC
│   ├── authenticator/  657 LOC
│   │   └── src/  657 LOC
│   ├── metrics/  655 LOC
│   │   └── src/  655 LOC
│   ├── auth/  617 LOC
│   │   └── src/  617 LOC
│   ├── orchestrator-tracing/  552 LOC
│   │   └── src/  552 LOC
│   ├── prof/  541 LOC (23 own)
│   │   └── src/  518 LOC
│   ├── durable-cache/  539 LOC
│   │   └── src/  539 LOC
│   ├── orchestrator/  525 LOC
│   │   └── src/  525 LOC
│   ├── prof-http/  525 LOC (13 own)
│   │   ├── src/  512 LOC (481 own)
│   │   │   ├── bin/  31 LOC
│   │   │   └── http/  0 LOC
│   │   │       └── static/  0 LOC
│   │   │           ├── css/  0 LOC
│   │   │           └── js/  0 LOC
│   │   └── templates/  0 LOC
│   ├── secrets/  518 LOC
│   │   └── src/  518 LOC
│   ├── ore-proc/  512 LOC
│   │   ├── src/  493 LOC
│   │   └── tests/  19 LOC
│   ├── license-keys/  478 LOC
│   │   ├── src/  349 LOC
│   │   │   └── license_keys/  0 LOC
│   │   └── examples/  129 LOC
│   ├── adapter-types/  431 LOC
│   │   └── src/  431 LOC
│   ├── rocksdb-types/  414 LOC
│   │   └── src/  414 LOC
│   ├── postgres-client/  358 LOC
│   │   └── src/  358 LOC
│   ├── npm/  332 LOC
│   │   └── src/  332 LOC
│   ├── cluster-client/  328 LOC
│   │   └── src/  328 LOC
│   ├── http-util/  324 LOC
│   │   └── src/  324 LOC
│   ├── repr-test-util/  322 LOC
│   │   ├── src/  218 LOC
│   │   └── tests/  104 LOC
│   │       └── testdata/  0 LOC
│   ├── tracing/  291 LOC
│   │   └── src/  291 LOC
│   ├── metabase/  268 LOC
│   │   └── src/  268 LOC
│   ├── oidc-mock/  268 LOC
│   │   └── src/  268 LOC
│   ├── segment/  255 LOC
│   │   └── src/  255 LOC
│   ├── lowertest-derive/  245 LOC
│   │   └── src/  245 LOC
│   ├── dyncfg-file/  243 LOC
│   │   └── src/  243 LOC
│   ├── regexp/  241 LOC
│   │   └── src/  241 LOC
│   ├── dyncfg-launchdarkly/  238 LOC
│   │   └── src/  238 LOC
│   ├── aws-secrets-controller/  234 LOC
│   │   └── src/  234 LOC
│   ├── tls-util/  189 LOC
│   │   └── src/  189 LOC
│   ├── foundationdb/  174 LOC
│   │   └── src/  174 LOC
│   ├── build-info/  172 LOC (17 own)
│   │   └── src/  155 LOC
│   ├── s3-datagen/  168 LOC
│   │   └── src/  168 LOC
│   ├── ore-build/  148 LOC
│   │   └── src/  148 LOC
│   ├── persist-proc/  130 LOC
│   │   └── src/  130 LOC
│   ├── controller-types/  115 LOC
│   │   └── src/  115 LOC
│   ├── cloud-provider/  85 LOC
│   │   └── src/  85 LOC
│   ├── expr-derive/  69 LOC
│   │   └── src/  69 LOC
│   ├── build-tools/  51 LOC
│   │   └── src/  51 LOC
│   ├── dyncfgs/  37 LOC
│   │   └── src/  37 LOC
│   ├── alloc/  35 LOC
│   │   └── src/  35 LOC
│   ├── materialized/  33 LOC (13 own)
│   │   ├── src/  20 LOC
│   │   │   └── bin/  20 LOC
│   │   └── ci/  0 LOC
│   │       └── listener_configs/  0 LOC
│   ├── alloc-default/  10 LOC
│   │   └── src/  10 LOC
│   └── workspace-hack/  0 LOC
├── misc/  102,367 LOC
│   ├── python/  94,690 LOC
│   │   ├── materialize/  94,690 LOC (7,487 own)
│   │   │   ├── checks/  15,819 LOC (2,769 own)
│   │   │   │   └── all_checks/  13,050 LOC
│   │   │   ├── output_consistency/  15,248 LOC (394 own)
│   │   │   │   ├── input_data/  6,916 LOC (202 own)
│   │   │   │   │   ├── operations/  3,214 LOC
│   │   │   │   │   ├── params/  1,126 LOC
│   │   │   │   │   ├── types/  1,082 LOC
│   │   │   │   │   ├── values/  671 LOC
│   │   │   │   │   ├── return_specs/  394 LOC
│   │   │   │   │   ├── validators/  106 LOC
│   │   │   │   │   ├── special/  80 LOC
│   │   │   │   │   ├── scenarios/  24 LOC
│   │   │   │   │   └── constants/  17 LOC
│   │   │   │   ├── generators/  1,229 LOC
│   │   │   │   ├── execution/  1,080 LOC
│   │   │   │   ├── validation/  927 LOC
│   │   │   │   ├── ignore_filter/  898 LOC
│   │   │   │   ├── expression/  691 LOC
│   │   │   │   ├── query/  643 LOC
│   │   │   │   ├── operation/  521 LOC
│   │   │   │   ├── status/  385 LOC
│   │   │   │   ├── output/  379 LOC
│   │   │   │   ├── runner/  277 LOC
│   │   │   │   ├── data_value/  251 LOC
│   │   │   │   ├── selection/  213 LOC
│   │   │   │   ├── enum/  182 LOC
│   │   │   │   ├── data_type/  154 LOC
│   │   │   │   ├── common/  80 LOC
│   │   │   │   └── debug/  28 LOC
│   │   │   ├── cli/  8,452 LOC (7,628 own)
│   │   │   │   └── scratch/  824 LOC
│   │   │   ├── mzcompose/  6,650 LOC (3,049 own)
│   │   │   │   ├── services/  3,373 LOC
│   │   │   │   └── helpers/  228 LOC
│   │   │   ├── parallel_workload/  5,995 LOC
│   │   │   ├── feature_benchmark/  4,681 LOC (1,536 own)
│   │   │   │   └── scenarios/  3,145 LOC
│   │   │   ├── zippy/  4,443 LOC
│   │   │   ├── workload_replay/  4,274 LOC
│   │   │   ├── scalability/  3,062 LOC (14 own)
│   │   │   │   ├── result/  552 LOC
│   │   │   │   ├── operation/  510 LOC (238 own)
│   │   │   │   │   └── operations/  272 LOC
│   │   │   │   ├── executor/  489 LOC
│   │   │   │   ├── workload/  400 LOC (128 own)
│   │   │   │   │   └── workloads/  272 LOC
│   │   │   │   ├── endpoint/  329 LOC
│   │   │   │   ├── df/  311 LOC
│   │   │   │   ├── plot/  294 LOC
│   │   │   │   ├── schema/  81 LOC
│   │   │   │   ├── io/  57 LOC
│   │   │   │   └── config/  25 LOC
│   │   │   ├── buildkite_insights/  3,030 LOC
│   │   │   │   ├── annotation_search/  526 LOC
│   │   │   │   ├── artifact_search/  477 LOC
│   │   │   │   ├── util/  450 LOC
│   │   │   │   ├── cache/  444 LOC
│   │   │   │   ├── buildkite_api/  380 LOC
│   │   │   │   ├── costs/  329 LOC
│   │   │   │   ├── step_analysis/  244 LOC
│   │   │   │   ├── data/  140 LOC
│   │   │   │   └── segfaults/  40 LOC
│   │   │   ├── cloudtest/  2,852 LOC (12 own)
│   │   │   │   ├── k8s/  2,114 LOC (1,575 own)
│   │   │   │   │   └── api/  539 LOC
│   │   │   │   ├── app/  364 LOC
│   │   │   │   └── util/  362 LOC
│   │   │   ├── data_ingest/  2,692 LOC
│   │   │   ├── test_analytics/  2,176 LOC (133 own)
│   │   │   │   ├── data/  1,178 LOC (52 own)
│   │   │   │   │   ├── build/  284 LOC
│   │   │   │   │   ├── cluster_spec_sheet/  135 LOC
│   │   │   │   │   ├── feature_benchmark/  131 LOC
│   │   │   │   │   ├── parallel_benchmark/  107 LOC
│   │   │   │   │   ├── bounded_memory/  91 LOC
│   │   │   │   │   ├── build_annotation/  83 LOC
│   │   │   │   │   ├── output_consistency/  70 LOC
│   │   │   │   │   ├── scalability_framework/  63 LOC
│   │   │   │   │   ├── product_limits/  61 LOC
│   │   │   │   │   ├── upgrade_downtime/  58 LOC
│   │   │   │   │   └── known_issues/  43 LOC
│   │   │   │   ├── connector/  346 LOC
│   │   │   │   ├── search/  343 LOC
│   │   │   │   ├── config/  113 LOC
│   │   │   │   ├── setup/  43 LOC
│   │   │   │   │   ├── cleanup/  0 LOC
│   │   │   │   │   ├── tables/  0 LOC
│   │   │   │   │   └── views/  0 LOC
│   │   │   │   └── util/  20 LOC
│   │   │   ├── postgres_consistency/  2,025 LOC (161 own)
│   │   │   │   ├── ignore_filter/  1,331 LOC
│   │   │   │   ├── validation/  216 LOC
│   │   │   │   ├── custom/  210 LOC
│   │   │   │   └── execution/  107 LOC
│   │   │   ├── parallel_benchmark/  1,702 LOC
│   │   │   ├── mzexplore/  1,461 LOC
│   │   │   │   ├── catalog/  0 LOC
│   │   │   │   └── sql/  0 LOC
│   │   │   ├── version_consistency/  654 LOC (255 own)
│   │   │   │   ├── ignore_filter/  383 LOC
│   │   │   │   └── execution/  16 LOC
│   │   │   ├── ci_util/  529 LOC
│   │   │   ├── feature_flag_consistency/  510 LOC (218 own)
│   │   │   │   ├── ignore_filter/  112 LOC
│   │   │   │   ├── input_data/  79 LOC
│   │   │   │   ├── feature_flag/  66 LOC
│   │   │   │   └── execution/  35 LOC
│   │   │   ├── benches/  290 LOC
│   │   │   ├── optbench/  263 LOC
│   │   │   │   ├── schema/  0 LOC
│   │   │   │   └── workload/  0 LOC
│   │   │   ├── release/  252 LOC
│   │   │   └── query_fitness/  143 LOC
│   │   └── stubs/  0 LOC
│   │       ├── confluent_kafka/  0 LOC
│   │       │   └── schema_registry/  0 LOC
│   │       ├── docker/  0 LOC
│   │       │   └── models/  0 LOC
│   │       └── pg8000/  0 LOC
│   │           └── native/  0 LOC
│   ├── dbt-materialize/  5,631 LOC (201 own)
│   │   ├── tests/  4,662 LOC (34 own)
│   │   │   └── adapter/  4,628 LOC
│   │   └── dbt/  768 LOC (15 own)
│   │       ├── adapters/  719 LOC (15 own)
│   │       │   └── materialize/  704 LOC
│   │       └── include/  34 LOC (15 own)
│   │           ├── materialize/  19 LOC
│   │           │   └── macros/  0 LOC
│   │           │       ├── ci/  0 LOC
│   │           │       ├── deploy/  0 LOC
│   │           │       ├── materializations/  0 LOC
│   │           │       │   └── seed/  0 LOC
│   │           │       ├── tests/  0 LOC
│   │           │       └── utils/  0 LOC
│   │           └── starter_project/  0 LOC
│   │               └── models/  0 LOC
│   │                   └── example/  0 LOC
│   │                       └── sources/  0 LOC
│   ├── mcp-materialize/  948 LOC (83 own)
│   │   ├── mcp_materialize/  613 LOC
│   │   │   └── sql/  0 LOC
│   │   └── tests/  252 LOC
│   │       └── integration/  252 LOC
│   ├── wasm/  462 LOC
│   │   └── src/  462 LOC
│   │       ├── sql-parser-wasm/  344 LOC
│   │       │   └── src/  344 LOC
│   │       ├── sql-lexer-wasm/  73 LOC
│   │       │   └── src/  73 LOC
│   │       └── sql-pretty-wasm/  45 LOC
│   │           └── src/  45 LOC
│   ├── mcp-materialize-agents/  401 LOC
│   │   └── mcp_materialize_agents/  401 LOC
│   │       └── mcp_materialize_agents/  401 LOC
│   ├── lint/  163 LOC
│   ├── monitoring/  72 LOC
│   │   └── grafana/  0 LOC
│   │       └── datasources/  0 LOC
│   ├── alloydb/  0 LOC
│   ├── buildkite/  0 LOC
│   ├── cockroach/  0 LOC
│   ├── completions/  0 LOC
│   │   ├── bash/  0 LOC
│   │   └── zsh/  0 LOC
│   ├── dist/  0 LOC
│   │   └── deb-scripts/  0 LOC
│   ├── doc/  0 LOC
│   ├── editor/  0 LOC
│   │   ├── dap/  0 LOC
│   │   ├── emacs/  0 LOC
│   │   ├── rustrover/  0 LOC
│   │   └── vscode/  0 LOC
│   │       └── syntaxes/  0 LOC
│   ├── githooks/  0 LOC
│   ├── helm-charts/  0 LOC
│   │   ├── operator/  0 LOC
│   │   │   ├── templates/  0 LOC
│   │   │   └── tests/  0 LOC
│   │   └── testing/  0 LOC
│   ├── images/  0 LOC
│   │   ├── cli/  0 LOC
│   │   ├── debezium/  0 LOC
│   │   ├── distroless-prod-base/  0 LOC
│   │   ├── fivetran-destination-tester/  0 LOC
│   │   ├── frontegg-mock/  0 LOC
│   │   ├── jobs/  0 LOC
│   │   ├── materialized-base/  0 LOC
│   │   ├── mysql-client/  0 LOC
│   │   ├── mz/  0 LOC
│   │   ├── prod-base/  0 LOC
│   │   ├── psql/  0 LOC
│   │   ├── sshd/  0 LOC
│   │   └── ubuntu-base/  0 LOC
│   ├── kind/  0 LOC
│   │   └── configmaps/  0 LOC
│   ├── mzcompose/  0 LOC
│   │   ├── grafana/  0 LOC
│   │   │   └── datasources/  0 LOC
│   │   └── prometheus/  0 LOC
│   ├── mzexplore/  0 LOC
│   ├── nix/  0 LOC
│   ├── perf/  0 LOC
│   ├── postgres/  0 LOC
│   ├── sanshim/  0 LOC
│   ├── scratch/  0 LOC
│   ├── shlib/  0 LOC
│   ├── tb/  0 LOC
│   └── www/  0 LOC
│       └── apt/  0 LOC
├── console/  54,202 LOC (447 own)
│   ├── src/  48,245 LOC (773 own)
│   │   ├── api/  23,291 LOC (1,330 own)
│   │   │   ├── materialize/  18,721 LOC (5,507 own)
│   │   │   │   ├── cluster/  3,208 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── source/  2,271 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── roles/  2,182 LOC
│   │   │   │   ├── query-history/  1,661 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── connection/  1,400 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── object-explorer/  954 LOC
│   │   │   │   ├── sink/  569 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── freshness/  281 LOC
│   │   │   │   ├── secret/  264 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── notice/  149 LOC
│   │   │   │   ├── privilege-table/  149 LOC
│   │   │   │   │   └── __snapshots__/  0 LOC
│   │   │   │   ├── environment-overview/  84 LOC
│   │   │   │   └── license/  42 LOC
│   │   │   ├── mocks/  1,470 LOC
│   │   │   ├── schemas/  1,420 LOC
│   │   │   ├── frontegg/  252 LOC
│   │   │   └── incident-io/  98 LOC
│   │   ├── platform/  12,630 LOC (166 own)
│   │   │   ├── shell/  3,547 LOC (1,656 own)
│   │   │   │   ├── store/  925 LOC
│   │   │   │   ├── machines/  723 LOC
│   │   │   │   └── plan-insights/  243 LOC
│   │   │   ├── roles/  1,621 LOC (443 own)
│   │   │   │   ├── create/  835 LOC
│   │   │   │   └── edit/  343 LOC
│   │   │   ├── clusters/  1,226 LOC (1,185 own)
│   │   │   │   └── ClusterOverview/  41 LOC
│   │   │   ├── connectors/  1,090 LOC
│   │   │   ├── query-history/  998 LOC
│   │   │   ├── object-explorer/  997 LOC
│   │   │   ├── sources/  638 LOC (362 own)
│   │   │   │   ├── create/  213 LOC
│   │   │   │   │   ├── shared/  133 LOC
│   │   │   │   │   ├── kafka/  80 LOC
│   │   │   │   │   ├── mysql/  0 LOC
│   │   │   │   │   ├── postgres/  0 LOC
│   │   │   │   │   ├── sqlserver/  0 LOC
│   │   │   │   │   └── webhook/  0 LOC
│   │   │   │   └── SourceOverview/  63 LOC
│   │   │   ├── billing/  629 LOC
│   │   │   ├── environment-overview/  616 LOC
│   │   │   ├── integrations/  508 LOC
│   │   │   ├── sinks/  290 LOC (262 own)
│   │   │   │   └── SinkOverview/  28 LOC
│   │   │   ├── environment-not-ready/  97 LOC
│   │   │   ├── secrets/  68 LOC
│   │   │   ├── internal/  66 LOC
│   │   │   │   └── notices/  66 LOC
│   │   │   ├── connections/  37 LOC
│   │   │   └── auth/  36 LOC
│   │   ├── theme/  2,000 LOC (753 own)
│   │   │   └── components/  1,247 LOC
│   │   ├── store/  1,652 LOC
│   │   ├── components/  1,568 LOC
│   │   │   ├── CommandBlock/  411 LOC
│   │   │   ├── Graph/  404 LOC
│   │   │   ├── IncidentStatusWidget/  291 LOC
│   │   │   ├── SchemaObjectFilter/  129 LOC
│   │   │   ├── WorkflowGraph/  119 LOC
│   │   │   ├── SearchableSelect/  83 LOC
│   │   │   ├── DatePicker/  44 LOC
│   │   │   ├── FreshnessGraph/  41 LOC
│   │   │   ├── WelcomeDialog/  24 LOC
│   │   │   ├── EventEmitter/  11 LOC
│   │   │   ├── LaunchDarkly/  11 LOC
│   │   │   └── Dropdown/  0 LOC
│   │   ├── hooks/  1,489 LOC
│   │   ├── utils/  1,285 LOC
│   │   ├── config/  1,156 LOC
│   │   ├── analytics/  835 LOC
│   │   ├── test/  495 LOC (203 own)
│   │   │   └── sql/  292 LOC
│   │   ├── queries/  411 LOC
│   │   ├── external-library-wrappers/  286 LOC (167 own)
│   │   │   └── __mocks__/  119 LOC
│   │   ├── version/  123 LOC
│   │   ├── __mocks__/  99 LOC
│   │   ├── layouts/  88 LOC (26 own)
│   │   │   ├── NavBar/  51 LOC
│   │   │   └── JotaiProviderWrapper/  11 LOC
│   │   ├── access/  46 LOC
│   │   │   └── license/  46 LOC
│   │   ├── forms/  18 LOC
│   │   ├── icons/  0 LOC
│   │   └── svg/  0 LOC
│   │       └── nav/  0 LOC
│   ├── types/  4,614 LOC
│   ├── e2e-tests/  786 LOC
│   ├── sitemap-to-json/  76 LOC
│   ├── __mocks__/  34 LOC (18 own)
│   │   └── @materializeinc/  16 LOC
│   ├── bin/  0 LOC
│   ├── doc/  0 LOC
│   │   └── design/  0 LOC
│   │       └── 20241004_freshness/  0 LOC
│   ├── font/  0 LOC
│   ├── icon/  0 LOC
│   ├── img/  0 LOC
│   │   └── integrations/  0 LOC
│   ├── misc/  0 LOC
│   │   ├── dnsmasq/  0 LOC
│   │   ├── docker/  0 LOC
│   │   └── githooks/  0 LOC
│   └── public/  0 LOC
│       └── app-config/  0 LOC
├── test/  53,017 LOC
│   ├── cluster/  6,703 LOC
│   │   ├── blue-green-deployment/  0 LOC
│   │   ├── cluster-drop-concurrent/  0 LOC
│   │   ├── github-7645/  0 LOC
│   │   ├── github-cloud-7998/  0 LOC
│   │   ├── pg-snapshot-resumption/  0 LOC
│   │   ├── query-without-default-cluster/  0 LOC
│   │   ├── resources/  0 LOC
│   │   ├── sink-failure/  0 LOC
│   │   ├── statement-logging/  0 LOC
│   │   ├── storage/  0 LOC
│   │   └── upsert/  0 LOC
│   ├── cluster-spec-sheet/  3,333 LOC
│   ├── cloudtest/  3,159 LOC (2,726 own)
│   │   └── node_recovery/  433 LOC
│   ├── orchestratord/  3,136 LOC
│   ├── 0dt/  2,784 LOC
│   ├── limits/  2,389 LOC
│   ├── bounded-memory/  1,763 LOC
│   ├── terraform/  1,477 LOC
│   │   ├── aws-persistent/  0 LOC
│   │   ├── aws-temporary/  0 LOC
│   │   ├── aws-upgrade/  0 LOC
│   │   ├── azure-temporary/  0 LOC
│   │   └── gcp-temporary/  0 LOC
│   ├── race-condition/  1,069 LOC
│   ├── restart/  988 LOC
│   ├── parallel-benchmark/  916 LOC
│   ├── feature-benchmark/  907 LOC
│   ├── balancerd/  814 LOC
│   ├── sqllogictest/  773 LOC
│   │   ├── advent-of-code/  0 LOC
│   │   │   └── 2023/  0 LOC
│   │   ├── attributes/  0 LOC
│   │   ├── autogenerated/  0 LOC
│   │   ├── cockroach/  0 LOC
│   │   ├── explain/  0 LOC
│   │   ├── introspection/  0 LOC
│   │   ├── postgres/  0 LOC
│   │   │   ├── pgcrypto/  0 LOC
│   │   │   └── testdata/  0 LOC
│   │   ├── special/  0 LOC
│   │   └── transform/  0 LOC
│   │       ├── fold_vs_dataflow/  0 LOC
│   │       └── notice/  0 LOC
│   ├── upsert/  760 LOC
│   │   ├── failpoint/  0 LOC
│   │   ├── incident-49/  0 LOC
│   │   ├── load-test/  0 LOC
│   │   ├── rehydration/  0 LOC
│   │   └── rocksdb-cleanup/  0 LOC
│   ├── ssh-connection/  746 LOC
│   ├── scalability/  715 LOC
│   │   └── results/  0 LOC
│   ├── mysql-cdc-resumption/  705 LOC
│   ├── mysql-cdc-resumption-old-syntax/  705 LOC
│   ├── dataflow-visualizer/  684 LOC (75 own)
│   │   └── tests/  609 LOC
│   ├── lang/  615 LOC
│   │   ├── python/  326 LOC
│   │   ├── js/  181 LOC
│   │   ├── java/  37 LOC
│   │   │   └── smoketest/  0 LOC
│   │   ├── ruby/  37 LOC
│   │   └── csharp/  34 LOC
│   ├── pg-cdc-old-syntax/  601 LOC
│   │   ├── override/  0 LOC
│   │   └── status/  0 LOC
│   ├── canary-load/  565 LOC
│   ├── source-sink-errors/  563 LOC
│   ├── aws/  557 LOC
│   ├── replica-isolation/  532 LOC
│   ├── retain-history/  521 LOC
│   ├── copy/  472 LOC
│   │   ├── http/  0 LOC
│   │   └── nightly/  0 LOC
│   ├── testdrive-old-kafka-src-syntax/  471 LOC
│   │   └── disabled/  0 LOC
│   ├── pg-cdc/  458 LOC
│   │   ├── override/  0 LOC
│   │   └── status/  0 LOC
│   ├── cloud-canary/  431 LOC
│   ├── canary-environment/  410 LOC
│   │   ├── macros/  0 LOC
│   │   ├── models/  0 LOC
│   │   │   ├── loadgen/  0 LOC
│   │   │   ├── mysql_cdc/  0 LOC
│   │   │   ├── pg_cdc/  0 LOC
│   │   │   ├── table/  0 LOC
│   │   │   └── tpch/  0 LOC
│   │   └── tests/  0 LOC
│   │       └── generic/  0 LOC
│   ├── pg-cdc-resumption/  409 LOC
│   ├── mcp/  382 LOC
│   ├── mysql-cdc-old-syntax/  372 LOC
│   │   ├── override/  0 LOC
│   │   └── schema-restart/  0 LOC
│   ├── mysql-cdc/  370 LOC
│   │   ├── override/  0 LOC
│   │   ├── proxied/  0 LOC
│   │   └── schema-restart/  0 LOC
│   ├── sql-server-cdc/  350 LOC
│   │   └── setup/  0 LOC
│   ├── kafka-auth/  344 LOC
│   ├── legacy-upgrade/  343 LOC
│   ├── platform-checks/  338 LOC
│   ├── rqg/  338 LOC
│   │   ├── datasets/  0 LOC
│   │   └── grammars/  0 LOC
│   ├── sql-server-resumption-old-syntax/  338 LOC
│   │   └── setup/  0 LOC
│   ├── launchdarkly/  336 LOC
│   ├── cluster-isolation/  328 LOC
│   ├── workload-replay/  311 LOC
│   ├── pg-cdc-resumption-old-syntax/  308 LOC
│   ├── aws-localstack/  304 LOC
│   │   ├── aws-connection/  0 LOC
│   │   └── copy-to-s3/  0 LOC
│   ├── sql-feature-flags/  288 LOC
│   ├── zippy/  286 LOC
│   ├── test-util/  279 LOC
│   │   └── src/  279 LOC (163 own)
│   │       ├── kafka/  97 LOC
│   │       └── generator/  19 LOC
│   ├── txn-wal-fencing/  276 LOC
│   ├── mz-e2e/  268 LOC
│   ├── testdrive/  266 LOC
│   │   └── disabled/  0 LOC
│   ├── iceberg/  264 LOC
│   ├── storage-usage/  261 LOC
│   ├── parallel-workload/  243 LOC
│   ├── kafka-rtr/  240 LOC
│   │   ├── resumption/  0 LOC
│   │   └── simple/  0 LOC
│   ├── doc-examples/  239 LOC
│   ├── sqlsmith/  233 LOC
│   ├── persistence/  228 LOC
│   │   ├── compaction/  0 LOC
│   │   ├── failpoints/  0 LOC
│   │   ├── kafka-sources/  0 LOC
│   │   └── user-tables/  0 LOC
│   ├── metabase/  216 LOC (33 own)
│   │   └── smoketest/  183 LOC
│   │       ├── src/  183 LOC
│   │       │   └── bin/  183 LOC
│   │       └── ci/  0 LOC
│   ├── console/  212 LOC
│   ├── fivetran-destination/  208 LOC
│   │   ├── data/  0 LOC
│   │   ├── test-deletes-no-primary/  0 LOC
│   │   ├── test-describe/  0 LOC
│   │   ├── test-describe-fivetran-id/  0 LOC
│   │   ├── test-multi-table/  0 LOC
│   │   ├── test-no-database/  0 LOC
│   │   ├── test-no-permissions/  0 LOC
│   │   ├── test-odd-writes/  0 LOC
│   │   ├── test-supported-data-types/  0 LOC
│   │   ├── test-weird-names/  0 LOC
│   │   └── test-writes/  0 LOC
│   ├── data-ingest/  206 LOC
│   ├── tracing/  206 LOC
│   ├── crdb-restarts/  197 LOC
│   ├── pubsub-disruption/  186 LOC
│   ├── kafka-resumption/  173 LOC
│   │   ├── sink-kafka-restart/  0 LOC
│   │   ├── sink-networking/  0 LOC
│   │   ├── sink-queue-full/  0 LOC
│   │   └── source-resumption/  0 LOC
│   ├── feature-flag-consistency/  155 LOC
│   ├── version-consistency/  148 LOC
│   ├── chbench/  133 LOC
│   │   └── chbench/  0 LOC
│   │       ├── bin/  0 LOC
│   │       ├── cmake/  0 LOC
│   │       └── src/  0 LOC
│   │           └── dialect/  0 LOC
│   ├── persist/  114 LOC
│   ├── kafka-matrix/  106 LOC
│   ├── rtr-combined/  96 LOC
│   │   └── rtr/  0 LOC
│   ├── foundationdb/  92 LOC
│   │   └── image/  0 LOC
│   ├── secrets-logging/  90 LOC
│   ├── secrets-local-file/  87 LOC
│   ├── backup-restore/  85 LOC
│   ├── mzcompose_examples/  85 LOC
│   ├── dyncfg/  84 LOC
│   ├── sql-server-cdc-old-syntax/  84 LOC
│   │   └── setup/  0 LOC
│   ├── backup-restore-postgres/  83 LOC
│   ├── debezium/  74 LOC
│   │   ├── mysql/  0 LOC
│   │   ├── postgres/  0 LOC
│   │   └── sql-server/  0 LOC
│   ├── postgres-consistency/  63 LOC
│   ├── sqlancer/  61 LOC
│   ├── sqlancerplusplus/  58 LOC
│   ├── kafka-exactly-once/  54 LOC
│   ├── kafka-multi-broker/  53 LOC
│   ├── output-consistency/  53 LOC
│   ├── mz-debug/  50 LOC
│   ├── mysql-rtr/  48 LOC
│   │   └── rtr/  0 LOC
│   ├── mysql-rtr-old-syntax/  48 LOC
│   │   └── rtr/  0 LOC
│   ├── pg-rtr/  45 LOC
│   │   └── rtr/  0 LOC
│   ├── pg-rtr-old-syntax/  45 LOC
│   │   └── rtr/  0 LOC
│   ├── get-cloud-hostname/  29 LOC
│   ├── emulator/  26 LOC
│   ├── azurite/  0 LOC
│   ├── dbbench/  0 LOC
│   ├── dnsmasq/  0 LOC
│   ├── ldbc-bi/  0 LOC
│   ├── minio/  0 LOC
│   ├── mssql-server/  0 LOC
│   ├── mysql/  0 LOC
│   ├── pgtest/  0 LOC
│   ├── pgtest-mz/  0 LOC
│   ├── playwright/  0 LOC
│   ├── postgres/  0 LOC
│   ├── ssh-bastion-host/  0 LOC
│   └── test-certs/  0 LOC
├── ci/  3,309 LOC (1,188 own)
│   ├── test/  945 LOC (370 own)
│   │   ├── cargo-test/  460 LOC
│   │   ├── lint-buf/  115 LOC
│   │   ├── console/  0 LOC
│   │   ├── docs-widgets/  0 LOC
│   │   ├── lint-deps/  0 LOC
│   │   └── lint-main/  0 LOC
│   │       ├── after/  0 LOC
│   │       ├── before/  0 LOC
│   │       └── checks/  0 LOC
│   ├── deploy/  448 LOC
│   ├── deploy_mz/  234 LOC
│   ├── cleanup/  208 LOC
│   ├── deploy_mz_lsp_server/  141 LOC
│   ├── deploy_mz-debug/  104 LOC
│   ├── load/  41 LOC
│   ├── builder/  0 LOC
│   │   └── sanshim/  0 LOC
│   │       ├── aarch64/  0 LOC
│   │       └── x86_64/  0 LOC
│   ├── deploy_website/  0 LOC
│   ├── license/  0 LOC
│   ├── nightly/  0 LOC
│   ├── plugins/  0 LOC
│   │   ├── cloudtest/  0 LOC
│   │   │   └── hooks/  0 LOC
│   │   ├── mzcompose/  0 LOC
│   │   │   └── hooks/  0 LOC
│   │   └── scratch-aws-access/  0 LOC
│   │       └── hooks/  0 LOC
│   ├── publish-helm-charts/  0 LOC
│   ├── qa-canary/  0 LOC
│   └── release-qualification/  0 LOC
├── .claude/  141 LOC
│   ├── skills/  141 LOC
│   │   ├── mz-query-tracing/  141 LOC
│   │   ├── mz-adapter-guide/  0 LOC
│   │   ├── mz-benchmark/  0 LOC
│   │   ├── mz-commit/  0 LOC
│   │   ├── mz-debug-ci/  0 LOC
│   │   ├── mz-limits-test/  0 LOC
│   │   ├── mz-parallel-workload/  0 LOC
│   │   ├── mz-platform-checks/  0 LOC
│   │   ├── mz-pr-review/  0 LOC
│   │   ├── mz-profile/  0 LOC
│   │   ├── mz-run/  0 LOC
│   │   └── mz-test/  0 LOC
│   └── commands/  0 LOC
├── .config/  0 LOC
├── .github/  0 LOC
│   └── workflows/  0 LOC
├── bin/  0 LOC
└── doc/  0 LOC
    ├── developer/  0 LOC
    │   ├── assets/  0 LOC
    │   ├── design/  0 LOC
    │   │   └── static/  0 LOC
    │   │       ├── 20231113_optimizer_notice_catalog/  0 LOC
    │   │       ├── 20240205_cluster_specific_optimization/  0 LOC
    │   │       ├── a_small_coordinator/  0 LOC
    │   │       ├── catalog_migration_to_persist/  0 LOC
    │   │       ├── distributed_ts_oracle/  0 LOC
    │   │       ├── occ_read_then_write/  0 LOC
    │   │       ├── qgm/  0 LOC
    │   │       └── source_metrics_2/  0 LOC
    │   ├── generated/  0 LOC
    │   │   ├── adapter/  0 LOC
    │   │   │   ├── catalog/  0 LOC
    │   │   │   │   ├── builtin_table_updates/  0 LOC
    │   │   │   │   └── open/  0 LOC
    │   │   │   ├── config/  0 LOC
    │   │   │   ├── coord/  0 LOC
    │   │   │   │   ├── catalog_implications/  0 LOC
    │   │   │   │   └── sequencer/  0 LOC
    │   │   │   │       └── inner/  0 LOC
    │   │   │   ├── explain/  0 LOC
    │   │   │   └── optimize/  0 LOC
    │   │   ├── adapter-types/  0 LOC
    │   │   ├── alloc/  0 LOC
    │   │   ├── alloc-default/  0 LOC
    │   │   ├── arrow-util/  0 LOC
    │   │   ├── audit-log/  0 LOC
    │   │   ├── auth/  0 LOC
    │   │   ├── authenticator/  0 LOC
    │   │   ├── avro/  0 LOC
    │   │   ├── aws-secrets-controller/  0 LOC
    │   │   ├── aws-util/  0 LOC
    │   │   ├── balancerd/  0 LOC
    │   │   ├── build-info/  0 LOC
    │   │   ├── build-tools/  0 LOC
    │   │   ├── catalog/  0 LOC
    │   │   │   ├── builtin/  0 LOC
    │   │   │   ├── durable/  0 LOC
    │   │   │   │   ├── objects/  0 LOC
    │   │   │   │   └── upgrade/  0 LOC
    │   │   │   └── memory/  0 LOC
    │   │   ├── catalog-debug/  0 LOC
    │   │   ├── catalog-protos/  0 LOC
    │   │   ├── ccsr/  0 LOC
    │   │   ├── cloud-api/  0 LOC
    │   │   ├── cloud-provider/  0 LOC
    │   │   ├── cloud-resources/  0 LOC
    │   │   │   ├── bin/  0 LOC
    │   │   │   └── crd/  0 LOC
    │   │   │       └── generated/  0 LOC
    │   │   │           └── cert_manager/  0 LOC
    │   │   ├── cluster/  0 LOC
    │   │   ├── cluster-client/  0 LOC
    │   │   ├── clusterd/  0 LOC
    │   │   ├── compute/  0 LOC
    │   │   │   ├── arrangement/  0 LOC
    │   │   │   ├── compute_state/  0 LOC
    │   │   │   ├── extensions/  0 LOC
    │   │   │   ├── logging/  0 LOC
    │   │   │   ├── render/  0 LOC
    │   │   │   │   └── join/  0 LOC
    │   │   │   └── sink/  0 LOC
    │   │   ├── compute-client/  0 LOC
    │   │   │   ├── controller/  0 LOC
    │   │   │   └── protocol/  0 LOC
    │   │   ├── compute-types/  0 LOC
    │   │   │   ├── explain/  0 LOC
    │   │   │   └── plan/  0 LOC
    │   │   │       ├── interpret/  0 LOC
    │   │   │       ├── join/  0 LOC
    │   │   │       └── transform/  0 LOC
    │   │   ├── controller/  0 LOC
    │   │   ├── controller-types/  0 LOC
    │   │   ├── durable-cache/  0 LOC
    │   │   ├── dyncfg/  0 LOC
    │   │   ├── dyncfg-file/  0 LOC
    │   │   ├── dyncfg-launchdarkly/  0 LOC
    │   │   ├── dyncfgs/  0 LOC
    │   │   ├── environmentd/  0 LOC
    │   │   │   ├── deployment/  0 LOC
    │   │   │   ├── environmentd/  0 LOC
    │   │   │   └── http/  0 LOC
    │   │   ├── expr/  0 LOC
    │   │   │   ├── explain/  0 LOC
    │   │   │   ├── relation/  0 LOC
    │   │   │   ├── row/  0 LOC
    │   │   │   └── scalar/  0 LOC
    │   │   │       └── func/  0 LOC
    │   │   │           └── impls/  0 LOC
    │   │   ├── expr-derive/  0 LOC
    │   │   ├── expr-derive-impl/  0 LOC
    │   │   ├── expr-parser/  0 LOC
    │   │   ├── expr-test-util/  0 LOC
    │   │   ├── fivetran-destination/  0 LOC
    │   │   │   └── destination/  0 LOC
    │   │   ├── foundationdb/  0 LOC
    │   │   ├── frontegg-auth/  0 LOC
    │   │   │   └── client/  0 LOC
    │   │   ├── frontegg-client/  0 LOC
    │   │   │   └── client/  0 LOC
    │   │   ├── frontegg-mock/  0 LOC
    │   │   │   ├── handlers/  0 LOC
    │   │   │   ├── middleware/  0 LOC
    │   │   │   └── models/  0 LOC
    │   │   ├── http-util/  0 LOC
    │   │   ├── interchange/  0 LOC
    │   │   │   ├── avro/  0 LOC
    │   │   │   └── bin/  0 LOC
    │   │   ├── kafka-util/  0 LOC
    │   │   ├── license-keys/  0 LOC
    │   │   ├── lowertest/  0 LOC
    │   │   ├── lowertest-derive/  0 LOC
    │   │   ├── lsp-server/  0 LOC
    │   │   ├── materialized/  0 LOC
    │   │   ├── metabase/  0 LOC
    │   │   ├── metrics/  0 LOC
    │   │   ├── mysql-util/  0 LOC
    │   │   ├── mz/  0 LOC
    │   │   │   ├── bin/  0 LOC
    │   │   │   │   └── mz/  0 LOC
    │   │   │   │       └── command/  0 LOC
    │   │   │   └── command/  0 LOC
    │   │   ├── mz-debug/  0 LOC
    │   │   ├── npm/  0 LOC
    │   │   ├── oidc-mock/  0 LOC
    │   │   ├── orchestrator/  0 LOC
    │   │   ├── orchestrator-kubernetes/  0 LOC
    │   │   ├── orchestrator-process/  0 LOC
    │   │   ├── orchestrator-tracing/  0 LOC
    │   │   ├── orchestratord/  0 LOC
    │   │   │   └── controller/  0 LOC
    │   │   │       └── materialize/  0 LOC
    │   │   ├── ore/  0 LOC
    │   │   │   ├── channel/  0 LOC
    │   │   │   ├── collections/  0 LOC
    │   │   │   ├── metrics/  0 LOC
    │   │   │   └── netio/  0 LOC
    │   │   ├── ore-build/  0 LOC
    │   │   ├── ore-proc/  0 LOC
    │   │   ├── persist/  0 LOC
    │   │   │   └── indexed/  0 LOC
    │   │   │       └── columnar/  0 LOC
    │   │   ├── persist-cli/  0 LOC
    │   │   │   └── maelstrom/  0 LOC
    │   │   ├── persist-client/  0 LOC
    │   │   │   ├── cli/  0 LOC
    │   │   │   ├── internal/  0 LOC
    │   │   │   └── operators/  0 LOC
    │   │   ├── persist-proc/  0 LOC
    │   │   ├── persist-types/  0 LOC
    │   │   │   └── stats/  0 LOC
    │   │   ├── pgcopy/  0 LOC
    │   │   ├── pgrepr/  0 LOC
    │   │   │   └── value/  0 LOC
    │   │   ├── pgrepr-consts/  0 LOC
    │   │   ├── pgtest/  0 LOC
    │   │   ├── pgtz/  0 LOC
    │   │   ├── pgwire/  0 LOC
    │   │   ├── pgwire-common/  0 LOC
    │   │   ├── postgres-client/  0 LOC
    │   │   ├── postgres-util/  0 LOC
    │   │   ├── prof/  0 LOC
    │   │   ├── prof-http/  0 LOC
    │   │   ├── proto/  0 LOC
    │   │   ├── regexp/  0 LOC
    │   │   ├── repr/  0 LOC
    │   │   │   ├── adt/  0 LOC
    │   │   │   ├── explain/  0 LOC
    │   │   │   └── row/  0 LOC
    │   │   ├── repr-test-util/  0 LOC
    │   │   ├── rocksdb/  0 LOC
    │   │   ├── rocksdb-types/  0 LOC
    │   │   ├── s3-datagen/  0 LOC
    │   │   ├── secrets/  0 LOC
    │   │   ├── segment/  0 LOC
    │   │   ├── server-core/  0 LOC
    │   │   ├── service/  0 LOC
    │   │   │   └── transport/  0 LOC
    │   │   ├── sql/  0 LOC
    │   │   │   ├── ast/  0 LOC
    │   │   │   ├── plan/  0 LOC
    │   │   │   │   ├── explain/  0 LOC
    │   │   │   │   ├── lowering/  0 LOC
    │   │   │   │   └── statement/  0 LOC
    │   │   │   │       └── ddl/  0 LOC
    │   │   │   ├── pure/  0 LOC
    │   │   │   └── session/  0 LOC
    │   │   │       └── vars/  0 LOC
    │   │   ├── sql-lexer/  0 LOC
    │   │   ├── sql-parser/  0 LOC
    │   │   │   └── ast/  0 LOC
    │   │   │       └── defs/  0 LOC
    │   │   ├── sql-pretty/  0 LOC
    │   │   ├── sql-server-util/  0 LOC
    │   │   ├── sqllogictest/  0 LOC
    │   │   │   └── bin/  0 LOC
    │   │   ├── ssh-util/  0 LOC
    │   │   ├── storage/  0 LOC
    │   │   │   ├── decode/  0 LOC
    │   │   │   ├── metrics/  0 LOC
    │   │   │   │   ├── sink/  0 LOC
    │   │   │   │   └── source/  0 LOC
    │   │   │   ├── render/  0 LOC
    │   │   │   ├── sink/  0 LOC
    │   │   │   ├── source/  0 LOC
    │   │   │   │   ├── generator/  0 LOC
    │   │   │   │   ├── mysql/  0 LOC
    │   │   │   │   │   └── replication/  0 LOC
    │   │   │   │   ├── postgres/  0 LOC
    │   │   │   │   ├── reclock/  0 LOC
    │   │   │   │   └── sql_server/  0 LOC
    │   │   │   ├── storage_state/  0 LOC
    │   │   │   └── upsert/  0 LOC
    │   │   ├── storage-client/  0 LOC
    │   │   │   ├── storage_collections/  0 LOC
    │   │   │   └── util/  0 LOC
    │   │   ├── storage-controller/  0 LOC
    │   │   │   └── persist_handles/  0 LOC
    │   │   ├── storage-operators/  0 LOC
    │   │   │   ├── oneshot_source/  0 LOC
    │   │   │   └── s3_oneshot_sink/  0 LOC
    │   │   ├── storage-types/  0 LOC
    │   │   │   ├── connections/  0 LOC
    │   │   │   ├── sinks/  0 LOC
    │   │   │   └── sources/  0 LOC
    │   │   ├── testdrive/  0 LOC
    │   │   │   ├── action/  0 LOC
    │   │   │   │   ├── duckdb/  0 LOC
    │   │   │   │   ├── kafka/  0 LOC
    │   │   │   │   ├── mysql/  0 LOC
    │   │   │   │   ├── postgres/  0 LOC
    │   │   │   │   └── sql_server/  0 LOC
    │   │   │   ├── format/  0 LOC
    │   │   │   └── util/  0 LOC
    │   │   ├── timely-util/  0 LOC
    │   │   │   ├── columnar/  0 LOC
    │   │   │   └── containers/  0 LOC
    │   │   ├── timestamp-oracle/  0 LOC
    │   │   ├── tls-util/  0 LOC
    │   │   ├── tracing/  0 LOC
    │   │   ├── transform/  0 LOC
    │   │   │   ├── analysis/  0 LOC
    │   │   │   ├── canonicalization/  0 LOC
    │   │   │   ├── compound/  0 LOC
    │   │   │   ├── cse/  0 LOC
    │   │   │   ├── fusion/  0 LOC
    │   │   │   ├── movement/  0 LOC
    │   │   │   └── notice/  0 LOC
    │   │   ├── txn-wal/  0 LOC
    │   │   └── walkabout/  0 LOC
    │   ├── platform/  0 LOC
    │   │   └── assets/  0 LOC
    │   └── reference/  0 LOC
    │       ├── adapter/  0 LOC
    │       ├── assets/  0 LOC
    │       ├── compute/  0 LOC
    │       └── storage/  0 LOC
    ├── third-party/  0 LOC
    │   └── fivetran/  0 LOC
    └── user/  0 LOC
        ├── .prompts/  0 LOC
        ├── archetypes/  0 LOC
        ├── assets/  0 LOC
        │   └── sass/  0 LOC
        ├── content/  0 LOC
        │   ├── administration/  0 LOC
        │   ├── concepts/  0 LOC
        │   ├── console/  0 LOC
        │   ├── get-started/  0 LOC
        │   ├── headless/  0 LOC
        │   │   ├── fdw/  0 LOC
        │   │   ├── iceberg-sinks/  0 LOC
        │   │   ├── materialize-intro/  0 LOC
        │   │   ├── rbac-cloud/  0 LOC
        │   │   ├── rbac-sm/  0 LOC
        │   │   ├── replacement-views/  0 LOC
        │   │   ├── self-managed-deployments/  0 LOC
        │   │   └── sql-command-privileges/  0 LOC
        │   ├── ingest-data/  0 LOC
        │   │   ├── kafka/  0 LOC
        │   │   ├── mongodb/  0 LOC
        │   │   ├── mysql/  0 LOC
        │   │   ├── network-security/  0 LOC
        │   │   ├── postgres/  0 LOC
        │   │   ├── redpanda/  0 LOC
        │   │   ├── sql-server/  0 LOC
        │   │   └── webhooks/  0 LOC
        │   ├── integrations/  0 LOC
        │   │   ├── cli/  0 LOC
        │   │   │   └── reference/  0 LOC
        │   │   ├── client-libraries/  0 LOC
        │   │   ├── mcp-server/  0 LOC
        │   │   └── mz-debug/  0 LOC
        │   ├── manage/  0 LOC
        │   │   ├── dbt/  0 LOC
        │   │   ├── disaster-recovery/  0 LOC
        │   │   ├── monitor/  0 LOC
        │   │   │   ├── cloud/  0 LOC
        │   │   │   └── self-managed/  0 LOC
        │   │   └── terraform/  0 LOC
        │   ├── reference/  0 LOC
        │   │   └── system-catalog/  0 LOC
        │   ├── releases/  0 LOC
        │   ├── security/  0 LOC
        │   │   ├── appendix/  0 LOC
        │   │   ├── cloud/  0 LOC
        │   │   │   ├── access-control/  0 LOC
        │   │   │   └── users-service-accounts/  0 LOC
        │   │   └── self-managed/  0 LOC
        │   │       └── access-control/  0 LOC
        │   ├── self-managed/  0 LOC
        │   │   └── v25.2/  0 LOC
        │   │       └── installation/  0 LOC
        │   ├── self-managed-deployments/  0 LOC
        │   │   ├── appendix/  0 LOC
        │   │   │   └── legacy/  0 LOC
        │   │   ├── deployment-guidelines/  0 LOC
        │   │   ├── installation/  0 LOC
        │   │   │   └── legacy/  0 LOC
        │   │   └── upgrading/  0 LOC
        │   │       └── legacy/  0 LOC
        │   ├── serve-results/  0 LOC
        │   │   ├── bi-tools/  0 LOC
        │   │   └── sink/  0 LOC
        │   ├── sql/  0 LOC
        │   │   ├── create-sink/  0 LOC
        │   │   ├── create-source/  0 LOC
        │   │   ├── functions/  0 LOC
        │   │   ├── select/  0 LOC
        │   │   └── types/  0 LOC
        │   └── transform-data/  0 LOC
        │       ├── idiomatic-materialize-sql/  0 LOC
        │       │   └── appendix/  0 LOC
        │       ├── patterns/  0 LOC
        │       └── updating-materialized-views/  0 LOC
        ├── data/  0 LOC
        │   ├── best_practices/  0 LOC
        │   ├── console/  0 LOC
        │   ├── disaster_recovery/  0 LOC
        │   ├── examples/  0 LOC
        │   │   ├── index_usage/  0 LOC
        │   │   ├── ingest_data/  0 LOC
        │   │   │   ├── mysql/  0 LOC
        │   │   │   ├── postgres/  0 LOC
        │   │   │   └── sql_server/  0 LOC
        │   │   ├── rbac-cloud/  0 LOC
        │   │   ├── rbac-sm/  0 LOC
        │   │   ├── sql_functions/  0 LOC
        │   │   └── sql_types/  0 LOC
        │   ├── idiomatic_mzsql/  0 LOC
        │   ├── mz-debug/  0 LOC
        │   ├── mz_now/  0 LOC
        │   ├── rbac/  0 LOC
        │   └── self_managed/  0 LOC
        │       └── monitoring/  0 LOC
        ├── layouts/  0 LOC
        │   ├── _default/  0 LOC
        │   │   └── _markup/  0 LOC
        │   ├── api/  0 LOC
        │   ├── katacoda/  0 LOC
        │   ├── partials/  0 LOC
        │   │   ├── create-source/  0 LOC
        │   │   │   ├── connector/  0 LOC
        │   │   │   │   ├── file/  0 LOC
        │   │   │   │   └── kafka/  0 LOC
        │   │   │   └── envelope/  0 LOC
        │   │   │       ├── append-only/  0 LOC
        │   │   │       ├── debezium/  0 LOC
        │   │   │       └── upsert/  0 LOC
        │   │   ├── explain-plans/  0 LOC
        │   │   └── yaml-tables/  0 LOC
        │   └── shortcodes/  0 LOC
        │       ├── create-source/  0 LOC
        │       ├── explain-plans/  0 LOC
        │       ├── icons/  0 LOC
        │       ├── idiomatic-sql/  0 LOC
        │       ├── ingest-data/  0 LOC
        │       ├── integrations/  0 LOC
        │       │   └── mz-debug/  0 LOC
        │       ├── kafka/  0 LOC
        │       │   └── cockroachdb/  0 LOC
        │       ├── mysql-direct/  0 LOC
        │       │   └── ingesting-data/  0 LOC
        │       ├── network-security/  0 LOC
        │       ├── plugins/  0 LOC
        │       ├── self-managed/  0 LOC
        │       │   └── versions/  0 LOC
        │       │       └── upgrade/  0 LOC
        │       └── sql-server-direct/  0 LOC
        │           └── ingesting-data/  0 LOC
        ├── shared-content/  0 LOC
        │   └── self-managed/  0 LOC
        │       └── upgrade-notes/  0 LOC
        ├── sql-grammar/  0 LOC
        └── static/  0 LOC
            └── images/  0 LOC
                ├── console/  0 LOC
                │   └── console-create-new/  0 LOC
                │       └── postgresql/  0 LOC
                ├── demos/  0 LOC
                ├── excel/  0 LOC
                ├── monitoring/  0 LOC
                ├── releases/  0 LOC
                └── self-managed/  0 LOC
```
