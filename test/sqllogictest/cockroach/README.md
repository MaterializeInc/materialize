# Cockroach SQL Logic Tests

These tests are derived from CockroachDB's in-house SQL logic tests. The tests
use an extended version of the ["sqllogictest" format][slt]. The only
documentation for these extensions is [CockroachDB's in-house sqllogictest
runner][slt-ext].

Many CockroachDB tests are not applicable to Materialize. Tests that exercise
CockroachDB internal concepts, like "ranges" and "zones", have been deleted
outright. Tests that exercise features that we may support someday, like
collations, have been retained, but the files are skipped with an unconditional
`halt` directive at the top. Files that diverge from Materialize's behavior
partway through are cut short with a `halt` directive at the point of
divergence, so the prefix still runs.

## Legal details

The test files were retrieved in two batches. Each file's license header
records which batch it came from.

The original batch was retrieved on June 10, 2019 from:

> https://github.com/cockroachdb/cockroach/tree/d2f7fbf5dd1fc1a099bbad790a2e1f7c60a66cc3/pkg/sql/logictest/testdata/logic_test

Please note that this commit was carefully chosen to precede Cockroach Labs's
decision to relicense CockroachDB under the Business Source License (BSL). (For
more details about the BSL, see the blog post, ["Why We're Relicensing
CockroachDB"][blog].)

The second batch was retrieved on July 6, 2026 from the commit tagged v23.1.0:

> https://github.com/cockroachdb/cockroach/tree/358e0d87912365b8976c55ab9b3292e999cf720d/pkg/sql/logictest/testdata/logic_test

CockroachDB v23.1 is licensed under BSL 1.1 with a Change Date of April 1,
2026 and a Change License of Apache 2.0 (see `licenses/BSL.txt` at that
commit). The retrieval date is after the Change Date, so the retrieved files
are governed by the Apache 2.0 license.

Future refreshes have a limited window. CockroachDB v23.2 converts to Apache
2.0 on October 1, 2026, v24.1 on April 1, 2027, and v24.2 on October 1, 2027.
Releases from v24.3 onward use the CockroachDB Software License, which never
converts, so v24.2 is the last version that can ever be imported.

[blog]: https://www.cockroachlabs.com/blog/oss-relicensing-cockroachdb/
[slt]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
[slt-ext]: https://github.com/cockroachdb/cockroach/blob/358e0d87912365b8976c55ab9b3292e999cf720d/pkg/sql/logictest/logic.go
