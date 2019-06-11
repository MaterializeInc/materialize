# Cockroach SQL Logic Tests

These tests are derived from CockroachDB's in-house SQL logic tests. The tests
use an extended version of the ["sqllogictest" format][slt]. The only
documentation for these extensions is [CockroachDB's in-house sqllogictest
runner][slt-ext].

Many CockroachDB tests are not applicable to Materialize. Tests that exercise
CockroachDB internal concepts, like "ranges" and "zones", have been deleted
outright. Tests that exercise features that we may support someday, like
collations, have been retained, but the files are skipped with an unconditional
`halt` directive at the top.

## Legal details

The test files were retrieved on June 10, 2019 from:

> https://github.com/cockroachdb/cockroach/tree/d2f7fbf5dd1fc1a099bbad790a2e1f7c60a66cc3/pkg/sql/logictest/testdata/logic_test

Please note that this commit was carefully chosen to precede Cockroach Labs's
decision to relicense CockroachDB under the Business Source License (BSL). (For
more details about the BSL, see the blog post, ["Why We're Relicensing
CockroachDB"][blog].)

[blog]: https://www.cockroachlabs.com/blog/oss-relicensing-cockroachdb/
[slt]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
[slt-ext]: https://github.com/cockroachdb/cockroach/blob/d2f7fbf5dd1fc1a099bbad790a2e1f7c60a66cc3/pkg/sql/logictest/logic.go
