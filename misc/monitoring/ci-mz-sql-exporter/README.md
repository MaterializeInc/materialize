materialized sql_exporter
=========================

A docker image that configures [sql_exporter][] to look at metric tables that we care
about.

This is a subset of the functionality provided by our [dashboard][], and is just used for
ci and load tests for now.

[sql_exporter]: https://github.com/free/sql_exporter
[dashboard]: ../dashboard
