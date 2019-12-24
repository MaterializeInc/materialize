# Materialize SQL parser

This parser is a fork of <https://github.com/andygrove/sqlparser-rs>, with
some additional patches from <http://github.com/nickolay/sqlparser-rs>.

We decided to jettison compatibility with upstream so that we can perform
large-scale refactors and rip out code for supporting other dialects, like
MySQL and MSSQL, in favor of parsing just Materialize's SQL dialect.
