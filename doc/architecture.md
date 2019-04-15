# Architecture overview

This document is a partial dump of Nikhil's brain state on April 12, 2019. It is
likely to be incorrect and poorly edited. Please don't hesitate to rectify the
situation.

## Historical context

Materialize refers to the SQL materialized view engine that lives in this
repository. It sits atop [Differential Dataflow], which in turn sits atop
[Timely Dataflow], and those two components together provide a Rust API to an
extremely efficient streaming dataflow engine. The history of Timely and
Differential, hereafter referred to as T/D for brevity, is interesting, and
details can be found elsewhere for the curious [[1]].

For now, just know that T/D predates Materialize, the company, by several
years. There are several folks who have found use for T/D using their existing
APIs, both in research and industry. We intend to make T/D available under those
terms in perpetuity, in the spirit of scientific advancement.

Our goal with Materialize, the product, is to build a "better" interface on top
of T/D, and then sell that software to businesses with streaming data problems
for a tidy profit. The reason I say "better" is because the existing APIs for
T/D are, in fact, well designed and quite powerful; their problem is that they
are inaccessible to the average developer. Using T/D today requires being
intimately familiar with the Rust programming language, which takes even
experienced developers several months to learn. Perhaps worse, the T/D API is an
extremely unfamiliar programming model (in other words, it's not SQL) and
requires understanding the core ideas of the [Naiad paper], upon which T/D is
based.

The exact terms of the licensing of Materialize have yet to be worked out, so
for now this entire repository is both proprietary and private.

[1]: https://paper.dropbox.com/doc/Materialize-Product--AbHSqqXlN5YNKHiYEXm3EKyNAg-eMbfh2QTOCPrU7drExDCm
[Naiad paper]: http://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf

## Design overview

The best way I have of describing the product concisely is a "streaming SQL
materialized view engine." That means it takes SQL queries, like

```sql
SELECT state, avg(total)
  FROM orders
 WHERE product IN ('widget')
 GROUP BY state
```

and keeps them perpetually up to date.

What does it mean, though, for a query to be always up to date? In a traditional
database, you pose your query once. The database then sequences your query with
whatever writes it might be receiving, tabulates a consistent result, and then
gives you the answer that is up-to-date as of a particular time. If you want
an updated answer, you pose the same query again, and get an updated result.

There are two ways you might think about
the output of the SQL query. You can ask for a stream of updates, in which case
you might get output like this:

```
(timestamp, data, diff)
(20190411034000, ('NY', 30.22), -1)
(20190411034000, ('NY', 30.25), +1)
```

The information here indicates that the average order value for widgets in New
York increased three cents, from $30.22 to $30.25. The first stream update
indicates that the previous average of $30.22 is no longer valid, and the next
stream update indicates the new average.

TODO(benesch): complete document.
