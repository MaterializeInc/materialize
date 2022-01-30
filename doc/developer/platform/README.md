# Materialize Platform

Materialize Platform is the evolution of `materialized` into a
horizontally scalable, highly available distributed system.

The intended experience with Materialize Platform is that many independent users
can establish changing data sets, create and interrogate views over these data,
and share the data, views, and results with each other in a consistent fashion.
The intent is that in each dimension we remove limits to scaling:

* We aim to support unbounded numbers of concurrent users and sessions
* We aim to support unbounded numbers of data sources, and with unbounded rates
* We aim to support unbounded numbers of views over these data

Here "unbounded" does not mean "infinite", only "can be increased by spending
more resources".

The above are goals, and the path to Materialize Platform starts with none being
the case yet.

Initial design work has begun in the documents below.

* [Database Architecture](db-architecture.md)
* [Cloud Architecture](db-architecture.md) (TODO)
* [User Experience](ux.md)
