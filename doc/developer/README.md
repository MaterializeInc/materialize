# Developer documentation

This is the root directory of the Materialize developer documentation, which
describes our engineering process and philosophy.

For a more general introduction to Materialize, see the [user
documentation](https://materialize.com/docs). For API documentation, see
[mtrlz.dev](https://mtrlz.dev).

## New contributors

If you're a new contributor, we recommend reading the following chapters in the
user documentation, if you haven't already:

  1. [Get Started](https://materialize.com/docs/get-started/)
  2. [What Is Materialized?](https://materialize.com/docs/overview/what-is-materialize/)
  3. [Architecture Overview](https://materialize.com/docs/overview/architecture/)

Then, once you're up to speed, dive into the [developer guide](guide.md). The
guide is intended to be skimmed from start to finish, to give you the lay of the
land, and then browsed as reference material as you skill up on the codebase.

## Table of contents

* [change-data-capture.md](change-data-capture.md) describes our change data
  capture (CDC) requirements.

* [debugging.md](debugging.md) is a guide to debugging Materialize using
  rust-gdb / rust-lldb.

* [demos.md](demos.md) explains the two tiers of demos in this codebase and
  how to add a new one.

* Our diagnostics guide is a work in progress. The sections we have so far are
  themselves works in progress. Feel free to add to them if you think up of
  something useful.
  * [diagnostic-queries.md](diagnostic-queries.md) has queries to find the
    specific, problematic Timely/Differential operators.
  * [diagnostic-questions.md](diagnostic-questions.md) is a checklist of
    higher-level things to look for and think about if something is not working
    as expected.

* [fast-compiles.md](fast-compiles.md) describes various tricks for speeding
  up Rust compilation.

* [guide.md](guide.md) walks you through hacking on this codebase and our
  development philosophy.

  Several sections of the guide are important enough to warrant their own
  documents:

  * [guide-changes.md](guide-changes.md) describes practices and expectations for
    committing and reviewing code.
  * [guide-testing.md](guide-testing.md) describes our various test suites and
    our testing philosophy.

* [mzbuild.md](mzbuild.md) describes the custom build system we use to manage
  our Docker images and Docker Compose configurations.

* [project-management.md](project-management.md) attempts to describe our
  current process for managing GitHub issues and milestones. Note that while all
  of these docs are subject to change, this one is _especially_ subject to
  change.

* [setup-mysql-debezium.md](setup-mysql-debezium.md) describes how to use
  Materialize as a fast read replica for a MySQL database.

* [setup-postgres-debezium.md](setup-postgres-debezium.md) describes how to use
  Materialize as a fast read replica for a PostgreSQL database.

* [sqllogictest.md](sqllogictest.md) contains detailed information about
  our SQL logic testing framework.
