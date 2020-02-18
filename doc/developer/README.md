# Developer Documentation Overview

This is the root directory for the Materialize developer documentation, which
explains everything needed to start contributing changes to Materialize!

## Table of Contents

* [develop.md](develop.md) contains instructions for building Materialize
from source and getting your local environment set up to run all of the tests
* [testing.md](testing.md) contains information about our main categories of tests
and guidance on which ones to add
* [debugging.md](debugging.md) is a guide to debugging Materialize using
rust-gdb / rust-lldb
* [demo.md](demo.md) contains instructions for running a simple Materialize demo
locally
* [metabase-demo.md](metabase-demo.md) contains instructions for setting up and viewing
Metabase dashboards with Materialize
* [project-management.md](project-management.md) tries to describe our current
process for managing Github issues and milestones. Note that while all of these
docs are subject to change, this one is _especially_ subject to change.
* [setup-mysql-debezium.md](setup-mysql-debezium.md) describes how to use Materialize
as a fast read replica for a MySQL database
* [setup-postgres-debezium.md](setup-postgres-debezium.md) describes how to use
Materialize as a fast read replica for a PostgeSQL database
* [style.md](style.md) is our style guide for Rust and SQL for the Materialize
codebase

## Additional Info

For a more general introduction to Materialize, see the [user
documentation](https://materialize.io/docs). For Rust API documentation, see
<https://mtrlz.dev/api/>. If you need anything else, [file an
issue](https://github.com/MaterializeInc/materialize/issues/new/choose).
