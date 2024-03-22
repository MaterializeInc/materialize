# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzexplore — tools for analyzing query plans

`bin/mzexplore` can extract plans from environments running
Materialize. By running:

```sh
mzexplore extract plans ... \
          --explainee-type catalog_item \
          --with arity --with humanized_exprs --with join_impls \
          --format json \
          --stage optimized_plan
```

you can extract JSON files corresponding to the optimized MIR plan of
a catalog item.

This directory contains `jq` scripts for extracting information from
these JSON outputs.

- `filters.jq`: finds all predicates used in filters and returns a
  list, sorted by number of occurrences descending

- `leftjoins.jq`: finds unreduced left joins (particularly shaped
  unions of antijoins and an inner join)

- `defs.jq`: utility functions

Note that the ASTs we produce in JSON are quite deeply nested, so you
can't use stock `jq`. As of 2024-03-12, there is a [PR open to
`jq`](https://github.com/jqlang/jq/pull/3063) to allow you to specify
unbounded depth on the command-line, i.e., `--depth 0`.

If you have multiple JSON files you'd like to analyze together, you
can run:

```sh
find ~/mz-support/tmpfs/ -name \*.json |
  xargs jq --slurp --depth 0 -f filters.jq
```
