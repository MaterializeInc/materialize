#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Single first command for the parallel-workload template.

Antithesis selects exactly one `first_*` command per execution history
(see https://antithesis.com/docs/test_templates/test_composer_reference/#first-command).
This template has three logical setup steps, all of which the
parallel_driver_* commands here depend on:

  1. Polaris iceberg catalog + namespace + minio bucket — required by
     parallel_driver_parallel_workload's CreateIcebergSinkAction; the
     bootstrap is implemented in helper_polaris_setup.
  2. `antithesis_txn_table` + matching MV — required by
     parallel_driver_explicit_txn_no_since_violation; in
     helper_explicit_txn_setup.
  3. `pw_hot_table` + supporting MVs — required by
     parallel_driver_pw_hot_objects; in helper_pw_hot_objects_setup.

Chaining them under a single `first_*` command instead of three
separate ones means every execution history gets all three setups,
rather than 1/3 of histories running each.  Each helper is idempotent
on re-entry, so reruns are cheap.
"""

from __future__ import annotations

import sys

import helper_explicit_txn_setup
import helper_logging
import helper_polaris_setup
import helper_pw_hot_objects_setup

LOG = helper_logging.setup_logging("first.parallel_workload_setup")


def main() -> int:
    LOG.info("parallel-workload first command: polaris → explicit_txn → pw_hot")
    helper_polaris_setup.main()
    helper_explicit_txn_setup.main()
    helper_pw_hot_objects_setup.main()
    LOG.info("parallel-workload first command: all setups complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
