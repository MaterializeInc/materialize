# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import datetime

from materialize import scratch


def main() -> None:
    desc = scratch.MachineDesc(
        name="chbench monthly",
        launch_script="bin/mzcompose --preserve-ports --find chbench run load-test",
        instance_type="r5ad.4xlarge",
        ami="ami-0aeb7c931a5a61206",
        tags={
            "scrape_benchmark_numbers": "true",
            "lt_name": "monthly-chbench",
            "purpose": "load_test_monthly",
            "mzconduct_workflow": "load-test",
            "test": "chbench",
            "environment": "scratch",
        },
        size_gb=64,
    )
    now = datetime.datetime.utcnow()
    scratch.launch_cluster(
        [desc],
        nonce=now.replace(tzinfo=datetime.timezone.utc).isoformat(),
        # Keep alive for at least a day.
        delete_after=datetime.datetime.utcnow() + datetime.timedelta(days=1),
    )


if __name__ == "__main__":
    main()
