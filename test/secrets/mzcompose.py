# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized, Testdrive

SERVICES = [
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized("materialized")

    # ensure that the directory has restricted permissions
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `stat -c \"%a\" /share/mzdata/secrets` == '700' ]] && exit 0 || exit 1",
    )

    c.sql("CREATE SECRET foo AS 'bar'")
    # Check that the contents of the secret have made it to the storage
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `cat /share/mzdata/secrets/*` == 'bar' ]] && exit 0 || exit 1",
    )

    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `stat -c \"%a\" /share/mzdata/secrets/*` == '600' ]] && exit 0 || exit 1",
    )

    c.sql("DROP SECRET foo")
    # Check that the file has been deleted from the storage
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ -z `ls -A /share/mzdata/secrets` ]] && exit 0 || exit 1",
    )
