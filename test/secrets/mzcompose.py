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
        "[[ `stat -c \"%a\" /mzdata/secrets` == '700' ]] && exit 0 || exit 1",
    )

    c.sql("CREATE SECRET secret AS 's3cret'")
    # Check that the contents of the secret have made it to the storage
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `cat /mzdata/secrets/*` == 's3cret' ]] && exit 0 || exit 1",
    )

    # Check that the file permissions are restrictive
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `stat -c \"%a\" /mzdata/secrets/*` == '600' ]] && exit 0 || exit 1",
    )

    # Check that alter secret gets reflected on disk
    c.sql("ALTER SECRET secret AS 'tops3cret'")
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `cat /mzdata/secrets/*` == 'tops3cret' ]] && exit 0 || exit 1",
    )

    # check that replacing the file did not change permissions
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `stat -c \"%a\" /mzdata/secrets/*` == '600' ]] && exit 0 || exit 1",
    )

    # Rename should not change the contents on disk
    c.sql("ALTER SECRET secret RENAME TO renamed_secret")

    # Check that the contents of the secret have made it to the storage
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ `cat /mzdata/secrets/*` == 'tops3cret' ]] && exit 0 || exit 1",
    )

    c.sql("DROP SECRET renamed_secret")
    # Check that the file has been deleted from the storage
    c.exec(
        "materialized",
        "bash",
        "-c",
        "[[ -z `ls -A /mzdata/secrets` ]] && exit 0 || exit 1",
    )
