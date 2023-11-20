# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services.minio import MINIO_BLOB_URI


class Backup(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.exec("mc", "mc", "mb", "--ignore-existing", "persist/crdb-backup")
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
           CREATE EXTERNAL CONNECTION backup_bucket AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
           BACKUP INTO 'external://backup_bucket';
           DROP EXTERNAL CONNECTION backup_bucket;
        """,
        )

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass


class Restore(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
            DROP DATABASE defaultdb;
            CREATE EXTERNAL CONNECTION backup_bucket AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
            RESTORE DATABASE defaultdb FROM LATEST IN 'external://backup_bucket';
            DROP EXTERNAL CONNECTION backup_bucket;
        """,
        )
        c.run(
            "persistcli",
            "admin",
            "--commit",
            "restore-blob",
            f"--blob-uri={MINIO_BLOB_URI}",
            "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
        )

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass
