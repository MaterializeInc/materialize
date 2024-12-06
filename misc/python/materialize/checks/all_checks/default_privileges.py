# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class DefaultPrivileges(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE DATABASE defpriv_db
            > SET DATABASE = defpriv_db
            > CREATE SCHEMA defpriv_schema
            > SET SCHEMA defpriv_schema
            > CREATE ROLE defpriv_role1

            $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
            GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO defpriv_role1

            > CREATE TABLE defpriv_table1 (c int)
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > SET DATABASE = defpriv_db
                > SET SCHEMA defpriv_schema
                > ALTER DEFAULT PRIVILEGES FOR ROLE materialize IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role1;
                > CREATE ROLE defpriv_role2

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO defpriv_role2

                > CREATE TABLE defpriv_table2 (c int)
                """,
                """
                > SET DATABASE = defpriv_db
                > SET SCHEMA defpriv_schema
                > ALTER DEFAULT PRIVILEGES FOR ROLE materialize IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role2;
                > CREATE ROLE defpriv_role3

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO defpriv_role3

                > CREATE TABLE defpriv_table3 (c int)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET DATABASE = defpriv_db
                > SET SCHEMA defpriv_schema
                > ALTER DEFAULT PRIVILEGES FOR ROLE materialize IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role3;
                > SELECT
                    (CASE defaults.role_id WHEN 'p' THEN 'PUBLIC' ELSE roles.name END) AS role_name,
                    databases.name AS database_name,
                    schemas.name AS schema_name,
                    defaults.object_type AS object_type,
                    (CASE defaults.grantee WHEN 'p' THEN 'PUBLIC' ELSE grantees.name END) AS grantee_name,
                    defaults.privileges AS privileges
                  FROM mz_default_privileges defaults
                  LEFT JOIN mz_roles AS roles ON defaults.role_id = roles.id
                  LEFT JOIN mz_roles AS grantees ON defaults.grantee = grantees.id
                  LEFT JOIN mz_databases AS databases ON defaults.database_id = databases.id
                  LEFT JOIN mz_schemas AS schemas ON defaults.schema_id = schemas.id
                  ORDER BY role_name, grantee_name;
                PUBLIC <null> <null> cluster mz_support U
                PUBLIC <null> <null> database mz_support U
                PUBLIC <null> <null> schema mz_support U
                PUBLIC <null> <null> type PUBLIC U
                materialize defpriv_db defpriv_schema table defpriv_role1 arwd
                materialize defpriv_db defpriv_schema table defpriv_role2 arwd
                materialize defpriv_db defpriv_schema table defpriv_role3 arwd

                > SELECT name, unnest(privileges)::text FROM mz_tables WHERE name LIKE 'defpriv_table%'
                defpriv_table1 materialize=arwd/materialize
                defpriv_table2 defpriv_role1=arwd/materialize
                defpriv_table2 materialize=arwd/materialize
                defpriv_table3 defpriv_role1=arwd/materialize
                defpriv_table3 defpriv_role2=arwd/materialize
                defpriv_table3 materialize=arwd/materialize
                """
            )
        )
