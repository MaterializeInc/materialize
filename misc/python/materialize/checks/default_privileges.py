# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.util import MzVersion


class DefaultPrivileges(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE DATABASE defpriv_db
            > SET DATABASE = defpriv_db
            > CREATE SCHEMA defpriv_schema
            > SET SCHEMA defpriv_schema
            > CREATE ROLE defpriv_role1
            >[version<5900] ALTER ROLE defpriv_role1 CREATEDB CREATECLUSTER

            $[version>=5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
            GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO defpriv_role1

            > CREATE TABLE defpriv_table1 (c int)
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > SET DATABASE = defpriv_db
                > SET SCHEMA defpriv_schema
                > ALTER DEFAULT PRIVILEGES IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role1;
                > CREATE ROLE defpriv_role2
                >[version<5900] ALTER ROLE defpriv_role2 CREATEDB CREATECLUSTER

                $[version>=5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO defpriv_role2

                > CREATE TABLE defpriv_table2 (c int)
                """,
                """
                > SET DATABASE = defpriv_db
                > SET SCHEMA defpriv_schema
                > ALTER DEFAULT PRIVILEGES IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role2;
                > CREATE ROLE defpriv_role3
                >[version<5900] ALTER ROLE defpriv_role3 CREATEDB CREATECLUSTER

                $[version>=5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
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
                > ALTER DEFAULT PRIVILEGES IN SCHEMA defpriv_db.defpriv_schema GRANT ALL PRIVILEGES ON TABLES TO defpriv_role3;
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
                PUBLIC <null> <null> CLUSTER mz_introspection U
                PUBLIC <null> <null> TYPE PUBLIC U
                materialize defpriv_db defpriv_schema TABLE defpriv_role1 arwd
                materialize defpriv_db defpriv_schema TABLE defpriv_role2 arwd
                materialize defpriv_db defpriv_schema TABLE defpriv_role3 arwd

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
