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


class Owners(Check):
    def _create_objects(self, role: str, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
            $[version>=5200] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
            GRANT CREATE ON DATABASE materialize TO {role}
            GRANT CREATE ON SCHEMA materialize.public TO {role}
            GRANT CREATE ON CLUSTER default TO {role}
            $ postgres-execute connection=postgres://{role}@materialized:6875/materialize
            CREATE DATABASE owner_db{i}
            CREATE SCHEMA owner_schema{i}
            CREATE CONNECTION owner_kafka_conn{i} FOR KAFKA BROKER '${{testdrive.kafka-addr}}'
            CREATE CONNECTION owner_csr_conn{i} FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}'
            CREATE TYPE owner_type{i} AS LIST (ELEMENT TYPE = text)
            CREATE TABLE owner_t{i} (c1 int, c2 owner_type{i})
            CREATE INDEX owner_i{i} ON owner_t{i} (c2)
            CREATE VIEW owner_v{i} AS SELECT * FROM owner_t{i}
            CREATE MATERIALIZED VIEW owner_mv{i} AS SELECT * FROM owner_t{i}
            CREATE SECRET owner_secret{i} AS 'MY_SECRET'
            """
        )
        if expensive:
            s += dedent(
                f"""
                CREATE SOURCE owner_source{i} FROM LOAD GENERATOR COUNTER (SCALE FACTOR 0.01)
                CREATE SINK owner_sink{i} FROM owner_mv{i} INTO KAFKA CONNECTION owner_kafka_conn{i} (TOPIC 'sink-sink-owner{i}') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION owner_csr_conn{i} ENVELOPE DEBEZIUM
                CREATE CLUSTER owner_cluster{i} REPLICAS (owner_cluster_r{i} (SIZE '4'))
                """
            )

        return s

    def _drop_objects(
        self, role: str, i: int, expensive: bool = False, success: bool = True
    ) -> str:
        cmds = [
            f"DROP SECRET owner_secret{i}",
            f"DROP MATERIALIZED VIEW owner_mv{i}",
            f"DROP VIEW owner_v{i}",
            f"DROP INDEX owner_i{i}",
            f"DROP TABLE owner_t{i}",
            f"DROP TYPE owner_type{i}",
            f"DROP CONNECTION owner_csr_conn{i}",
            f"DROP CONNECTION owner_kafka_conn{i}",
            f"DROP SCHEMA owner_schema{i}",
            f"DROP DATABASE owner_db{i}",
        ]
        if expensive:
            cmds += [
                f"DROP SOURCE owner_source{i}",
                f"DROP SINK owner_sink{i}",
                f"DROP CLUSTER owner_cluster{i}",
            ]
        if success:
            return (
                f"$ postgres-execute connection=postgres://{role}@materialized:6875/materialize\n"
                + "\n".join(cmds)
                + "\n"
            )
        if role != "materialize":
            raise ValueError(
                "Can't check for failures with user other than materialize"
            )
        return "\n".join(
            [f"! {cmd} CASCADE\ncontains: must be owner of\n" for cmd in cmds]
        )

    def _can_run(self) -> bool:
        # The code works from 0.47.0, but object owner only works from 0.48.0.
        # For the combinations of upgrade tests this is difficult to handle, so
        # instead only run the test from 0.48.0 on.
        return self.base_version >= MzVersion.parse("0.48.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            "> CREATE ROLE owner_role_01 CREATEDB CREATECLUSTER"
            + self._create_objects("owner_role_01", 1, expensive=True)
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                self._create_objects("owner_role_01", 2)
                + "> CREATE ROLE owner_role_02 CREATEDB CREATECLUSTER",
                self._create_objects("owner_role_01", 3)
                + self._create_objects("owner_role_02", 4)
                + "> CREATE ROLE owner_role_03 CREATEDB CREATECLUSTER",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            # materialize role is not allowed to drop the objects since it is
            # not the owner, verify this:
            (
                # Requires enable_ld_rbac_checks
                (
                    self._drop_objects("materialize", 1, success=False, expensive=True)
                    + self._drop_objects("materialize", 2, success=False)
                    + self._drop_objects("materialize", 3, success=False)
                    + self._drop_objects("materialize", 4, success=False)
                )
                if self.base_version >= MzVersion.parse("0.51.0-dev")
                else ""
            )
            + self._create_objects("owner_role_01", 5)
            + self._create_objects("owner_role_02", 6)
            + self._create_objects("owner_role_03", 7)
            + dedent(
                """
                $ psql-execute command="\\l owner_db*"
                \\                             List of databases
                   Name    |     Owner     | Encoding | Collate | Ctype | Access privileges
                -----------+---------------+----------+---------+-------+-------------------
                 owner_db1 | owner_role_01 | UTF8     | C       | C     |
                 owner_db2 | owner_role_01 | UTF8     | C       | C     |
                 owner_db3 | owner_role_01 | UTF8     | C       | C     |
                 owner_db4 | owner_role_02 | UTF8     | C       | C     |
                 owner_db5 | owner_role_01 | UTF8     | C       | C     |
                 owner_db6 | owner_role_02 | UTF8     | C       | C     |
                 owner_db7 | owner_role_03 | UTF8     | C       | C     |

                $ psql-execute command="\\dn owner_schema*"
                \\        List of schemas
                     Name      |     Owner
                ---------------+---------------
                 owner_schema1 | owner_role_01
                 owner_schema2 | owner_role_01
                 owner_schema3 | owner_role_01
                 owner_schema4 | owner_role_02
                 owner_schema5 | owner_role_01
                 owner_schema6 | owner_role_02
                 owner_schema7 | owner_role_03

                $ psql-execute command="\\dt owner_t*"
                \\             List of relations
                 Schema |   Name   | Type  |     Owner
                --------+----------+-------+---------------
                 public | owner_t1 | table | owner_role_01
                 public | owner_t2 | table | owner_role_01
                 public | owner_t3 | table | owner_role_01
                 public | owner_t4 | table | owner_role_02
                 public | owner_t5 | table | owner_role_01
                 public | owner_t6 | table | owner_role_02
                 public | owner_t7 | table | owner_role_03

                $ psql-execute command="\\di owner_i*"
                \\                  List of relations
                 Schema |   Name   | Type  |     Owner     |  Table
                --------+----------+-------+---------------+----------
                 public | owner_i1 | index | owner_role_01 | owner_t1
                 public | owner_i2 | index | owner_role_01 | owner_t2
                 public | owner_i3 | index | owner_role_01 | owner_t3
                 public | owner_i4 | index | owner_role_02 | owner_t4
                 public | owner_i5 | index | owner_role_01 | owner_t5
                 public | owner_i6 | index | owner_role_02 | owner_t6
                 public | owner_i7 | index | owner_role_03 | owner_t7

                $ psql-execute command="\\dv owner_v*"
                \\            List of relations
                 Schema |   Name   | Type |     Owner
                --------+----------+------+---------------
                 public | owner_v1 | view | owner_role_01
                 public | owner_v2 | view | owner_role_01
                 public | owner_v3 | view | owner_role_01
                 public | owner_v4 | view | owner_role_02
                 public | owner_v5 | view | owner_role_01
                 public | owner_v6 | view | owner_role_02
                 public | owner_v7 | view | owner_role_03

                $ psql-execute command="\\dmv owner_mv*"
                \\                   List of relations
                 Schema |   Name    |       Type        |     Owner
                --------+-----------+-------------------+---------------
                 public | owner_mv1 | materialized view | owner_role_01
                 public | owner_mv2 | materialized view | owner_role_01
                 public | owner_mv3 | materialized view | owner_role_01
                 public | owner_mv4 | materialized view | owner_role_02
                 public | owner_mv5 | materialized view | owner_role_01
                 public | owner_mv6 | materialized view | owner_role_02
                 public | owner_mv7 | materialized view | owner_role_03

                > SELECT mz_types.name, mz_roles.name FROM mz_types JOIN mz_roles ON mz_types.owner_id = mz_roles.id WHERE mz_types.name LIKE 'owner_type%'
                owner_type1 owner_role_01
                owner_type2 owner_role_01
                owner_type3 owner_role_01
                owner_type4 owner_role_02
                owner_type5 owner_role_01
                owner_type6 owner_role_02
                owner_type7 owner_role_03

                > SELECT mz_secrets.name, mz_roles.name FROM mz_secrets JOIN mz_roles ON mz_secrets.owner_id = mz_roles.id WHERE mz_secrets.name LIKE 'owner_secret%'
                owner_secret1 owner_role_01
                owner_secret2 owner_role_01
                owner_secret3 owner_role_01
                owner_secret4 owner_role_02
                owner_secret5 owner_role_01
                owner_secret6 owner_role_02
                owner_secret7 owner_role_03

                > SELECT mz_sources.name, mz_roles.name FROM mz_sources JOIN mz_roles ON mz_sources.owner_id = mz_roles.id WHERE mz_sources.name LIKE 'owner_source%' AND type = 'load-generator'
                owner_source1 owner_role_01

                > SELECT mz_sinks.name, mz_roles.name FROM mz_sinks JOIN mz_roles ON mz_sinks.owner_id = mz_roles.id WHERE mz_sinks.name LIKE 'owner_sink%'
                owner_sink1 owner_role_01

                > SELECT mz_clusters.name, mz_roles.name FROM mz_clusters JOIN mz_roles ON mz_clusters.owner_id = mz_roles.id WHERE mz_clusters.name LIKE 'owner_cluster%'
                owner_cluster1 owner_role_01

                > SELECT mz_cluster_replicas.name, mz_roles.name FROM mz_cluster_replicas JOIN mz_roles ON mz_cluster_replicas.owner_id = mz_roles.id WHERE mz_cluster_replicas.name LIKE 'owner_cluster_r%'
                owner_cluster_r1 owner_role_01

                > SELECT mz_connections.name, mz_roles.name FROM mz_connections JOIN mz_roles ON mz_connections.owner_id = mz_roles.id WHERE mz_connections.name LIKE 'owner_%'
                owner_csr_conn1 owner_role_01
                owner_csr_conn2 owner_role_01
                owner_csr_conn3 owner_role_01
                owner_csr_conn4 owner_role_02
                owner_csr_conn5 owner_role_01
                owner_csr_conn6 owner_role_02
                owner_csr_conn7 owner_role_03
                owner_kafka_conn1 owner_role_01
                owner_kafka_conn2 owner_role_01
                owner_kafka_conn3 owner_role_01
                owner_kafka_conn4 owner_role_02
                owner_kafka_conn5 owner_role_01
                owner_kafka_conn6 owner_role_02
                owner_kafka_conn7 owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_databases WHERE name LIKE 'owner_db%'
                owner_db1 owner_role_01=UC/owner_role_01
                owner_db2 owner_role_01=UC/owner_role_01
                owner_db3 owner_role_01=UC/owner_role_01
                owner_db4 owner_role_02=UC/owner_role_02
                owner_db5 owner_role_01=UC/owner_role_01
                owner_db6 owner_role_02=UC/owner_role_02
                owner_db7 owner_role_03=UC/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_schemas WHERE name LIKE 'owner_schema%'
                owner_schema1 owner_role_01=UC/owner_role_01
                owner_schema2 owner_role_01=UC/owner_role_01
                owner_schema3 owner_role_01=UC/owner_role_01
                owner_schema4 owner_role_02=UC/owner_role_02
                owner_schema5 owner_role_01=UC/owner_role_01
                owner_schema6 owner_role_02=UC/owner_role_02
                owner_schema7 owner_role_03=UC/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_tables WHERE name LIKE 'owner_t%'
                owner_t1 owner_role_01=arwd/owner_role_01
                owner_t2 owner_role_01=arwd/owner_role_01
                owner_t3 owner_role_01=arwd/owner_role_01
                owner_t4 owner_role_02=arwd/owner_role_02
                owner_t5 owner_role_01=arwd/owner_role_01
                owner_t6 owner_role_02=arwd/owner_role_02
                owner_t7 owner_role_03=arwd/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_views WHERE name LIKE 'owner_v%'
                owner_v1 owner_role_01=r/owner_role_01
                owner_v2 owner_role_01=r/owner_role_01
                owner_v3 owner_role_01=r/owner_role_01
                owner_v4 owner_role_02=r/owner_role_02
                owner_v5 owner_role_01=r/owner_role_01
                owner_v6 owner_role_02=r/owner_role_02
                owner_v7 owner_role_03=r/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_materialized_views WHERE name LIKE 'owner_mv%'
                owner_mv1 owner_role_01=r/owner_role_01
                owner_mv2 owner_role_01=r/owner_role_01
                owner_mv3 owner_role_01=r/owner_role_01
                owner_mv4 owner_role_02=r/owner_role_02
                owner_mv5 owner_role_01=r/owner_role_01
                owner_mv6 owner_role_02=r/owner_role_02
                owner_mv7 owner_role_03=r/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_types WHERE name LIKE 'owner_type%'
                owner_type1 =U/owner_role_01
                owner_type1 owner_role_01=U/owner_role_01
                owner_type2 =U/owner_role_01
                owner_type2 owner_role_01=U/owner_role_01
                owner_type3 =U/owner_role_01
                owner_type3 owner_role_01=U/owner_role_01
                owner_type4 =U/owner_role_02
                owner_type4 owner_role_02=U/owner_role_02
                owner_type5 =U/owner_role_01
                owner_type5 owner_role_01=U/owner_role_01
                owner_type6 =U/owner_role_02
                owner_type6 owner_role_02=U/owner_role_02
                owner_type7 =U/owner_role_03
                owner_type7 owner_role_03=U/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_secrets WHERE name LIKE 'owner_secret%'
                owner_secret1 owner_role_01=U/owner_role_01
                owner_secret2 owner_role_01=U/owner_role_01
                owner_secret3 owner_role_01=U/owner_role_01
                owner_secret4 owner_role_02=U/owner_role_02
                owner_secret5 owner_role_01=U/owner_role_01
                owner_secret6 owner_role_02=U/owner_role_02
                owner_secret7 owner_role_03=U/owner_role_03

                > SELECT name, unnest(privileges)::text FROM mz_sources WHERE name LIKE 'owner_source%' AND type = 'load-generator'
                owner_source1 owner_role_01=r/owner_role_01

                ! SELECT name, unnest(privileges)::text FROM mz_sinks WHERE name LIKE 'owner_sink%'
                contains: column "privileges" does not exist

                > SELECT name, unnest(privileges)::text FROM mz_clusters WHERE name LIKE 'owner_cluster%'
                owner_cluster1 owner_role_01=UC/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_connections WHERE name LIKE 'owner_%'
                owner_csr_conn1 owner_role_01=U/owner_role_01
                owner_csr_conn2 owner_role_01=U/owner_role_01
                owner_csr_conn3 owner_role_01=U/owner_role_01
                owner_csr_conn4 owner_role_02=U/owner_role_02
                owner_csr_conn5 owner_role_01=U/owner_role_01
                owner_csr_conn6 owner_role_02=U/owner_role_02
                owner_csr_conn7 owner_role_03=U/owner_role_03
                owner_kafka_conn1 owner_role_01=U/owner_role_01
                owner_kafka_conn2 owner_role_01=U/owner_role_01
                owner_kafka_conn3 owner_role_01=U/owner_role_01
                owner_kafka_conn4 owner_role_02=U/owner_role_02
                owner_kafka_conn5 owner_role_01=U/owner_role_01
                owner_kafka_conn6 owner_role_02=U/owner_role_02
                owner_kafka_conn7 owner_role_03=U/owner_role_03
                """
            )
            + self._drop_objects("owner_role_01", 5)
            + self._drop_objects("owner_role_02", 6)
            + self._drop_objects("owner_role_03", 7)
        )
