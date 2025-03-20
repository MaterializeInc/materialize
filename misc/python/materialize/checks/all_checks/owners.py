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


class Owners(Check):
    def _create_objects(self, role: str, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
            $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
            GRANT CREATE ON DATABASE materialize TO {role}
            GRANT CREATE ON SCHEMA materialize.public TO {role}
            GRANT CREATE ON CLUSTER {self._default_cluster()} TO {role}
            GRANT CREATEDB ON SYSTEM TO {role}
            $ postgres-execute connection=postgres://{role}@${{testdrive.materialize-sql-addr}}
            CREATE DATABASE owner_db{i}
            CREATE SCHEMA owner_schema{i}
            CREATE CONNECTION owner_kafka_conn{i} FOR KAFKA {self._kafka_broker()}
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
                $ postgres-execute connection=postgres://{role}@${{testdrive.materialize-sql-addr}}
                CREATE SOURCE owner_source{i} FROM LOAD GENERATOR COUNTER
                $ postgres-execute connection=postgres://{role}@${{testdrive.materialize-sql-addr}}
                CREATE SINK owner_sink{i} FROM owner_mv{i} INTO KAFKA CONNECTION owner_kafka_conn{i} (TOPIC 'sink-sink-owner{i}') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION owner_csr_conn{i} ENVELOPE DEBEZIUM
                CREATE CLUSTER owner_cluster{i} REPLICAS (owner_cluster_r{i} (SIZE '4'))
                """
            )

        return s

    def _alter_object_owners(self, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
            $ postgres-execute connection=postgres://mz_system@${{testdrive.materialize-internal-sql-addr}}
            ALTER DATABASE owner_db{i} OWNER TO other_owner
            ALTER SCHEMA owner_schema{i} OWNER TO other_owner
            ALTER CONNECTION owner_kafka_conn{i} OWNER TO other_owner
            ALTER CONNECTION owner_csr_conn{i} OWNER TO other_owner
            ALTER TYPE owner_type{i} OWNER TO other_owner
            ALTER TABLE owner_t{i} OWNER TO other_owner
            ALTER INDEX owner_i{i} OWNER TO other_owner
            ALTER VIEW owner_v{i} OWNER TO other_owner
            ALTER MATERIALIZED VIEW owner_mv{i} OWNER TO other_owner
            ALTER SECRET owner_secret{i} OWNER TO other_owner
            """
        )
        if expensive:
            s += dedent(
                f"""
                ALTER SOURCE owner_source{i} OWNER TO other_owner
                ALTER SINK owner_sink{i} OWNER TO other_owner
                ALTER CLUSTER owner_cluster{i} OWNER TO other_owner
                """
            )

        return s

    def _drop_objects(
        self, role: str, i: int, expensive: bool = False, success: bool = True
    ) -> str:
        cmds = []
        # Drop the sink first so we can drop the materialized view without CASCADE.
        if expensive:
            cmds += [
                f"DROP SOURCE owner_source{i}",
                f"DROP SINK owner_sink{i}",
                f"DROP CLUSTER owner_cluster{i}",
            ]
        cmds += [
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
        if success:
            return (
                f"$ postgres-execute connection=postgres://{role}@${{testdrive.materialize-sql-addr}}\n"
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

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET SESSION enable_session_rbac_checks TO true

                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEROLE ON SYSTEM TO materialize

                > CREATE ROLE owner_role_01
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO owner_role_01

                > CREATE ROLE other_owner
                """
            )
            + self._create_objects("owner_role_01", 1, expensive=True)
            + self._create_objects("owner_role_01", 2, expensive=True)
            + self._alter_object_owners(2, expensive=True)
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                dedent(
                    """
                        > SET SESSION enable_session_rbac_checks TO true
                    """
                )
                + self._create_objects("owner_role_01", 3)
                + self._create_objects("owner_role_01", 4)
                + self._alter_object_owners(4)
                + dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEROLE ON SYSTEM TO materialize

                    > CREATE ROLE owner_role_02

                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO owner_role_02
                    """
                ),
                self._create_objects("owner_role_01", 5)
                + self._create_objects("owner_role_01", 6)
                + self._alter_object_owners(6)
                + self._create_objects("owner_role_02", 7)
                + self._create_objects("owner_role_02", 8)
                + self._alter_object_owners(8)
                + dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEROLE ON SYSTEM TO materialize

                    > CREATE ROLE owner_role_03

                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEDB, CREATECLUSTER ON SYSTEM TO owner_role_03
                    """
                ),
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET SESSION enable_session_rbac_checks TO true
                """
            )
            +
            # materialize role is not allowed to drop the objects since it is
            # not the owner, verify this:
            self._drop_objects("materialize", 1, success=False, expensive=True)
            + self._drop_objects("materialize", 2, success=False, expensive=True)
            + self._drop_objects("materialize", 3, success=False)
            + self._drop_objects("materialize", 4, success=False)
            + self._drop_objects("materialize", 5, success=False)
            + self._drop_objects("materialize", 6, success=False)
            + self._drop_objects("materialize", 7, success=False)
            + self._drop_objects("materialize", 8, success=False)
            + self._create_objects("owner_role_01", 9)
            + self._create_objects("owner_role_02", 10)
            + self._create_objects("owner_role_03", 11)
            + dedent(
                """
                $ psql-execute command="\\l owner_db*"
                \\                                                   List of databases
                    Name    |     Owner     | Encoding | Locale Provider | Collate | Ctype | ICU Locale | ICU Rules | Access privileges
                ------------+---------------+----------+-----------------+---------+-------+------------+-----------+-------------------
                 owner_db1  | owner_role_01 | UTF8     | libc            | C       | C     |            |           |
                 owner_db10 | owner_role_02 | UTF8     | libc            | C       | C     |            |           |
                 owner_db11 | owner_role_03 | UTF8     | libc            | C       | C     |            |           |
                 owner_db2  | other_owner   | UTF8     | libc            | C       | C     |            |           |
                 owner_db3  | owner_role_01 | UTF8     | libc            | C       | C     |            |           |
                 owner_db4  | other_owner   | UTF8     | libc            | C       | C     |            |           |
                 owner_db5  | owner_role_01 | UTF8     | libc            | C       | C     |            |           |
                 owner_db6  | other_owner   | UTF8     | libc            | C       | C     |            |           |
                 owner_db7  | owner_role_02 | UTF8     | libc            | C       | C     |            |           |
                 owner_db8  | other_owner   | UTF8     | libc            | C       | C     |            |           |
                 owner_db9  | owner_role_01 | UTF8     | libc            | C       | C     |            |           |


                $ psql-execute command="\\dn owner_schema*"
                \\        List of schemas
                      Name      |     Owner
                ----------------+---------------
                 owner_schema1  | owner_role_01
                 owner_schema10 | owner_role_02
                 owner_schema11 | owner_role_03
                 owner_schema2  | other_owner
                 owner_schema3  | owner_role_01
                 owner_schema4  | other_owner
                 owner_schema5  | owner_role_01
                 owner_schema6  | other_owner
                 owner_schema7  | owner_role_02
                 owner_schema8  | other_owner
                 owner_schema9  | owner_role_01

                $ psql-execute command="\\dt owner_t*"
                \\             List of relations
                 Schema |   Name    | Type  |     Owner
                --------+-----------+-------+---------------
                 public | owner_t1  | table | owner_role_01
                 public | owner_t10 | table | owner_role_02
                 public | owner_t11 | table | owner_role_03
                 public | owner_t2  | table | other_owner
                 public | owner_t3  | table | owner_role_01
                 public | owner_t4  | table | other_owner
                 public | owner_t5  | table | owner_role_01
                 public | owner_t6  | table | other_owner
                 public | owner_t7  | table | owner_role_02
                 public | owner_t8  | table | other_owner
                 public | owner_t9  | table | owner_role_01

                $ psql-execute command="\\di owner_i*"
                \\                   List of relations
                 Schema |   Name    | Type  |     Owner     |   Table
                --------+-----------+-------+---------------+-----------
                 public | owner_i1  | index | owner_role_01 | owner_t1
                 public | owner_i10 | index | owner_role_02 | owner_t10
                 public | owner_i11 | index | owner_role_03 | owner_t11
                 public | owner_i2  | index | other_owner   | owner_t2
                 public | owner_i3  | index | owner_role_01 | owner_t3
                 public | owner_i4  | index | other_owner   | owner_t4
                 public | owner_i5  | index | owner_role_01 | owner_t5
                 public | owner_i6  | index | other_owner   | owner_t6
                 public | owner_i7  | index | owner_role_02 | owner_t7
                 public | owner_i8  | index | other_owner   | owner_t8
                 public | owner_i9  | index | owner_role_01 | owner_t9

                $ psql-execute command="\\dv owner_v*"
                \\             List of relations
                 Schema |   Name    | Type |     Owner
                --------+-----------+------+---------------
                 public | owner_v1  | view | owner_role_01
                 public | owner_v10 | view | owner_role_02
                 public | owner_v11 | view | owner_role_03
                 public | owner_v2  | view | other_owner
                 public | owner_v3  | view | owner_role_01
                 public | owner_v4  | view | other_owner
                 public | owner_v5  | view | owner_role_01
                 public | owner_v6  | view | other_owner
                 public | owner_v7  | view | owner_role_02
                 public | owner_v8  | view | other_owner
                 public | owner_v9  | view | owner_role_01

                $ psql-execute command="\\dmv owner_mv*"
                \\                    List of relations
                 Schema |    Name    |       Type        |     Owner
                --------+------------+-------------------+---------------
                 public | owner_mv1  | materialized view | owner_role_01
                 public | owner_mv10 | materialized view | owner_role_02
                 public | owner_mv11 | materialized view | owner_role_03
                 public | owner_mv2  | materialized view | other_owner
                 public | owner_mv3  | materialized view | owner_role_01
                 public | owner_mv4  | materialized view | other_owner
                 public | owner_mv5  | materialized view | owner_role_01
                 public | owner_mv6  | materialized view | other_owner
                 public | owner_mv7  | materialized view | owner_role_02
                 public | owner_mv8  | materialized view | other_owner
                 public | owner_mv9  | materialized view | owner_role_01

                > SELECT mz_types.name, mz_roles.name FROM mz_types JOIN mz_roles ON mz_types.owner_id = mz_roles.id WHERE mz_types.name LIKE 'owner_type%'
                owner_type1  owner_role_01
                owner_type10 owner_role_02
                owner_type11 owner_role_03
                owner_type2  other_owner
                owner_type3  owner_role_01
                owner_type4  other_owner
                owner_type5  owner_role_01
                owner_type6  other_owner
                owner_type7  owner_role_02
                owner_type8  other_owner
                owner_type9  owner_role_01

                > SELECT mz_secrets.name, mz_roles.name FROM mz_secrets JOIN mz_roles ON mz_secrets.owner_id = mz_roles.id WHERE mz_secrets.name LIKE 'owner_secret%'
                owner_secret1  owner_role_01
                owner_secret10 owner_role_02
                owner_secret11 owner_role_03
                owner_secret2  other_owner
                owner_secret3  owner_role_01
                owner_secret4  other_owner
                owner_secret5  owner_role_01
                owner_secret6  other_owner
                owner_secret7  owner_role_02
                owner_secret8  other_owner
                owner_secret9  owner_role_01

                > SELECT mz_sources.name, mz_roles.name FROM mz_sources JOIN mz_roles ON mz_sources.owner_id = mz_roles.id WHERE mz_sources.name LIKE 'owner_source%' AND type = 'load-generator'
                owner_source1 owner_role_01
                owner_source2 other_owner

                > SELECT mz_sinks.name, mz_roles.name FROM mz_sinks JOIN mz_roles ON mz_sinks.owner_id = mz_roles.id WHERE mz_sinks.name LIKE 'owner_sink%'
                owner_sink1 owner_role_01
                owner_sink2 other_owner

                > SELECT mz_clusters.name, mz_roles.name FROM mz_clusters JOIN mz_roles ON mz_clusters.owner_id = mz_roles.id WHERE mz_clusters.name LIKE 'owner_cluster%'
                owner_cluster1 owner_role_01
                owner_cluster2 other_owner

                > SELECT mz_cluster_replicas.name, mz_roles.name FROM mz_cluster_replicas JOIN mz_roles ON mz_cluster_replicas.owner_id = mz_roles.id WHERE mz_cluster_replicas.name LIKE 'owner_cluster_r%'
                owner_cluster_r1 owner_role_01
                owner_cluster_r2 other_owner

                > SELECT mz_connections.name, mz_roles.name FROM mz_connections JOIN mz_roles ON mz_connections.owner_id = mz_roles.id WHERE mz_connections.name LIKE 'owner_%'
                owner_csr_conn1  owner_role_01
                owner_csr_conn10 owner_role_02
                owner_csr_conn11 owner_role_03
                owner_csr_conn2  other_owner
                owner_csr_conn3  owner_role_01
                owner_csr_conn4  other_owner
                owner_csr_conn5  owner_role_01
                owner_csr_conn6  other_owner
                owner_csr_conn7  owner_role_02
                owner_csr_conn8  other_owner
                owner_csr_conn9  owner_role_01
                owner_kafka_conn1  owner_role_01
                owner_kafka_conn10 owner_role_02
                owner_kafka_conn11 owner_role_03
                owner_kafka_conn2  other_owner
                owner_kafka_conn3  owner_role_01
                owner_kafka_conn4  other_owner
                owner_kafka_conn5  owner_role_01
                owner_kafka_conn6  other_owner
                owner_kafka_conn7  owner_role_02
                owner_kafka_conn8  other_owner
                owner_kafka_conn9  owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_databases WHERE name LIKE 'owner_db%'
                owner_db1  owner_role_01=UC/owner_role_01
                owner_db10 owner_role_02=UC/owner_role_02
                owner_db11 owner_role_03=UC/owner_role_03
                owner_db2  other_owner=UC/other_owner
                owner_db3  owner_role_01=UC/owner_role_01
                owner_db4  other_owner=UC/other_owner
                owner_db5  owner_role_01=UC/owner_role_01
                owner_db6  other_owner=UC/other_owner
                owner_db7  owner_role_02=UC/owner_role_02
                owner_db8  other_owner=UC/other_owner
                owner_db9  owner_role_01=UC/owner_role_01
                owner_db1  mz_support=U/owner_role_01
                owner_db10 mz_support=U/owner_role_02
                owner_db11 mz_support=U/owner_role_03
                owner_db2  mz_support=U/other_owner
                owner_db3  mz_support=U/owner_role_01
                owner_db4  mz_support=U/other_owner
                owner_db5  mz_support=U/owner_role_01
                owner_db6  mz_support=U/other_owner
                owner_db7  mz_support=U/owner_role_02
                owner_db8  mz_support=U/other_owner
                owner_db9  mz_support=U/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_schemas WHERE name LIKE 'owner_schema%'
                owner_schema1  owner_role_01=UC/owner_role_01
                owner_schema10 owner_role_02=UC/owner_role_02
                owner_schema11 owner_role_03=UC/owner_role_03
                owner_schema2  other_owner=UC/other_owner
                owner_schema3  owner_role_01=UC/owner_role_01
                owner_schema4  other_owner=UC/other_owner
                owner_schema5  owner_role_01=UC/owner_role_01
                owner_schema6  other_owner=UC/other_owner
                owner_schema7  owner_role_02=UC/owner_role_02
                owner_schema8  other_owner=UC/other_owner
                owner_schema9  owner_role_01=UC/owner_role_01
                owner_schema1  mz_support=U/owner_role_01
                owner_schema10 mz_support=U/owner_role_02
                owner_schema11 mz_support=U/owner_role_03
                owner_schema2  mz_support=U/other_owner
                owner_schema3  mz_support=U/owner_role_01
                owner_schema4  mz_support=U/other_owner
                owner_schema5  mz_support=U/owner_role_01
                owner_schema6  mz_support=U/other_owner
                owner_schema7  mz_support=U/owner_role_02
                owner_schema8  mz_support=U/other_owner
                owner_schema9  mz_support=U/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_tables WHERE name LIKE 'owner_t%'
                owner_t1  owner_role_01=arwd/owner_role_01
                owner_t10 owner_role_02=arwd/owner_role_02
                owner_t11 owner_role_03=arwd/owner_role_03
                owner_t2  other_owner=arwd/other_owner
                owner_t3  owner_role_01=arwd/owner_role_01
                owner_t4  other_owner=arwd/other_owner
                owner_t5  owner_role_01=arwd/owner_role_01
                owner_t6  other_owner=arwd/other_owner
                owner_t7  owner_role_02=arwd/owner_role_02
                owner_t8  other_owner=arwd/other_owner
                owner_t9  owner_role_01=arwd/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_views WHERE name LIKE 'owner_v%'
                owner_v1  owner_role_01=r/owner_role_01
                owner_v10 owner_role_02=r/owner_role_02
                owner_v11 owner_role_03=r/owner_role_03
                owner_v2  other_owner=r/other_owner
                owner_v3  owner_role_01=r/owner_role_01
                owner_v4  other_owner=r/other_owner
                owner_v5  owner_role_01=r/owner_role_01
                owner_v6  other_owner=r/other_owner
                owner_v7  owner_role_02=r/owner_role_02
                owner_v8  other_owner=r/other_owner
                owner_v9  owner_role_01=r/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_materialized_views WHERE name LIKE 'owner_mv%'
                owner_mv1  owner_role_01=r/owner_role_01
                owner_mv10 owner_role_02=r/owner_role_02
                owner_mv11 owner_role_03=r/owner_role_03
                owner_mv2  other_owner=r/other_owner
                owner_mv3  owner_role_01=r/owner_role_01
                owner_mv4  other_owner=r/other_owner
                owner_mv5  owner_role_01=r/owner_role_01
                owner_mv6  other_owner=r/other_owner
                owner_mv7  owner_role_02=r/owner_role_02
                owner_mv8  other_owner=r/other_owner
                owner_mv9  owner_role_01=r/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_types WHERE name LIKE 'owner_type%'
                owner_type1  =U/owner_role_01
                owner_type1  owner_role_01=U/owner_role_01
                owner_type10 =U/owner_role_02
                owner_type10 owner_role_02=U/owner_role_02
                owner_type11 =U/owner_role_03
                owner_type11 owner_role_03=U/owner_role_03
                owner_type2  =U/other_owner
                owner_type2  other_owner=U/other_owner
                owner_type3  =U/owner_role_01
                owner_type3  owner_role_01=U/owner_role_01
                owner_type4  =U/other_owner
                owner_type4  other_owner=U/other_owner
                owner_type5  =U/owner_role_01
                owner_type5  owner_role_01=U/owner_role_01
                owner_type6  =U/other_owner
                owner_type6  other_owner=U/other_owner
                owner_type7  =U/owner_role_02
                owner_type7  owner_role_02=U/owner_role_02
                owner_type8  =U/other_owner
                owner_type8  other_owner=U/other_owner
                owner_type9  =U/owner_role_01
                owner_type9  owner_role_01=U/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_secrets WHERE name LIKE 'owner_secret%'
                owner_secret1  owner_role_01=U/owner_role_01
                owner_secret10 owner_role_02=U/owner_role_02
                owner_secret11 owner_role_03=U/owner_role_03
                owner_secret2  other_owner=U/other_owner
                owner_secret3  owner_role_01=U/owner_role_01
                owner_secret4  other_owner=U/other_owner
                owner_secret5  owner_role_01=U/owner_role_01
                owner_secret6  other_owner=U/other_owner
                owner_secret7  owner_role_02=U/owner_role_02
                owner_secret8  other_owner=U/other_owner
                owner_secret9  owner_role_01=U/owner_role_01

                > SELECT name, unnest(privileges)::text FROM mz_sources WHERE name LIKE 'owner_source%' AND type = 'load-generator'
                owner_source1 owner_role_01=r/owner_role_01
                owner_source2 other_owner=r/other_owner

                ! SELECT name, unnest(privileges)::text FROM mz_sinks WHERE name LIKE 'owner_sink%'
                contains: column "privileges" does not exist

                > SELECT name, unnest(privileges)::text FROM mz_clusters WHERE name LIKE 'owner_cluster%'
                owner_cluster1 mz_support=U/owner_role_01
                owner_cluster1 owner_role_01=UC/owner_role_01
                owner_cluster2 mz_support=U/other_owner
                owner_cluster2 other_owner=UC/other_owner

                > SELECT name, unnest(privileges)::text FROM mz_connections WHERE name LIKE 'owner_%'
                owner_csr_conn1  owner_role_01=U/owner_role_01
                owner_csr_conn10 owner_role_02=U/owner_role_02
                owner_csr_conn11 owner_role_03=U/owner_role_03
                owner_csr_conn2  other_owner=U/other_owner
                owner_csr_conn3  owner_role_01=U/owner_role_01
                owner_csr_conn4  other_owner=U/other_owner
                owner_csr_conn5  owner_role_01=U/owner_role_01
                owner_csr_conn6  other_owner=U/other_owner
                owner_csr_conn7  owner_role_02=U/owner_role_02
                owner_csr_conn8  other_owner=U/other_owner
                owner_csr_conn9  owner_role_01=U/owner_role_01
                owner_kafka_conn1  owner_role_01=U/owner_role_01
                owner_kafka_conn10 owner_role_02=U/owner_role_02
                owner_kafka_conn11 owner_role_03=U/owner_role_03
                owner_kafka_conn2  other_owner=U/other_owner
                owner_kafka_conn3  owner_role_01=U/owner_role_01
                owner_kafka_conn4  other_owner=U/other_owner
                owner_kafka_conn5  owner_role_01=U/owner_role_01
                owner_kafka_conn6  other_owner=U/other_owner
                owner_kafka_conn7  owner_role_02=U/owner_role_02
                owner_kafka_conn8  other_owner=U/other_owner
                owner_kafka_conn9  owner_role_01=U/owner_role_01
                """
            )
            + self._drop_objects("owner_role_01", 9)
            + self._drop_objects("owner_role_02", 10)
            + self._drop_objects("owner_role_03", 11)
        )
