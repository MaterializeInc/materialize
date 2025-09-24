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


class Privileges(Check):
    def _create_objects(self, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
            $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
            CREATE DATABASE privilege_db{i}
            CREATE SCHEMA privilege_schema{i}
            CREATE CONNECTION privilege_kafka_conn{i} FOR KAFKA {self._kafka_broker()}
            CREATE CONNECTION privilege_csr_conn{i} FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}'
            CREATE TYPE privilege_type{i} AS LIST (ELEMENT TYPE = text)
            CREATE TABLE privilege_t{i} (c1 int, c2 privilege_type{i})
            CREATE INDEX privilege_i{i} ON privilege_t{i} (c2)
            CREATE VIEW privilege_v{i} AS SELECT * FROM privilege_t{i}
            CREATE MATERIALIZED VIEW privilege_mv{i} AS SELECT * FROM privilege_t{i}
            CREATE SECRET privilege_secret{i} AS 'MY_SECRET'
            """
        )
        if expensive:
            s += dedent(
                f"""
                $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
                CREATE SOURCE privilege_source{i} FROM LOAD GENERATOR COUNTER
                $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
                CREATE SINK privilege_sink{i} FROM privilege_mv{i} INTO KAFKA CONNECTION privilege_kafka_conn{i} (TOPIC 'sink-sink-privilege{i}') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION privilege_csr_conn{i} ENVELOPE DEBEZIUM
                CREATE CLUSTER privilege_cluster{i} REPLICAS (privilege_cluster_r{i} (SIZE 'scale=1,workers=4'))
                """
            )

        return s

    def _grant_privileges(self, role: str, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
            $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
            GRANT ALL PRIVILEGES ON DATABASE privilege_db{i} TO {role}
            GRANT ALL PRIVILEGES ON SCHEMA privilege_schema{i} TO {role}
            GRANT ALL PRIVILEGES ON CONNECTION privilege_kafka_conn{i} TO {role}
            GRANT ALL PRIVILEGES ON CONNECTION privilege_csr_conn{i} TO {role}
            GRANT ALL PRIVILEGES ON TYPE privilege_type{i} TO {role}
            GRANT ALL PRIVILEGES ON TABLE privilege_t{i} TO {role}
            GRANT ALL PRIVILEGES ON TABLE privilege_v{i} TO {role}
            GRANT ALL PRIVILEGES ON TABLE privilege_mv{i} TO {role}
            GRANT ALL PRIVILEGES ON SECRET privilege_secret{i} TO {role}
            """
        )
        if expensive:
            s += dedent(
                f"""
                GRANT ALL PRIVILEGES ON TABLE privilege_source{i} TO {role}
                GRANT ALL PRIVILEGES ON CLUSTER privilege_cluster{i} TO {role}
                """
            )

        return s

    def _revoke_privileges(self, role: str, i: int, expensive: bool = False) -> str:
        s = dedent(
            f"""
                $ postgres-execute connection=postgres://materialize@${{testdrive.materialize-sql-addr}}
                REVOKE ALL PRIVILEGES ON DATABASE privilege_db{i} FROM {role}
                REVOKE ALL PRIVILEGES ON SCHEMA privilege_schema{i} FROM {role}
                REVOKE ALL PRIVILEGES ON CONNECTION privilege_kafka_conn{i} FROM {role}
                REVOKE ALL PRIVILEGES ON CONNECTION privilege_csr_conn{i} FROM {role}
                REVOKE ALL PRIVILEGES ON TYPE privilege_type{i} FROM {role}
                REVOKE ALL PRIVILEGES ON TABLE privilege_t{i} FROM {role}
                REVOKE ALL PRIVILEGES ON TABLE privilege_v{i} FROM {role}
                REVOKE ALL PRIVILEGES ON TABLE privilege_mv{i} FROM {role}
                REVOKE ALL PRIVILEGES ON SECRET privilege_secret{i} FROM {role}
                """
        )
        if expensive:
            s += dedent(
                f"""
                    REVOKE ALL PRIVILEGES ON TABLE privilege_source{i} FROM {role}
                    REVOKE ALL PRIVILEGES ON CLUSTER privilege_cluster{i} FROM {role}
                    """
            )

        return s

    def _drop_objects(
        self, i: int, expensive: bool = False, success: bool = True
    ) -> str:
        cmds = []
        # Drop the sink first so we can drop the materialized view without CASCADE.
        if expensive:
            cmds += [
                f"DROP SOURCE privilege_source{i}",
                f"DROP SINK privilege_sink{i}",
                f"DROP CLUSTER privilege_cluster{i}",
            ]
        cmds += [
            f"DROP SECRET privilege_secret{i}",
            f"DROP MATERIALIZED VIEW privilege_mv{i}",
            f"DROP VIEW privilege_v{i}",
            f"DROP INDEX privilege_i{i}",
            f"DROP TABLE privilege_t{i}",
            f"DROP TYPE privilege_type{i}",
            f"DROP CONNECTION privilege_csr_conn{i}",
            f"DROP CONNECTION privilege_kafka_conn{i}",
            f"DROP SCHEMA privilege_schema{i}",
            f"DROP DATABASE privilege_db{i}",
        ]
        return (
            "$ postgres-execute connection=postgres://materialize@${testdrive.materialize-sql-addr}/materialize\n"
            + "\n".join(cmds)
            + "\n"
        )

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEROLE ON SYSTEM TO materialize

                > CREATE ROLE role_1
                > CREATE ROLE role_2
                """
            )
            + self._create_objects(1, expensive=True)
            + self._grant_privileges("role_1", 1, expensive=True)
            + self._grant_privileges("role_2", 1, expensive=True)
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEROLE ON SYSTEM TO materialize
                    """
                )
                + self._revoke_privileges("role_2", 1, expensive=True)
                + self._create_objects(2)
                + self._grant_privileges("role_1", 2)
                + self._grant_privileges("role_2", 2),
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                    GRANT CREATEROLE ON SYSTEM TO materialize
                    """
                )
                + self._revoke_privileges("role_2", 2)
                + self._create_objects(3)
                + self._grant_privileges("role_1", 3)
                + self._grant_privileges("role_1", 3),
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            self._revoke_privileges("role_2", 3)
            + self._create_objects(4)
            + self._grant_privileges("role_1", 4)
            + self._grant_privileges("role_2", 4)
            + self._revoke_privileges("role_2", 4)
            + dedent(
                """
                > SELECT name, unnest(privileges)::text FROM mz_databases WHERE name LIKE 'privilege_db%'
                privilege_db1  materialize=UC/materialize
                privilege_db2  materialize=UC/materialize
                privilege_db3  materialize=UC/materialize
                privilege_db4  materialize=UC/materialize
                privilege_db1  mz_support=U/materialize
                privilege_db2  mz_support=U/materialize
                privilege_db3  mz_support=U/materialize
                privilege_db4  mz_support=U/materialize
                privilege_db1  role_1=UC/materialize
                privilege_db2  role_1=UC/materialize
                privilege_db3  role_1=UC/materialize
                privilege_db4  role_1=UC/materialize

                > SELECT name, unnest(privileges)::text FROM mz_schemas WHERE name LIKE 'privilege_schema%'
                privilege_schema1  materialize=UC/materialize
                privilege_schema2  materialize=UC/materialize
                privilege_schema3  materialize=UC/materialize
                privilege_schema4  materialize=UC/materialize
                privilege_schema1  mz_support=U/materialize
                privilege_schema2  mz_support=U/materialize
                privilege_schema3  mz_support=U/materialize
                privilege_schema4  mz_support=U/materialize
                privilege_schema1  role_1=UC/materialize
                privilege_schema2  role_1=UC/materialize
                privilege_schema3  role_1=UC/materialize
                privilege_schema4  role_1=UC/materialize

                > SELECT name, unnest(privileges)::text FROM mz_tables WHERE name LIKE 'privilege_t%'
                privilege_t1  materialize=arwd/materialize
                privilege_t2  materialize=arwd/materialize
                privilege_t3  materialize=arwd/materialize
                privilege_t4  materialize=arwd/materialize
                privilege_t1  role_1=arwd/materialize
                privilege_t2  role_1=arwd/materialize
                privilege_t3  role_1=arwd/materialize
                privilege_t4  role_1=arwd/materialize

                > SELECT name, unnest(privileges)::text FROM mz_views WHERE name LIKE 'privilege_v%'
                privilege_v1  materialize=r/materialize
                privilege_v2  materialize=r/materialize
                privilege_v3  materialize=r/materialize
                privilege_v4  materialize=r/materialize
                privilege_v1  role_1=r/materialize
                privilege_v2  role_1=r/materialize
                privilege_v3  role_1=r/materialize
                privilege_v4  role_1=r/materialize

                > SELECT name, unnest(privileges)::text FROM mz_materialized_views WHERE name LIKE 'privilege_mv%'
                privilege_mv1  materialize=r/materialize
                privilege_mv2  materialize=r/materialize
                privilege_mv3  materialize=r/materialize
                privilege_mv4  materialize=r/materialize
                privilege_mv1  role_1=r/materialize
                privilege_mv2  role_1=r/materialize
                privilege_mv3  role_1=r/materialize
                privilege_mv4  role_1=r/materialize

                > SELECT name, unnest(privileges)::text FROM mz_types WHERE name LIKE 'privilege_type%'
                privilege_type1  =U/materialize
                privilege_type1  materialize=U/materialize
                privilege_type1  role_1=U/materialize
                privilege_type2  =U/materialize
                privilege_type2  materialize=U/materialize
                privilege_type2  role_1=U/materialize
                privilege_type3  =U/materialize
                privilege_type3  materialize=U/materialize
                privilege_type3  role_1=U/materialize
                privilege_type4  =U/materialize
                privilege_type4  materialize=U/materialize
                privilege_type4  role_1=U/materialize

                > SELECT name, unnest(privileges)::text FROM mz_secrets WHERE name LIKE 'privilege_secret%'
                privilege_secret1  materialize=U/materialize
                privilege_secret2  materialize=U/materialize
                privilege_secret3  materialize=U/materialize
                privilege_secret4  materialize=U/materialize
                privilege_secret1  role_1=U/materialize
                privilege_secret2  role_1=U/materialize
                privilege_secret3  role_1=U/materialize
                privilege_secret4  role_1=U/materialize

                > SELECT name, unnest(privileges)::text FROM mz_sources WHERE name LIKE 'privilege_source%' AND type = 'load-generator'
                privilege_source1 materialize=r/materialize
                privilege_source1 role_1=r/materialize

                ! SELECT name, unnest(privileges)::text FROM mz_sinks WHERE name LIKE 'privilege_sink%'
                contains: column "privileges" does not exist

                > SELECT name, unnest(privileges)::text FROM mz_clusters WHERE name LIKE 'privilege_cluster%'
                privilege_cluster1 mz_support=U/materialize
                privilege_cluster1 materialize=UC/materialize
                privilege_cluster1 role_1=UC/materialize

                > SELECT name, unnest(privileges)::text FROM mz_connections WHERE name LIKE 'privilege_%'
                privilege_csr_conn1  materialize=U/materialize
                privilege_csr_conn2  materialize=U/materialize
                privilege_csr_conn3  materialize=U/materialize
                privilege_csr_conn4  materialize=U/materialize
                privilege_csr_conn1  role_1=U/materialize
                privilege_csr_conn2  role_1=U/materialize
                privilege_csr_conn3  role_1=U/materialize
                privilege_csr_conn4  role_1=U/materialize
                privilege_kafka_conn1  materialize=U/materialize
                privilege_kafka_conn2  materialize=U/materialize
                privilege_kafka_conn3  materialize=U/materialize
                privilege_kafka_conn4  materialize=U/materialize
                privilege_kafka_conn1  role_1=U/materialize
                privilege_kafka_conn2  role_1=U/materialize
                privilege_kafka_conn3  role_1=U/materialize
                privilege_kafka_conn4  role_1=U/materialize
                """
            )
            + self._drop_objects(4)
        )
