// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize/db";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { waitFor } from "~/test/sql/waitFor";

import { buildWorkflowGraphQuery } from "./workflowGraph";

describe("createPostgresSourceStatement", () => {
  it("returns an array of graph edges", async () => {
    await testdrive(
      `
        $ postgres-execute connection=postgres://postgres:postgres@postgres
        ALTER USER postgres WITH replication;
        DROP SCHEMA IF EXISTS public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE t (c INTEGER);
        ALTER TABLE t REPLICA IDENTITY FULL;
        DROP PUBLICATION IF EXISTS mz_source;
        CREATE PUBLICATION mz_source FOR ALL TABLES;

        $ kafka-create-topic topic=sink-topic

        > CREATE SECRET pgpass AS 'postgres'
        > CREATE CONNECTION pg TO POSTGRES (
            HOST postgres,
            DATABASE postgres,
            USER postgres,
            PASSWORD SECRET pgpass
          )
        > CREATE SOURCE pg_source
          FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
          FOR ALL TABLES
        > CREATE VIEW t_view AS select * from t;
        > CREATE MATERIALIZED VIEW t_mv AS
          SELECT c, count(c) from t_view
          GROUP BY c
        > CREATE INDEX t_ind ON t_view (c)

        > CREATE CONNECTION kafka_conn
          TO KAFKA (BROKER '\${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

        > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
            URL '\${testdrive.schema-registry-url}'
          )

        > CREATE SINK t_sink FROM t_mv
          INTO KAFKA CONNECTION kafka_conn (TOPIC 'sink-topic')
          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
          ENVELOPE DEBEZIUM
      `,
    );

    const client = await getMaterializeClient();
    const {
      rows: [source],
    } = await client.query(
      "select id from mz_sources where name = 'pg_source'",
    );
    const {
      rows: [subsource],
    } = await client.query("select id from mz_sources where name = 't'");
    const {
      rows: [matView],
    } = await client.query(
      "select id from mz_materialized_views where name = 't_mv'",
    );
    const {
      rows: [index],
    } = await client.query("select id from mz_indexes where name = 't_ind'");

    const {
      rows: [sink],
    } = await client.query("select id from mz_sinks where name = 't_sink'");
    const query = buildWorkflowGraphQuery({
      objectId: source.id,
    }).compile(queryBuilder);
    // Compute dependencies are not updated transactionally, so sometimes we have to wait
    // a bit for all the deps to show up.
    await waitFor(async () => {
      const result = await executeSqlHttp(query);
      expect(result.rows).toEqual([
        { parentId: source.id, childId: subsource.id },
        { parentId: subsource.id, childId: matView.id },
        { parentId: subsource.id, childId: index.id },
        { parentId: matView.id, childId: sink.id },
      ]);
    });
  });
});
