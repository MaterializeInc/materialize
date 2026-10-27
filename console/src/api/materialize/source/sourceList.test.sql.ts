// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildSourceListQuery } from "~/api/materialize/source/sourceList";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

describe("buildSourceListQuery", () => {
  it("with no filters", async () => {
    await testdrive(
      `> CREATE SOURCE webhook_text FROM WEBHOOK BODY FORMAT TEXT;`,
    );
    const query = buildSourceListQuery({ filters: {} }).compile();
    const result = await executeSqlHttp(query);
    expect(result.rows).toEqual([
      expect.objectContaining({
        clusterId: "u1",
        clusterName: "quickstart",
        connectionId: null,
        connectionName: null,
        createdAt: expect.any(Date),
        databaseName: "materialize",
        error: null,
        id: expect.stringMatching("^u"),
        isOwner: true,
        kafkaTopic: null,
        webhookUrl: expect.stringContaining("webhook_text"),
        name: "webhook_text",
        schemaName: "public",
        size: null,
        status: "running",
        type: "webhook",
        snapshotCommitted: null,
      }),
    ]);
  });

  it("with all filters", async () => {
    await testdrive(
      `
      $ postgres-execute connection=postgres://postgres:postgres@postgres
      ALTER USER postgres WITH replication
      DROP SCHEMA IF EXISTS public CASCADE
      CREATE SCHEMA public
      CREATE TABLE t (c INTEGER)
      ALTER TABLE t REPLICA IDENTITY FULL
      DROP PUBLICATION IF EXISTS mz_source
      CREATE PUBLICATION mz_source FOR ALL TABLES

      > CREATE SOURCE wrong_database FROM WEBHOOK BODY FORMAT TEXT
      > CREATE database target
      > SET database to target
      > CREATE SOURCE wrong_schema FROM WEBHOOK BODY FORMAT TEXT
      > CREATE schema target
      > SET schema to target
      > CREATE SOURCE webhook_text FROM WEBHOOK BODY FORMAT TEXT
      > CREATE SOURCE webhook_oops FROM WEBHOOK BODY FORMAT TEXT
      > CREATE SECRET pgpass AS 'postgres'
      > CREATE CONNECTION pg TO POSTGRES (
            HOST postgres,
            DATABASE postgres,
            USER postgres,
            PASSWORD SECRET pgpass
        )
      > CREATE SOURCE pg_text_source
        FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
        FOR ALL TABLES
      `,
    );
    const client = await getMaterializeClient();

    // Find webhook source
    const { rows: databases } = await client.query(
      "select id, name from mz_databases order by name desc",
    );
    const { rows: schemas } = await client.query(
      "select id, name from mz_schemas order by name desc",
    );

    const webhookDatabase = databases.find((d) => d.name === "target");
    const webhookSchema = schemas.find((s) => s.name === "target");
    const webhookQuery = buildSourceListQuery({
      filters: {
        databaseId: webhookDatabase.id,
        schemaId: webhookSchema.id,
        nameFilter: "text",
        type: "webhook",
      },
    }).compile();
    const webhookResult = await executeSqlHttp(webhookQuery);
    expect(webhookResult.rows).toHaveLength(1);
    expect(webhookResult.rows[0]).toEqual(
      expect.objectContaining({
        databaseName: "target",
        name: "webhook_text",
        schemaName: "target",
      }),
    );

    // Find postgres source
    const postgresQuery = buildSourceListQuery({
      filters: {
        nameFilter: "pg_text_source",
        type: "postgres",
      },
    }).compile();
    const postgresResult = await executeSqlHttp(postgresQuery);
    expect(postgresResult.rows).toHaveLength(1);

    expect(postgresResult.rows[0]).toEqual(
      expect.objectContaining({
        name: "pg_text_source",
      }),
    );
  });

  it("includes snapshot status for sources with snapshotting", async () => {
    await testdrive(
      `
      $ postgres-execute connection=postgres://postgres:postgres@postgres
      ALTER USER postgres WITH replication
      DROP SCHEMA IF EXISTS public CASCADE
      CREATE SCHEMA public
      CREATE TABLE snapshot_test (id INTEGER, data TEXT)
      INSERT INTO snapshot_test VALUES (1, 'test')
      ALTER TABLE snapshot_test REPLICA IDENTITY FULL
      DROP PUBLICATION IF EXISTS mz_source
      CREATE PUBLICATION mz_source FOR ALL TABLES

      > CREATE SECRET snapshot_pgpass AS 'postgres'
      > CREATE CONNECTION snapshot_pg_conn TO POSTGRES (
            HOST postgres,
            DATABASE postgres,
            USER postgres,
            PASSWORD SECRET snapshot_pgpass
        )
      > CREATE SOURCE snapshot_pg_source
        FROM POSTGRES CONNECTION snapshot_pg_conn (PUBLICATION 'mz_source')
        FOR ALL TABLES
      `,
    );

    const query = buildSourceListQuery({
      filters: {
        nameFilter: "snapshot_pg_source",
      },
    }).compile();
    const result = await executeSqlHttp(query);

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      name: "snapshot_pg_source",
      type: "postgres",
    });

    // snapshotCommitted should be present in the result
    // It will be null if statistics haven't been collected yet, or a boolean if they have
    expect(result.rows[0]).toHaveProperty("snapshotCommitted");
    expect([null, true, false]).toContain(result.rows[0].snapshotCommitted);
  });
});
