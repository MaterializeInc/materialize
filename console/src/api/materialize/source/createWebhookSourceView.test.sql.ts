// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { queryBuilder } from "../db";
import { createWebhookSourceStatement } from "./createWebhookSourceStatement";
import {
  createWebhookSourceViewStatement,
  extractKeyPaths,
} from "./createWebhookSourceView";
import { buildSourceListQuery, Source } from "./sourceList";

async function createSource(
  clusterId: string,
  clusterName: string,
): Promise<Source> {
  const sourceQuery = createWebhookSourceStatement({
    name: "webhook_source",
    databaseName: "materialize",
    schemaName: "public",
    cluster: { id: clusterId, name: clusterName },
    headerBehavior: "all",
    headers: [],
    bodyFormat: "json",
    validateRequests: false,
    checkStatement: "",
  }).compile(queryBuilder);
  await executeSqlHttp(sourceQuery);
  const result = await executeSqlHttp(
    buildSourceListQuery({ filters: { type: "webhook" } }).compile(),
  );
  return result.rows[0];
}

describe("createWebhookSourceView", () => {
  it("Creates a view for a webhook source with a simple object", async () => {
    await testdrive(`> CREATE CLUSTER c (SIZE = 'scale=1,workers=1')`);
    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const source = await createSource(cluster.id, "c");
    const sampleObject = { key: "1" };
    const columns = extractKeyPaths(sampleObject);
    const query = createWebhookSourceViewStatement(
      columns,
      "test_view",
      source,
      "view",
    ).compile(queryBuilder);
    await executeSqlHttp(query);
    const {
      rows: [ddl],
    } = await client.query("show create view public.test_view");
    expect(ddl.create_sql).toContain("CREATE VIEW");
    expect(ddl.create_sql).toContain("test_view");
    expect(ddl.create_sql).toContain("body ->> 'key' AS key");
    expect(ddl.create_sql).toContain("webhook_source");
  });

  it("Creates a view for a webhook source with an array", async () => {
    await testdrive(`> CREATE CLUSTER c (SIZE = 'scale=1,workers=1')`);
    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const source = await createSource(cluster.id, "c");
    const sampleObject = { arr: [1, 10] };
    const columns = extractKeyPaths(sampleObject);
    const query = createWebhookSourceViewStatement(
      columns,
      "test_view",
      source,
      "view",
    ).compile(queryBuilder);
    await executeSqlHttp(query);
    const {
      rows: [ddl],
    } = await client.query("show create view public.test_view");
    expect(ddl.create_sql).toContain("CREATE VIEW");
    expect(ddl.create_sql).toContain("test_view");
    expect(ddl.create_sql).toContain("body -> 'arr' -> 0");
    expect(ddl.create_sql).toContain("body -> 'arr' -> 1");
    expect(ddl.create_sql).toContain("::pg_catalog.numeric");
    expect(ddl.create_sql).toContain("arr_0");
    expect(ddl.create_sql).toContain("arr_1");
    expect(ddl.create_sql).toContain("webhook_source");
  });

  it("Creates a view for a webhook source with a timestamp", async () => {
    await testdrive(`> CREATE CLUSTER c (SIZE = 'scale=1,workers=1')`);
    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const source = await createSource(cluster.id, "c");
    const sampleObject = { timestamp: "2024-01-13T03:59:20.978Z" };
    const columns = extractKeyPaths(sampleObject);
    const query = createWebhookSourceViewStatement(
      columns,
      "test_view",
      source,
      "view",
    ).compile(queryBuilder);
    await executeSqlHttp(query);
    const {
      rows: [ddl],
    } = await client.query("show create view public.test_view");
    expect(ddl.create_sql).toContain("CREATE VIEW");
    expect(ddl.create_sql).toContain("test_view");
    expect(ddl.create_sql).toContain("try_parse_monotonic_iso8601_timestamp");
    expect(ddl.create_sql).toContain("body ->> 'timestamp'");
    expect(ddl.create_sql).toContain("timestamp");
    expect(ddl.create_sql).toContain("webhook_source");
  });
});
