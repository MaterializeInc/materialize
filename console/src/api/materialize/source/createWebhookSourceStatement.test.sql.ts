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
import { buildSourceListQuery } from "./sourceList";

describe("createWebhookSourceStatement", () => {
  it("Creates a webhook without custom headers and without validation", async () => {
    await testdrive(`> CREATE CLUSTER c (SIZE = 'scale=1,workers=1')`);

    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const query = createWebhookSourceStatement({
      name: "webhook_source",
      databaseName: "materialize",
      schemaName: "public",
      cluster: { id: cluster.id, name: "c" },
      headerBehavior: "all",
      headers: [],
      bodyFormat: "json_array",
      validateRequests: false,
      checkStatement: "",
    }).compile(queryBuilder);
    await executeSqlHttp(query);
    const result = await executeSqlHttp(
      buildSourceListQuery({ filters: { type: "webhook" } }).compile(),
    );
    expect(result.rows).toEqual([
      expect.objectContaining({
        clusterId: cluster.id,
        clusterName: "c",
        connectionId: null,
        connectionName: null,
        createdAt: expect.any(Date),
        databaseName: "materialize",
        error: null,
        id: expect.stringMatching("^u"),
        isOwner: true,
        kafkaTopic: null,
        name: "webhook_source",
        schemaName: "public",
        size: null,
        status: "running",
        type: "webhook",
        webhookUrl:
          "https://host/api/webhook/materialize/public/webhook_source",
      }),
    ]);
  });

  it("Creates a webhook custom headers and with validation", async () => {
    await testdrive(`
      > CREATE CLUSTER c (SIZE = 'scale=1,workers=1')
      > CREATE SECRET my_secret AS 'sshhhhhhh'
    `);

    const client = await getMaterializeClient();
    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const query = createWebhookSourceStatement({
      name: "webhook_source",
      databaseName: "materialize",
      schemaName: "public",
      cluster: { id: cluster.id, name: "c" },
      headerBehavior: "include_specific",
      headers: [{ header: "x-foo", column: "foo" }],
      bodyFormat: "json_array",
      validateRequests: true,
      checkStatement: `
CHECK (
  WITH (
    HEADERS, BODY as request_body,
    SECRET "materialize"."public"."my_secret"
  )
  constant_time_eq(
    decode(headers->'x-signature', 'base64'),
    hmac(request_body, my_secret, 'sha256')
  )
)`,
    }).compile(queryBuilder);
    await executeSqlHttp(query);
    const result = await executeSqlHttp(
      buildSourceListQuery({ filters: { type: "webhook" } }).compile(),
    );
    expect(result.rows).toEqual([
      expect.objectContaining({
        clusterId: cluster.id,
        clusterName: "c",
        connectionId: null,
        connectionName: null,
        createdAt: expect.any(Date),
        databaseName: "materialize",
        error: null,
        id: expect.stringMatching("^u"),
        isOwner: true,
        kafkaTopic: null,
        name: "webhook_source",
        schemaName: "public",
        size: null,
        status: "running",
        type: "webhook",
        webhookUrl:
          "https://host/api/webhook/materialize/public/webhook_source",
      }),
    ]);
  });
});
