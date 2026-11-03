// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "../db";
import {
  createWebhookSourceViewStatement,
  extractKeyPaths,
} from "./createWebhookSourceView";
import { Source } from "./sourceList";

const SOURCE = {
  name: "webhook_source",
  schemaName: "public",
  databaseName: "materialize",
} as Source;

const buildViewSql = (sample: object) =>
  createWebhookSourceViewStatement(
    extractKeyPaths(sample),
    "v",
    SOURCE,
    "view",
  ).compile(queryBuilder).sql;

describe("createWebhookSourceViewStatement", () => {
  it("should address array elements by index", () => {
    expect(buildViewSql({ items: [{ id: 1 }] })).toContain(
      `"body"->'items'->0->>'id'`,
    );
  });

  it("should treat numeric-looking object keys as text keys", () => {
    const viewSql = buildViewSql({ "-1": "a", "1e3": { b: "c" } });
    expect(viewSql).toContain(`"body"->>'-1'`);
    expect(viewSql).toContain(`"body"->'1e3'->>'b'`);
  });
});
