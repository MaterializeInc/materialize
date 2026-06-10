// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildSubscribeQuery } from "./buildSubscribeQuery";
import { queryBuilder } from "./db";

const selectQuery = queryBuilder.selectFrom("mz_objects").select("id");

describe("buildSubscribeQuery", () => {
  it("with simple upsert key", () => {
    const result = buildSubscribeQuery(selectQuery, {
      upsertKey: "id",
    }).compile(queryBuilder);
    expect(result.sql).toEqual(
      `SUBSCRIBE ((select "id" from "mz_objects")) WITH (PROGRESS) ENVELOPE UPSERT (KEY ("id"));`,
    );
  });

  it("with compound upsert key", () => {
    const result = buildSubscribeQuery(selectQuery, {
      upsertKey: ["id", "process_id"],
    }).compile(queryBuilder);
    expect(result.sql).toEqual(
      `SUBSCRIBE ((select "id" from "mz_objects")) WITH (PROGRESS) ENVELOPE UPSERT (KEY ("id", "process_id"));`,
    );
  });

  it("with as of", () => {
    const asOfAtLeast = new Date("1/1/2024");
    const result = buildSubscribeQuery(selectQuery, {
      upsertKey: "id",
      asOfAtLeast,
    }).compile(queryBuilder);
    expect(result.sql).toEqual(
      `SUBSCRIBE ((select "id" from "mz_objects")) WITH (PROGRESS) AS OF AT LEAST TIMESTAMP '${asOfAtLeast.toISOString()}' ENVELOPE UPSERT (KEY ("id"));`,
    );
  });
});
