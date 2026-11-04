// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import { HydrationAggregateRow } from "~/api/materialize/maintained-objects/hydrationAggregate";
import { DatabaseObject } from "~/api/materialize/objects";

import { buildMaintainedObjectsList } from "./queries";

const buildObject = (overrides: Partial<DatabaseObject>): DatabaseObject => ({
  id: "u1",
  name: "object",
  objectType: "materialized-view",
  schemaId: "u3",
  schemaName: "public",
  databaseId: "u1",
  databaseName: "materialize",
  sourceType: null,
  isWebhookTable: null,
  clusterId: null,
  clusterName: null,
  ...overrides,
});

const buildHydration = (
  overrides: Partial<HydrationAggregateRow>,
): HydrationAggregateRow => ({
  object_id: "u1",
  hydratedReplicas: null,
  totalReplicas: null,
  sourceStatus: "running",
  sourceError: null,
  snapshotCommitted: true,
  parentSourceId: null,
  ...overrides,
});

const pgSource = buildObject({
  id: "u10",
  name: "pg_src",
  objectType: "source",
  sourceType: "postgres",
  clusterId: "u2",
  clusterName: "ingest",
});
const subsource = buildObject({
  id: "u11",
  name: "customers",
  objectType: "source",
  sourceType: "subsource",
});
const sourceTable = buildObject({
  id: "u12",
  name: "orders",
  objectType: "table",
});
const progress = buildObject({
  id: "u13",
  name: "pg_src_progress",
  objectType: "source",
  sourceType: "progress",
});
const writtenTable = buildObject({
  id: "u14",
  name: "written",
  objectType: "table",
});

describe("buildMaintainedObjectsList", () => {
  it("lists subsources and source-fed tables on their source's cluster", () => {
    const rows = buildMaintainedObjectsList({
      objects: [pgSource, subsource, sourceTable],
      lagRows: [{ object_id: "u12", lag: parse("00:01:05") }],
      hydrationRows: [
        buildHydration({ object_id: "u10" }),
        buildHydration({ object_id: "u11", parentSourceId: "u10" }),
        buildHydration({
          object_id: "u12",
          parentSourceId: "u10",
          snapshotCommitted: false,
        }),
      ],
    });

    const byName = new Map(rows.map((r) => [r.name, r]));
    expect(byName.get("customers")).toMatchObject({
      objectType: "subsource",
      sourceType: "postgres",
      parentSource: { id: "u10", name: "pg_src" },
      cluster: { id: "u2", name: "ingest" },
    });
    expect(byName.get("orders")).toMatchObject({
      objectType: "table",
      sourceType: "postgres",
      parentSource: { id: "u10", name: "pg_src" },
      cluster: { id: "u2", name: "ingest" },
      lag: { ms: 65_000 },
      sourceStatus: { status: "running", snapshotCommitted: false },
    });
    expect(byName.get("pg_src")).toMatchObject({
      objectType: "source",
      parentSource: null,
    });
  });

  it("hides progress collections and tables written with INSERT", () => {
    const rows = buildMaintainedObjectsList({
      objects: [pgSource, progress, writtenTable],
      lagRows: [],
      hydrationRows: [buildHydration({ object_id: "u10" })],
    });

    expect(rows.map((r) => r.name)).toEqual(["pg_src"]);
  });

  it("lists a subsource before its parent source is known", () => {
    const rows = buildMaintainedObjectsList({
      objects: [pgSource, subsource],
      lagRows: [],
      hydrationRows: [],
    });

    expect(rows.find((r) => r.name === "customers")).toMatchObject({
      objectType: "subsource",
      parentSource: null,
      cluster: null,
    });
  });
});
