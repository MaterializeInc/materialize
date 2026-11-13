// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { buildCatalogGraph } from "./catalogGraph";
import { filterGraph, matchesSearch, neighborhood } from "./filterGraph";
import { formatLag, freshnessOf } from "./freshness";
import { buildJoinSql } from "./joinSql";
import { layoutGraph, maxCardsPerRow, NODE_HEIGHT, NODE_WIDTH } from "./layout";
import { OntologyEdge, OntologyGraph, OntologyNode } from "./model";

const node = (id: string, group = "g"): OntologyNode => ({
  id,
  label: id,
  group,
  kind: "table",
  relation: `"db"."${group}"."${id}"`,
  description: null,
  columns: [],
  primaryKey: [],
  objectId: id,
  databaseName: "db",
  schemaName: group,
  clusterName: null,
  isDataProduct: false,
  isUnresolved: false,
});

const edge = (
  source: string,
  target: string,
  kind: OntologyEdge["kind"] = "join",
): OntologyEdge => ({
  id: `${kind}|${source}|${target}`,
  source,
  target,
  kind,
  name: null,
  sourceColumns: kind === "lineage" ? [] : [`${target}_id`],
  targetColumns: kind === "lineage" ? [] : ["id"],
  description: null,
  cardinality: null,
  via: null,
  note: null,
  declaredIn: null,
  issues: [],
});

describe("buildCatalogGraph", () => {
  it("builds entities, typed links and column metadata", () => {
    const graph = buildCatalogGraph({
      entities: [
        {
          name: "cluster",
          relation: "mz_catalog.mz_clusters",
          properties: { primary_key: ["id"] },
          description: "A cluster",
        },
        {
          name: "replica",
          relation: "mz_catalog.mz_cluster_replicas",
          properties: null,
          description: "",
        },
      ],
      links: [
        {
          name: "belongs_to",
          sourceEntity: "replica",
          targetEntity: "cluster",
          properties: {
            kind: "foreign_key",
            source_column: "cluster_id",
            target_column: "id",
            cardinality: "many_to_one",
          },
          description: null,
        },
        {
          name: "dangling",
          sourceEntity: "replica",
          targetEntity: "nowhere",
          properties: { kind: "foreign_key" },
          description: null,
        },
        {
          name: "odd",
          sourceEntity: "replica",
          targetEntity: "cluster",
          properties: { kind: "made_up", requires_mapping: "mz_mapping" },
          description: null,
        },
      ],
      properties: [
        {
          entityType: "replica",
          columnName: "cluster_id",
          semanticType: "ClusterId",
          description: "The cluster.",
          type: "text",
        },
      ],
    });
    expect(graph.nodes.map((n) => [n.id, n.group, n.primaryKey])).toEqual([
      ["cluster", "mz_catalog", ["id"]],
      ["replica", "mz_catalog", []],
    ]);
    expect(graph.nodes[1].description).toBeNull();
    expect(graph.nodes[1].columns).toEqual([
      {
        name: "cluster_id",
        type: "text",
        description: "The cluster.",
        semanticType: "ClusterId",
      },
    ]);
    expect(graph.edges.map((e) => [e.kind, e.name, e.via])).toEqual([
      ["foreign_key", "belongs_to", null],
      ["other", "odd", "mz_mapping"],
    ]);
    expect(graph.edges[0]).toMatchObject({
      sourceColumns: ["cluster_id"],
      targetColumns: ["id"],
      cardinality: "many_to_one",
    });
  });
});

describe("filterGraph", () => {
  const graph: OntologyGraph = {
    nodes: [node("a", "x"), node("b", "x"), node("c", "y"), node("d", "y")],
    edges: [edge("a", "b"), edge("b", "c", "lineage")],
    issues: [
      {
        severity: "error",
        message: "broken",
        declaredIn: null,
        edgeId: edge("b", "c", "lineage").id,
      },
    ],
  };
  const all = {
    groups: null,
    hiddenEdgeKinds: new Set<OntologyEdge["kind"]>(),
    showIsolated: true,
    focus: null,
  };

  it("draws everything by default and counts isolated entities", () => {
    const filtered = filterGraph(graph, all);
    expect(filtered.nodes).toHaveLength(4);
    expect(filtered.edges).toHaveLength(2);
    expect(filtered.isolatedCount).toBe(1);
  });

  it("drops edges and issues that leave the drawn groups", () => {
    const filtered = filterGraph(graph, { ...all, groups: new Set(["x"]) });
    expect(filtered.nodes.map((n) => n.id)).toEqual(["a", "b"]);
    expect(filtered.edges.map((e) => e.id)).toEqual(["join|a|b"]);
    expect(filtered.issues).toEqual([]);
  });

  it("hides edge kinds and, on request, the entities they isolate", () => {
    const filtered = filterGraph(graph, {
      ...all,
      hiddenEdgeKinds: new Set(["lineage"]),
      showIsolated: false,
    });
    expect(filtered.nodes.map((n) => n.id)).toEqual(["a", "b"]);
    expect(filtered.isolatedCount).toBe(2);
  });

  it("focuses on a neighborhood across groups", () => {
    const filtered = filterGraph(graph, {
      ...all,
      groups: new Set(["x"]),
      focus: { nodeId: "b", depth: 1 },
    });
    expect(filtered.nodes.map((n) => n.id)).toEqual(["a", "b", "c"]);
  });

  it("walks neighborhoods in both directions", () => {
    expect([...neighborhood(graph.edges, "c", 2)].sort()).toEqual([
      "a",
      "b",
      "c",
    ]);
    expect([...neighborhood(graph.edges, "c", 0)]).toEqual(["c"]);
  });

  it("matches search on name, relation and description", () => {
    expect(matchesSearch(node("orders"), "ORD")).toBe(true);
    expect(matchesSearch(node("orders"), "  ")).toBe(false);
    expect(
      matchesSearch({ ...node("x"), description: "Live revenue" }, "revenue"),
    ).toBe(true);
  });
});

describe("buildJoinSql", () => {
  it("joins on positionally paired columns", () => {
    const sql = buildJoinSql(
      {
        ...edge("orders", "customers"),
        sourceColumns: ["customer_id", "region"],
        targetColumns: ["id", "region"],
      },
      node("orders"),
      node("customers"),
    );
    expect(sql).toBe(
      [
        "SELECT *",
        'FROM "db"."g"."orders" AS a',
        'JOIN "db"."g"."customers" AS b',
        '  ON a."customer_id" = b."id"',
        ' AND a."region" = b."region"',
        "LIMIT 100;",
      ].join("\n"),
    );
  });

  it("writes nothing for lineage or unpaired columns", () => {
    expect(
      buildJoinSql(edge("a", "b", "lineage"), node("a"), node("b")),
    ).toBeNull();
    expect(
      buildJoinSql(
        { ...edge("a", "b"), targetColumns: [] },
        node("a"),
        node("b"),
      ),
    ).toBeNull();
  });

  it("flags joins across id domains", () => {
    const sql = buildJoinSql(
      { ...edge("a", "b"), via: "mz_internal.mz_object_global_ids" },
      node("a"),
      node("b"),
    );
    expect(sql).toContain(
      "-- Bridge them through mz_internal.mz_object_global_ids",
    );
  });
});

describe("freshnessOf", () => {
  const now = 100_000;

  it("grades lag against the write frontier", () => {
    const at = (frontier: number) =>
      freshnessOf({ writeFrontier: frontier, hydrated: true }, "table", now)
        .state;
    expect(at(now - 500)).toBe("fresh");
    expect(at(now - 5_000)).toBe("lagging");
    expect(at(now - 60_000)).toBe("stale");
  });

  it("distinguishes hydrating, computed on read and unknown", () => {
    expect(
      freshnessOf({ writeFrontier: now, hydrated: false }, "view", now).state,
    ).toBe("hydrating");
    expect(
      freshnessOf({ writeFrontier: null, hydrated: null }, "view", now).state,
    ).toBe("computedOnRead");
    expect(freshnessOf(undefined, "table", now).state).toBe("unknown");
  });

  it("formats lag compactly", () => {
    expect(formatLag(420)).toBe("0.4s");
    expect(formatLag(42_000)).toBe("42s");
    expect(formatLag(180_000)).toBe("3m");
    expect(formatLag(7_200_000)).toBe("2h");
  });
});

describe("layoutGraph", () => {
  const overlaps = (a: { x: number; y: number }, b: { x: number; y: number }) =>
    Math.abs(a.x - b.x) < NODE_WIDTH && Math.abs(a.y - b.y) < NODE_HEIGHT;

  const assertNoOverlap = (layout: ReturnType<typeof layoutGraph>) => {
    const positions = [...layout.nodes.values()];
    for (let i = 0; i < positions.length; i++) {
      for (let j = i + 1; j < positions.length; j++) {
        expect(overlaps(positions[i], positions[j])).toBe(false);
      }
    }
    for (const p of positions) {
      expect(p.x - NODE_WIDTH / 2).toBeGreaterThanOrEqual(0);
      expect(p.y - NODE_HEIGHT / 2).toBeGreaterThanOrEqual(0);
      expect(p.x + NODE_WIDTH / 2).toBeLessThanOrEqual(layout.width);
      expect(p.y + NODE_HEIGHT / 2).toBeLessThanOrEqual(layout.height);
    }
  };

  it("places referenced entities above their dependents", () => {
    const nodes = [node("orders"), node("customers")];
    const layout = layoutGraph(nodes, [edge("orders", "customers")], {
      mode: "hierarchical",
      showEdgeLabels: true,
    });
    const orders = layout.nodes.get("orders");
    const customers = layout.nodes.get("customers");
    expect(customers && orders && customers.y < orders.y).toBe(true);
    expect(layout.edges.get("join|orders|customers")?.path).toMatch(/^M/);
    assertNoOverlap(layout);
  });

  it("boxes each group when there are several", () => {
    const layout = layoutGraph(
      [node("a", "x"), node("b", "y")],
      [edge("a", "b")],
      { mode: "hierarchical", showEdgeLabels: false },
    );
    expect(layout.groups.map((g) => g.name)).toEqual(["x", "y"]);
  });

  it("wraps a hub's wide rank into rows", () => {
    const spokes = Array.from({ length: 40 }, (_, i) => node(`s${i}`));
    const nodes = [node("hub"), ...spokes];
    const edges = spokes.map((spoke) => edge(spoke.id, "hub"));
    const layout = layoutGraph(nodes, edges, {
      mode: "hierarchical",
      showEdgeLabels: false,
    });
    const rows = new Set([...layout.nodes.values()].map((p) => p.y));
    expect(rows.size).toBeGreaterThan(2);
    expect(layout.width).toBeLessThanOrEqual(
      maxCardsPerRow(nodes.length) * (NODE_WIDTH + 32) + 200,
    );
    expect(layout.edges.size).toBe(edges.length);
    assertNoOverlap(layout);
  });

  it("lays out a circle with an edge per relationship", () => {
    const nodes = ["a", "b", "c", "d"].map((id) => node(id));
    const layout = layoutGraph(nodes, [edge("a", "b"), edge("c", "c")], {
      mode: "circular",
      showEdgeLabels: true,
    });
    expect(layout.edges.size).toBe(2);
    assertNoOverlap(layout);
  });
});
