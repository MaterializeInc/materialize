// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  buildDataProductGraph,
  CatalogRelation,
  DataProductGraphInput,
  OntologyTableRef,
  parseQualifiedName,
  resolveRelationName,
} from "./dataProductGraph";
import { OntologyColumn } from "./model";

const relation = (
  id: string,
  name: string,
  schemaName = "shop",
  objectType = "table",
  databaseName: string | null = "materialize",
): CatalogRelation => ({
  id,
  name,
  objectType,
  schemaName,
  databaseName,
  clusterName: null,
  sourceType: null,
});

const ONTOLOGY: OntologyTableRef = {
  id: "u100",
  name: "ontology",
  schemaName: "shop",
  databaseName: "materialize",
};

const column = (name: string, type = "bigint"): OntologyColumn => ({
  name,
  type,
  description: null,
  semanticType: null,
});

const byName = (relations: CatalogRelation[]) => {
  const map = new Map<string, CatalogRelation[]>();
  for (const r of relations) map.set(r.name, [...(map.get(r.name) ?? []), r]);
  return map;
};

function input(
  overrides: Partial<DataProductGraphInput>,
): DataProductGraphInput {
  return {
    relations: [],
    ontologyTables: [ONTOLOGY],
    ontologyRows: [],
    lineage: [],
    columns: new Map(),
    comments: new Map(),
    dataProducts: new Set(),
    ...overrides,
  };
}

describe("parseQualifiedName", () => {
  it("folds unquoted parts and keeps quoted ones", () => {
    expect(parseQualifiedName("Shop.Orders")).toEqual(["shop", "orders"]);
    expect(parseQualifiedName('"Shop"."Ord.ers"')).toEqual(["Shop", "Ord.ers"]);
    expect(parseQualifiedName('a."b""c"')).toEqual(["a", 'b"c']);
  });

  it("rejects malformed names", () => {
    expect(parseQualifiedName("")).toBeNull();
    expect(parseQualifiedName("a..b")).toBeNull();
    expect(parseQualifiedName("a.b.c.d")).toBeNull();
    expect(parseQualifiedName('"open')).toBeNull();
  });
});

describe("resolveRelationName", () => {
  const relations = [
    relation("u1", "orders", "shop"),
    relation("u2", "orders", "archive"),
    relation("u3", "customers", "crm"),
    relation("u4", "customers", "crm", "table", "other_db"),
  ];
  const index = byName(relations);

  it("prefers the ontology table's own schema for unqualified names", () => {
    expect(resolveRelationName("orders", ONTOLOGY, index)).toEqual({
      status: "resolved",
      relation: relations[0],
    });
  });

  it("prefers the ontology table's database before searching everywhere", () => {
    expect(resolveRelationName("customers", ONTOLOGY, index)).toEqual({
      status: "resolved",
      relation: relations[2],
    });
  });

  it("honors schema and database qualification", () => {
    expect(resolveRelationName("archive.orders", ONTOLOGY, index)).toEqual({
      status: "resolved",
      relation: relations[1],
    });
    expect(
      resolveRelationName("other_db.crm.customers", ONTOLOGY, index),
    ).toEqual({ status: "resolved", relation: relations[3] });
  });

  it("reports names that match nothing or several relations", () => {
    expect(resolveRelationName("payments", ONTOLOGY, index)).toEqual({
      status: "missing",
    });
    const elsewhere = { ...ONTOLOGY, schemaName: "other" };
    expect(resolveRelationName("orders", elsewhere, index).status).toBe(
      "ambiguous",
    );
  });
});

describe("buildDataProductGraph", () => {
  const orders = relation("u1", "orders");
  const customers = relation("u2", "customers");
  const columns = new Map([
    ["u1", [column("id"), column("customer_id"), column("email", "text")]],
    ["u2", [column("id"), column("email", "text")]],
  ]);

  it("turns ontology rows into join edges and marks the referenced keys", () => {
    const graph = buildDataProductGraph(
      input({
        relations: [orders, customers],
        ontologyRows: [
          {
            ontologyId: "u100",
            tableName: "orders",
            columns: ["customer_id"],
            referencedTable: "customers",
            referencedColumns: ["id"],
          },
        ],
        columns,
      }),
    );
    expect(graph.issues).toEqual([]);
    expect(graph.edges).toHaveLength(1);
    expect(graph.edges[0]).toMatchObject({
      source: "u1",
      target: "u2",
      kind: "join",
      sourceColumns: ["customer_id"],
      targetColumns: ["id"],
      declaredIn: "materialize.shop.ontology",
    });
    expect(graph.nodes.find((n) => n.id === "u2")?.primaryKey).toEqual(["id"]);
  });

  it("does not draw the ontology table itself as an entity", () => {
    const graph = buildDataProductGraph(
      input({ relations: [orders, relation("u100", "ontology")] }),
    );
    expect(graph.nodes.map((n) => n.id)).toEqual(["u1"]);
  });

  it("reports unknown relations and draws them as unresolved", () => {
    const graph = buildDataProductGraph(
      input({
        relations: [orders],
        ontologyRows: [
          {
            ontologyId: "u100",
            tableName: "payments",
            columns: ["order_id"],
            referencedTable: "orders",
            referencedColumns: ["id"],
          },
        ],
        columns,
      }),
    );
    const unresolved = graph.nodes.find((n) => n.isUnresolved);
    expect(unresolved?.label).toBe("payments");
    expect(graph.edges[0].source).toBe(unresolved?.id);
    expect(graph.issues.map((i) => i.message)).toEqual([
      '"payments" does not match any relation.',
    ]);
  });

  it("reports missing columns, count mismatches and type mismatches", () => {
    const rows = [
      ["bidder_id", "id"],
      ["email", "id"],
    ].map(([from, to]) => ({
      ontologyId: "u100",
      tableName: "orders",
      columns: [from],
      referencedTable: "customers",
      referencedColumns: [to],
    }));
    rows.push({
      ontologyId: "u100",
      tableName: "orders",
      columns: ["customer_id"],
      referencedTable: "customers",
      referencedColumns: ["id", "email"],
    });
    const graph = buildDataProductGraph(
      input({ relations: [orders, customers], ontologyRows: rows, columns }),
    );
    const messages = graph.issues.map((i) => `${i.severity}: ${i.message}`);
    expect(messages).toContain('error: orders has no column "bidder_id".');
    expect(messages).toContain(
      "warning: email (text) pairs with id (bigint). The join needs a cast.",
    );
    expect(messages.some((m) => m.startsWith("error: 1 column(s)"))).toBe(true);
  });

  it("skips column checks until columns are loaded", () => {
    const graph = buildDataProductGraph(
      input({
        relations: [orders, customers],
        ontologyRows: [
          {
            ontologyId: "u100",
            tableName: "orders",
            columns: ["anything"],
            referencedTable: "customers",
            referencedColumns: ["id"],
          },
        ],
      }),
    );
    expect(graph.issues).toEqual([]);
  });

  it("draws lineage between drawn relations only", () => {
    const view = relation("u3", "big_orders", "shop", "materialized-view");
    const graph = buildDataProductGraph(
      input({
        relations: [orders, view],
        lineage: [
          { childId: "u3", parentId: "u1" },
          { childId: "u3", parentId: "s42" },
        ],
      }),
    );
    expect(graph.edges.map((e) => [e.kind, e.source, e.target])).toEqual([
      ["lineage", "u3", "u1"],
    ]);
  });

  it("marks relations the agent MCP server lists as data products", () => {
    const graph = buildDataProductGraph(
      input({
        relations: [orders],
        dataProducts: new Set(['"materialize"."shop"."orders"']),
        comments: new Map([["u1", "Every order."]]),
      }),
    );
    expect(graph.nodes[0]).toMatchObject({
      isDataProduct: true,
      description: "Every order.",
    });
  });
});
