// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildDataProductGraph,
  CatalogRelation,
  DataProductGraphInput,
} from "./dataProductGraph";
import { buildDeclareSql } from "./declareSql";
import { OntologyColumn, OntologyEdge } from "./model";
import { ViewJoins } from "./viewJoins";

const relation = (
  id: string,
  name: string,
  objectType = "table",
  schemaName = "shop",
): CatalogRelation => ({
  id,
  name,
  objectType,
  schemaName,
  databaseName: "materialize",
  clusterName: null,
  sourceType: null,
});

const column = (name: string, type = "bigint"): OntologyColumn => ({
  name,
  type,
  description: null,
  semanticType: null,
});

const ORDERS = relation("u1", "orders");
const CUSTOMERS = relation("u2", "customers");
const SUMMARY = relation("u3", "order_summary", "view");

const COLUMNS = new Map([
  ["u1", [column("id"), column("customer_id"), column("total")]],
  ["u2", [column("id"), column("name", "text")]],
  ["u3", [column("customer_id"), column("total")]],
]);

/** `order_summary` joins `orders.customer_id = customers.id`. */
const SUMMARY_JOINS: ViewJoins = {
  pairs: [
    {
      left: { objectId: "u1", column: "customer_id" },
      right: { objectId: "u2", column: "id" },
    },
  ],
  usingJoins: [],
  outputs: [],
};

function build(overrides: Partial<DataProductGraphInput> = {}) {
  return buildDataProductGraph({
    relations: [ORDERS, CUSTOMERS, SUMMARY],
    ontologyTables: [],
    ontologyRows: [],
    lineage: [],
    columns: COLUMNS,
    comments: new Map(),
    dataProducts: new Set(),
    viewJoins: new Map([["u3", SUMMARY_JOINS]]),
    indexKeys: new Map(),
    ...overrides,
  });
}

const joins = (edges: OntologyEdge[]) =>
  edges
    .filter((edge) => edge.kind !== "lineage")
    .map(
      (edge) =>
        `${edge.kind}: ${edge.source}.${edge.sourceColumns} -> ${edge.target}.${edge.targetColumns}`,
    );

describe("inferred joins", () => {
  it("infers a join a view makes, pointing at the key", () => {
    const graph = build();
    expect(joins(graph.edges)).toEqual([
      "inferred_join: u1.customer_id -> u2.id",
    ]);
    const inferred = graph.edges.find((e) => e.kind === "inferred_join")!;
    expect(inferred.inferredFrom).toEqual(["materialize.shop.order_summary"]);
    expect(inferred.cardinality).toBe("many_to_one");
    expect(graph.nodes.find((n) => n.id === "u2")?.primaryKey).toEqual(["id"]);
  });

  it("keeps an inferred join beside the declared one, cross-referenced", () => {
    const graph = build({
      ontologyTables: [
        {
          id: "u9",
          name: "ontology",
          schemaName: "shop",
          databaseName: "materialize",
        },
      ],
      ontologyRows: [
        {
          ontologyId: "u9",
          tableName: "orders",
          columns: ["customer_id"],
          referencedTable: "customers",
          referencedColumns: ["id"],
        },
      ],
    });
    const declared = graph.edges.filter((e) => e.kind === "join");
    expect(declared).toHaveLength(1);
    expect(declared[0].inferredFrom).toEqual([
      "materialize.shop.order_summary",
    ]);
    const inferred = graph.edges.filter((e) => e.kind === "inferred_join");
    expect(inferred.map((e) => e.declaredIn)).toEqual([
      "materialize.shop.ontology",
    ]);
  });

  it("guesses direction only when neither side is a key, and says so", () => {
    const graph = build({
      columns: new Map([
        ["u1", [column("region")]],
        ["u2", [column("region")]],
        ["u3", []],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [
              {
                left: { objectId: "u1", column: "region" },
                right: { objectId: "u2", column: "region" },
              },
            ],
            usingJoins: [],
            outputs: [],
          },
        ],
      ]),
    });
    const [edge] = graph.edges;
    expect(edge.kind).toBe("inferred_join");
    expect(edge.cardinality).toBeNull();
    expect(edge.note).toMatch(/direction is a guess/);
    expect(graph.nodes.find((n) => n.id === edge.target)?.primaryKey).toEqual(
      [],
    );
  });

  it("lets a single-column index decide which side is the key", () => {
    const graph = build({
      columns: new Map([
        ["u1", [column("sku")]],
        ["u2", [column("sku")]],
        ["u3", []],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [
              {
                left: { objectId: "u2", column: "sku" },
                right: { objectId: "u1", column: "sku" },
              },
            ],
            usingJoins: [],
            outputs: [],
          },
        ],
      ]),
      indexKeys: new Map([["u2", [["sku"]]]]),
    });
    expect(joins(graph.edges)).toEqual(["inferred_join: u1.sku -> u2.sku"]);
  });

  it("resolves a USING join to the earlier relation that has the column", () => {
    const graph = build({
      columns: new Map([
        ["u1", [column("id"), column("customer_id")]],
        ["u2", [column("customer_id"), column("name", "text")]],
        ["u3", []],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [],
            usingJoins: [
              { right: "u2", candidates: ["u1"], columns: ["customer_id"] },
            ],
            outputs: [],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toContain(
      "inferred_join: u1.customer_id -> u2.customer_id",
    );
  });

  it("ignores a join on a column the relation does not have", () => {
    const graph = build({
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [
              {
                left: { objectId: "u1", column: "gone" },
                right: { objectId: "u2", column: "id" },
              },
            ],
            usingJoins: [],
            outputs: [],
          },
        ],
      ]),
    });
    expect(graph.edges.some((e) => e.kind === "inferred_join")).toBe(false);
  });
});

describe("columns views pass through", () => {
  const ORG_SPEND = relation("u4", "org_spend", "view");
  const TOP_ORGS = relation("u5", "top_orgs", "view");

  it("follows a passed-through key, and a passed-through reference", () => {
    const graph = build({
      relations: [ORDERS, CUSTOMERS, SUMMARY, ORG_SPEND],
      columns: new Map([
        ...COLUMNS,
        ["u3", [column("customer_id"), column("total")]],
        ["u4", [column("customer_key")]],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            ...SUMMARY_JOINS,
            // order_summary.customer_id is orders.customer_id.
            outputs: [
              {
                column: "customer_id",
                source: { objectId: "u1", column: "customer_id" },
              },
            ],
          },
        ],
        [
          "u4",
          {
            pairs: [],
            usingJoins: [],
            // org_spend.customer_key is customers.id.
            outputs: [
              {
                column: "customer_key",
                source: { objectId: "u2", column: "id" },
              },
            ],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toEqual([
      "inferred_join: u1.customer_id -> u2.id",
      "inferred_join: u3.customer_id -> u2.id",
      "inferred_join: u4.customer_key -> u2.id",
    ]);
    const passed = graph.edges.find((e) => e.source === "u4")!;
    expect(passed.note).toBe(
      "org_spend.customer_key is customers.id, passed through.",
    );
    expect(passed.inferredFrom).toEqual(["materialize.shop.org_spend"]);
  });

  it("carries a reference back to the input column a view passes", () => {
    // order_summary passes orders.cust_ref, and joins its own output on it.
    const graph = build({
      columns: new Map([
        ["u1", [column("id"), column("cust_ref")]],
        ["u2", [column("id")]],
        ["u3", [column("cust_ref")]],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [
              {
                left: { objectId: "u3", column: "cust_ref" },
                right: { objectId: "u2", column: "id" },
              },
            ],
            usingJoins: [],
            outputs: [
              {
                column: "cust_ref",
                source: { objectId: "u1", column: "cust_ref" },
              },
            ],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toContain("inferred_join: u1.cust_ref -> u2.id");
  });

  it("follows references through views over views", () => {
    const graph = build({
      relations: [ORDERS, CUSTOMERS, SUMMARY, TOP_ORGS],
      columns: new Map([...COLUMNS, ["u5", [column("cust")]]]),
      viewJoins: new Map([
        [
          "u3",
          {
            ...SUMMARY_JOINS,
            outputs: [
              {
                column: "customer_id",
                source: { objectId: "u1", column: "customer_id" },
              },
            ],
          },
        ],
        [
          "u5",
          {
            pairs: [],
            usingJoins: [],
            outputs: [
              {
                column: "cust",
                source: { objectId: "u3", column: "customer_id" },
              },
            ],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toContain("inferred_join: u5.cust -> u2.id");
  });

  it("gives a passed key only its direct reference, not the closure", () => {
    // top_orgs.cust passes order_summary.id, a key that itself passes orders.id.
    const graph = build({
      relations: [ORDERS, CUSTOMERS, SUMMARY, TOP_ORGS],
      columns: new Map([
        ["u1", [column("id")]],
        ["u2", [column("id")]],
        ["u3", [column("id")]],
        ["u5", [column("id")]],
      ]),
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [],
            usingJoins: [],
            outputs: [
              { column: "id", source: { objectId: "u1", column: "id" } },
            ],
          },
        ],
        [
          "u5",
          {
            pairs: [],
            usingJoins: [],
            outputs: [
              { column: "id", source: { objectId: "u3", column: "id" } },
            ],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toEqual([
      "inferred_join: u3.id -> u1.id",
      "inferred_join: u5.id -> u3.id",
    ]);
  });

  it("ignores passed-through columns that are neither key nor reference", () => {
    const graph = build({
      viewJoins: new Map([
        [
          "u3",
          {
            pairs: [],
            usingJoins: [],
            outputs: [
              { column: "total", source: { objectId: "u1", column: "total" } },
            ],
          },
        ],
      ]),
    });
    expect(joins(graph.edges)).toEqual([]);
  });
});

const ONTOLOGY_TABLE = {
  id: "u9",
  name: "ontology",
  schemaName: "shop",
  databaseName: "materialize",
};

const declaring = (columns: string[], referencedColumns: string[]) => ({
  ontologyTables: [ONTOLOGY_TABLE],
  ontologyRows: [
    {
      ontologyId: "u9",
      tableName: "orders",
      columns,
      referencedTable: "customers",
      referencedColumns,
    },
  ],
});

const warnings = (edges: OntologyEdge[]) =>
  edges.flatMap((edge) =>
    edge.issues
      .filter((issue) => issue.severity === "warning")
      .map((issue) => issue.message),
  );

describe("declared joins the views contradict", () => {
  it("warns when views join the relations only on other columns", () => {
    const graph = build({
      ...declaring(["total"], ["id"]),
      columns: new Map([
        ["u1", [column("id"), column("customer_id"), column("total")]],
        ["u2", [column("id")]],
        ["u3", []],
      ]),
    });
    expect(warnings(graph.edges)).toEqual([
      "Views relate orders to customers on customer_id = id, never on the declared columns.",
    ]);
    expect(graph.issues).toHaveLength(1);
  });

  it("does not warn when no view joins the two relations", () => {
    const graph = build({
      ...declaring(["total"], ["id"]),
      viewJoins: new Map(),
    });
    expect(warnings(graph.edges)).toEqual([]);
  });
});

describe("too wide indexes", () => {
  it("warns when every covering index has more columns than the join", () => {
    const graph = build({
      indexKeys: new Map([["u2", [["id", "name"]]]]),
    });
    expect(warnings(graph.edges)).toEqual([
      'The index on customers (id, name) is wider than this join\'s key (id), so the join cannot use it and arranges customers again. CREATE INDEX ON "materialize"."shop"."customers" ("id") would serve it.',
    ]);
  });

  it("counts a key expression as a column the join lacks", () => {
    const graph = build({
      indexKeys: new Map([["u1", [["customer_id", null]]]]),
    });
    expect(warnings(graph.edges)).toHaveLength(1);
  });

  it("is satisfied by an exact index in any column order", () => {
    const graph = build({
      indexKeys: new Map([["u2", [["id", "name"], ["id"]]]]),
    });
    expect(warnings(graph.edges)).toEqual([]);
  });

  it("ignores indexes that do not cover the join, and unindexed relations", () => {
    const graph = build({
      indexKeys: new Map([["u2", [["name"]]]]),
    });
    expect(warnings(graph.edges)).toEqual([]);
  });
});

describe("buildDeclareSql", () => {
  const graph = build();
  const nodes = new Map(graph.nodes.map((node) => [node.id, node]));
  const inferred = graph.edges.find((e) => e.kind === "inferred_join")!;
  const declare = (home: Parameters<typeof buildDeclareSql>[3]) =>
    buildDeclareSql(inferred, nodes.get("u1")!, nodes.get("u2")!, home);

  it("inserts into the ontology table, with names relative to it", () => {
    expect(
      declare({
        databaseName: "materialize",
        schemaName: "shop",
        name: "Joins",
        exists: true,
      }),
    ).toBe(
      `INSERT INTO "materialize"."shop"."Joins" VALUES (
  'orders', ARRAY['customer_id'],
  'customers', ARRAY['id']
);`,
    );
  });

  it("qualifies names the ontology table's schema would not resolve", () => {
    expect(
      declare({
        databaseName: "materialize",
        schemaName: "public",
        name: "ontology",
        exists: true,
      }),
    ).toContain(`'shop.orders', ARRAY['customer_id'],`);
  });

  it("creates the ontology table when it does not exist", () => {
    expect(
      declare({
        databaseName: "materialize",
        schemaName: "public",
        name: "ontology",
        exists: false,
      }),
    ).toBe(
      `CREATE TABLE "materialize"."public"."ontology" (
  table_name text NOT NULL,
  columns text[] NOT NULL,
  referenced_table text NOT NULL,
  referenced_columns text[] NOT NULL
);
CREATE DEFAULT INDEX ON "materialize"."public"."ontology";
INSERT INTO "materialize"."public"."ontology" VALUES (
  'shop.orders', ARRAY['customer_id'],
  'shop.customers', ARRAY['id']
);`,
    );
  });
});
