// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type * as monaco from "monaco-editor";

import type { DatabaseObject } from "~/api/materialize/objects";

// Monaco imports browser APIs that aren't available in JSDOM.
// Stub the module so the pure functions we're testing can be imported.
vi.mock("monaco-editor", () => ({}));

const { getQualifiedIdentifier, resolveObject } = await import(
  "./useGoToDefinition"
);

/** Creates a minimal Monaco model mock that returns the given line content. */
function mockModel(lineContent: string): monaco.editor.ITextModel {
  return {
    getLineContent: () => lineContent,
  } as unknown as monaco.editor.ITextModel;
}

/** Creates a minimal Monaco position. Column is 1-based like Monaco. */
function pos(lineNumber: number, column: number): monaco.Position {
  return { lineNumber, column } as monaco.Position;
}

const makeObj = (
  overrides: Partial<DatabaseObject> & {
    name: string;
    schemaName: string;
  },
): DatabaseObject => ({
  id: "u1",
  databaseName: "materialize",
  databaseId: "u1",
  schemaId: "u1",
  objectType: "table",
  sourceType: null,
  isWebhookTable: null,
  owner: "materialize",
  clusterId: null,
  clusterName: null,
  ...overrides,
});

const objects: DatabaseObject[] = [
  makeObj({ id: "u1", name: "customers", schemaName: "public" }),
  makeObj({
    id: "u2",
    name: "customers",
    schemaName: "raw",
    databaseName: "materialize",
  }),
  makeObj({
    id: "u3",
    name: "customers",
    schemaName: "analytics",
    databaseName: "other_db",
  }),
  makeObj({
    id: "u4",
    name: "mz_tables",
    schemaName: "mz_catalog",
    databaseName: null,
  }),
  makeObj({
    id: "u5",
    name: "pg_type",
    schemaName: "pg_catalog",
    databaseName: null,
  }),
];

const defaultSession = {
  database: "materialize",
  searchPath: "public",
};

describe("getQualifiedIdentifier", () => {
  it("extracts an unqualified identifier when cursor is at the start", () => {
    const result = getQualifiedIdentifier(
      mockModel("customers"),
      pos(1, 1), // cursor at 'c'
    );
    expect(result.text).toBe("customers");
  });

  it("extracts an unqualified identifier when cursor is in the middle", () => {
    const result = getQualifiedIdentifier(
      mockModel("customers"),
      pos(1, 5), // cursor at 'o'
    );
    expect(result.text).toBe("customers");
  });

  it("extracts an unqualified identifier when cursor is at the end", () => {
    const result = getQualifiedIdentifier(
      mockModel("customers"),
      pos(1, 10), // cursor past last char
    );
    expect(result.text).toBe("customers");
  });

  it("extracts a schema-qualified identifier", () => {
    const result = getQualifiedIdentifier(
      mockModel("SELECT * FROM raw.customers WHERE 1=1"),
      pos(1, 18), // cursor on 'r' of raw
    );
    expect(result.text).toBe("raw.customers");
  });

  it("extracts a fully qualified identifier", () => {
    const result = getQualifiedIdentifier(
      mockModel("db.raw.customers"),
      pos(1, 10), // cursor in 'raw'
    );
    expect(result.text).toBe("db.raw.customers");
  });

  it("returns the full identifier when cursor is on a dot", () => {
    const result = getQualifiedIdentifier(
      mockModel("raw.customers"),
      pos(1, 4), // cursor on the dot
    );
    expect(result.text).toBe("raw.customers");
  });

  it("returns empty text for an empty line", () => {
    const result = getQualifiedIdentifier(mockModel(""), pos(1, 1));
    expect(result.text).toBe("");
  });

  it("returns empty text when cursor is at column 1 on a line with only spaces", () => {
    const result = getQualifiedIdentifier(mockModel("   "), pos(1, 1));
    expect(result.text).toBe("");
  });

  it("returns the full dotted identifier even with >3 parts", () => {
    const result = getQualifiedIdentifier(
      mockModel("a.b.c.d"),
      pos(1, 3), // cursor on 'b'
    );
    expect(result.text).toBe("a.b.c.d");
  });
});

describe("resolveObject", () => {
  it("resolves an unqualified name via the search path", () => {
    const result = resolveObject("customers", objects, defaultSession);
    expect(result?.id).toBe("u1");
  });

  it("returns undefined for an unqualified name not on the search path", () => {
    const result = resolveObject("customers", objects, {
      database: "materialize",
      searchPath: "analytics",
    });
    expect(result).toBeUndefined();
  });

  it("resolves a schema-qualified name in the current database", () => {
    const result = resolveObject("raw.customers", objects, defaultSession);
    expect(result?.id).toBe("u2");
  });

  it("resolves a fully qualified name", () => {
    const result = resolveObject(
      "other_db.analytics.customers",
      objects,
      defaultSession,
    );
    expect(result?.id).toBe("u3");
  });

  it("returns undefined for >3 parts", () => {
    const result = resolveObject("a.b.c.d", objects, defaultSession);
    expect(result).toBeUndefined();
  });

  it("matches case-insensitively for unquoted identifiers", () => {
    const result = resolveObject("Customers", objects, defaultSession);
    expect(result?.id).toBe("u1");
  });

  it("matches case-insensitively for schema-qualified identifiers", () => {
    const result = resolveObject("Raw.Customers", objects, defaultSession);
    expect(result?.id).toBe("u2");
  });

  it("matches case-insensitively for fully qualified identifiers", () => {
    const result = resolveObject(
      "Other_DB.Analytics.Customers",
      objects,
      defaultSession,
    );
    expect(result?.id).toBe("u3");
  });

  it("resolves unqualified names via implicit mz_catalog", () => {
    const result = resolveObject("mz_tables", objects, defaultSession);
    expect(result?.id).toBe("u4");
  });

  it("resolves unqualified names via implicit pg_catalog", () => {
    const result = resolveObject("pg_type", objects, defaultSession);
    expect(result?.id).toBe("u5");
  });

  it("resolves with multi-schema search path, preferring earlier schemas", () => {
    const result = resolveObject("customers", objects, {
      database: "materialize",
      searchPath: "raw, public",
    });
    // "raw" comes first in the search path, so u2 should be returned
    expect(result?.id).toBe("u2");
  });

  it("falls through to later schemas in search path if not found in earlier", () => {
    const onlyPublic = objects.filter((o) => o.id !== "u2");
    const result = resolveObject("customers", onlyPublic, {
      database: "materialize",
      searchPath: "raw, public",
    });
    // "raw" has no customers, so it should fall through to "public"
    expect(result?.id).toBe("u1");
  });

  it("resolves schema-qualified name against null-database system catalog objects", () => {
    const result = resolveObject(
      "mz_catalog.mz_tables",
      objects,
      defaultSession,
    );
    expect(result?.id).toBe("u4");
  });
});
