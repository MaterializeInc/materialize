// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CatalogRelation } from "./dataProductGraph";
import {
  fuzzyScore,
  rankOntologyTables,
  resolveOntologyTable,
} from "./ontologyTableChoice";

const relation = (
  id: string,
  databaseName: string,
  schemaName: string,
  name: string,
): CatalogRelation => ({
  id,
  name,
  objectType: "table",
  schemaName,
  databaseName,
  clusterName: null,
  sourceType: null,
});

const RELATIONS = [
  relation("u1", "materialize", "public", "ontology"),
  relation("u2", "other", "public", "ontology"),
  relation("u3", "materialize", "auction", "ontology"),
  relation("u4", "materialize", "auction", "orders_to_logs_by_yday"),
];

describe("resolveOntologyTable", () => {
  it("prefers the default database for a schema-qualified name", () => {
    expect(resolveOntologyTable("public.ontology", RELATIONS)?.id).toBe("u1");
  });

  it("honors a database-qualified name", () => {
    expect(resolveOntologyTable("other.public.ontology", RELATIONS)?.id).toBe(
      "u2",
    );
  });

  it("returns null for a name that matches nothing", () => {
    expect(resolveOntologyTable("public.joins", RELATIONS)).toBeNull();
    expect(resolveOntologyTable("a..b", RELATIONS)).toBeNull();
  });
});

describe("fuzzy ranking", () => {
  it("matches in-order subsequences only", () => {
    expect(fuzzyScore("onto", "materialize.auction.ontology")).not.toBeNull();
    expect(fuzzyScore("otno", "ontology")).toBeNull();
  });

  it("ranks tables with the ontology columns first, then by match", () => {
    const ranked = rankOntologyTables(
      "auction.o",
      RELATIONS,
      new Set(["u3"]),
    ).map((candidate) => candidate.name);
    expect(ranked).toEqual([
      "materialize.auction.ontology",
      "materialize.auction.orders_to_logs_by_yday",
    ]);
  });
});
