// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompletionContext } from "@codemirror/autocomplete";
import { EditorState } from "@codemirror/state";

import {
  buildCompletionNamespace,
  cellReferenceCompletion,
} from "./sqlCompletion";

describe("buildCompletionNamespace", () => {
  it("nests relations the way Materialize resolves names", () => {
    expect(
      buildCompletionNamespace([
        {
          databaseName: "materialize",
          schemaName: "public",
          name: "orders",
          columns: ["id", "total"],
        },
        {
          databaseName: "materialize",
          schemaName: "sales",
          name: "leads",
          columns: ["id"],
        },
        {
          databaseName: "other",
          schemaName: "public",
          name: "events",
          columns: ["ts"],
        },
        {
          databaseName: null,
          schemaName: "mz_catalog",
          name: "mz_clusters",
          columns: ["id", "name"],
        },
        {
          databaseName: null,
          schemaName: "mz_internal",
          name: "mz_sessions",
          columns: ["id"],
        },
      ]),
    ).toEqual({
      materialize: {
        public: { orders: ["id", "total"] },
        sales: { leads: ["id"] },
      },
      other: { public: { events: ["ts"] } },
      public: { orders: ["id", "total"] },
      sales: { leads: ["id"] },
      orders: ["id", "total"],
      mz_catalog: { mz_clusters: ["id", "name"] },
      mz_clusters: ["id", "name"],
      mz_internal: { mz_sessions: ["id"] },
    });
  });
});

describe("cellReferenceCompletion", () => {
  const complete = (doc: string) => {
    const state = EditorState.create({ doc });
    const source = cellReferenceCompletion(() => ["base", "totals"]);
    return source(
      new CompletionContext(
        state,
        doc.indexOf("|") >= 0 ? doc.indexOf("|") : doc.length,
        false,
      ),
    );
  };

  it("offers cell names after {{ and closes the braces", () => {
    const result = complete("SELECT * FROM {{ ba");
    expect(result).toMatchObject({ from: 17 });
    expect(result && "options" in result && result.options[0]).toMatchObject({
      label: "base",
      apply: "base}}",
    });
  });

  it("stays quiet outside {{", () => {
    expect(complete("SELECT * FROM ba")).toBeNull();
  });
});
