// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Connector, sortConnectors } from "./sort";

function buildSource(overrides: Partial<Connector>): Connector {
  return {
    databaseName: "materialize",
    schemaName: "public",
    name: "upstream",
    status: "starting",
    type: "postgres",
    ...overrides,
  };
}

describe("sortConnectors", () => {
  it("name sorts by database, schema and object name", () => {
    const sources = [
      buildSource({
        databaseName: "database_b",
        schemaName: "schema_b",
        name: "source_a",
      }),
      buildSource({
        databaseName: "database_b",
        schemaName: "schema_b",
        name: "source_b",
      }),
      buildSource({
        databaseName: "database_b",
        schemaName: "schema_a",
        name: "source_c",
      }),
      buildSource({
        databaseName: "database_b",
        schemaName: "schema_a",
        name: "source_d",
      }),
      buildSource({
        databaseName: "database_a",
        schemaName: "schema_a",
        name: "source_e",
      }),
    ];
    const result = sortConnectors(sources, "name", "asc");
    expect(result).toEqual([
      expect.objectContaining({
        databaseName: "database_a",
        schemaName: "schema_a",
        name: "source_e",
      }),
      expect.objectContaining({
        databaseName: "database_b",
        schemaName: "schema_a",
        name: "source_c",
      }),
      expect.objectContaining({
        databaseName: "database_b",
        schemaName: "schema_a",
        name: "source_d",
      }),
      expect.objectContaining({
        databaseName: "database_b",
        schemaName: "schema_b",
        name: "source_a",
      }),
      expect.objectContaining({
        databaseName: "database_b",
        schemaName: "schema_b",
        name: "source_b",
      }),
    ]);
  });

  it("type sorts alphabetically", () => {
    const sources = [
      buildSource({ type: "postgres" }),
      buildSource({ type: "webhook" }),
      buildSource({ type: "kafka" }),
    ];
    expect(sortConnectors(sources, "type", "asc")).toEqual([
      expect.objectContaining({
        type: "kafka",
      }),
      expect.objectContaining({
        type: "postgres",
      }),
      expect.objectContaining({
        type: "webhook",
      }),
    ]);
    expect(sortConnectors(sources, "type", "desc")).toEqual([
      expect.objectContaining({
        type: "webhook",
      }),
      expect.objectContaining({
        type: "postgres",
      }),
      expect.objectContaining({
        type: "kafka",
      }),
    ]);
  });

  it("status sorts in a custom order", () => {
    const sources = [
      buildSource({ status: "created" }),
      buildSource({ status: "dropped" }),
      buildSource({ status: "failed" }),
      buildSource({ status: "paused" }),
      buildSource({ status: "running" }),
      buildSource({ status: "stalled" }),
      buildSource({ status: "starting" }),
    ];
    expect(sortConnectors(sources, "status", "asc")).toEqual([
      expect.objectContaining({
        status: "stalled",
      }),
      expect.objectContaining({
        status: "starting",
      }),
      expect.objectContaining({
        status: "failed",
      }),
      expect.objectContaining({
        status: "dropped",
      }),
      expect.objectContaining({
        status: "paused",
      }),
      expect.objectContaining({
        status: "created",
      }),
      expect.objectContaining({
        status: "running",
      }),
    ]);
  });
});
