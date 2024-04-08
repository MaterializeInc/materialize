// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { beforeEach, describe, it, expect } from "vitest";
import { JSDOM } from "jsdom";
import fs from "fs";
import path from "path";

describe("JSON Widget", () => {
  let window;

  // Setup before each test
  beforeEach(() => {
    const htmlPath = path.join(
      __dirname,
      "../../../doc/user/layouts/shortcodes/json-parser.html"
    );
    const htmlContent = fs.readFileSync(htmlPath, "utf-8");
    const dom = new JSDOM(htmlContent, {
      runScripts: "dangerously",
      resources: "usable",
    });
    window = dom.window;

    window.addEventListener("error", function (event) {
      if (event.message === "Unexpected end of JSON input") {
        event.preventDefault();
      }
    });
    return new Promise((resolve) => {
      window.document.addEventListener("DOMContentLoaded", resolve);
    });
  });

  it("should generate correct SQL from valid JSON input", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"name": "test", "value": 123}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("SELECT");
    expect(sqlOutput.textContent).toContain("name");
    expect(sqlOutput.textContent).toContain("value");
  });

  it("should update SQL when view name is changed", async () => {
    const viewNameInput = window.document.getElementById("view_name");
    const sqlOutput = window.document.getElementById("output");

    // Set initial JSON to trigger SQL generation
    const jsonInput = window.document.getElementById("json_sample");
    jsonInput.value = '{"name": "test", "value": 123}';
    jsonInput.dispatchEvent(new window.Event("input"));
    await new Promise((r) => setTimeout(r, 650));

    // Change the view name
    viewNameInput.value = "new_view";
    viewNameInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("CREATE VIEW new_view");
  });

  it("should handle empty JSON input appropriately", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = "";
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toBe("");
  });

  it("should correctly handle a complex JSON structure", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    const complexJson =
      '{"user": {"name": "Jane", "age": 30, "skills": ["JS", "Python"]}}';
    jsonInput.value = complexJson;
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("user_name");
    expect(sqlOutput.textContent).toContain("user_age");
    expect(sqlOutput.textContent).toContain("user_skills");
  });

  it("should correctly process JSON with special characters", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"special": "\\"Hello, \\n world!\\""}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("special");
  });

  it("should accurately reflect different data types in JSON", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"boolean": true, "nullValue": null, "number": 123}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("boolean");
    expect(sqlOutput.textContent).toContain("nullValue");
    expect(sqlOutput.textContent).toContain("number");
  });

  it("should update SQL when object type selection changes", async () => {
    const viewTypeRadio = window.document.getElementById("view");
    const materializedViewTypeRadio =
      window.document.getElementById("materialized-view");
    const sqlOutput = window.document.getElementById("output");

    // Trigger an initial selection
    viewTypeRadio.checked = true;
    viewTypeRadio.dispatchEvent(new window.Event("change"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("CREATE VIEW");

    // Change to materialized view
    materializedViewTypeRadio.checked = true;
    materializedViewTypeRadio.dispatchEvent(new window.Event("change"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain("CREATE MATERIALIZED VIEW");
  });

  it("should cast different fields correctly based on their data types", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");
    const viewNameInput = window.document.getElementById("view_name");
    viewNameInput.value = "my_view"; // Set the view name

    // Use the provided JSON structure
    const complexJson = `{
      "payload": "materialize",
      "event": {
        "kind": 1,
        "success": true,
        "createdAt": "2023-02-01T17:00:00.000Z"
      },
      "ts": "2023-02-01T17:00:00.000Z"
    }`;

    jsonInput.value = complexJson;
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    // Check if the SQL contains the correct casting
    const expectedSql = [
      "CREATE VIEW my_view AS SELECT",
      "json_column->>'payload' AS payload,",
      "(json_column->'event'->>'kind')::numeric AS event_kind,",
      "(json_column->'event'->>'success')::bool AS event_success,",
      "try_parse_monotonic_iso8601_timestamp(json_column->'event'->>'createdAt') AS event_createdAt,",
      "try_parse_monotonic_iso8601_timestamp(json_column->>'ts') AS ts",
      "FROM my_source;",
    ];

    // Join the expected SQL lines and check if the output contains this SQL statement
    expect(sqlOutput.textContent.replace(/\s+/g, " ")).toContain(
      expectedSql.join(" ").replace(/\s+/g, " ")
    );
  });
  // Test Casting for Numeric Types
  it("should cast numeric types correctly", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"number": 42}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain(
      "(json_column->>'number')::numeric AS number"
    );
  });

  // Test Casting for Boolean Types
  it("should cast boolean types correctly", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"isValid": true}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain(
      "(json_column->>'isValid')::bool AS isValid"
    );
  });

  // Test Casting for Date Strings
  it("should cast date strings correctly", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"date": "2023-02-01T17:00:00.000Z"}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    expect(sqlOutput.textContent).toContain(
      "try_parse_monotonic_iso8601_timestamp(json_column->>'date') AS date"
    );
  });

  // Test Casting for Null Values
  it("should handle null values correctly", async () => {
    const jsonInput = window.document.getElementById("json_sample");
    const sqlOutput = window.document.getElementById("output");

    jsonInput.value = '{"nullable": null}';
    jsonInput.dispatchEvent(new window.Event("input"));

    await new Promise((r) => setTimeout(r, 650));

    // Update this based on how your script handles null values
    expect(sqlOutput.textContent).toContain("nullable");
  });
});
