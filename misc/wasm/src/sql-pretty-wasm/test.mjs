#!/usr/bin/env node

import assert from "node:assert/strict";

if (!process.execArgv.includes("--experimental-wasm-modules")) {
  console.error(
    "Run with: node --experimental-wasm-modules misc/wasm/src/sql-pretty-wasm/test.mjs",
  );
  process.exit(1);
}

const { prettyStrConfig, prettyStrsConfig } = await import(
  "./pkg/mz_sql_pretty_wasm.js"
);

function assertError(fn, message) {
  assert.throws(fn, (error) => error instanceof Error && error.message === message);
}

assert.equal(
  prettyStrConfig("CREATE TABLE t (a int, b int)", { width: 1, indent: 2 }),
  "CREATE TABLE\n  t\n    (\n      a int4,\n      b int4\n    );",
);

assert.deepEqual(
  prettyStrsConfig("SELECT 1; SELECT 2", { width: 100, indent: 2 }),
  ["SELECT 1;", "SELECT 2;"],
);

assertError(
  () => prettyStrConfig("SELECT 1", { indent: -1 }),
  "indent is out of range",
);
assertError(
  () => prettyStrConfig("SELECT 1", { indent: 1.5 }),
  "indent must be an integer",
);
assertError(
  () => prettyStrConfig("SELECT 1", { indent: "2" }),
  "indent must be a number",
);
assertError(
  () => prettyStrConfig("SELECT 1", { width: "100" }),
  "width must be a number",
);
assertError(
  () => prettyStrConfig("SELECT 1", { formatMode: "bogus" }),
  "invalid formatMode: bogus",
);
assertError(
  () => prettyStrConfig("SELECT 1", { formatMode: 1 }),
  "formatMode must be a string",
);

console.log("sql-pretty-wasm test passed");
