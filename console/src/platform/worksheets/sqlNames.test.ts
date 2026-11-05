// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { quoteIdentifier, shortestName } from "./sqlNames";

describe("quoteIdentifier", () => {
  it.each([
    ["orders", "orders"],
    ["Orders", '"Orders"'],
    ["my table", '"my table"'],
    ['say "hi"', '"say ""hi"""'],
  ])("quotes %j as %s", (name, expected) => {
    expect(quoteIdentifier(name)).toBe(expected);
  });
});

describe("shortestName", () => {
  it.each([
    ["materialize", "public", "orders", "orders"],
    ["materialize", "sales", "orders", "sales.orders"],
    ["other", "public", "orders", "other.public.orders"],
    [null, "mz_catalog", "mz_clusters", "mz_clusters"],
    [null, "mz_internal", "mz_sessions", "mz_internal.mz_sessions"],
    ["materialize", "public", "Mixed Case", '"Mixed Case"'],
  ])("names %s.%s.%s as %s", (database, schema, name, expected) => {
    expect(shortestName(database, schema, name)).toBe(expected);
  });
});
