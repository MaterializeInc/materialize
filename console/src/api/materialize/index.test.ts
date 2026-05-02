// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { queryBuilder } from "./db";
import { escapedLiteral, quoteIdentifier } from "./index";

describe("quoteIdentifier", () => {
  it("should wrap identifier in double quotes", () => {
    expect(quoteIdentifier("foo")).toBe('"foo"');
  });

  it("should escape a double quote", () => {
    expect(quoteIdentifier('foo"bar')).toBe('"foo""bar"');
  });

  it("should escape multiple double quotes", () => {
    expect(quoteIdentifier('foo"bar"baz')).toBe('"foo""bar""baz"');
  });
});

describe("escapedLiteral", () => {
  const compileToSql = (literal: string) => {
    return queryBuilder
      .selectFrom("test" as never)
      .select(sql`${escapedLiteral(literal)}`.as("value"))
      .compile().sql;
  };

  it("should escape a single quote", () => {
    expect(compileToSql("foo'bar")).toContain("'foo''bar'");
  });

  it("should escape multiple single quotes", () => {
    expect(compileToSql("a'b'c")).toContain("'a''b''c'");
  });
});
