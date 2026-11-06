// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RawBuilder, sql } from "kysely";

import { queryBuilder } from "./db";
import {
  escapedLiteral,
  quoteIdentifier,
  rawKeyword,
  rawNumber,
} from "./index";

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

const compileFragment = (fragment: RawBuilder<unknown>) =>
  sql`${fragment}`.compile(queryBuilder).sql;

describe("rawNumber", () => {
  it("should inline numbers, bigints, and numeric strings", () => {
    expect(compileFragment(rawNumber(15))).toBe("15");
    expect(compileFragment(rawNumber(6666.5))).toBe("6666.5");
    expect(compileFragment(rawNumber(10n))).toBe("10");
    expect(compileFragment(rawNumber("5432"))).toBe("5432");
  });

  it("should throw on anything but a plain non-negative number", () => {
    for (const value of [
      "5432; DROP TABLE t",
      "1' MILLISECONDS'",
      "",
      -1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      expect(() => rawNumber(value)).toThrow();
    }
  });
});

describe("rawKeyword", () => {
  it("should inline keywords and function names", () => {
    expect(compileFragment(rawKeyword("MATERIALIZED VIEW"))).toBe(
      "MATERIALIZED VIEW",
    );
    expect(
      compileFragment(rawKeyword("try_parse_monotonic_iso8601_timestamp")),
    ).toBe("try_parse_monotonic_iso8601_timestamp");
  });

  it("should throw on quotes, punctuation, and statement separators", () => {
    for (const value of [
      "DESC; DROP TABLE t",
      "TABLE t --",
      "a'b",
      'a"b',
      "desc, name",
      "f(x)",
      "",
    ]) {
      expect(() => rawKeyword(value)).toThrow();
    }
  });
});
