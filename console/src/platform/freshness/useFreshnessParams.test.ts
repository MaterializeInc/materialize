// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  parseObjectTypes,
  parsePositiveNumber,
  parsePredicate,
  parseTimePeriod,
} from "./useFreshnessParams";

describe("parsePositiveNumber", () => {
  it("reads a number", () => {
    expect(parsePositiveNumber("2500")).toBe(2500);
    expect(parsePositiveNumber("0")).toBe(0);
    expect(parsePositiveNumber("1.5")).toBe(1.5);
  });

  it("rejects anything that is not one", () => {
    // A NaN threshold compares false against every line, so the graph would
    // quietly highlight nothing rather than fail visibly.
    expect(parsePositiveNumber("abc")).toBeNull();
    expect(parsePositiveNumber("")).toBeNull();
    expect(parsePositiveNumber("  ")).toBeNull();
    expect(parsePositiveNumber(null)).toBeNull();
    expect(parsePositiveNumber("Infinity")).toBeNull();
  });

  it("rejects negatives, which no duration can be", () => {
    expect(parsePositiveNumber("-1")).toBeNull();
  });
});

describe("parseTimePeriod prototype keys", () => {
  it("rejects inherited keys", () => {
    // These used to parse to NaN and reach the query as
    // `INTERVAL 'NaN MILLISECONDS'`.
    for (const key of PROTOTYPE_KEYS) {
      expect(parseTimePeriod(key)).toBeNull();
    }
  });
});

describe("parseTimePeriod", () => {
  it("accepts a period the selector offers", () => {
    expect(parseTimePeriod("60")).toBe(60);
    expect(parseTimePeriod("1440")).toBe(1440);
  });

  it("rejects one it does not", () => {
    // Honouring an arbitrary window would leave the selector showing a value
    // it has no option for.
    expect(parseTimePeriod("999")).toBeNull();
    expect(parseTimePeriod("abc")).toBeNull();
    expect(parseTimePeriod(null)).toBeNull();
  });
});

// Every object inherits these, so an `in` check accepts them as menu values.
const PROTOTYPE_KEYS = ["constructor", "toString", "valueOf", "__proto__"];

describe("parsePredicate", () => {
  it("accepts a predicate the menu offers", () => {
    expect(parsePredicate("peak")).toBe("peak");
  });

  it("rejects inherited keys", () => {
    // `toString` used to pass and then fall through `statFor` to p90, so the
    // page judged by a statistic the menu was not showing.
    for (const key of PROTOTYPE_KEYS) {
      expect(parsePredicate(key)).toBeNull();
    }
  });
});

describe("parseObjectTypes", () => {
  it("reads a comma separated list", () => {
    expect(parseObjectTypes("source,index")).toEqual(["source", "index"]);
    expect(parseObjectTypes(" source , sink ")).toEqual(["source", "sink"]);
  });

  it("is empty when the param is absent", () => {
    expect(parseObjectTypes(null)).toEqual([]);
    expect(parseObjectTypes("")).toEqual([]);
  });

  it("drops types the menu does not offer", () => {
    // An empty result means no filter, so a stale or hand-edited param shows
    // everything rather than an empty page.
    expect(parseObjectTypes("source,table,nonsense")).toEqual(["source"]);
    expect(parseObjectTypes("table")).toEqual([]);
  });
});
