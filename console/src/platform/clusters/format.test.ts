// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import { formatLagInfoDetailed, formatLagInfoSimple } from "./format";

describe("WorkflowGraph format", () => {
  describe("formatLagInfoSimple", () => {
    it("returns '-' for null values ", async () => {
      expect(formatLagInfoSimple(null)).toEqual("-");
    });

    it("returns 'Not hydrated' for non-hydrated values", async () => {
      expect(
        formatLagInfoSimple({
          lag: parse("00:00:00"),
          hydrated: false,
          isOutdated: false,
        }),
      ).toEqual("Not hydrated");
    });

    it("returns 'Less than 1s' for zero duration", async () => {
      expect(
        formatLagInfoSimple({
          lag: parse("00:00:00"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("Less than 1s");
    });

    it("returns seconds for any value less than 1 minute", async () => {
      expect(
        formatLagInfoSimple({
          lag: parse("00:00:00.999"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("Less than 1s");
      expect(
        formatLagInfoSimple({
          lag: parse("00:00:01.000"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("1s");
      expect(
        formatLagInfoSimple({
          lag: parse("00:00:59.123"),
          hydrated: true,
          isOutdated: true,
        }),
      ).toEqual("59s");
    });

    it("returns minutes for exactly 1 minute", async () => {
      const result = formatLagInfoSimple({
        lag: parse("00:01:00"),
        hydrated: true,
        isOutdated: true,
      });
      expect(result).toEqual("1m");
    });

    it("returns minutes for anything less than 1 hour", async () => {
      const result = formatLagInfoSimple({
        lag: parse("00:59:01"),
        hydrated: true,
        isOutdated: true,
      });
      expect(result).toEqual("59m");
    });

    it("returns hours for exactly 1 hour", async () => {
      const result = formatLagInfoSimple({
        lag: parse("01:00:00"),
        hydrated: true,
        isOutdated: true,
      });
      expect(result).toEqual("1h");
    });

    it("returns hours for anything larger than one hour", async () => {
      const result = formatLagInfoSimple({
        lag: parse("48:59:01"),
        hydrated: true,
        isOutdated: true,
      });
      expect(result).toEqual("48h");
    });
  });

  describe("formatLagInfoDetailed", () => {
    it("returns '-' for null values", async () => {
      expect(formatLagInfoDetailed(null)).toEqual("-");
    });

    it("returns 'Not hydrated' for non-hydrated values", async () => {
      expect(
        formatLagInfoDetailed({
          lag: parse("00:00:00"),
          hydrated: false,
          isOutdated: false,
        }),
      ).toEqual("Not hydrated");
    });

    it("returns 'Less than 1s' for zero duration", async () => {
      expect(
        formatLagInfoDetailed({
          lag: parse("00:00:00"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("Less than 1s");
    });

    it("returns seconds for small values", async () => {
      expect(
        formatLagInfoDetailed({
          lag: parse("00:00:00.999"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("Less than 1s");
      expect(
        formatLagInfoDetailed({
          lag: parse("00:00:01.000"),
          hydrated: true,
          isOutdated: false,
        }),
      ).toEqual("1s");
      expect(
        formatLagInfoDetailed({
          lag: parse("00:00:59.123"),
          hydrated: true,
          isOutdated: true,
        }),
      ).toEqual("59s");
    });

    it("returns hours and minutes", async () => {
      expect(
        formatLagInfoDetailed({
          lag: parse("01:01:00"),
          hydrated: true,
          isOutdated: true,
        }),
      ).toEqual("1h 1m");
    });

    it("returns hours, minutes and seconds", async () => {
      expect(
        formatLagInfoDetailed({
          lag: parse("48:20:50.123"),
          hydrated: true,
          isOutdated: true,
        }),
      ).toEqual("48h 20m 50s");
    });
  });
});
