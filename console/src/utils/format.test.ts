// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import { formatInterval, formatIntervalShort } from "./format";

describe("formatIntervalShort", () => {
  it("only returns years if present", () => {
    const interval = parse("1 years 2 mons 3 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("1 year");
  });

  it("rounds years up when appropriate", () => {
    const interval = parse("1 years 6 mons 3 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("2 years");
  });

  it("does not round years up from 0 to 1", () => {
    const interval = parse("6 mons 3 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("6 months");
  });

  it("only returns months if present", () => {
    const interval = parse("2 mons 3 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("2 months");
  });

  it("rounds months up when appropriate", () => {
    const interval = parse("2 mons 15 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("3 months");
  });

  it("does not round months up from 0 to 1", () => {
    const interval = parse("15 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("15 days");
  });

  it("only returns days if present", () => {
    const interval = parse("3 days 04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("3 days");
  });

  it("rounds days up when appropriate", () => {
    const interval = parse("3 days 12:05:06.789");
    expect(formatIntervalShort(interval)).toBe("4 days");
  });

  it("does not round days up from 0 to 1", () => {
    const interval = parse("12:05:06.789");
    expect(formatIntervalShort(interval)).toBe("12 hours");
  });

  it("only returns hours if present", () => {
    const interval = parse("04:05:06.789");
    expect(formatIntervalShort(interval)).toBe("4 hours");
  });

  it("rounds hours up when appropriate", () => {
    const interval = parse("04:30:06.789");
    expect(formatIntervalShort(interval)).toBe("5 hours");
  });

  it("does not round hours up from 0 to 1", () => {
    const interval = parse("00:30:06.789");
    expect(formatIntervalShort(interval)).toBe("30 minutes");
  });

  it("only returns minutes if present", () => {
    const interval = parse("00:05:06.789");
    expect(formatIntervalShort(interval)).toBe("5 minutes");
  });

  it("rounds minutes up when appropriate", () => {
    const interval = parse("00:05:30.789");
    expect(formatIntervalShort(interval)).toBe("6 minutes");
  });

  it("does not round minutes up from 0 to 1", () => {
    const interval = parse("00:00:30.123");
    expect(formatIntervalShort(interval)).toBe("30 seconds");
  });

  it("only returns seconds if present", () => {
    const interval = parse("00:00:06.123");
    expect(formatIntervalShort(interval)).toBe("6 seconds");
  });

  it("rounds seconds up when appropriate", () => {
    const interval = parse("00:00:06.789");
    expect(formatIntervalShort(interval)).toBe("7 seconds");
  });

  it("does not round seconds up from 0 to 1", () => {
    const interval = parse("00:00:00.789");
    expect(formatIntervalShort(interval)).toBe("< 1 second");
  });

  it("returns < 1 sec for milliseconds", () => {
    const interval = parse("00:00:00.123");
    expect(formatIntervalShort(interval)).toBe("< 1 second");
  });
});

describe("formatInterval", () => {
  it("formats interval with years, months, days, hours, minutes, seconds, and milliseconds", () => {
    const interval = parse("1 years 2 mons 3 days 04:05:06.789");
    expect(formatInterval(interval)).toBe("1y 2m 3d 4h 5m 6.789s");
  });

  it("formats interval with only seconds and milliseconds", () => {
    const interval = parse("00:00:12.345");
    expect(formatInterval(interval)).toBe("12.345s");
  });
});
