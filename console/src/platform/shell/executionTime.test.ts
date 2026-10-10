// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  EXECUTION_TIME_NOTICE_CODE,
  formatExecutionTime,
  isUnsupportedExecutionTimeSetting,
  parseExecutionTimeNotice,
} from "./executionTime";

const notice = (
  detail: string | undefined,
  code = EXECUTION_TIME_NOTICE_CODE,
) => ({
  code,
  message: "execution time: 12.345 ms (first_row)",
  severity: "Notice" as const,
  detail,
});

describe("parseExecutionTimeNotice", () => {
  it("parses the server's detail payload", () => {
    expect(
      parseExecutionTimeNotice(
        notice(
          '{"duration_us":12345,"kind":"first_row","strategy":"fast-path"}',
        ),
      ),
    ).toEqual({ kind: "first_row", durationMs: 12.345, strategy: "fast-path" });
  });

  it("accepts a missing strategy", () => {
    expect(
      parseExecutionTimeNotice(
        notice('{"duration_us":500,"kind":"committed","strategy":null}'),
      ),
    ).toEqual({ kind: "committed", durationMs: 0.5, strategy: null });
  });

  it.each([
    [
      "another notice code",
      notice('{"duration_us":1,"kind":"completed"}', "00000"),
    ],
    ["no detail", notice(undefined)],
    ["malformed JSON", notice("{")],
    ["an unknown kind", notice('{"duration_us":1,"kind":"later"}')],
    [
      "a non-numeric duration",
      notice('{"duration_us":"1","kind":"completed"}'),
    ],
  ])("returns null for %s", (_, input) => {
    expect(parseExecutionTimeNotice(input)).toBeNull();
  });
});

describe("formatExecutionTime", () => {
  it("names where the interval ends and how a read was served", () => {
    expect(
      formatExecutionTime({
        kind: "first_row",
        durationMs: 12,
        strategy: "fast-path",
      }),
    ).toBe("12.0ms to first row (served from an index)");
    expect(
      formatExecutionTime({
        kind: "committed",
        durationMs: 1500,
        strategy: null,
      }),
    ).toBe("1.50s in Materialize, including commit");
  });
});

describe("isUnsupportedExecutionTimeSetting", () => {
  it("matches only the startup notice for this session variable", () => {
    const startup = (variable: string) => ({
      code: "00000",
      message: `startup setting ${variable} not set: unrecognized configuration parameter "${variable}"`,
      severity: "Notice" as const,
    });
    expect(
      isUnsupportedExecutionTimeSetting(startup("emit_execution_time_notice")),
    ).toBe(true);
    expect(isUnsupportedExecutionTimeSetting(startup("cluster"))).toBe(false);
  });
});
