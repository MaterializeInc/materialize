// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  describeExecutionTime,
  EXECUTION_TIME_NOTICE_CODE,
  findExecutionTime,
  formatExecutionTime,
  parseExecutionTimeNotice,
} from "./executionTime";

const notice = (
  detail: string | undefined,
  code = EXECUTION_TIME_NOTICE_CODE,
) => ({
  code,
  message: "execution time: 12.345 ms",
  severity: "Notice" as const,
  detail,
});

describe("parseExecutionTimeNotice", () => {
  it("parses the server's detail payload", () => {
    expect(
      parseExecutionTimeNotice(
        notice('{"duration_us":12345,"kind":"first_row"}'),
      ),
    ).toEqual({ kind: "first_row", durationMs: 12.345 });
  });

  it("ignores fields it does not know, such as one a later server adds", () => {
    expect(
      parseExecutionTimeNotice(
        notice(
          '{"duration_us":500,"kind":"completed","strategy":"index scan"}',
        ),
      ),
    ).toEqual({ kind: "completed", durationMs: 0.5 });
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

describe("findExecutionTime", () => {
  it("finds the execution time among a statement's notices", () => {
    expect(
      findExecutionTime([
        notice('{"duration_us":1,"kind":"completed"}', "00000"),
        notice('{"duration_us":2000,"kind":"completed"}'),
      ]),
    ).toEqual({ kind: "completed", durationMs: 2 });
  });

  it("returns null when no notice carries one it can read", () => {
    expect(findExecutionTime([])).toBeNull();
    expect(findExecutionTime([notice("{")])).toBeNull();
  });
});

describe("describeExecutionTime", () => {
  it("labels the tooltip row like the footer, capitalized", () => {
    expect(describeExecutionTime({ kind: "staged", durationMs: 1 })).toEqual({
      label: "In Materialize, staged",
      description:
        "The time Materialize took for the statement, without the commit of its transaction.",
    });
  });

  it("explains that a read's time leaves out sending the rows", () => {
    expect(
      describeExecutionTime({ kind: "first_row", durationMs: 1 }).description,
    ).toMatch(/does not include sending the rows/);
  });
});

describe("formatExecutionTime", () => {
  it("labels a read and a completed statement", () => {
    expect(formatExecutionTime({ kind: "first_row", durationMs: 12 })).toBe(
      "12.0ms in Materialize",
    );
    expect(formatExecutionTime({ kind: "completed", durationMs: 1500 })).toBe(
      "1.50s in Materialize",
    );
  });

  it.each([
    ["empty_result", "3.0ms in Materialize"],
    ["completed", "3.0ms in Materialize"],
    ["staged", "3.0ms in Materialize, staged"],
  ] as const)("labels %s", (kind, expected) => {
    expect(formatExecutionTime({ kind, durationMs: 3 })).toBe(expected);
  });

  it("shows sub-0.1 ms times as <0.1ms", () => {
    expect(formatExecutionTime({ kind: "completed", durationMs: 0.04 })).toBe(
      "<0.1ms in Materialize",
    );
  });
});
