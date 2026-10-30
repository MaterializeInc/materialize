// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  autoScalingChange,
  defaultBurstSize,
  lingerInputValue,
  nextLargerSize,
  parseLinger,
} from "./autoScalingForm";

const lingerMs = (input: string) => {
  const parsed = parseLinger(input);
  return parsed.ok ? parsed.lingerMs : parsed.message;
};

describe("parseLinger", () => {
  it("reads milliseconds, seconds, minutes, hours and their sums", () => {
    expect(lingerMs("15s")).toBe(15_000);
    expect(lingerMs("2m")).toBe(120_000);
    expect(lingerMs("1500ms")).toBe(1_500);
    expect(lingerMs("1h")).toBe(3_600_000);
    expect(lingerMs("1m 30s")).toBe(90_000);
    expect(lingerMs(" 1.5S ")).toBe(1_500);
    expect(lingerMs("0s")).toBe(0);
  });

  it("reads an empty linger as the default", () => {
    expect(lingerMs("")).toBeUndefined();
    expect(lingerMs("  ")).toBeUndefined();
  });

  it("rejects a duration without a unit it knows", () => {
    for (const input of ["15", "abc", "15 seconds", "-5s", "2d", "s"]) {
      expect(lingerMs(input)).toBe("Enter a duration like 15s, 2m or 1500ms.");
    }
  });

  it("rejects a linger too long to write exactly", () => {
    expect(lingerMs("9999999999999h")).toBe("Linger duration is too long.");
  });
});

describe("lingerInputValue", () => {
  it("writes the largest unit that keeps the linger exact", () => {
    expect(lingerInputValue(undefined)).toBe("");
    expect(lingerInputValue(0)).toBe("0s");
    expect(lingerInputValue(3_600_000)).toBe("1h");
    expect(lingerInputValue(120_000)).toBe("2m");
    expect(lingerInputValue(90_000)).toBe("90s");
    expect(lingerInputValue(1_500)).toBe("1500ms");
    // `{ secs: 1, nanos: 32_053_000 }`, which parses to 1032.0529999999999.
    expect(lingerInputValue(1_000 + 32_053_000 / 1e6)).toBe("1032.053ms");
  });

  it("writes what parseLinger reads back", () => {
    for (const value of [0, 250, 1_500, 15_000, 90_000, 7_200_000]) {
      expect(lingerMs(lingerInputValue(value))).toBe(value);
    }
  });
});

describe("nextLargerSize", () => {
  const sizes = ["100cc", "400cc", "800cc"];

  it("picks the size after the cluster's", () => {
    expect(nextLargerSize(sizes, "400cc")).toBe("800cc");
  });

  it("picks nothing for the largest size or one not offered", () => {
    expect(nextLargerSize(sizes, "800cc")).toBe("");
    expect(nextLargerSize(sizes, "50cc")).toBe("");
    expect(nextLargerSize(sizes, null)).toBe("");
  });

  it("picks nothing past the end of the cluster's family", () => {
    // Families in the order `buildAvailableClusterReplicaSizesQuery` returns.
    const mixed = [
      "1600cc",
      "3200cc",
      "M.1-nano",
      "M.1-small",
      "D.1-small",
      "xsmall",
    ];
    expect(nextLargerSize(mixed, "1600cc")).toBe("3200cc");
    expect(nextLargerSize(mixed, "3200cc")).toBe("");
    expect(nextLargerSize(mixed, "M.1-nano")).toBe("M.1-small");
    expect(nextLargerSize(mixed, "M.1-small")).toBe("");
    expect(nextLargerSize(mixed, "D.1-small")).toBe("");
  });
});

describe("defaultBurstSize", () => {
  const sizes = ["100cc", "400cc", "800cc", "1600cc"];

  it("picks the size after the cluster's when nothing is resizing", () => {
    expect(defaultBurstSize(sizes, "400cc", undefined)).toBe("800cc");
  });

  it("picks the size after an in-progress resize's target", () => {
    expect(defaultBurstSize(sizes, "400cc", "800cc")).toBe("1600cc");
    expect(defaultBurstSize(sizes, "100cc", "1600cc")).toBe("");
  });

  it("skips the cluster's size after a resize down", () => {
    expect(defaultBurstSize(sizes, "800cc", "400cc")).toBe("1600cc");
  });
});

describe("autoScalingChange", () => {
  const current = { hydrationSize: "800cc", lingerMs: 15_000 };
  const form = (overrides = {}) => ({
    autoScalingEnabled: true,
    hydrationSize: "800cc",
    linger: "15s",
    ...overrides,
  });

  it("removes a policy that is turned off, and leaves an absent one alone", () => {
    expect(
      autoScalingChange(current, form({ autoScalingEnabled: false })),
    ).toBe(null);
    expect(
      autoScalingChange(null, form({ autoScalingEnabled: false })),
    ).toBeUndefined();
  });

  it("sends nothing for the policy already in place", () => {
    expect(autoScalingChange(current, form())).toBeUndefined();
    expect(
      autoScalingChange(current, form({ linger: "15000ms" })),
    ).toBeUndefined();
    expect(
      autoScalingChange(
        { hydrationSize: "800cc", lingerMs: undefined },
        form({ linger: "" }),
      ),
    ).toBeUndefined();
  });

  it("sets a new or changed policy", () => {
    expect(autoScalingChange(null, form())).toEqual(current);
    expect(
      autoScalingChange(current, form({ hydrationSize: "1600cc" })),
    ).toEqual({ hydrationSize: "1600cc", lingerMs: 15_000 });
    expect(autoScalingChange(current, form({ linger: "" }))).toEqual({
      hydrationSize: "800cc",
      lingerMs: undefined,
    });
  });

  it("builds no policy from an unchosen size or an unreadable linger", () => {
    expect(
      autoScalingChange(null, form({ hydrationSize: "" })),
    ).toBeUndefined();
    expect(
      autoScalingChange(current, form({ linger: "soon" })),
    ).toBeUndefined();
  });
});
