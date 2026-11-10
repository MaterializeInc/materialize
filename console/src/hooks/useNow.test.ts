// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useNow } from "./useNow";

describe("useNow", () => {
  beforeEach(() => {
    // The project default leaves `setInterval` real.
    vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
    vi.setSystemTime(1_793_000_000_000);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts at the current time and advances once per interval", () => {
    const { result } = renderHook(() => useNow(60_000));
    expect(result.current).toBe(1_793_000_000_000);

    act(() => vi.advanceTimersByTime(59_000));
    expect(result.current).toBe(1_793_000_000_000);

    act(() => vi.advanceTimersByTime(1_000));
    expect(result.current).toBe(1_793_000_060_000);
  });
});
