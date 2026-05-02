// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook } from "@testing-library/react";

import useDelayedLoading from "./useDelayedLoading";

describe("useDelayedLoading", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("returns false until the timeout has elapsed", () => {
    const { result } = renderHook(() => useDelayedLoading(true, 100));
    expect(result.current).toBe(false);
    act(() => vi.advanceTimersByTime(100));
    expect(result.current).toBe(true);
  });

  it("never returns true if loading is false before the timeout", () => {
    const { result, rerender } = renderHook(
      (loading) => useDelayedLoading(loading, 100),
      { initialProps: true },
    );
    expect(result.current).toBe(false);
    act(() => vi.advanceTimersByTime(99));
    expect(result.current).toBe(false);
    rerender(false);
    act(() => vi.advanceTimersByTime(100));
    expect(result.current).toBe(false);
  });
});
