// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useThresholdControl } from "./useThresholdControl";

describe("useThresholdControl", () => {
  it("shows an edit on both controls at once and holds the commit back", () => {
    const commit = vi.fn();
    const { result } = renderHook(() => useThresholdControl(1_000, commit));

    act(() => result.current.graphProps.onThresholdChange(5_000));

    // One value, two controls: dragging the line moves the number in the
    // field, and the rest of the page waits.
    expect(result.current.graphProps.threshold).toBe(5_000);
    expect(result.current.inputProps.valueMs).toBe(5_000);
    expect(result.current.settled).toBe(1_000);
    expect(commit).not.toHaveBeenCalled();
  });

  it("commits only the last value of a burst", async () => {
    const commit = vi.fn();
    const { result } = renderHook(() => useThresholdControl(1_000, commit));

    act(() => {
      result.current.graphProps.onThresholdChange(2_000);
      result.current.graphProps.onThresholdChange(3_000);
      result.current.graphProps.onThresholdChange(4_000);
    });

    await waitFor(() => expect(commit).toHaveBeenCalledTimes(1));
    expect(commit).toHaveBeenCalledWith(4_000);
  });

  it("gives the field a longer window than the drag", async () => {
    const commit = vi.fn();
    const { result } = renderHook(() => useThresholdControl(1_000, commit));

    act(() => result.current.inputProps.onChange(7_000));
    // Past the drag window, still inside the field's.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(commit).not.toHaveBeenCalled();

    await waitFor(() => expect(commit).toHaveBeenCalledWith(7_000));
  });

  it("keeps an edit in flight when the commit function is replaced", async () => {
    // `commit` usually reaches this hook through react-router's
    // `setSearchParams`, which is rebuilt whenever any search param changes.
    // Changing another control mid-edit therefore hands the hook a new
    // function. Depending on it used to rebuild the timer and discard the call
    // in flight, along with the state reset riding on that call: the controls
    // then stayed on the edited value and `committed` could never move them.
    const replacement: (value: number) => void = vi.fn();
    const original: (value: number) => void = vi.fn();
    const { result, rerender } = renderHook(
      ({ commit }) => useThresholdControl(1_000, commit),
      { initialProps: { commit: original } },
    );

    act(() => result.current.graphProps.onThresholdChange(5_000));
    rerender({ commit: replacement });

    await waitFor(() => expect(replacement).toHaveBeenCalledWith(5_000));
    await waitFor(() =>
      expect(result.current.graphProps.threshold).toBe(1_000),
    );
  });

  it("follows a value that changed elsewhere once no edit is in flight", () => {
    const { result, rerender } = renderHook(
      ({ committed }) => useThresholdControl(committed, vi.fn()),
      { initialProps: { committed: 1_000 } },
    );

    // A pasted link or the back button.
    rerender({ committed: 9_000 });

    expect(result.current.graphProps.threshold).toBe(9_000);
    expect(result.current.inputProps.valueMs).toBe(9_000);
  });

  it("keeps each prop object stable while nothing changes", () => {
    const commit = vi.fn();
    const { result, rerender } = renderHook(() =>
      useThresholdControl(1_000, commit),
    );

    const first = result.current.graphProps;
    rerender();

    // A rebuilt object each render would reach a memoized graph as a changed
    // prop, which is what the memo exists to avoid.
    expect(result.current.graphProps).toBe(first);
  });
});
