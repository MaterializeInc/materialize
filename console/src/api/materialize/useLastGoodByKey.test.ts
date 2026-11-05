// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook } from "@testing-library/react";

import {
  useLastGoodByKey,
  type UseLastGoodByKeyParams,
} from "./useLastGoodByKey";

type Params = UseLastGoodByKeyParams<string, string>;

function setup(initial: Params) {
  return renderHook((props: Params) => useLastGoodByKey(props), {
    initialProps: initial,
  });
}

const compute = (r: string) => `computed:${r}`;

// A real query hook always starts loading on first mount for a fresh key,
// only later flipping to `loading: false` with a result. Mounting directly
// in the already-loaded state (skipping that transition) would never set
// sawLoadingRef, so this drives a hook through the realistic sequence
// before a test builds further scenarios on top of an accepted result.
function primeAccepted(
  key: string,
  results: string,
  fn: (r: string) => string,
) {
  const rendered = setup({
    key,
    results: null,
    error: null,
    loading: true,
    compute: fn,
  });
  rendered.rerender({ key, results, error: null, loading: false, compute: fn });
  return rendered;
}

describe("useLastGoodByKey", () => {
  it("returns null until a result arrives", () => {
    const { result } = setup({
      key: "a",
      results: null,
      error: null,
      loading: true,
      compute,
    });
    expect(result.current).toBeNull();
  });

  it("accepts a result once loading finishes for the same key", () => {
    const { result, rerender } = setup({
      key: "a",
      results: null,
      error: null,
      loading: true,
      compute,
    });
    rerender({ key: "a", results: "r1", error: null, loading: false, compute });
    expect(result.current).toEqual("computed:r1");
  });

  it("keeps showing the previous key's data while a new key is loading, not stale data mislabeled under the new key", () => {
    const { result, rerender } = primeAccepted("a", "r1", compute);
    expect(result.current).toEqual("computed:r1");

    // Key changes; a fetch hasn't started yet (loading still false from the
    // previous render). Without CNS-109's guard, the still-resident "r1"
    // result would get relabeled as belonging to key "b".
    rerender({ key: "b", results: "r1", error: null, loading: false, compute });
    expect(result.current).toBeNull();

    // The new key's fetch starts (loading flips true), still with no new
    // result yet.
    rerender({ key: "b", results: "r1", error: null, loading: true, compute });
    expect(result.current).toBeNull();

    // The new key's own result arrives.
    rerender({ key: "b", results: "r2", error: null, loading: false, compute });
    expect(result.current).toEqual("computed:r2");
  });

  it("clears data immediately on error, even before a refetch resolves", () => {
    const { result, rerender } = primeAccepted("a", "r1", compute);
    expect(result.current).toEqual("computed:r1");

    rerender({
      key: "a",
      results: "r1",
      error: "boom",
      loading: false,
      compute,
    });
    expect(result.current).toBeNull();
  });

  it("recomputes only when compute is referentially stable across renders with the same results", () => {
    let calls = 0;
    const countingCompute = (r: string) => {
      calls++;
      return `computed:${r}`;
    };
    const { rerender } = primeAccepted("a", "r1", countingCompute);
    expect(calls).toEqual(1);
    rerender({
      key: "a",
      results: "r1",
      error: null,
      loading: false,
      compute: countingCompute,
    });
    rerender({
      key: "a",
      results: "r1",
      error: null,
      loading: false,
      compute: countingCompute,
    });
    expect(calls).toEqual(1);
  });
});
