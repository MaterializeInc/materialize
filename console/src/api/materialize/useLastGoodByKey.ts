// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

export interface UseLastGoodByKeyParams<TResult, TData> {
  // Null when there's nothing to fetch yet (e.g. required params missing).
  // Identifies the request a result belongs to (e.g. a JSON tuple of the
  // params that vary the query), not the query text itself.
  key: string | null;
  results: TResult | null | undefined;
  error: unknown;
  loading: boolean;
  // Must be referentially stable (e.g. wrapped in useCallback with an empty
  // or narrow dependency list) or this recomputes on every render even when
  // `results` hasn't actually changed.
  compute: (results: TResult) => TData;
}

/**
 * Retains the last successfully computed value for a given key, so a caller
 * keeps showing prior data while a new fetch for a *different* key is in
 * flight, without briefly flashing the previous key's data under the new
 * one. A result only replaces `lastGood` once a fetch has actually started
 * under the current key (see `sawLoadingRef` below): without that guard, a
 * key change followed by data that hasn't refetched yet would tag the old
 * fetch's still-resident result with the new key in the render before the
 * underlying query's own effect flips `loading` (CNS-109).
 */
export function useLastGoodByKey<TResult, TData>({
  key,
  results,
  error,
  loading,
  compute,
}: UseLastGoodByKeyParams<TResult, TData>): TData | null {
  const [lastGood, setLastGood] = React.useState<{
    key: string;
    data: TData;
  } | null>(null);

  const sawLoadingRef = React.useRef(false);

  // Reset acceptance synchronously when the key changes, using the
  // render-phase state-adjustment pattern so the reset lands before any
  // effect runs, not one render later.
  const [trackedKey, setTrackedKey] = React.useState(key);
  if (key !== trackedKey) {
    setTrackedKey(key);
    // Deliberate render-phase write, not a side effect: it must land before
    // this render commits, so a stale sawLoadingRef can't be read by the
    // effect below in the same commit (one render later would be too late).
    // eslint-disable-next-line react-compiler/react-compiler
    sawLoadingRef.current = false;
  }

  React.useEffect(() => {
    if (loading) sawLoadingRef.current = true;
    if (key && results != null && !error && !loading && sawLoadingRef.current) {
      setLastGood({ key, data: compute(results) });
    }
  }, [key, results, error, loading, compute]);

  // Error always wins, and data for other keys never renders.
  return !error && lastGood && lastGood.key === key ? lastGood.data : null;
}
