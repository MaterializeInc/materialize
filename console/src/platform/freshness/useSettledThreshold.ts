// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import debounce from "lodash.debounce";
import React from "react";

import { THRESHOLD_SETTLE_MS } from "./constants";

export interface SettledThreshold {
  /** Tracks the cursor. Drives the handle and the threshold line. */
  live: number;
  /** Trails `live` by `THRESHOLD_SETTLE_MS`. Drives everything else. */
  settled: number;
  onChange: (value: number) => void;
}

/**
 * Splits the threshold into the value the handle follows and the value the rest
 * of the page follows.
 *
 * A drag emits a value per pointer move. The handle has to keep up with all of
 * them or it lags the cursor, but the work behind it does not: writing the
 * search param re-renders the route and rebuilding the table re-renders a row
 * per object, and doing either at pointer-move rate is what makes dragging
 * stutter.
 *
 * So `live` updates on every tick and `settled` follows once the drag pauses.
 * The cost is that swatches in the table trail the colored lines for one settle
 * window mid-drag; they converge as soon as the handle stops.
 *
 * The only state here is the value of a drag still in flight. Everything else
 * derives from `committed`, so a pasted link or the back button moves the
 * handle with no effect to copy one into the other, and no render that shows
 * the stale value first.
 */
export function useSettledThreshold(
  committed: number,
  commit: (value: number) => void,
): SettledThreshold {
  const [dragValue, setDragValue] = React.useState<number | undefined>(
    undefined,
  );

  const debouncedCommit = React.useMemo(
    () =>
      debounce((value: number) => {
        // Both updates come from the same callback, so React batches them into
        // one render. Clearing the drag value in a later render would briefly
        // show `committed` before it caught up, which reads as the handle
        // springing back.
        commit(value);
        setDragValue(undefined);
      }, THRESHOLD_SETTLE_MS),
    [commit],
  );

  // Without this a drag that unmounts the page mid-flight still fires, writing
  // a search param for a route that is gone.
  React.useEffect(() => debouncedCommit.cancel, [debouncedCommit]);

  const onChange = React.useCallback(
    (value: number) => {
      setDragValue(value);
      debouncedCommit(value);
    },
    [debouncedCommit],
  );

  // A drag in flight outranks `committed`, which is still the pre-drag value
  // until the debounce fires.
  return { live: dragValue ?? committed, settled: committed, onChange };
}
