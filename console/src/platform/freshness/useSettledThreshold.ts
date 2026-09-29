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
 * `committed` seeds the hook and overrides it whenever it changes from
 * elsewhere, so a pasted link or the back button still moves the handle.
 */
export function useSettledThreshold(
  committed: number,
  commit: (value: number) => void,
): SettledThreshold {
  const [live, setLive] = React.useState(committed);

  // A change to `committed` that did not come from this hook, for example a
  // pasted URL, is the source of truth and takes the handle with it.
  React.useEffect(() => {
    setLive(committed);
  }, [committed]);

  const debouncedCommit = React.useMemo(
    () => debounce(commit, THRESHOLD_SETTLE_MS),
    [commit],
  );

  // Without this a drag that unmounts the page mid-flight still fires, writing
  // a search param for a route that is gone.
  React.useEffect(() => debouncedCommit.cancel, [debouncedCommit]);

  const onChange = React.useCallback(
    (value: number) => {
      setLive(value);
      debouncedCommit(value);
    },
    [debouncedCommit],
  );

  return { live, settled: committed, onChange };
}
