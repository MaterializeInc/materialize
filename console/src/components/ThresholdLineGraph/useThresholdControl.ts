// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLatestRef } from "@chakra-ui/react";
import React from "react";

import {
  THRESHOLD_DRAG_SETTLE_MS,
  THRESHOLD_INPUT_SETTLE_MS,
  THRESHOLD_STEP_MS,
} from "./constants";

export interface ThresholdControl {
  /** Spread onto `ThresholdInput`. */
  inputProps: {
    valueMs: number;
    onChange: (value: number) => void;
  };
  /** Spread onto `ThresholdLineGraph`, alongside its data props. */
  graphProps: {
    threshold: number;
    onThresholdChange: (value: number) => void;
    thresholdStep: number;
  };
  /**
   * The value everything other than the two controls should read: tables,
   * counts, headlines. Trails an edit by that control's settle window.
   */
  settled: number;
}

/**
 * Drives a threshold that a reader can drag on a graph or type in a field.
 *
 * Both controls edit one number, and they are rarely adjacent on screen: a
 * field usually sits in a page header while the graph sits lower down. So the
 * value they share has to live above both, which is here.
 *
 * The two controls stop differently, so each gets its own settle window. This
 * hook builds both prop objects so a caller cannot pair the wrong window with
 * the wrong control, and so adding the pair to a page is three lines rather
 * than six.
 *
 * `committed` is the value of record, usually a search param, and `commit`
 * writes it back. Anything not mid-edit derives from `committed`, so a pasted
 * link or the back button moves both controls.
 */
export function useThresholdControl(
  committed: number,
  commit: (value: number) => void,
): ThresholdControl {
  const [pending, setPending] = React.useState<number | undefined>(undefined);
  const timer = React.useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );

  // NOTE: a ref, not a dependency. `commit` often reaches us through
  // react-router's `setSearchParams`, which is rebuilt whenever any search
  // param changes. As a dependency it would rebuild the timer when an
  // unrelated control moved, cancelling the edit in flight along with the
  // state reset riding on it, leaving a control stuck on a stale value.
  const commitRef = useLatestRef(commit);

  // Without this, an edit that unmounts the page mid-flight still fires,
  // writing to a route that is gone.
  React.useEffect(() => () => clearTimeout(timer.current), []);

  const change = React.useCallback(
    (value: number, settleMs: number) => {
      setPending(value);
      clearTimeout(timer.current);
      timer.current = setTimeout(() => {
        // Both updates come from one callback, so React batches them into a
        // single render. Clearing `pending` later would show `committed`
        // before it caught up, which reads as the control springing back.
        commitRef.current(value);
        setPending(undefined);
      }, settleMs);
    },
    [commitRef],
  );

  const live = pending ?? committed;

  const onInputChange = React.useCallback(
    (value: number) => change(value, THRESHOLD_INPUT_SETTLE_MS),
    [change],
  );
  const onDragChange = React.useCallback(
    (value: number) => change(value, THRESHOLD_DRAG_SETTLE_MS),
    [change],
  );

  // Memoized because a drag re-renders the page on every pointer move, and a
  // rebuilt prop object would defeat the memo on whatever receives it.
  const inputProps = React.useMemo(
    () => ({ valueMs: live, onChange: onInputChange }),
    [live, onInputChange],
  );
  const graphProps = React.useMemo(
    () => ({
      threshold: live,
      onThresholdChange: onDragChange,
      thresholdStep: THRESHOLD_STEP_MS,
    }),
    [live, onDragChange],
  );

  return { inputProps, graphProps, settled: committed };
}
