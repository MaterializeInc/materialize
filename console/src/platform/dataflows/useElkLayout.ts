// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import ELK, { type ElkNode } from "elkjs/lib/elk-api.js";
// The elk engine detects a worker context at load time and registers itself
// as the worker's message handler, so it must be the worker entry point
// itself. A hand-written worker wrapping it cannot work: imported into
// another worker the engine hijacks that worker's onmessage and exports
// nothing. elk-api on the main thread speaks to it and correlates
// request/response pairs internally.
import ElkWorker from "elkjs/lib/elk-worker.min.js?worker";
import React from "react";

import type { LirGrouping, VisibleGraph } from "./dataflowGraph";
import { extractPositions, type Positions, toElkGraph } from "./elkGraph";

export function useElkLayout(
  graph: VisibleGraph | null,
  cacheKey: string,
  grouping?: LirGrouping,
) {
  const elkRef = React.useRef<InstanceType<typeof ELK> | null>(null);
  const requestIdRef = React.useRef(0);
  const cacheRef = React.useRef(new Map<string, Positions>());
  const [state, setState] = React.useState<{
    key: string | null;
    positions: Positions | null;
    error: string | null;
  }>({ key: null, positions: null, error: null });
  // Bumped by retry() to force the layout effect to run again for the same
  // (graph, cacheKey): a failed layout is never cached, so re-running it is
  // enough to retry, but the effect's own dependencies wouldn't otherwise
  // change.
  const [retryNonce, setRetryNonce] = React.useState(0);

  React.useEffect(() => {
    const elk = new ELK({ workerFactory: () => new ElkWorker() });
    elkRef.current = elk;
    return () => {
      elk.terminateWorker();
    };
  }, []);

  React.useEffect(() => {
    if (!graph) return;
    const cached = cacheRef.current.get(cacheKey);
    if (cached) {
      setState({ key: cacheKey, positions: cached, error: null });
      return;
    }
    const elk = elkRef.current;
    if (!elk) return;
    const requestId = ++requestIdRef.current;
    elk.layout(toElkGraph(graph, grouping)).then(
      (layouted: ElkNode) => {
        // Drop responses for superseded requests.
        if (requestId !== requestIdRef.current) return;
        const positions = extractPositions(layouted);
        cacheRef.current.set(cacheKey, positions);
        setState({ key: cacheKey, positions, error: null });
      },
      (error: unknown) => {
        if (requestId !== requestIdRef.current) return;
        setState({ key: cacheKey, positions: null, error: String(error) });
      },
    );
  }, [graph, cacheKey, retryNonce, grouping]);

  return {
    positions: state.key === cacheKey ? state.positions : null,
    layouting: graph !== null && state.key !== cacheKey,
    error: state.key === cacheKey ? state.error : null,
    retry: () => setRetryNonce((n) => n + 1),
  };
}
