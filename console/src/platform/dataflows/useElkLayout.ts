// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import type { VisibleGraph } from "./dataflowGraph";
import type { Positions } from "./elkGraph";
import type { LayoutRequest, LayoutResponse } from "./layout.worker";

export function useElkLayout(graph: VisibleGraph | null, cacheKey: string) {
  const workerRef = React.useRef<Worker | null>(null);
  const requestIdRef = React.useRef(0);
  const cacheRef = React.useRef(new Map<string, Positions>());
  const [state, setState] = React.useState<{
    key: string | null;
    positions: Positions | null;
    error: string | null;
  }>({ key: null, positions: null, error: null });

  React.useEffect(() => {
    const worker = new Worker(new URL("./layout.worker.ts", import.meta.url), {
      type: "module",
    });
    workerRef.current = worker;
    return () => worker.terminate();
  }, []);

  React.useEffect(() => {
    if (!graph) return;
    const cached = cacheRef.current.get(cacheKey);
    if (cached) {
      setState({ key: cacheKey, positions: cached, error: null });
      return;
    }
    const requestId = ++requestIdRef.current;
    const worker = workerRef.current;
    if (!worker) return;
    const onMessage = (event: MessageEvent<LayoutResponse>) => {
      // Drop responses for superseded requests.
      if (event.data.requestId !== requestIdRef.current) return;
      if (event.data.positions) {
        cacheRef.current.set(cacheKey, event.data.positions);
        setState({
          key: cacheKey,
          positions: event.data.positions,
          error: null,
        });
      } else {
        setState({
          key: cacheKey,
          positions: null,
          error: event.data.error ?? "layout failed",
        });
      }
    };
    worker.addEventListener("message", onMessage);
    worker.postMessage({ requestId, graph } satisfies LayoutRequest);
    return () => worker.removeEventListener("message", onMessage);
  }, [graph, cacheKey]);

  return {
    positions: state.key === cacheKey ? state.positions : null,
    layouting: graph !== null && state.key !== cacheKey,
    error: state.key === cacheKey ? state.error : null,
  };
}
