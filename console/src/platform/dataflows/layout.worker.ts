// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import ELK from "elkjs/lib/elk.bundled.js";

import type { VisibleGraph } from "./dataflowGraph";
import { extractPositions, type Positions, toElkGraph } from "./elkGraph";

export interface LayoutRequest {
  requestId: number;
  graph: VisibleGraph;
}

export interface LayoutResponse {
  requestId: number;
  positions?: Positions;
  error?: string;
}

const elk = new ELK();

self.onmessage = async (event: MessageEvent<LayoutRequest>) => {
  const { requestId, graph } = event.data;
  try {
    const layouted = await elk.layout(toElkGraph(graph));
    const response: LayoutResponse = {
      requestId,
      positions: extractPositions(layouted),
    };
    self.postMessage(response);
  } catch (error) {
    self.postMessage({
      requestId,
      error: String(error),
    } satisfies LayoutResponse);
  }
};
