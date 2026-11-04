// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { WorkflowGraphEdgeRow } from "./workflowGraphEdges";
import { traverseWorkflowGraph } from "./workflowGraphTraversal";

// Chain: u1 <- u2 <- u4 <- u5 <- u6, plus a sibling u1 <- u3.
const EDGES: WorkflowGraphEdgeRow[] = [
  { childId: "u2", parentId: "u1" },
  { childId: "u3", parentId: "u1" },
  { childId: "u4", parentId: "u2" },
  { childId: "u5", parentId: "u4" },
  { childId: "u6", parentId: "u5" },
];

const keys = (edges: { childId: string | null; parentId: string | null }[]) =>
  edges.map((e) => `${e.childId}->${e.parentId}`).sort();

describe("traverseWorkflowGraph", () => {
  it("collects both upstream and downstream from a middle node", () => {
    // From u4: upstream u4<-u2<-u1, downstream u4->u5->u6. Sibling u3 is not reachable.
    expect(keys(traverseWorkflowGraph(EDGES, "u4"))).toEqual([
      "u2->u1",
      "u4->u2",
      "u5->u4",
      "u6->u5",
    ]);
  });

  it("collects the whole subtree from a root source", () => {
    expect(keys(traverseWorkflowGraph(EDGES, "u1"))).toEqual([
      "u2->u1",
      "u3->u1",
      "u4->u2",
      "u5->u4",
      "u6->u5",
    ]);
  });

  it("returns only upstream edges from a leaf", () => {
    expect(keys(traverseWorkflowGraph(EDGES, "u6"))).toEqual([
      "u2->u1",
      "u4->u2",
      "u5->u4",
      "u6->u5",
    ]);
  });

  it("returns nothing for an object not in the graph", () => {
    expect(traverseWorkflowGraph(EDGES, "u999")).toEqual([]);
    expect(traverseWorkflowGraph([], "u1")).toEqual([]);
  });

  it("dedupes duplicate input edges", () => {
    const dup = [...EDGES, { childId: "u2", parentId: "u1" }];
    expect(keys(traverseWorkflowGraph(dup, "u2"))).toEqual([
      "u2->u1",
      "u4->u2",
      "u5->u4",
      "u6->u5",
    ]);
  });

  it("terminates on a cycle", () => {
    const cyclic: WorkflowGraphEdgeRow[] = [
      { childId: "u2", parentId: "u1" },
      { childId: "u1", parentId: "u2" },
    ];
    expect(keys(traverseWorkflowGraph(cyclic, "u1"))).toEqual([
      "u1->u2",
      "u2->u1",
    ]);
  });
});
