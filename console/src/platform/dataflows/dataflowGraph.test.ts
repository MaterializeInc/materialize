// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import {
  buildDataflowStructure,
  type ChannelRow,
  type LirSpanRow,
  nodeIdOf,
  type OperatorRow,
} from "./dataflowGraph";

// Dataflow 5: root [5], region [5,1] with children [5,1,1], [5,1,2], leaf [5,2].
export const OPS: OperatorRow[] = [
  {
    id: "10",
    address: ["5"],
    name: "Dataflow",
    arrangementRecords: null,
    arrangementSize: null,
    elapsedNs: "0",
  },
  {
    id: "11",
    address: ["5", "1"],
    name: "Region",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "5",
  },
  {
    id: "12",
    address: ["5", "1", "1"],
    name: "Join",
    arrangementRecords: "100",
    arrangementSize: "4096",
    elapsedNs: "7",
  },
  {
    id: "13",
    address: ["5", "1", "2"],
    name: "Map",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "1",
  },
  {
    id: "14",
    address: ["5", "2"],
    name: "Sink",
    arrangementRecords: "0",
    arrangementSize: "0",
    elapsedNs: "2",
  },
];
export const CHANNELS: ChannelRow[] = [
  // into the region: leaf [5,2] <- region output; region input port -> child
  {
    id: "1",
    fromOperatorAddress: ["5", "1", "0"],
    fromPort: "0",
    toOperatorAddress: ["5", "1", "1"],
    toPort: "0",
    messagesSent: "3",
    batchesSent: "1",
    channelType: "rows",
  },
  {
    id: "2",
    fromOperatorAddress: ["5", "1", "1"],
    fromPort: "0",
    toOperatorAddress: ["5", "1", "2"],
    toPort: "0",
    messagesSent: "5",
    batchesSent: "2",
    channelType: "rows",
  },
  {
    id: "3",
    fromOperatorAddress: ["5", "1"],
    fromPort: "0",
    toOperatorAddress: ["5", "2"],
    toPort: "0",
    messagesSent: "5",
    batchesSent: "2",
    channelType: "batches",
  },
];
export const LIR_SPANS: LirSpanRow[] = [
  {
    exportId: "u42",
    lirId: "1",
    operator: "Join::Differential",
    operatorIdStart: "11",
    operatorIdEnd: "13",
  },
];

describe("buildDataflowStructure", () => {
  it("builds the region tree from address prefixes", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.root).toEqual(nodeIdOf([5]));
    const root = s.nodes.get(s.root)!;
    expect(root.children).toEqual([nodeIdOf([5, 1]), nodeIdOf([5, 2])]);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.parent).toEqual(s.root);
    expect(region.children).toEqual([nodeIdOf([5, 1, 1]), nodeIdOf([5, 1, 2])]);
  });

  it("precomputes transitive stats", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    const region = s.nodes.get(nodeIdOf([5, 1]))!;
    expect(region.own.elapsedNs).toEqual(5n);
    expect(region.transitive.elapsedNs).toEqual(13n);
    expect(region.transitive.arrangementRecords).toEqual(100n);
    expect(region.transitive.arrangementSize).toEqual(4096n);
  });

  it("maps LIR spans to operators by id range, as an array", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, LIR_SPANS);
    expect(s.nodes.get(nodeIdOf([5, 1, 1]))!.lir).toEqual([
      { exportId: "u42", lirId: "1", operator: "Join::Differential" },
    ]);
    // end is exclusive
    expect(s.nodes.get(nodeIdOf([5, 2]))!.lir).toEqual([]);
  });

  it("throws without exactly one root", () => {
    expect(() => buildDataflowStructure([], [], [])).toThrow();
  });

  it("normalizes channels", () => {
    const s = buildDataflowStructure(OPS, CHANNELS, []);
    expect(s.channels[0]).toEqual({
      id: 1,
      fromAddress: [5, 1, 0],
      fromPort: 0,
      toAddress: [5, 1, 1],
      toPort: 0,
      messagesSent: 3n,
      batchesSent: 1n,
      channelType: "rows",
    });
  });
});
