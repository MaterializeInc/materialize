// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, Tooltip } from "@chakra-ui/react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  type EdgeProps,
  getBezierPath,
} from "@xyflow/react";
import React from "react";

import { HIGHLIGHT_COLORS } from "./nodeStyle";

export type ChannelEdgeData = {
  messagesSent: bigint;
  batchesSent: bigint;
  channelTypes: string[];
  dimmed: boolean;
  selected: boolean;
};

const compactCount = Intl.NumberFormat("default", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const compact = (n: bigint) => compactCount.format(n);

export const ChannelEdge = (props: EdgeProps & { data: ChannelEdgeData }) => {
  const [path, labelX, labelY] = getBezierPath(props);
  const { messagesSent, batchesSent, channelTypes, dimmed, selected } =
    props.data;
  const idle = messagesSent === 0n;
  // Inline label stays terse: compact record/batch counts only. Exact figures
  // and the full container type names live in the tooltip, since those Rust
  // type strings are long enough to swamp the canvas. Selecting an idle edge
  // still shows its (zero) counts, confirming the click landed.
  const label =
    idle && !selected
      ? ""
      : `${compact(messagesSent)} / ${compact(batchesSent)}`;
  const tooltip = idle
    ? channelTypes.join(", ") || "unknown channel type"
    : `${messagesSent} records / ${batchesSent} batches` +
      (channelTypes.length > 0 ? ` · ${channelTypes.join(", ")}` : "");
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          strokeDasharray: idle ? "6 4" : undefined,
          opacity: dimmed ? 0.15 : 1,
          stroke: selected ? HIGHLIGHT_COLORS.selected : undefined,
          strokeWidth: selected ? 2.5 : undefined,
        }}
      />
      {label && (
        <EdgeLabelRenderer>
          <Tooltip label={tooltip}>
            <Text
              position="absolute"
              transform={`translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`}
              fontSize="2xs"
              background="whiteAlpha.800"
              px={1}
              borderRadius="sm"
              opacity={dimmed ? 0.15 : 1}
              className="nopan"
            >
              {label}
            </Text>
          </Tooltip>
        </EdgeLabelRenderer>
      )}
    </>
  );
};
