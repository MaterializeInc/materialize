// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import { Node as DagreNode } from "@dagrejs/dagre";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const STROKE_WIDTH = 2;

export const GraphEdge = (props: {
  from: DagreNode;
  to: DagreNode;
  points: { x: number; y: number }[];
  isAdjacentToSelectedNode?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { points } = props;
  const [start, ...rest] = points;

  const end = rest.pop();
  if (!end) return null;

  // Some edges only have a single control point, others have 2
  const controlPoints = rest.map((p) => `${p.x}, ${p.y}`).join(" ");

  const markerEnd = `url(#circle${props.isAdjacentToSelectedNode ? "-focused" : "-default"})`;

  return (
    <path
      d={`M${start.x},${start.y} S ${controlPoints} ${end.x},${end.y}`}
      strokeWidth={STROKE_WIDTH}
      markerEnd={markerEnd}
      // For some reason, in Safari, changing the marker reference for "markerEnd" doesn't apply the changes. This is a hack that
      // forces React to remount it when the markerEnd changes, forcing the appropriate DOM operations.
      // Filed an issue here: https://github.com/facebook/react/issues/29672
      key={markerEnd}
      {...(props.isAdjacentToSelectedNode
        ? {
            stroke: colors.accent.brightPurple,
            strokeDasharray: "8 8",
            style: { animation: "dash 20s linear infinite" },
          }
        : { stroke: colors.border.secondary })}
    />
  );
};
