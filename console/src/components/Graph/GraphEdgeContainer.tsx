// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Icon, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

export const GraphEdgeContainer = ({
  width,
  height,
  children,
}: React.PropsWithChildren<{
  width: number;
  height: number;
}>) => {
  const { colors } = useTheme<MaterializeTheme>();

  const markerProps = {
    viewBox: "0 0 10 10",
    refX: "5",
    refY: "5",
    markerUnits: "strokeWidth",
    markerWidth: "10",
    markerHeight: "10",
    orient: "auto",
  };

  const circleProps = {
    cx: "5",
    cy: "5",
    r: "2",
    stroke: colors.background.primary,
    strokeWidth: "1",
  };

  return (
    <Icon
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      top="0"
      left="0"
      position="absolute"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      pointerEvents="none"
      sx={{
        "@keyframes dash": {
          to: {
            strokeDashoffset: -1000,
          },
        },
      }}
    >
      <defs>
        <marker {...markerProps} id="circle-default">
          <circle {...circleProps} fill={colors.background.secondary} />
        </marker>
        <marker {...markerProps} id="circle-focused">
          <circle {...circleProps} fill={colors.accent.brightPurple} />
        </marker>
      </defs>
      {children}
    </Icon>
  );
};
