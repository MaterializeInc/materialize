// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Freshness (wallclock lag) indicators for the catalog detail view.
 *
 * Status and wording come from the workflow graph's helpers, so an object
 * reads the same here as on the Workflow page this view links to.
 */

import { Box, HStack, Text, Tooltip, useTheme } from "@chakra-ui/react";
import React from "react";

import { LagInfo } from "~/api/materialize/cluster/materializationLag";
import {
  formatLagInfoDetailed,
  formatLagInfoSimple,
  getLagInfoColorScheme,
} from "~/platform/clusters/format";
import { useObjectsLagHistory } from "~/platform/maintained-objects/queries";
import type { MaterializeTheme } from "~/theme";
import { sumPostgresIntervalMs } from "~/util";

const SPARKLINE_LOOKBACK_MS = 60 * 60 * 1000;

function describeLag(lagInfo: LagInfo) {
  if (lagInfo.hydrated === false) return "Not hydrated";
  if (!lagInfo.lag) return "Not queryable";
  return `${formatLagInfoDetailed(lagInfo)} behind wall clock`;
}

const StatusDot = ({ color }: { color: string }) => (
  <Box
    as="span"
    flexShrink={0}
    boxSize="2"
    borderRadius="full"
    bg={color}
    aria-hidden
  />
);

/**
 * Compact lag indicator for a row: a status dot and the lag in its largest
 * unit. Renders nothing until a reading arrives.
 */
export const LagDot = ({
  lagInfo,
  clusterName,
}: {
  lagInfo: LagInfo | undefined;
  /** Cluster that maintains the object, named in the tooltip. */
  clusterName?: string | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!lagInfo) return null;
  const description = describeLag(lagInfo);
  return (
    <Tooltip
      label={clusterName ? `${description} · on ${clusterName}` : description}
      placement="top"
      hasArrow
    >
      <HStack spacing="1" flexShrink={0}>
        <StatusDot color={getLagInfoColorScheme(lagInfo).pillColor} />
        <Text
          fontSize="xs"
          color={colors.foreground.secondary}
          whiteSpace="nowrap"
        >
          {formatLagInfoSimple(lagInfo)}
        </Text>
      </HStack>
    </Tooltip>
  );
};

/**
 * Headline freshness for the object itself: status, current lag, and a
 * sparkline of the last hour. The sparkline gives way first when the panel is
 * narrow.
 */
export const FreshnessSummary = ({
  objectId,
  lagInfo,
}: {
  objectId: string;
  lagInfo: LagInfo | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: history } = useObjectsLagHistory({
    objectIds: [objectId],
    lookbackMs: SPARKLINE_LOOKBACK_MS,
  });

  if (!lagInfo) return null;

  const color = getLagInfoColorScheme(lagInfo).pillColor;
  const label =
    lagInfo.hydrated === false
      ? "Not hydrated"
      : !lagInfo.lag
        ? "Not queryable"
        : `${formatLagInfoSimple(lagInfo)} behind`;

  return (
    <Tooltip
      label={`${describeLag(lagInfo)}. Sparkline shows the last hour.`}
      placement="top"
      hasArrow
    >
      <HStack spacing="2" minW="0" mt="2">
        <StatusDot color={color} />
        <Text fontSize="sm" whiteSpace="nowrap" flexShrink={0}>
          {label}
        </Text>
        {history && history.length > 1 && (
          <Sparkline
            values={history.map((r) =>
              r.lag === null ? null : sumPostgresIntervalMs(r.lag),
            )}
            color={colors.foreground.secondary}
          />
        )}
      </HStack>
    </Tooltip>
  );
};

/**
 * Minimal line of `values`, scaled to its own max. A null value is an
 * unreadable reading and breaks the line, rather than being drawn as zero.
 */
const Sparkline = ({
  values,
  color,
}: {
  values: (number | null)[];
  color: string;
}) => {
  const max = Math.max(1, ...values.map((v) => v ?? 0));
  const step = 100 / Math.max(1, values.length - 1);
  let path = "";
  let penDown = false;
  values.forEach((value, i) => {
    if (value === null) {
      penDown = false;
      return;
    }
    const x = (i * step).toFixed(2);
    const y = (20 - (value / max) * 18 - 1).toFixed(2);
    path += `${penDown ? "L" : "M"}${x} ${y} `;
    penDown = true;
  });

  return (
    <Box flex="1" minW="0" h="5" overflow="hidden">
      <svg
        width="100%"
        height="100%"
        viewBox="0 0 100 20"
        preserveAspectRatio="none"
        aria-hidden
      >
        <path
          d={path}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
          vectorEffect="non-scaling-stroke"
        />
      </svg>
    </Box>
  );
};
