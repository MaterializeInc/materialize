// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Grid, Skeleton, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import { MaterializeTheme } from "~/theme";
import { formatDurationExact } from "~/utils/format";

import { NO_VALUE } from "./constants";
import { UNREADABLE } from "./freshnessRows";
import { LagPercentiles, useScopeLagPercentiles } from "./queries";

interface CardSpec {
  key: keyof LagPercentiles;
  label: string;
}

const CARDS: CardSpec[] = [
  { key: "p90", label: "p90" },
  { key: "p99", label: "p99" },
  { key: "p99_9", label: "p99.9" },
  { key: "peak", label: "pMAX" },
];

const StatCard = ({
  spec,
  percentiles,
  isLoading,
}: {
  spec: CardSpec;
  percentiles: Partial<LagPercentiles> | null | undefined;
  isLoading: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const value = percentiles?.[spec.key];
  const unreadable = value === UNREADABLE;

  return (
    <Box
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="lg"
      background="chakra-subtle-bg"
      padding="4"
    >
      <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
        {spec.label}
      </Text>
      {isLoading ? (
        <Skeleton height="6" width="16" marginY="1" />
      ) : (
        <Text textStyle="heading-lg">
          {value === undefined
            ? NO_VALUE
            : unreadable
              ? NULL_LAG_TEXT
              : formatDurationExact(value)}
        </Text>
      )}
    </Box>
  );
};

export interface FreshnessStatboxesProps {
  /** The objects ranked together: the scope, narrowed to the selected types. */
  objectIds: string[];
  lookbackMs: number;
  /**
   * The scope's worst newest minute, from `latestPeak`. Folded into pMAX the
   * way `computeStats` folds "Now" into each object's, so the two agree.
   */
  latestPeak: number | undefined;
  /** False until the scope's object list has loaded. The cards wait for it. */
  objectsReady: boolean;
}

/**
 * The scope's percentiles over every minute of every object, ranked together.
 *
 * Pooling dilutes one slow object with the healthy ones' minutes, so these
 * describe the scope, not any one object, and never judge by the threshold.
 */
const FreshnessStatboxesInner = ({
  objectIds,
  lookbackMs,
  latestPeak,
  objectsReady,
}: FreshnessStatboxesProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useScopeLagPercentiles({
    objectIds,
    lookbackMs,
    enabled: objectsReady,
  });

  // Only pMAX can take in a newer minute. A percentile is a rank among every
  // minute in the range, which only the query has.
  const percentiles: Partial<LagPercentiles> | null | undefined =
    latestPeak === undefined || data === undefined
      ? data
      : data === null
        ? { peak: latestPeak }
        : { ...data, peak: Math.max(data.peak, latestPeak) };

  if (isError && data === undefined) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        An error occurred loading the summary percentiles.
      </Text>
    );
  }

  return (
    <Box as="section" aria-label="Summary percentiles">
      <Grid
        templateColumns={{
          base: "repeat(2, minmax(0, 1fr))",
          lg: "repeat(4, minmax(0, 1fr))",
        }}
        gap="3"
      >
        {CARDS.map((spec) => (
          <StatCard
            key={spec.key}
            spec={spec}
            percentiles={percentiles}
            // A disabled query is not loading, so it is not `isLoading`.
            isLoading={!objectsReady || isLoading}
          />
        ))}
      </Grid>
    </Box>
  );
};

export const FreshnessStatboxes = React.memo(FreshnessStatboxesInner);
