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
  /** What an unreadable value at this rank says about the minutes. */
  unreadableCaption: string;
}

// Nearest rank lands on an unreadable minute only when more than the
// percentile's tail is unreadable: 7 of 60 for p90, where 6 would not.
const CARDS: CardSpec[] = [
  {
    key: "p90",
    label: "p90",
    unreadableCaption: "More than 10% of minutes could not be read",
  },
  {
    key: "p99",
    label: "p99",
    unreadableCaption: "More than 1% of minutes could not be read",
  },
  {
    key: "p99_9",
    label: "p99.9",
    unreadableCaption: "More than 0.1% of minutes could not be read",
  },
  {
    key: "peak",
    label: "pMAX",
    unreadableCaption: "At least one minute could not be read",
  },
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

  // Only an edge state gets a caption.
  const caption =
    value === undefined
      ? "No readings in this range"
      : unreadable
        ? spec.unreadableCaption
        : spec.key !== "peak" && value === percentiles?.peak
          ? "Same as pMAX at this range"
          : undefined;

  return (
    <Box
      borderWidth="1px"
      borderColor={colors.border.primary}
      borderRadius="lg"
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
      {!isLoading && caption && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {caption}
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
}: FreshnessStatboxesProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data, isLoading, isError } = useScopeLagPercentiles({
    objectIds,
    lookbackMs,
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
            isLoading={isLoading}
          />
        ))}
      </Grid>
    </Box>
  );
};

export const FreshnessStatboxes = React.memo(FreshnessStatboxesInner);
