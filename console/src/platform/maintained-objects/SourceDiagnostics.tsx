// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Card, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import { DetailItem } from "~/platform/connectors/AsideBox";
import { snapshotEstimateNote } from "~/platform/connectors/utils";
import { MaterializeTheme } from "~/theme";

import {
  MaintainedObjectSourceStatus,
  useObjectSourceStatistics,
} from "./queries";

export interface SourceDiagnosticsProps {
  sourceId: string;
  sourceType: string | null;
  /** Null until the source status subscribe delivers a row. */
  sourceStatus: MaintainedObjectSourceStatus | null;
}

/**
 * Renders only while the source has something diagnostic to say: a status
 * error, or an in-progress snapshot. Steady-state lifecycle facts live on the
 * source details page instead.
 */
export const SourceDiagnostics = ({
  sourceId,
  sourceType,
  sourceStatus,
}: SourceDiagnosticsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: stats } = useObjectSourceStatistics(sourceId);

  const error = sourceStatus?.error;
  // `snapshot_committed` is authoritative. Judging by the staged/known ratio
  // would report a live source as stuck at 99% forever.
  const snapshotting = sourceStatus?.snapshotCommitted === false;
  if (!error && !snapshotting) return null;

  const snapshotKnown = stats?.snapshotRecordsKnown ?? 0;
  const snapshotStaged = stats?.snapshotRecordsStaged ?? 0;
  const snapshotPercent =
    snapshotKnown > 0
      ? Math.min(100, Math.round((snapshotStaged / snapshotKnown) * 100))
      : null;

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Source diagnostics</Text>

        {error && <Alert variant="error" width="100%" message={error} />}

        {snapshotting && (
          <VStack align="stretch" spacing={2} width="100%">
            <DetailItem label="Snapshot progress" color={colors.accent.orange}>
              {snapshotPercent === null
                ? "In progress"
                : `${snapshotPercent}% (${snapshotStaged.toLocaleString()} / ${snapshotKnown.toLocaleString()} records)`}
            </DetailItem>
            {snapshotPercent !== null && (
              <Box
                width="100%"
                height="1.5"
                borderRadius="full"
                bg={colors.background.secondary}
              >
                <Box
                  height="100%"
                  width={`${snapshotPercent}%`}
                  borderRadius="full"
                  bg={colors.accent.brightPurple}
                  transition="width 0.3s ease"
                />
              </Box>
            )}
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              {snapshotEstimateNote(sourceType)}
            </Text>
          </VStack>
        )}
      </VStack>
    </Card>
  );
};

export default SourceDiagnostics;
