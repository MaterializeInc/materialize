// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Card,
  Grid,
  GridItem,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { createNamespace } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import { IPostgresInterval } from "~/api/materialize";
import { MaterializeTheme } from "~/theme";
import { sumPostgresIntervalMs } from "~/util";
import { formatBytesShort, formatIntervalShort } from "~/utils/format";

import { MaintainedObjectListRow } from "./types";

export interface ObjectDetailsCardProps {
  object: MaintainedObjectListRow;
  replicaName?: string | null;
  replicaSize?: string | null;
  clusterManaged?: boolean | null;
  memoryBytes?: string | null;
  replicaTotalMemoryBytes?: string | null;
}

export const ObjectDetailsCard = ({
  object,
  replicaName,
  replicaSize,
  clusterManaged,
  memoryBytes,
  replicaTotalMemoryBytes,
}: ObjectDetailsCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const lag = object.lag as IPostgresInterval | null;
  const lagMs = lag ? sumPostgresIntervalMs(lag) : null;
  const outdatedMs = OUTDATED_THRESHOLD_SECONDS * 1_000;

  return (
    <Card
      p={6}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={4} width="100%">
        <HStack spacing="3" alignItems="center">
          <Text textStyle="heading-lg">{object.name}</Text>
          <LagBadge lagMs={lagMs} outdatedMs={outdatedMs} lag={lag} />
        </HStack>

        <Text textStyle="heading-sm" color={colors.foreground.secondary}>
          Overview
        </Text>

        <Grid templateColumns="1fr 1fr" gap={4} width="100%">
          <GridItem>
            <VStack align="start" spacing={3}>
              <DetailRow label="Object name" value={object.name} />
              <DetailRow label="Object type" value={object.objectType} />
              <DetailRow
                label="Schema"
                value={
                  createNamespace(object.databaseName, object.schemaName) ??
                  "—"
                }
              />
            </VStack>
          </GridItem>
          <GridItem>
            <VStack align="start" spacing={3}>
              <DetailRow
                label="Cluster"
                value={object.clusterName ?? "—"}
              />
              {replicaName && (
                <DetailRow
                  label="Replica"
                  value={`${replicaName}${replicaSize ? ` (${replicaSize})` : ""}`}
                />
              )}
              {clusterManaged !== null && clusterManaged !== undefined && (
                <DetailRow
                  label="Cluster type"
                  value={clusterManaged ? "managed" : "unmanaged"}
                />
              )}
              <HStack spacing={3}>
                <Text
                  textStyle="text-ui-reg"
                  color={colors.foreground.secondary}
                  width={LABEL_WIDTH}
                  flexShrink={0}
                >
                  Hydration
                </Text>
                <HydrationBadge hydrated={object.hydrated} />
              </HStack>
              {object.sourceType && (
                <DetailRow label="Source type" value={object.sourceType} />
              )}
              {memoryBytes && (
                <DetailRow
                  label="Memory"
                  value={formatMemory(
                    memoryBytes,
                    replicaTotalMemoryBytes,
                  )}
                />
              )}
            </VStack>
          </GridItem>
        </Grid>
      </VStack>
    </Card>
  );
};

const LABEL_WIDTH = "120px";

const DetailRow = ({
  label,
  value,
}: {
  label: string;
  value: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={3}>
      <Text
        textStyle="text-ui-reg"
        color={colors.foreground.secondary}
        width={LABEL_WIDTH}
        flexShrink={0}
      >
        {label}
      </Text>
      <Text textStyle="text-ui-med">{value}</Text>
    </HStack>
  );
};

const LagBadge = ({
  lagMs,
  outdatedMs,
  lag,
}: {
  lagMs: number | null;
  outdatedMs: number;
  lag: IPostgresInterval | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (lagMs === null || !lag) return null;

  const isOutdated = lagMs >= outdatedMs;
  const isWarning = lagMs >= outdatedMs / 2;

  const bg = isOutdated
    ? colors.accent.red
    : isWarning
      ? colors.accent.orange
      : colors.accent.green;

  const color = "white";

  return (
    <Text
      textStyle="text-small-heavy"
      px="2"
      py="0.5"
      borderRadius="full"
      bg={bg}
      color={color}
    >
      {formatIntervalShort(lag!)}{" "}
      behind
    </Text>
  );
};

const HydrationBadge = ({
  hydrated,
}: {
  hydrated: boolean | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (hydrated === null) {
    return (
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  return (
    <Text
      textStyle="text-small-heavy"
      px="2"
      py="0.5"
      borderRadius="full"
      bg={hydrated ? colors.background.secondary : colors.accent.orange}
      color={hydrated ? colors.foreground.primary : "white"}
    >
      {hydrated ? "Hydrated" : "Hydrating..."}
    </Text>
  );
};

const formatMemory = (
  memoryBytes: string | null,
  totalBytes: string | null | undefined,
): string => {
  if (!memoryBytes) return "—";
  const bytes = BigInt(memoryBytes);

  const formatted = formatBytesShort(bytes);

  if (totalBytes) {
    const total = BigInt(totalBytes);
    if (total > 0n) {
      const pct = Math.round(Number((bytes * 100n) / total));
      return `${formatted} (${pct}% of ${formatBytesShort(total)})`;
    }
  }

  return formatted;
};

export default ObjectDetailsCard;
