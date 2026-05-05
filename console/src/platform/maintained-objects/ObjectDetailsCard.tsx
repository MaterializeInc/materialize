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
import { Link as RouterLink } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import TextLink from "~/components/TextLink";
import { DetailItem } from "~/platform/connectors/AsideBox";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { formatIntervalShort } from "~/utils/format";

import { MaintainedObjectListItem } from "./queries";

const OUTDATED_MS = OUTDATED_THRESHOLD_SECONDS * 1_000;
const WARNING_MS = OUTDATED_MS / 2;

export interface ObjectDetailsCardProps {
  item: MaintainedObjectListItem;
}

export const ObjectDetailsCard = ({ item }: ObjectDetailsCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();
  const { getClusterById } = useAllClusters();

  const cluster = item.cluster ? getClusterById(item.cluster.id) : undefined;
  const namespace = createNamespace(item.databaseName, item.schemaName);
  const replicas = cluster?.replicas ?? [];

  return (
    <Card
      p={6}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={4} width="100%">
        <HStack spacing={3} alignItems="center">
          <Text textStyle="heading-lg">{item.name}</Text>
          <LagBadge lag={item.lag} />
        </HStack>

        <Text textStyle="heading-sm" color={colors.foreground.secondary}>
          Overview
        </Text>

        <Grid templateColumns="1fr 1fr" gap={6} width="100%">
          <GridItem minW={0}>
            <VStack align="stretch" spacing={2} width="100%">
              <DetailItem label="Object name">{item.name}</DetailItem>
              <DetailItem label="Object type">{item.objectType}</DetailItem>
              <DetailItem label="Schema">{namespace ?? "-"}</DetailItem>
              {item.sourceType && (
                <DetailItem label="Source type">{item.sourceType}</DetailItem>
              )}
            </VStack>
          </GridItem>
          <GridItem minW={0}>
            <VStack align="stretch" spacing={2} width="100%">
              <DetailItem label="Cluster">
                {item.cluster ? (
                  <TextLink
                    as={RouterLink}
                    to={absoluteClusterPath(regionSlug, item.cluster)}
                  >
                    {item.cluster.name}
                  </TextLink>
                ) : (
                  "-"
                )}
              </DetailItem>
              {cluster?.managed !== null && cluster?.managed !== undefined && (
                <DetailItem label="Cluster type">
                  {cluster.managed ? "managed" : "unmanaged"}
                </DetailItem>
              )}
              {replicas.length > 0 && (
                <DetailItem
                  label={replicas.length === 1 ? "Replica" : "Replicas"}
                >
                  {replicas
                    .map((r) => (r.size ? `${r.name} (${r.size})` : r.name))
                    .join(", ")}
                </DetailItem>
              )}
            </VStack>
          </GridItem>
        </Grid>
      </VStack>
    </Card>
  );
};

const LagBadge = ({ lag }: { lag: MaintainedObjectListItem["lag"] }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!lag) return null;

  const bg =
    lag.ms >= OUTDATED_MS
      ? colors.accent.red
      : lag.ms >= WARNING_MS
        ? colors.accent.orange
        : colors.accent.green;

  return (
    <Text
      textStyle="text-small-heavy"
      px={2}
      py={0.5}
      borderRadius="full"
      bg={bg}
      color={colors.foreground.primaryButtonLabel}
    >
      {formatIntervalShort(lag.value)} behind
    </Text>
  );
};

export default ObjectDetailsCard;
