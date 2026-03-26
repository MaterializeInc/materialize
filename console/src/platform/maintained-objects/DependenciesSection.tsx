// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  Card,
  HStack,
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { IPostgresInterval } from "~/api/materialize";
import { OUTDATED_THRESHOLD_SECONDS } from "~/api/materialize/cluster/materializationLag";
import { UpstreamDependencyRow } from "~/api/materialize/maintained-objects/objectDependencies";

import { CriticalPathChart } from "./CriticalPathChart";
import { MaterializeTheme } from "~/theme";
import { sumPostgresIntervalMs } from "~/util";
import { formatIntervalShort } from "~/utils/format";

import {
  useCriticalPath,
  useDownstreamDependents,
  useUpstreamDependencies,
} from "./queries";

export interface DependenciesSectionProps {
  objectId: string;
  objectType: string;
  lagMs: number | null;
  onObjectClick: (objectId: string) => void;
}

export const DependenciesSection = ({
  objectId,
  objectType,
  lagMs,
  onObjectClick,
}: DependenciesSectionProps) => {
  const isRootObject = objectType === "source" || objectType === "table";

  return (
    <VStack align="start" spacing={6} width="100%">
      {!isRootObject && (
        <UpstreamDependencies
          objectId={objectId}
          lagMs={lagMs}
          onObjectClick={onObjectClick}
        />
      )}
      <DownstreamDependents
        objectId={objectId}
        onObjectClick={onObjectClick}
      />
    </VStack>
  );
};

const UpstreamDependencies = ({
  objectId,
  lagMs,
  onObjectClick,
}: {
  objectId: string;
  lagMs: number | null;
  onObjectClick: (objectId: string) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: upstream, isLoading } = useUpstreamDependencies(objectId);
  const outdatedMs = OUTDATED_THRESHOLD_SECONDS * 1_000;

  const [showFullPath, setShowFullPath] = React.useState(false);

  // Reset when object changes
  React.useEffect(() => {
    setShowFullPath(false);
  }, [objectId]);

  const {
    data: criticalPath,
    isLoading: critPathLoading,
  } = useCriticalPath(objectId, showFullPath);

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Upstream dependencies</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Direct inputs to this object. Bottlenecks are highlighted — these
          are the inputs at the minimum frontier holding this object back.
        </Text>

        {isLoading ? (
          <HStack justify="center" width="100%" py={4}>
            <Spinner size="sm" />
          </HStack>
        ) : !upstream || upstream.length === 0 ? (
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary} py={2}>
            No upstream dependencies
          </Text>
        ) : (
          <>
            <Box width="100%" overflowX="auto">
              <Table variant="linkable" size="sm" borderRadius="md">
                <Thead>
                  <Tr>
                    <Th>Name</Th>
                    <Th>Type</Th>
                    <Th>Freshness</Th>
                    <Th>Delay</Th>
                    <Th>Cluster</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {upstream.map((dep) => (
                    <UpstreamRow
                      key={dep.id}
                      dep={dep}
                      outdatedMs={outdatedMs}
                      onClick={() => onObjectClick(dep.id)}
                    />
                  ))}
                </Tbody>
              </Table>
            </Box>

            <ComputeBottleneckInsight
              lagMs={lagMs}
              upstream={upstream}
              outdatedMs={outdatedMs}
            />

            <Button
              variant="ghost"
              size="sm"
              onClick={() => setShowFullPath((prev) => !prev)}
              color={colors.accent.brightPurple}
              rightIcon={
                <Text
                  fontSize="12px"
                  transition="transform 0.2s"
                  transform={showFullPath ? "rotate(180deg)" : "rotate(0deg)"}
                >
                  ▾
                </Text>
              }
            >
              {showFullPath
                ? "Hide critical path"
                : "Show full critical path to sources"}
            </Button>

            {showFullPath && (
              <>
                {critPathLoading ? (
                  <HStack spacing={2} py={2}>
                    <Spinner size="sm" />
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      Tracing critical path...
                    </Text>
                  </HStack>
                ) : criticalPath && criticalPath.length > 0 ? (
                  <CriticalPathChart
                    edges={criticalPath}
                    probeId={objectId}
                    onObjectClick={onObjectClick}
                  />
                ) : (
                  <Text
                    textStyle="text-ui-reg"
                    color={colors.foreground.secondary}
                    py={2}
                  >
                    All inputs are fresh — no delay detected along the
                    dependency chain.
                  </Text>
                )}
              </>
            )}
          </>
        )}
      </VStack>
    </Card>
  );
};

const UpstreamRow = ({
  dep,
  outdatedMs,
  onClick,
}: {
  dep: UpstreamDependencyRow;
  outdatedMs: number;
  onClick: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const lag = dep.lag as IPostgresInterval | null;
  const lagMs = lag ? sumPostgresIntervalMs(lag) : null;
  const delay = dep.delay != null ? Number(dep.delay) : 0;
  const delayMs = isNaN(delay) ? 0 : delay;
  const delaySeconds = delayMs / 1000;

  // Show bottleneck badge using Frank's self-delay concept:
  // An input is a bottleneck if IT is behind (high lag), not just because there's
  // a gap between it and the probe. Sources at 2s lag delivering data fine shouldn't
  // be badged even if the probe is 600s behind.
  //
  // self-delay ≈ input's own lag. If the input's lag is significant (>= threshold),
  // it means the input itself isn't keeping up.
  const inputLagMs = dep.lag ? sumPostgresIntervalMs(dep.lag as IPostgresInterval) : 0;
  const isActualBottleneck =
    dep.isBottleneck &&
    inputLagMs >= OUTDATED_THRESHOLD_SECONDS * 1000;
  const rowBg = isActualBottleneck ? `${colors.accent.red}08` : undefined;

  return (
    <Tr
      cursor="pointer"
      onClick={onClick}
      _hover={{ bg: colors.background.secondary }}
      bg={rowBg}
    >
      <Td>
        <VStack align="start" spacing={0}>
          <HStack spacing={2}>
            <Text
              textStyle="text-ui-med"
              color={colors.accent.brightPurple}
              _hover={{ textDecoration: "underline" }}
            >
              {dep.name}
            </Text>
            {isActualBottleneck && (
              <Text
                textStyle="text-small-heavy"
                px="1.5"
                py="0.5"
                borderRadius="full"
                bg={colors.accent.red}
                color="white"
                fontSize="10px"
              >
                bottleneck
              </Text>
            )}
          </HStack>
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
          >
            {dep.databaseName}.{dep.schemaName}
          </Text>
        </VStack>
      </Td>
      <Td>
        <Text textStyle="text-ui-reg">{dep.objectType}</Text>
      </Td>
      <Td>
        {lagMs !== null && lag ? (
          <Text
            textStyle="text-ui-med"
            color={
              lagMs >= outdatedMs
                ? colors.accent.red
                : lagMs >= outdatedMs / 2
                  ? colors.accent.orange
                  : colors.foreground.primary
            }
          >
            {formatIntervalShort(lag)}
          </Text>
        ) : (
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            —
          </Text>
        )}
      </Td>
      <Td>
        {delaySeconds > 0 ? (
          <Text
            textStyle="text-ui-med"
            color={
              delaySeconds >= OUTDATED_THRESHOLD_SECONDS
                ? colors.accent.red
                : delaySeconds >= OUTDATED_THRESHOLD_SECONDS / 2
                  ? colors.accent.orange
                  : colors.foreground.primary
            }
          >
            +{delaySeconds.toFixed(1)}s
          </Text>
        ) : (
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            0s
          </Text>
        )}
      </Td>
      <Td>
        <Text textStyle="text-ui-reg">{dep.clusterName ?? "—"}</Text>
      </Td>
    </Tr>
  );
};

const DownstreamDependents = ({
  objectId,
  onObjectClick,
}: {
  objectId: string;
  onObjectClick: (objectId: string) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: downstream, isLoading } = useDownstreamDependents(objectId);
  const outdatedMs = OUTDATED_THRESHOLD_SECONDS * 1_000;

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <Text textStyle="heading-sm">Downstream dependencies</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Objects that depend on this one. If this object is stale, these are
          affected.
        </Text>

        {isLoading ? (
          <HStack justify="center" width="100%" py={4}>
            <Spinner size="sm" />
          </HStack>
        ) : !downstream || downstream.length === 0 ? (
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary} py={2}>
            No downstream dependencies
          </Text>
        ) : (
          <Box width="100%" overflowX="auto">
            <Table variant="linkable" size="sm" borderRadius="md">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th>Type</Th>
                  <Th>Freshness</Th>
                  <Th>Cluster</Th>
                </Tr>
              </Thead>
              <Tbody>
                {downstream.map((dep) => {
                  const lag = dep.lag as IPostgresInterval | null;
                  const lagMs = lag ? sumPostgresIntervalMs(lag) : null;

                  return (
                    <Tr
                      key={dep.id}
                      cursor="pointer"
                      onClick={() => onObjectClick(dep.id)}
                      _hover={{ bg: colors.background.secondary }}
                    >
                      <Td>
                        <VStack align="start" spacing={0}>
                          <Text
                            textStyle="text-ui-med"
                            color={colors.accent.brightPurple}
                            _hover={{ textDecoration: "underline" }}
                          >
                            {dep.name}
                          </Text>
                          <Text
                            textStyle="text-small"
                            color={colors.foreground.secondary}
                          >
                            {dep.databaseName}.{dep.schemaName}
                          </Text>
                        </VStack>
                      </Td>
                      <Td>
                        <Text textStyle="text-ui-reg">{dep.objectType}</Text>
                      </Td>
                      <Td>
                        {lagMs !== null && lag ? (
                          <Text
                            textStyle="text-ui-med"
                            color={
                              lagMs >= outdatedMs
                                ? colors.accent.red
                                : lagMs >= outdatedMs / 2
                                  ? colors.accent.orange
                                  : colors.foreground.primary
                            }
                          >
                            {formatIntervalShort(lag)}
                          </Text>
                        ) : (
                          <Text
                            textStyle="text-ui-reg"
                            color={colors.foreground.secondary}
                          >
                            —
                          </Text>
                        )}
                      </Td>
                      <Td>
                        <Text textStyle="text-ui-reg">
                          {dep.clusterName ?? "—"}
                        </Text>
                      </Td>
                    </Tr>
                  );
                })}
              </Tbody>
            </Table>
          </Box>
        )}
      </VStack>
    </Card>
  );
};

/**
 * Shows an insight when the object has high lag but its upstream inputs
 * have low delay — meaning the bottleneck is the object's own compute,
 * not its inputs.
 */
const ComputeBottleneckInsight = ({
  lagMs,
  upstream,
  outdatedMs,
}: {
  lagMs: number | null;
  upstream: UpstreamDependencyRow[];
  outdatedMs: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (!lagMs || lagMs < outdatedMs) return null;

  const maxUpstreamDelayMs = upstream.reduce((max, dep) => {
    const d = dep.delay != null ? Number(dep.delay) : 0;
    return isNaN(d) ? max : Math.max(max, d);
  }, 0);

  // If the object's lag is significantly higher than the max upstream delay,
  // the bottleneck is the object's own processing
  if (maxUpstreamDelayMs > lagMs * 0.5) return null;

  const objectLagSeconds = Math.round(lagMs / 1000);

  return (
    <Box
      width="100%"
      p={3}
      borderRadius="md"
      bg={colors.accent.orange + "12"}
      border="1px"
      borderColor={colors.accent.orange + "30"}
    >
      <Text textStyle="text-small" color={colors.foreground.primary}>
        <Text as="span" fontWeight="600">
          This object is {objectLagSeconds}s behind, but its inputs are
          delivering data on time.
        </Text>{" "}
        The delay is likely in this object&apos;s own compute — consider
        checking the cluster&apos;s resource utilization or viewing the
        dataflow to identify expensive operators.
      </Text>
    </Box>
  );
};

export default DependenciesSection;
