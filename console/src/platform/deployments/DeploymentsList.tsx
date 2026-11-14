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
  HStack,
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
import { useNavigate } from "react-router-dom";

import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import StatusPill from "~/components/StatusPill";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import {
  ClusterStatusPill,
  CutoverPill,
  DeployStatePill,
  Muted,
  RelativeBar,
  SectionHeading,
} from "./deploymentComponents";
import {
  ClusterHealth,
  ClusterStatus,
  clusterVerdict,
  CutoverState,
  Deployment,
  DeployTool,
  formatDuration,
  liveDeployKeys,
  promoteBlockers,
  sinkCutover,
  SinkCutoverRow,
  timeAgo,
} from "./deploymentModel";
import {
  useDeployments,
  useFrontierStalls,
  useInProgressStatus,
} from "./queries";

const CLUSTER_RANK: Record<ClusterStatus, number> = {
  failing: 0,
  hydrating: 1,
  lagging: 2,
  ready: 3,
};

const CUTOVER_RANK: Record<CutoverState, number> = {
  "no-replacement": 0,
  stuck: 1,
  unknown: 2,
  behind: 3,
  ready: 4,
};

const TOOL_LABEL: Record<DeployTool, string | null> = {
  "mz-deploy": "mz-deploy",
  dbt: "dbt",
  unknown: null,
};

const NoDeploys = () => (
  <EmptyListWrapper>
    <EmptyListHeader>
      <EmptyListHeaderContents
        title="No blue/green deploys found"
        helpText="Deploys show up here when a schema is staged next to its live copy as <schema>_<suffix>, the way mz-deploy and dbt-materialize stage them, and when they are swapped into place."
      />
    </EmptyListHeader>
  </EmptyListWrapper>
);

const DeploymentsList = () => {
  const { data, isLoading, error } = useDeployments();

  return (
    <MainContentContainer>
      <PageHeader>
        <VStack alignItems="start" spacing="0">
          <PageHeading>Deployments</PageHeading>
          <Text textStyle="text-small" color="gray.500">
            Blue/green deploys, found from the catalog and the audit log
          </Text>
        </VStack>
      </PageHeader>
      {isLoading ? (
        <LoadingContainer />
      ) : error ? (
        <ErrorBox message="We couldn't load deployments." />
      ) : !data || data.length === 0 ? (
        <NoDeploys />
      ) : (
        <VStack alignItems="stretch" spacing="10">
          <InProgress deployments={data} />
          <History deployments={data} />
        </VStack>
      )}
    </MainContentContainer>
  );
};

const DeployName = ({ deployment }: { deployment: Deployment }) => {
  const tool = TOOL_LABEL[deployment.tool];
  return (
    <Box>
      <Text textStyle="text-ui-med" fontFamily="mono">
        {deployment.suffix}
      </Text>
      {tool && (
        <Text textStyle="text-small" color="gray.500">
          {tool}
        </Text>
      )}
    </Box>
  );
};

const InProgress = ({ deployments }: { deployments: Deployment[] }) => {
  const navigate = useNavigate();
  const status = useInProgressStatus(deployments);
  const inProgress = deployments.filter((d) => d.state === "in-progress");
  const healthByName = React.useMemo(
    () => new Map((status.data?.clusters ?? []).map((h) => [h.name, h])),
    [status.data],
  );
  const frontiers = React.useMemo(
    () =>
      status.data?.sinks.map((s) => ({
        id: s.sinkId,
        frontier: s.sinkWriteFrontierMs,
      })),
    [status.data],
  );
  const stalls = useFrontierStalls(frontiers);

  return (
    <Box>
      <SectionHeading
        title="In progress"
        description="Staged next to the live schemas and not yet swapped in."
      />
      {inProgress.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          Nothing staged right now.
        </Text>
      ) : (
        <Table variant="linkable" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Deploy</Th>
              <Th>Staged</Th>
              <Th>Schemas</Th>
              <Th>New clusters</Th>
              <Th width="32%">Status</Th>
            </Tr>
          </Thead>
          <Tbody>
            {inProgress.map((d) => (
              <Tr
                key={d.key}
                cursor="pointer"
                onClick={() => navigate(encodeURIComponent(d.key))}
              >
                <Td>
                  <DeployName deployment={d} />
                </Td>
                <Td>
                  {d.stagedAt ? (
                    <>
                      <Text textStyle="text-ui-reg">{timeAgo(d.stagedAt)}</Text>
                      {d.stagedBy && (
                        <Text textStyle="text-small" color="gray.500">
                          by {d.stagedBy}
                        </Text>
                      )}
                    </>
                  ) : (
                    <Muted>—</Muted>
                  )}
                </Td>
                <Td>{d.schemas.map((s) => s.liveName).join(", ")}</Td>
                <Td>
                  {d.clusters.map((c) => c.stagedName).join(", ") || (
                    <Muted>—</Muted>
                  )}
                </Td>
                <Td>
                  {status.isLoading ? (
                    <Muted>Checking…</Muted>
                  ) : status.isError && !status.data ? (
                    <Muted>Couldn&apos;t check status</Muted>
                  ) : (
                    <DeployStatus
                      deployment={d}
                      healthByName={healthByName}
                      sinks={(status.data?.sinks ?? []).filter(
                        (s) => s.deployId === d.suffix,
                      )}
                      stalls={stalls}
                    />
                  )}
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

/** The worst cluster or sink problem standing between a deploy and promote. */
const DeployStatus = ({
  deployment,
  healthByName,
  sinks,
  stalls,
}: {
  deployment: Deployment;
  healthByName: Map<string, ClusterHealth>;
  sinks: SinkCutoverRow[];
  stalls: Map<string, number>;
}) => {
  const clusters = deployment.clusters.map((c) => ({
    liveName: c.liveName,
    stagingName: c.stagedName,
    verdict: clusterVerdict(healthByName.get(c.stagedName)),
  }));
  const cutovers = sinks.map((row) => ({
    row,
    cutover: sinkCutover(row, stalls.get(row.sinkId) ?? 0),
  }));
  const blockers = promoteBlockers(clusters, cutovers);
  const worstCluster = clusters.reduce<ClusterStatus>(
    (worst, c) =>
      CLUSTER_RANK[c.verdict.status] < CLUSTER_RANK[worst]
        ? c.verdict.status
        : worst,
    "ready",
  );
  const worstSink = cutovers.reduce<CutoverState>(
    (worst, s) =>
      CUTOVER_RANK[s.cutover.state] < CUTOVER_RANK[worst]
        ? s.cutover.state
        : worst,
    "ready",
  );

  return (
    <HStack spacing="3" alignItems="start">
      {worstCluster !== "ready" || worstSink === "ready" ? (
        <ClusterStatusPill status={worstCluster} />
      ) : (
        <CutoverPill state={worstSink} />
      )}
      <Text textStyle="text-small" color="gray.500">
        {blockers.length === 0
          ? "Ready to promote"
          : `${blockers[0].name}: ${blockers[0].message}${
              blockers.length > 1 ? ` (+${blockers.length - 1} more)` : ""
            }`}
      </Text>
    </HStack>
  );
};

const History = ({ deployments }: { deployments: Deployment[] }) => {
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();
  const past = deployments.filter(
    (d): d is Deployment & { endedAt: Date } =>
      d.state !== "in-progress" && d.endedAt !== null,
  );
  const staged = (d: Deployment & { endedAt: Date }) =>
    d.stagedAt ? d.endedAt.getTime() - d.stagedAt.getTime() : null;
  const longest = Math.max(1, ...past.map((d) => staged(d) ?? 0));
  const live = liveDeployKeys(deployments);

  return (
    <Box>
      <SectionHeading
        title="History"
        description="Promotes are swaps in the audit log. Aborted deploys staged a schema and dropped it without a swap."
      />
      {past.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          No deploys promoted or aborted yet.
        </Text>
      ) : (
        <Table variant="linkable" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Deploy</Th>
              <Th>Outcome</Th>
              <Th>When</Th>
              <Th>Time staged</Th>
              <Th>Schemas</Th>
              <Th>By</Th>
            </Tr>
          </Thead>
          <Tbody>
            {past.map((d) => {
              const duration = staged(d);
              return (
                <Tr
                  key={d.key}
                  cursor="pointer"
                  onClick={() => navigate(encodeURIComponent(d.key))}
                >
                  <Td>
                    <DeployName deployment={d} />
                  </Td>
                  <Td>
                    <HStack spacing="2">
                      <DeployStatePill state={d.state} />
                      {live.has(d.key) && (
                        <StatusPill
                          status="live"
                          label="Live"
                          colorScheme="green"
                        />
                      )}
                    </HStack>
                  </Td>
                  <Td>
                    {formatDate(d.endedAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
                  </Td>
                  <Td>
                    {duration === null ? (
                      <Muted>—</Muted>
                    ) : (
                      <HStack spacing="3">
                        <RelativeBar
                          fraction={duration / longest}
                          color={
                            d.state === "promoted"
                              ? colors.accent.purple
                              : colors.foreground.tertiary
                          }
                        />
                        <Text textStyle="text-ui-reg">
                          {formatDuration(duration)}
                        </Text>
                      </HStack>
                    )}
                  </Td>
                  <Td>{d.schemas.map((s) => s.liveName).join(", ")}</Td>
                  <Td>{d.endedBy ?? <Muted>—</Muted>}</Td>
                </Tr>
              );
            })}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

export default DeploymentsList;
