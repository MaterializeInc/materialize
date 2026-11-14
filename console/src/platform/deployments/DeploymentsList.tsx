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

import { CodeBlock } from "~/components/copyableComponents";
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
  Muted,
  RelativeBar,
  SectionHeading,
} from "./deploymentComponents";
import {
  ClusterHealth,
  ClusterStatus,
  clusterVerdict,
  Deployment,
  formatDuration,
  stagingName,
  swappedSchemas,
  timeAgo,
} from "./deploymentModel";
import {
  DeploymentsData,
  useDeployments,
  useStagedClusterHealth,
} from "./queries";

const STATUS_RANK: Record<ClusterStatus, number> = {
  failing: 0,
  hydrating: 1,
  lagging: 2,
  ready: 3,
};

const NotSetUp = ({ access }: { access: "missing" | "denied" }) => (
  <EmptyListWrapper>
    <EmptyListHeader>
      <EmptyListHeaderContents
        title={
          access === "denied"
            ? "You can't read mz-deploy's tracking tables"
            : "No mz-deploy deployments found"
        }
        helpText={
          access === "denied"
            ? "This page reads _mz_deploy on the _mz_deploy_server cluster. Ask an admin to grant you the materialize_monitor role that mz-deploy setup creates."
            : "This page reads mz-deploy's tracking database, _mz_deploy. It doesn't exist here, or your role can't see it. Run setup once with an admin role, then grant materialize_monitor to people who should see deploys."
        }
      />
    </EmptyListHeader>
    {access === "missing" && (
      <Box width="100%" maxWidth="560px">
        <CodeBlock
          title="Set up mz-deploy"
          contents={"mz-deploy setup\nmz-deploy stage"}
          lineNumbers
        />
      </Box>
    )}
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
            Blue/green deploys staged with mz-deploy
          </Text>
        </VStack>
      </PageHeader>
      {isLoading ? (
        <LoadingContainer />
      ) : error ? (
        <ErrorBox message="We couldn't load deployments." />
      ) : data && data.access !== "ok" ? (
        <NotSetUp access={data.access} />
      ) : !data ? null : (
        <VStack alignItems="stretch" spacing="10">
          <InProgress data={data} />
          <History deployments={data.deployments} />
        </VStack>
      )}
    </MainContentContainer>
  );
};

const InProgress = ({ data }: { data: DeploymentsData }) => {
  const navigate = useNavigate();
  const health = useStagedClusterHealth(data);
  const inProgress = data.deployments.filter((d) => d.promotedAt === null);
  const healthByName = React.useMemo(
    () => new Map((health.data ?? []).map((h) => [h.name, h])),
    [health.data],
  );

  return (
    <Box>
      <SectionHeading
        title="In progress"
        description="Staged with mz-deploy stage and not yet promoted or aborted."
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
              <Th width="30%">Status</Th>
            </Tr>
          </Thead>
          <Tbody>
            {inProgress.map((d) => (
              <Tr
                key={d.deployId}
                cursor="pointer"
                onClick={() => navigate(d.deployId)}
              >
                <Td>
                  <Text textStyle="text-ui-med" fontFamily="mono">
                    {d.deployId}
                  </Text>
                </Td>
                <Td>
                  <Text textStyle="text-ui-reg">{timeAgo(d.deployedAt)}</Text>
                  <Text textStyle="text-small" color="gray.500">
                    by {d.deployedBy}
                  </Text>
                </Td>
                <Td>
                  {swappedSchemas(d)
                    .map((s) => s.schema)
                    .join(", ") || <Muted>—</Muted>}
                </Td>
                <Td>
                  {(data.clustersByDeploy.get(d.deployId) ?? [])
                    .map((name) => stagingName(name, d.deployId))
                    .join(", ")}
                </Td>
                <Td>
                  <DeployStatus
                    deployment={d}
                    clusters={data.clustersByDeploy.get(d.deployId) ?? []}
                    healthByName={healthByName}
                    isLoading={health.isLoading}
                  />
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

/** The worst status across a deploy's staged clusters. */
const DeployStatus = ({
  deployment,
  clusters,
  healthByName,
  isLoading,
}: {
  deployment: Deployment;
  clusters: string[];
  healthByName: Map<string, ClusterHealth>;
  isLoading: boolean;
}) => {
  if (isLoading) return <Muted>Checking…</Muted>;
  if (clusters.length === 0) return <Muted>No clusters staged</Muted>;
  const verdicts = clusters.map((name) =>
    clusterVerdict(healthByName.get(stagingName(name, deployment.deployId))),
  );
  const worst = verdicts.reduce((a, b) =>
    STATUS_RANK[b.status] < STATUS_RANK[a.status] ? b : a,
  );
  const notReady = verdicts.filter((v) => v.status !== "ready").length;
  return (
    <HStack spacing="3">
      <ClusterStatusPill status={worst.status} />
      <Text textStyle="text-small" color="gray.500">
        {notReady === 0
          ? "Clusters ready, open to check sinks"
          : `${worst.detail}${notReady > 1 ? ` (+${notReady - 1} more)` : ""}`}
      </Text>
    </HStack>
  );
};

const History = ({ deployments }: { deployments: Deployment[] }) => {
  const navigate = useNavigate();
  const { colors } = useTheme<MaterializeTheme>();
  const promoted = deployments.filter(
    (d): d is Deployment & { promotedAt: Date } => d.promotedAt !== null,
  );
  const durations = promoted.map(
    (d) => d.promotedAt.getTime() - d.deployedAt.getTime(),
  );
  const longest = Math.max(1, ...durations);

  return (
    <Box>
      <SectionHeading
        title="History"
        description="Promoted deploys. mz-deploy removes aborted deploys from its tracking tables, so they are not listed."
      />
      {promoted.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          No deploys promoted yet.
        </Text>
      ) : (
        <Table variant="linkable" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Deploy</Th>
              <Th>Promoted</Th>
              <Th>Time staged before promote</Th>
              <Th>Schemas</Th>
              <Th>By</Th>
            </Tr>
          </Thead>
          <Tbody>
            {promoted.map((d, i) => (
              <Tr
                key={d.deployId}
                cursor="pointer"
                onClick={() => navigate(d.deployId)}
              >
                <Td>
                  <HStack spacing="3">
                    <Text textStyle="text-ui-med" fontFamily="mono">
                      {d.deployId}
                    </Text>
                    {i === 0 && (
                      <StatusPill
                        status="live"
                        label="Live"
                        colorScheme="green"
                      />
                    )}
                  </HStack>
                </Td>
                <Td>
                  {formatDate(
                    d.promotedAt,
                    FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
                  )}
                </Td>
                <Td>
                  <HStack spacing="3">
                    <RelativeBar
                      fraction={durations[i] / longest}
                      color={colors.accent.purple}
                    />
                    <Text textStyle="text-ui-reg">
                      {formatDuration(durations[i])}
                    </Text>
                  </HStack>
                </Td>
                <Td>
                  {swappedSchemas(d)
                    .map((s) => s.schema)
                    .join(", ")}
                </Td>
                <Td>{d.deployedBy}</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

export default DeploymentsList;
