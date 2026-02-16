// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  FormLabel,
  HStack,
  Switch,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
} from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import useLatestOfflineReplica, {
  LatestOfflineReplicaMap,
} from "~/api/materialize/cluster/useLatestOfflineReplica";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { ClustersIcon } from "~/icons";
import { InfoIcon } from "~/icons";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { relativeClusterPath } from "~/platform/routeHelpers";
import WarningIcon from "~/svg/WarningIcon";
import { truncateMaxWidth } from "~/theme/components/Table";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import AlterClusterMenuItem from "./AlterClusterMenuItem";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { useClusters } from "./queries";
import { useShowSystemObjects } from "./useShowSystemObjects";

const createClusterSuggestion = {
  title: "Create a cluster",
  string: `CREATE CLUSTER <cluster_name>
  (SIZE = '25cc');`,
};

const ClustersListContent = ({
  showSystemObjects,
}: {
  showSystemObjects: boolean;
}) => {
  const { data: clusters, refetch } = useClusters({
    includeSystemObjects: showSystemObjects,
  });

  const deprioritizeSystemClusters = React.useMemo(() => {
    const systemClusters = clusters.filter((cluster) =>
      isSystemCluster(cluster.id),
    );
    const nonSystemClusters = clusters
      ?.filter((cluster) => !isSystemCluster(cluster.id))
      .sort((a, b) => a.name.localeCompare(b.name));
    return [...(nonSystemClusters ?? []), ...(systemClusters ?? [])];
  }, [clusters]);

  const isEmpty = clusters !== null && clusters.length === 0;

  if (isEmpty) {
    return (
      <EmptyListWrapper>
        <EmptyListHeader>
          <IconBox type="Empty">
            <ClustersIcon />
          </IconBox>
          <EmptyListHeaderContents
            title="No available clusters"
            helpText="Create a cluster and one or more replicas to enable dataflows."
          />
        </EmptyListHeader>
        <SampleCodeBoxWrapper docsUrl={docUrls["/docs/sql/create-cluster/"]}>
          <CodeBlock
            title={createClusterSuggestion.title}
            contents={createClusterSuggestion.string}
            lineNumbers
          >
            {`CREATE CLUSTER <cluster_name>
  REPLICAS (
    <replica_name> (SIZE = 'xsmall')
);`}
          </CodeBlock>
        </SampleCodeBoxWrapper>
      </EmptyListWrapper>
    );
  }

  return (
    <ClusterTable
      clusters={deprioritizeSystemClusters ?? []}
      refetchClusters={refetch}
    />
  );
};

const ClustersListPage = () => {
  const [showSystemObjects, setShowSystemObjects] = useShowSystemObjects();

  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>Clusters</PageHeading>
        <HStack spacing={10}>
          <HStack spacing={2}>
            <FormLabel
              htmlFor="show-system-objects"
              variant="inline"
              textStyle="text-base"
            >
              Show system clusters
            </FormLabel>
            <Switch
              id="show-system-objects"
              isChecked={showSystemObjects}
              onChange={() => setShowSystemObjects((value: boolean) => !value)}
            />
          </HStack>
        </HStack>
      </PageHeader>
      <AppErrorBoundary message={CLUSTERS_FETCH_ERROR_MESSAGE}>
        <React.Suspense fallback={<LoadingContainer />}>
          <ClustersListContent showSystemObjects={showSystemObjects} />
        </React.Suspense>
      </AppErrorBoundary>
    </MainContentContainer>
  );
};

const LastStatusChangeColumn = (props: {
  latestOfflineReplicaMap: LatestOfflineReplicaMap;
  cluster: ClusterWithOwnership;
}) => {
  const latestOfflineReplicaStatus = props.latestOfflineReplicaMap.get(
    props.cluster.id,
  );

  const lastStatusChangeString = props.cluster.latestStatusUpdate
    ? formatDate(
        props.cluster.latestStatusUpdate,
        FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
      )
    : "-";

  return (
    <HStack>
      <Text noOfLines={1} paddingRight="6" position="relative">
        {lastStatusChangeString}
        {latestOfflineReplicaStatus &&
          latestOfflineReplicaStatus.shouldSurfaceOom && (
            <Tooltip
              px={3}
              py={2}
              minWidth="fit-content"
              rounded="md"
              label={`A replica ran out of memory on ${formatDate(
                latestOfflineReplicaStatus.lastOfflineAt,
                FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
              )}`}
            >
              <WarningIcon position="absolute" right="0" />
            </Tooltip>
          )}
      </Text>
    </HStack>
  );
};

interface ClusterTableProps {
  clusters: ClusterWithOwnership[];
  refetchClusters: () => void;
}

const ClusterTable = (props: ClusterTableProps) => {
  const navigate = useNavigate();
  const latestOfflineReplicaResult = useLatestOfflineReplica();

  return (
    <Table variant="linkable" data-testid="cluster-table" borderRadius="xl">
      <Thead>
        <Tr>
          <Th minW={{ md: "280px", sm: "auto" }}>Name</Th>
          <Th>Replicas</Th>
          <Th>Size</Th>
          {!latestOfflineReplicaResult.error && (
            <Th>
              <Text noOfLines={1}>Last status change</Text>
            </Th>
          )}
          <Th width={OVERFLOW_BUTTON_WIDTH}></Th>
        </Tr>
      </Thead>
      <Tbody>
        {props.clusters.map((c) => (
          <Tr
            key={c.id}
            onClick={() => navigate(relativeClusterPath(c))}
            cursor="pointer"
          >
            <Td {...truncateMaxWidth}>
              <HStack>
                <Text textStyle="text-ui-med" noOfLines={1}>
                  {c.name}
                </Text>
                {c.id.startsWith("s") && (
                  <Tooltip
                    label="This is a built-in system cluster. You are not billed for this cluster."
                    lineHeight={1.2}
                  >
                    <InfoIcon />
                  </Tooltip>
                )}
              </HStack>
            </Td>
            <Td>{c.replicas.length}</Td>
            <Td>
              {Array.from(new Set(c.replicas.map((r) => r.size)).values()).join(
                ", ",
              )}
            </Td>
            {!latestOfflineReplicaResult.error && (
              <Td>
                <LastStatusChangeColumn
                  latestOfflineReplicaMap={latestOfflineReplicaResult.data}
                  cluster={c}
                />
              </Td>
            )}
            <Td>
              <OverflowMenu
                items={[
                  {
                    visible: !isSystemCluster(c.id) && c.isOwner,
                    render: () => (
                      <>
                        {c.managed && <AlterClusterMenuItem cluster={c} />}
                        <DeleteObjectMenuItem
                          key="delete-object"
                          selectedObject={c}
                          onSuccessAction={props.refetchClusters}
                          objectType="CLUSTER"
                        />
                      </>
                    ),
                  },
                ]}
              />
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};

export default ClustersListPage;
