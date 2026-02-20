// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  HStack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { ClusterReplicaWithUtilizaton } from "~/api/materialize/cluster/replicasWithUtilization";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import { ClustersIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
  IconBox,
  SampleCodeBoxWrapper,
} from "~/layouts/listPageComponents";
import docUrls from "~/mz-doc-urls.json";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";

import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import NewReplicaModal from "./NewReplicaModal";
import {
  useClusterReplicasWithUtilization,
  useClusters,
  useMaxReplicasPerCluster,
} from "./queries";

export const ClusterReplicasPage = () => {
  return (
    <AppErrorBoundary
      fallback={<ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />}
    >
      <React.Suspense fallback={<LoadingContainer />}>
        <ClusterReplicasInner />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

const ClusterReplicasInner = () => {
  const { track } = useSegment();

  const { clusterId, clusterName } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById } = useClusters();
  const cluster = getClusterById(clusterId);
  const { data: replicas, refetch } = useClusterReplicasWithUtilization({
    clusterId,
  });

  const { data: maxReplicas } = useMaxReplicasPerCluster();
  const { isOpen, onOpen, onClose } = useDisclosure();

  const handleCreateReplica = () => {
    onClose();
    refetch();
  };

  const isEmpty = replicas && replicas.length === 0;
  assert(clusterName);

  return (
    <MainContentContainer>
      <HStack mb="6" alignItems="flex-start" justifyContent="space-between">
        <Text textStyle="heading-sm">Replicas</Text>
        {cluster &&
          !cluster.managed &&
          cluster.isOwner &&
          replicas &&
          maxReplicas &&
          replicas.length < maxReplicas && (
            <Button
              variant="primary"
              size="sm"
              onClick={() => {
                onOpen();
                track("New Replica Clicked");
              }}
            >
              New Replica
            </Button>
          )}
      </HStack>
      {isEmpty ? (
        <EmptyListWrapper>
          <EmptyListHeader>
            <IconBox type="Missing">
              <ClustersIcon />
            </IconBox>
            <EmptyListHeaderContents
              title="This cluster has no replicas"
              helpText="Without replicas, your cluster cannot compute dataflows."
            />
          </EmptyListHeader>
          <SampleCodeBoxWrapper
            docsUrl={docUrls["/docs/sql/create-cluster-replica/"]}
          >
            <CodeBlock
              title="Create a cluster replica"
              contents={
                cluster && cluster.managed
                  ? `ALTER CLUSTER ${clusterName}\nSET (REPLICATION FACTOR = <factor>);`
                  : `CREATE CLUSTER REPLICA\n${clusterName}.<replica_name>\nSIZE = 'xsmall';`
              }
              lineNumbers
            >
              {cluster && cluster.managed
                ? `ALTER CLUSTER ${clusterName}\nSET (REPLICATION FACTOR = <factor>);`
                : `CREATE CLUSTER REPLICA\n${clusterName}.<replica_name>\nSIZE = 'xsmall';`}
            </CodeBlock>
          </SampleCodeBoxWrapper>
        </EmptyListWrapper>
      ) : (
        <ReplicaTable
          clusterId={clusterId}
          replicas={replicas ?? []}
          refetchReplicas={refetch}
        />
      )}
      <NewReplicaModal
        isOpen={isOpen}
        onClose={onClose}
        clusterName={clusterName}
        onSubmit={handleCreateReplica}
      />
    </MainContentContainer>
  );
};

interface ReplicaTableProps {
  clusterId?: string;
  replicas: ClusterReplicaWithUtilizaton[];
  refetchReplicas: () => void;
}

const ReplicaTable = (props: ReplicaTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const showDiskColumn = props.replicas.some((r) => r.disk);

  return (
    <Table variant="standalone" data-testid="cluster-table" borderRadius="xl">
      <Thead>
        <Tr>
          <Th>Name</Th>
          <Th>Size</Th>
          <Th>CPU</Th>
          <Th>Memory Utilization</Th>
          {showDiskColumn && <Th>Disk</Th>}
          <Th width={OVERFLOW_BUTTON_WIDTH}></Th>
        </Tr>
      </Thead>
      <Tbody>
        {props.replicas.map((r) => {
          return (
            <Tr key={r.name}>
              <Td>{r.name}</Td>
              <Td>{r.size}</Td>
              <Td>
                {r.cpuPercent && (
                  <>
                    {r.cpuPercent.toFixed(1)}
                    <Text as="span" color={colors.foreground.secondary}>
                      %
                    </Text>
                  </>
                )}
              </Td>
              <Td>
                {r.memoryUtilizationPercent && (
                  <>
                    {r.memoryUtilizationPercent.toFixed(1)}
                    <Text as="span" color={colors.foreground.secondary}>
                      %
                    </Text>
                  </>
                )}
              </Td>
              {showDiskColumn && (
                <Td>
                  {r.diskPercent && (
                    <>
                      {r.diskPercent.toFixed(1)}
                      <Text as="span" color={colors.foreground.secondary}>
                        %
                      </Text>
                    </>
                  )}
                </Td>
              )}
              <Td>
                <OverflowMenu
                  items={[
                    {
                      visible: r.isOwner,
                      render: () => (
                        <DeleteObjectMenuItem
                          key="delete-object"
                          selectedObject={r}
                          onSuccessAction={props.refetchReplicas}
                          objectType="CLUSTER REPLICA"
                        />
                      ),
                    },
                  ]}
                />
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
};

export default ClusterReplicasPage;
