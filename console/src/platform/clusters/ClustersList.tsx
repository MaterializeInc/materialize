// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { FormLabel, HStack, Switch } from "@chakra-ui/react";
import React from "react";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { CodeBlock } from "~/components/copyableComponents";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import { useFlags } from "~/hooks/useFlags";
import { ClustersIcon } from "~/icons";
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
import { useAllClusters } from "~/store/allClusters";

import { ClusterTable } from "./ClusterTable";
import { ClusterUsageTable } from "./ClusterUsageTable";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import { useOwners } from "./queries";
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
  const { data: clusters, snapshotComplete, isError } = useAllClusters();
  const { isOwner } = useOwners();
  const flags = useFlags();

  const orderedClusters = React.useMemo(() => {
    const visibleClusters = clusters
      .filter((c) => showSystemObjects || !isSystemCluster(c.id))
      .map((c) => ({ ...c, isOwner: isOwner(c.ownerId) }));
    // The subscribe upserts by id, so the atom's order is arbitrary. Sort each
    // group by name and keep system clusters at the end.
    const byName = (a: ClusterWithOwnership, b: ClusterWithOwnership) =>
      a.name.localeCompare(b.name);
    const systemClusters = visibleClusters
      .filter((c) => isSystemCluster(c.id))
      .sort(byName);
    const nonSystemClusters = visibleClusters
      .filter((c) => !isSystemCluster(c.id))
      .sort(byName);
    return [...nonSystemClusters, ...systemClusters];
  }, [clusters, isOwner, showSystemObjects]);

  if (isError) {
    return <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />;
  }

  // The atom starts out empty, so the empty state has to wait for the snapshot
  // or it would flash before the first rows arrive.
  if (!snapshotComplete) {
    return <LoadingContainer />;
  }

  if (orderedClusters.length === 0) {
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

  // The two tables report on different subjects: one row per cluster summarizing
  // its replicas, versus a row per replica carrying its own utilization. Their
  // column sets and row models have nothing in common, so the flag picks a whole
  // table rather than switching columns within one.
  if (flags["usage-metrics-in-cluster-list-CNS121"]) {
    return <ClusterUsageTable clusters={orderedClusters} />;
  }
  return <ClusterTable clusters={orderedClusters} />;
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

export default ClustersListPage;
