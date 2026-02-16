// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Flex,
  HStack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import React, { useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { createNamespace } from "~/api/materialize";
import { PermissionError } from "~/api/materialize/DatabaseError";
import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { InfoIcon } from "~/icons";
import { useBuildWorkflowGraphPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { truncateMaxWidth } from "~/theme/components/Table";
import { notNullOrUndefined } from "~/util";
import { formatMemoryUsage } from "~/utils/format";

import { formatTableLagInfo } from "./format";
import {
  useLargestClusterReplica,
  useLargestMaintainedQueries,
  useMaterializationLag,
} from "./queries";

const typeLabel = (type: string) => {
  switch (type) {
    case "index":
      return "Index";
    case "materialized-view":
      return "Materialized View";
    default:
      return "-";
  }
};

export interface LargestMaintainedQueriesProps {
  clusterId: string;
  clusterName: string;
}

const LargestMaintainedQueriesError = ({
  replicaName,
  error,
}: {
  replicaName?: string | null;
  error: unknown;
}) => {
  Sentry.captureException(
    new Error("LargestMaintainedQueries failed to load"),
    {
      extra: {
        details: error,
      },
    },
  );
  if (
    error instanceof Error &&
    error.message ===
      "cannot execute queries on cluster containing sources or sinks"
  ) {
    // We could try again and check this up front, but there would still be a race conditon, so we will just ignore the error
    return null;
  }
  if (error instanceof PermissionError) {
    return (
      <Flex alignItems="center" justifyContent="center">
        <Alert
          variant="info"
          message={
            <Text>
              You&apos;ll need{" "}
              <Text as="span" textStyle="monospace">
                USAGE
              </Text>{" "}
              privilege on this cluster to see additional insights.
            </Text>
          }
        />
      </Flex>
    );
  }

  const replica = replicaName ? `replica ${replicaName}` : "your cluster";
  return (
    <Flex justifyContent="center" mb="6">
      <Alert
        variant="info"
        message={
          <>
            It&apos;s taking longer than usual to fetch fine-grained memory
            usage about your indexes and materialized views from {replica},
            which might mean it&apos;s busy.
          </>
        }
      />
    </Flex>
  );
};

const LargestMaintainedQueries = (props: LargestMaintainedQueriesProps) => {
  return (
    <AppErrorBoundary
      renderFallback={({ error }) => (
        <LargestMaintainedQueriesError error={error} />
      )}
    >
      <React.Suspense fallback={<LoadingContainer />}>
        <LargestReplicaLoader {...props} />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

const LargestReplicaLoader = (props: LargestMaintainedQueriesProps) => {
  const { data: largestReplica } = useLargestClusterReplica({
    clusterId: props.clusterId,
  });

  // Don't show anything if the cluster has no replicas
  if (!largestReplica) return null;

  return (
    <AppErrorBoundary
      renderFallback={({ error }) => (
        <LargestMaintainedQueriesError
          error={error}
          replicaName={largestReplica.name}
        />
      )}
    >
      <LargestMaintainedQueriesInner
        {...props}
        replicaName={largestReplica.name}
        replicaHeapLimit={Number(largestReplica.heapLimit)}
      />
    </AppErrorBoundary>
  );
};

const LargestMaintainedQueriesInner = ({
  clusterName,
  replicaName,
  replicaHeapLimit,
}: LargestMaintainedQueriesProps & {
  replicaName: string;
  replicaHeapLimit: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const navigate = useNavigate();
  const workflowGraphPath = useBuildWorkflowGraphPath();

  const { data: largestMaintainedQueries } = useLargestMaintainedQueries({
    clusterName,
    replicaName,
    replicaHeapLimit,
  });

  const objectIds = useMemo(
    () =>
      largestMaintainedQueries?.map((r) => r.id).filter(notNullOrUndefined) ??
      [],
    [largestMaintainedQueries],
  );

  const { data: materializationLag, error: lagFromSourceObjectsError } =
    useMaterializationLag({
      objectIds,
    });

  if (lagFromSourceObjectsError) {
    return (
      <LargestMaintainedQueriesError
        error={lagFromSourceObjectsError}
        replicaName={replicaName}
      />
    );
  }
  if (!largestMaintainedQueries || largestMaintainedQueries?.length === 0) {
    // If the cluster has no maintained queries, show nothing
    return null;
  }
  return (
    <>
      <Text textStyle="heading-xs">Resource intensive objects</Text>
      <HStack spacing={1}>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          These objects are using the most resources on this cluster.
        </Text>
        <Tooltip
          label={`These metrics are pulled from replica: ${replicaName}`}
        >
          <InfoIcon />
        </Tooltip>
      </HStack>
      <Table variant="linkable" borderRadius="xl" mt={4}>
        <Thead>
          <Tr>
            <Th>Name</Th>
            <Th>Type</Th>
            <Th>
              <HStack>
                <Text>Memory Utilization</Text>
                <Tooltip
                  display="inline"
                  label="Includes intermediate state required to efficiently maintain the object."
                >
                  <InfoIcon />
                </Tooltip>
              </HStack>
            </Th>
            <Th>Freshness</Th>
          </Tr>
        </Thead>
        <Tbody>
          {largestMaintainedQueries?.map((r) => {
            const lagInfo = r.id ? materializationLag?.lagMap.get(r.id) : null;

            const formattedLag = formatTableLagInfo(lagInfo);

            return (
              <Tr
                key={r.id ?? r.dataflowId}
                onClick={() => {
                  if (r.isOrphanedDataflow) {
                    return;
                  }
                  const { id, name, databaseName, schemaName } = r;
                  if (id && name && schemaName) {
                    const path = workflowGraphPath({
                      databaseObject: { id, name, databaseName, schemaName },
                      type: r.type,
                    });
                    if (path) navigate(path);
                  }
                }}
                cursor={!r.isOrphanedDataflow ? "pointer" : "auto"}
              >
                <Td {...truncateMaxWidth} py="2">
                  <Text
                    textStyle="text-small"
                    fontWeight="500"
                    noOfLines={1}
                    color={colors.foreground.secondary}
                  >
                    {createNamespace(r.databaseName, r.schemaName)}
                  </Text>
                  <Text textStyle="text-ui-med" noOfLines={1}>
                    {r.name}
                  </Text>
                </Td>
                <Td>{typeLabel(r.type)}</Td>
                <Td>
                  {formatMemoryUsage({
                    size: r.size,
                    memoryPercentage: r.memoryPercentage,
                  })}{" "}
                  {lagInfo && !lagInfo.hydrated && (
                    <Tooltip label="Memory usage will continue increase until hydration is complete.">
                      <InfoIcon />
                    </Tooltip>
                  )}
                </Td>
                <Td>{formattedLag}</Td>
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </>
  );
};

export default LargestMaintainedQueries;
