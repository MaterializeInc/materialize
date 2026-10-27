// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Tooltip } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import { LatestOfflineReplicaMap } from "~/api/materialize/cluster/useLatestOfflineReplica";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import OverflowMenu from "~/components/OverflowMenu";
import TextLink from "~/components/TextLink";
import { InfoIcon } from "~/icons";
import { relativeClusterPath } from "~/platform/routeHelpers";

import AlterClusterMenuItem from "./AlterClusterMenuItem";

/**
 * Shared data threaded into cell components via TanStack's table `meta`.
 * Read from `info.table.options.meta` and cast to this shape inside cells.
 */
export interface ClusterTableMeta {
  offlineReplicaMap: LatestOfflineReplicaMap | undefined;
}

export const ClusterNameCell = ({
  cluster,
}: {
  cluster: ClusterWithOwnership;
}) => (
  <HStack>
    <TextLink
      as={RouterLink}
      aria-label={`View detailed information about cluster ${cluster.name}`}
      to={relativeClusterPath(cluster)}
      textStyle="text-ui-med"
      noOfLines={1}
    >
      {cluster.name}
    </TextLink>
    {isSystemCluster(cluster.id) && (
      <Tooltip
        label="This is a built-in system cluster. You are not billed for this cluster."
        lineHeight={1.2}
      >
        <InfoIcon />
      </Tooltip>
    )}
  </HStack>
);

export const ClusterActionsCell = ({
  cluster,
}: {
  cluster: ClusterWithOwnership;
}) => (
  <OverflowMenu
    items={[
      {
        visible: !isSystemCluster(cluster.id) && cluster.isOwner,
        render: () => (
          <>
            {cluster.managed && <AlterClusterMenuItem cluster={cluster} />}
            <DeleteObjectMenuItem
              key="delete-object"
              selectedObject={cluster}
              // the subscribe drops the row from our list
              onSuccessAction={() => undefined}
              objectType="CLUSTER"
            />
          </>
        ),
      },
    ]}
  />
);
