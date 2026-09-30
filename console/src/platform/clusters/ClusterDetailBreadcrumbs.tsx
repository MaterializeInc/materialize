// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, MenuItem } from "@chakra-ui/react";
import React from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import OverflowMenu from "~/components/OverflowMenu";
import { UiPreviewToggle } from "~/components/UiPreviewToggle";
import { Breadcrumb, PageBreadcrumbs } from "~/layouts/BaseLayout";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { useAllClusters } from "~/store/allClusters";
import { assert } from "~/util";

import { replaceClusterIdAndName } from "../routeHelpers";
import AlterClusterMenuItem from "./AlterClusterMenuItem";
import { useOwners } from "./queries";
import { useShowSystemObjects } from "./useShowSystemObjects";

export const ClusterDetailBreadcrumbs = (props: { crumbs: Breadcrumb[] }) => {
  const [showSystemObjects] = useShowSystemObjects();
  const { clusterId, clusterName } = useParams<ClusterParams>();
  const { data: clusters, getClusterById } = useAllClusters();
  const { isOwner } = useOwners();
  const { pathname, search } = useLocation();
  assert(clusterId);
  assert(clusterName);
  const cluster = getClusterById(clusterId);

  // The subscribe upserts by id, so the atom's order is arbitrary.
  const clustersToShow = clusters
    .filter((c) => showSystemObjects || !isSystemCluster(c.id))
    .sort((a, b) => a.name.localeCompare(b.name));

  const menu = (
    <>
      {clustersToShow.map((c) => (
        <MenuItem
          as={Link}
          disabled={c.name === clusterName}
          to={
            replaceClusterIdAndName({
              pathname,
              currentClusterId: clusterId,
              currentClusterName: clusterName,
              targetCluster: c,
            }) + search
          }
          key={c.id}
        >
          {c.name}
        </MenuItem>
      ))}
    </>
  );

  return (
    <PageBreadcrumbs
      crumbs={props.crumbs}
      contextMenuChildren={menu}
      rightSideChildren={
        <HStack spacing={2}>
          <UiPreviewToggle
            previewKey="clusterDetailsRedesign"
            label="Show the new cluster details page"
            optOutLabel="Show classic cluster details page"
          />
          {cluster && (
            <OverflowMenuContainer
              cluster={{ ...cluster, isOwner: isOwner(cluster.ownerId) }}
            />
          )}
        </HStack>
      }
    />
  );
};

const OverflowMenuContainer = ({
  cluster,
}: {
  cluster: ClusterWithOwnership;
}) => {
  return (
    <OverflowMenu
      items={[
        {
          visible: !isSystemCluster(cluster.id) && cluster.managed,
          render: () => <AlterClusterMenuItem cluster={cluster} />,
        },
        {
          visible: !isSystemCluster(cluster.id) && cluster?.isOwner,
          render: () =>
            cluster && (
              <DeleteObjectMenuItem
                key="delete-object"
                selectedObject={cluster}
                // subscribe will update our list and the cluster routes will redirect
                onSuccessAction={() => undefined}
                objectType="CLUSTER"
              />
            ),
        },
      ]}
    />
  );
};
