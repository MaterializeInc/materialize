// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuItem, useDisclosure } from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";

import { AlterClusterModal } from "./AlterClusterModal";
import {
  availableClusterSizesQuery,
  useMaxReplicasPerCluster,
} from "./queries";

const AlterClusterMenuItem = ({ cluster }: { cluster: Cluster }) => {
  // NOTE: this mounts while its overflow menu is closed, in cluster list rows
  // and the cluster detail header. Suspending would swap the page it renders
  // in for that page's Suspense fallback.
  const { data: clusterSizes } = useQuery(availableClusterSizesQuery());
  const { data: maxReplicas } = useMaxReplicasPerCluster();

  const { isOpen, onOpen, onClose } = useDisclosure();

  return (
    <>
      <MenuItem
        isDisabled={!clusterSizes}
        onClick={(e) => {
          e.stopPropagation();
          onOpen();
        }}
        textStyle="text-ui-med"
      >
        Alter cluster
      </MenuItem>
      {isOpen && clusterSizes && (
        <AlterClusterModal
          cluster={cluster}
          clusterSizes={clusterSizes}
          isOpen={isOpen}
          maxReplicas={maxReplicas ?? undefined}
          onClose={onClose}
        />
      )}
    </>
  );
};

export default AlterClusterMenuItem;
