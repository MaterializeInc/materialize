// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Tab, TabList, TabPanel, TabPanels, Tabs } from "@chakra-ui/react";
import React from "react";
import { Navigate, useNavigate, useParams } from "react-router-dom";

import { MAIN_CONTENT_MARGIN } from "~/layouts/BaseLayout";

import { CLUSTER_OBJECT_TYPES } from "./clusterObjectTypes";

export const ClusterObjects = () => {
  const { objectType } = useParams<{ objectType: string }>();
  const navigate = useNavigate();
  const selectedIndex = CLUSTER_OBJECT_TYPES.findIndex(
    ({ path }) => path === objectType,
  );

  if (selectedIndex === -1) {
    return (
      <Navigate
        to={`../${CLUSTER_OBJECT_TYPES[0].path}`}
        relative="path"
        replace
      />
    );
  }

  return (
    <Tabs
      variant="soft-rounded"
      size="sm"
      index={selectedIndex}
      onChange={(index) =>
        navigate(`../${CLUSTER_OBJECT_TYPES[index].path}`, {
          relative: "path",
        })
      }
      isLazy
    >
      <TabList mx={MAIN_CONTENT_MARGIN} mt={6}>
        {CLUSTER_OBJECT_TYPES.map(({ path, label }) => (
          <Tab key={path}>{label}</Tab>
        ))}
      </TabList>
      <TabPanels>
        {CLUSTER_OBJECT_TYPES.map(({ path, Component }) => (
          <TabPanel key={path} my={0}>
            <Component />
          </TabPanel>
        ))}
      </TabPanels>
    </Tabs>
  );
};
