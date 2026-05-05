// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Tab, TabList, TabPanel, TabPanels, Tabs } from "@chakra-ui/react";
import React from "react";

import { ObjectDetailsCard } from "./ObjectDetailsCard";
import { ObjectFreshness } from "./ObjectFreshness";
import { ObjectMetadata } from "./ObjectMetadata";
import { MaintainedObjectListItem } from "./queries";

export interface ObjectDetailPanelProps {
  item: MaintainedObjectListItem;
}

export const ObjectDetailPanel = ({ item }: ObjectDetailPanelProps) => {
  return (
    <Box p={4}>
      <ObjectDetailsCard item={item} />
      <Tabs mt={6}>
        <TabList mb={6}>
          <Tab>Metadata</Tab>
          <Tab>Freshness</Tab>
        </TabList>
        <TabPanels>
          <TabPanel px={0}>
            <ObjectMetadata item={item} />
          </TabPanel>
          <TabPanel px={0}>
            <ObjectFreshness item={item} />
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
};

export default ObjectDetailPanel;
