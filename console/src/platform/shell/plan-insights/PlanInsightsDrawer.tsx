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
  Drawer,
  DrawerBody,
  DrawerCloseButton,
  DrawerContent,
  DrawerFooter,
  DrawerHeader,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";

import { historyItemAtom, shellStateAtom } from "../store/shell";
import { INSIGHTS_LIST } from "./utils";

const PlanInsightsDrawer = () => {
  const [shellState, setShellState] = useAtom(shellStateAtom);
  const { historyId, commandResultIndex } =
    shellState.currentPlanInsights ?? {};
  const [historyItem] = useAtom(historyItemAtom(historyId ?? ""));
  const onClose = () => {
    setShellState((prevShellState) => ({
      ...prevShellState,
      currentPlanInsights: null,
    }));
  };

  if (historyItem?.kind !== "command" || commandResultIndex === undefined) {
    return null;
  }

  const planInsights =
    historyItem.commandResultsDisplayStates[commandResultIndex].planInsights;

  if (!planInsights) {
    return null;
  }

  const insightsToRender = INSIGHTS_LIST.filter(({ shouldRender }) =>
    shouldRender(planInsights),
  );

  if (insightsToRender.length === 0) {
    return null;
  }

  return (
    <Drawer
      isOpen
      onClose={onClose}
      blockScrollOnMount={false}
      size="lg"
      trapFocus={false}
      key={`${historyId}.${commandResultIndex}`}
    >
      <DrawerContent
        containerProps={{
          width: "auto",
        }}
      >
        <DrawerCloseButton />
        <DrawerHeader>Query Insights</DrawerHeader>

        <DrawerBody overflow="auto">
          <VStack spacing="4" alignItems="flex-start">
            {insightsToRender.map((insight, idx) => (
              <insight.component key={idx} planInsights={planInsights} />
            ))}
          </VStack>
        </DrawerBody>
        <DrawerFooter justifyContent="flex-start">
          <Button variant="outline" mr={3} onClick={onClose}>
            Close
          </Button>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  );
};

export default PlanInsightsDrawer;
