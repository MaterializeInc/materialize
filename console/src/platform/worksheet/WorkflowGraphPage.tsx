// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ArrowBackIcon } from "@chakra-ui/icons";
import {
  Box,
  Flex,
  HStack,
  IconButton,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { useNavigate, useParams } from "react-router-dom";

import WorkflowGraph from "~/components/WorkflowGraph/WorkflowGraph";
import { shellPath } from "~/platform/routeHelpers";
import { useAllObjects } from "~/store/allObjects";
import { useRegionSlug } from "~/store/environments";
import type { MaterializeTheme } from "~/theme";

/** Overlay page for the workflow graph (dependency DAG). Reads object ID from URL params. */
const WorkflowGraphPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const { data: allObjects } = useAllObjects();
  const object = allObjects.find((o) => o.id === id);

  const handleBack = React.useCallback(() => {
    navigate(shellPath(regionSlug));
  }, [navigate, regionSlug]);

  if (!id) return null;

  return (
    <Flex direction="column" height="100%" overflow="hidden">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.primary}
        flexShrink={0}
        spacing="3"
      >
        <IconButton
          icon={<ArrowBackIcon />}
          aria-label="Back to worksheet"
          onClick={handleBack}
          variant="ghost"
          size="sm"
        />
        <Text fontWeight="medium" fontSize="md" noOfLines={1}>
          {object?.name ?? id}
        </Text>
      </HStack>
      <Box flex="1" overflow="hidden">
        <WorkflowGraph focusedObjectId={id} />
      </Box>
    </Flex>
  );
};

export default WorkflowGraphPage;
