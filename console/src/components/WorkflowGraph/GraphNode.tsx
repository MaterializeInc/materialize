// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  BoxProps,
  Flex,
  FlexProps,
  HStack,
  Text,
  TextProps,
  useTheme,
} from "@chakra-ui/react";
import dagre from "@dagrejs/dagre";
import React from "react";

import { LagInfo } from "~/api/materialize/cluster/materializationLag";
import { WorkflowGraphNode } from "~/api/materialize/workflowGraphNodes";
import { WORKFLOW_GRAPH_NODE_Z_INDEX } from "~/layouts/zIndex";
import {
  formatLagInfoSimple,
  getLagInfoColorScheme,
} from "~/platform/clusters/format";
import { MaterializeTheme } from "~/theme";

import { nodeIcon } from "./icons";

// Dagre needs to know the height and width of the nodes up front, so these constants
// must reflect the height of the nodes

/** Width of a node, 280px plus 2px for borders */
export const NODE_WIDTH = 282;
/** Base height of a node, 58px plus 2px for borders */
export const BASE_NODE_HEIGHT = 60;
/** Max height of a node, 98px plus 2px for borders */
export const NODE_HEIGHT_WITH_STATUS = 100;

export const NodeStatusContainer = (props: BoxProps) => {
  return (
    <HStack
      py="2"
      px="4"
      justifyContent="space-between"
      borderRadius="0 0 7px 7px"
      {...props}
    />
  );
};

export const NodeStatusText = (props: TextProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Text
      textStyle="text-small"
      fontWeight="500"
      color={colors.white}
      {...props}
    />
  );
};

export const NodeStatus = ({
  nodeLagInfo,
}: {
  nodeLagInfo: LagInfo | undefined;
}) => {
  if (!nodeLagInfo) return <NodeStatusContainer height="8" />;
  const nodeStatusInfo = getLagInfoColorScheme(nodeLagInfo);

  const lag = formatLagInfoSimple(nodeLagInfo);

  return (
    <NodeStatusContainer background={nodeStatusInfo.nodeColor}>
      <NodeStatusText>Freshness</NodeStatusText>
      <NodeStatusText title={lag}>{lag}</NodeStatusText>
    </NodeStatusContainer>
  );
};

export const GraphNode = ({
  graph,
  node,
  nodeLagInfo,
  isSelected,
  ...flexProps
}: {
  graph: dagre.graphlib.Graph;
  node: WorkflowGraphNode | undefined;
  nodeLagInfo: LagInfo | undefined;
  isSelected: boolean;
} & FlexProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  if (!node) return null;

  const namespace = `${node.databaseName}.${node.schemaName}`;
  const fullyQualifiedName = `${node.databaseName}.${node.schemaName}.${node.name}`;

  return (
    <Flex direction="column" position="absolute" {...flexProps}>
      {node.clusterName && (
        <Box
          zIndex={WORKFLOW_GRAPH_NODE_Z_INDEX}
          backgroundColor={
            isSelected
              ? colors.accent.brightPurple
              : colors.background.secondary
          }
          position="absolute"
          top="-24px"
          px="2"
          py="2px"
          textStyle="text-small"
          fontWeight="500"
          color={isSelected ? colors.white : colors.foreground.primary}
          borderRadius="8px"
          border={isSelected ? "none" : "1px solid"}
          borderColor={isSelected ? "" : colors.border.secondary}
        >
          {node.clusterName}
        </Box>
      )}
      <Box
        as="button"
        backgroundColor={colors.background.secondary}
        border="1px solid"
        borderColor={
          isSelected ? colors.accent.brightPurple : colors.border.secondary
        }
        borderRadius="8px"
        boxShadow={
          isSelected
            ? shadows.input.focus
            : "0px 0.5px 2.5px 0 rgba(0, 0, 0, 0.08)"
        }
      >
        <Flex
          px="4"
          py="3"
          gap="4"
          alignItems="center"
          title={fullyQualifiedName}
        >
          {nodeIcon(node)}

          <Flex
            flexDirection="column"
            justifyContent="center"
            gap="2px"
            textAlign="left"
            overflowX="hidden"
          >
            <Text
              textStyle="text-small"
              fontWeight="500"
              color={colors.foreground.secondary}
            >
              {namespace}
            </Text>
            <Text
              width="100%"
              textStyle="text-ui-med"
              overflow="hidden"
              textOverflow="ellipsis"
              whiteSpace="nowrap"
              css={{
                direction: "rtl",
              }}
            >
              {node.name}
            </Text>
          </Flex>
        </Flex>
        <NodeStatus nodeLagInfo={nodeLagInfo} />
      </Box>
    </Flex>
  );
};
