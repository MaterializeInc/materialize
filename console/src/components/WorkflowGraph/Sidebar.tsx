// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChevronDownIcon } from "@chakra-ui/icons";
import {
  Box,
  BoxProps,
  Button,
  chakra,
  Flex,
  FlexProps,
  HStack,
  Text,
  TextProps,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { Link, LinkProps } from "react-router-dom";

import { LagInfo } from "~/api/materialize/cluster/materializationLag";
import { WorkflowGraphNode } from "~/api/materialize/workflowGraphNodes";
import { ConnectorStatusPill } from "~/components/StatusPill";
import {
  formatLagInfoDetailed,
  formatObjectType,
} from "~/platform/clusters/format";
import {
  relativeClusterPath,
  useBuildSourcePath,
} from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";
import ExternalLinkIcon from "~/svg/ExternalLinkIcon";
import { MaterializeTheme } from "~/theme";
import { formatDate } from "~/utils/dateFormat";

import { nodeIcon } from "./icons";

export type OnNodeClick = (nodeId: string) => void;

export interface WorkflowGraphSidebarProps {
  nodeMap: Map<string, WorkflowGraphNode>;
  selectedNode?: WorkflowGraphNode;
  upstreamNodes: WorkflowGraphNode[];
  downstreamNodes: WorkflowGraphNode[];
  onNodeClick: OnNodeClick;
  lagInfo: LagInfo | undefined;
}

export const SIDEBAR_WIDTH = 340;

const ClusterSidebarItem = ({
  selectedNode,
}: {
  selectedNode: WorkflowGraphNode;
}) => {
  const regionSlug = useRegionSlug();
  const { getClusterById } = useAllClusters();

  const cluster = selectedNode.clusterId
    ? getClusterById(selectedNode.clusterId)
    : null;

  if (!cluster) {
    return null;
  }

  return (
    <SidebarItem pl="4" pr="5" py="1">
      <SidebarItemLabel>Cluster</SidebarItemLabel>
      <SidebarButton
        to={`/regions/${regionSlug}/clusters/${relativeClusterPath({
          id: cluster.id,
          name: cluster.name,
        })}`}
      >
        <SidebarItemValue>{selectedNode.clusterName}</SidebarItemValue>
        <ExternalLinkIcon ml="1" position="relative" top="-1px" />
      </SidebarButton>
    </SidebarItem>
  );
};

const WorkflowGraphSidebar = ({
  nodeMap,
  selectedNode,
  upstreamNodes,
  downstreamNodes,
  onNodeClick,
  lagInfo,
}: WorkflowGraphSidebarProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const sourcePath = useBuildSourcePath();

  if (!selectedNode) return;

  return (
    <Flex
      width={SIDEBAR_WIDTH}
      height="100%"
      position="absolute"
      top="0"
      right="0"
      backgroundColor={colors.background.primary}
      borderColor={colors.border.secondary}
      borderLeftWidth="1px"
      direction="column"
      overflow="auto"
    >
      {selectedNode && (
        <>
          <SidebarHeaderContainer>
            {selectedNode && (
              <Text
                textStyle="text-ui-med"
                noOfLines={1}
                title={selectedNode.name}
              >
                {selectedNode.name}
              </Text>
            )}
          </SidebarHeaderContainer>
          <SidebarSection title="Details">
            {selectedNode.sourceStatus && (
              <SidebarItem>
                <SidebarItemLabel>Status</SidebarItemLabel>
                <ConnectorStatusPill
                  connector={{
                    status: selectedNode.sourceStatus,
                    snapshotCommitted: selectedNode.sourceSnapshotCommitted,
                    type: selectedNode.sourceType,
                  }}
                />
              </SidebarItem>
            )}
            {selectedNode.sinkStatus && (
              <SidebarItem>
                <SidebarItemLabel>Status</SidebarItemLabel>
                <ConnectorStatusPill
                  connector={{
                    status: selectedNode.sinkStatus,
                    type: selectedNode.sourceType,
                  }}
                />
              </SidebarItem>
            )}
            {selectedNode.sourceType &&
            selectedNode.sourceType !== "subsource" ? (
              <SidebarItem pl="4" pr="5" py="1">
                <SidebarItemLabel>Type</SidebarItemLabel>
                <SidebarButton to={sourcePath(selectedNode)}>
                  <SidebarItemValue>
                    {formatObjectType(selectedNode)}
                  </SidebarItemValue>
                  <ExternalLinkIcon ml="1" position="relative" top="-1px" />
                </SidebarButton>
              </SidebarItem>
            ) : (
              <SidebarItem>
                <SidebarItemLabel>Type</SidebarItemLabel>
                <SidebarItemValue>
                  {formatObjectType(selectedNode)}
                </SidebarItemValue>
              </SidebarItem>
            )}
            <SidebarItem>
              <SidebarItemLabel>Database</SidebarItemLabel>
              <SidebarItemValue>{selectedNode.databaseName}</SidebarItemValue>
            </SidebarItem>
            <SidebarItem>
              <SidebarItemLabel>Schema</SidebarItemLabel>
              <SidebarItemValue>{selectedNode.schemaName}</SidebarItemValue>
            </SidebarItem>

            <React.Suspense fallback={null}>
              <ClusterSidebarItem selectedNode={selectedNode} />
            </React.Suspense>
            {selectedNode.connectionName && (
              <SidebarItem>
                <SidebarItemLabel>Connection</SidebarItemLabel>
                <SidebarItemValue>
                  {selectedNode.connectionName}
                </SidebarItemValue>
              </SidebarItem>
            )}
            <SidebarItem>
              <SidebarItemLabel>Created</SidebarItemLabel>
              <SidebarItemValue>
                {formatDate(new Date(selectedNode.createdAt), "MMM d, yyyy z")}
              </SidebarItemValue>
            </SidebarItem>
          </SidebarSection>
          {lagInfo && (
            <SidebarSection title="Data Freshness">
              <SidebarItem>
                <SidebarItemLabel>Freshness</SidebarItemLabel>
                <SidebarItemValue p={1}>
                  {formatLagInfoDetailed(lagInfo)}
                </SidebarItemValue>
              </SidebarItem>
            </SidebarSection>
          )}
          <SidebarSection title="Upstream">
            <VStack spacing="2" align="stretch">
              {upstreamNodes.map((node) => (
                <RelatedNode
                  key={node.id}
                  node={node}
                  onNodeClick={onNodeClick}
                />
              ))}
              {upstreamNodes.length === 0 && (
                <SidebarItem>
                  <SidebarItemValue>No upstream objects</SidebarItemValue>
                </SidebarItem>
              )}
            </VStack>
          </SidebarSection>
          <SidebarSection title="Downstream">
            <VStack spacing="2" align="stretch">
              {downstreamNodes.map((node) => (
                <RelatedNode
                  key={node.id}
                  node={node}
                  onNodeClick={onNodeClick}
                />
              ))}
              {downstreamNodes.length === 0 && (
                <SidebarItem>
                  <SidebarItemValue>No downstream objects</SidebarItemValue>
                </SidebarItem>
              )}
            </VStack>
          </SidebarSection>
        </>
      )}
    </Flex>
  );
};

export const SidebarHeaderContainer = (props: React.PropsWithChildren) => {
  return (
    <Flex px="4" py="2" alignItems="center" justifyContent="space-between">
      {props.children}
    </Flex>
  );
};

export interface SidebarHeadingProps {
  title: string;
}

export const SidebarHeading = (props: SidebarHeadingProps) => {
  return (
    <Text textStyle="text-small" fontWeight="500">
      {props.title}
    </Text>
  );
};

export interface SidebarSectionProps {
  title: string;
  children: React.ReactNode | React.ReactNode[];
}

export const SidebarSection = (props: SidebarSectionProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [collapsed, setCollapsed] = React.useState(false);
  return (
    <Box
      borderTop="1px solid"
      borderColor={colors.border.primary}
      mb={collapsed ? 0 : 1}
    >
      <SidebarHeaderContainer>
        <SidebarHeading title={props.title} />
        <Button
          height="4"
          width="4"
          p="0"
          minWidth="0"
          variant="ghost"
          onClick={() => setCollapsed((v) => !v)}
        >
          <ChevronDownIcon
            transform={collapsed ? "rotate(180deg)" : undefined}
          />
        </Button>
      </SidebarHeaderContainer>
      {!collapsed && props.children}
    </Box>
  );
};

export const SidebarItem = (props: FlexProps) => {
  return (
    <Flex
      px="4"
      py="1"
      alignItems="center"
      justifyContent="space-between"
      width="100%"
      {...props}
    />
  );
};

export const SidebarItemLabel = (props: TextProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Text
      color={colors.foreground.tertiary}
      textStyle="text-ui-med"
      {...props}
    />
  );
};

export const SidebarItemValue = (props: TextProps) => {
  let title: string | undefined = props.title;

  if (title === undefined) {
    if (
      Array.isArray(props.children) &&
      props.children.every((child) => typeof child === "string")
    ) {
      title = props.children.join(" ");
    } else if (typeof props.children === "string") {
      title = props.children;
    }
  }

  return (
    <Text
      overflow="hidden"
      textOverflow="ellipsis"
      whiteSpace="nowrap"
      textStyle="text-ui-reg"
      title={title}
      {...props}
    />
  );
};

export const SidebarButton = ({ children, ...props }: BoxProps & LinkProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box as={Link} target="_blank" minWidth="0" {...props}>
      <HStack
        gap="0"
        px="1"
        py="1"
        position="relative"
        right="-8px"
        rounded="md"
        _hover={{
          backgroundColor: colors.background.tertiary,
        }}
      >
        {children}
      </HStack>
    </Box>
  );
};

export interface RelatedNodeProps {
  node: WorkflowGraphNode;
  onNodeClick: OnNodeClick;
}

export const RelatedNode = ({ node, onNodeClick }: RelatedNodeProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const fullyQualifiedName = `${node.databaseName}.${node.schemaName}.${node.name}`;

  return (
    <chakra.button onClick={() => onNodeClick(node.id)} width="100%">
      <SidebarItem _hover={{ backgroundColor: colors.background.secondary }}>
        <HStack width="100%">
          {nodeIcon(node)}
          <SidebarItemValue title={fullyQualifiedName}>
            {node.name}
          </SidebarItemValue>
        </HStack>
      </SidebarItem>
    </chakra.button>
  );
};

export default WorkflowGraphSidebar;
