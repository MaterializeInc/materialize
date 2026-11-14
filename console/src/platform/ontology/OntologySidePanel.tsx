// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Badge,
  Box,
  Button,
  Code,
  HStack,
  IconButton,
  Tab,
  Table,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useNavigate } from "react-router-dom";

import { CodeBlock, CopyButton } from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import {
  CloseIcon,
  CrosshairIcon,
  ExternalLinkIcon,
  TerminalIcon,
} from "~/icons";
import { useBuildObjectPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";

import { Freshness, freshnessColor, freshnessText } from "./freshness";
import { buildJoinSql } from "./joinSql";
import { LivePreview } from "./LivePreview";
import { edgeLabel, OntologyEdge, OntologyNode } from "./model";
import { Selection } from "./OntologyCanvas";
import { IssueList } from "./overviewComponents";
import { edgeKindStyle, nodeKindLabel, SIDE_PANEL_WIDTH } from "./style";
import { useOpenInShell } from "./useOpenInShell";

export interface OntologySidePanelProps {
  selection: Selection;
  onSelect: (selection: Selection) => void;
  nodesById: Map<string, OntologyNode>;
  edges: OntologyEdge[];
  /** Shown while nothing is selected. */
  overview: React.ReactNode;
  focusedNodeId: string | null;
  onFocus: (nodeId: string | null) => void;
  getFreshness: ((node: OntologyNode) => Freshness) | undefined;
  /** The cluster a node's live preview reads on, null for none. */
  previewCluster: (node: OntologyNode) => string | null;
  /** Suggests `COMMENT ON` for undocumented user relations. */
  suggestComments: boolean;
}

export const OntologySidePanel = ({
  selection,
  onSelect,
  nodesById,
  edges,
  overview,
  focusedNodeId,
  onFocus,
  getFreshness,
  previewCluster,
  suggestComments,
}: OntologySidePanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const node =
    selection?.type === "node" ? nodesById.get(selection.id) : undefined;
  const edge =
    selection?.type === "edge"
      ? edges.find((e) => e.id === selection.id)
      : undefined;

  return (
    <Box
      position="absolute"
      top={0}
      right={0}
      bottom={0}
      width={`${SIDE_PANEL_WIDTH}px`}
      borderLeft="1px solid"
      borderColor={colors.border.primary}
      background={colors.background.primary}
      overflowY="auto"
      data-testid="ontology-side-panel"
    >
      {node ? (
        <NodeDetails
          key={node.id}
          node={node}
          edges={edges}
          nodesById={nodesById}
          onSelect={onSelect}
          isFocused={focusedNodeId === node.id}
          onFocus={onFocus}
          freshness={getFreshness?.(node)}
          previewCluster={previewCluster(node)}
          suggestComments={suggestComments}
        />
      ) : edge ? (
        <EdgeDetails
          edge={edge}
          source={nodesById.get(edge.source)}
          target={nodesById.get(edge.target)}
          onSelect={onSelect}
        />
      ) : (
        <Box p={5}>{overview}</Box>
      )}
    </Box>
  );
};

const PanelHeader = ({
  title,
  subtitle,
  onClose,
  children,
}: React.PropsWithChildren<{
  title: React.ReactNode;
  subtitle: React.ReactNode;
  onClose: () => void;
}>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack align="stretch" spacing={2} p={5} pb={3}>
      <HStack justify="space-between" align="start">
        <VStack align="start" spacing={0.5} minW={0}>
          <Text textStyle="heading-sm" wordBreak="break-word">
            {title}
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {subtitle}
          </Text>
        </VStack>
        <IconButton
          aria-label="Close details"
          icon={<CloseIcon />}
          variant="inline"
          size="sm"
          onClick={onClose}
        />
      </HStack>
      {children}
    </VStack>
  );
};

const NodeDetails = ({
  node,
  edges,
  nodesById,
  onSelect,
  isFocused,
  onFocus,
  freshness,
  previewCluster,
  suggestComments,
}: {
  node: OntologyNode;
  edges: OntologyEdge[];
  nodesById: Map<string, OntologyNode>;
  onSelect: (selection: Selection) => void;
  isFocused: boolean;
  onFocus: (nodeId: string | null) => void;
  freshness: Freshness | undefined;
  previewCluster: string | null;
  suggestComments: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const openInShell = useOpenInShell();
  const buildObjectPath = useBuildObjectPath();
  const navigate = useNavigate();
  const outgoing = edges.filter((edge) => edge.source === node.id);
  const incoming = edges.filter(
    (edge) => edge.target === node.id && edge.source !== node.id,
  );
  const status = freshness ? freshnessText(freshness) : null;

  return (
    <Box>
      <PanelHeader
        title={node.label}
        subtitle={`${nodeKindLabel(node.kind)} in ${node.group}`}
        onClose={() => onSelect(null)}
      >
        <HStack spacing={2} flexWrap="wrap">
          {node.isDataProduct && (
            <Badge colorScheme="purple" title="Listed by the agent MCP server">
              MCP data product
            </Badge>
          )}
          {node.clusterName && <Badge>{node.clusterName}</Badge>}
        </HStack>
        {freshness && status && (
          <HStack spacing={2}>
            <Box
              width="8px"
              height="8px"
              borderRadius="full"
              background={freshnessColor(freshness, colors)}
            />
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              {freshness.state === "hydrating" ||
              freshness.state === "computedOnRead"
                ? status
                : `${status}, kept current with SUBSCRIBE`}
            </Text>
          </HStack>
        )}
        {node.relation && (
          <HStack spacing={1}>
            <Code
              fontSize="xs"
              px={2}
              py={1}
              borderRadius="md"
              wordBreak="break-all"
            >
              {node.relation}
            </Code>
            <CopyButton contents={node.relation} size="xs" />
          </HStack>
        )}
        <HStack spacing={2} flexWrap="wrap">
          {node.relation && !node.isUnresolved && (
            <Button
              size="sm"
              variant="secondary"
              leftIcon={<TerminalIcon />}
              onClick={() =>
                openInShell(`SELECT * FROM ${node.relation} LIMIT 100;`)
              }
            >
              Query
            </Button>
          )}
          <Button
            size="sm"
            variant="secondary"
            leftIcon={<CrosshairIcon />}
            onClick={() => onFocus(isFocused ? null : node.id)}
          >
            {isFocused ? "Show all" : "Neighborhood"}
          </Button>
          {node.objectId && node.schemaName && (
            <Button
              size="sm"
              variant="secondary"
              leftIcon={<ExternalLinkIcon />}
              onClick={() =>
                navigate(
                  buildObjectPath({
                    databaseName: node.databaseName,
                    schemaName: node.schemaName ?? "",
                    objectType: node.kind,
                    objectName: node.label,
                    id: node.objectId ?? "",
                  }),
                )
              }
            >
              Details
            </Button>
          )}
        </HStack>
        {node.description ? (
          <Text textStyle="text-ui-reg">{node.description}</Text>
        ) : node.isUnresolved ? (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            An ontology table names this relation, but no relation by this name
            exists. Fix the row or create the relation.
          </Text>
        ) : suggestComments && node.relation ? (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            No description. Agents read comments to decide when to use a
            relation:{" "}
            <Code fontSize="xs">
              COMMENT ON {commentObjectType(node.kind)} {node.relation} IS
              &apos;…&apos;;
            </Code>
          </Text>
        ) : null}
      </PanelHeader>

      <Tabs isLazy size="sm">
        <TabList px={5}>
          <Tab>Columns{node.columns ? ` (${node.columns.length})` : ""}</Tab>
          <Tab>Relationships ({outgoing.length + incoming.length})</Tab>
          {!node.isUnresolved && <Tab>Live data</Tab>}
        </TabList>
        <TabPanels>
          <TabPanel px={5}>
            <ColumnsTable node={node} />
          </TabPanel>
          <TabPanel px={5}>
            <VStack align="stretch" spacing={4}>
              <RelationshipList
                title="References"
                edges={outgoing}
                otherEnd={(edge) => nodesById.get(edge.target)}
                onSelect={onSelect}
              />
              <RelationshipList
                title="Referenced by"
                edges={incoming}
                otherEnd={(edge) => nodesById.get(edge.source)}
                onSelect={onSelect}
              />
            </VStack>
          </TabPanel>
          {!node.isUnresolved && (
            <TabPanel px={5}>
              <LivePreview node={node} clusterName={previewCluster} />
            </TabPanel>
          )}
        </TabPanels>
      </Tabs>
    </Box>
  );
};

function commentObjectType(kind: string) {
  switch (kind) {
    case "materialized-view":
      return "MATERIALIZED VIEW";
    case "view":
      return "VIEW";
    case "source":
      return "SOURCE";
    default:
      return "TABLE";
  }
}

const ColumnsTable = ({ node }: { node: OntologyNode }) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!node.columns) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Loading columns…
      </Text>
    );
  }
  if (node.columns.length === 0) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        No columns.
      </Text>
    );
  }
  return (
    <Table variant="borderless" size="sm">
      <Thead>
        <Tr>
          <Th pl={0}>Column</Th>
          <Th>Type</Th>
        </Tr>
      </Thead>
      <Tbody>
        {node.columns.map((column) => (
          <Tr key={column.name}>
            <Td pl={0} verticalAlign="top">
              <HStack spacing={1.5}>
                <Text textStyle="text-ui-med" fontFamily="mono" fontSize="xs">
                  {column.name}
                </Text>
                {node.primaryKey.includes(column.name) && (
                  <Badge fontSize="2xs" title="Key column">
                    key
                  </Badge>
                )}
              </HStack>
              {column.description && (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                  mt={1}
                >
                  {column.description}
                </Text>
              )}
            </Td>
            <Td verticalAlign="top">
              <VStack align="start" spacing={1}>
                <Text
                  fontFamily="mono"
                  fontSize="xs"
                  color={colors.foreground.secondary}
                >
                  {column.type}
                </Text>
                {column.semanticType && (
                  <Badge
                    colorScheme="purple"
                    fontSize="2xs"
                    textTransform="none"
                    title="Semantic type"
                  >
                    {column.semanticType}
                  </Badge>
                )}
              </VStack>
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
};

const RelationshipList = ({
  title,
  edges,
  otherEnd,
  onSelect,
}: {
  title: string;
  edges: OntologyEdge[];
  otherEnd: (edge: OntologyEdge) => OntologyNode | undefined;
  onSelect: (selection: Selection) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (edges.length === 0) return null;
  return (
    <VStack align="stretch" spacing={1}>
      <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
        {title}
      </Text>
      {edges.map((edge) => {
        const style = edgeKindStyle(edge.kind, colors);
        const hasIssues = edge.issues.length > 0;
        return (
          <HStack
            key={edge.id}
            as="button"
            onClick={() => onSelect({ type: "edge", id: edge.id })}
            spacing={2}
            p={2}
            borderRadius="md"
            textAlign="left"
            _hover={{ background: colors.background.secondary }}
          >
            <Box
              width="8px"
              height="8px"
              borderRadius="full"
              background={hasIssues ? colors.accent.red : style.color}
              flexShrink={0}
            />
            <VStack align="start" spacing={0} minW={0}>
              <Text textStyle="text-ui-med">
                {otherEnd(edge)?.label ?? "?"}
              </Text>
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                fontFamily="mono"
                noOfLines={1}
              >
                {style.label} · {edgeLabel(edge) || "derives from"}
              </Text>
            </VStack>
          </HStack>
        );
      })}
    </VStack>
  );
};

const EdgeDetails = ({
  edge,
  source,
  target,
  onSelect,
}: {
  edge: OntologyEdge;
  source: OntologyNode | undefined;
  target: OntologyNode | undefined;
  onSelect: (selection: Selection) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const openInShell = useOpenInShell();
  const style = edgeKindStyle(edge.kind, colors);
  const joinSql = source && target ? buildJoinSql(edge, source, target) : null;
  const hasErrors = edge.issues.some((issue) => issue.severity === "error");

  return (
    <Box>
      <PanelHeader
        title={
          <HStack as="span" spacing={2} flexWrap="wrap">
            <TextLink
              as="button"
              onClick={() =>
                source && onSelect({ type: "node", id: source.id })
              }
            >
              {source?.label}
            </TextLink>
            <Text as="span" color={colors.foreground.tertiary}>
              →
            </Text>
            <TextLink
              as="button"
              onClick={() =>
                target && onSelect({ type: "node", id: target.id })
              }
            >
              {target?.label}
            </TextLink>
          </HStack>
        }
        subtitle={`${style.label}${edge.name ? `: ${edge.name}` : ""}. ${style.description}.`}
        onClose={() => onSelect(null)}
      >
        <IssueList issues={edge.issues} />
        {edge.description && (
          <Text textStyle="text-ui-reg">{edge.description}</Text>
        )}
      </PanelHeader>
      <VStack align="stretch" spacing={4} px={5} pb={5}>
        {edge.sourceColumns.length > 0 && (
          <Table variant="borderless" size="sm">
            <Thead>
              <Tr>
                <Th pl={0}>{source?.label}</Th>
                <Th />
                <Th>{target?.label}</Th>
              </Tr>
            </Thead>
            <Tbody>
              {edge.sourceColumns.map((column, i) => (
                <Tr key={`${column}-${i}`}>
                  <Td pl={0} fontFamily="mono" fontSize="xs">
                    {column}
                  </Td>
                  <Td color={colors.foreground.tertiary}>=</Td>
                  <Td fontFamily="mono" fontSize="xs">
                    {edge.targetColumns[i] ?? "—"}
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}
        <VStack align="stretch" spacing={1}>
          {edge.cardinality && (
            <DetailRow
              label="Cardinality"
              value={edge.cardinality.replace(/_/g, " ")}
            />
          )}
          {edge.via && (
            <DetailRow label="Bridge through" value={edge.via} mono />
          )}
          {edge.declaredIn && (
            <DetailRow label="Declared in" value={edge.declaredIn} mono />
          )}
          {edge.note && <DetailRow label="Note" value={edge.note} />}
        </VStack>
        {joinSql && (
          <VStack align="stretch" spacing={2}>
            <CodeBlock title="Join" contents={joinSql} lineNumbers={false} />
            <Button
              size="sm"
              variant="secondary"
              alignSelf="start"
              leftIcon={<TerminalIcon />}
              isDisabled={hasErrors}
              title={hasErrors ? "Fix the relationship first" : undefined}
              onClick={() => openInShell(joinSql)}
            >
              Open in SQL Shell
            </Button>
          </VStack>
        )}
      </VStack>
    </Box>
  );
};

const DetailRow = ({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack align="start" spacing={3}>
      <Text
        textStyle="text-small"
        color={colors.foreground.secondary}
        minW="110px"
      >
        {label}
      </Text>
      <Text
        textStyle="text-small"
        fontFamily={mono ? "mono" : undefined}
        wordBreak="break-all"
      >
        {value}
      </Text>
    </HStack>
  );
};
