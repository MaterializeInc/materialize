// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { CATALOG_SERVER_CLUSTER } from "~/api/materialize/executeSql";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import { MaterializeTheme } from "~/theme";

import { OntologyGraph } from "./model";
import { OntologyExplorer, OverviewContext } from "./OntologyExplorer";
import { Figure } from "./overviewComponents";
import { useCatalogOntologyGraph } from "./queries";

const previewOnCatalogServer = () => CATALOG_SERVER_CLUSTER;

/** Semantic types by how many entities carry a column of each. */
function semanticTypeUsage(graph: OntologyGraph) {
  const entities = new Map<string, Set<string>>();
  for (const node of graph.nodes) {
    for (const column of node.columns ?? []) {
      if (!column.semanticType) continue;
      const set = entities.get(column.semanticType) ?? new Set<string>();
      set.add(node.id);
      entities.set(column.semanticType, set);
    }
  }
  return [...entities.entries()].sort(
    (a, b) => b[1].size - a[1].size || a[0].localeCompare(b[0]),
  );
}

export const CatalogLens = () => {
  const { graph, unsupported, isLoading, isError } = useCatalogOntologyGraph();
  const [groups, setGroups] = React.useState<ReadonlySet<string> | null>(null);
  const allGroups = React.useMemo(
    () => [...new Set(graph?.nodes.map((node) => node.group))].sort(),
    [graph],
  );
  const renderOverview = React.useCallback(
    (context: OverviewContext) => <CatalogOverview context={context} />,
    [],
  );

  if (unsupported) {
    return (
      <ErrorBox message="The system catalog ontology needs a newer version of Materialize." />
    );
  }
  if (isError) {
    return (
      <ErrorBox message="An error occurred loading the catalog ontology." />
    );
  }
  if (isLoading || !graph) return <LoadingContainer />;

  return (
    <OntologyExplorer
      name="system-catalog-ontology"
      graph={graph}
      allGroups={allGroups}
      groups={groups}
      onGroupsChange={setGroups}
      defaultShowEdgeLabels={false}
      renderOverview={renderOverview}
      previewCluster={previewOnCatalogServer}
      isLive={false}
      suggestComments={false}
      isReady
    />
  );
};

const CatalogOverview = ({ context }: { context: OverviewContext }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const usage = React.useMemo(
    () => semanticTypeUsage(context.graph),
    [context.graph],
  );
  const [activeType, setActiveType] = React.useState<string | null>(null);
  const { setHighlighted } = context;

  const toggle = (type: string, entities: Set<string>) => {
    if (activeType === type) {
      setActiveType(null);
      setHighlighted(null);
    } else {
      setActiveType(type);
      setHighlighted(entities);
    }
  };
  // The highlight lives in the explorer, so clear it when this unmounts.
  React.useEffect(() => () => setHighlighted(null), [setHighlighted]);

  return (
    <VStack align="stretch" spacing={6}>
      <VStack align="stretch" spacing={2}>
        <Text textStyle="heading-sm">The system catalog, as an ontology</Text>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          Materialize describes its own catalog in{" "}
          <Text as="span" fontFamily="mono" fontSize="sm">
            mz_internal.mz_ontology_*
          </Text>
          : what each relation means, how relations join, and which id domain
          each column holds. Agents on the MCP developer server read it before
          writing catalog queries. Select an entity to stream its rows.
        </Text>
      </VStack>
      <HStack spacing={6}>
        <Figure value={context.drawn.nodes} label="Entities" />
        <Figure value={context.drawn.edges} label="Relationships" />
        <Figure value={usage.length} label="Semantic types" />
      </HStack>
      <VStack align="stretch" spacing={2}>
        <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
          Semantic types
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Columns of the same SQL type can hold different kinds of id. A
          GlobalId and a CatalogItemId are both text and look alike, but joining
          one to the other is wrong. Select a type to find the entities that
          carry it.
        </Text>
        {usage.map(([type, entities]) => (
          <HStack
            key={type}
            as="button"
            justify="space-between"
            px={2}
            py={1}
            borderRadius="md"
            background={
              activeType === type ? colors.background.accent : undefined
            }
            _hover={{ background: colors.background.secondary }}
            onClick={() => toggle(type, entities)}
            aria-pressed={activeType === type}
          >
            <HStack spacing={2}>
              <Box
                width="8px"
                height="8px"
                borderRadius="full"
                background={
                  activeType === type
                    ? colors.accent.orange
                    : colors.border.secondary
                }
              />
              <Text textStyle="text-small" fontFamily="mono">
                {type}
              </Text>
            </HStack>
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              {entities.size}
            </Text>
          </HStack>
        ))}
      </VStack>
    </VStack>
  );
};
