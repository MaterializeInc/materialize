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

import { OntologyTableRow } from "~/api/materialize/ontology/ontology";
import { CodeBlock } from "~/components/copyableComponents";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { useNow } from "~/hooks/useNow";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import { displayName, groupName } from "./dataProductGraph";
import { freshnessOf } from "./freshness";
import { OntologyGraph, OntologyNode } from "./model";
import { OntologyExplorer, OverviewContext } from "./OntologyExplorer";
import { Figure, IssueList } from "./overviewComponents";
import { useDataProductOntologyGraph, useRelationVitals } from "./queries";

/**
 * Above this many relations, the explorer opens on the schemas the ontology
 * tables describe instead of everything, since a graph that large is neither
 * readable nor cheap to keep live.
 */
const MAX_DEFAULT_ENTITIES = 150;
const FRESHNESS_TICK_MS = 1_000;

const EXAMPLE_ONTOLOGY_TABLE = `CREATE TABLE ontology (
    table_name         text   NOT NULL,
    columns            text[] NOT NULL,
    referenced_table   text   NOT NULL,
    referenced_columns text[] NOT NULL
);
CREATE DEFAULT INDEX ON ontology;

INSERT INTO ontology VALUES
  ('orders', ARRAY['customer_id'], 'customers', ARRAY['id']);`;

/**
 * Groups to open on: every group for a small environment, otherwise the
 * groups holding the ontology tables and the relations they connect.
 */
function defaultGroups(
  graph: OntologyGraph,
  ontologyTables: OntologyTableRow[],
  allGroups: string[],
): ReadonlySet<string> | null {
  if (graph.nodes.length <= MAX_DEFAULT_ENTITIES) return null;
  const groups = new Set(ontologyTables.map(groupName));
  const nodesById = new Map(graph.nodes.map((node) => [node.id, node]));
  for (const edge of graph.edges) {
    if (edge.kind !== "join") continue;
    for (const id of [edge.source, edge.target]) {
      const node = nodesById.get(id);
      if (node) groups.add(node.group);
    }
  }
  if (groups.size === 0 && allGroups.length > 0) groups.add(allGroups[0]);
  return groups;
}

/** Group names never contain a newline, so they join into a stable key. */
const groupsToKey = (groups: ReadonlySet<string> | null) =>
  groups === null ? null : [...groups].sort().join("\n");

function useGroupsFromKey(key: string | null): ReadonlySet<string> | null {
  return React.useMemo(
    () => (key === null ? null : new Set(key === "" ? [] : key.split("\n"))),
    [key],
  );
}

export const DataProductsLens = () => {
  // Undefined until the user picks groups, so the default can follow the
  // ontology as it loads.
  const [chosenGroups, setChosenGroups] = React.useState<
    ReadonlySet<string> | null | undefined
  >(undefined);
  // The columns subscribe is scoped to the drawn groups, and the default
  // groups depend on the graph, so the groups the hook last saw are kept in
  // state, by content.
  const [queryGroupsKey, setQueryGroupsKey] = React.useState<string | null>(
    null,
  );
  const queryGroups = useGroupsFromKey(queryGroupsKey);
  const ontology = useDataProductOntologyGraph(queryGroups);
  const groupsKey = groupsToKey(
    chosenGroups !== undefined
      ? chosenGroups
      : defaultGroups(
          ontology.graph,
          ontology.ontologyTables,
          ontology.allGroups,
        ),
  );
  if (groupsKey !== queryGroupsKey) setQueryGroupsKey(groupsKey);
  const groups = useGroupsFromKey(groupsKey);

  const vitalIds = React.useMemo(
    () =>
      ontology.graph.nodes
        .filter((node) => node.objectId && (!groups || groups.has(node.group)))
        .map((node) => node.objectId ?? ""),
    [ontology.graph.nodes, groups],
  );
  const { vitals, isLive } = useRelationVitals(vitalIds);
  const now = useNow(FRESHNESS_TICK_MS);
  const getFreshness = React.useCallback(
    (node: OntologyNode) =>
      freshnessOf(
        node.objectId ? vitals.get(node.objectId) : undefined,
        node.kind,
        now,
      ),
    [vitals, now],
  );

  const readCluster = ontology.readCluster;
  const previewCluster = React.useCallback(
    (node: OntologyNode) => node.clusterName ?? readCluster,
    [readCluster],
  );

  const renderOverview = React.useCallback(
    (context: OverviewContext) => (
      <DataProductsOverview
        context={context}
        ontologyTables={ontology.ontologyTables}
        readCluster={readCluster}
        rowsError={ontology.rowsError?.message}
      />
    ),
    [ontology.ontologyTables, readCluster, ontology.rowsError],
  );

  // Only the first load waits. Later, a new ontology table restarts the rows
  // SUBSCRIBE, and the graph should keep drawing what it has meanwhile.
  const [hasLoaded, setHasLoaded] = React.useState(false);
  if (!hasLoaded && !ontology.isLoading) setHasLoaded(true);

  if (ontology.isError) {
    return <ErrorBox message="An error occurred loading the ontology." />;
  }
  if (!hasLoaded) return <LoadingContainer />;

  return (
    <OntologyExplorer
      name="data-products-ontology"
      graph={ontology.graph}
      allGroups={ontology.allGroups}
      groups={groups}
      onGroupsChange={setChosenGroups}
      defaultShowEdgeLabels
      renderOverview={renderOverview}
      // Until the first snapshot every relation would read as unmaintained.
      getFreshness={isLive ? getFreshness : undefined}
      previewCluster={previewCluster}
      isLive={isLive}
      suggestComments
      isReady={hasLoaded && ontology.detailsReady}
    />
  );
};

const OverviewSection = ({
  title,
  children,
}: React.PropsWithChildren<{ title: string }>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack align="stretch" spacing={2}>
      <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
        {title}
      </Text>
      {children}
    </VStack>
  );
};

const DataProductsOverview = ({
  context,
  ontologyTables,
  readCluster,
  rowsError,
}: {
  context: OverviewContext;
  ontologyTables: OntologyTableRow[];
  readCluster: string | null;
  rowsError: string | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const joinCount = context.graph.edges.filter(
    (edge) => edge.kind === "join",
  ).length;
  const dataProductCount = context.graph.nodes.filter(
    (node) => node.isDataProduct,
  ).length;
  const undocumented = context.graph.nodes.filter(
    (node) => node.isDataProduct && !node.description,
  );

  return (
    <VStack align="stretch" spacing={6}>
      <VStack align="stretch" spacing={2}>
        <Text textStyle="heading-sm">Your data, as a live ontology</Text>
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          Entities are the tables, views and materialized views in this
          environment. Joins come from your ontology tables and lineage from
          each view&apos;s definition. Everything here is kept current with
          SUBSCRIBE, so new objects, new relationships and freshness appear as
          they change.
        </Text>
      </VStack>

      <HStack spacing={6}>
        <Figure value={joinCount} label="Joins" />
        <Figure value={dataProductCount} label="MCP data products" />
        <Figure
          value={context.graph.issues.length}
          label="Issues"
          color={
            context.graph.issues.length > 0 ? colors.accent.red : undefined
          }
        />
      </HStack>

      <OverviewSection title="Ontology tables">
        {ontologyTables.length === 0 ? (
          <VStack align="stretch" spacing={3}>
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              No ontology table yet. An ontology table lists the joins between
              your relations, so agents and people can write correct joins. Any
              relation with these four columns is read as one:
            </Text>
            <CodeBlock
              title="Ontology table"
              contents={EXAMPLE_ONTOLOGY_TABLE}
              lineNumbers={false}
            />
            <TextLink
              href={docUrls["/docs/transform-data/patterns/ontology/"]}
              isExternal
              textStyle="text-small"
            >
              Use an ontology table
            </TextLink>
          </VStack>
        ) : (
          <VStack align="stretch" spacing={1}>
            {ontologyTables.map((table) => (
              <HStack key={table.id} justify="space-between">
                <Text textStyle="text-small" fontFamily="mono">
                  {displayName(table)}
                </Text>
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  {table.indexClusterName
                    ? `indexed on ${table.indexClusterName}`
                    : "not indexed"}
                </Text>
              </HStack>
            ))}
            {!readCluster && (
              <Text textStyle="text-small" color={colors.accent.red}>
                No ontology table is indexed, so its rows cannot be kept live.
                Run CREATE DEFAULT INDEX ON the table.
              </Text>
            )}
            {rowsError && (
              <Text textStyle="text-small" color={colors.accent.red}>
                {rowsError}
              </Text>
            )}
          </VStack>
        )}
      </OverviewSection>

      {context.graph.issues.length > 0 && (
        <OverviewSection title="Issues">
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Rows an agent would trip over. Select one to see the relationship.
          </Text>
          {context.graph.issues.map((issue, i) => (
            <Box
              key={i}
              as="button"
              textAlign="left"
              onClick={() =>
                issue.edgeId &&
                context.select({ type: "edge", id: issue.edgeId })
              }
            >
              <IssueList issues={[issue]} />
            </Box>
          ))}
        </OverviewSection>
      )}

      {undocumented.length > 0 && (
        <OverviewSection title="Undocumented data products">
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Agents read comments to decide when to use a data product. These
            have none.
          </Text>
          <HStack flexWrap="wrap" spacing={2}>
            {undocumented.map((node) => (
              <TextLink
                key={node.id}
                as="button"
                textStyle="text-small"
                onClick={() => context.select({ type: "node", id: node.id })}
              >
                {node.label}
              </TextLink>
            ))}
          </HStack>
        </OverviewSection>
      )}

      <OverviewSection title="Freshness">
        <Legend
          items={[
            [colors.accent.green, "Under 2s behind"],
            [colors.accent.darkYellow, "Under 30s behind"],
            [colors.accent.red, "30s or more behind"],
            [colors.accent.blue, "Hydrating"],
          ]}
        />
      </OverviewSection>
    </VStack>
  );
};

const Legend = ({ items }: { items: [string, string][] }) => (
  <VStack align="stretch" spacing={1}>
    {items.map(([color, label]) => (
      <HStack key={label} spacing={2}>
        <Box width="8px" height="8px" borderRadius="full" background={color} />
        <Text textStyle="text-small">{label}</Text>
      </HStack>
    ))}
  </VStack>
);
