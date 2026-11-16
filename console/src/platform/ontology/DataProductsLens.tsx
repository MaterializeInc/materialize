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
import { useSearchParams } from "react-router-dom";

import {
  ONTOLOGY_TABLE_COLUMNS,
  OntologyTableRow,
} from "~/api/materialize/ontology/ontology";
import { CodeBlock } from "~/components/copyableComponents";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import { useNow } from "~/hooks/useNow";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import {
  CatalogRelation,
  displayName,
  groupName,
  parseQualifiedName,
  qualifiedName,
} from "./dataProductGraph";
import { buildDeclareSql, OntologyTableHome } from "./declareSql";
import { freshnessOf } from "./freshness";
import {
  INFERRED_JOIN_EDGE_KIND,
  JOIN_EDGE_KIND,
  OntologyEdge,
  OntologyGraph,
  OntologyNode,
} from "./model";
import {
  OntologyExplorer,
  OverviewContext,
  ToolbarControls,
} from "./OntologyExplorer";
import { DEFAULT_ONTOLOGY_TABLE } from "./ontologyTableChoice";
import { OntologyTablePicker } from "./OntologyTablePicker";
import { Figure, IssueList } from "./overviewComponents";
import { useDataProductOntologyGraph, useRelationVitals } from "./queries";

/**
 * Above this many relations, the explorer opens on the schemas the ontology
 * tables describe instead of everything, since a graph that large is neither
 * readable nor cheap to keep live.
 */
const MAX_DEFAULT_ENTITIES = 150;
const FRESHNESS_TICK_MS = 1_000;

const ONTOLOGY_PARAM = "ontology";
const DEFAULT_SCHEMA = "public";

function createOntologyTableSql(name: string) {
  return `CREATE TABLE ${name} (
${ONTOLOGY_TABLE_COLUMNS.map(([column, type]) => `    ${column.padEnd(18)} ${type} NOT NULL`).join(",\n")}
);
CREATE DEFAULT INDEX ON ${name};`;
}

/**
 * Where declared joins go for a typed name with no table behind it: the
 * named schema, or public, in the named database, or else `databaseName`.
 */
function homeFor(
  name: string,
  databaseName: string | null,
): OntologyTableHome | null {
  const parts = parseQualifiedName(name);
  if (!parts) return null;
  const [item, schema, database] = [...parts].reverse();
  return {
    databaseName: database ?? databaseName,
    schemaName: schema ?? DEFAULT_SCHEMA,
    name: item,
    exists: false,
  };
}

/**
 * Groups to open on: every group for a small environment, otherwise the
 * groups holding the ontology table and the relations joins connect.
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
    if (edge.kind !== JOIN_EDGE_KIND && edge.kind !== INFERRED_JOIN_EDGE_KIND) {
      continue;
    }
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
  const [searchParams, setSearchParams] = useSearchParams();
  const ontologyName =
    searchParams.get(ONTOLOGY_PARAM) ?? DEFAULT_ONTOLOGY_TABLE;
  const setOntologyName = React.useCallback(
    (name: string) =>
      setSearchParams(
        (params) => {
          const next = new URLSearchParams(params);
          if (name === DEFAULT_ONTOLOGY_TABLE) next.delete(ONTOLOGY_PARAM);
          else next.set(ONTOLOGY_PARAM, name);
          return next;
        },
        { replace: true },
      ),
    [setSearchParams],
  );
  const ontology = useDataProductOntologyGraph(queryGroups, ontologyName);
  const ontologyTable = ontology.ontologyTable;
  const groupsKey = groupsToKey(
    chosenGroups !== undefined
      ? chosenGroups
      : defaultGroups(
          ontology.graph,
          ontologyTable ? [ontologyTable] : [],
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

  const chosenRelation = ontology.chosenRelation;
  const declareSql = React.useCallback(
    (edge: OntologyEdge, source: OntologyNode, target: OntologyNode) => {
      // A relation by that name without the ontology columns takes no rows.
      if (chosenRelation && !ontologyTable) return null;
      const home: OntologyTableHome | null = ontologyTable
        ? { ...ontologyTable, exists: true }
        : homeFor(ontologyName, source.databaseName);
      return home ? buildDeclareSql(edge, source, target, home) : null;
    },
    [chosenRelation, ontologyTable, ontologyName],
  );

  const renderToolbar = React.useCallback(
    ({ showEdgeKind }: ToolbarControls) => (
      <OntologyTablePicker
        value={ontologyName}
        onChange={(name) => {
          setOntologyName(name);
          showEdgeKind(JOIN_EDGE_KIND);
        }}
        relations={ontology.relations}
        shapeIds={ontology.ontologyShapeIds}
      />
    ),
    [
      ontologyName,
      setOntologyName,
      ontology.relations,
      ontology.ontologyShapeIds,
    ],
  );

  const renderOverview = React.useCallback(
    (context: OverviewContext) => (
      <DataProductsOverview
        context={context}
        ontologyName={ontologyName}
        chosenRelation={chosenRelation}
        ontologyTable={ontologyTable}
        readCluster={readCluster}
        rowsError={ontology.rowsError?.message}
      />
    ),
    [
      ontologyName,
      chosenRelation,
      ontologyTable,
      readCluster,
      ontology.rowsError,
    ],
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
      // Inferred joins need nothing from the user, so they are the default
      // reading, and declared joins an overlay to check against them.
      defaultHiddenEdgeKinds={[JOIN_EDGE_KIND]}
      renderToolbar={renderToolbar}
      renderOverview={renderOverview}
      // Until the first snapshot every relation would read as unmaintained.
      getFreshness={isLive ? getFreshness : undefined}
      previewCluster={previewCluster}
      isLive={isLive}
      suggestComments
      declareSql={declareSql}
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

interface OntologyTableState {
  /** The name in the picker. */
  ontologyName: string;
  /** The relation the name resolves to, if any. */
  chosenRelation: CatalogRelation | null;
  /** The chosen relation, when it has the ontology table's columns. */
  ontologyTable: OntologyTableRow | null;
  readCluster: string | null;
  rowsError: string | undefined;
}

/** Where declared joins come from, and why there are none when there are none. */
const OntologyTableStatus = ({
  ontologyName,
  chosenRelation,
  ontologyTable,
  readCluster,
  rowsError,
}: OntologyTableState) => {
  const { colors } = useTheme<MaterializeTheme>();
  const docsLink = (
    <TextLink
      href={docUrls["/docs/transform-data/patterns/ontology/"]}
      isExternal
      textStyle="text-small"
    >
      Use an ontology table
    </TextLink>
  );
  if (!chosenRelation) {
    return (
      <VStack align="stretch" spacing={3}>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          There is no {ontologyName}, so every join here is inferred. An
          ontology table declares joins, so agents and people can rely on them.
          Pick another table above, select an inferred join to declare it, or
          create the table:
        </Text>
        <CodeBlock
          title="Ontology table"
          contents={createOntologyTableSql(ontologyName)}
          lineNumbers={false}
        />
        {docsLink}
      </VStack>
    );
  }
  if (!ontologyTable) {
    return (
      <VStack align="stretch" spacing={2}>
        <Text textStyle="text-small" color={colors.accent.red}>
          {displayName(chosenRelation)} is not an ontology table. It needs these
          columns:{" "}
          {ONTOLOGY_TABLE_COLUMNS.map(
            ([column, type]) => `${column} ${type}`,
          ).join(", ")}
          .
        </Text>
        {docsLink}
      </VStack>
    );
  }
  return (
    <VStack align="stretch" spacing={1}>
      <HStack justify="space-between">
        <Text textStyle="text-small" fontFamily="mono">
          {displayName(ontologyTable)}
        </Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {readCluster ? `indexed on ${readCluster}` : "not indexed"}
        </Text>
      </HStack>
      {!readCluster && (
        <Text textStyle="text-small" color={colors.accent.red}>
          The table is not indexed, so its rows cannot be kept live. Run CREATE
          DEFAULT INDEX ON {qualifiedName(ontologyTable)}.
        </Text>
      )}
      {rowsError && (
        <Text textStyle="text-small" color={colors.accent.red}>
          {rowsError}
        </Text>
      )}
    </VStack>
  );
};

const DataProductsOverview = ({
  context,
  ...ontologyTableState
}: OntologyTableState & { context: OverviewContext }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const countOf = (kind: OntologyEdge["kind"]) =>
    context.graph.edges.filter((edge) => edge.kind === kind).length;
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
          environment. Joins are declared in your ontology tables or inferred
          from the joins your views already make. Lineage comes from each
          view&apos;s definition. Everything here is kept current with
          SUBSCRIBE, so new objects, new relationships and freshness appear as
          they change.
        </Text>
      </VStack>

      <HStack spacing={6} rowGap={3} flexWrap="wrap">
        <Figure value={countOf(JOIN_EDGE_KIND)} label="Declared joins" />
        <Figure value={countOf(INFERRED_JOIN_EDGE_KIND)} label="Inferred" />
        <Figure value={dataProductCount} label="MCP data products" />
        <Figure
          value={context.graph.issues.length}
          label="Issues"
          color={
            context.graph.issues.length > 0 ? colors.accent.red : undefined
          }
        />
      </HStack>

      <OverviewSection title="Ontology table">
        <OntologyTableStatus {...ontologyTableState} />
      </OverviewSection>

      {context.graph.issues.length > 0 && (
        <OverviewSection title="Issues">
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Joins an agent would trip over, or that cost more than they need to.
            Select one to see the relationship.
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
