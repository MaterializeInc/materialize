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
  Divider,
  HStack,
  Link,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { prettyStr } from "@materializeinc/sql-pretty";
import { useSetAtom } from "jotai";
import React, { useCallback } from "react";
import { Link as RouterLink, useNavigate } from "react-router-dom";

import { useSqlLazy } from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import {
  buildShowCreateStatement,
  type ShowCreateObjectType,
} from "~/api/materialize/showCreate";
import { Sink } from "~/api/materialize/sink/sinkList";
import { Source } from "~/api/materialize/source/sourceList";
import type { SupportedObjectType } from "~/api/materialize/types";
import { MzDataType } from "~/api/materialize/types";
import { CommentText } from "~/components/CommentText";
import { objectIcon } from "~/components/objectIcons";
import { ConnectorStatusPill } from "~/components/StatusPill";
import { useFlags } from "~/hooks/useFlags";
import { ClustersIcon } from "~/icons";
import { NULL_DATABASE_NAME } from "~/platform/constants";
import {
  absoluteClusterPath,
  dataflowPath,
  monitorPath,
  workflowPath,
} from "~/platform/routeHelpers";
import { useSinkList } from "~/platform/sinks/queries";
import { useSourcesList } from "~/platform/sources/queries";
import { useCatalogObject } from "~/store/catalog";
import type { DepRow } from "~/store/catalogDependencies";
import { useRegionSlug } from "~/store/environments";
import type { MaterializeTheme } from "~/theme";

import type { CatalogSelection } from "./CatalogPanel";
import {
  type QueryResult,
  resultsPanelOpenAtom,
  worksheetResultAtom,
} from "./store";

/**
 * Props for the catalog object detail panel, shown when a user drills into
 * a single object from the catalog tree.
 */
export interface CatalogDetailViewProps {
  /** System ID of the catalog object (e.g. "u123"). */
  id: string;
  /** Database that contains this object. */
  databaseName: string;
  /** Schema that contains this object. */
  schemaName: string;
  /** Unqualified name of the object. */
  objectName: string;
  /** The kind of catalog object (table, view, source, etc.). */
  objectType: SupportedObjectType;
  /** Cluster ID, if the object is cluster-bound (e.g. materialized views, indexes). */
  clusterId?: string;
  /** Human-readable cluster name, displayed as a navigable badge. */
  clusterName?: string;
  /** Called when the user clicks a dependency link to navigate to another object. */
  onNavigate: (obj: CatalogSelection) => void;
}

/** Object types that have inspectable columns. */
const COLUMN_TYPES = new Set(["table", "view", "materialized-view", "source"]);

/** Object types that can have indexes. */
const INDEX_TYPES = new Set(["materialized-view", "table", "view", "source"]);

/** Line width for SQL pretty-printing in SHOW CREATE results. */
const SQL_PRETTY_LINE_WIDTH = 100;

/** Maps internal object type keys to user-facing uppercase labels for the detail header badge. */
const TYPE_LABELS: Record<string, string> = {
  table: "TABLE",
  view: "VIEW",
  "materialized-view": "MATERIALIZED VIEW",
  source: "SOURCE",
  sink: "SINK",
  connection: "CONNECTION",
  secret: "SECRET",
};

/** Object types that have a connector overview page (statistics, errors). */
const CONNECTOR_TYPES = new Set(["source", "sink"]);

/** Object types that have a workflow graph (dependency DAG). */
const WORKFLOW_TYPES = new Set([
  "source",
  "sink",
  "table",
  "materialized-view",
  "index",
]);

/** Object types that have a dataflow visualizer (internal execution graph). */
const DATAFLOW_TYPES = new Set(["materialized-view", "index"]);

const SYSTEM_SCHEMAS = new Set([
  "mz_catalog",
  "mz_internal",
  "mz_unsafe",
  "pg_catalog",
  "information_schema",
]);

/** Object types that support SHOW CREATE, mapped to their parser kind. */
const SHOW_CREATE_TYPE_TO_KIND: Record<string, string> = {
  table: "show_create_table",
  view: "show_create_view",
  "materialized-view": "show_create_materialized_view",
  source: "show_create_source",
  sink: "show_create_sink",
  connection: "show_create_connection",
  index: "show_create_index",
};

const SHOW_CREATE_TYPES = new Set<string>(
  Object.keys(SHOW_CREATE_TYPE_TO_KIND),
);

/**
 * Full detail view for a single object. Replaces the tree when the user
 * clicks "View full detail →". Shows columns, indexes, dependencies, and
 * referenced-by sections with dividers between them.
 */
export const CatalogDetailView = (props: CatalogDetailViewProps) => (
  <CatalogDetailViewContent {...props} />
);

/** Inner content of the catalog detail view. Fetches columns, indexes, and wires up SHOW CREATE. */
const CatalogDetailViewContent = ({
  id,
  databaseName,
  schemaName,
  objectName,
  objectType,
  clusterId,
  clusterName,
  onNavigate,
}: CatalogDetailViewProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();
  const navigate = useNavigate();
  const setResult = useSetAtom(worksheetResultAtom);
  const setResultsPanelOpen = useSetAtom(resultsPanelOpenAtom);
  const flags = useFlags();
  const showColumns = COLUMN_TYPES.has(objectType);
  const showIndexes = INDEX_TYPES.has(objectType);
  const isConnector = CONNECTOR_TYPES.has(objectType);
  const hasWorkflow = WORKFLOW_TYPES.has(objectType);
  const hasDataflow =
    DATAFLOW_TYPES.has(objectType) && flags["visualization-features"];
  const supportsShowCreate = SHOW_CREATE_TYPES.has(objectType);
  const isSystemSchema = SYSTEM_SCHEMAS.has(schemaName);
  const isSystemObject =
    isSystemSchema && (objectType === "table" || objectType === "source");

  const { runSql: runShowCreate } = useSqlLazy<{
    objectType: ShowCreateObjectType;
    object: { databaseName: string | null; schemaName: string; name: string };
  }>({
    queryBuilder: (vars) => {
      const compiled = buildShowCreateStatement(vars).compile(queryBuilder);
      return compiled.sql;
    },
    onError: (error) => {
      console.error("SHOW CREATE failed:", error);
      const queryResult: QueryResult = {
        columns: [
          {
            name: "error",
            type_oid: MzDataType.text,
            type_len: -1,
            type_mod: -1,
          },
        ],
        rows: [[error ?? "Unknown error"]],
        commandComplete: "ERROR",
        durationMs: 0,
      };
      setResult(queryResult);
      setResultsPanelOpen(true);
    },
  });

  const handleShowSql = useCallback(() => {
    if (!supportsShowCreate) return;
    const dbName = databaseName === NULL_DATABASE_NAME ? null : databaseName;
    const qualifiedName = [dbName, schemaName, objectName]
      .filter(Boolean)
      .join(".");
    const kind = SHOW_CREATE_TYPE_TO_KIND[objectType];
    runShowCreate(
      {
        objectType: objectType as ShowCreateObjectType,
        object: { databaseName: dbName, schemaName, name: objectName },
      },
      {
        onSuccess: (data) => {
          if (!data || data.length === 0) return;
          const result = data[0];
          const rawSql = String(result.rows[0]?.[0] ?? "");
          let formattedSql: string;
          try {
            formattedSql = prettyStr(rawSql, SQL_PRETTY_LINE_WIDTH);
          } catch {
            formattedSql = rawSql;
          }
          const queryResult: QueryResult = {
            columns: [
              {
                name: "sql",
                type_oid: MzDataType.text,
                type_len: -1,
                type_mod: -1,
              },
            ],
            rows: [[formattedSql]],
            commandComplete: "SHOW CREATE",
            durationMs: 0,
            displayMode: "sql",
            kind,
            objectName: qualifiedName,
          };
          setResult(queryResult);
          setResultsPanelOpen(true);
        },
      },
    );
  }, [
    supportsShowCreate,
    runShowCreate,
    objectType,
    databaseName,
    schemaName,
    objectName,
    setResult,
    setResultsPanelOpen,
  ]);

  const handleOpenMonitor = useCallback(() => {
    navigate(monitorPath(regionSlug, id));
  }, [navigate, regionSlug, id]);

  const handleOpenWorkflow = useCallback(() => {
    navigate(workflowPath(regionSlug, id));
  }, [navigate, regionSlug, id]);

  const handleOpenDataflow = useCallback(() => {
    navigate(dataflowPath(regionSlug, id));
  }, [navigate, regionSlug, id]);

  const { columns, description, indexes, owner, dependencies, referencedBy } =
    useCatalogObject(id);

  return (
    <VStack align="stretch" spacing="0" px="4" pb="4">
      {/* Header */}
      <Box py="3">
        <HStack spacing="2">
          <Tooltip
            label={TYPE_LABELS[objectType] ?? objectType.toUpperCase()}
            placement="top"
            hasArrow
          >
            <Box as="span" display="inline-flex">
              {objectIcon(objectType)}
            </Box>
          </Tooltip>
          <Text fontWeight="bold" fontSize="lg">
            {objectName}
          </Text>
          {isConnector && (
            <React.Suspense fallback={null}>
              <ConnectorStatus
                objectType={objectType as "source" | "sink"}
                databaseName={databaseName}
                schemaName={schemaName}
                objectName={objectName}
              />
            </React.Suspense>
          )}
        </HStack>
        {(clusterName ||
          supportsShowCreate ||
          isConnector ||
          hasWorkflow ||
          hasDataflow) && (
          <HStack mt="1" spacing="2">
            {clusterId && clusterName && (
              <Tooltip label="Cluster" placement="top" hasArrow>
                <Badge
                  as={RouterLink}
                  to={absoluteClusterPath(regionSlug, {
                    id: clusterId,
                    name: clusterName,
                  })}
                  fontSize="xx-small"
                  colorScheme="gray"
                  variant="subtle"
                  cursor="pointer"
                  _hover={{ textDecoration: "none", opacity: 0.8 }}
                >
                  <ClustersIcon width="10px" height="10px" />
                  &nbsp;{clusterName}
                </Badge>
              </Tooltip>
            )}
            {supportsShowCreate && (
              <Tooltip
                label="Cannot show create for system objects"
                placement="top"
                hasArrow
                isDisabled={!isSystemObject}
              >
                <Button
                  size="xs"
                  variant="outline"
                  onClick={handleShowSql}
                  isDisabled={isSystemObject}
                >
                  SQL
                </Button>
              </Tooltip>
            )}
            {isConnector && (
              <Button size="xs" variant="outline" onClick={handleOpenMonitor}>
                Monitor
              </Button>
            )}
            {hasWorkflow && (
              <Tooltip
                label="Not available for system objects"
                placement="top"
                hasArrow
                isDisabled={!isSystemSchema}
              >
                <Button
                  size="xs"
                  variant="outline"
                  onClick={handleOpenWorkflow}
                  isDisabled={isSystemSchema}
                >
                  Workflow
                </Button>
              </Tooltip>
            )}
            {hasDataflow && (
              <Button size="xs" variant="outline" onClick={handleOpenDataflow}>
                Visualize
              </Button>
            )}
          </HStack>
        )}
        {description && (
          <CommentText fontSize="sm" color={colors.foreground.secondary} mt="1">
            {description}
          </CommentText>
        )}
        {owner && (
          <Text fontSize="xs" color={colors.foreground.secondary} mt="1">
            Owner: {owner}
          </Text>
        )}
      </Box>

      {/* Columns */}
      {showColumns && columns.length > 0 && (
        <>
          <Divider />
          <Box py="3">
            <Text
              fontSize="xs"
              fontWeight="bold"
              textTransform="uppercase"
              letterSpacing="wide"
              mb="2"
            >
              Columns
            </Text>
            <VStack align="stretch" spacing="0">
              {columns.map((col) => (
                <Box
                  key={col.name}
                  py="1.5"
                  borderBottomWidth="1px"
                  borderColor={colors.border.secondary}
                >
                  <HStack justifyContent="space-between">
                    <Text fontSize="sm">{col.name}</Text>
                    <Text fontSize="sm" color={colors.foreground.secondary}>
                      {col.type}
                    </Text>
                  </HStack>
                  {!col.nullable && (
                    <Badge
                      fontSize="xx-small"
                      colorScheme="gray"
                      variant="subtle"
                      mt="0.5"
                    >
                      NOT NULL
                    </Badge>
                  )}
                  {col.columnComment && (
                    <CommentText
                      fontSize="xs"
                      color={colors.foreground.secondary}
                      mt="0.5"
                    >
                      {col.columnComment}
                    </CommentText>
                  )}
                </Box>
              ))}
            </VStack>
          </Box>
        </>
      )}

      {/* Indexes */}
      {showIndexes && indexes.length > 0 && (
        <>
          <Divider />
          <Box py="3">
            <Text
              fontSize="xs"
              fontWeight="bold"
              textTransform="uppercase"
              letterSpacing="wide"
              mb="2"
            >
              Indexes
            </Text>
            <VStack align="stretch" spacing="1">
              {indexes.map((idx) => (
                <HStack key={idx.id} justifyContent="space-between">
                  <Box>
                    <Text fontSize="sm" fontWeight="medium">
                      {idx.name}
                    </Text>
                    <Text fontSize="xs" color={colors.foreground.secondary}>
                      ({idx.indexedColumns})
                    </Text>
                  </Box>
                  <VStack spacing="1" align="flex-end">
                    <HStack spacing="1">
                      <Button
                        size="xs"
                        variant="outline"
                        onClick={() => {
                          const idxQualifiedName = [
                            idx.databaseName,
                            idx.schemaName,
                            idx.name,
                          ]
                            .filter(Boolean)
                            .join(".");
                          runShowCreate(
                            {
                              objectType: "index",
                              object: {
                                databaseName: idx.databaseName,
                                schemaName: idx.schemaName,
                                name: idx.name,
                              },
                            },
                            {
                              onSuccess: (data) => {
                                if (!data || data.length === 0) return;
                                const res = data[0];
                                const rawSql = String(res.rows[0]?.[0] ?? "");
                                let formattedSql: string;
                                try {
                                  formattedSql = prettyStr(
                                    rawSql,
                                    SQL_PRETTY_LINE_WIDTH,
                                  );
                                } catch {
                                  formattedSql = rawSql;
                                }
                                setResult({
                                  columns: [
                                    {
                                      name: "sql",
                                      type_oid: MzDataType.text,
                                      type_len: -1,
                                      type_mod: -1,
                                    },
                                  ],
                                  rows: [[formattedSql]],
                                  commandComplete: "SHOW CREATE",
                                  durationMs: 0,
                                  displayMode: "sql",
                                  kind: "show_create_index",
                                  objectName: idxQualifiedName,
                                });
                                setResultsPanelOpen(true);
                              },
                            },
                          );
                        }}
                      >
                        SQL
                      </Button>
                      {idx.clusterId && idx.clusterName && (
                        <Tooltip label="Cluster" placement="top" hasArrow>
                          <Badge
                            as={RouterLink}
                            to={absoluteClusterPath(regionSlug, {
                              id: idx.clusterId,
                              name: idx.clusterName,
                            })}
                            fontSize="xx-small"
                            colorScheme="gray"
                            variant="subtle"
                            cursor="pointer"
                            _hover={{ textDecoration: "none", opacity: 0.8 }}
                          >
                            <ClustersIcon width="10px" height="10px" />
                            &nbsp;{idx.clusterName}
                          </Badge>
                        </Tooltip>
                      )}
                    </HStack>
                    <HStack spacing="1">
                      <Tooltip
                        label="Not available for system objects"
                        placement="top"
                        hasArrow
                        isDisabled={!SYSTEM_SCHEMAS.has(idx.schemaName)}
                      >
                        <Button
                          size="xs"
                          variant="outline"
                          isDisabled={SYSTEM_SCHEMAS.has(idx.schemaName)}
                          onClick={() =>
                            navigate(workflowPath(regionSlug, idx.id))
                          }
                        >
                          Workflow
                        </Button>
                      </Tooltip>
                      {flags["visualization-features"] && (
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() =>
                            navigate(dataflowPath(regionSlug, idx.id))
                          }
                        >
                          Visualize
                        </Button>
                      )}
                    </HStack>
                  </VStack>
                </HStack>
              ))}
            </VStack>
          </Box>
        </>
      )}

      {/* Dependencies */}
      <DependenciesSection
        deps={dependencies}
        direction="depends_on"
        onNavigate={onNavigate}
      />

      {/* Referenced By */}
      <DependenciesSection
        deps={referencedBy}
        direction="referenced_by"
        onNavigate={onNavigate}
      />
    </VStack>
  );
};

/**
 * Displays upstream dependencies ("depends on") or downstream dependents
 * ("referenced by") for a catalog object. Indexes are filtered from
 * "referenced by" since they have a dedicated section.
 */
const DependenciesSection = ({
  deps: rawDeps,
  direction,
  onNavigate,
}: {
  deps: DepRow[];
  direction: "depends_on" | "referenced_by";
  onNavigate: (obj: CatalogSelection) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  // Filter out indexes from REFERENCED BY — they have their own section
  const deps = rawDeps.filter(
    (dep) => !(direction === "referenced_by" && dep.objectType === "index"),
  );

  if (deps.length === 0) return null;

  const title = direction === "depends_on" ? "DEPENDS ON" : "REFERENCED BY";
  const headerColor =
    direction === "depends_on"
      ? colors.accent.darkYellow
      : colors.accent.orange;

  return (
    <>
      <Divider />
      <Box py="3">
        <Text
          fontSize="xs"
          fontWeight="bold"
          textTransform="uppercase"
          letterSpacing="wide"
          mb="2"
          color={headerColor}
        >
          {title}
        </Text>
        <VStack align="stretch" spacing="1">
          {deps.map((dep) => (
            <HStack key={dep.id} justifyContent="space-between">
              <Link
                fontSize="sm"
                color={colors.accent.purple}
                onClick={() =>
                  onNavigate({
                    id: dep.id,
                    databaseName: dep.databaseName ?? NULL_DATABASE_NAME,
                    schemaName: dep.schemaName ?? "",
                    objectName: dep.name,
                    objectType: dep.objectType as SupportedObjectType,
                  })
                }
                textDecoration="underline"
                _hover={{ textDecoration: "none" }}
              >
                {dep.name}
              </Link>
              <Text fontSize="xs" color={colors.foreground.secondary}>
                {dep.objectType}
              </Text>
            </HStack>
          ))}
        </VStack>
      </Box>
    </>
  );
};

/** Fetches and displays the connector status pill for a source or sink. */
const ConnectorStatus = ({
  objectType,
  databaseName,
  schemaName,
  objectName,
}: {
  objectType: "source" | "sink";
  databaseName: string;
  schemaName: string;
  objectName: string;
}) => {
  if (objectType === "source") {
    return (
      <SourceStatus
        databaseName={databaseName}
        schemaName={schemaName}
        objectName={objectName}
      />
    );
  }
  return (
    <SinkStatus
      databaseName={databaseName}
      schemaName={schemaName}
      objectName={objectName}
    />
  );
};

const SourceStatus = ({
  databaseName,
  schemaName,
  objectName,
}: {
  databaseName: string;
  schemaName: string;
  objectName: string;
}) => {
  const { data } = useSourcesList({});
  const source = data.rows.find(
    (s: Source) =>
      s.databaseName === databaseName &&
      s.schemaName === schemaName &&
      s.name === objectName,
  );
  if (!source?.status) return null;
  return <ConnectorStatusPill connector={source} />;
};

const SinkStatus = ({
  databaseName,
  schemaName,
  objectName,
}: {
  databaseName: string;
  schemaName: string;
  objectName: string;
}) => {
  const { data } = useSinkList();
  const sink = data.rows.find(
    (s: Sink) =>
      s.databaseName === databaseName &&
      s.schemaName === schemaName &&
      s.name === objectName,
  );
  if (!sink?.status) return null;
  return <ConnectorStatusPill connector={sink} />;
};
