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
  Button,
  Divider,
  Flex,
  HStack,
  IconButton,
  Link,
  MenuItem,
  Text,
  TextProps,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { prettyStr } from "@materializeinc/sql-pretty";
import React, { useCallback, useState } from "react";
import { Link as RouterLink, useNavigate } from "react-router-dom";

import { useSqlLazy } from "~/api/materialize";
import { queryBuilder } from "~/api/materialize/db";
import {
  buildShowCreateStatement,
  type ShowCreateObjectType,
} from "~/api/materialize/showCreate";
import { Sink } from "~/api/materialize/sink/sinkList";
import { Source } from "~/api/materialize/source/sourceList";
import type { SupportedObjectType } from "~/api/materialize-v2/types";
import { MzDataType } from "~/api/materialize-v2/types";
import { objectIcon } from "~/components/objectIcons";
import OverflowMenu from "~/components/OverflowMenu";
import { ConnectorStatusPill } from "~/components/StatusPill";
import { CommentText } from "~/components-v2/CommentText";
import { useFlags } from "~/hooks/useFlags";
import {
  ClustersIcon,
  CommandIcon,
  EyeOpenIcon,
  MonitorIcon,
  WorkflowIcon,
} from "~/icons";
import { NULL_DATABASE_NAME } from "~/platform/constants";
import { useSinkList } from "~/platform/sinks/queries";
import { useSourcesList } from "~/platform/sources/queries";
import {
  absoluteClusterPath,
  dataflowPath,
  monitorPath,
  workflowPath,
} from "~/platform/v2/routeHelpers";
import { useCatalogObject } from "~/store/catalog";
import type { DepRow } from "~/store/catalogDependencies";
import { useRegionSlug } from "~/store/environments";
import type { MaterializeTheme } from "~/theme";

import type { CatalogSelection } from "./CatalogPanel";
import { type QueryResult, useShowResult } from "./store";

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

/** Columns shown before the list collapses behind "Show all". */
const COLUMN_PREVIEW_COUNT = 8;

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
  const showResult = useShowResult();
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
      showResult(queryResult);
    },
  });

  const showCreate = useCallback(
    (target: ShowCreateTarget) => {
      const qualifiedName = [
        target.databaseName,
        target.schemaName,
        target.name,
      ]
        .filter(Boolean)
        .join(".");
      runShowCreate(
        {
          objectType: target.objectType,
          object: {
            databaseName: target.databaseName,
            schemaName: target.schemaName,
            name: target.name,
          },
        },
        {
          onSuccess: (data) => {
            if (!data || data.length === 0) return;
            const rawSql = String(data[0].rows[0]?.[0] ?? "");
            let formattedSql: string;
            try {
              formattedSql = prettyStr(rawSql, SQL_PRETTY_LINE_WIDTH);
            } catch {
              formattedSql = rawSql;
            }
            showResult({
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
              kind: SHOW_CREATE_TYPE_TO_KIND[target.objectType],
              objectName: qualifiedName,
              clusterName: target.clusterName,
              clusterId: target.clusterId,
            });
          },
        },
      );
    },
    [runShowCreate, showResult],
  );

  const handleShowSql = useCallback(() => {
    if (!supportsShowCreate) return;
    showCreate({
      objectType: objectType as ShowCreateObjectType,
      databaseName: databaseName === NULL_DATABASE_NAME ? null : databaseName,
      schemaName,
      name: objectName,
      clusterId,
      clusterName,
    });
  }, [
    supportsShowCreate,
    showCreate,
    objectType,
    databaseName,
    schemaName,
    objectName,
    clusterId,
    clusterName,
  ]);

  const [expandedColumnsFor, setExpandedColumnsFor] = useState<string>();

  const { columns, description, indexes, owner, dependencies, referencedBy } =
    useCatalogObject(id);

  const visibleColumns =
    expandedColumnsFor === id
      ? columns
      : columns.slice(0, COLUMN_PREVIEW_COUNT);

  return (
    <VStack align="stretch" spacing="0" px="4" pb="4" minW="0">
      <Box py="3">
        <HStack spacing="2" align="flex-start" minW="0">
          <Tooltip
            label={TYPE_LABELS[objectType] ?? objectType.toUpperCase()}
            placement="top"
            hasArrow
          >
            <Box as="span" display="inline-flex" pt="1.5">
              {objectIcon(objectType)}
            </Box>
          </Tooltip>
          <Text fontWeight="bold" fontSize="lg" minW="0" wordBreak="break-word">
            {objectName}
          </Text>
        </HStack>
        {(clusterName || owner) && (
          <Flex
            mt="1"
            wrap="wrap"
            columnGap="1"
            fontSize="xs"
            color={colors.foreground.secondary}
          >
            {clusterId && clusterName && (
              <Tooltip label="Cluster" placement="top" hasArrow>
                <Link
                  as={RouterLink}
                  to={absoluteClusterPath(regionSlug, {
                    id: clusterId,
                    name: clusterName,
                  })}
                  display="inline-flex"
                  alignItems="center"
                  gap="1"
                >
                  <ClustersIcon width="10px" height="10px" />
                  {clusterName}
                </Link>
              </Tooltip>
            )}
            {clusterName && owner && <Text as="span">·</Text>}
            {owner && (
              <Tooltip label="Owner" placement="top" hasArrow>
                <Text as="span">{owner}</Text>
              </Tooltip>
            )}
          </Flex>
        )}
        <Flex mt="1" wrap="wrap" align="center" gap="1">
          {supportsShowCreate && (
            <ActionButton
              label={
                isSystemObject
                  ? "Cannot show create for system objects"
                  : "Show SQL"
              }
              icon={<CommandIcon />}
              onClick={handleShowSql}
              isDisabled={isSystemObject}
            />
          )}
          {isConnector && (
            <ActionButton
              label="Monitor"
              icon={<MonitorIcon />}
              onClick={() => navigate(monitorPath(regionSlug, id))}
            />
          )}
          {hasWorkflow && (
            <ActionButton
              label={
                isSystemSchema
                  ? "Workflow is not available for system objects"
                  : "Workflow"
              }
              icon={<WorkflowIcon />}
              onClick={() => navigate(workflowPath(regionSlug, id))}
              isDisabled={isSystemSchema}
            />
          )}
          {hasDataflow && (
            <ActionButton
              label="Visualize dataflow"
              icon={<EyeOpenIcon />}
              onClick={() => navigate(dataflowPath(regionSlug, id))}
            />
          )}
          {isConnector && (
            <Box ml="auto">
              <React.Suspense fallback={null}>
                <ConnectorStatus
                  objectType={objectType as "source" | "sink"}
                  databaseName={databaseName}
                  schemaName={schemaName}
                  objectName={objectName}
                />
              </React.Suspense>
            </Box>
          )}
        </Flex>
        {description && (
          <CommentText fontSize="sm" color={colors.foreground.secondary} mt="1">
            {description}
          </CommentText>
        )}
      </Box>

      {showColumns && columns.length > 0 && (
        <DetailSection title="Columns">
          {visibleColumns.map((col) => (
            <Box key={col.name} py="0.5">
              <HStack spacing="2" minW="0">
                <TruncatedText fontSize="sm" flex="1">
                  {col.name}
                </TruncatedText>
                <TruncatedText
                  fontSize="sm"
                  color={colors.foreground.secondary}
                  maxW="50%"
                  title={col.nullable ? `${col.type} (nullable)` : col.type}
                >
                  {col.nullable ? `${col.type}?` : col.type}
                </TruncatedText>
              </HStack>
              {col.columnComment && (
                <CommentText fontSize="xs" color={colors.foreground.secondary}>
                  {col.columnComment}
                </CommentText>
              )}
            </Box>
          ))}
          {columns.length > COLUMN_PREVIEW_COUNT && (
            <Button
              variant="link"
              size="xs"
              alignSelf="flex-start"
              mt="1"
              color={colors.accent.brightPurple}
              onClick={() =>
                setExpandedColumnsFor(
                  expandedColumnsFor === id ? undefined : id,
                )
              }
            >
              {expandedColumnsFor === id
                ? "Show fewer"
                : `Show all ${columns.length}`}
            </Button>
          )}
        </DetailSection>
      )}

      {showIndexes && indexes.length > 0 && (
        <DetailSection title="Indexes">
          {indexes.map((idx) => {
            const isSystemIndex = SYSTEM_SCHEMAS.has(idx.schemaName);
            return (
              <HStack key={idx.id} spacing="1" minW="0" py="0.5">
                <Box flex="1" minW="0">
                  <TruncatedText fontSize="sm" fontWeight="medium">
                    {idx.name}
                  </TruncatedText>
                  <TruncatedText
                    fontSize="xs"
                    color={colors.foreground.secondary}
                  >
                    ({idx.indexedColumns})
                  </TruncatedText>
                </Box>
                <OverflowMenu
                  placement="bottom-end"
                  items={[
                    {
                      visible: true,
                      render: () => (
                        <MenuItem
                          onClick={() =>
                            showCreate({
                              objectType: "index",
                              databaseName: idx.databaseName,
                              schemaName: idx.schemaName,
                              name: idx.name,
                              clusterId: idx.clusterId ?? undefined,
                              clusterName: idx.clusterName ?? undefined,
                            })
                          }
                        >
                          Show SQL
                        </MenuItem>
                      ),
                    },
                    {
                      visible: true,
                      render: () => (
                        <MenuItem
                          isDisabled={isSystemIndex}
                          onClick={() =>
                            navigate(workflowPath(regionSlug, idx.id))
                          }
                        >
                          Workflow
                        </MenuItem>
                      ),
                    },
                    {
                      visible: flags["visualization-features"],
                      render: () => (
                        <MenuItem
                          onClick={() =>
                            navigate(dataflowPath(regionSlug, idx.id))
                          }
                        >
                          Visualize dataflow
                        </MenuItem>
                      ),
                    },
                    {
                      visible: Boolean(idx.clusterId && idx.clusterName),
                      render: () =>
                        idx.clusterId &&
                        idx.clusterName && (
                          <MenuItem
                            as={RouterLink}
                            to={absoluteClusterPath(regionSlug, {
                              id: idx.clusterId,
                              name: idx.clusterName,
                            })}
                          >
                            Open cluster {idx.clusterName}
                          </MenuItem>
                        ),
                    },
                  ]}
                />
              </HStack>
            );
          })}
        </DetailSection>
      )}

      <DependenciesSection
        deps={dependencies}
        direction="depends_on"
        onNavigate={onNavigate}
      />
      <DependenciesSection
        deps={referencedBy}
        direction="referenced_by"
        onNavigate={onNavigate}
      />
    </VStack>
  );
};

/** Object that a SHOW CREATE result is rendered for. */
interface ShowCreateTarget {
  objectType: ShowCreateObjectType;
  databaseName: string | null;
  schemaName: string;
  name: string;
  clusterId?: string;
  clusterName?: string;
}

/** Icon-only header action. The label doubles as tooltip and accessible name. */
const ActionButton = ({
  label,
  icon,
  onClick,
  isDisabled,
}: {
  label: string;
  icon: React.ReactElement;
  onClick: () => void;
  isDisabled?: boolean;
}) => (
  <Tooltip label={label} placement="top" hasArrow>
    <IconButton
      aria-label={label}
      icon={icon}
      size="xs"
      variant="ghost"
      onClick={onClick}
      isDisabled={isDisabled}
    />
  </Tooltip>
);

/** Single-line text that ellipsizes and shows its full value on hover. */
const TruncatedText = ({
  children,
  title,
  ...props
}: TextProps & { children: string | string[] }) => {
  const text = Array.isArray(children) ? children.join("") : children;
  return (
    <Text
      minW="0"
      noOfLines={1}
      wordBreak="break-all"
      title={title ?? text}
      {...props}
    >
      {text}
    </Text>
  );
};

/** Divider plus small heading above one section of the detail view. */
const DetailSection = ({
  title,
  color,
  children,
}: React.PropsWithChildren<{ title: string; color?: string }>) => (
  <>
    <Divider />
    <Box py="3">
      <Text
        fontSize="xs"
        fontWeight="bold"
        textTransform="uppercase"
        letterSpacing="wide"
        mb="2"
        color={color}
      >
        {title}
      </Text>
      <VStack align="stretch" spacing="0">
        {children}
      </VStack>
    </Box>
  </>
);

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

  const deps = rawDeps.filter(
    (dep) => !(direction === "referenced_by" && dep.objectType === "index"),
  );

  if (deps.length === 0) return null;

  return (
    <DetailSection
      title={direction === "depends_on" ? "Depends on" : "Referenced by"}
      color={
        direction === "depends_on"
          ? colors.accent.darkYellow
          : colors.accent.orange
      }
    >
      {deps.map((dep) => (
        <HStack key={dep.id} spacing="2" minW="0" py="0.5">
          <Tooltip label={dep.objectType} placement="top" hasArrow>
            <Box
              as="span"
              display="inline-flex"
              flexShrink={0}
              color={colors.foreground.secondary}
            >
              {objectIcon(dep.objectType)}
            </Box>
          </Tooltip>
          <Link
            fontSize="sm"
            minW="0"
            noOfLines={1}
            wordBreak="break-all"
            title={dep.name}
            color={colors.accent.brightPurple}
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
        </HStack>
      ))}
    </DetailSection>
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
