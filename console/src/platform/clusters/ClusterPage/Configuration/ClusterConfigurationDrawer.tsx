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
  Grid,
  HStack,
  Spinner,
  Text,
  Tooltip,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useQuery } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import React from "react";

import { useSegment } from "~/analytics/segment";
import { isSystemCluster } from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import { ConnectMethodCard } from "~/components/connect/connectComponents";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import DeleteObjectModal from "~/components/DeleteObjectModal";
import OverflowMenu, { OVERFLOW_BUTTON_WIDTH } from "~/components/OverflowMenu";
import SideDrawer from "~/components/SideDrawer";
import StatusPill from "~/components/StatusPill";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { ActivityIcon, ClustersIcon, InfoIcon } from "~/icons";
import { AlterClusterModal } from "~/platform/clusters/AlterClusterModal";
import { AUTO_SCALING_DOCS } from "~/platform/clusters/constants";
import NewReplicaModal from "~/platform/clusters/NewReplicaModal";
import {
  availableClusterSizesQuery,
  useClusterConfiguration,
  useMaxReplicasPerCluster,
  useOwners,
} from "~/platform/clusters/queries";
import { MaterializeTheme } from "~/theme";
import { notNullOrUndefined, pluralize } from "~/util";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import {
  AutoScaling,
  ConfigurationSection,
  formatLinger,
  parseAutoScaling,
  replicaStatus,
  resizeNote,
} from "./clusterConfigurationModel";

type Configuration = ReturnType<typeof useClusterConfiguration>["data"];

const formatCreatedAt = (date: Date | undefined) =>
  date ? formatDate(date, FRIENDLY_DATETIME_FORMAT_NO_SECONDS) : "-";

interface DetailProps {
  label: string;
  tooltip?: string;
}

const Detail = ({
  label,
  tooltip,
  children,
}: React.PropsWithChildren<DetailProps>) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing={1} alignItems="flex-start" minWidth={0}>
      <HStack spacing={1}>
        <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
          {label}
        </Text>
        {tooltip && (
          <Tooltip label={tooltip} lineHeight={1.2}>
            <InfoIcon color={colors.foreground.secondary} />
          </Tooltip>
        )}
      </HStack>
      <Text textStyle="text-base" overflowWrap="anywhere">
        {children}
      </Text>
    </VStack>
  );
};

interface PanelHeaderProps {
  title: React.ReactNode;
  action?: React.ReactNode;
}

const PanelHeader = ({ title, action }: PanelHeaderProps) => (
  <Flex justifyContent="space-between" alignItems="center" gap={3}>
    <Text textStyle="heading-xs">{title}</Text>
    {action}
  </Flex>
);

interface EditClusterButtonProps {
  cluster: Cluster;
  clusterSizes: string[] | null | undefined;
  maxReplicas: number | null | undefined;
  focusAutoScaling?: boolean;
}

const EditClusterButton = ({
  cluster,
  clusterSizes,
  maxReplicas,
  focusAutoScaling,
}: EditClusterButtonProps) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  return (
    <>
      <Button
        size="sm"
        variant="secondary"
        onClick={onOpen}
        isDisabled={!clusterSizes}
      >
        Edit
      </Button>
      {isOpen && clusterSizes && (
        <AlterClusterModal
          cluster={cluster}
          clusterSizes={clusterSizes}
          isOpen
          maxReplicas={maxReplicas ?? undefined}
          onClose={onClose}
          focusAutoScaling={focusAutoScaling}
        />
      )}
    </>
  );
};

interface DropClusterCardProps {
  cluster: Cluster;
  onDropped: () => void;
}

const DropClusterCard = ({ cluster, onDropped }: DropClusterCardProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onOpen, onClose } = useDisclosure();
  return (
    <Box
      borderWidth="1px"
      borderColor={colors.border.error}
      borderRadius="lg"
      overflow="hidden"
    >
      <VStack spacing={1} alignItems="stretch" px={5} py={4}>
        <Text textStyle="heading-xs">Drop Cluster</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Permanently drop this cluster and all its replicas, along with every
          index, materialized view, source, and sink running on it. This cannot
          be undone.
        </Text>
      </VStack>
      <Flex
        justifyContent="flex-end"
        px={5}
        py={3}
        background={colors.background.error}
      >
        <Button size="sm" colorScheme="red" onClick={onOpen}>
          Drop cluster
        </Button>
      </Flex>
      {isOpen && (
        <DeleteObjectModal
          isOpen
          onClose={onClose}
          onSuccess={onDropped}
          dbObject={cluster}
          objectType="CLUSTER"
        />
      )}
    </Box>
  );
};

interface OverviewPanelProps {
  cluster: Cluster;
  configuration: Configuration;
  ownerName: string | undefined;
  isClusterOwner: boolean;
  canEdit: boolean;
  edit: Omit<EditClusterButtonProps, "cluster">;
  onDropped: () => void;
}

const OverviewPanel = ({
  cluster,
  configuration,
  ownerName,
  isClusterOwner,
  canEdit,
  edit,
  onDropped,
}: OverviewPanelProps) => {
  const isUserCluster = !isSystemCluster(cluster.id);
  const createdAt = configuration?.creationTimes.find(
    ({ id, objectType }) => objectType === "cluster" && id === cluster.id,
  )?.occurredAt;
  const resize =
    configuration?.resizeTarget &&
    resizeNote(cluster, configuration.resizeTarget);
  return (
    <VStack spacing={6} alignItems="stretch">
      <PanelHeader
        title="Overview"
        action={canEdit && <EditClusterButton cluster={cluster} {...edit} />}
      />
      {resize && <Alert variant="info" message={resize} />}
      <Grid templateColumns="repeat(3, minmax(0, 1fr))" gap={5}>
        <Detail label="Name">{cluster.name}</Detail>
        <Detail label="Size">{cluster.size ?? "-"}</Detail>
        <Detail
          label="Management"
          tooltip="A managed cluster's replicas follow its size and replication factor. An unmanaged cluster's replicas are created and dropped one at a time."
        >
          {cluster.managed ? "Managed" : "Unmanaged"}
        </Detail>
        <Detail label="Replication factor">
          {cluster.replicationFactor ?? "-"}
        </Detail>
        <Detail label="Owner">{ownerName ?? "-"}</Detail>
        <Detail label="Created">{formatCreatedAt(createdAt)}</Detail>
      </Grid>
      {isUserCluster && isClusterOwner && (
        <DropClusterCard cluster={cluster} onDropped={onDropped} />
      )}
    </VStack>
  );
};

interface ReplicaRow {
  id: string;
  name: string;
  clusterName: string;
  size: string | null;
  status: ReturnType<typeof replicaStatus>;
  createdAt: Date | undefined;
  canDrop: boolean;
}

interface ReplicaStatusCellProps {
  row: ReplicaRow;
}

const ReplicaStatusCell = ({ row }: ReplicaStatusCellProps) => {
  const { status } = row;
  if (status.kind === "online") {
    return <StatusPill status="online" colorScheme="green" />;
  }
  if (status.kind === "offline") {
    const pill = <StatusPill status="offline" colorScheme="red" />;
    return status.reason ? (
      <Tooltip label={status.reason}>
        <Box display="inline-block">{pill}</Box>
      </Tooltip>
    ) : (
      pill
    );
  }
  return <StatusPill status="unknown" colorScheme="gray" />;
};

const replicaColumnHelper = createColumnHelper<ReplicaRow>();

const replicaColumns = [
  replicaColumnHelper.accessor("name", {
    header: "Replica",
    sortingFn: "alphanumeric",
  }),
  replicaColumnHelper.accessor((row) => row.size ?? "", {
    id: "size",
    header: "Size",
    sortingFn: "alphanumeric",
  }),
  replicaColumnHelper.display({
    id: "status",
    header: "Status",
    cell: (info) => <ReplicaStatusCell row={info.row.original} />,
  }),
  replicaColumnHelper.accessor((row) => row.createdAt?.getTime() ?? 0, {
    id: "created",
    header: "Created",
    cell: (info) => formatCreatedAt(info.row.original.createdAt),
  }),
  replicaColumnHelper.display({
    id: "actions",
    header: "",
    cell: (info) => (
      <OverflowMenu
        items={[
          {
            visible: info.row.original.canDrop,
            render: () => (
              <DeleteObjectMenuItem
                key="delete-object"
                selectedObject={info.row.original}
                // The cluster list subscribe drops the row.
                onSuccessAction={() => undefined}
                objectType="CLUSTER REPLICA"
              />
            ),
          },
        ]}
      />
    ),
    enableSorting: false,
    size: OVERFLOW_BUTTON_WIDTH,
  }),
];

interface ReplicasPanelProps {
  cluster: Cluster;
  configuration: Configuration;
  isClusterOwner: boolean;
  maxReplicas: number | null | undefined;
}

const ReplicasPanel = ({
  cluster,
  configuration,
  isClusterOwner,
  maxReplicas,
}: ReplicasPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { track } = useSegment();
  const { isOpen, onOpen, onClose } = useDisclosure();
  // Not the replica count, which includes any burst replica.
  const { replicationFactor } = cluster;
  const createdAtById = React.useMemo(
    () =>
      new Map(
        (configuration?.creationTimes ?? []).flatMap(
          ({ id, objectType, occurredAt }) =>
            objectType === "cluster-replica" ? [[id, occurredAt] as const] : [],
        ),
      ),
    [configuration],
  );
  const rows = React.useMemo(
    () =>
      cluster.replicas.map((replica): ReplicaRow => ({
        id: replica.id,
        name: replica.name,
        clusterName: cluster.name,
        size: replica.size,
        status: replicaStatus(replica.statuses),
        createdAt: createdAtById.get(replica.id),
        // A managed cluster's replicas follow its replication factor.
        canDrop: !cluster.managed && isClusterOwner,
      })),
    [cluster, createdAtById, isClusterOwner],
  );
  const table = useUniversalTable({
    data: rows,
    columns: replicaColumns,
    getRowId: (row) => row.id,
    initialSorting: [{ id: "name", desc: false }],
  });
  const canCreate =
    !cluster.managed &&
    isClusterOwner &&
    notNullOrUndefined(maxReplicas) &&
    cluster.replicas.length < maxReplicas;
  return (
    <VStack spacing={4} alignItems="stretch">
      <PanelHeader
        title="Replicas"
        action={
          canCreate && (
            <Button
              size="sm"
              variant="primary"
              onClick={() => {
                onOpen();
                track("New Replica Clicked");
              }}
            >
              New replica
            </Button>
          )
        }
      />
      {cluster.managed && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {notNullOrUndefined(replicationFactor) ? (
            <>
              A managed cluster runs {replicationFactor}{" "}
              {pluralize(replicationFactor, "replica", "replicas")} of its size,
              one per unit of replication factor.
            </>
          ) : (
            "A managed cluster runs one replica of its size per unit of replication factor."
          )}
          {!isSystemCluster(cluster.id) &&
            " Change the factor with Edit on the overview."}
        </Text>
      )}
      {rows.length === 0 ? (
        <Text textStyle="text-base" color={colors.foreground.secondary}>
          This cluster has no replicas, so it can&apos;t compute dataflows or
          answer queries.
        </Text>
      ) : (
        <UniversalTable table={table} variant="standalone" />
      )}
      {isOpen && (
        <NewReplicaModal
          clusterName={cluster.name}
          isOpen
          onClose={onClose}
          onSubmit={onClose}
        />
      )}
    </VStack>
  );
};

interface AutoScalingPanelProps {
  cluster: Cluster;
  /** Undefined until read. */
  autoScaling: AutoScaling | undefined;
  isError: boolean;
  canEdit: boolean;
  edit: Omit<EditClusterButtonProps, "cluster">;
}

const AutoScalingPanel = ({
  cluster,
  autoScaling,
  isError,
  canEdit,
  edit,
}: AutoScalingPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!cluster.managed) {
    return (
      <VStack spacing={4} alignItems="stretch">
        <PanelHeader title="Auto-Scaling Policy" />
        <Text textStyle="text-base" color={colors.foreground.secondary}>
          Auto-scaling needs a managed cluster.
        </Text>
      </VStack>
    );
  }
  if (autoScaling === undefined) {
    return isError ? null : (
      <Flex justifyContent="center" py={10}>
        <Spinner />
      </Flex>
    );
  }
  const { policy, burst } = autoScaling;
  return (
    <VStack spacing={4} alignItems="stretch">
      <PanelHeader
        title={
          <HStack spacing={2}>
            <span>Auto-Scaling Policy</span>
            {policy ? (
              <StatusPill status="enabled" colorScheme="green" />
            ) : (
              <StatusPill status="off" colorScheme="gray" />
            )}
          </HStack>
        }
        action={
          canEdit && (
            <EditClusterButton cluster={cluster} {...edit} focusAutoScaling />
          )
        }
      />
      {policy && (
        <Grid templateColumns="repeat(3, minmax(0, 1fr))" gap={5}>
          <Detail
            label="Burst size for hydration"
            tooltip="The size of the replica added while objects hydrate."
          >
            {policy.hydrationSize}
          </Detail>
          <Detail
            label="Linger duration"
            tooltip="How long the burst replica stays after hydration finishes, so a brief catch-up doesn't add and remove it repeatedly."
          >
            {policy.lingerMs === undefined
              ? "Default"
              : formatLinger(policy.lingerMs)}
          </Detail>
        </Grid>
      )}
      {burst && (
        <Alert
          variant="info"
          message={`A burst replica of size ${burst.size} is running while objects hydrate.`}
        />
      )}
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        While the cluster has objects to hydrate, Materialize adds a replica of
        the burst size and removes it once they catch up, after the linger
        duration.{" "}
        <TextLink href={AUTO_SCALING_DOCS} isExternal>
          Learn more
        </TextLink>
      </Text>
    </VStack>
  );
};

const replicaSummary = (cluster: Cluster) => {
  const online = cluster.replicas.filter(
    (replica) => replicaStatus(replica.statuses).kind === "online",
  ).length;
  if (cluster.replicas.length === 0) return "None";
  return online === cluster.replicas.length
    ? `${online} online`
    : `${online} of ${cluster.replicas.length} online`;
};

export interface ClusterConfigurationDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  cluster: Cluster;
  section: ConfigurationSection;
  onSectionChange: (section: ConfigurationSection) => void;
}

/**
 * A cluster's configuration in a side drawer: its details, replicas and
 * autoscaling policy, each a section picked from the cards at the top, as in
 * the connect drawer.
 */
export const ClusterConfigurationDrawer = ({
  isOpen,
  onClose,
  cluster,
  section,
  onSectionChange,
}: ClusterConfigurationDrawerProps) => (
  <SideDrawer
    title={`Configure ${cluster.name}`}
    width="640px"
    isOpen={isOpen}
    onClose={onClose}
  >
    <Box px="6" py="5">
      <ConfigurationContent
        cluster={cluster}
        section={section}
        onSectionChange={onSectionChange}
        onDropped={onClose}
      />
    </Box>
  </SideDrawer>
);

interface ConfigurationContentProps {
  cluster: Cluster;
  section: ConfigurationSection;
  onSectionChange: (section: ConfigurationSection) => void;
  onDropped: () => void;
}

const ConfigurationContent = ({
  cluster,
  section,
  onSectionChange,
  onDropped,
}: ConfigurationContentProps) => {
  const {
    data: configuration,
    isError,
    includeAutoScaling: hasAutoScaling,
  } = useClusterConfiguration({
    clusterId: cluster.id,
    replicaIds: cluster.replicas.map(({ id }) => id),
  });
  // Read here, not in the panels: a panel remounts on every section switch,
  // and its queries would refetch each time.
  const { isOwner, ownerName } = useOwners();
  const isClusterOwner = isOwner(cluster.ownerId);
  const { data: maxReplicas } = useMaxReplicasPerCluster();
  const canEdit = !isSystemCluster(cluster.id) && cluster.managed;
  const { data: clusterSizes } = useQuery({
    ...availableClusterSizesQuery(),
    enabled: canEdit,
  });
  const autoScaling =
    configuration?.autoScaling === undefined
      ? undefined
      : parseAutoScaling(configuration.autoScaling);
  const edit = { clusterSizes, maxReplicas };
  const sections = [
    {
      id: "overview" as const,
      label: "Overview",
      sublabel: cluster.managed ? (cluster.size ?? "Managed") : "Unmanaged",
      icon: <InfoIcon w="4" h="4" color="inherit" />,
    },
    {
      id: "replicas" as const,
      label: "Replicas",
      sublabel: replicaSummary(cluster),
      icon: <ClustersIcon w="4" h="4" color="inherit" />,
    },
    ...(hasAutoScaling
      ? [
          {
            id: "auto-scaling" as const,
            label: "Auto-Scaling",
            sublabel:
              autoScaling === undefined
                ? "-"
                : autoScaling.policy
                  ? "Enabled"
                  : "Off",
            icon: <ActivityIcon w="4" h="4" color="inherit" />,
          },
        ]
      : []),
  ];
  // An environment without autoscaling falls back to the overview.
  const shown = sections.some(({ id }) => id === section)
    ? section
    : "overview";
  return (
    <VStack alignItems="stretch" spacing="5">
      <Grid
        templateColumns={`repeat(${sections.length}, 1fr)`}
        gap="2"
        data-testid="configuration-sections"
      >
        {sections.map(({ id, label, sublabel, icon }) => (
          <ConnectMethodCard
            key={id}
            isActive={shown === id}
            label={label}
            sublabel={sublabel}
            icon={icon}
            onClick={() => onSectionChange(id)}
          />
        ))}
      </Grid>
      <Divider />
      {isError && (
        <Alert
          variant="error"
          message="Some of this cluster's configuration is unavailable."
        />
      )}
      {shown === "overview" && (
        <OverviewPanel
          cluster={cluster}
          configuration={configuration}
          ownerName={ownerName(cluster.ownerId)}
          isClusterOwner={isClusterOwner}
          canEdit={canEdit}
          edit={edit}
          onDropped={onDropped}
        />
      )}
      {shown === "replicas" && (
        <ReplicasPanel
          cluster={cluster}
          configuration={configuration}
          isClusterOwner={isClusterOwner}
          maxReplicas={maxReplicas}
        />
      )}
      {shown === "auto-scaling" && (
        <AutoScalingPanel
          cluster={cluster}
          autoScaling={autoScaling}
          isError={isError}
          canEdit={canEdit}
          edit={edit}
        />
      )}
    </VStack>
  );
};
