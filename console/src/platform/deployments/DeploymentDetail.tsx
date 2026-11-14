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
  HStack,
  ListItem,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  UnorderedList,
  useTheme,
  VStack,
  Wrap,
} from "@chakra-ui/react";
import React from "react";
import { Link, useParams } from "react-router-dom";

import {
  AFTER_SWAP_WINDOW_MINUTES,
  DeployCatalogObjectRow,
} from "~/api/materialize/deployments/deploymentCatalog";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import PercentBar from "~/components/PercentBar";
import StatusPill from "~/components/StatusPill";
import { MainContentContainer, PageBreadcrumbs } from "~/layouts/BaseLayout";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import {
  ClusterStatusPill,
  CommandSnippet,
  CutoverPill,
  DeployStatePill,
  LagText,
  Muted,
  NotSetUp,
  SectionHeading,
} from "./deploymentComponents";
import {
  ClusterHealth,
  ClusterVerdict,
  clusterVerdict,
  DEFAULT_ALLOWED_LAG_MS,
  Deployment,
  diffObjects,
  formatDuration,
  ObjectChange,
  ObjectDiffEntry,
  promoteBlockers,
  sinkCutover,
  SinkCutoverRow,
  stagingName,
  timeAgo,
} from "./deploymentModel";
import {
  useAfterSwap,
  useDeploymentDetail,
  useFrontierStalls,
} from "./queries";

const CHANGE_PILL: Record<
  ObjectChange,
  { label: string; colorScheme: "blue" | "green" | "gray" | "red" }
> = {
  changed: { label: "Changed", colorScheme: "blue" },
  added: { label: "New", colorScheme: "green" },
  unchanged: { label: "Redeployed", colorScheme: "gray" },
  removed: { label: "Removed", colorScheme: "red" },
};

const DeploymentDetail = () => {
  const { deployId = "" } = useParams<{ deployId: string }>();
  const detail = useDeploymentDetail(deployId);
  const { deployment, list } = detail;

  const crumbs = [{ title: "Deployments", href: ".." }, { title: deployId }];

  let content: React.ReactNode;
  if (list.isLoading) {
    content = <LoadingContainer />;
  } else if (list.error) {
    content = <ErrorBox message="We couldn't load this deploy." />;
  } else if (list.data && list.data.access !== "ok") {
    content = <NotSetUp access={list.data.access} />;
  } else if (!deployment) {
    content = (
      <ErrorBox
        message={`No deploy ${deployId} in _mz_deploy. It may have been aborted.`}
      />
    );
  } else {
    content = <DeploymentContent detail={detail} deployment={deployment} />;
  }

  return (
    <>
      <PageBreadcrumbs crumbs={crumbs} />
      <MainContentContainer>{content}</MainContentContainer>
    </>
  );
};

const DeploymentContent = ({
  detail,
  deployment,
}: {
  detail: ReturnType<typeof useDeploymentDetail>;
  deployment: Deployment;
}) => {
  const { tracking, baseline } = detail;
  const diff = React.useMemo(
    () =>
      tracking.data
        ? diffObjects(deployment.deployId, tracking.data.objects, baseline)
        : [],
    [tracking.data, deployment.deployId, baseline],
  );
  const isPromoted = deployment.promotedAt !== null;

  return (
    <VStack alignItems="stretch" spacing="8" pb="10">
      <DeploymentHeader deployment={deployment} />
      {isPromoted ? (
        <PromotedSummary
          deployment={deployment}
          diff={diff}
          tracking={tracking}
        />
      ) : (
        <InProgressDetail detail={detail} deployment={deployment} diff={diff} />
      )}
    </VStack>
  );
};

const DeploymentHeader = ({ deployment }: { deployment: Deployment }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isPromoted = deployment.promotedAt !== null;
  return (
    <HStack justifyContent="space-between" alignItems="start" spacing="6">
      <VStack alignItems="start" spacing="1">
        <HStack spacing="3">
          <Text textStyle="heading-lg">
            Deploy{" "}
            <Text as="span" fontFamily="mono">
              {deployment.deployId}
            </Text>
          </Text>
          <DeployStatePill promoted={isPromoted} />
        </HStack>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {`mz-deploy ${deployment.mode} · staged by ${deployment.deployedBy} ${timeAgo(deployment.deployedAt)}`}
          {deployment.commit && ` · commit ${deployment.commit.slice(0, 12)}`}
        </Text>
        <Wrap spacing="1" pt="1">
          {deployment.schemas.map((s) => (
            <StatusPill
              key={`${s.database}.${s.schema}`}
              status={s.kind}
              label={
                s.kind === "objects"
                  ? `${s.database}.${s.schema}`
                  : `${s.database}.${s.schema} (sinks)`
              }
              colorScheme="gray"
            />
          ))}
        </Wrap>
      </VStack>
      {!isPromoted && (
        <VStack alignItems="end" spacing="2" flexShrink={0}>
          <CommandSnippet
            command={`mz-deploy promote ${deployment.deployId}`}
          />
          <CommandSnippet command={`mz-deploy abort ${deployment.deployId}`} />
        </VStack>
      )}
    </HStack>
  );
};

interface ClusterPair {
  liveName: string;
  stagingName: string;
  live: ClusterHealth | undefined;
  staged: ClusterHealth | undefined;
  verdict: ClusterVerdict;
}

const InProgressDetail = ({
  detail,
  deployment,
  diff,
}: {
  detail: ReturnType<typeof useDeploymentDetail>;
  deployment: Deployment;
  diff: ObjectDiffEntry[];
}) => {
  const { catalog, tracking, liveClusters } = detail;

  const pairs: ClusterPair[] = React.useMemo(() => {
    const byName = new Map(
      (catalog.data?.clusters ?? []).map((c) => [c.name, c]),
    );
    return liveClusters
      .map((liveName) => {
        const staged = byName.get(stagingName(liveName, deployment.deployId));
        return {
          liveName,
          stagingName: stagingName(liveName, deployment.deployId),
          live: byName.get(liveName),
          staged,
          verdict: clusterVerdict(staged),
        };
      })
      .sort((a, b) => a.liveName.localeCompare(b.liveName));
  }, [catalog.data, liveClusters, deployment.deployId]);

  const sinkFrontiers = React.useMemo(
    () =>
      catalog.data?.sinks.map((s) => ({
        id: s.sinkId,
        frontier: s.sinkWriteFrontierMs,
      })),
    [catalog.data],
  );
  const stalls = useFrontierStalls(sinkFrontiers);
  const sinks = React.useMemo(
    () =>
      (catalog.data?.sinks ?? []).map((row) => ({
        row,
        cutover: sinkCutover(row, stalls.get(row.sinkId) ?? 0),
        stalledForMs: stalls.get(row.sinkId) ?? 0,
      })),
    [catalog.data, stalls],
  );

  if (catalog.error || tracking.error) {
    return <ErrorBox message="We couldn't load this deploy's status." />;
  }
  // NOTE: isPending, not isLoading. The catalog query is disabled until the
  // tracking rows arrive, and a disabled query is pending without loading.
  if (catalog.isPending || tracking.isPending) return <LoadingContainer />;

  const blockers = promoteBlockers(pairs, sinks);

  return (
    <>
      <Verdict blockers={blockers} />
      <ClustersSection pairs={pairs} />
      <SinksSection
        sinks={sinks}
        pending={tracking.data?.pendingStatements ?? []}
        existingSinks={catalog.data?.existingSinks ?? []}
      />
      <ObjectsSection
        deployment={deployment}
        diff={diff}
        objects={catalog.data?.objects ?? []}
      />
    </>
  );
};

const Verdict = ({
  blockers,
}: {
  blockers: ReturnType<typeof promoteBlockers>;
}) =>
  blockers.length === 0 ? (
    <Alert
      variant="info"
      label="Ready to promote"
      message={`Every new cluster is hydrated and within ${formatDuration(DEFAULT_ALLOWED_LAG_MS)} of live, and every sink can switch without waiting.`}
    />
  ) : (
    <Alert
      variant="warning"
      label="Not ready to promote"
      message={
        <UnorderedList spacing="1" mt="1">
          {blockers.map((b) => (
            <ListItem key={`${b.kind}:${b.name}`}>
              <Text as="span" fontWeight="500">
                {b.name}
              </Text>
              : {b.message}
            </ListItem>
          ))}
        </UnorderedList>
      }
    />
  );

const ClusterLink = ({ cluster }: { cluster: ClusterHealth | undefined }) => {
  const regionSlug = useRegionSlug();
  if (!cluster) return <Muted>Not found</Muted>;
  return (
    <Box>
      <Text
        as={Link}
        to={absoluteClusterPath(regionSlug, cluster)}
        textStyle="text-ui-med"
        _hover={{ textDecoration: "underline" }}
      >
        {cluster.name}
      </Text>
      <Text textStyle="text-small" color="gray.500">
        {cluster.size ?? "—"} · {cluster.replicas}{" "}
        {cluster.replicas === 1 ? "replica" : "replicas"}
      </Text>
    </Box>
  );
};

const ClustersSection = ({ pairs }: { pairs: ClusterPair[] }) => (
  <Box>
    <SectionHeading
      title="Clusters"
      description={`Each live cluster next to the copy this deploy staged. Same rule as mz-deploy wait: hydrated, and lag at most ${formatDuration(DEFAULT_ALLOWED_LAG_MS)}.`}
    />
    <Table variant="standalone" borderRadius="xl">
      <Thead>
        <Tr>
          <Th>Live</Th>
          <Th>New</Th>
          <Th>Status</Th>
          <Th>Hydrated</Th>
          <Th>Lag</Th>
          <Th>Memory, live</Th>
          <Th>Memory, new</Th>
        </Tr>
      </Thead>
      <Tbody>
        {pairs.map((pair) => (
          <Tr key={pair.liveName}>
            <Td>
              <ClusterLink cluster={pair.live} />
            </Td>
            <Td>
              <ClusterLink cluster={pair.staged} />
            </Td>
            <Td>
              <VStack alignItems="start" spacing="1">
                <ClusterStatusPill status={pair.verdict.status} />
                {pair.verdict.status === "failing" && (
                  <Text textStyle="text-small" color="gray.500">
                    {pair.verdict.detail}
                  </Text>
                )}
              </VStack>
            </Td>
            <Td>
              {pair.staged ? (
                `${pair.staged.hydrated} of ${pair.staged.total}`
              ) : (
                <Muted>—</Muted>
              )}
            </Td>
            <Td>
              <LagText lagMs={pair.staged?.maxLagMs} />
            </Td>
            <Td>
              <PercentBar
                fraction={
                  pair.live?.memoryPercent != null
                    ? pair.live.memoryPercent / 100
                    : null
                }
              />
            </Td>
            <Td>
              <PercentBar
                fraction={
                  pair.staged?.memoryPercent != null
                    ? pair.staged.memoryPercent / 100
                    : null
                }
              />
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  </Box>
);

const SinksSection = ({
  sinks,
  pending,
  existingSinks,
}: {
  sinks: {
    row: SinkCutoverRow;
    cutover: ReturnType<typeof sinkCutover>;
    stalledForMs: number;
  }[];
  pending: {
    database: string;
    schema: string;
    object: string;
    statementKind: string;
    statementSql: string;
  }[];
  existingSinks: { database: string; schema: string; name: string }[];
}) => {
  const exists = new Set(
    existingSinks.map((k) => `${k.database}.${k.schema}.${k.name}`),
  );
  const pendingSinks = pending.filter(
    (p) =>
      p.statementKind === "sink" &&
      !exists.has(`${p.database}.${p.schema}.${p.object}`),
  );

  return (
    <Box>
      <SectionHeading
        title="Sinks at promote"
        description="Promote repoints these sinks to the new objects with ALTER SINK ... SET FROM. Each switch waits until the sink has written up to where the new object's data starts."
      />
      {sinks.length === 0 && pendingSinks.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          No sinks read from the schemas this deploy swaps.
        </Text>
      ) : (
        <Table variant="standalone" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Sink</Th>
              <Th>Reported status</Th>
              <Th>Reads from</Th>
              <Th>Sink lag</Th>
              <Th width="32%">Cutover</Th>
            </Tr>
          </Thead>
          <Tbody>
            {sinks.map(({ row, cutover, stalledForMs }) => (
              <Tr key={row.sinkId}>
                <Td>
                  <Text textStyle="text-small" color="gray.500">
                    {row.sinkDatabase}.{row.sinkSchema}
                  </Text>
                  <Text textStyle="text-ui-med">{row.sinkName}</Text>
                </Td>
                <Td>
                  {row.status ? (
                    <Tooltip label={row.error ?? undefined}>
                      <Box display="inline-block">
                        <StatusPill
                          status={row.status}
                          label={
                            row.status.charAt(0).toUpperCase() +
                            row.status.slice(1)
                          }
                          colorScheme={
                            row.status === "running" ? "green" : "yellow"
                          }
                        />
                      </Box>
                    </Tooltip>
                  ) : (
                    <Muted>—</Muted>
                  )}
                </Td>
                <Td>
                  <Text textStyle="text-ui-reg">
                    {row.dependencySchema}.{row.dependencyName}
                  </Text>
                  <Text textStyle="text-small" color="gray.500">
                    then the new {row.dependencyName}
                  </Text>
                </Td>
                <Td>
                  <LagText lagMs={row.sinkLagMs} />
                </Td>
                <Td>
                  <VStack alignItems="start" spacing="1">
                    <CutoverPill state={cutover.state} />
                    <Text textStyle="text-small" color="gray.500">
                      {cutoverDetail(
                        cutover.state,
                        cutover.gapMs,
                        stalledForMs,
                      )}
                    </Text>
                  </VStack>
                </Td>
              </Tr>
            ))}
            {pendingSinks.map((p) => (
              <Tr key={`${p.database}.${p.schema}.${p.object}`}>
                <Td>
                  <Text textStyle="text-small" color="gray.500">
                    {p.database}.{p.schema}
                  </Text>
                  <Text textStyle="text-ui-med">{p.object}</Text>
                </Td>
                <Td colSpan={3}>
                  <Text
                    textStyle="text-small"
                    fontFamily="mono"
                    noOfLines={2}
                    title={p.statementSql}
                  >
                    {p.statementSql}
                  </Text>
                </Td>
                <Td>
                  <VStack alignItems="start" spacing="1">
                    <StatusPill
                      status="pending"
                      label="Created at promote"
                      colorScheme="blue"
                    />
                    <Text textStyle="text-small" color="gray.500">
                      A new sink writes a full snapshot first.
                    </Text>
                  </VStack>
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

function cutoverDetail(
  state: ReturnType<typeof sinkCutover>["state"],
  gapMs: number,
  stalledForMs: number,
) {
  switch (state) {
    case "ready":
      return "The sink has already written past where the new object starts.";
    case "behind":
      return `${formatDuration(gapMs)} still to write before it can switch.`;
    case "stuck":
      return `${formatDuration(gapMs)} still to write, and its frontier has not moved in the ${formatDuration(stalledForMs)} this page has watched it.`;
    case "no-replacement":
      return "The new schema has no object with this name.";
    case "unknown":
      return "The sink has not reported a write frontier.";
  }
}

const ObjectsSection = ({
  deployment,
  diff,
  objects,
}: {
  deployment: Deployment;
  diff: ObjectDiffEntry[];
  objects: DeployCatalogObjectRow[];
}) => {
  const changeByObject = new Map(
    diff.map((d) => [`${d.database}.${d.schema}.${d.object}`, d.change]),
  );
  const staged = objects
    .map((o) => {
      const liveSchema = o.schema.slice(
        0,
        o.schema.length - deployment.deployId.length - 1,
      );
      return {
        ...o,
        liveSchema,
        change: changeByObject.get(`${o.database}.${liveSchema}.${o.name}`),
      };
    })
    .sort((a, b) =>
      `${a.liveSchema}.${a.name}`.localeCompare(`${b.liveSchema}.${b.name}`),
    );
  const counts = diff.reduce(
    (acc, d) => ({ ...acc, [d.change]: (acc[d.change] ?? 0) + 1 }),
    {} as Partial<Record<ObjectChange, number>>,
  );
  const removed = diff.filter((d) => d.change === "removed");

  return (
    <Box>
      <SectionHeading
        title="What this deploy changes"
        description="Compared by definition hash with the deploy each schema was last promoted from. Unchanged objects are still rebuilt, because mz-deploy swaps whole schemas."
      >
        <HStack spacing="2" flexShrink={0} whiteSpace="nowrap">
          {(["changed", "added", "removed", "unchanged"] as const).map(
            (change) =>
              counts[change] ? (
                <StatusPill
                  key={change}
                  status={change}
                  label={`${counts[change]} ${CHANGE_PILL[change].label.toLowerCase()}`}
                  colorScheme={CHANGE_PILL[change].colorScheme}
                />
              ) : null,
          )}
        </HStack>
      </SectionHeading>
      <Table variant="standalone" borderRadius="xl">
        <Thead>
          <Tr>
            <Th>Object</Th>
            <Th>Type</Th>
            <Th>Change</Th>
            <Th>New cluster</Th>
            <Th>Hydrated</Th>
            <Th>Lag</Th>
          </Tr>
        </Thead>
        <Tbody>
          {staged.map((o) => (
            <Tr key={o.id}>
              <Td>
                <Text textStyle="text-small" color="gray.500">
                  {o.database}.{o.liveSchema}
                </Text>
                <Text textStyle="text-ui-med">{o.name}</Text>
              </Td>
              <Td>{o.type}</Td>
              <Td>
                {o.change ? (
                  <StatusPill
                    status={o.change}
                    label={CHANGE_PILL[o.change].label}
                    colorScheme={CHANGE_PILL[o.change].colorScheme}
                  />
                ) : o.type === "index" ? (
                  // mz-deploy hashes an index with the view it is defined on.
                  <Muted>With its view</Muted>
                ) : (
                  <Muted>—</Muted>
                )}
              </Td>
              <Td>{o.clusterName ?? <Muted>—</Muted>}</Td>
              <Td>
                {o.hydrated === null ? (
                  <Muted>—</Muted>
                ) : o.hydrated ? (
                  "Yes"
                ) : (
                  <Text as="span" fontWeight="500">
                    Not yet
                  </Text>
                )}
              </Td>
              <Td>
                <LagText lagMs={o.lagMs} />
              </Td>
            </Tr>
          ))}
          {removed.map((d) => (
            <Tr key={`removed:${d.schema}.${d.object}`}>
              <Td>
                <Text textStyle="text-small" color="gray.500">
                  {d.database}.{d.schema}
                </Text>
                <Text textStyle="text-ui-med">{d.object}</Text>
              </Td>
              <Td>
                <Muted>—</Muted>
              </Td>
              <Td>
                <StatusPill
                  status="removed"
                  label="Removed"
                  colorScheme="red"
                />
              </Td>
              <Td colSpan={3}>
                <Muted>Dropped with the old schema after promote</Muted>
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  );
};

const PromotedSummary = ({
  deployment,
  diff,
  tracking,
}: {
  deployment: Deployment;
  diff: ObjectDiffEntry[];
  tracking: ReturnType<typeof useDeploymentDetail>["tracking"];
}) => {
  const promotedAt = deployment.promotedAt;
  if (!promotedAt) return null;
  const staged = promotedAt.getTime() - deployment.deployedAt.getTime();
  const changed = diff.filter((d) => d.change !== "unchanged");
  return (
    <>
      <Alert
        variant="info"
        label={`Promoted ${formatDate(promotedAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}`}
        message={`Staged for ${formatDuration(staged)} before promotion. Its clusters and schemas now run under their production names.`}
      />
      <AfterSwap deployment={deployment} />
      <Box>
        <SectionHeading
          title="What this deploy changed"
          description="Compared by definition hash with the deploy each schema was promoted from before it."
        />
        {tracking.isPending ? (
          <LoadingContainer />
        ) : tracking.error ? (
          <ErrorBox message="We couldn't load this deploy's objects." />
        ) : changed.length === 0 ? (
          <Text textStyle="text-base" color="gray.500">
            No definitions changed. Every object was redeployed as is.
          </Text>
        ) : (
          <Table variant="standalone" borderRadius="xl">
            <Thead>
              <Tr>
                <Th>Object</Th>
                <Th>Change</Th>
              </Tr>
            </Thead>
            <Tbody>
              {changed.map((d) => (
                <Tr key={`${d.schema}.${d.object}`}>
                  <Td>
                    <Text textStyle="text-small" color="gray.500">
                      {d.database}.{d.schema}
                    </Text>
                    <Text textStyle="text-ui-med">{d.object}</Text>
                  </Td>
                  <Td>
                    <StatusPill
                      status={d.change}
                      label={CHANGE_PILL[d.change].label}
                      colorScheme={CHANGE_PILL[d.change].colorScheme}
                    />
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}
      </Box>
    </>
  );
};

const AfterSwap = ({ deployment }: { deployment: Deployment }) => {
  const { data, isLoading, error } = useAfterSwap(deployment);
  return (
    <Box>
      <SectionHeading
        title="After the swap"
        description={`Load on the clusters this deploy promoted, in the ${AFTER_SWAP_WINDOW_MINUTES} minutes after promote, when clients reconnect and subscribes restart on them.`}
      />
      {isLoading ? (
        <LoadingContainer />
      ) : error ? (
        <ErrorBox message="We couldn't load cluster metrics for this deploy." />
      ) : !data || data.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          No clusters were created for this deploy, or the audit log no longer
          has them.
        </Text>
      ) : (
        <Table variant="standalone" borderRadius="xl">
          <Thead>
            <Tr>
              <Th>Cluster</Th>
              <Th>Peak memory</Th>
              <Th>Peak CPU</Th>
              <Th>Restarts</Th>
              <Th>Out of memory</Th>
            </Tr>
          </Thead>
          <Tbody>
            {data.map((row) => (
              <Tr key={row.clusterId}>
                <Td>
                  <Text textStyle="text-ui-med">
                    {row.currentName ?? row.stagedName}
                  </Text>
                  <Text textStyle="text-small" color="gray.500">
                    {row.currentName
                      ? `staged as ${row.stagedName}`
                      : "dropped since"}
                  </Text>
                </Td>
                <Td>
                  {row.samples === 0 ? (
                    <Muted>No metrics recorded</Muted>
                  ) : (
                    <PercentBar fraction={row.peakMemoryFraction} />
                  )}
                </Td>
                <Td>
                  {row.samples === 0 ? (
                    <Muted>—</Muted>
                  ) : (
                    <PercentBar fraction={row.peakCpuFraction} />
                  )}
                </Td>
                <Td>{row.restarts}</Td>
                <Td>{row.oomKills}</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </Box>
  );
};

export default DeploymentDetail;
