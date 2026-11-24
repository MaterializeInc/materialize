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
  SectionHeading,
} from "./deploymentComponents";
import {
  ClusterHealth,
  ClusterVerdict,
  clusterVerdict,
  DEFAULT_ALLOWED_LAG_MS,
  Deployment,
  diffDefinitions,
  finishCommands,
  formatDuration,
  ObjectChange,
  ObjectDiffEntry,
  promoteBlockers,
  sinkCutover,
  SinkCutoverRow,
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
  const { deployKey = "" } = useParams<{ deployKey: string }>();
  const detail = useDeploymentDetail(deployKey);
  const { deployment, list } = detail;

  const crumbs = [
    { title: "Deployments", href: ".." },
    { title: deployment?.suffix ?? deployKey },
  ];

  let content: React.ReactNode;
  if (list.isLoading) {
    content = <LoadingContainer />;
  } else if (list.error) {
    content = <ErrorBox message="We couldn't load this deploy." />;
  } else if (!deployment) {
    content = (
      <ErrorBox message="This deploy is no longer in the catalog or the audit log." />
    );
  } else {
    content = (
      <VStack alignItems="stretch" spacing="8" pb="10">
        <DeploymentHeader deployment={deployment} />
        {deployment.state === "in-progress" ? (
          <InProgressDetail detail={detail} deployment={deployment} />
        ) : deployment.state === "promoted" ? (
          <PromotedSummary deployment={deployment} />
        ) : (
          <AbortedSummary deployment={deployment} />
        )}
      </VStack>
    );
  }

  return (
    <>
      <PageBreadcrumbs crumbs={crumbs} />
      <MainContentContainer>{content}</MainContentContainer>
    </>
  );
};

const DeploymentHeader = ({ deployment }: { deployment: Deployment }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const commands =
    deployment.state === "in-progress" ? finishCommands(deployment) : null;
  const meta = [
    deployment.tool !== "unknown" ? deployment.tool : null,
    deployment.stagedAt
      ? `staged ${deployment.stagedBy ? `by ${deployment.stagedBy} ` : ""}${timeAgo(deployment.stagedAt)}`
      : null,
  ].filter(Boolean);
  return (
    <HStack justifyContent="space-between" alignItems="start" spacing="6">
      <VStack alignItems="start" spacing="1">
        <HStack spacing="3">
          <Text textStyle="heading-lg">
            Deploy{" "}
            <Text as="span" fontFamily="mono">
              {deployment.suffix}
            </Text>
          </Text>
          <DeployStatePill state={deployment.state} />
        </HStack>
        {meta.length > 0 && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {meta.join(" · ")}
          </Text>
        )}
        <Wrap spacing="1" pt="1">
          {deployment.schemas.map((s) => (
            <StatusPill
              key={s.id}
              status="schema"
              label={`${s.database}.${s.liveName}`}
              colorScheme="gray"
            />
          ))}
        </Wrap>
      </VStack>
      {commands && (
        <VStack alignItems="end" spacing="2" flexShrink={0}>
          <CommandSnippet command={commands.promote} />
          <CommandSnippet command={commands.abort} />
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
}: {
  detail: ReturnType<typeof useDeploymentDetail>;
  deployment: Deployment;
}) => {
  const { catalog } = detail;

  const pairs: ClusterPair[] = React.useMemo(() => {
    const byName = new Map(
      (catalog.data?.clusters ?? []).map((c) => [c.name, c]),
    );
    return deployment.clusters.map((c) => {
      const staged = byName.get(c.stagedName);
      return {
        liveName: c.liveName,
        stagingName: c.stagedName,
        live: byName.get(c.liveName),
        staged,
        verdict: clusterVerdict(staged),
      };
    });
  }, [catalog.data, deployment.clusters]);

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
  const diff = React.useMemo(
    () => diffDefinitions(deployment, catalog.data?.definitions ?? []),
    [deployment, catalog.data],
  );

  if (catalog.error) {
    return <ErrorBox message="We couldn't load this deploy's status." />;
  }
  if (catalog.isLoading || !catalog.data) return <LoadingContainer />;

  const blockers = promoteBlockers(pairs, sinks);

  return (
    <>
      <Verdict blockers={blockers} />
      {pairs.length > 0 && <ClustersSection pairs={pairs} />}
      <SinksSection sinks={sinks} />
      <ObjectsSection diff={diff} objects={catalog.data.objects} />
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
}: {
  sinks: {
    row: SinkCutoverRow;
    cutover: ReturnType<typeof sinkCutover>;
    stalledForMs: number;
  }[];
}) => (
  <Box>
    <SectionHeading
      title="Sinks at promote"
      description="Promote repoints these sinks to the new objects with ALTER SINK ... SET FROM. Each switch waits until the sink has written up to where the new object's data starts."
    />
    {sinks.length === 0 ? (
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
                    {cutoverDetail(cutover.state, cutover.gapMs, stalledForMs)}
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

const ChangePill = ({ change }: { change: ObjectChange }) => (
  <StatusPill
    status={change}
    label={CHANGE_PILL[change].label}
    colorScheme={CHANGE_PILL[change].colorScheme}
  />
);

const ObjectsSection = ({
  diff,
  objects,
}: {
  diff: ObjectDiffEntry[];
  objects: DeployCatalogObjectRow[];
}) => {
  const stateById = new Map(objects.map((o) => [o.id, o]));
  const counts = diff.reduce(
    (acc, d) => ({ ...acc, [d.change]: (acc[d.change] ?? 0) + 1 }),
    {} as Partial<Record<ObjectChange, number>>,
  );

  return (
    <Box>
      <SectionHeading
        title="What this deploy changes"
        description="Each staged definition compared with the live object of the same name. Unchanged objects are still rebuilt, because the deploy swaps whole schemas."
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
          {diff.map((d) => {
            const state = d.stagedId ? stateById.get(d.stagedId) : undefined;
            return (
              <Tr key={`${d.database}.${d.schema}.${d.object}`}>
                <Td>
                  <Text textStyle="text-small" color="gray.500">
                    {d.database}.{d.schema}
                  </Text>
                  <Text textStyle="text-ui-med">{d.object}</Text>
                </Td>
                <Td>{d.type}</Td>
                <Td>
                  <ChangePill change={d.change} />
                </Td>
                {d.change === "removed" ? (
                  <Td colSpan={3}>
                    <Muted>Dropped with the old schema after promote</Muted>
                  </Td>
                ) : (
                  <>
                    <Td>{state?.clusterName ?? <Muted>—</Muted>}</Td>
                    <Td>
                      {state?.hydrated == null ? (
                        <Muted>—</Muted>
                      ) : state.hydrated ? (
                        "Yes"
                      ) : (
                        <Text as="span" fontWeight="500">
                          Not yet
                        </Text>
                      )}
                    </Td>
                    <Td>
                      <LagText lagMs={state?.lagMs} />
                    </Td>
                  </>
                )}
              </Tr>
            );
          })}
        </Tbody>
      </Table>
    </Box>
  );
};

const StagedPairsTable = ({ deployment }: { deployment: Deployment }) => (
  <Box>
    <SectionHeading
      title="What it staged"
      description="Each schema and cluster under its staged name, next to the live one it copied."
    />
    <Table variant="standalone" borderRadius="xl">
      <Thead>
        <Tr>
          <Th>Kind</Th>
          <Th>Live</Th>
          <Th>Staged as</Th>
        </Tr>
      </Thead>
      <Tbody>
        {[
          ...deployment.schemas.map((p) => ({ kind: "Schema", ...p })),
          ...deployment.clusters.map((p) => ({ kind: "Cluster", ...p })),
        ].map((p) => (
          <Tr key={p.id}>
            <Td>{p.kind}</Td>
            <Td>{p.database ? `${p.database}.${p.liveName}` : p.liveName}</Td>
            <Td>
              <Text fontFamily="mono" textStyle="text-small">
                {p.stagedName}
              </Text>
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  </Box>
);

const stagedFor = (deployment: Deployment) =>
  deployment.stagedAt && deployment.endedAt
    ? formatDuration(
        deployment.endedAt.getTime() - deployment.stagedAt.getTime(),
      )
    : null;

const PromotedSummary = ({ deployment }: { deployment: Deployment }) => {
  if (!deployment.endedAt) return null;
  const staged = stagedFor(deployment);
  return (
    <>
      <Alert
        variant="info"
        label={`Promoted ${formatDate(deployment.endedAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}${deployment.endedBy ? ` by ${deployment.endedBy}` : ""}`}
        message={`${staged ? `Staged for ${staged} before the swap. ` : ""}Its schemas and clusters now run under their live names. The catalog keeps no record of the definitions it replaced, so it can't show what this deploy changed.`}
      />
      <AfterSwap deployment={deployment} />
      <StagedPairsTable deployment={deployment} />
    </>
  );
};

const AbortedSummary = ({ deployment }: { deployment: Deployment }) => {
  if (!deployment.endedAt) return null;
  const staged = stagedFor(deployment);
  return (
    <>
      <Alert
        variant="warning"
        label={`Dropped ${formatDate(deployment.endedAt, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}${deployment.endedBy ? ` by ${deployment.endedBy}` : ""}`}
        message={`${staged ? `Staged for ${staged}, then ` : ""}dropped without being swapped in.`}
      />
      <StagedPairsTable deployment={deployment} />
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
      {deployment.clusters.length === 0 ? (
        <Text textStyle="text-base" color="gray.500">
          This deploy did not swap any clusters.
        </Text>
      ) : isLoading ? (
        <LoadingContainer />
      ) : error ? (
        <ErrorBox message="We couldn't load cluster metrics for this deploy." />
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
            {(data ?? []).map((row) => (
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
