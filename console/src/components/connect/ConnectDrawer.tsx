// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Divider, Flex, Grid, Spinner, VStack } from "@chakra-ui/react";
import { useAtom } from "jotai";
import React, { useState } from "react";
import { useParams } from "react-router-dom";

import SideDrawer from "~/components/SideDrawer";
import { useAppConfig } from "~/config/useAppConfig";
import { User } from "~/external-library-wrappers/frontegg";
import { type AuthContextProps } from "~/external-library-wrappers/oidc";
import { useSelfManagedProfile } from "~/hooks/useSelfManagedProfile";
import { ConnectionIcon, MonitorIcon, Terminal } from "~/icons";
import { ClusterDetailParams } from "~/platform/clusters/ClusterRoutes";
import {
  currentEnvironmentState,
  useEnvironmentGate,
} from "~/store/environments";

import { ConnectMethodCard } from "./connectComponents";
import { ConnectExternalToolsPanel } from "./ConnectExternalToolsPanel";
import { ConnectMcpPanel } from "./ConnectMcpPanel";
import { ConnectContext } from "./connectOptions";
import { ConnectTerminalPanel } from "./ConnectTerminalPanel";

const OIDC_USERNAME_PLACEHOLDER = "<your_oidc_username>";
const PASSWORD_USERNAME_PLACEHOLDER = "<your_username>";
const HOST_PLACEHOLDER = "<host>";

type ConnectMethodId = "mcp" | "external-tools" | "terminal";

const CONNECT_METHODS: {
  id: ConnectMethodId;
  label: string;
  sublabel: string;
  icon: React.ReactNode;
}[] = [
  {
    id: "mcp",
    label: "MCP Server",
    sublabel: "Connect your agent",
    icon: <ConnectionIcon w="4" h="4" color="inherit" />,
  },
  {
    id: "external-tools",
    label: "External tools",
    sublabel: "Connection details",
    icon: <MonitorIcon w="4" h="4" color="inherit" />,
  },
  {
    id: "terminal",
    label: "Terminal",
    sublabel: "psql",
    icon: <Terminal w="4" h="4" color="inherit" />,
  },
];

const ConnectPanels = ({ ctx }: { ctx: ConnectContext }) => {
  const [methodId, setMethodId] = useState<ConnectMethodId>("mcp");

  return (
    <VStack alignItems="stretch" spacing="5">
      <Grid
        templateColumns="repeat(3, 1fr)"
        gap="2"
        data-testid="connection-options"
      >
        {CONNECT_METHODS.map((method) => (
          <ConnectMethodCard
            key={method.id}
            isActive={methodId === method.id}
            label={method.label}
            sublabel={method.sublabel}
            icon={method.icon}
            onClick={() => setMethodId(method.id)}
          />
        ))}
      </Grid>
      <Divider />
      {methodId === "mcp" && <ConnectMcpPanel ctx={ctx} />}
      {methodId === "external-tools" && <ConnectExternalToolsPanel ctx={ctx} />}
      {methodId === "terminal" && <ConnectTerminalPanel ctx={ctx} />}
    </VStack>
  );
};

const LoadingPanel = () => (
  <Flex justifyContent="center" py="16">
    <Spinner />
  </Flex>
);

const CloudConnectContent = ({
  user,
  forAppPassword,
}: {
  user?: User;
  forAppPassword?: { user: string };
}) => {
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const { clusterName } = useParams<ClusterDetailParams>();
  const oauthAvailable = useEnvironmentGate("26.30.0") === true;

  if (currentEnvironment?.state !== "enabled") {
    return <LoadingPanel />;
  }

  const [host, port] = currentEnvironment.sqlAddress.split(":");
  // MCP is served on the HTTP endpoint, so build its URL from the HTTP host.
  // The pgwire host does not serve MCP.
  const mcpBaseUrl = `https://${currentEnvironment.httpAddress.split(":")[0]}`;

  const ctx: ConnectContext = {
    isCloud: true,
    user: forAppPassword?.user ?? user?.email ?? "",
    host,
    port,
    database: "materialize",
    ssl: true,
    mcpBaseUrl,
    oauthAvailable,
    clusterName,
    canCreateAppPassword: !forAppPassword,
  };

  return <ConnectPanels ctx={ctx} />;
};

const SelfManagedConnectContent = ({
  auth,
  oidcEnabled = false,
}: {
  auth?: AuthContextProps;
  oidcEnabled?: boolean;
}) => {
  const appConfig = useAppConfig();
  const [currentEnvironment] = useAtom(currentEnvironmentState);
  const { clusterName } = useParams<ClusterDetailParams>();
  const { sqlRole } = useSelfManagedProfile(auth);
  // Envs >= 26.30.0 advertise OAuth via RFC 9728, so MCP clients log in
  // through the browser instead of using a Basic-auth token. Requires OIDC on
  // self-managed. The version must match region-controller's gate for
  // `--frontegg-oauth-issuer-url` (precedence >= 26.30.0).
  const oauthAvailable = useEnvironmentGate("26.30.0") === true && oidcEnabled;

  if (
    appConfig.mode !== "self-managed" ||
    currentEnvironment?.state !== "enabled"
  ) {
    return <LoadingPanel />;
  }

  const balancerdHost = appConfig.balancerdDnsNames?.[0];
  const fallbackUser = oidcEnabled
    ? OIDC_USERNAME_PLACEHOLDER
    : PASSWORD_USERNAME_PLACEHOLDER;
  // The console's own HTTP address serves MCP (nginx proxies to environmentd).
  // The pgwire host advertised in `balancerdDnsNames` does not serve MCP.
  const mcpBaseUrl = `${appConfig.environmentdScheme}://${currentEnvironment.httpAddress}`;

  const ctx: ConnectContext = {
    isCloud: false,
    user: sqlRole ?? fallbackUser,
    host: balancerdHost ?? HOST_PLACEHOLDER,
    port: "6875",
    database: "materialize",
    ssl: appConfig.environmentdScheme === "https",
    mcpBaseUrl,
    oauthAvailable,
    clusterName,
    canCreateAppPassword: false,
    idToken: oidcEnabled ? auth?.user?.id_token : undefined,
  };

  return <ConnectPanels ctx={ctx} />;
};

export interface ConnectDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  /** Cloud sessions: the signed-in Frontegg user. */
  user?: User;
  /** Self-managed sessions: the OIDC auth context, when available. */
  auth?: AuthContextProps;
  /** Self-managed deployment with OIDC enabled; gets the browser OAuth flow. */
  oidcEnabled?: boolean;
  /** Pin the instructions to a specific user, e.g. a service account whose
   * app password was just created. Hides app password creation. */
  forAppPassword?: {
    user: string;
  };
}

/** Side drawer with instructions for connecting agents, external tools, and
 * psql to Materialize. */
const ConnectDrawer = ({
  isOpen,
  onClose,
  user,
  auth,
  oidcEnabled,
  forAppPassword,
}: ConnectDrawerProps) => {
  const appConfig = useAppConfig();

  return (
    <SideDrawer
      title="Connect to Materialize"
      width="640px"
      isOpen={isOpen}
      onClose={onClose}
    >
      <Box px="6" py="5">
        {appConfig.mode === "cloud" ? (
          <CloudConnectContent user={user} forAppPassword={forAppPassword} />
        ) : (
          <SelfManagedConnectContent auth={auth} oidcEnabled={oidcEnabled} />
        )}
      </Box>
    </SideDrawer>
  );
};

export default ConnectDrawer;
