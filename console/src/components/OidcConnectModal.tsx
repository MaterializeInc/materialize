// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Code,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import ConnectInstructions from "~/components/ConnectInstructions";
import {
  SecretCopyableBox,
  TabbedCodeBlock,
} from "~/components/copyableComponents";
import McpConnectInstructions from "~/components/McpConnectInstructions";
import { Modal } from "~/components/Modal";
import TextLink from "~/components/TextLink";
import { useAppConfig } from "~/config/useAppConfig";
import { type AuthContextProps } from "~/external-library-wrappers/oidc";
import { useSelfManagedProfile } from "~/hooks/useSelfManagedProfile";
import ConnectionIcon from "~/svg/ConnectionIcon";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

const OIDC_USERNAME_PLACEHOLDER = "<your_oidc_username>";
const HOST_PLACEHOLDER = "<host>";

const OidcConnectModal = ({
  onClose,
  isOpen,
  auth,
}: {
  onClose: () => void;
  isOpen: boolean;
  auth: AuthContextProps;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const appConfig = useAppConfig();
  const idToken = auth.user?.id_token;

  const { sqlRole } = useSelfManagedProfile(auth);
  const userStr = sqlRole ?? OIDC_USERNAME_PLACEHOLDER;

  const obfuscated = idToken ? obfuscateSecret(idToken) : "";

  const balancerdHost =
    appConfig.mode === "self-managed"
      ? appConfig.balancerdDnsNames?.[0]
      : undefined;
  const sqlAddress = balancerdHost
    ? `${balancerdHost}:6875`
    : `${HOST_PLACEHOLDER}:6875`;

  return (
    <Modal size="3xl" isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader fontWeight="500">Connect To Materialize</ModalHeader>
        <ModalCloseButton />
        <ModalBody pt="2" pb="6" alignItems="stretch">
          <Text
            fontSize="sm"
            whiteSpace="normal"
            color={colors.foreground.secondary}
          >
            {!balancerdHost && (
              <>
                Replace <Code fontSize="xs">{HOST_PLACEHOLDER}</Code> with your
                Materialize SQL endpoint.{" "}
              </>
            )}
            See the{" "}
            <TextLink
              href="https://materialize.com/docs/self-managed-deployments/installation/install-on-gcp/#step-3-apply-the-terraform"
              isExternal
            >
              installation docs
            </TextLink>{" "}
            for full setup details. When prompted for a password, paste the ID
            token below.
          </Text>
          {idToken ? (
            <VStack alignItems="stretch" spacing={6} mt="4">
              <ConnectInstructions
                userStr={userStr}
                environmentdAddress={sqlAddress}
                oidcEnabled
              />
              <VStack alignItems="stretch" spacing={2}>
                <Text textStyle="heading-xs">ID Token</Text>
                <SecretCopyableBox
                  label="idToken"
                  contents={idToken}
                  obfuscatedContent={obfuscated}
                  overflow="hidden"
                  minWidth={0}
                />
                <Text fontSize="sm" color={colors.foreground.secondary}>
                  When prompted for a password, paste this ID token.
                </Text>
              </VStack>
            </VStack>
          ) : (
            <TabbedCodeBlock
              mt="4"
              tabs={[
                {
                  title: "MCP Server",
                  children: (
                    <McpConnectInstructions userStr={userStr} oidcEnabled />
                  ),
                  icon: <ConnectionIcon w="4" h="4" />,
                },
              ]}
              minHeight="208px"
            />
          )}
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default OidcConnectModal;
