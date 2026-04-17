// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
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
import { useAuth } from "~/external-library-wrappers/oidc";
import ConnectionIcon from "~/svg/ConnectionIcon";
import { MaterializeTheme } from "~/theme";
import { obfuscateSecret } from "~/utils/format";

const OIDC_USERNAME_PLACEHOLDER = "<your_oidc_username>";

const OidcConnectModal = ({
  onClose,
  isOpen,
}: {
  onClose: () => void;
  isOpen: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const auth = useAuth();
  const idToken = auth.user?.id_token;

  const obfuscated = idToken ? obfuscateSecret(idToken) : "";

  // Default to the console's hostname, but customers may need to adjust
  // if Materialize is exposed on a different host behind a load balancer.
  const defaultSqlAddress = `${window.location.hostname}:6875`;

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
            Use the details below to connect to Materialize. The host and port
            shown are defaults and may need to be adjusted for your deployment.
            When prompted for a password, paste the ID token.
          </Text>
          {idToken ? (
            <VStack alignItems="stretch" spacing={6} mt="4">
              <ConnectInstructions
                userStr={OIDC_USERNAME_PLACEHOLDER}
                environmentdAddress={defaultSqlAddress}
                psqlQueryParams="options=--oidc_auth_enabled%3Dtrue"
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
            <VStack alignItems="stretch" spacing={4} mt="4">
              <Text fontSize="sm">
                No ID token available. Please sign in again to access SQL
                connection details.
              </Text>
              <TabbedCodeBlock
                tabs={[
                  {
                    title: "MCP Server",
                    children: (
                      <McpConnectInstructions
                        userStr={OIDC_USERNAME_PLACEHOLDER}
                      />
                    ),
                    icon: <ConnectionIcon w="4" h="4" />,
                  },
                ]}
                minHeight="208px"
              />
            </VStack>
          )}
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default OidcConnectModal;
