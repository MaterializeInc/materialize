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
import { SecretCopyableBox } from "~/components/copyableComponents";
import { Modal } from "~/components/Modal";
import { useAuth } from "~/external-library-wrappers/oidc";
import { MaterializeTheme } from "~/theme";

/** Decode the payload of a JWT without verifying the signature. */
function decodeJwtPayload(token: string): Record<string, unknown> {
  const [, payload] = token.split(".");
  return JSON.parse(atob(payload));
}

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

  const username = React.useMemo(() => {
    if (!idToken) return "";
    const claims = decodeJwtPayload(idToken);
    return (claims.email as string) ?? (claims.sub as string) ?? "";
  }, [idToken]);

  const obfuscated = idToken ? "*".repeat(Math.min(idToken.length, 40)) : "";

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
                userStr={username}
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
            <Text fontSize="sm" mt="4">
              No ID token available. Please sign in again.
            </Text>
          )}
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default OidcConnectModal;
