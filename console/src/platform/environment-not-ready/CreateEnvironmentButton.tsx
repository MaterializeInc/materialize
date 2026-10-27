// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  ButtonProps,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Spinner,
  Text,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom } from "jotai";
import React from "react";

import { hasEnvironmentWritePermission } from "~/api/auth";
import { Modal } from "~/components/Modal";
import { User } from "~/external-library-wrappers/__mocks__/frontegg";
import { useFlags } from "~/hooks/useFlags";
import { CreateRegion } from "~/platform/environment-not-ready/useCreateEnvironment";
import { numTotalEnvironmentsState } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import CreateEnvironmentWarning from "./CreateEnvironmentWarning";

interface Props extends ButtonProps {
  regionId: string | null;
  createRegion: CreateRegion;
  creatingRegionId?: string;
  tenantIsBlocked?: boolean;
  user: User;
}

const CreateEnvironmentModal = ({
  regionId,
  createRegion,
  onClose,
}: {
  regionId: string | null;
  createRegion: CreateRegion;
  onClose: () => void;
}) => {
  const { shadows, colors } = useTheme<MaterializeTheme>();

  const onClick = () => {
    createRegion(regionId!);
    onClose();
  };

  return (
    <Modal isOpen onClose={onClose}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <ModalHeader p="4" borderBottom={`1px solid ${colors.border.primary}`}>
          Enable {regionId} region
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <VStack spacing="4">
            <Text
              textStyle="text-base"
              color={colors.foreground.primary}
              lineHeight="20px"
            >
              Are you sure you want to enable your <strong>{regionId}</strong>{" "}
              region?
            </Text>
            <CreateEnvironmentWarning />
          </VStack>
        </ModalBody>
        <ModalFooter>
          <Button type="submit" variant="primary" size="sm" onClick={onClick}>
            Enable region
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

/*
 * Button that creates an environment for given region.
 * Creation is handled by the useCreateEnvironment hook,
 * createRegion and creatingRegionId should be passed down from there.
 * Has some default button styling but you can override it with whatever.
 */
const CreateEnvironmentButton = (props: Props) => {
  const flags = useFlags();
  const canWriteEnvironments = hasEnvironmentWritePermission(props.user);
  const [numTotalEnvironments] = useAtom(numTotalEnvironmentsState);
  const {
    isOpen: isConfirmOpen,
    onOpen: onConfirmOpen,
    onClose: onConfirmClose,
  } = useDisclosure();
  const {
    regionId: selectedRegionId,
    creatingRegionId,
    createRegion,
    tenantIsBlocked,
    ...buttonProps
  } = props;

  const creatingThisRegion = creatingRegionId === selectedRegionId;
  const exceededMaxEnvironments =
    numTotalEnvironments !== undefined &&
    numTotalEnvironments >= flags["max-environments"];

  const onClick = () => {
    // Unless this is the first region for the organization, ask for additional
    // confirmation that the user intends to enable the region, to minimize the
    // chance of a user accidentally accruing charges for an unnecessary region.
    if (numTotalEnvironments === 0) {
      createRegion(selectedRegionId!);
    } else {
      onConfirmOpen();
    }
  };

  return (
    <>
      <Button
        alignSelf="flex-end"
        {...buttonProps}
        isLoading={creatingThisRegion}
        spinner={<Spinner size="sm" />}
        variant="primary"
        isDisabled={
          selectedRegionId == null ||
          !canWriteEnvironments ||
          !!creatingThisRegion ||
          !!tenantIsBlocked ||
          exceededMaxEnvironments
        }
        onClick={onClick}
        title={
          !canWriteEnvironments
            ? "Only admins can enable new regions."
            : exceededMaxEnvironments
              ? "You have already enabled the maximum allowed number of regions for your account. Contact support to raise your limit."
              : selectedRegionId
                ? `Enable ${selectedRegionId}`
                : "Please select a region"
        }
      >
        Get started with Materialize
      </Button>
      {isConfirmOpen && (
        <CreateEnvironmentModal
          regionId={selectedRegionId}
          createRegion={createRegion}
          onClose={onConfirmClose}
        />
      )}
    </>
  );
};

export default CreateEnvironmentButton;
