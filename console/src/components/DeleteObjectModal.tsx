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
  Flex,
  FormControl,
  FormErrorMessage,
  FormLabel,
  Input,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import React from "react";
import { useForm } from "react-hook-form";

import { useSegment } from "~/analytics/segment";
import { useSqlLazy } from "~/api/materialize";
import {
  buildDropObjectStatement,
  DeletableObjectType,
  supportsDropCascade,
} from "~/api/materialize/buildDropObjectStatement";
import { DatabaseObject } from "~/api/materialize/types";
import useObjectDependencies from "~/api/materialize/useObjectDependencies";
import { Modal } from "~/components/Modal";
import { useToast } from "~/hooks/useToast";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import Alert from "./Alert";
import { ObjectToastDescription } from "./Toast";

export interface DeleteObjectModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
  dbObject: DatabaseObject;
  objectType: DeletableObjectType;
  message?: string;
}

const DeleteObjectModal = ({
  isOpen,
  onClose,
  onSuccess,
  dbObject,
  objectType,
  message,
}: DeleteObjectModalProps) => {
  const { shadows, colors } = useTheme<MaterializeTheme>();
  const toast = useToast();
  const { track } = useSegment();
  const [showConfirmation, setShowConfirmation] = React.useState(false);
  const { register, handleSubmit, formState } = useForm<{
    objectName: string;
  }>({
    mode: "onTouched",
  });

  const { loading: dependencyCountLoading, results: dependencyCount } =
    useObjectDependencies(dbObject.id, objectType);
  const {
    runSql: deleteObject,
    loading: isDeleting,
    error,
  } = useSqlLazy({
    queryBuilder: buildDropObjectStatement,
  });

  const handleDelete = () => {
    track("Delete object clicked", { name: dbObject.name });
    deleteObject(
      { dbObject, objectType },
      {
        onSuccess: () => {
          onClose();
          onSuccess();
          toast({
            description: (
              <ObjectToastDescription
                name={dbObject.name}
                message="dropped successfully"
              />
            ),
          });
        },
        onError: (errorMessage) => {
          Sentry.captureException(
            new Error("Drop object error: " + errorMessage),
          );
        },
      },
    );
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <form onSubmit={handleSubmit(handleDelete)}>
          <ModalHeader
            p="4"
            borderBottom={`1px solid ${colors.border.primary}`}
          >
            Drop {dbObject.name}
          </ModalHeader>
          <ModalCloseButton />
          {dependencyCountLoading || dependencyCount === null ? (
            <>
              <ModalBody>
                <Flex width="100%" justifyContent="center">
                  <Spinner />
                </Flex>
              </ModalBody>
            </>
          ) : showConfirmation ||
            !supportsDropCascade(objectType) ||
            dependencyCount === BigInt(0) ? (
            <>
              <ModalBody pb="6">
                <VStack spacing="4" width="100%">
                  {error && (
                    <Alert
                      variant="error"
                      message={`There was an error deleting the object: ${error}.`}
                    />
                  )}
                  {message && <Text textStyle="text-base">{message}</Text>}
                  <FormControl isInvalid={!!formState.errors.objectName}>
                    <FormLabel>
                      To confirm, type {dbObject.name} below
                    </FormLabel>
                    <Input
                      autoFocus
                      placeholder={dbObject.name}
                      variant={
                        formState.errors.objectName ? "error" : "default"
                      }
                      {...register("objectName", {
                        required: "Object name is required.",
                        validate: (value) => {
                          if (value !== dbObject.name) {
                            return "Object name must match exactly.";
                          }
                        },
                      })}
                    />
                    <FormErrorMessage>
                      {formState.errors.objectName?.message}
                    </FormErrorMessage>
                  </FormControl>
                  <Text
                    textStyle="text-base"
                    color={colors.foreground.secondary}
                  >
                    {dependencyCount === BigInt(0) ? (
                      <>
                        This action will permanently drop {dbObject.name} and
                        can not be undone.
                      </>
                    ) : (
                      <>
                        This will permanently drop {dbObject.name} and all
                        sources, materialized views, views, indexes, and sinks
                        that depend on it.
                      </>
                    )}
                  </Text>
                </VStack>
              </ModalBody>
              <ModalFooter>
                <Button
                  type="submit"
                  colorScheme="red"
                  size="sm"
                  width="100%"
                  isDisabled={isDeleting}
                >
                  Drop {objectType.toLowerCase()}
                </Button>
              </ModalFooter>
            </>
          ) : (
            <>
              <ModalBody pb="10">
                <VStack spacing="4">
                  <Alert
                    width="100%"
                    showLabel={false}
                    variant="warning"
                    message={
                      <Text as="span" textStyle="text-ui-reg">
                        The {objectType.toLowerCase()}{" "}
                        <Text as="span" textStyle="text-ui-med">
                          {dbObject.name}
                        </Text>{" "}
                        has {dependencyCount.toString()}{" "}
                        {pluralize(dependencyCount, "dependent", "dependents")}
                      </Text>
                    }
                    data-testid="dependents-warning"
                  />
                  <Text textStyle="text-base" color={colors.foreground.primary}>
                    This {objectType.toLowerCase()} is used by other objects. In
                    order to drop {dbObject.name}, all its dependents will be
                    dropped as well.
                  </Text>
                </VStack>
              </ModalBody>
              <ModalFooter>
                <Button
                  variant="secondary"
                  size="sm"
                  width="100%"
                  isDisabled={isDeleting}
                  type="button"
                  onClick={(e) => {
                    e.preventDefault();
                    // Since this button is sometimes a child of a clickable row
                    e.stopPropagation();
                    setShowConfirmation(true);
                  }}
                >
                  Yes, I am sure I want to drop all dependents
                </Button>
              </ModalFooter>
            </>
          )}
        </form>
      </ModalContent>
    </Modal>
  );
};

export default DeleteObjectModal;
