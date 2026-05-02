// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ModalBody, ModalContent, useSteps } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route, useLocation, useNavigate } from "react-router-dom";

import { FormTopBar, WizardStep } from "~/components/formComponentsV2";
import { Modal } from "~/components/Modal";
import { useMaxMySqlConnections } from "~/platform/connections/queries";
import { SentryRoutes } from "~/sentry";

import NewKafkaSourceContent from "./kafka/NewKafkaSourceContent";
import NewMySqlSourceContent from "./mysql/NewMySqlSourceContent";
import { NewWebhookSourceContent } from "./NewWebhookSource";
import NewPostgresSourceContent from "./postgres/NewPostgresSourceContent";
import SourceGallery from "./SourceGallery";
import NewSqlServerSourceContent from "./sqlserver/NewSqlServerSourceContent";

const SOURCE_STEPS: WizardStep[] = [{ id: "choose", label: "Create a source" }];

const SourceGalleryContent = ({
  initialSteps,
}: {
  initialSteps: WizardStep[];
}) => {
  const wizardSteps = useSteps({ index: 0, count: initialSteps.length });
  return (
    <>
      <FormTopBar
        title="New source"
        steps={SOURCE_STEPS}
        {...wizardSteps}
      ></FormTopBar>
      <ModalBody>
        <SourceGallery />
      </ModalBody>
    </>
  );
};

const NewSource = () => {
  const navigate = useNavigate();
  const { state: locationState } = useLocation();
  const previousPage = locationState?.previousPage ?? "../..";
  const { data: maxMySqlConnections } = useMaxMySqlConnections();
  const mySqlEnabled = (maxMySqlConnections ?? 0) > 0;
  return (
    <Modal
      isOpen
      onClose={() => navigate(previousPage)}
      size="full"
      closeOnEsc={false}
      scrollBehavior="inside"
    >
      <ModalContent>
        <SentryRoutes>
          <Route
            index
            element={<SourceGalleryContent initialSteps={SOURCE_STEPS} />}
          />
          <Route
            path="/webhook"
            element={<NewWebhookSourceContent initialSteps={SOURCE_STEPS} />}
          />
          <Route
            path="/postgres"
            element={<NewPostgresSourceContent initialSteps={SOURCE_STEPS} />}
          />
          <Route
            path="/sqlserver"
            element={<NewSqlServerSourceContent initialSteps={SOURCE_STEPS} />}
          />
          {mySqlEnabled && (
            <Route
              path="/mysql"
              element={<NewMySqlSourceContent initialSteps={SOURCE_STEPS} />}
            />
          )}
          <Route
            path="/kafka"
            element={<NewKafkaSourceContent initialSteps={SOURCE_STEPS} />}
          />
          <Route path="*" element={<Navigate to="../.." />} />
        </SentryRoutes>
      </ModalContent>
    </Modal>
  );
};

export default NewSource;
