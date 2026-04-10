// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ArrowBackIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  Center,
  Flex,
  HStack,
  IconButton,
  Spinner,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";

import { Sink } from "~/api/materialize/sink/sinkList";
import { Source } from "~/api/materialize/source/sourceList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import {
  monitorErrorsPath,
  monitorPath,
  shellPath,
} from "~/platform/routeHelpers";
import { useSinkList } from "~/platform/sinks/queries";
import SinkErrors from "~/platform/sinks/SinkErrors";
import SinkOverview from "~/platform/sinks/SinkOverview";
import { useSourcesList } from "~/platform/sources/queries";
import SourceErrors from "~/platform/sources/SourceErrors";
import { SourceOverview } from "~/platform/sources/SourceOverview";
import { useAllObjects } from "~/store/allObjects";
import { useRegionSlug } from "~/store/environments";
import type { MaterializeTheme } from "~/theme";

/** Overlay page for source/sink monitor. Reads object ID from URL params, tab from pathname. */
const ConnectorMonitorPage = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const { id } = useParams<{ id: string }>();
  const location = useLocation();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const { data: allObjects } = useAllObjects();
  const object = allObjects.find((o) => o.id === id);

  const isErrorsTab = location.pathname.endsWith("/errors");
  const isSource = object?.objectType === "source";
  const isSink = object?.objectType === "sink";

  const handleBack = React.useCallback(() => {
    navigate(shellPath(regionSlug));
  }, [navigate, regionSlug]);

  const handleTabChange = React.useCallback(
    (tab: "monitor" | "errors") => {
      if (!id) return;
      navigate(
        tab === "errors"
          ? monitorErrorsPath(regionSlug, id)
          : monitorPath(regionSlug, id),
        { replace: true },
      );
    },
    [navigate, regionSlug, id],
  );

  if (!id || !object) return null;

  return (
    <Flex direction="column" height="100%" overflow="hidden">
      <HStack
        px="4"
        py="2"
        borderBottomWidth="1px"
        borderColor={colors.border.primary}
        flexShrink={0}
        spacing="3"
      >
        <IconButton
          icon={<ArrowBackIcon />}
          aria-label="Back to worksheet"
          onClick={handleBack}
          variant="ghost"
          size="sm"
        />
        <Text fontWeight="medium" fontSize="md" noOfLines={1}>
          {object.name}
        </Text>
        <HStack spacing="1" ml="4">
          <Button
            size="xs"
            variant={!isErrorsTab ? "solid" : "ghost"}
            onClick={() => handleTabChange("monitor")}
          >
            Monitor
          </Button>
          <Button
            size="xs"
            variant={isErrorsTab ? "solid" : "ghost"}
            onClick={() => handleTabChange("errors")}
          >
            Errors
          </Button>
        </HStack>
      </HStack>

      <Box flex="1" overflow="auto">
        <AppErrorBoundary message="An error occurred loading connector details.">
          <React.Suspense
            fallback={
              <Center flex={1} py="20">
                <Spinner />
              </Center>
            }
          >
            {isSource && (
              <SourceContent
                objectId={id}
                showErrors={isErrorsTab}
                onClose={handleBack}
              />
            )}
            {isSink && (
              <SinkContent
                objectId={id}
                showErrors={isErrorsTab}
                onClose={handleBack}
              />
            )}
          </React.Suspense>
        </AppErrorBoundary>
      </Box>
    </Flex>
  );
};

const SourceContent = ({
  objectId,
  showErrors,
  onClose,
}: {
  objectId: string;
  showErrors: boolean;
  onClose: () => void;
}) => {
  const { data } = useSourcesList({});
  const source = data.rows.find((s: Source) => s.id === objectId);
  const hasNavigated = React.useRef(false);

  React.useEffect(() => {
    if (!source && !hasNavigated.current) {
      hasNavigated.current = true;
      onClose();
    }
  }, [source, onClose]);

  if (!source) return null;

  return showErrors ? (
    <SourceErrors source={source} />
  ) : (
    <SourceOverview source={source} />
  );
};

const SinkContent = ({
  objectId,
  showErrors,
  onClose,
}: {
  objectId: string;
  showErrors: boolean;
  onClose: () => void;
}) => {
  const { data } = useSinkList();
  const sink = data.rows.find((s: Sink) => s.id === objectId);
  const hasNavigated = React.useRef(false);

  React.useEffect(() => {
    if (!sink && !hasNavigated.current) {
      hasNavigated.current = true;
      onClose();
    }
  }, [sink, onClose]);

  if (!sink) return null;

  return showErrors ? <SinkErrors sink={sink} /> : <SinkOverview sink={sink} />;
};

export default ConnectorMonitorPage;
