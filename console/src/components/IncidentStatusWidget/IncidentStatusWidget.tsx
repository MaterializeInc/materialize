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
  chakra,
  CloseButton,
  HStack,
  Link,
  Text,
  ToastId,
  useTheme,
  useToast,
} from "@chakra-ui/react";
import { ErrorBoundary } from "@sentry/react";
import { parseISO } from "date-fns";
import { useAtomValue } from "jotai";
import React, {
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
} from "react";

import {
  EventTypes,
  isIncident,
  isInProgressMaintenance,
  isScheduledMaintenance,
} from "~/api/incident-io/types";
import useLocalStorage from "~/hooks/useLocalStorage";
import { useSummary } from "~/queries/incident-io";
import { currentRegionIdAtom } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { snakeToSentenceCase } from "~/util";
import {
  formatDate,
  formatDateInUtc,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { getHeadlineEvents, LastDismissedEvents } from "./utils";

const StatusEventToast = ({
  event,
  onClose,
}: {
  event: EventTypes;
  onClose: (
    incidentType: keyof LastDismissedEvents,
    incidentId: string,
  ) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  let bgColor = colors.background.tertiary;
  let category: string | JSX.Element = "Unknown";
  let incidentType: keyof LastDismissedEvents;
  if (isIncident(event)) {
    bgColor = colors.background.error;
    category = `${snakeToSentenceCase(event.current_worst_impact)}: ${event.status}`;
    incidentType = "ongoing_incident";
  } else if (isInProgressMaintenance(event)) {
    bgColor = colors.background.warn;
    category = "Maintenance in Progress";
    incidentType = "in_progress_maintenance";
  } else if (isScheduledMaintenance(event)) {
    const startDate = parseISO(event.starts_at);
    bgColor = colors.background.info;
    category = (
      <chakra.time
        dateTime={event.starts_at}
        title={formatDateInUtc(startDate, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
      >
        {formatDate(startDate, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
      </chakra.time>
    );
    incidentType = "scheduled_maintenance";
  }

  return (
    <Box p={3} color={colors.foreground.primary} bg={bgColor}>
      <HStack justifyContent="space-between">
        <Box>
          <Text
            textStyle="text-small-heavy"
            textTransform="uppercase"
            color={colors.foreground.secondary}
          >
            {category}
          </Text>
          <Text textStyle="heading-xs">{event.name}</Text>
        </Box>
        <CloseButton
          alignSelf="flex-start"
          onClick={() => onClose(incidentType, event.id)}
        />
      </HStack>
      <Text textStyle="text-ui-reg" mt={1}>
        {event.last_update_message}
      </Text>
      <Box textAlign="right">
        <Link href={event.url} target="_blank">
          Read more...
        </Link>
      </Box>
    </Box>
  );
};

function getDefaultDismissedEvents(): LastDismissedEvents {
  return {
    ongoing_incident: [],
    in_progress_maintenance: [],
    scheduled_maintenance: [],
  };
}

const IncidentStatusWidgetInner = () => {
  const toastIds = useRef<ToastId[]>([]);
  const [dismissedEvents, setDismissedEvents] = useLocalStorage(
    "mz-dismissed-incidents",
    getDefaultDismissedEvents(),
  );
  const toast = useToast({ duration: null, position: "bottom-right" });
  const currentRegionId = useAtomValue(currentRegionIdAtom);

  const onClose = useCallback(
    (type: keyof LastDismissedEvents, id: string) => {
      toast.close(id);
      toastIds.current = toastIds.current.filter((tid) => tid !== id);
      setDismissedEvents((value: LastDismissedEvents) => {
        const dismissed = { ...value, [type]: [id, ...value[type]] };
        if (dismissed[type].length > 10) {
          dismissed[type].splice(5);
        }
        return dismissed;
      });
    },
    [setDismissedEvents, toast],
  );

  const { data: summary } = useSummary();

  const events = useMemo(
    () => getHeadlineEvents(summary, dismissedEvents, currentRegionId),
    [summary, dismissedEvents, currentRegionId],
  );

  useEffect(() => {
    for (const toastId of toastIds.current) {
      // If the event has been resolved, or if we have a new headline event,
      // dismiss the active toast.
      if (!events.find((e) => e.id === toastId)) {
        toast.close(toastId);
        toastIds.current = toastIds.current.filter((tid) => tid !== toastId);
      }
    }
    for (const event of events) {
      if (!toast.isActive(event.id)) {
        const newToastId = toast({
          id: event.id,
          render: () => <StatusEventToast event={event} onClose={onClose} />,
        });
        toastIds.current.push(newToastId);
      }
    }
  }, [events, toast, onClose]);

  useEffect(
    () => () => {
      toastIds.current.map(toast.close);
    },
    [toast.close],
  );

  return null;
};

const IncidentStatusWidget = () => (
  <ErrorBoundary>
    <Suspense>
      <IncidentStatusWidgetInner />
    </Suspense>
  </ErrorBoundary>
);

export default IncidentStatusWidget;
