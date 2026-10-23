// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  createSupportTicket,
  CreateSupportTicketRequest,
  getSupportTicket,
  listSupportTickets,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";

export const supportTicketQueryKeys = {
  all: () => buildGlobalQueryKey("support-tickets"),
  list: () => [...supportTicketQueryKeys.all(), "list"] as const,
  detail: (ticketId: string) =>
    [...supportTicketQueryKeys.all(), "detail", ticketId] as const,
};

/**
 * Status codes that mean the ticket endpoints can't be used, rather than a
 * transient failure: 503 when Intercom isn't configured, 404 when the global
 * API doesn't have the endpoints yet. Retrying won't help.
 */
const UNAVAILABLE_STATUSES = [404, 503];

const retryUnlessUnavailable = (failureCount: number, error: unknown) =>
  !(
    error instanceof OpenApiFetchError &&
    UNAVAILABLE_STATUSES.includes(error.status)
  ) && failureCount < 2;

/**
 * The signed-in user's tickets. While this errors, the page falls back to
 * the Messenger and Help Center links, so it works before the endpoints are
 * deployed.
 */
export function useSupportTickets() {
  return useQuery({
    queryKey: supportTicketQueryKeys.list(),
    queryFn: async ({ signal }) => {
      const { data } = await listSupportTickets({ signal });
      return data;
    },
    retry: retryUnlessUnavailable,
    refetchOnWindowFocus: true,
  });
}

export function useSupportTicket(ticketId: string | null) {
  return useQuery({
    queryKey: supportTicketQueryKeys.detail(ticketId ?? ""),
    queryFn: async ({ signal }) => {
      const { data } = await getSupportTicket(ticketId ?? "", { signal });
      return data;
    },
    enabled: ticketId !== null,
    retry: retryUnlessUnavailable,
  });
}

export function useCreateSupportTicket() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (ticket: CreateSupportTicketRequest) => {
      const { data } = await createSupportTicket(ticket);
      return data;
    },
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: supportTicketQueryKeys.list(),
      }),
  });
}
