// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  createSupportTicket,
  CreateSupportTicketRequest,
  getSupportTicket,
  listSupportTickets,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { useAuthUser } from "~/external-library-wrappers/frontegg";

/**
 * Tickets belong to the signed-in user, and the query cache outlives a sign
 * out, so the keys include who's signed in and to which organization.
 */
type TicketOwner = { userId: string; organizationId: string };

export const supportTicketQueryKeys = {
  all: () => buildGlobalQueryKey("support-tickets"),
  owner: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.all(), owner] as const,
  list: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.owner(owner), "list"] as const,
  detail: (owner: TicketOwner, ticketId: string) =>
    [...supportTicketQueryKeys.owner(owner), "detail", ticketId] as const,
};

const useTicketOwner = (): TicketOwner => {
  const user = useAuthUser();
  return { userId: user.id, organizationId: user.tenantId };
};

/**
 * Status codes that mean the ticket endpoints can't be used, rather than a
 * transient failure: 503 when Intercom isn't configured, 404 when the global
 * API doesn't have the endpoints yet. Retrying won't help.
 */
const UNAVAILABLE_STATUSES = [404, 503];

/** Whether an error means the ticket endpoints can't be used here. */
export const isTicketsUnavailableError = (error: unknown) =>
  error instanceof OpenApiFetchError &&
  UNAVAILABLE_STATUSES.includes(error.status);

const retryUnlessUnavailable = (failureCount: number, error: unknown) =>
  !isTicketsUnavailableError(error) && failureCount < 2;

/**
 * How many empty pages in a row to skip before returning one anyway, so a
 * long run of hidden tickets can't keep a request going indefinitely.
 */
const MAX_EMPTY_PAGES = 5;

/**
 * The signed-in user's tickets, a page at a time. While this errors, the page
 * falls back to the Messenger and Help Center links, so it works before the
 * endpoints are deployed.
 *
 * The global API hides internal and unshared tickets after searching, so a
 * page can come back empty while there are more pages. Empty pages are
 * skipped here, so each page has tickets to show, unless it's the last one.
 */
export function useSupportTickets() {
  const owner = useTicketOwner();
  return useInfiniteQuery({
    queryKey: supportTicketQueryKeys.list(owner),
    queryFn: async ({ pageParam, signal }) => {
      let { data } = await listSupportTickets(
        { cursor: pageParam },
        { signal },
      );
      for (
        let skipped = 0;
        data.data.length === 0 && data.nextCursor && skipped < MAX_EMPTY_PAGES;
        skipped++
      ) {
        ({ data } = await listSupportTickets(
          { cursor: data.nextCursor },
          { signal },
        ));
      }
      return data;
    },
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.nextCursor ?? undefined,
    retry: retryUnlessUnavailable,
    refetchOnWindowFocus: true,
  });
}

export function useSupportTicket(ticketId: string | null) {
  const owner = useTicketOwner();
  return useQuery({
    queryKey: supportTicketQueryKeys.detail(owner, ticketId ?? ""),
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
  const owner = useTicketOwner();
  return useMutation({
    mutationFn: async (ticket: CreateSupportTicketRequest) => {
      const { data } = await createSupportTicket(ticket);
      return data;
    },
    onSuccess: () =>
      queryClient.invalidateQueries({
        queryKey: supportTicketQueryKeys.list(owner),
      }),
  });
}
