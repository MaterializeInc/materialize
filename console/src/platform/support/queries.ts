// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  infiniteQueryOptions,
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
  SupportTicket,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { useAuthUser } from "~/external-library-wrappers/frontegg";

/**
 * Tickets belong to the signed-in user, and the query cache outlives a sign
 * out, so the keys include who's signed in and to which organization.
 */
export type TicketOwner = { userId: string; organizationId: string };

export const supportTicketQueryKeys = {
  all: () => buildGlobalQueryKey("support-tickets"),
  owner: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.all(), owner] as const,
  list: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.owner(owner), "list"] as const,
  detail: (owner: TicketOwner, ticketId: string) =>
    [...supportTicketQueryKeys.owner(owner), "detail", ticketId] as const,
};

export const useTicketOwner = (): TicketOwner => {
  const user = useAuthUser();
  return { userId: user.id, organizationId: user.tenantId };
};

/**
 * Status codes that mean the ticket endpoints can't be used, rather than a
 * transient failure: 503 when Intercom isn't configured, 404 when the global
 * API doesn't have the endpoints yet.
 */
const UNAVAILABLE_STATUSES = [404, 503];

/** Whether an error means the ticket endpoints can't be used here. */
export const isTicketsUnavailableError = (error: unknown) =>
  error instanceof OpenApiFetchError &&
  UNAVAILABLE_STATUSES.includes(error.status);

/**
 * Retries network errors and server errors that may pass, but not 503 (not
 * configured) or 4xx responses such as 401, 403, 404 and 409, which won't
 * succeed on a retry and would only hold the error back.
 */
const retryTransientErrors = (failureCount: number, error: unknown) => {
  if (failureCount >= 2) return false;
  if (!(error instanceof OpenApiFetchError)) return true;
  return error.status >= 500 && error.status !== 503;
};

/**
 * Each request is a couple of Intercom calls, and every customer shares one
 * Intercom app rate limit, so tickets aren't refetched on every window focus
 * while they're this fresh.
 */
const TICKETS_STALE_TIME_MS = 60_000;

/**
 * How many empty pages in a row to skip before returning one anyway, so a
 * long run of hidden tickets can't keep a request going indefinitely.
 */
const MAX_EMPTY_PAGES = 5;

/**
 * The signed-in user's tickets, a page at a time. Shared by the page and by
 * the nav item, which prefetches it.
 *
 * The global API hides internal and unshared tickets after searching, so a
 * page can come back empty while there are more pages. Empty pages are
 * skipped here, so each page has tickets to show, unless it's the last one.
 */
export const supportTicketsQueryOptions = (owner: TicketOwner) =>
  infiniteQueryOptions({
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
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });

/**
 * The signed-in user's tickets. While this errors, the page falls back to the
 * Messenger and Help Center links, so it works before the endpoints are
 * deployed.
 */
export function useSupportTickets() {
  return useInfiniteQuery(supportTicketsQueryOptions(useTicketOwner()));
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
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });
}

/**
 * A ticket's row from the loaded list, so its title and state can show while
 * its replies load.
 */
export function useListedSupportTicket(
  ticketId: string | null,
): SupportTicket | undefined {
  const queryClient = useQueryClient();
  const owner = useTicketOwner();
  if (ticketId === null) return undefined;
  const list = queryClient.getQueryData(
    supportTicketsQueryOptions(owner).queryKey,
  );
  return list?.pages
    .flatMap((page) => page.data)
    .find((ticket) => ticket.id === ticketId);
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
