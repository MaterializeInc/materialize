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
import React from "react";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  attachSupportTicketFiles,
  createSupportTicket,
  CreateSupportTicketRequest,
  getSupportTicket,
  listSupportOrganizations,
  listSupportTickets,
  SupportTicket,
} from "~/api/cloudGlobalApi";
import { OpenApiFetchError } from "~/api/OpenApiFetchError";
import { useAuthUser } from "~/external-library-wrappers/frontegg";

import { readAsBase64 } from "./ticketForm";

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
  organizations: (owner: TicketOwner) =>
    [...supportTicketQueryKeys.owner(owner), "organizations"] as const,
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

/** Keep the first load small because Intercom returns full conversations. */
const INITIAL_TICKETS_PAGE_SIZE = 10;

/** The most tickets the global API returns in later pages. */
const TICKETS_PAGE_SIZE = 50;

/**
 * How many pages to load at most, so a user with a very long history can't
 * run up Intercom calls. The API returns the most recently updated first.
 */
const MAX_TICKET_PAGES = 10;

/**
 * The signed-in user's tickets, one API page per query page. Shared by the
 * page and by the nav item, which prefetches the first page.
 *
 * The global API hides internal and unshared tickets after searching, so a
 * page can come back with fewer tickets, or none, while there are more pages.
 */
export const supportTicketsQueryOptions = (owner: TicketOwner) =>
  infiniteQueryOptions({
    queryKey: supportTicketQueryKeys.list(owner),
    queryFn: async ({ pageParam, signal }) => {
      const { data } = await listSupportTickets(
        {
          cursor: pageParam,
          limit:
            pageParam === undefined
              ? INITIAL_TICKETS_PAGE_SIZE
              : TICKETS_PAGE_SIZE,
        },
        { signal },
      );
      return data;
    },
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage, allPages) =>
      allPages.length < MAX_TICKET_PAGES
        ? (lastPage.nextCursor ?? undefined)
        : undefined,
    // Retries one page at a time, so a failed page doesn't refetch the rest.
    retry: retryTransientErrors,
    staleTime: TICKETS_STALE_TIME_MS,
  });

/**
 * All of the signed-in user's tickets, so the table can filter and page
 * through them. The first page shows as soon as it arrives; the rest load
 * one after another until the cursor runs out or `MAX_TICKET_PAGES` is
 * reached. A page that fails stops the loading, keeps what loaded, and can be
 * retried with `query.fetchNextPage()`.
 *
 * While the first page errors, the page falls back to the Messenger and Help
 * Center links, so it works before the endpoints are deployed.
 */
export function useSupportTickets() {
  const query = useInfiniteQuery(supportTicketsQueryOptions(useTicketOwner()));
  const { hasNextPage, isFetching, isFetchNextPageError } = query;
  const { fetchNextPage, dataUpdatedAt } = query;
  const pages = query.data?.pages;
  // Wait for any fetch to finish, including a refresh of stale pages: by
  // default `fetchNextPage` cancels it and pages on from the old cursor.
  // `dataUpdatedAt` re-runs this after every fetch, since a fast one can land
  // without a render that shows it fetching.
  React.useEffect(() => {
    if (hasNextPage && !isFetching && !isFetchNextPageError) {
      void fetchNextPage({ cancelRefetch: false });
    }
  }, [
    dataUpdatedAt,
    hasNextPage,
    isFetching,
    isFetchNextPageError,
    fetchNextPage,
  ]);
  return {
    query,
    tickets: pages?.flatMap((page) => page.data),
    /** More pages are on their way. */
    isLoadingMore: hasNextPage && !isFetchNextPageError,
    /** A later page failed; the tickets before it are still listed. */
    isLoadMoreError: isFetchNextPageError,
    /** Stopped at `MAX_TICKET_PAGES` with more tickets left. */
    isTruncated: !hasNextPage && Boolean(pages?.at(-1)?.nextCursor),
  };
}

/** Organizations rarely change while the form is open. */
const ORGANIZATIONS_STALE_TIME_MS = 5 * 60_000;

/**
 * The organizations the user can open tickets for: the ones they belong to,
 * which the global API checks with Frontegg. Until the API has this endpoint,
 * it errors and the form stays on the signed-in organization.
 */
export function useSupportOrganizations() {
  return useQuery({
    queryKey: supportTicketQueryKeys.organizations(useTicketOwner()),
    queryFn: async ({ signal }) => {
      const { data } = await listSupportOrganizations({ signal });
      return data.data;
    },
    retry: retryTransientErrors,
    staleTime: ORGANIZATIONS_STALE_TIME_MS,
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
    onSuccess: ({ pagesOnCall: _, ...ticket }) => {
      const { queryKey } = supportTicketsQueryOptions(owner);
      // Intercom's ticket search takes a few seconds to include a new ticket,
      // so a refetch now would come back without it. Add it to the loaded
      // list and leave the updated cache fresh until its stale time expires.
      queryClient.setQueryData(queryKey, (list) => {
        if (!list) return list;
        const [first, ...rest] = list.pages;
        if (
          !first ||
          list.pages.some((page) =>
            page.data.some(({ id }) => id === ticket.id),
          )
        ) {
          return list;
        }
        return {
          ...list,
          pages: [{ ...first, data: [ticket, ...first.data] }, ...rest],
        };
      });
    },
  });
}

/**
 * Adds files to one of the user's tickets, as a reply from them. The files
 * are read and base64-encoded here, as the global API takes them.
 */
export function useAttachSupportTicketFiles() {
  return useMutation({
    mutationFn: async ({
      ticketId,
      files,
    }: {
      ticketId: string;
      files: File[];
    }) => {
      const encoded = await Promise.all(
        files.map(async (file) => ({
          name: file.name,
          data: await readAsBase64(file),
        })),
      );
      const { data } = await attachSupportTicketFiles(ticketId, encoded);
      return data;
    },
  });
}
