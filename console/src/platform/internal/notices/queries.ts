// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  DetailFilters,
  fetchNoticeDetail,
} from "~/api/materialize/notice/fetchNotice";
import {
  fetchNoticesList,
  ListFilters,
} from "~/api/materialize/notice/fetchNoticesList";

export const noticeQueryKeys = {
  all: () => buildRegionQueryKey("notices"),
  list: ({ databaseId, schemaId, nameFilter }: ListFilters = {}) =>
    [
      ...noticeQueryKeys.all(),
      buildQueryKeyPart("list", {
        databaseId: databaseId ?? "",
        schemaId: schemaId ?? "",
        nameFilter: nameFilter ?? "",
      }),
    ] as const,
  details: (detailFilters: DetailFilters) =>
    [...noticeQueryKeys.all(), detailFilters] as const,
};

export function useNoticesListPage(filters: ListFilters) {
  return useQuery({
    queryKey: noticeQueryKeys.list(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchNoticesList({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
  });
}

export function useNoticeDetailPage(filters: DetailFilters) {
  return useQuery({
    queryKey: noticeQueryKeys.details(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, detailFilters] = queryKey;
      return fetchNoticeDetail({
        queryKey,
        filters: detailFilters,
        requestOptions: { signal },
      });
    },
  });
}
