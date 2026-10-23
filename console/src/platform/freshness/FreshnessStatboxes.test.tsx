// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";
import { describe, expect, it } from "vitest";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
} from "~/api/mocks/buildSqlQueryHandler";
import { mapKyselyToTabular } from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { renderComponent } from "~/test/utils";
import { formatDurationExact } from "~/utils/format";

import {
  FreshnessStatboxes,
  FreshnessStatboxesProps,
} from "./FreshnessStatboxes";
import { freshnessQueryKeys } from "./queries";

const OBJECT_IDS = ["u1", "u2", "u3"];
const LOOKBACK_MS = 86_400_000;

const PERCENTILE_COLUMNS = buildColumns(
  ["peak", "p90", "p99", "p99_9"].map((name) => ({
    name,
    type_oid: MzDataType.interval,
  })),
);

type Lag = string | null;

const mockPercentiles = (
  row: { peak: Lag; p90: Lag; p99: Lag; p99_9: Lag } | undefined,
) =>
  server.use(
    buildSqlQueryHandlerV2({
      queryKey: freshnessQueryKeys.scopeLagPercentiles({
        objectIds: OBJECT_IDS,
        lookbackMs: LOOKBACK_MS,
      }),
      results: mapKyselyToTabular({
        columns: PERCENTILE_COLUMNS,
        rows: row ? [row] : [],
      }),
    }),
  );

const render = (props: Partial<FreshnessStatboxesProps> = {}) =>
  renderComponent(
    <FreshnessStatboxes
      objectIds={OBJECT_IDS}
      lookbackMs={LOOKBACK_MS}
      latestPeak={undefined}
      {...props}
    />,
  );

describe("FreshnessStatboxes", () => {
  it("shows each percentile of the scope's minutes", async () => {
    mockPercentiles({
      peak: "00:00:14.8",
      p90: "00:00:00.82",
      p99: "00:00:02.4",
      p99_9: "00:00:06.1",
    });
    await render();

    expect(await screen.findByText(formatDurationExact(820))).toBeVisible();
    expect(screen.getByText(formatDurationExact(2_400))).toBeVisible();
    expect(screen.getByText(formatDurationExact(6_100))).toBeVisible();
    expect(screen.getByText(formatDurationExact(14_800))).toBeVisible();
  });

  it("raises pMAX to a newer minute the percentiles have not caught up with", async () => {
    mockPercentiles({
      peak: "00:00:03",
      p90: "00:00:00.4",
      p99: "00:00:01",
      p99_9: "00:00:03",
    });
    await render({ latestPeak: 40_000 });

    expect(await screen.findByText(formatDurationExact(40_000))).toBeVisible();
    // p99.9 matched the old pMAX, not the new one.
    expect(
      screen.queryByText("Same as pMAX at this range"),
    ).not.toBeInTheDocument();
  });

  it("shows a newer minute as pMAX before the range has any readings", async () => {
    mockPercentiles(undefined);
    await render({ latestPeak: 5_000 });

    expect(await screen.findByText(formatDurationExact(5_000))).toBeVisible();
    expect(screen.getAllByText("No readings in this range")).toHaveLength(3);
  });

  it("says how many minutes could not be read", async () => {
    mockPercentiles({
      peak: null,
      p90: "00:00:00.4",
      p99: null,
      p99_9: null,
    });
    await render();

    expect(
      await screen.findByText("At least one minute could not be read"),
    ).toBeVisible();
    expect(
      screen.getByText("More than 1% of minutes could not be read"),
    ).toBeVisible();
    expect(screen.getAllByText(NULL_LAG_TEXT)).toHaveLength(3);
  });

  it("says when a percentile only repeats pMAX", async () => {
    mockPercentiles({
      peak: "00:00:03",
      p90: "00:00:00.4",
      p99: "00:00:01",
      p99_9: "00:00:03",
    });
    await render();

    expect(await screen.findByText("Same as pMAX at this range")).toBeVisible();
  });

  it("says so when the scope has no readings", async () => {
    mockPercentiles(undefined);
    await render();

    expect(
      await screen.findAllByText("No readings in this range"),
    ).toHaveLength(4);
  });

  it("says so when the request fails", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: freshnessQueryKeys.scopeLagPercentiles({
          objectIds: OBJECT_IDS,
          lookbackMs: LOOKBACK_MS,
        }),
        results: {
          error: { message: "boom", code: ErrorCode.INTERNAL_ERROR },
          notices: [],
        },
      }),
    );
    await render();

    expect(
      await screen.findByText(
        "An error occurred loading the summary percentiles.",
      ),
    ).toBeVisible();
  });
});
