// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// eslint-disable-next-line no-restricted-imports
import { format } from "date-fns";

import { formatDate } from "./dateFormat";

describe("formatDate", () => {
  it("should format the date in the specified timezone", () => {
    const date = new Date("2022-01-01T12:00:00Z");
    const formatString = "MMM dd, yyyy HH:mm:ss z";
    const timezone = "America/Los_Angeles";
    const formattedDate = formatDate(date, formatString, timezone);

    expect(formattedDate).toBe("Jan 01, 2022 04:00:00 PST");
  });

  it("should format the date in a European timezone", () => {
    const date = new Date("2022-01-01T12:00:00Z");
    const formatString = "MMM dd, yyyy HH:mm:ss z";
    const timezone = "Europe/Paris";
    const formattedDate = formatDate(date, formatString, timezone);

    // For some reason France doesn't use CDT/CET but follows the GMT+X pattern
    expect(formattedDate).toBe("Jan 01, 2022 13:00:00 GMT+1");
  });

  it("should default to using date-fn's `format` to display the timezone if timezone is invalid, which is an offset of GMT using the local browser time", () => {
    const date = new Date("2022-01-01T12:00:00Z");
    const formatString = "MMM dd, yyyy HH:mm:ss z";
    const invalidTimezone = "Invalid/Timezone";
    const formattedDate = formatDate(date, formatString, invalidTimezone);

    expect(formattedDate).toBe(format(date, formatString));
  });
});
