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
import { formatInTimeZone } from "date-fns-tz";

export const DATE_FORMAT = "MMM. dd, yyyy";
export const DATE_FORMAT_SHORT = "MM-dd-yy";
export const TIME_FORMAT = "HH:mm:ss";
export const TIME_FORMAT_NO_SECONDS = "HH:mm";
/**
 * Friendly date formats are used for displaying dates to the user. We should include the time zone
 * per date since using the same timezone for different dates close together can be incorrect due to
 * events like daylight savings.
 */
export const FRIENDLY_DATE_FORMAT = `${DATE_FORMAT} z`;
export const FRIENDLY_DATETIME_FORMAT = `${DATE_FORMAT} ${TIME_FORMAT} z`;
export const FRIENDLY_DATETIME_FORMAT_NO_SECONDS = `${DATE_FORMAT} HH:mm z`;
export const ISO_FORMAT = `yyyy-MM-dd'T'${TIME_FORMAT}'Z'`;

type FormatParameters = Parameters<typeof format>;
type FormatInTimezoneParameters = Parameters<typeof formatInTimeZone>;

/**
 *
 * Function used to format dates. It accepts a timezone
 * and if the format string includes it, will display it in localized form
 * (i.e. "America/New_York" will display as "EDT" or "EST" depending on the date).
 *
 * If the timezone is not valid, it will default to displaying the timezone as an offset of GMT (i.e. GMT-5).
 *
 * This function should be used instead of date-fns' `format`.
 */
export function formatDate(
  date: FormatParameters[0],
  formatString: FormatInTimezoneParameters[1],
  timezone: FormatInTimezoneParameters[2] = Intl.DateTimeFormat().resolvedOptions()
    .timeZone,
  options?: FormatInTimezoneParameters[3],
) {
  try {
    const formattedString = formatInTimeZone(
      date,
      timezone,
      formatString,
      options,
    );
    return formattedString;
  } catch {
    return format(date, formatString, options);
  }
}

export function formatDateInUtc(
  timestampOrDate: number | Date,
  formatString?: string,
) {
  return formatInTimeZone(timestampOrDate, "UTC", formatString ?? "yy-dd-MM");
}

export function formatUtcIso(timestampOrDate: number | Date) {
  return formatDateInUtc(timestampOrDate, ISO_FORMAT);
}

export function formatBrowserTimezone(formatString?: string) {
  return formatDate(new Date(), formatString ?? "z");
}
