// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Column, MzDataType } from "~/api/materialize/types";
import { assert } from "~/util";
import { formatDateInUtc } from "~/utils/dateFormat";

const FIRST_AD_TIMESTAMP = new Date("0001-01-01T00:00:00Z").getTime();

function formatTimestamp(timestamp: unknown, withTz: boolean): string {
  let timestampNumber = 0;
  if (typeof timestamp === "number") {
    timestampNumber = timestamp;
  } else if (typeof timestamp === "string") {
    timestampNumber = parseInt(timestamp);
    if (isNaN(timestampNumber)) {
      return timestamp;
    }
  } else {
    return JSON.stringify(timestamp);
  }

  // Timestamps are always stored and transmitted in Materialize as UTC. The
  // only difference between the `timestamp without timezone` (`withTz = false`)
  // and `timestamp with timezone` (`withTz = true`) types is whether the time
  // zone indication is included in the format.
  //
  // A future version of Materialize may allow users to configure the
  // `timezone` session parameter, in which case `withTz = true` would need to
  // display the timestamp in the configured time zone. However, at present,
  // Materialize always uses `utc` is as the value for `timezone`, so we can
  // always format the time in UTC.

  let formatted = formatDateInUtc(timestampNumber, "yyyy-MM-dd HH:mm:ss");
  const milliseconds = formatDateInUtc(timestampNumber, "SSS").replace(
    /0*$/,
    "",
  );
  formatted =
    milliseconds.length > 0 ? `${formatted}.${milliseconds}` : formatted;
  if (withTz) {
    // Append UTC timezone
    formatted += "+00";
  }

  const isBcDate = timestampNumber < FIRST_AD_TIMESTAMP;

  if (isBcDate) {
    formatted += " BC";
  }
  return formatted;
}

/**
 * Formats an array for output as psql would.
 *
 * See `mz_repr::strconv::format_array` for the implementation in Materialize.
 */
function formatArray(
  vals: unknown,
  formatValue: (_: unknown) => string = formatCellDefault,
) {
  assert(Array.isArray(vals));

  let out = "{";
  let firstValue = true;
  for (const val of vals) {
    if (!firstValue) {
      out += ",";
    }

    // Format the value.
    const formattedVal = formatValue(val);

    // If the formatted value contains special characters, the value needs
    // to be escaped. Otherwise it can be emitted directly.
    if (!Array.isArray(val) && /[\s{},\\"]/.test(formattedVal)) {
      out += '"';
      // Backslash-escape any backslashes or double quotes inside the value.
      out += formattedVal.replace(/["|\\]/g, "\\$&");
      out += '"';
    } else {
      out += formattedVal;
    }

    firstValue = false;
  }
  out += "}";
  return out;
}

type FormatFn = (val: unknown) => string;

// TODO: this does not properly handle list types (e.g., `timestamp list`,
// `timestamptz list`, `jsonb list`). List types do not have stable OIDs and so
// we can't presently detect them.
const MZ_DATA_TYPE_TO_FORMAT_FN = new Map<number, FormatFn>([
  [MzDataType.timestamp, (t) => formatTimestamp(t, false)],
  [
    MzDataType._timestamp,
    (ts) => formatArray(ts, (t) => formatTimestamp(t, false)),
  ],
  [MzDataType.timestamptz, (t) => formatTimestamp(t, true)],
  [
    MzDataType._timestamptz,
    (ts) => formatArray(ts, (t) => formatTimestamp(t, true)),
  ],
  [MzDataType.jsonb, JSON.stringify],
  [MzDataType._jsonb, (js) => formatArray(js, JSON.stringify)],
]);

export default function formatRows(cols: Column[], rows: unknown[][]) {
  return rows.map((row) => {
    return row.map((cell, idx) => {
      const colType = cols[idx].type_oid;
      const formatFn = MZ_DATA_TYPE_TO_FORMAT_FN.get(colType);
      if (formatFn) {
        return formatFn(cell);
      }
      return formatCellDefault(cell);
    });
  });
}

function formatCellDefault(cell: unknown) {
  if (typeof cell === "string") {
    return cell;
  } else if (Array.isArray(cell)) {
    return formatArray(cell);
  } else {
    // TODO: handle record types (represented here as JavaScript objects) as
    // psql does. psql only renders `jsonb` data as JSON.
    return JSON.stringify(cell);
  }
}
