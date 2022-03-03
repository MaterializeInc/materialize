# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# If mz_persist::gen::ProtoMeta::ENCODING_VERSION is changed, the version of this file needs to change to "current_source"

> SELECT f1 FROM persistence_timestamp_with_time_zone;
"2007-02-01 09:04:05.123456 UTC"

> SELECT f1::text FROM persistence_nested_array;
"{{a,\"white space\"},{NULL,\"\"},{\"escape\\\"m\\\\e\",nUlL}}"

> SELECT f1 FROM persistence_decimal;
1234567890123456.1234567890123456

> SELECT f1 FROM persistence_double;
1234.5678

> SELECT f1::text FROM persistence_bigint_list_table;
{0,1}
{9223372036854775807,-9223372036854775808}

> SELECT f1::text FROM persistence_bigint_list_list_table;
{{9223372036854775807,-9223372036854775808}}

> SELECT f1::text FROM persistence_bigint_map_table;
{abcdef=>9223372036854775807}

> SELECT f1::text FROM persistence_bigint_map_map_table;
"{abcdef=>{\"abc def\"=>-9223372036854775808}}"
