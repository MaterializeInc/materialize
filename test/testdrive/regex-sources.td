# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-write path=request.log
123.17.127.5 - - [22/Jan/2020 18:59:52] "GET / HTTP/1.1" 200 -
8.15.119.56 - - [22/Jan/2020 18:59:52] "GET /detail/nNZpqxzR HTTP/1.1" 200 -
96.12.83.72 - - [22/Jan/2020 18:59:52] "GET /search/?kw=helper+ins+hennaed HTTP/1.1" 200 -

# Regex explained here: https://www.debuggex.com/r/k48kBEt-lTMUZbaw
> CREATE MATERIALIZED SOURCE regex_source FROM FILE '${testdrive.temp-dir}/request.log'
  FORMAT REGEX '(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -'

> SHOW COLUMNS FROM regex_source
Field              Nullable  Type
---------------------------------
ip                 YES       text
ts                 YES       text
path               YES       text
search_kw          YES       text
product_detail_id  YES       text
code               YES       text
mz_line_no         NO        int8

> SELECT * FROM regex_source ORDER BY mz_line_no
ip            ts                      path                                           search_kw           product_detail_id  code  mz_line_no
--------------------------------------------------------------------------------------------------------------------------------------------
123.17.127.5  "22/Jan/2020 18:59:52"  "GET / HTTP/1.1"                               <null>              <null>             200   1
8.15.119.56   "22/Jan/2020 18:59:52"  "GET /detail/nNZpqxzR HTTP/1.1"                <null>              nNZpqxzR           200   2
96.12.83.72   "22/Jan/2020 18:59:52"  "GET /search/?kw=helper+ins+hennaed HTTP/1.1"  helper+ins+hennaed  <null>             200   3

> SELECT search_kw FROM regex_source WHERE search_kw IS NOT NULL
search_kw
------------------
helper+ins+hennaed
