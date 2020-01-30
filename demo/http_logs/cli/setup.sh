#!/bin/sh

# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

psql -h materialized -p 6875 -d materialize << EOF
-- Flask request logs format
CREATE SOURCE requests FROM 'file:///log/requests' WITH ( regex='(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -', tail=true);

-- Average number of product detail pages viewed per IP that has viewed the
-- search page at least once
CREATE VIEW avg_dps_for_searcher AS SELECT avg(dp_hits) FROM (SELECT ip, count(product_detail_id) AS dp_hits, count(search_kw) AS search_hits FROM requests GROUP BY ip) WHERE search_hits > 0;

-- Number of unique IP hits
-- This could just be SELECT count(distinct(ip)) FROM requests
-- once https://github.com/MaterializeInc/materialize/issues/676
-- is addressed.
CREATE VIEW unique_visitors AS SELECT count(*) FROM (SELECT ip FROM requests GROUP BY ip);

-- 40 products with the most hits
CREATE VIEW top_products AS SELECT count(product_detail_id) ct, product_detail_id from requests GROUP BY product_detail_id ORDER BY ct DESC LIMIT 40;
EOF
