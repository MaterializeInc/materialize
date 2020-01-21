#!/bin/sh
psql -h materialized -p 6875 sslmode=disable << EOF
-- Flask request logs format
create source requests from 'file:///log/requests' with ( regex='(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -', tail=true);

-- Average number of product detail pages viewed per IP that has viewed the 
-- search page at least once
CREATE VIEW avg_dps_for_searcher AS SELECT avg(dp_hits) FROM (SELECT ip, count(product_detail_id) AS dp_hits, count(search_kw) AS search_hits FROM requests GROUP BY ip) WHERE search_hits > 0;
EOF
