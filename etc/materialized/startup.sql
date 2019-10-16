-- Copyright 2019 Materialize, Inc. All rights reserved.
--
-- This file is part of Materialize. Materialize may not be used or
-- distributed without the express permission of Materialize, Inc.

CREATE VIEW logs_records_per_dataflow AS
SELECT logs_addresses.value AS dataflow, SUM(records) AS records
FROM
    logs_operates,
    logs_addresses,
    logs_arrangement
WHERE
    logs_operates.id = logs_arrangement.operator AND
    logs_operates.worker = logs_arrangement.worker AND
    logs_addresses.id = logs_operates.id AND
    logs_addresses.worker = logs_addresses.worker AND
    logs_addresses.slot = 0
GROUP BY logs_addresses.value;
