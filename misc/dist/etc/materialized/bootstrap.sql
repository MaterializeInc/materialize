-- Copyright 2019 Materialize, Inc. All rights reserved.
--
-- This file is part of Materialize. Materialize may not be used or
-- distributed without the express permission of Materialize, Inc.

CREATE VIEW logs_dataflow_names AS
SELECT logs_addresses.id, logs_addresses.worker, logs_addresses.value as local_id, logs_operates.name as name
FROM
    logs_addresses,
    logs_operates
WHERE
    logs_addresses.id = logs_operates.id AND
    logs_addresses.worker = logs_operates.worker AND
    logs_addresses.slot = 0 AND
    logs_addresses.id IN (
        SELECT logs_addresses.id
        FROM
            logs_addresses
        GROUP BY logs_addresses.id
        HAVING count(*) = 1);

CREATE VIEW logs_records_per_dataflow AS
SELECT logs_dataflow_names.name AS dataflow, logs_dataflow_names.id as dataflow_id, SUM(records) AS records
FROM
    logs_addresses,
    logs_arrangement,
    logs_dataflow_names
WHERE
    logs_addresses.id = logs_arrangement.operator AND
    logs_addresses.worker = logs_arrangement.worker AND
    logs_addresses.value = logs_dataflow_names.local_id AND
    logs_addresses.worker = logs_dataflow_names.worker AND
    logs_addresses.slot = 0
GROUP BY logs_dataflow_names.name, logs_dataflow_names.id;
