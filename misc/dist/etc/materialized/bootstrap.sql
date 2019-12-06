-- Copyright 2019 Materialize, Inc. All rights reserved.
--
-- This file is part of Materialize. Materialize may not be used or
-- distributed without the express permission of Materialize, Inc.

-- Stores all addresses that only have one slot (0) in mz_dataflow_operator_addresses
-- The resulting addresses are either channels or dataflows
CREATE VIEW logs_unit_length_addresses as
SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
FROM
    mz_dataflow_operator_addresses
GROUP BY
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker
HAVING count(*) = 1;

-- Maintains a list of the current dataflow operator ids, and their corresponding operator names and local ids (per worker)
CREATE VIEW logs_dataflow_names AS
SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.value as local_id,
    mz_dataflow_operators.name
FROM
    mz_dataflow_operator_addresses,
    mz_dataflow_operators,
    logs_unit_length_addresses
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_dataflow_operator_addresses.id = logs_unit_length_addresses.id AND
    mz_dataflow_operator_addresses.worker = logs_unit_length_addresses.worker AND
    mz_dataflow_operator_addresses.slot = 0;

-- Maintains a list of all operators bound to a dataflow and their corresponding names and dataflow names and ids (per worker)
-- Keeping this as a separate view instead of rolling it into logs_records_per_dataflow_operator to simplify logic
CREATE VIEW logs_dataflow_operators AS
SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    logs_dataflow_names.id as dataflow_id,
    logs_dataflow_names.name as dataflow_name
FROM
    mz_dataflow_operators,
    mz_dataflow_operator_addresses,
    logs_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_operator_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_operator_addresses.worker AND
    mz_dataflow_operator_addresses.slot = 0 AND
    logs_dataflow_names.local_id = mz_dataflow_operator_addresses.value AND
    logs_dataflow_names.worker = mz_dataflow_operator_addresses.worker;

-- Maintains the number of records used by each operator in a dataflow (per worker)
-- Operators not using any records are not shown
CREATE VIEW logs_records_per_dataflow_operator AS
SELECT
    logs_dataflow_operators.id,
    logs_dataflow_operators.name,
    logs_dataflow_operators.worker,
    logs_dataflow_operators.dataflow_id,
    mz_arrangement_sizes.records
FROM
    mz_arrangement_sizes,
    logs_dataflow_operators
WHERE
    logs_dataflow_operators.id = mz_arrangement_sizes.operator AND
    logs_dataflow_operators.worker = mz_arrangement_sizes.worker;

-- Maintains the number of records used by each dataflow (per worker)
CREATE VIEW logs_records_per_dataflow AS
SELECT
    logs_records_per_dataflow_operator.dataflow_id as id,
    logs_dataflow_names.name,
    logs_records_per_dataflow_operator.worker,
    SUM(logs_records_per_dataflow_operator.records) as records
FROM
    logs_records_per_dataflow_operator,
    logs_dataflow_names
WHERE
    logs_records_per_dataflow_operator.dataflow_id = logs_dataflow_names.id AND
    logs_records_per_dataflow_operator.worker = logs_dataflow_names.worker
GROUP BY
    logs_records_per_dataflow_operator.dataflow_id,
    logs_dataflow_names.name,
    logs_records_per_dataflow_operator.worker;

-- Maintains the number of records used by each dataflow (across all workers)
CREATE VIEW logs_records_per_dataflow_global AS
SELECT
    logs_records_per_dataflow.id,
    logs_records_per_dataflow.name,
    SUM(logs_records_per_dataflow.records) as records
FROM
    logs_records_per_dataflow
GROUP BY
    logs_records_per_dataflow.id,
    logs_records_per_dataflow.name;
