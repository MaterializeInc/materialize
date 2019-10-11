-- Copyright 2019 Materialize, Inc. All rights reserved.
--
-- This file is part of Materialize. Materialize may not be used or
-- distributed without the express permission of Materialize, Inc.

CREATE VIEW logs_records_per_dataflow AS
SELECT address_value AS dataflow, SUM(records) AS records
FROM logs_operates AS lo
JOIN logs_arrangement AS la
ON lo.id = la.operator
AND lo.worker = la.worker
AND lo.address_slot = 0
GROUP BY address_value;
