# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# This test only verifies that the log relations are published, not that they
# have any specific output.

> CREATE VIEW count_operates AS SELECT count(*) FROM mz_dataflow_operators;
> SELECT count(*) FROM count_operates;
1

> CREATE VIEW count_channels AS SELECT count(*) FROM mz_dataflow_channels;
> SELECT count(*) FROM count_channels;
1

> CREATE VIEW count_elapsed AS SELECT count(*) FROM mz_scheduling_elapsed;
> SELECT count(*) FROM count_elapsed;
1

> CREATE VIEW count_histogram AS SELECT count(*) FROM mz_scheduling_histogram;
> SELECT count(*) FROM count_histogram;
1

> CREATE VIEW count_arrangement AS SELECT count(*) FROM mz_arrangement_size;
> SELECT count(*) FROM count_arrangement;
1

> CREATE VIEW count_dataflows AS SELECT count(*) FROM mz_views;
> SELECT count(*) FROM count_dataflows;
1

> CREATE VIEW count_frontiers AS SELECT count(*) FROM mz_view_frontier;
> SELECT count(*) FROM count_frontiers;
1

> CREATE VIEW count_peeks AS SELECT count(*) FROM mz_peek_active;
> SELECT count(*) FROM count_peeks;
1

> CREATE VIEW count_peek_durations AS SELECT count(*) FROM mz_peek_durations;
> SELECT count(*) FROM count_peek_durations;
1
