# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# This test only verifies that the log relations are published, not that they have any specific output.

> create view count_operates as select count(*) from logs_operates;
> select count (*) from count_operates;
1

> create view count_channels as select count(*) from logs_channels;
> select count (*) from count_channels;
1

> create view count_shutdown as select count(*) from logs_shutdown;
> select count (*) from count_shutdown;
1

> create view count_text as select count(*) from logs_text;
> select count (*) from count_text;
1

> create view count_elapsed as select count(*) from logs_elapsed;
> select count (*) from count_elapsed;
1

> create view count_histogram as select count(*) from logs_histogram;
> select count (*) from count_histogram;
1

> create view count_arrangement as select count(*) from logs_arrangement;
> select count (*) from count_arrangement;
1

> create view count_dataflows as select count(*) from logs_dataflows;
> select count (*) from count_dataflows;
1

> create view count_frontiers as select count(*) from logs_frontiers;
> select count (*) from count_frontiers;
1

> create view count_peeks as select count(*) from logs_peeks;
> select count (*) from count_peeks;
1

> create view count_peek_durations as select count(*) from logs_peek_durations;
> select count (*) from count_peek_durations;
1
