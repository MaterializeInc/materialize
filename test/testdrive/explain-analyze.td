# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=((\d+)\sbytes|u(\d+)|s(\d+)|(\d+:\d+:\d+(.\d+)?)) replacement=<XXX>

# In case the environment has other replicas
> SET cluster_replica = r1

> CREATE SCHEMA blue;
> CREATE SCHEMA green;

> SET SCHEMA = blue;
> CREATE TABLE t (x INT, y INT);
> CREATE INDEX t_idx ON t(x);

> SET SCHEMA = green;
> CREATE TABLE t (x INT, y INT);
> CREATE INDEX t_idx ON t(x);
> INSERT INTO t VALUES (0, 1), (0, 2), (1, 3), (2, 4);

# both work
> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.green.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          4
"  Stream <XXX>"              <null>         <null>

> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.blue.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          <null>
"  Stream <XXX>"              <null>         <null>

> EXPLAIN ANALYZE CLUSTER CPU;
object                      global_id    total_elapsed
------------------------------------------------------
materialize.green.t_idx     <XXX>        <XXX>
materialize.blue.t_idx      <XXX>        <XXX>

> ALTER SCHEMA blue SWAP WITH green;

> DROP SCHEMA green CASCADE;

# should still get output for blue!
> EXPLAIN ANALYZE MEMORY FOR INDEX materialize.blue.t_idx;
operator                      total_memory   total_records
----------------------------------------------------------
"Arrange (#0{x})"             <XXX>          4
"  Stream <XXX>"              <null>         <null>

> EXPLAIN ANALYZE CLUSTER CPU;
object                      global_id    total_elapsed
------------------------------------------------------
materialize.blue.t_idx      <XXX>        <XXX>

# ensure we can explain mz_internal objects

> SET CLUSTER = mz_catalog_server;

> EXPLAIN ANALYZE CPU FOR INDEX mz_internal.mz_console_cluster_utilization_overview_ind;
operator                     total_elapsed
-----------------------------------------
"Arrange (#17{cluster_id})" <XXX>
"  Stream <XXX>" <null>
"Returning Map/Filter/Project" <XXX>
"  Union" <XXX>
"    Read l4" <XXX>
"    Map/Filter/Project" <XXX>
"      Consolidating Union" <XXX>
"        Read l3" <XXX>
"        Negate Diffs" <XXX>
"          Read l4" <XXX>
"With l4 = Differential Join %1 » %0" <XXX>
"  Arrange (#1, #0)" <XXX>
"    Non-incremental GroupAggregate`" <XXX>
"      Arranged <XXX>" <XXX>
"  Arrange (#0{replica_id}, #3{bucket_start})" <XXX>
"    Stream l3" <null>
"l3 = Differential Join %1 » %0" <XXX>
"  Arrange (#0, #1)" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Differential Join %1 » %0" <XXX>
"          Arranged <XXX>" <null>
"          Arrange (#0{replica_id})" <XXX>
"            Distinct GroupAggregate" <XXX>
"              Read l2" <XXX>
"  Arrange (#0, #3)" <XXX>
"    Stream l2" <null>
"l2 = Delta Join [%0 » %1 » %2 » %3 » %4 » %5][%1 » %0 » %2 » %3 » %4 » %5][%2 » %0 » %1 » %3 » %4 » %5][%3 » %0 » %1 » %2 » %4 » %5][%4 » %0 » %1 » %2 » %3 » %5][%5 » %0 » %1 » %2 » %3 » %4]" <XXX>
"  Arrange (#0{replica_id})" <XXX>
"    Arranged l0" <null>
"  Arrange (#0{replica_id}, #3{bucket_start})" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Read l1" <XXX>
"  Arrange (#0{replica_id}, #4{bucket_start})" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Read l1" <XXX>
"  Arrange (#0{replica_id}, #3{bucket_start})" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Read l1" <XXX>
"  Arrange (#0{replica_id}, #3{bucket_start})" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Read l1" <XXX>
"  Arrange (#0{replica_id}) (#0{replica_id}, #3{bucket_start})" <XXX>
"    Map/Filter/Project" <XXX>
"      Non-monotonic TopK" <XXX>
"        Read l1" <XXX>
"l1 = Differential Join %0 » %1" <XXX>
"  Arrange (#1{replica_id})" <XXX>
"    Map/Filter/Project" <XXX>
"      Accumulable GroupAggregate" <XXX>
"        Map/Filter/Project" <XXX>
"          Delta Join [%0 » %2 » %1][%1 » %0 » %2][%2 » %0 » %1]" <XXX>
"            Arranged <XXX>" <null>
"            Arranged <XXX>" <null>
"            Arrange (#0{replica_id}) (#1{size})" <XXX>
"              Arranged l0" <XXX>
"  Arrange (#0{replica_id})" <XXX>
"    Arranged l0" <XXX>
"l0 = Distinct GroupAggregate" <XXX>
"  Union" <XXX>
"    Arranged <XXX>" <XXX>
"    Arranged <XXX>" <XXX>
