// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildReplicaUtilizationHistoryQuery } from "./replicaUtilizationHistory";

const size25cc = {
  cpuNanoCores: 500_000_000,
  memoryBytes: 4_069_523_456,
  diskBytes: 8_139_046_912,
};

describe("replicaUtilizationHistory", () => {
  it("buckets cluster metrics", async () => {
    const mockedSearchPath = "internal_test, " + SEARCH_PATH;
    const client = await getMaterializeClient();

    // Mock tables
    await testdrive(`
        > CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'), r3 (SIZE 'scale=2,workers=1'));
        > CREATE SCHEMA internal_test;
        > SET schema = internal_test;
        > CREATE TABLE mz_cluster_replica_metrics_history (
            occurred_at TIMESTAMP NOT NULL,
            replica_id TEXT NOT NULL,
            process_id uint8 NOT NULL,
            cpu_nano_cores double,
            memory_bytes double,
            disk_bytes double,
            heap_bytes double,
            heap_limit double
          );
        > CREATE TABLE mz_cluster_replica_status_history (
            replica_id TEXT NOT NULL,
            process_id uint8 NOT NULL,
            occurred_at TIMESTAMP NOT NULL,
            status TEXT NOT NULL,
            reason TEXT
          );

        # We mock size 'scale=1,workers=1' to have the same spec as 25cc.
        > CREATE TABLE mz_cluster_replica_sizes (
            size TEXT NOT NULL,
            processes uint8 NOT NULL,
            cpu_nano_cores uint8 NOT NULL,
            memory_bytes uint8 NOT NULL,
            disk_bytes uint8
          );
        > INSERT INTO internal_test.mz_cluster_replica_sizes VALUES (
            'scale=1,workers=1',
            1,
            ${size25cc.cpuNanoCores},
            ${size25cc.memoryBytes},
            ${size25cc.diskBytes}
          );
        # We mock size 'scale=2,workers=1' to have the same spec as 25cc but with 2 processes.
        > INSERT INTO internal_test.mz_cluster_replica_sizes VALUES (
            'scale=2,workers=1',
            2,
            ${size25cc.cpuNanoCores},
            ${size25cc.memoryBytes},
            ${size25cc.diskBytes}
          );
    `);

    // Set search path to mock tables.
    await client.query(`SET search_path TO ${mockedSearchPath};`);

    async function cleanup() {
      await client.query(
        `DELETE FROM internal_test.mz_cluster_replica_metrics_history;`,
      );
      await client.query(
        `DELETE FROM internal_test.mz_cluster_replica_status_history;`,
      );
    }

    async function insertReplicaMetricRecords(
      ...records: {
        replicaId: string;
        processId?: number;
        cpuNanoCores: number;
        memoryBytes: number;
        diskBytes: number;
        timestamp: string;
        heapBytes?: number;
        heapLimit?: number;
      }[]
    ) {
      const values = [...records]
        .map(
          (record) => `(
          TIMESTAMP '${record.timestamp}',
          '${record.replicaId}',
          ${record.processId ?? 0},
          ${record.cpuNanoCores},
          ${record.memoryBytes},
          ${record.diskBytes},
          ${record.heapBytes ?? "NULL"},
          ${record.heapLimit ?? "NULL"}
      )`,
        )
        .join(",");

      await client.query(`
        INSERT INTO internal_test.mz_cluster_replica_metrics_history VALUES ${values}`);
    }

    const {
      rows: [cluster],
    } = await client.query("select id from mz_clusters where name = 'c'");
    const { rows: replicas } = await client.query(
      `SELECT
        id,
        name
      FROM mz_cluster_replicas
        JOIN mz_cluster_replica_sizes AS sizes USING (size)
      WHERE cluster_id = '${cluster.id}'
      ORDER BY id`,
    );

    const [replica1, replica2, replicaWithTwoProcesses] = replicas as {
      id: string;
      name: string;
    }[];

    // should bucket correctly and return the correct data per bucket
    {
      const record1 = {
        replicaId: replica1.id,
        cpuNanoCores: 5_789_441,
        memoryBytes: 46_788_608,
        diskBytes: 937_984,
        timestamp: "2030-01-01T00:00:00.000Z",
        heapBytes: 100,
        heapLimit: 1000,
      };

      const record2 = {
        replicaId: replica2.id,
        cpuNanoCores: 5_789_441,
        memoryBytes: 46_788_608,
        diskBytes: 937_984,
        timestamp: "2030-01-01T00:01:35.000Z",
        heapBytes: 200,
        heapLimit: 1000,
      };

      await insertReplicaMetricRecords(record1, record2);

      // Add an oom event to replica 2
      await client.query(`
        INSERT INTO internal_test.mz_cluster_replica_status_history VALUES (
          '${record2.replicaId}',
          0,
          TIMESTAMP '${record2.timestamp}',
          'offline',
          'oom-killed'
        )`);

      const startTime = "2030-01-01T00:00:30Z";

      const query = buildReplicaUtilizationHistoryQuery({
        startDate: startTime,
        bucketSizeMs: 60_000,
        clusterIds: [cluster.id],
      }).compile();

      const [bucket1, bucket2] = (
        await executeSqlHttp(query, {
          sessionVariables: {
            search_path: mockedSearchPath,
            cluster: QUICKSTART_CLUSTER,
          },
        })
      ).rows;

      // should bucket bucket1 to the first minute
      expect(bucket1.bucketStart.toISOString()).toEqual(
        "2030-01-01T00:00:00.000Z",
      );
      expect(bucket1.bucketEnd.toISOString()).toEqual(
        "2030-01-01T00:01:00.000Z",
      );

      // should bucket bucket2 to the second minute
      expect(bucket2.bucketStart.toISOString()).toEqual(
        "2030-01-01T00:01:00.000Z",
      );
      expect(bucket2.bucketEnd.toISOString()).toEqual(
        "2030-01-01T00:02:00.000Z",
      );

      // should have correct offline events
      expect(bucket2.offlineEvents).toEqual([
        {
          replicaId: record2.replicaId,
          reason: "oom-killed",
          status: "offline",
          occurredAt: "2030-01-01 00:01:35",
        },
      ]);

      expect(bucket1.offlineEvents).toBeNull();

      // should have correct utilization values
      expect(bucket1.clusterId).toEqual(cluster.id);
      expect(bucket1.size).toEqual("scale=1,workers=1");
      expect(bucket1.name).toEqual(replica1.name);
      expect(bucket1.maxCpuAt?.toISOString()).toEqual(record1.timestamp);
      expect(bucket1.maxCpuPercent).toBeCloseTo(
        record1.cpuNanoCores / size25cc.cpuNanoCores,
      );
      expect(bucket1.maxDiskAt?.toISOString()).toEqual(record1.timestamp);
      expect(bucket1.maxDiskPercent).toBeCloseTo(
        record1.diskBytes / size25cc.diskBytes,
      );

      expect(bucket1.maxMemoryAt?.toISOString()).toEqual(record1.timestamp);
      expect(bucket1.maxMemoryPercent).toBeCloseTo(
        record1.memoryBytes / size25cc.memoryBytes,
      );

      expect(bucket1.maxMemoryAndDiskAt?.toISOString()).toEqual(
        record1.timestamp,
      );
      expect(bucket1.maxMemoryAndDiskMemoryPercent).toBeCloseTo(
        record1.memoryBytes / size25cc.memoryBytes,
      );
      expect(bucket1.maxMemoryAndDiskDiskPercent).toBeCloseTo(
        record1.diskBytes / size25cc.diskBytes,
      );

      await cleanup();
    }
    // maxMemoryAndDisk should choose the record with the highest memory + disk utilization
    {
      const record1 = {
        replicaId: replica1.id,
        cpuNanoCores: 5_789_441,
        memoryBytes: 8,
        diskBytes: 937_984,
        timestamp: "2030-01-01T00:00:00.000Z",
        heapBytes: 100,
        heapLimit: 1000,
      };

      const record2 = {
        replicaId: replica1.id,
        cpuNanoCores: 5_789_441,
        memoryBytes: 46_788_608,
        diskBytes: 16,
        timestamp: "2030-01-01T00:00:35.000Z",
        heapBytes: 200,
        heapLimit: 1000,
      };

      await insertReplicaMetricRecords(record1, record2);

      const startTime = "2030-01-01T00:00:30Z";

      const query = buildReplicaUtilizationHistoryQuery({
        startDate: startTime,
        bucketSizeMs: 60_000,
        clusterIds: [cluster.id],
      }).compile();

      const [bucket1] = (
        await executeSqlHttp(query, {
          sessionVariables: {
            search_path: mockedSearchPath,
            cluster: QUICKSTART_CLUSTER,
          },
        })
      ).rows;

      expect(bucket1.maxMemoryAndDiskMemoryPercent).toEqual(
        record2.memoryBytes / size25cc.memoryBytes,
      );
      expect(bucket1.maxMemoryAndDiskDiskPercent).toEqual(
        record2.diskBytes / size25cc.diskBytes,
      );
      expect(bucket1.maxDiskPercent).toEqual(
        record1.diskBytes / size25cc.diskBytes,
      );
      expect(bucket1.maxMemoryPercent).toEqual(
        record2.memoryBytes / size25cc.memoryBytes,
      );
      await cleanup();
    }
    // Should merge metrics across processes.
    // i.e. replica_metric_percentage = (process_1_raw_metric + process_2_raw_metric) / (process_1_capacity + process_2_capacity)
    {
      const record1 = {
        replicaId: replicaWithTwoProcesses.id,
        processId: 0,
        cpuNanoCores: 250_000_000,
        memoryBytes: 2_069_523_456,
        diskBytes: 8_139_046_912,
        timestamp: "2030-01-01T00:00:00.000Z",
        heapBytes: 100,
        heapLimit: 1000,
      };

      const record2 = {
        replicaId: replicaWithTwoProcesses.id,
        processId: 1,
        cpuNanoCores: 350_000_000,
        memoryBytes: 2_069_523_456,
        diskBytes: 8_139_046_912,
        timestamp: "2030-01-01T00:00:00.000Z",
        heapBytes: 200,
        heapLimit: 1000,
      };

      await insertReplicaMetricRecords(record1, record2);

      const startTime = "2030-01-01T00:00:00Z";

      const query = buildReplicaUtilizationHistoryQuery({
        startDate: startTime,
        bucketSizeMs: 60_000,
        clusterIds: [cluster.id],
      }).compile();

      const [bucket1] = (
        await executeSqlHttp(query, {
          sessionVariables: {
            search_path: mockedSearchPath,
            cluster: QUICKSTART_CLUSTER,
          },
        })
      ).rows;

      expect(bucket1.maxDiskPercent).toEqual(
        (record1.diskBytes + record2.diskBytes) / (size25cc.diskBytes * 2),
      );
      expect(bucket1.maxMemoryPercent).toEqual(
        (record1.memoryBytes + record2.memoryBytes) /
          (size25cc.memoryBytes * 2),
      );
      expect(bucket1.maxCpuPercent).toEqual(
        (record1.cpuNanoCores + record2.cpuNanoCores) /
          (size25cc.cpuNanoCores * 2),
      );

      expect(bucket1.maxMemoryAndDiskMemoryPercent).toEqual(
        bucket1.maxMemoryPercent,
      );
      expect(bucket1.maxMemoryAndDiskDiskPercent).toEqual(
        bucket1.maxDiskPercent,
      );
      await cleanup();
    }
  });
});
