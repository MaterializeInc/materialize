// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

using Npgsql;
using NUnit.Framework;
using System;
using System.Threading;

namespace csharp
{
    public class Tests
    {
        private NpgsqlConnection OpenConnection() {
            var conn = new NpgsqlConnection("host=materialized;port=6875;database=materialize;username=materialize");
            conn.Open();
            return conn;
        }

        [Test]
        public void BasicQuery()
        {
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT 42::int8", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Assert.That(reader.GetValue(0), Is.EqualTo(42));
            }
        }

        [Test]
        public void BasicSubscribe() {
            using var conn = OpenConnection();

            // Create a table with one row of data.
            new NpgsqlCommand("CREATE TABLE t (a int, b text)", conn).ExecuteNonQuery();
            new NpgsqlCommand("INSERT INTO t VALUES (1, 'a')", conn).ExecuteNonQuery();

            var txn = conn.BeginTransaction();
            new NpgsqlCommand("DECLARE c CURSOR FOR SUBSCRIBE t", conn, txn).ExecuteNonQuery();
            using (var cmd = new NpgsqlCommand("FETCH ALL c", conn, txn))
            using (var reader  = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader[1], Is.EqualTo(1)); // diff
                Assert.That(reader[2], Is.EqualTo(1)); // a
                Assert.That(reader[3], Is.EqualTo("a")); // b
                Assert.That(reader.Read(), Is.False);
            }

            // Insert another row from another connection to simulate an update
            // arriving.
            using (var conn2 = OpenConnection()) {
                new NpgsqlCommand("INSERT INTO t VALUES (2, 'b')", conn2).ExecuteNonQuery();
            }

            using (var cmd = new NpgsqlCommand("FETCH ALL c", conn, txn))
            using (var reader  = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader[1], Is.EqualTo(1)); // diff
                Assert.That(reader[2], Is.EqualTo(2)); // a
                Assert.That(reader[3], Is.EqualTo("b")); // b
                Assert.That(reader.Read(), Is.False);
            }
            txn.Commit();

            new NpgsqlCommand("DROP TABLE t", conn).ExecuteNonQuery();
        }

        [Test]
        public void CopySubscribe() {
            using var conn = OpenConnection();

            // Create a table with one row of data.
            new NpgsqlCommand("CREATE TABLE t (a int, b text)", conn).ExecuteNonQuery();
            new NpgsqlCommand("INSERT INTO t VALUES (1, 'a')", conn).ExecuteNonQuery();

            // Start a subscribe using the binary copy protocol.
            var reader = conn.BeginBinaryExport("COPY (SUBSCRIBE t) TO STDOUT (FORMAT BINARY)");
            // Validate the first row.
            Assert.That(reader.StartRow(), Is.EqualTo(4));
            reader.Read<decimal>(); // ignore timestamp column
            Assert.That(reader.Read<long>(), Is.EqualTo(1)); // diff column
            Assert.That(reader.Read<int>(), Is.EqualTo(1)); // a column
            Assert.That(reader.Read<string>(), Is.EqualTo("a")); // b column

            // Wait 2s so that the 1s NoticeResponse "test that the connection is still
            // alive" check triggers. This verifies Npgsql can successfully ignore the
            // NoticeResponse.
            Thread.Sleep(2000);

            // Insert another row from another connection to simulate an update
            // arriving.
            using (var conn2 = OpenConnection()) {
                new NpgsqlCommand("INSERT INTO t VALUES (2, 'b')", conn2).ExecuteNonQuery();
            }

            // Validate the new row.
            Assert.That(reader.StartRow(), Is.EqualTo(4));
            reader.Read<decimal>(); // ignore timestamp column
            Assert.That(reader.Read<long>(), Is.EqualTo(1)); // diff column
            Assert.That(reader.Read<int>(), Is.EqualTo(2)); // a column
            Assert.That(reader.Read<string>(), Is.EqualTo("b")); // b column

            // The subscribe won't end until we send a cancel request.
            reader.Cancel();

            // Ensure the COPY has ended after being canceled.
            Assert.Throws<OperationCanceledException>(delegate { reader.StartRow(); });

            new NpgsqlCommand("DROP TABLE t", conn).ExecuteNonQuery();
        }
    }
}
