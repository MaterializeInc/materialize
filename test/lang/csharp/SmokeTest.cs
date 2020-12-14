// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

using Npgsql;
using NUnit.Framework;
using System.Threading;

namespace csharp
{
    public class Tests
    {
        private NpgsqlConnection conn;

        private NpgsqlConnection OpenConnection() {
            var conn = new NpgsqlConnection("host=materialized;port=6875;database=materialize");
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
                Assert.AreEqual(42, reader.GetValue(0));
            }
        }

        [Test]
        public void BasicTail() {
            using var conn = OpenConnection();

            // Create a table with one row of data.
            new NpgsqlCommand("CREATE TABLE t (a int, b text)", conn).ExecuteNonQuery();
            new NpgsqlCommand("INSERT INTO t VALUES (1, 'a')", conn).ExecuteNonQuery();

            // Start a tail using the binary copy protocol.
            var reader = conn.BeginBinaryExport("COPY (TAIL t) TO STDOUT (FORMAT BINARY)");

            // Validate the first row.
            Assert.AreEqual(4, reader.StartRow());
            reader.Read<decimal>(); // ignore timestamp column
            Assert.AreEqual(1, reader.Read<long>()); // diff column
            Assert.AreEqual(1, reader.Read<int>()); // a column
            Assert.AreEqual("a", reader.Read<string>()); // b column

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
            Assert.AreEqual(4, reader.StartRow());
            reader.Read<decimal>(); // ignore timestamp column
            Assert.AreEqual(1, reader.Read<long>()); // diff column
            Assert.AreEqual(2, reader.Read<int>()); // a column
            Assert.AreEqual("b", reader.Read<string>()); // b column

            // The tail won't end until we send a cancel request.
            reader.Cancel();
        }
    }
}
