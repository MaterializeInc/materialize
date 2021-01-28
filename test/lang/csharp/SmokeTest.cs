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

            var txn = conn.BeginTransaction();
            new NpgsqlCommand("DECLARE c CURSOR FOR TAIL t", conn, txn).ExecuteNonQuery();
            using (var cmd = new NpgsqlCommand("FETCH ALL c", conn, txn))
            using (var reader  = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(1, reader[1]); // diff
                Assert.AreEqual(1, reader[2]); // a
                Assert.AreEqual("a", reader[3]); // b
                Assert.IsFalse(reader.Read());
            }

            // Insert another row from another connection to simulate an update
            // arriving.
            using (var conn2 = OpenConnection()) {
                new NpgsqlCommand("INSERT INTO t VALUES (2, 'b')", conn2).ExecuteNonQuery();
            }

            using (var cmd = new NpgsqlCommand("FETCH ALL c", conn, txn))
            using (var reader  = cmd.ExecuteReader())
            {
                Assert.IsTrue(reader.Read());
                Assert.AreEqual(1, reader[1]); // diff
                Assert.AreEqual(2, reader[2]); // a
                Assert.AreEqual("b", reader[3]); // b
                Assert.IsFalse(reader.Read());
            }
            txn.Commit();

            new NpgsqlCommand("DROP TABLE t", conn).ExecuteNonQuery();
        }
    }
}
