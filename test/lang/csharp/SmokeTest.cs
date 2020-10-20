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

namespace csharp
{
    public class Tests
    {
        [Test]
        public void BasicQuery()
        {
            using var conn = new NpgsqlConnection("host=materialized;port=6875");
            conn.Open();

            // TODO(benesch): enable once pg_type is sufficiently supported
            // for Npgsql to determine the types here.
            if (false) {
                using var cmd = new NpgsqlCommand("SELECT 42::int8", conn);
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    Assert.AreEqual(42, reader.GetInt64(0));
                }
            }
        }
    }
}
