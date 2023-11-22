# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

require "bundler/setup"
require "pg"
require "test/unit"


class MaterializeTest < Test::Unit::TestCase
  def connect
    PG.connect(
      host: ENV["PGHOST"] || "localhost",
      port: (ENV["PGPORT"] || 6875).to_i,
      dbname: ENV["PGDATABASE"] || "materialize",
      user: ENV["PGUSER"] || "materialize",
    )
  end

  def test_type_map_all_strings
    conn = connect
    conn.exec("VALUES ('a'::text, 1::integer, '2023-01-01T01:23:45'::timestamp), ('b', NULL, NULL) ORDER BY 1") do |result|
      assert_equal(result.collect.to_a, [
          {"column1" => "a", "column2" => "1", "column3" => "2023-01-01 01:23:45"},
          {"column1" => "b", "column2" => nil, "column3" => nil}
      ])
    end
  end

  def test_type_map_basic_type_map
    conn = connect
    conn.type_map_for_results = PG::BasicTypeMapForResults.new(conn)
    conn.exec("VALUES ('a'::text, 1::integer, '2023-01-01T01:23:45'::timestamp), ('b', NULL, NULL) ORDER BY 1") do |result|
      assert_equal(result.collect.to_a, [
          {"column1" => "a", "column2" => 1, "column3" => Time.new(2023, 1, 1, 1, 23, 45)},
          {"column1" => "b", "column2" => nil, "column3" => nil}
      ])
    end
  end
end
