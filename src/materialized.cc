/*
Copyright 2019 Materialize, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "materialized.h"

// CREATE SOURCES LIKE 'mysql.tpcch.%' FROM 'kafka://kafka:9092' USING SCHEMA REGISTRY 'http://schema-registry:8081';
pqxx::result querySync(pqxx::connection& c, const char *query) {
    pqxx::nontransaction w(c);
    return w.exec(query);
}

std::vector<std::string>
createAllSources(pqxx::connection &c, std::string from, std::string registry, std::optional<std::string> like) {
    std::vector<std::string> results;
    pqxx::nontransaction w(c);
    from = c.quote(from);
    registry = c.quote(registry);
    auto realLike = c.quote(like ? like.value() : std::string("%"));
    auto pqResult = w.exec("CREATE SOURCES LIKE " + realLike + " FROM " + from + " USING SCHEMA REGISTRY " + registry);
    if (pqResult.columns() != 1) // TODO
        abort();
    // TODO -- assert that the column is a string.
    results.reserve(pqResult.size());
    for (const auto& row: pqResult) {
        results.push_back(row[0].as<std::string>());
    }
    return results;
}
