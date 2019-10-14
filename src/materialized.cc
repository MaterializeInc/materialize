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

std::vector<std::string>
mz::createAllSources(pqxx::connection &c, std::string from, std::string registry, std::optional<std::string> like) {
    std::vector<std::string> results;
    pqxx::nontransaction w(c);
    from = c.quote(from);
    registry = c.quote(registry);
    auto realLike = c.quote(like ? like.value() : std::string("%"));
    auto pqResult = w.exec("CREATE SOURCES LIKE " + realLike + " FROM " + from + " USING SCHEMA REGISTRY " + registry);
    auto cols = pqResult.columns();
    if (cols != 1) {
        throw UnexpectedCreateSourcesResult {"Wrong number of columns: " + std::to_string(cols)};
    }
    // TODO -- assert that the column is a string.
    results.reserve(pqResult.size());
    for (const auto& row: pqResult) {
        results.push_back(row[0].as<std::string>());
    }
    return results;
}

void mz::createMaterializedView(pqxx::connection& c, std::string&& name, std::string&& query) {
    pqxx::nontransaction w(c);
    w.exec0("CREATE VIEW " + std::move(name) + " AS " + std::move(query));
}

const char *mz::UnexpectedCreateSourcesResult::what() const noexcept {
    return what_rendered.c_str();
}

mz::UnexpectedCreateSourcesResult::UnexpectedCreateSourcesResult(const std::string& explanation) {
    what_rendered = std::string {"Unexpected result from CREATE SOURCES: "} + explanation;
}
