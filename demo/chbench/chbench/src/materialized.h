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
#pragma once

#include <pqxx/pqxx>
#include <vector>
#include <optional>
#include <string>
#include <exception>
#include <unordered_set>
#include <unordered_map>
#include <chrono>

namespace mz {
class MaterializedException : public std::exception {
};

class UnexpectedCreateSourcesResult : public MaterializedException {
    std::string what_rendered;
public:
    const char *what() const noexcept override;

    explicit UnexpectedCreateSourcesResult(const std::string &explanation);
};

struct ViewDefinition {
    std::string query;
    std::optional<std::string> order;
    std::optional<unsigned> limit;
};

struct PeekResults {
    std::chrono::nanoseconds latency;
    pqxx::result inner;
};

std::vector<std::string> showAllSources(pqxx::connection &c);

void createMaterializedView(pqxx::connection& c, const std::string &name, const std::string &query);
bool createSource(pqxx::connection &c, const std::string& kafkaUrl, const std::string& registry, const std::string& source, const std::string& consistencySource,
        bool materialize);

PeekResults peekView(pqxx::connection& c, const std::string& name, const std::optional<std::string>& order,
                     std::optional<unsigned> limit);
}
