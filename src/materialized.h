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

namespace mz {
class MaterializedException : public std::exception {
};

class UnexpectedCreateSourcesResult : public MaterializedException {
    std::string what_rendered;
public:
    const char *what() const noexcept override;

    explicit UnexpectedCreateSourcesResult(const std::string &explanation);
};

std::vector<std::string> createAllSources(
        pqxx::connection& c,
        std::string from,
        std::string registry,
        std::optional<std::string> like);

void createMaterializedView(pqxx::connection& c, std::string&& name, std::string&& query);
}

