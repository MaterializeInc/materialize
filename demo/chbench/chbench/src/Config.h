/*
Copyright 2019 Materialize, Inc

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

#include <string>
#include <exception>
#include <libconfig.h++>

namespace mz {
    struct Config;
}

namespace Config {
class ConfigException : public std::exception {};

class UnrecognizedDialectException : public ConfigException {
    std::string what_rendered;
public:
    const char *what() const noexcept override;

    explicit UnrecognizedDialectException(const std::string &dialect);
};

class UnrecognizedDistributionException : public ConfigException {
    std::string what_rendered;
public:
    const char *what() const noexcept override;

    explicit UnrecognizedDistributionException(const std::string &distribution);
};

mz::Config get_config(libconfig::Config& config);
}
