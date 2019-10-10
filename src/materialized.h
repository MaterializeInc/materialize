// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#pragma once

#include <pqxx/pqxx>
#include <vector>
#include <optional>
#include <string>
#include <exception>

class MaterializedException : public std::exception {};

class UnexpectedCreateSourcesResult : public MaterializedException {
    std::string what_rendered;
public:
    const char* what() const noexcept override;
    explicit UnexpectedCreateSourcesResult(const std::string& explanation);
};

std::vector<std::string> createAllSources(
        pqxx::connection& c,
        std::string from,
        std::string registry,
        std::optional<std::string> like);
