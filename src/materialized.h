// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#pragma once

#include <pqxx/pqxx>
#include <vector>
#include <optional>
#include <string>


std::vector<std::string> createAllSources(
        pqxx::connection& c,
        std::string from,
        std::string registry,
        std::optional<std::string> like);
pqxx::result querySync(pqxx::connection& c, const char *query);