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

#include <unordered_set>
#include <string>
#include <unordered_map>
#include <vector>
#include "materialized.h"
#include "Config.h"
#include "Random.h"

namespace mz {

struct Config {
    std::unordered_set<std::string> expectedSources;
    std::string viewPattern;
    std::string materializedUrl;
    std::string kafkaUrl;
    std::string schemaRegistryUrl;
    std::vector<std::pair<const std::string, ViewDefinition>*> hQueries; // pointers into allQueries
    std::unordered_map<std::string, ViewDefinition> allQueries;
    chRandom::int_distribution hist_date_offset_millis;
    chRandom::int_distribution order_entry_date_offset_millis;
    chRandom::int_distribution orderline_delivery_date_offset_millis;
    chRandom::int_distribution payment_amount_cents;
    chRandom::int_distribution item_price_cents;
};

const Config& defaultConfig(); // The config that works with our current docker-compose setup
}
