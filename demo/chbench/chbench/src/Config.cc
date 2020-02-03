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

#include "MySqlDialect.h"
#include "HanaDialect.h"
#include "Random.h"
#include "Config.h"
#include "mz-config.h"

#include <libconfig.h++>
#include <iostream>

static chRandom::int_distribution get_int_dist(libconfig::Setting& setting) {
    std::string name = setting["name"];
    chRandom::int_distribution::inner_type dist;
    if (name == "binomial") {
        dist = std::binomial_distribution<int64_t>(setting["t"], setting["p"]);
    }
    else if (name == "uniform") {
        dist = std::uniform_int_distribution<int64_t>(setting["a"], setting["b"]);
    }
    else if (name == "poisson") {
        dist = std::poisson_distribution<int64_t>(setting["mean"]);
    }
    else if (name == "negative_binomial") {
        dist = std::negative_binomial_distribution<int64_t>(setting["k"], setting["p"]);
    }
    else if (name == "geometric") {
        dist = std::geometric_distribution<int64_t>(setting["p"]);
    }
    else {
        throw Config::UnrecognizedDistributionException {name};
    }
    return chRandom::int_distribution {dist};
}

mz::Config Config::get_config(libconfig::Config &config) {
    using libconfig::Setting;
    mz::Config ret = mz::defaultConfig();
    if (config.exists("dialect")) {
        const std::string dialect = config.lookup("dialect");
        if (dialect == "mysql") {
            ret.dialect = new MySqlDialect();
        } else if (dialect == "hana") {
            ret.dialect = new HanaDialect();
        } else {
            throw Config::UnrecognizedDialectException {dialect};
        }
    }
    if (config.exists("all_queries")) {
        const Setting& all_queries = config.lookup("all_queries");
        for (const auto& query: all_queries) {
            ret.allQueries[query["name"]] = mz::ViewDefinition {
                .query = query["query"],
                .order = query.exists("order") ? (std::optional<std::string>)((std::string)query["order"]) : std::nullopt,
                .limit = query.exists("limit") ? (std::optional<bool>)((bool)query["limit"]) : std::nullopt,
            };
        }
    }
    if (config.exists("active_queries")) {
        const Setting& active_queries = config.lookup("active_queries");

        for (const auto& q: active_queries) {
            if (auto i = ret.allQueries.find(q); i != ret.allQueries.end()) {
                ret.hQueries.push_back(&*i);
            } else {
                throw std::out_of_range { "no such query defined: " +  (std::string)q };
            }
        }
    }
    if (config.exists("hist_date_offset")) {
        ret.hist_date_offset_millis = get_int_dist(config.lookup("hist_date_offset"));
    }
    if (config.exists("order_entry_date_offset")) {
        ret.order_entry_date_offset_millis = get_int_dist(config.lookup("order_entry_date_offset"));
    }
    if (config.exists("orderline_delivery_date_offset")) {
        ret.orderline_delivery_date_offset_millis = get_int_dist(config.lookup("orderline_delivery_date_offset"));
    }
    if (config.exists("payment_amount")) {
        ret.payment_amount_cents = get_int_dist(config.lookup("payment_amount"));
    }
    if (config.exists("item_price")) {
        ret.item_price_cents = get_int_dist(config.lookup("item_price"));
    }
    return ret;
}


const char* Config::UnrecognizedDistributionException::what() const noexcept {
    return what_rendered.c_str();
}

Config::UnrecognizedDistributionException::UnrecognizedDistributionException(const std::string &distribution) :
 what_rendered { "Unrecognized distribution: " + distribution } {}

const char* Config::UnrecognizedDialectException::what() const noexcept {
    return what_rendered.c_str();
}

Config::UnrecognizedDialectException::UnrecognizedDialectException(const std::string &dialect) :
 what_rendered { "Unrecognized dialect: " + dialect } {}

