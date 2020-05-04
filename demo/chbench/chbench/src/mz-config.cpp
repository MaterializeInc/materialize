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

#include <mz-config.h>
#include "materialized.h"
#include "MySqlDialect.h"

const mz::Config& mz::defaultConfig() {
    using inner_type = chRandom::int_distribution::inner_type;
    using chRandom::int_distribution;
    static Config singleton {
        .expectedSources = {
            "debezium_tpcch_customer",
            "debezium_tpcch_history",
            "debezium_tpcch_district",
            "debezium_tpcch_neworder",
            "debezium_tpcch_order",
            "debezium_tpcch_orderline",
           "debezium_tpcch_warehouse",
            "debezium_tpcch_item",
            "debezium_tpcch_stock",
            "debezium_tpcch_nation",
            "debezium_tpcch_region",
            "debezium_tpcch_supplier"
        },
        .viewPattern = "debezium.tpcch.%",
        .materializedUrl = "postgresql://materialized:6875/materialize?sslmode=disable",
        .kafkaUrl = "kafka:9092",
        .schemaRegistryUrl = "http://schema-registry:8081",
        .hQueries = {},
        .dialect = new MySqlDialect(),
        .hist_date_offset_millis =  static_cast<inner_type>(std::uniform_int_distribution<int64_t>(946684800,1704067200)), //  2010-2024
        .order_entry_date_offset_millis =  static_cast<inner_type>(std::uniform_int_distribution<int64_t>(946684800,1704067200)), // 2010-2024
        .orderline_delivery_date_offset_millis = static_cast<inner_type>(std::uniform_int_distribution<int64_t>(946684800,1704067200)), // 2010-2024
        .payment_amount_cents = static_cast<inner_type>(std::uniform_int_distribution<int64_t>(100, 500000)),
        .item_price_cents = static_cast<inner_type>(std::uniform_int_distribution<int64_t>(100, 10000)),
    };
    return singleton;
}
