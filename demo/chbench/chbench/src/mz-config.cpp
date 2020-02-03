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
    int_distribution zero_const {static_cast<inner_type>(std::uniform_int_distribution<int64_t>(0, 0))};
    static Config singleton {
        .expectedSources = {
            "materialize.public.mysql_tpcch_customer",
            "materialize.public.mysql_tpcch_history",
            "materialize.public.mysql_tpcch_district",
            "materialize.public.mysql_tpcch_neworder",
            "materialize.public.mysql_tpcch_order",
            "materialize.public.mysql_tpcch_orderline",
            "materialize.public.mysql_tpcch_warehouse",
            "materialize.public.mysql_tpcch_item",
            "materialize.public.mysql_tpcch_stock",
            "materialize.public.mysql_tpcch_nation",
            "materialize.public.mysql_tpcch_region",
            "materialize.public.mysql_tpcch_supplier"
        },
        .viewPattern = "mysql.tpcch.%",
        .materializedUrl = "postgresql://materialized:6875/?sslmode=disable",
        .kafkaUrl = "kafka://kafka:9092",
        .schemaRegistryUrl = "http://schema-registry:8081",
        .hQueries = {},
        .dialect = new MySqlDialect(),
        .hist_date_offset_millis = zero_const,
        .order_entry_date_offset_millis = zero_const,
        .orderline_delivery_date_offset_millis = zero_const,
        .payment_amount_cents = static_cast<inner_type>(std::uniform_int_distribution<int64_t>(100, 500000)),
        .item_price_cents = static_cast<inner_type>(std::uniform_int_distribution<int64_t>(100, 10000)),
    };
    return singleton;
}
