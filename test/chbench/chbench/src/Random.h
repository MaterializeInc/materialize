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

#include <random>
#include <variant>

namespace chRandom {

extern thread_local std::mt19937 rng;

class int_distribution {
public:
    using inner_type = std::variant<
        std::binomial_distribution<int64_t>,
        std::uniform_int_distribution<int64_t>,
        std::poisson_distribution<int64_t>,
        std::negative_binomial_distribution<int64_t>,
        std::geometric_distribution<int64_t>
    >;
private:
    inner_type inner;

public:
    template <class Generator>
    int operator()(Generator& g) {
        return std::visit([&g](auto&& arg) {
            return arg(g);
        }, inner);
    }

    int_distribution(inner_type dist) : inner(dist) {}
};



// FIXME: This should be a template
inline int uniformInt(int min, int max) {
    std::uniform_int_distribution<int> dist(min, max);
    return dist(rng);
}

inline int nonUniformInt(int A, int x, int y, int C) {
    return (((uniformInt(0, A) | uniformInt(x, y)) + C) % (y - x + 1)) + x;
}

inline double uniformDouble(double min, double max, int decimals) {
    min *= pow(10.0, decimals);
    max *= pow(10.0, decimals);
    return double(uniformInt((int) min, (int) max)) / pow(10.0, decimals);
}

} // namespace chRandom
