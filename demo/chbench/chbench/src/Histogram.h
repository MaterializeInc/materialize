#pragma once

#include <vector>
#include <cstdint>

class Histogram {
    std::vector<uint64_t> counts;
public:
    Histogram& operator+=(const Histogram& other);
    void increment(uint64_t);
    std::vector<uint64_t> getCounts() const;
};
