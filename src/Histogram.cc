//
// Created by brennan on 10/17/19.
//

#include "Histogram.h"

// This takes around 64 CPU cycles in the worst case (each loop iteration will take 1 on a modern CPU).
// It's not worth optimizing with bit twiddling hacks.
static unsigned nextPow2(uint64_t val) {
    uint64_t testVal = 1;
    unsigned count = 0;
    while (val > testVal) {
        testVal *= 2;
        ++count;
    }
    return count;
}

void Histogram::increment(uint64_t val) {
    unsigned slot = nextPow2(val);
    if (counts.size() <= slot) {
        counts.resize(slot);
    }
    ++counts[slot];
}

std::vector<uint64_t> Histogram::getCounts() const {
    return counts;
}

Histogram &Histogram::operator+=(const Histogram &other) {
    if (other.counts.size() >= counts.size()) {
        counts.resize(other.counts.size());
    }
    for (size_t i = 0; i < other.counts.size(); ++i) {
        ++counts[i];
    }
    return *this;
}
