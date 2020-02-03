#pragma once

#include <chrono>
#include <utility>

template <typename F>
auto timeInvocation(F&& func) -> std::pair<std::chrono::nanoseconds, decltype(func())> {
    auto begin = std::chrono::high_resolution_clock::now();
    auto result = func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::make_pair(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin), result);
}