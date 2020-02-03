/*
Copyright 2014 Florian Wolf, SAP AG
Modifications Copyright 2019 Materialize, Inc.

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

#include "AnalyticalStatistic.h"

AnalyticalStatistic::AnalyticalStatistic() {
    for (int i = 0; i < 22; i++) {
        executeTPCHSuccessCount[i] = 0;
        executeTPCHFailCount[i] = 0;
    }
}

void AnalyticalStatistic::addResult(unsigned long long& analyticalResults) {
    for (int i = 0; i < 22; i++) {
        analyticalResults += executeTPCHSuccessCount[i];
    }
}

void AnalyticalStatistic::executeTPCHSuccess(int queryNumber, bool success) {
    if (success)
        executeTPCHSuccessCount[queryNumber - 1]++;
    else
        executeTPCHFailCount[queryNumber - 1]++;
}
