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

#ifndef DATASOURCE_H
#define DATASOURCE_H

#include <fstream>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>
#include <vector>

struct Nation {
    int id;
    std::string name;
    int rId;
};

class DataSource {

  private:
    static const std::vector<const char*> cLastParts;
    static const std::vector<const char*> tpchNouns;
    static const std::vector<const char*> tpchVerbs;
    static const std::vector<const char*> tpchAdjectives;
    static const std::vector<const char*> tpchAdverbs;
    static const std::vector<const char*> tpchPrepositions;
    static const std::vector<const char*> tpchTerminators;
    static const std::vector<const char*> tpchAuxiliaries;
    static const char* regions[];
    static int warehouseCount;

    static std::string tpchText(int length);
    static std::string tpchSentence();
    static std::string tpchNounPhrase();
    static std::string tpchVerbPhrase();
    static std::string tpchPrepositionalPhrase();

  public:
    static void initialize(int warehouseCount);
    static bool randomTrue(double probability);
    static int permute(int value, int low, int high);
    static void getCurrentTimestamp(SQL_TIMESTAMP_STRUCT& ret, int64_t offset = 0);
    static void genCLast(int value, std::string& ret);
    static void randomCLast(std::string& ret);
    static void getRemoteWId(int& currentWId, int& ret);
    static void addNumeric(int length, std::ofstream& stream, bool delimiter);
    static void addAlphanumeric62(int length, std::ofstream& stream,
                                  bool delimiter);
    static void addAlphanumeric64(int length, std::ofstream& stream,
                                  bool delimiter);
    static void addAlphanumeric64(int minLength, int maxLength,
                                  std::ofstream& stream, bool delimiter);
    static void addAlphanumeric64Original(int minLength, int maxLength,
                                          std::ofstream& stream,
                                          bool delimiter);
    static void addTextString(int minLength, int maxLength,
                              std::ofstream& stream, bool delimiter);
    static void addTextStringCustomer(int minLength, int maxLength,
                                      const char* action, std::ofstream& stream,
                                      bool delimiter);
    static void addInt(int minValue, int maxValue, std::ofstream& stream,
                       bool delimiter);
    static void addDouble(double minValue, double maxValue, int decimals,
                          std::ofstream& stream, bool delimiter);
    static void writeDouble(double d, std::ofstream& stream, bool delimiter);
    static void addNId(std::ofstream& stream, bool delimiter);
    static void addWDCZip(std::ofstream& stream, bool delimiter);
    static void addSuPhone(int& suId, std::ofstream& stream, bool delimiter);
    static std::string getCurrentTimeString(int64_t offset = 0);
    static std::string strLeadingZero(int i, int zeros);
    static Nation getNation(int i);
    static const char* getRegion(int i);

    static std::string randomState();
    static Nation randomNation();
};

#endif
