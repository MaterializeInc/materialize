/*
Copyright 2014 Florian Wolf, SAP AG

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
    static std::vector<const char*> cLastParts;
    static std::vector<const char*> tpchNouns;
    static std::vector<const char*> tpchVerbs;
    static std::vector<const char*> tpchAdjectives;
    static std::vector<const char*> tpchAdverbs;
    static std::vector<const char*> tpchPrepositions;
    static std::vector<const char*> tpchTerminators;
    static std::vector<const char*> tpchAuxiliaries;
    static const Nation nations[];
    static const char* regions[];
    static int lastOlCount;
    static int warehouseCount;

    static std::string tpchText(int length);
    static std::string tpchSentence();
    static std::string tpchNounPhrase();
    static std::string tpchVerbPhrase();
    static std::string tpchPrepositionalPhrase();

  public:
    static void initialize(int warehouseCount);
    static bool randomTrue(double probability);
    static int randomUniformInt(int minValue, int maxValue);
    static void randomUniformInt(int minValue, int maxValue, int& ret);
    static void randomNonUniformInt(int A, int x, int y, int C, int& ret);
    static void randomDouble(double minValue, double maxValue, int decimals,
                             double& ret);
    static int permute(int value, int low, int high);
    static void getCurrentTimestamp(SQL_TIMESTAMP_STRUCT& ret);
    static void genCLast(int value, std::string& ret);
    static void randomCLast(std::string& ret);
    static void getRemoteWId(int& currentWId, int& ret);
    static int nextOderlineCount();
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
    static void addNId(std::ofstream& stream, bool delimiter);
    static void addWDCZip(std::ofstream& stream, bool delimiter);
    static void addSuPhone(int& suId, std::ofstream& stream, bool delimiter);
    static std::string getCurrentTimeString();
    static std::string randomAlphanumeric62(int length);
    static std::string strLeadingZero(int i, int zeros);
    static Nation getNation(int i);
    static const char* getRegion(int i);
};

#endif
