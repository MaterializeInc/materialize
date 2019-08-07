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

#include "DataSource.h"

#include "Defines.h"

#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <math.h>
#include <sstream>

using namespace std;

const Nation DataSource::nations[] = {
    {48, "ALGERIA", 0},     {49, "ARGENTINA", 1},      {50, "BRAZIL", 1},
    {51, "CANADA", 1},      {52, "EGYPT", 4},          {53, "ETHIOPIA", 0},
    {54, "FRANCE", 3},      {55, "GERMANY", 3},        {56, "INDIA", 2},
    {57, "INDONESIA", 2},

    {65, "IRAN", 4},        {66, "IRAQ", 4},           {67, "JAPAN", 2},
    {68, "JORDAN", 4},      {69, "KENYA", 0},          {70, "MOROCCO", 0},
    {71, "MOZAMBIQUE", 0},  {72, "PERU", 1},           {73, "CHINA", 2},
    {74, "ROMANIA", 3},     {75, "SAUDI ARABIA", 4},   {76, "VIETNAM", 2},
    {77, "RUSSIA", 3},      {78, "UNITED KINGDOM", 3}, {79, "UNITED STATES", 1},
    {80, "CHINA", 2},       {81, "PAKISTAN", 2},       {82, "BANGLADESH", 2},
    {83, "MEXICO", 1},      {84, "PHILIPPINES", 2},    {85, "THAILAND", 2},
    {86, "ITALY", 3},       {87, "SOUTH AFRICA", 0},   {88, "SOUTH KOREA", 2},
    {89, "COLOMBIA", 1},    {90, "SPAIN", 3},

    {97, "UKRAINE", 3},     {98, "POLAND", 3},         {99, "SUDAN", 0},
    {100, "UZBEKISTAN", 2}, {101, "MALAYSIA", 2},      {102, "VENEZUELA", 1},
    {103, "NEPAL", 2},      {104, "AFGHANISTAN", 2},   {105, "NORTH KOREA", 2},
    {106, "TAIWAN", 2},     {107, "GHANA", 0},         {108, "IVORY COAST", 0},
    {109, "SYRIA", 4},      {110, "MADAGASCAR", 0},    {111, "CAMEROON", 0},
    {112, "SRI LANKA", 2},  {113, "ROMANIA", 3},       {114, "NETHERLANDS", 3},
    {115, "CAMBODIA", 2},   {116, "BELGIUM", 3},       {117, "GREECE", 3},
    {118, "PORTUGAL", 3},   {119, "ISRAEL", 4},        {120, "FINLAND", 3},
    {121, "SINGAPORE", 2},  {122, "NORWAY", 3}};

const char* DataSource::regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE",
                                     "MIDDLE EAST"};

vector<const char*> DataSource::cLastParts = {"BAR",   "OUGHT", "ABLE", "PRI",
                                              "PRES",  "ESE",   "ANTI", "CALLY",
                                              "ATION", "EING"};

vector<const char*> DataSource::tpchNouns = {
    "foxes",        "ideas",        "theodolites",    "pinto beans",
    "instructions", "dependencies", "excuses",        "platelets",
    "asymptotes",   "courts",       "dolphins",       "multipliers",
    "sauternes",    "warthogs",     "frets",          "dinos",
    "attainments",  "somas",        "Tiresias'",      "patterns",
    "forges",       "braids",       "hockey players", "frays",
    "warhorses",    "dugouts",      "notornis",       "epitaphs",
    "pearls",       "tithes",       "waters",         "orbits",
    "gifts",        "sheaves",      "depths",         "sentiments",
    "decoys",       "realms",       "pains",          "grouches",
    "escapades"};

vector<const char*> DataSource::tpchVerbs = {
    "sleep",  "wake",    "are",    "cajole",    "haggle",   "nag",     "use",
    "boost",  "affix",   "detect", "integrate", "maintain", "nod",     "was",
    "lose",   "sublate", "solve",  "thrash",    "promise",  "engage",  "hinder",
    "print",  "x-ray",   "breach", "eat",       "grow",     "impress", "mold",
    "poach",  "serve",   "run",    "dazzle",    "snooze",   "doze",    "unwind",
    "kindle", "play",    "hang",   "believe",   "doubt"};

vector<const char*> DataSource::tpchAdjectives = {
    "furious",  "sly",       "careful",  "blithe", "quick",  "fluffy",  "slow",
    "quiet",    "ruthless",  "thin",     "close",  "dogged", "daring",  "brave",
    "stealthy", "permanent", "enticing", "idle",   "busy",   "regular", "final",
    "ironic",   "even",      "bold",     "silent"};

vector<const char*> DataSource::tpchAdverbs = {
    "sometimes", "always",     "never",      "furiously",   "slyly",
    "carefully", "blithely",   "quickly",    "fluffily",    "slowly",
    "quietly",   "ruthlessly", "thinly",     "closely",     "doggedly",
    "daringly",  "bravely",    "stealthily", "permanently", "enticingly",
    "idly",      "busily",     "regularly",  "finally",     "ironically",
    "evenly",    "boldly",     "silently"};

vector<const char*> DataSource::tpchPrepositions = {
    "about",       "above",   "according to", "across", "after",
    "against",     "along",   "alongside of", "among",  "around",
    "at",          "atop",    "before",       "behind", "beneath",
    "beside",      "besides", "between",      "beyond", "by",
    "despite",     "during",  "except",       "for",    "from",
    "in place of", "inside",  "instead of",   "into",   "near",
    "of",          "on",      "outside",      "over",   "past",
    "since",       "through", "throughout",   "to",     "toward",
    "under",       "until",   "up",           "upon",   "without",
    "with",        "within"};

vector<const char*> DataSource::tpchTerminators = {".", ";", ":",
                                                   "?", "!", "--"};

vector<const char*> DataSource::tpchAuxiliaries = {"do",
                                                   "may",
                                                   "might",
                                                   "shall",
                                                   "will",
                                                   "would",
                                                   "can",
                                                   "could",
                                                   "should",
                                                   "ought to",
                                                   "must",
                                                   "will have to",
                                                   "shall have to",
                                                   "could have to",
                                                   "should have to",
                                                   "must have to",
                                                   "need to",
                                                   "try to"};

int DataSource::lastOlCount = 0;
int DataSource::warehouseCount = 0;

string DataSource::tpchText(int length) {
    string s = " ";
    for (int i = 0; i < 25; i++)
        s = s + (i == 0 ? "" : " ") + tpchSentence();
    int pos = randomUniformInt(0, s.length() - length);
    return s.substr(pos, length);
}

string DataSource::tpchSentence() {
    if (randomTrue(1 / 5))
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " +
               tpchTerminators[randomUniformInt(0, tpchTerminators.size() - 1)];
    else if (randomTrue(1 / 5))
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " +
               tpchPrepositionalPhrase() + " " +
               tpchTerminators[randomUniformInt(0, tpchTerminators.size() - 1)];
    else if (randomTrue(1 / 5))
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " +
               tpchNounPhrase() + " " +
               tpchTerminators[randomUniformInt(0, tpchTerminators.size() - 1)];
    else if (randomTrue(1 / 5))
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " +
               tpchVerbPhrase() + " " + tpchNounPhrase() + " " +
               tpchTerminators[randomUniformInt(0, tpchTerminators.size() - 1)];
    else
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " +
               tpchVerbPhrase() + " " + tpchPrepositionalPhrase() + " " +
               tpchTerminators[randomUniformInt(0, tpchTerminators.size() - 1)];
}

string DataSource::tpchNounPhrase() {
    if (randomTrue(1 / 4))
        return string(tpchNouns[randomUniformInt(0, tpchNouns.size() - 1)]);
    else if (randomTrue(1 / 4))
        return string(tpchAdjectives[randomUniformInt(0, tpchAdjectives.size() -
                                                             1)]) +
               " " + tpchNouns[randomUniformInt(0, tpchNouns.size() - 1)];
    else if (randomTrue(1 / 4))
        return string(tpchAdjectives[randomUniformInt(0, tpchAdjectives.size() -
                                                             1)]) +
               ", " +
               tpchAdjectives[randomUniformInt(0, tpchAdjectives.size() - 1)] +
               " " + tpchNouns[randomUniformInt(0, tpchNouns.size() - 1)];
    else
        return string(
                   tpchAdverbs[randomUniformInt(0, tpchAdverbs.size() - 1)]) +
               " " +
               tpchAdjectives[randomUniformInt(0, tpchAdjectives.size() - 1)] +
               " " + tpchNouns[randomUniformInt(0, tpchNouns.size() - 1)];
}

string DataSource::tpchVerbPhrase() {
    if (randomTrue(1 / 4))
        return string(tpchVerbs[randomUniformInt(0, tpchVerbs.size() - 1)]);
    else if (randomTrue(1 / 4))
        return string(tpchAuxiliaries[randomUniformInt(
                   0, tpchAuxiliaries.size() - 1)]) +
               " " + tpchVerbs[randomUniformInt(0, tpchVerbs.size() - 1)];
    else if (randomTrue(1 / 4))
        return string(tpchVerbs[randomUniformInt(0, tpchVerbs.size() - 1)]) +
               " " + tpchAdverbs[randomUniformInt(0, tpchAdverbs.size() - 1)];
    else
        return string(tpchAuxiliaries[randomUniformInt(
                   0, tpchAuxiliaries.size() - 1)]) +
               " " + tpchVerbs[randomUniformInt(0, tpchVerbs.size() - 1)] +
               " " + tpchAdverbs[randomUniformInt(0, tpchAdverbs.size() - 1)];
}

string DataSource::tpchPrepositionalPhrase() {
    return string(tpchPrepositions[randomUniformInt(0, tpchPrepositions.size() -
                                                           1)]) +
           " the " + tpchNounPhrase();
}

void DataSource::initialize(int wc) {
    srand(1382350201);
    warehouseCount = wc;
}

bool DataSource::randomTrue(double probability) {
    double value = rand() / double(RAND_MAX);
    return value < probability;
}

int DataSource::randomUniformInt(int minValue, int maxValue) {
    return (rand() % (maxValue - minValue + 1)) + minValue;
}

void DataSource::randomUniformInt(int minValue, int maxValue, int& ret) {
    ret = (rand() % (maxValue - minValue + 1)) + minValue;
}

void DataSource::randomNonUniformInt(int A, int x, int y, int C, int& ret) {
    ret = ((((randomUniformInt(0, A) | randomUniformInt(x, y)) + C) %
            (y - x + 1)) +
           x);
}

void DataSource::randomDouble(double minValue, double maxValue, int decimals,
                              double& ret) {
    int min = int(minValue * pow(10.0, decimals));
    int max = int(maxValue * pow(10.0, decimals));
    ret = double(randomUniformInt(min, max)) / pow(10.0, decimals);
}

int DataSource::permute(int value, int low, int high) {
    int range = high - low + 1;
    return ((value * 9973) % range) + low;
}

void DataSource::getCurrentTimestamp(SQL_TIMESTAMP_STRUCT& ret) {
    time_t rawtime;
    time(&rawtime);
    tm* timeinfo = localtime(&rawtime);

    ret.year = timeinfo->tm_year + 1900;
    ret.month = timeinfo->tm_mon + 1;
    ret.day = timeinfo->tm_mday;
    ret.hour = timeinfo->tm_hour;
    ret.minute = timeinfo->tm_min;
    ret.second = timeinfo->tm_sec;
    ret.fraction = 0;
}

void DataSource::genCLast(int value, string& ret) {
    ret = "";
    ret += cLastParts[value / 100];
    value %= 100;
    ret += cLastParts[value / 10];
    value %= 10;
    ret += cLastParts[value];
}

void DataSource::randomCLast(string& ret) {
    int value = 0;
    randomNonUniformInt(255, 0, 999, 173, value);
    genCLast(value, ret);
}

void DataSource::getRemoteWId(int& currentWId, int& ret) {
    if (warehouseCount == 1) {
        ret = currentWId;
    } else {
        ret = currentWId;
        while (ret == currentWId)
            randomUniformInt(1, warehouseCount, ret);
    }
}

int DataSource::nextOderlineCount() {
    if (lastOlCount == 0) {
        randomUniformInt(5, 15, lastOlCount);
        return 20 - lastOlCount;
    }

    int temp = lastOlCount;
    lastOlCount = 0;
    return temp;
}

void DataSource::addNumeric(int length, ofstream& stream, bool delimiter) {
    string s = "";
    for (int i = 0; i < length; i++) {
        s += randomUniformInt('0', '9');
    }
    stream << s;
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addAlphanumeric62(int length, ofstream& stream,
                                   bool delimiter) {
    string s = "";
    int rand;
    for (int i = 0; i < length; i++) {
        rand = 0;
        while (rand == 0 || (rand > '9' && rand < 'A') ||
               (rand > 'Z' && rand < 'a'))
            randomUniformInt('0', 'z', rand);
        s += rand;
    }
    stream << s;
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addAlphanumeric64(int length, ofstream& stream,
                                   bool delimiter) {
    string s = "";
    int rand;
    for (int i = 0; i < length; i++) {
        rand = 0;
        while (rand == 0 || (rand > '9' && rand < 63) ||
               (rand > 'Z' && rand < 'a'))
            randomUniformInt('0', 'z', rand);
        s += rand;
    }
    stream << s;
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addAlphanumeric64(int minLength, int maxLength,
                                   ofstream& stream, bool delimiter) {
    addAlphanumeric64(randomUniformInt(minLength, maxLength), stream,
                      delimiter);
}

void DataSource::addAlphanumeric64Original(int minLength, int maxLength,
                                           ofstream& stream, bool delimiter) {
    int rLength = 0;
    randomUniformInt(minLength, maxLength, rLength);
    int rPosition = 0;
    randomUniformInt(0, rLength - 8, rPosition);
    addAlphanumeric64(rPosition, stream, 0);
    stream << "ORIGINAL";
    addAlphanumeric64(rLength - 8 - rPosition, stream, 0);
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addTextString(int minLength, int maxLength, ofstream& stream,
                               bool delimiter) {
    stream << tpchText(randomUniformInt(minLength, maxLength));
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addTextStringCustomer(int minLength, int maxLength,
                                       const char* action, ofstream& stream,
                                       bool delimiter) {
    int rLength = 0;
    randomUniformInt(minLength, maxLength, rLength);
    int l1 = 0;
    randomUniformInt(0, rLength - 10 - 8, l1);
    int l2 = 0;
    randomUniformInt(0, rLength - l1 - 10 - 8, l2);
    int l3 = rLength - l1 - l2 - 18;
    stream << tpchText(l1);
    stream << "Customer";
    stream << tpchText(l2);
    stream << action;
    stream << tpchText(l3);
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addInt(int minValue, int maxValue, ofstream& stream,
                        bool delimiter) {
    stream << randomUniformInt(minValue, maxValue);
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addDouble(double minValue, double maxValue, int decimals,
                           ofstream& stream, bool delimiter) {
    double d = 0;
    randomDouble(minValue, maxValue, decimals, d);
    stream << d;
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addNId(ofstream& stream, bool delimiter) {
    int rand = 0;
    while (rand == 0 || (rand > '9' && rand < 'A') ||
           (rand > 'Z' && rand < 'a'))
        randomUniformInt('0', 'z', rand);
    stream << rand;
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addWDCZip(ofstream& stream, bool delimiter) {
    addNumeric(4, stream, 0);
    stream << "11111";
    if (delimiter)
        stream << csvDelim;
}

void DataSource::addSuPhone(int& suId, ofstream& stream, bool delimiter) {
    int country_code = (suId % 90) + 10; // ensure length 2
    stream << country_code << "-";
    addInt(100, 999, stream, 0);
    stream << "-";
    addInt(100, 999, stream, 0);
    stream << "-";
    addInt(1000, 9999, stream, 0);
    if (delimiter)
        stream << csvDelim;
}

string DataSource::getCurrentTimeString() {
    time_t rawtime;
    struct tm* timeinfo;
    char buffer[24];
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(buffer, 80, "%F %X", timeinfo);
    return string(buffer);
}

string DataSource::randomAlphanumeric62(int length) {
    string s = "";
    int rand;
    for (int i = 0; i < length; i++) {
        rand = 0;
        while (rand == 0 || (rand > '9' && rand < 'A') ||
               (rand > 'Z' && rand < 'a'))
            randomUniformInt('0', 'z', rand);
        s += rand;
    }
    return s;
}

string DataSource::strLeadingZero(int i, int zeros) {
    stringstream ss;
    ss << setw(zeros) << setfill('0') << i;
    return ss.str();
}

Nation DataSource::getNation(int i) { return nations[i]; }

const char* DataSource::getRegion(int i) { return regions[i]; }
