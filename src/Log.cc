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

#include "Log.h"

#define WRITE_LOG_FILE

using namespace std;

LogTime Log::lt;
Log1 Log::log1;
Log2 Log::log2;
ofstream Log::logStream;

LogTime& Log::tm() { return lt; }

Log1& Log::l1() { return log1; }

Log2& Log::l2() { return log2; }

void Log::open(const std::string& path) {
    logStream.open(path);
    // TODO(benesch): check for errors.
}

ofstream* Log::getLogStream() {
    return &logStream;
}

Log1& operator<<(Log1& l, LogTime&) {
    time_t rawtime;
    time(&rawtime);
    struct tm* timeinfo = localtime(&rawtime);
    char buffer[24];
    strftime(buffer, 24, "%r: ", timeinfo);
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << buffer;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log1& operator<<(Log1& l, const char* c) {
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << c;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log1& operator<<(Log1& l, string s) {
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << s;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log1& operator<<(Log1& l, double d) {
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << d;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log2& operator<<(Log2& l, LogTime&) {
    time_t rawtime;
    time(&rawtime);
    struct tm* timeinfo = localtime(&rawtime);
    char buffer[24];
    strftime(buffer, 24, "%r: ", timeinfo);

#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << buffer;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log2& operator<<(Log2& l, const char* c) {
    cout << c;
    cout.flush();
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << c;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log2& operator<<(Log2& l, string s) {
    cout << s;
    cout.flush();
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << s;
    Log::getLogStream()->flush();
#endif
    return l;
};

Log2& operator<<(Log2& l, double d) {
    cout << d;
    cout.flush();
#ifdef WRITE_LOG_FILE
    *Log::getLogStream() << d;
    Log::getLogStream()->flush();
#endif
    return l;
};
