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

#ifndef LOG_H
#define LOG_H

#include <fstream>
#include <iostream>
#include <string>

class LogTime {};

class Log1 {};

class Log2 {};

class Log {

  private:
    static LogTime lt;
    static Log1 log1;
    static Log2 log2;
    static std::ofstream logStream;

  public:
    static LogTime& tm();
    static Log1& l1();
    static Log2& l2();
    static std::ofstream* getLogStream();
    static void open(const std::string& path);
};

Log1& operator<<(Log1& l, LogTime& lt);
Log1& operator<<(Log1& l, const char* c);
Log1& operator<<(Log1& l, const std::string& s);
Log1& operator<<(Log1& l, double d);

Log2& operator<<(Log2& l, LogTime& lt);
Log2& operator<<(Log2& l, const char* c);
Log2& operator<<(Log2& l, const std::string& s);
Log2& operator<<(Log2& l, double d);

#endif
