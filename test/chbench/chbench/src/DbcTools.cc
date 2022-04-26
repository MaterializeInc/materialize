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

#include "DbcTools.h"

#include "Log.h"

bool DbcTools::fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator,
                     int pos) {
    SQLRETURN ret = SQLFetch(hStmt);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
        ret = SQLGetData(hStmt, pos, SQL_C_CHAR, buf, 1024, nIdicator);
        if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
            return true;
        }
    }
    Log::l1() << Log::tm() << "-fetch failed\n";
    return false;
}

bool DbcTools::setEnv(SQLHENV& hEnv) {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    if (reviewReturn(hEnv, SQL_HANDLE_ENV, ret, true)) {
        ret = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION,
                            (SQLPOINTER) SQL_OV_ODBC3, 0);
        if (reviewReturn(hEnv, SQL_HANDLE_ENV, ret, true))
            return true;
    }
    Log::l2() << Log::tm() << "-environment not set\n";
    return false;
}

bool DbcTools::connect(SQLHENV& hEnv, SQLHDBC& hDBC, const char* dsn,
                       const char* username, const char* password) {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDBC);
    if (reviewReturn(hDBC, SQL_HANDLE_DBC, ret, true)) {
        ret = SQLConnect(hDBC, (SQLCHAR*) dsn, SQL_NTS, (SQLCHAR*) username,
                         SQL_NTS, (SQLCHAR*) password, SQL_NTS);
        if (reviewReturn(hDBC, SQL_HANDLE_DBC, ret, true)) {
            Log::l1() << Log::tm() << "-dbs connected\n";
            return true;
        }
    }
    Log::l2() << Log::tm() << "-dbs not connected\n";
    return false;
}

bool DbcTools::autoCommitOff(SQLHDBC& hDBC) {
    SQLRETURN ret = SQLSetConnectAttr(hDBC, SQL_ATTR_AUTOCOMMIT,
                                      SQL_AUTOCOMMIT_OFF, SQL_NTS);
    if (reviewReturn(hDBC, SQL_HANDLE_DBC, ret, true))
        return true;
    Log::l2() << Log::tm() << "-autoCommitOff failed\n";
    return false;
}

bool DbcTools::allocAndPrepareStmt(SQLHDBC& hDBC, SQLHSTMT& hStmt,
                                   const char* stmt) {
    if (hStmt != nullptr) {
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        hStmt = nullptr;
    }
    if (SQL_SUCCESS == SQLAllocHandle(SQL_HANDLE_STMT, hDBC, &hStmt)) {
        if (SQL_SUCCESS == SQLPrepare(hStmt, (unsigned char*) stmt, SQL_NTS))
            return true;
    }
    Log::l1() << Log::tm() << "-prepare statement failed:\n" << stmt << "\n";
    return false;
}

bool DbcTools::resetStatement(SQLHSTMT& hStmt) {
    SQLRETURN ret = SQLFreeStmt(hStmt, SQL_CLOSE);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
        ret = SQLFreeStmt(hStmt, SQL_UNBIND);
        if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
            ret = SQLFreeStmt(hStmt, SQL_RESET_PARAMS);
            if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
                return true;
            }
        }
    }
    return false;
}

bool DbcTools::bind(SQLHSTMT& hStmt, int pos, int& value) {
    SQLRETURN ret = SQLBindParameter(hStmt, pos, SQL_PARAM_INPUT, SQL_C_DEFAULT,
                                     SQL_INTEGER, 0, 0, &value, 0, nullptr);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret))
        return true;
    Log::l1() << Log::tm() << "-bind int failed\n";
    return false;
}

bool DbcTools::bind(SQLHSTMT& hStmt, int pos, double& value) {
    SQLRETURN ret = SQLBindParameter(hStmt, pos, SQL_PARAM_INPUT, SQL_C_DOUBLE,
                                     SQL_DOUBLE, 0, 0, &value, 0, nullptr);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret))
        return true;
    Log::l1() << Log::tm() << "-bind double failed\n";
    return false;
}

bool DbcTools::bind(SQLHSTMT& hStmt, int pos, int bufferLength, char* buffer) {
    SQLRETURN ret = SQLBindParameter(hStmt, pos, SQL_PARAM_INPUT, SQL_C_CHAR,
                                     SQL_CHAR, bufferLength, 0, buffer, 0, nullptr);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret))
        return true;
    Log::l1() << Log::tm() << "-bind string failed\n";
    return false;
}

bool DbcTools::bind(SQLHSTMT& hStmt, int pos, SQL_TIMESTAMP_STRUCT& ts) {
    SQLRETURN ret =
        SQLBindParameter(hStmt, pos, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP,
                         SQL_TIMESTAMP, 0, 0, &ts, 0, nullptr);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret))
        return true;
    Log::l1() << Log::tm() << "-bind timestamp failed\n";
    return false;
}

bool DbcTools::executePreparedStatement(SQLHSTMT& hStmt) {
    SQLRETURN ret = SQLExecute(hStmt);
    if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret)) {
        return true;
    }
    Log::l1() << Log::tm() << "-prepared statement failed\n";
    return false;
}

bool DbcTools::executeServiceStatement(SQLHSTMT& hStmt, const char* stmt,
                                       bool showError) {
    if (resetStatement(hStmt)) {
        SQLRETURN ret = SQLExecDirect(hStmt, (SQLCHAR*) stmt, SQL_NTS);
        if (reviewReturn(hStmt, SQL_HANDLE_STMT, ret, true))
            return true;
    }
    if (showError)
        Log::l2() << Log::tm() << "-service statement failed:\n    " << stmt
                  << "\n";
    else
        Log::l1() << Log::tm() << "-service statement failed:\n    " << stmt
                  << "\n";
    return false;
}

bool DbcTools::reviewReturn(SQLHANDLE& handle, SQLSMALLINT handleType,
                            SQLRETURN& ret, bool showError) {

    if (SQL_SUCCESS == ret) {
        return true;
    }

    SQLCHAR sql_state_buffer[10] = {0};
    SQLINTEGER native_error = 0;
    SQLCHAR message_text_buffer[4096] = {0};
    SQLSMALLINT text_length = 0;

    SQLGetDiagRec(handleType, handle, 1, sql_state_buffer, &native_error,
                  message_text_buffer, 4096, &text_length);

    if (SQL_SUCCESS_WITH_INFO == ret) {
        if (showError)
            Log::l2() << Log::tm()
                      << "-info:\n    SQL_SUCCESS_WITH_INFO\n    SQLSTATE: "
                      << (const char*) sql_state_buffer
                      << "\n    NATIVE ERROR: " << native_error
                      << "\n    MESSAGE TEXT: "
                      << (const char*) message_text_buffer << "\n";
        else
            Log::l1() << Log::tm()
                      << "-info:\n    SQL_SUCCESS_WITH_INFO\n    SQLSTATE: "
                      << (const char*) sql_state_buffer
                      << "\n    NATIVE ERROR: " << native_error
                      << "\n    MESSAGE TEXT: "
                      << (const char*) message_text_buffer << "\n";
        return true;
    }
    std::string s;
    if (SQL_NEED_DATA == ret) {
        s = "SQL_NEED_DATA";
    } else if (SQL_STILL_EXECUTING == ret) {
        s = "SQL_STILL_EXECUTING";
    } else if (SQL_ERROR == ret) {
        s = "SQL_ERROR";
    } else if (SQL_NO_DATA == ret) {
        s = "SQL_NO_DATA";
    } else if (SQL_INVALID_HANDLE == ret) {
        s = "SQL_INVALID_HANDLE";
    } else {
        s = "UNEXPECTED ODBC RETURN";
    }
    if (showError)
        Log::l2() << Log::tm() << "-bad return:\n    " << s
                  << "\n    SQLSTATE: " << (const char*) sql_state_buffer
                  << "\n    NATIVE ERROR: " << native_error
                  << "\n    MESSAGE TEXT: " << (const char*) message_text_buffer
                  << "\n";
    else
        Log::l1() << Log::tm() << "-bad return:\n    " << s
                  << "\n    SQLSTATE: " << (const char*) sql_state_buffer
                  << "\n    NATIVE ERROR: " << native_error
                  << "\n    MESSAGE TEXT: " << (const char*) message_text_buffer
                  << "\n";
    return false;
}

bool DbcTools::fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos,
                     std::string& value) {
    if (fetch(hStmt, buf, nIdicator, pos)) {
        value = std::string((char*) buf);
        return true;
    }
    return false;
}

bool DbcTools::fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos,
                     int& value) {
    if (fetch(hStmt, buf, nIdicator, pos)) {
        value = strtol((char*) buf, nullptr, 0);
        return true;
    }
    return false;
}

bool DbcTools::fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos,
                     double& value) {
    if (fetch(hStmt, buf, nIdicator, pos)) {
        value = atof((char*) buf);
        return true;
    }
    return false;
}

bool DbcTools::commit(SQLHDBC& hDBC) {
    Log::l1() << Log::tm() << "-commit\n";
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, hDBC, SQL_COMMIT);
    if (reviewReturn(hDBC, SQL_HANDLE_DBC, ret)) {
        return true;
    }
    Log::l1() << Log::tm() << "-commit failed\n";
    return false;
}

bool DbcTools::rollback(SQLHDBC& hDBC) {
    Log::l1() << Log::tm() << "-rollback\n";
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, hDBC, SQL_ROLLBACK);
    if (reviewReturn(hDBC, SQL_HANDLE_DBC, ret)) {
        return true;
    }
    Log::l1() << Log::tm() << "-rollback failed\n";
    return false;
}
