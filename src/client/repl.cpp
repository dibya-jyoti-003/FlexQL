/*
 * flexql-client  –  interactive REPL
 *
 * Usage:  ./flexql-client <host> <port>
 * Example: ./flexql-client 127.0.0.1 9000
 */
#include "../../include/flexql.h"
#include <iostream>
#include <string>
#include <fstream>
#include <cstdio>

/* ── callback: print one result row ─────────────────────────── */

static bool g_first_row = true;   /* flag: print newline before first row */

static int print_row(void * /*data*/,
                     int    columnCount,
                     char **values,
                     char **columnNames) {
    if (g_first_row) {
        std::cout << "\n";   /* move off the "flexql> " prompt line */
        g_first_row = false;
    }
    for (int i = 0; i < columnCount; ++i) {
        std::cout << columnNames[i] << " = "
                  << (values[i] ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0;   /* continue */
}

/* ── main ────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    const char *host = "127.0.0.1";
    int         port = 9000;
    const char *file_path = nullptr;

    if (argc >= 4) {
        host = argv[1];
        port = std::stoi(argv[2]);
        file_path = argv[3];
    } else if (argc >= 3) {
        host = argv[1];
        port = std::stoi(argv[2]);
    } else if (argc == 2) {
        port = std::stoi(argv[1]);
    }

    /* connect */
    FlexQL *db  = nullptr;
    int     rc  = flexql_open(host, port, &db);
    if (rc != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server at "
                  << host << ":" << port << "\n";
        return 1;
    }
    std::cout << "Connected to FlexQL server\n";

    if (file_path) {
        std::ifstream in(file_path);
        if (!in.is_open()) {
            std::cerr << "Cannot open file: " << file_path << "\n";
            flexql_close(db);
            return 1;
        }

        std::string line;
        std::string buffer;
        while (std::getline(in, line)) {
            if (line.empty() || line == "\r") continue;
            buffer += line;
            if (buffer.find(';') != std::string::npos) {
                char *errmsg = nullptr;
                g_first_row = true;
                rc = flexql_exec(db, buffer.c_str(), print_row, nullptr, &errmsg);
                if (rc != FLEXQL_OK) {
                    std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
                    flexql_free(errmsg);
                    flexql_close(db);
                    return 1;
                }
                buffer.clear();
            } else {
                buffer += " ";
            }
        }

        if (!buffer.empty()) {
            char *errmsg = nullptr;
            g_first_row = true;
            rc = flexql_exec(db, buffer.c_str(), print_row, nullptr, &errmsg);
            if (rc != FLEXQL_OK) {
                std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
                flexql_free(errmsg);
                flexql_close(db);
                return 1;
            }
        }

        flexql_close(db);
        return 0;
    }

    /* REPL */
    std::string line;
    std::string buffer;     /* accumulate multi-line input until ';' */

    while (true) {
        std::cout << (buffer.empty() ? "flexql> " : "   ...> ") << std::flush;

        if (!std::getline(std::cin, line)) break;   /* EOF / Ctrl-D */

        /* handle exit commands */
        if (line == ".exit" || line == "exit" || line == "quit" || line == "EXIT") {
            std::cout << "Connection closed\n";
            break;
        }

        /* .help fires immediately without needing a semicolon */
        if (line == ".help" || line == "help" || line == "HELP") {
            std::cout
                << "\n  FlexQL commands:\n"
                << "  ─────────────────────────────────────────────────────\n"
                << "  SHOW DATABASES;                  list all databases\n"
                << "  SHOW TABLES;                     list all tables + row counts\n"
                << "  DESCRIBE <table>;                show column names, types, constraints\n"
                << "  DESC <table>;                    shorthand for DESCRIBE\n"
                << "  CREATE TABLE t (col TYPE, ...);  create a table\n"
                << "  INSERT INTO t VALUES (...);       insert a row\n"
                << "  SELECT * FROM t;                 select all rows\n"
                << "  SELECT col1,col2 FROM t;          select specific columns\n"
                << "  SELECT * FROM t WHERE col = val; filter rows\n"
                << "  SELECT * FROM a INNER JOIN b\n"
                << "    ON a.col = b.col;              join two tables\n"
                << "  .help                            show this message\n"
                << "  .exit                            disconnect\n"
                << "  ─────────────────────────────────────────────────────\n"
                << "  Types: DECIMAL  VARCHAR  TEXT  INT\n"
                << "  Expiry: INSERT INTO t VALUES(...) EXPIRE=<unix_timestamp>\n\n";
            buffer.clear();
            continue;
        }

        buffer += line;

        /* execute when we see a semicolon (or if the line is .exit) */
        if (buffer.find(';') != std::string::npos) {
            char *errmsg = nullptr;
            g_first_row = true;   /* reset before each query */
            rc = flexql_exec(db, buffer.c_str(), print_row, nullptr, &errmsg);
            if (rc != FLEXQL_OK) {
                std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
                flexql_free(errmsg);
            } else {
                /* print OK for DDL/DML (SELECT already printed via callback) */
                std::string up = buffer;
                for (auto &c : up) c = toupper(c);
                if (up.find("SELECT") == std::string::npos)
                    std::cout << "OK\n";
            }
            buffer.clear();
        } else {
            buffer += " ";   /* allow multi-line queries */
        }
    }

    flexql_close(db);
    return 0;
}
