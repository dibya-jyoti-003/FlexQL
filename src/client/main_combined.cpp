#include <sstream>
/*
 * flexql — combined entry point
 * Per spec: "Add server code here itself" (hidden note under Client section).
 * This binary can run as EITHER the client REPL or the server depending on
 * the first argument.
 *
 * Usage:
 *   ./flexql server [port]              — start the database server
 *   ./flexql client [host] [port]       — start interactive REPL
 *   ./flexql 127.0.0.1 9000             — shortcut: connect as client
 */
#include "../../include/flexql.h"
#include "../../include/executor.h"
#include "../../include/network.h"
#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <thread>
#include <mutex>

using namespace flexql;

/* ── global mutex for thread safety ──────────────────────────── */
static std::mutex g_exec_mutex;

/* ════════════════════════════════════════════
   SERVER
   ════════════════════════════════════════════ */

static std::string serialise(const ResultSet &rs) {
    if (!rs.ok()) return "ERROR:" + rs.error;
    
    /* For SELECT, SHOW TABLES, SHOW DATABASES, DESCRIBE, etc. */
    if (!rs.column_names.empty()) {
        std::ostringstream oss;
        oss << "COLS:" << rs.column_names.size() << "\n";
        for (auto &c : rs.column_names) oss << c << "\n";

        for (auto &row : rs.rows) {
            oss << "ROW\n";
            for (auto &v : row) oss << v << "\n";
        }
        return oss.str();
    }

    return "OK";
}

static void handle_client(int fd, Executor *exec) {
    std::string sql;
    while (net::recv_msg(fd, sql)) {
        if (sql == ".exit" || sql == "EXIT" || sql == "QUIT") break;
        
        ResultSet rs;
        {
            std::lock_guard<std::mutex> lock(g_exec_mutex);
            rs = exec->execute(sql);
        }
        
        if (!net::send_msg(fd, serialise(rs))) break;
    }
    close(fd);
}

static int run_server(int port) {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);
    if (bind(sfd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(sfd, 8) < 0) { perror("listen"); return 1; }
    std::cout << "[server] FlexQL server listening on port " << port << "\n";
    Executor exec;
    while (true) {
        sockaddr_in ca{}; socklen_t cl = sizeof(ca);
        int cfd = accept(sfd, (sockaddr*)&ca, &cl);
        if (cfd < 0) { perror("accept"); continue; }
        std::thread(handle_client, cfd, &exec).detach();
    }
    close(sfd);
    return 0;
}

/* ════════════════════════════════════════════
   CLIENT REPL
   ════════════════════════════════════════════ */

static bool g_first_row = true;

static int print_row(void*, int columnCount, char **values, char **columnNames) {
    if (g_first_row) { std::cout << "\n"; g_first_row = false; }
    for (int i = 0; i < columnCount; ++i)
        std::cout << columnNames[i] << " = " << (values[i] ? values[i] : "NULL") << "\n";
    std::cout << "\n";
    return 0;
}

static int run_client(const char *host, int port) {
    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server at " << host << ":" << port << "\n";
        return 1;
    }
    std::cout << "Connected to FlexQL server\n";
    std::string line, buffer;
    while (true) {
        std::cout << (buffer.empty() ? "flexql> " : "   ...> ") << std::flush;
        if (!std::getline(std::cin, line)) break;
        if (line == ".exit" || line == "exit" || line == "quit") {
            std::cout << "Connection closed\n"; break;
        }
        if (line == ".help" || line == "help" || line == "HELP") {
            std::cout
                << "\n  FlexQL commands:\n"
                << "  ─────────────────────────────────────────────────────\n"
                << "  SHOW DATABASES;                  list all databases\n"
                << "  SHOW TABLES;                     list all tables + row counts\n"
                << "  DESCRIBE <table>;                show table schema\n"
                << "  CREATE TABLE t (col TYPE, …);    create a table\n"
                << "  INSERT INTO t VALUES (…);        insert a row\n"
                << "  SELECT * FROM t;                 select all rows\n"
                << "  SELECT col1,col2 FROM t;         select specific columns\n"
                << "  SELECT * FROM t WHERE col = val; filter rows\n"
                << "  .help                            show this message\n"
                << "  .exit                            disconnect\n\n";
            buffer.clear();
            continue;
        }
        buffer += line;
        if (buffer.find(';') != std::string::npos) {
            char *errmsg = nullptr;
            g_first_row = true;
            int rc = flexql_exec(db, buffer.c_str(), print_row, nullptr, &errmsg);
            if (rc != FLEXQL_OK) {
                std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
                flexql_free(errmsg);
            } else {
                std::string up = buffer;
                for (auto &c : up) c = toupper(c);
                if (up.find("SELECT") == std::string::npos) std::cout << "OK\n";
            }
            buffer.clear();
        } else { buffer += " "; }
    }
    flexql_close(db);
    return 0;
}

/* ════════════════════════════════════════════
   MAIN
   ════════════════════════════════════════════ */

int main(int argc, char *argv[]) {
    /* ./flexql server [port] */
    if (argc >= 2 && std::string(argv[1]) == "server") {
        int port = (argc >= 3) ? std::stoi(argv[2]) : 9000;
        return run_server(port);
    }
    /* ./flexql client [host] [port] */
    if (argc >= 2 && std::string(argv[1]) == "client") {
        const char *host = (argc >= 3) ? argv[2] : "127.0.0.1";
        int port         = (argc >= 4) ? std::stoi(argv[3]) : 9000;
        return run_client(host, port);
    }
    /* ./flexql 127.0.0.1 9000 — shortcut (looks like original flexql-client usage) */
    if (argc >= 3) {
        return run_client(argv[1], std::stoi(argv[2]));
    }
    std::cerr << "Usage:\n"
              << "  ./flexql server [port]\n"
              << "  ./flexql client [host] [port]\n";
    return 1;
}
