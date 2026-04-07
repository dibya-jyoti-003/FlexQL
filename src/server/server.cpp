/*
 * flexql-server
 * -------------
 * Single-threaded TCP server that processes one client at a time.
 *
 * Wire protocol (see network.h):
 *   Client sends a length-prefixed SQL string.
 *   Server replies with a length-prefixed response string.
 *
 * Response format for SELECT:
 *   COLS:<n>\n<col1>\n<col2>\n...\nROW\n<v1>\n<v2>\n...\nROW\n...
 *   ERROR:<message>
 *   OK
 */
#include "../include/executor.h"
#include "../include/network.h"
#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <thread>
#include <mutex>
#include <csignal>
#include <atomic>

using namespace flexql;

/* ── response serialisation ──────────────────────────────────── */

static std::string serialise(const ResultSet &rs) {
    if (!rs.ok()) return "ERROR:" + rs.error;
    
    if (!rs.column_names.empty()) {
        std::string out;
        /* pre-reserve some space based on row count – 100 bytes per row is a decent heuristic */
        out.reserve(rs.column_names.size() * 32 + rs.rows.size() * 100);

        out += "COLS:";
        out += std::to_string(rs.column_names.size());
        out += "\n";
        for (auto &c : rs.column_names) {
            out += c;
            out += "\n";
        }

        for (auto &row : rs.rows) {
            out += "ROW\n";
            for (auto &v : row) {
                out += v;
                out += "\n";
            }
        }
        return out;
    }

    return "OK";
}

static std::atomic<bool> g_stop{false};
static int g_server_fd = -1;

static void handle_signal(int) {
    g_stop.store(true);
    if (g_server_fd >= 0) {
        close(g_server_fd);
        g_server_fd = -1;
    }
}

/* ── handle one connected client ─────────────────────────────── */

static void handle_client(int client_fd, Executor *exec) {
    std::string sql;

    while (net::recv_msg(client_fd, sql)) {
        if (sql == ".exit" || sql == "EXIT" || sql == "QUIT") break;
        
        ResultSet rs = exec->execute(sql);
        
        std::string r = serialise(rs);

        if (!net::send_msg(client_fd, r)) {
            std::cerr << "[server] send failed\n";
            break;
        }
    }
    close(client_fd);
}

/* ── main ────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    int port = 9000;
    if (argc >= 2) port = std::stoi(argv[1]);

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    g_server_fd = server_fd;

    /* allow quick restart without 'address already in use' */
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(server_fd, 8) < 0) {
        perror("listen"); return 1;
    }

    std::cout << "[server] FlexQL server listening on port " << port << "\n";

    /* one shared Executor — one database instance per server process */
    Executor exec;

    /* accept loop — multithreaded */
    while (!g_stop.load()) {
        sockaddr_in client_addr{};
        socklen_t   len = sizeof(client_addr);
        int client_fd   = accept(server_fd,
                                  reinterpret_cast<sockaddr*>(&client_addr),
                                  &len);
        if (client_fd < 0) {
            if (g_stop.load()) break;
            perror("accept");
            continue;
        }

        std::thread(handle_client, client_fd, &exec).detach();
    }

    exec.save_db();

    close(server_fd);
    return 0;
}
