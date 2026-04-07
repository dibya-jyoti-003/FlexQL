/*
 * flexql client library implementation
 * Provides the C API: flexql_open / flexql_close / flexql_exec / flexql_free
 */
#include "../../include/flexql.h"
#include "../../include/network.h"
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

/* ── internal handle ─────────────────────────────────────────── */

struct FlexQL {
    int socket_fd = -1;
    std::string pipeline_buf;
    int pipeline_count = 0;
};

/* ── helpers ─────────────────────────────────────────────────── */

static char *make_errmsg(const std::string &msg) {
    char *buf = static_cast<char*>(std::malloc(msg.size() + 1));
    if (buf) std::memcpy(buf, msg.c_str(), msg.size() + 1);
    return buf;
}

/*
 * Parse the response string sent by the server.
 * Format:
 *   "OK"                        – non-result statement succeeded
 *   "ERROR:<msg>"               – failure
 *   "COLS:<n>\n<c1>\n...\nROW\n<v1>\n...\nROW\n..."
 */
static int parse_response(const std::string  &resp,
                           flexql_callback     callback,
                           void               *arg,
                           char              **errmsg) {
    if (resp.rfind("ERROR:", 0) == 0) {
        if (errmsg) *errmsg = make_errmsg(resp.substr(6));
        return FLEXQL_ERROR;
    }
    if (resp == "OK") return FLEXQL_OK;

    /* parse COLS */
    if (resp.rfind("COLS:", 0) != 0) {
        if (errmsg) *errmsg = make_errmsg("Unexpected server response");
        return FLEXQL_ERROR;
    }

    std::istringstream ss(resp);
    std::string line;

    std::getline(ss, line);             /* "COLS:<n>" */
    int n_cols = std::stoi(line.substr(5));

    std::vector<std::string> col_name_storage(n_cols);
    std::vector<char*>       col_names(n_cols);
    for (int i = 0; i < n_cols; ++i) {
        std::getline(ss, col_name_storage[i]);
        col_names[i] = const_cast<char*>(col_name_storage[i].c_str());
    }

    if (!callback) return FLEXQL_OK;

    /* read rows */
    while (std::getline(ss, line)) {
        if (line != "ROW") continue;
        std::vector<std::string> val_storage(n_cols);
        std::vector<char*>       values(n_cols);
        for (int i = 0; i < n_cols; ++i) {
            std::getline(ss, val_storage[i]);
            values[i] = const_cast<char*>(val_storage[i].c_str());
        }
        int rc = callback(arg, n_cols, values.data(), col_names.data());
        if (rc != 0) break;     /* callback requested abort */
    }
    return FLEXQL_OK;
}

/* ── API implementation ──────────────────────────────────────── */

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    /* resolve hostname */
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    std::string port_str = std::to_string(port);

    if (getaddrinfo(host, port_str.c_str(), &hints, &res) != 0 || !res)
        return FLEXQL_ERROR;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { freeaddrinfo(res); return FLEXQL_ERROR; }

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        close(fd); freeaddrinfo(res); return FLEXQL_ERROR;
    }
    freeaddrinfo(res);

    FlexQL *handle = new FlexQL();
    handle->socket_fd = fd;
    *db = handle;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    if (db->socket_fd >= 0) {
        /* flush any pending pipeline */
        if (!db->pipeline_buf.empty()) {
            flexql::net::send_msg(db->socket_fd, db->pipeline_buf);
            std::string resp;
            flexql::net::recv_msg(db->socket_fd, resp);
        }
        /* notify server */
        flexql::net::send_msg(db->socket_fd, ".exit");
        close(db->socket_fd);
    }
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL        *db,
                const char    *sql,
                flexql_callback callback,
                void          *arg,
                char         **errmsg) {
    if (!db || db->socket_fd < 0) {
        if (errmsg) *errmsg = make_errmsg("Invalid database handle");
        return FLEXQL_ERROR;
    }
    if (!sql) {
        if (errmsg) *errmsg = make_errmsg("NULL SQL string");
        return FLEXQL_ERROR;
    }

    std::string q(sql);
    /* identify if this is an INSERT statement */
    bool is_insert = (q.find("INSERT") == 0 || q.find("insert") == 0);

    if (is_insert) {
        if (!db->pipeline_buf.empty()) db->pipeline_buf += "\n";
        db->pipeline_buf += q;
        db->pipeline_count++;

        if (db->pipeline_count >= 10000) {
            if (!flexql::net::send_msg(db->socket_fd, db->pipeline_buf)) {
                if (errmsg) *errmsg = make_errmsg("Failed to send batch");
                return FLEXQL_ERROR;
            }
            std::string resp;
            if (!flexql::net::recv_msg(db->socket_fd, resp)) {
                if (errmsg) *errmsg = make_errmsg("Failed to receive batch response");
                return FLEXQL_ERROR;
            }
            db->pipeline_buf.clear();
            db->pipeline_count = 0;
            return parse_response(resp, callback, arg, errmsg);
        }
        return FLEXQL_OK;
    }

    /* For non-INSERT queries, flush any pending inserts first */
    if (!db->pipeline_buf.empty()) {
        flexql::net::send_msg(db->socket_fd, db->pipeline_buf);
        std::string dummy;
        flexql::net::recv_msg(db->socket_fd, dummy); /* ignore response of flushed inserts */
        db->pipeline_buf.clear();
        db->pipeline_count = 0;
    }

    /* send the main query */
    if (!flexql::net::send_msg(db->socket_fd, q)) {
        if (errmsg) *errmsg = make_errmsg("Failed to send query to server");
        return FLEXQL_ERROR;
    }

    /* receive response */
    std::string resp;
    if (!flexql::net::recv_msg(db->socket_fd, resp)) {
        if (errmsg) *errmsg = make_errmsg("Failed to receive response from server");
        return FLEXQL_ERROR;
    }

    return parse_response(resp, callback, arg, errmsg);
}

void flexql_free(void *ptr) {
    std::free(ptr);
}
