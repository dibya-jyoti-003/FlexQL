#include "../include/network.h"
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>

namespace flexql {
namespace net {

bool write_all(int fd, const char *buf, std::size_t n) {
    std::size_t sent = 0;
    while (sent < n) {
        ssize_t r = ::write(fd, buf + sent, n - sent);
        if (r <= 0) return false;
        sent += static_cast<std::size_t>(r);
    }
    return true;
}

bool read_all(int fd, char *buf, std::size_t n) {
    std::size_t got = 0;
    while (got < n) {
        ssize_t r = ::read(fd, buf + got, n - got);
        if (r <= 0) return false;
        got += static_cast<std::size_t>(r);
    }
    return true;
}

bool send_msg(int fd, const std::string &msg) {
    uint32_t len = htonl(static_cast<uint32_t>(msg.size()));
    if (!write_all(fd, reinterpret_cast<const char*>(&len), 4)) return false;
    return write_all(fd, msg.data(), msg.size());
}

bool recv_msg(int fd, std::string &out) {
    uint32_t len_net = 0;
    if (!read_all(fd, reinterpret_cast<char*>(&len_net), 4)) return false;
    uint32_t len = ntohl(len_net);
    if (len == 0) { out.clear(); return true; }
    out.resize(len);
    return read_all(fd, &out[0], len);
}

} /* namespace net */
} /* namespace flexql */
