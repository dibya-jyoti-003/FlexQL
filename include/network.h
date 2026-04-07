#pragma once
#include <string>
#include <cstdint>

/*
 * Wire protocol
 * -------------
 * Every message on the socket is framed as:
 *
 *   [ 4 bytes big-endian uint32 : payload length ]
 *   [ N bytes                   : payload UTF-8  ]
 *
 * This lets both sides know exactly how many bytes to read.
 */

namespace flexql {
namespace net {

/*
 * Send a length-prefixed message on the given socket fd.
 * Returns true on success.
 */
bool send_msg(int fd, const std::string &msg);

/*
 * Receive a length-prefixed message from the given socket fd.
 * Blocks until the full message arrives or the socket closes.
 * Returns true on success; fills `out` with the payload.
 */
bool recv_msg(int fd, std::string &out);

/*
 * Convenience: write exactly `n` bytes from `buf` to `fd`.
 * Returns true on success.
 */
bool write_all(int fd, const char *buf, std::size_t n);

/*
 * Convenience: read exactly `n` bytes into `buf` from `fd`.
 * Returns true on success.
 */
bool read_all(int fd, char *buf, std::size_t n);

} /* namespace net */
} /* namespace flexql */
