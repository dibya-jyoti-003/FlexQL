CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Iinclude
LDFLAGS  :=

# ── shared source files ────────────────────────────────────────
COMMON_SRCS := \
    src/parser/parser.cpp       \
    src/storage/storage.cpp     \
    src/index/index.cpp         \
    src/cache/cache.cpp         \
    src/query/executor.cpp      \
    src/network/network.cpp     \
    src/client/client.cpp

SERVER_SRCS  := $(COMMON_SRCS) src/server/server.cpp
CLIENT_SRCS  := $(COMMON_SRCS) src/client/repl.cpp
COMBINED_SRCS:= $(COMMON_SRCS) src/client/main_combined.cpp
TEST_SRCS    := $(COMMON_SRCS) tests/test_main.cpp
BENCHMARK_SRCS := benchmark_flexql.cpp src/client/client.cpp src/network/network.cpp

# ── targets ───────────────────────────────────────────────────
.PHONY: all clean test

all: server client benchmark

server: $(SERVER_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $(SERVER_SRCS) $(LDFLAGS) -lpthread

benchmark: $(BENCHMARK_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $(BENCHMARK_SRCS) $(LDFLAGS) -lpthread

client: $(CLIENT_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $(CLIENT_SRCS) $(LDFLAGS) -lpthread

clean:
	rm -f server client benchmark
	rm -rf data/
