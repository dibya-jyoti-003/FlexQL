# FlexQL

A lightweight, in-memory SQL-like database with a TCP server and C client API. Supports a subset of SQL optimized for OLTP workloads and includes persistence, indexing, and a benchmark suite.

---

## Features

- **SQL subset**: `CREATE TABLE`, `INSERT`, `SELECT`, `INNER JOIN`, `DELETE FROM`, `SHOW TABLES`, `SHOW DATABASES`, `DESCRIBE`
- **In-memory row-major storage** with primary-key hash indexing
- **Per-row expiry timestamps** (optional)
- **Thread-safe server** (one shared Executor, per-connection threads)
- **LRU query cache** (structure present; disabled for benchmark throughput)
- **Disk persistence** (schema + data files under `data/tables/`)
- **Network protocol**: length-prefixed messages over TCP
- **C client library** with pipelined INSERT support
- **Benchmark suite** (unit tests + large insert benchmark)

---

## Quick Start

### Build
```bash
make clean
make
```

Outputs:
- `./server`
- `./client`
- `./benchmark`

### Run server
```bash
./server 9000
```

### Run client REPL
```bash
./client 127.0.0.1 9000
```

### Run client with a query file
```bash
./client 127.0.0.1 9000 queries.sql
```

### Run benchmark
Unit tests only:
```bash
./benchmark --unit-test
```

Insertion benchmark (example: 100k rows):
```bash
./benchmark 100000
```

---

## Project Structure

```
flexql_safe_optimized/
├── include/           # Headers
│   ├── common.h
│   ├── storage.h
│   ├── index.h
│   ├── cache.h
│   ├── parser.h
│   ├── executor.h
│   ├── network.h
│   └── flexql.h
├── src/
│   ├── storage/      # Storage engine
│   ├── index/        # Indexing (primary + secondary)
│   ├── cache/        # LRU cache
│   ├── parser/       # SQL parsing
│   ├── query/        # Query execution engine
│   ├── network/      # Wire protocol
│   ├── server/       # TCP server
│   └── client/       # Client library + REPL
├── Makefile
├── DESIGN.md         # Design document
└── README.md         # This file
```

---

## Persistence

FlexQL persists tables under `data/tables/`:

- `<table>.schema` — schema metadata
- `<table>.data` — table data (append-friendly space-separated format)

On graceful shutdown (`SIGINT`/`SIGTERM`), the server flushes a consistent snapshot to disk. After restart, tables and rows are restored automatically.

---

## Protocol

Messages are length-prefixed (4-byte big-endian) UTF-8 strings.

Responses:
- `OK`
- `ERROR:<msg>`
- `COLS:<n>\n<col1>\n...\nROW\n<v1>\n...`

---

## Client API

```c
int flexql_open(const char *host, int port, FlexQL **db);
int flexql_exec(FlexQL *db, const char *sql, FlexQL_callback cb, void *arg, char **errmsg);
int flexql_close(FlexQL *db);
void flexql_free(void *ptr);
```

---

## Benchmark Results (sample)

`./benchmark 100000` on a typical machine:

- Rows inserted: `100000`
- Elapsed: `58 ms`
- Throughput: `1,724,137 rows/sec`

---

## Design Decisions

- **Row-major storage** for OLTP insert/point-lookup locality
- **Primary-key hash index** for O(1) point lookups
- **Secondary indexing** available but disabled in insert path for throughput
- **LRU cache** implemented but not consulted during query execution in benchmark
- **Batched inserts** with buffered file I/O and `posix_fallocate` for high throughput
- **Thread-per-connection server** with `shared_mutex` for safe concurrent reads/writes

See `DESIGN.md` for a detailed design document.

---

## License

This project is provided as part of academic coursework. See individual source files for details.
