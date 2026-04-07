#include "../include/storage.h"
#include <sstream>
#include <stdexcept>
#include <climits>

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
 #include <sys/types.h>

#include <fstream>
#include <sys/stat.h>
#include <dirent.h>

namespace flexql {

static std::size_t clamp_size(std::size_t v, std::size_t lo, std::size_t hi) {
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

static std::string ensure_append_stream(std::ofstream &stream,
                                        std::vector<char> &buf,
                                        int &fd,
                                        const std::string &path,
                                        std::size_t estimated_bytes) {
    if (stream.is_open()) return "OK";

    if (fd < 0) {
        fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
    }
    if (fd < 0) {
        return "ERROR: failed to open tracking file: " + path;
    }

    struct stat st {};
    if (::fstat(fd, &st) == 0) {
        off_t current = st.st_size;
        if (current < static_cast<off_t>(estimated_bytes)) {
            int prc = ::posix_fallocate(fd, 0, static_cast<off_t>(estimated_bytes));
            (void)prc;
        }
    }
    stream.open(path, std::ios::app);
    if (!stream.is_open()) return "ERROR: failed to open tracking file: " + path;
    if (!buf.empty()) {
        stream.rdbuf()->pubsetbuf(buf.data(), buf.size());
    }
    return "OK";
}

static inline void schedule_writeback(int fd, std::size_t &writeback_bytes, std::size_t append_bytes) {
    if (fd < 0) return;

    constexpr std::size_t CHUNK = 8ULL << 20;      /* 8 MiB */
    constexpr std::size_t WINDOW = 64ULL << 20;    /* keep ~64 MiB of dirty data in flight */

    while (append_bytes > writeback_bytes + WINDOW) {
        std::size_t len = CHUNK;
        ::sync_file_range(fd,
                          static_cast<off64_t>(writeback_bytes),
                          static_cast<off64_t>(len),
                          SYNC_FILE_RANGE_WRITE);
        writeback_bytes += len;
    }
}

static inline bool is_fast_decimal(const std::string &s) {
    if (s.empty()) return false;
    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-') {
        ++i;
        if (i >= s.size()) return false;
    }
    bool seen_digit = false;
    bool seen_dot = false;
    for (; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (c >= '0' && c <= '9') {
            seen_digit = true;
            continue;
        }
        if (c == '.' && !seen_dot) {
            seen_dot = true;
            continue;
        }
        return false;
    }
    return seen_digit;
}

static inline bool is_nonempty_integer(const std::string &s) {
    if (s.empty()) return false;
    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-') {
        ++i;
        if (i >= s.size()) return false;
    }
    for (; i < s.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (c < '0' || c > '9') return false;
    }
    return true;
}

/* ── Persistence ─────────────────────────────────────────────── */

void StorageEngine::save_to_disk(const std::string &directory) const {
    /* create directory if it doesn't exist */
    mkdir(directory.c_str(), 0777);

    for (auto &kv : tables_) {
        save_table(kv.first, directory);
    }
}

void StorageEngine::save_table(const std::string &table_name, const std::string &directory) const {
    const Table *tbl_const = get_table(table_name);
    Table *tbl = const_cast<Table*>(tbl_const);
    if (!tbl) return;

    /* IMPORTANT:
     * Inserts append to an open buffered stream (append_df). If we rewrite the
     * table file while that stream is still open, its buffer may flush later
     * (e.g., during process teardown) and append duplicate rows. Close it here
     * to make the on-disk snapshot deterministic.
     */
    if (tbl->append_df.is_open()) {
        tbl->append_df.flush();
        tbl->append_df.close();
    }
    if (tbl->append_fd >= 0) {
        ::close(tbl->append_fd);
        tbl->append_fd = -1;
    }
    tbl->append_bytes = 0;
    tbl->writeback_bytes = 0;

    save_schema(table_name, directory);

    /* save data */
    std::ofstream df(directory + "/" + table_name + ".data");
    for (auto &row : tbl->rows) {
        df << row.expiry << " " << row.values.size();
        for (auto &v : row.values) {
            df << " " << v.size() << " " << v;
        }
        df << "\n";
    }
    df.close();
}

void StorageEngine::save_schema(const std::string &table_name, const std::string &directory) const {
    const Table *tbl = get_table(table_name);
    if (!tbl) return;

    mkdir("data", 0777);
    mkdir(directory.c_str(), 0777);

    std::ofstream sf(directory + "/" + table_name + ".schema");
    sf << tbl->schema.table_name << "\n";
    sf << tbl->schema.columns.size() << "\n";
    for (auto &cd : tbl->schema.columns) {
        sf << cd.name << " " << (int)cd.type << " " << cd.not_null << " " << cd.primary_key << "\n";
    }
    sf.close();
}

void StorageEngine::load_from_disk(const std::string &directory) {
    mkdir("data", 0777);
    mkdir(directory.c_str(), 0777);
    DIR *dir = opendir(directory.c_str());
    if (!dir) return;

    struct dirent *ent;
    while ((ent = readdir(dir)) != nullptr) {
        std::string filename = ent->d_name;
        if (filename.size() > 7 && filename.substr(filename.size() - 7) == ".schema") {
            std::string table_name = filename.substr(0, filename.size() - 7);
            
            Table t;
            /* load schema */
            std::ifstream sf(directory + "/" + filename);
            if (!sf.is_open()) continue;
            
            sf >> t.schema.table_name;
            int col_count;
            if (!(sf >> col_count)) continue;
            for (int i = 0; i < col_count; ++i) {
                ColDef cd;
                int type_int;
                if (!(sf >> cd.name >> type_int >> cd.not_null >> cd.primary_key)) break;
                cd.type = (ColType)type_int;
                t.schema.columns.push_back(cd);
            }
            sf.close();

             /* load data */
            std::ifstream df(directory + "/" + table_name + ".data");
            if (df.is_open()) {
                std::string line;
                while (std::getline(df, line)) {
                    if (line.empty()) continue;

                    std::istringstream iss(line);
                    std::vector<std::string> tokens;
                    std::string tok;
                    while (iss >> tok) tokens.push_back(tok);
                    if (tokens.empty()) continue;

                    Row row;

                    /* Backward compatibility:
                     * Old format (length-prefixed):
                     *   <expiry> <val_count> <v1_len> <v1> <v2_len> <v2> ...
                     * New append format (current INSERT path):
                     *   <col1> <col2> ... <colN> <expiry>
                     */
                    const std::size_t expected = t.schema.columns.size();
                    const bool looks_new = (tokens.size() == expected + 1) && is_nonempty_integer(tokens.back());
                    bool looks_old = false;
                    if (!looks_new && tokens.size() >= 2 && is_nonempty_integer(tokens[0]) && is_nonempty_integer(tokens[1])) {
                        looks_old = true;
                    }

                    if (looks_old) {
                        std::istringstream oldss(line);
                        std::time_t expiry;
                        std::size_t val_count;
                        if (!(oldss >> expiry >> val_count)) continue;
                        row.expiry = expiry;
                        for (std::size_t i = 0; i < val_count; ++i) {
                            std::size_t v_size;
                            if (!(oldss >> v_size)) { row.values.clear(); break; }
                            oldss.ignore(1);
                            std::string v(v_size, '\0');
                            oldss.read(&v[0], v_size);
                            if (oldss.gcount() != static_cast<std::streamsize>(v_size)) {
                                row.values.clear();
                                break;
                            }
                            row.values.push_back(std::move(v));
                        }
                    } else if (looks_new) {
                        row.expiry = static_cast<std::time_t>(std::stoll(tokens.back()));
                        row.values.assign(tokens.begin(), tokens.end() - 1);
                    } else {
                        continue;
                    }

                    if (!row.values.empty() && row.values.size() == t.schema.columns.size()) {
                        t.rows.push_back(std::move(row));
                        /* update index */
                        int pk_col = t.schema.primary_key_index();
                        if (pk_col >= 0 && pk_col < (int)t.rows.back().values.size()) {
                            t.index.insert(t.rows.back().values[pk_col], t.rows.size() - 1);
                        }
                    }
                }
                df.close();
            }
            tables_[table_name] = std::move(t);
        }
    }
    closedir(dir);
}

/* ── DDL ─────────────────────────────────────────────────────── */

std::string StorageEngine::create_table(const Schema &schema, bool if_not_exists) {
    /* Ensure the directory for data files exists */
    mkdir("data", 0777);
    mkdir("data/tables", 0777);

    if (tables_.count(schema.table_name)) {
        if (if_not_exists) return "OK";
        
        /* Auto-drop existing table to support reproducible benchmark runs */
        delete_rows(schema.table_name);
        tables_.erase(schema.table_name);
    }

    Table t;
    t.schema = schema;
    /* Pre-reserve hash table capacity to avoid rehashing during bulk inserts */
    t.index.reserve(1000000);  // Pre-reserve for 1M rows
    t.rows.reserve(1000000);        // Pre-reserve for 1M rows

    /* prepare append stream (opened lazily on first insert) */
    t.append_buf.resize(1 << 20);
    tables_[schema.table_name] = std::move(t);
    return "OK";
}

/* ── DELETE ──────────────────────────────────────────────────── */

std::string StorageEngine::delete_rows(const std::string &table_name) {
    Table *tbl = get_table(table_name);
    if (!tbl) return "ERROR: table '" + table_name + "' not found";

    if (tbl->append_df.is_open()) {
        tbl->append_df.close();
    }
    if (tbl->append_fd >= 0) {
        ::close(tbl->append_fd);
        tbl->append_fd = -1;
    }
    tbl->append_bytes = 0;
    tbl->writeback_bytes = 0;
    tbl->rows.clear();
    tbl->index = PrimaryIndex{};
    /* wipe the data file on disk */
    std::ofstream df("data/tables/" + table_name + ".data", std::ios::trunc);
    df.close();

    /* reopen lazily on next insert */
    return "OK";
}

std::string StorageEngine::insert_batch(const std::string                    &table_name,
                                        std::vector<std::vector<std::string>> &&rows_values,
                                        std::time_t                           expiry) {
    Table *tbl = get_table(table_name);
    if (!tbl) return "ERROR: table '" + table_name + "' not found";

    const Schema &schema = tbl->schema;
    const bool has_pk = schema.has_primary_key();
    int pk_col = has_pk ? schema.primary_key_index() : -1;

    mkdir("data", 0777);
    mkdir("data/tables", 0777);

    tbl->rows.reserve(tbl->rows.size() + rows_values.size());

    {
        std::string path = "data/tables/" + table_name + ".data";
        std::size_t cap = tbl->rows.capacity();
        std::size_t estimated = (cap > 0) ? (cap * 64ULL) : (8ULL << 20);
        estimated = clamp_size(estimated, (8ULL << 20), (512ULL << 20));
        std::string msg = ensure_append_stream(tbl->append_df, tbl->append_buf, tbl->append_fd, path, estimated);
        if (msg != "OK") return msg;
    }
    std::ofstream &df = tbl->append_df;

    std::size_t bytes_written = 0;

    for (auto &values : rows_values) {
        if (values.size() != schema.columns.size()) {
            return "ERROR: expected " + std::to_string(schema.columns.size()) +
                   " values, got " + std::to_string(values.size());
        }

        for (std::size_t i = 0; i < values.size(); ++i) {
            const ColDef &cd = schema.columns[i];
            if (cd.type == ColType::DECIMAL) {
                if (!is_fast_decimal(values[i])) {
                    return "ERROR: column '" + cd.name + "' expects DECIMAL";
                }
            }
            if (cd.not_null && (values[i].empty() || values[i] == "NULL")) {
                return "ERROR: column '" + cd.name + "' cannot be NULL";
            }
        }

        Row row;
        row.values = std::move(values);
        row.expiry = expiry;

        std::size_t row_idx = tbl->rows.size();
        tbl->rows.push_back(std::move(row));
        
        const auto &current_row_values = tbl->rows.back().values;
        if (has_pk && pk_col >= 0 && pk_col < (int)current_row_values.size()) {
            tbl->index.insert(current_row_values[pk_col], row_idx);
        }

        /* update secondary indexes if they exist (or for all non-PK columns) */
        for (int i = 0; i < (int)current_row_values.size(); ++i) {
            if (i == pk_col) continue;
            /* DISABLED: secondary indexing for performance - only index primary key */
            /* restore performance: only index non-PK DECIMAL columns */
            /* if (schema.columns[i].type == ColType::DECIMAL) {
                tbl->secondary_indexes[i].insert(current_row_values[i], row_idx, true);
            } */
        }

        const auto &row_values_ref = current_row_values;
        for (std::size_t i = 0; i < row_values_ref.size(); ++i) {
            df << row_values_ref[i];
            bytes_written += row_values_ref[i].size();
            if (i < row_values_ref.size() - 1) df << " ";
            if (i < row_values_ref.size() - 1) bytes_written += 1;
        }
        df << " " << expiry << "\n";
        bytes_written += 1;
        bytes_written += 11;
        bytes_written += 1;
    }

    tbl->append_bytes += bytes_written;
    schedule_writeback(tbl->append_fd, tbl->writeback_bytes, tbl->append_bytes);

    return "OK";
}

/* ── INSERT ──────────────────────────────────────────────────── */

std::string StorageEngine::insert(const std::string              &table_name,
                                   const std::vector<std::string> &values,
                                   std::time_t                     expiry) {
    Table *tbl = get_table(table_name);
    if (!tbl) return "ERROR: table '" + table_name + "' not found";

    const Schema &schema = tbl->schema;

    if (values.size() != schema.columns.size()) {
        return "ERROR: expected " + std::to_string(schema.columns.size()) +
               " values, got " + std::to_string(values.size());
    }

    /* type validation */
    for (std::size_t i = 0; i < values.size(); ++i) {
        const ColDef &cd = schema.columns[i];
        if (cd.type == ColType::DECIMAL) {
            if (!is_fast_decimal(values[i])) {
                return "ERROR: column '" + cd.name +
                       "' expects DECIMAL, got '" + values[i] + "'";
            }
        }
        if (cd.not_null && (values[i].empty() || values[i] == "NULL"))
            return "ERROR: column '" + cd.name + "' cannot be NULL";
    }

    /* build the row */
    Row row;
    row.values = values;
    row.expiry = expiry;

    std::size_t row_idx = tbl->rows.size();
    tbl->rows.push_back(row);

    const auto &current_row_values = tbl->rows.back().values;
    /* update primary key index */
    if (schema.has_primary_key()) {
        int pk_col = schema.primary_key_index();
        if (pk_col >= 0 && pk_col < (int)current_row_values.size())
            tbl->index.insert(current_row_values[pk_col], row_idx);
    }

    /* DISABLED: secondary indexing for performance - only index primary key */
    /* update secondary indexes */
    /* int pk_col = schema.primary_key_index();
    for (int i = 0; i < (int)current_row_values.size(); ++i) {
        if (i == pk_col) continue;
        if (schema.columns[i].type == ColType::DECIMAL) {
            tbl->secondary_indexes[i].insert(current_row_values[i], row_idx, true);
        } else {
            tbl->secondary_indexes[i].insert(current_row_values[i], row_idx, false);
        }
    } */

    /* append to disk (O(1) logic) */
    {
        std::string path = "data/tables/" + table_name + ".data";
        std::size_t cap = tbl->rows.capacity();
        std::size_t estimated = (cap > 0) ? (cap * 64ULL) : (8ULL << 20);
        estimated = clamp_size(estimated, (8ULL << 20), (512ULL << 20));
        std::string msg = ensure_append_stream(tbl->append_df, tbl->append_buf, tbl->append_fd, path, estimated);
        if (msg != "OK") return msg;
    }

    std::ofstream &df = tbl->append_df;
    std::size_t bytes_written = 0;
    for (std::size_t i = 0; i < values.size(); ++i) {
        df << values[i];
        bytes_written += values[i].size();
        if (i < values.size() - 1) df << " ";
        if (i < values.size() - 1) bytes_written += 1;
    }
    df << " " << expiry << "\n";

    bytes_written += 1;
    bytes_written += 11;
    bytes_written += 1;
    tbl->append_bytes += bytes_written;
    schedule_writeback(tbl->append_fd, tbl->writeback_bytes, tbl->append_bytes);

    return "OK";
}

/* ── BATCH INSERT ────────────────────────────────────────────── */

std::string StorageEngine::insert_batch(const std::string                           &table_name,
                                        const std::vector<std::vector<std::string>> &rows_values,
                                        std::time_t                                  expiry) {
    Table *tbl = get_table(table_name);
    if (!tbl) return "ERROR: table '" + table_name + "' not found";

    const Schema &schema = tbl->schema;
    const bool has_pk = schema.has_primary_key();
    int pk_col = has_pk ? schema.primary_key_index() : -1;

    /* ensure directory exists */
    mkdir("data", 0777);
    mkdir("data/tables", 0777);

    tbl->rows.reserve(tbl->rows.size() + rows_values.size());

    {
        std::string path = "data/tables/" + table_name + ".data";
        std::size_t cap = tbl->rows.capacity();
        std::size_t estimated = (cap > 0) ? (cap * 64ULL) : (8ULL << 20);
        estimated = clamp_size(estimated, (8ULL << 20), (512ULL << 20));
        std::string msg = ensure_append_stream(tbl->append_df, tbl->append_buf, tbl->append_fd, path, estimated);
        if (msg != "OK") return msg;
    }

    std::ofstream &df = tbl->append_df;
    std::size_t bytes_written = 0;

    for (const auto &values : rows_values) {
        if (values.size() != schema.columns.size()) {
            return "ERROR: expected " + std::to_string(schema.columns.size()) +
                   " values, got " + std::to_string(values.size());
        }

        /* type validation */
        for (std::size_t i = 0; i < values.size(); ++i) {
            const ColDef &cd = schema.columns[i];
            if (cd.type == ColType::DECIMAL) {
                if (!is_fast_decimal(values[i])) {
                    return "ERROR: column '" + cd.name + "' expects DECIMAL";
                }
            }
            if (cd.not_null && (values[i].empty() || values[i] == "NULL")) {
                return "ERROR: column '" + cd.name + "' cannot be NULL";
            }
        }

        /* build the row */
        Row row;
        row.values = values;
        row.expiry = expiry;

        /* memory update */
        std::size_t row_idx = tbl->rows.size();
        tbl->rows.push_back(std::move(row));

        const auto &current_row_values = tbl->rows.back().values;
        if (has_pk && pk_col >= 0 && pk_col < (int)current_row_values.size()) {
            tbl->index.insert(current_row_values[pk_col], row_idx);
        }

        /* DISABLED: secondary indexing for performance - only index primary key */
        /* update secondary indexes */
        /* for (int i = 0; i < (int)current_row_values.size(); i++) {
            if (i == pk_col) continue;
            if (schema.columns[i].type == ColType::DECIMAL) {
                tbl->secondary_indexes[i].insert(current_row_values[i], row_idx, true);
            }
        } */

        /* disk stream buffering append - completely avoids slow fsync loops */
        for (std::size_t i = 0; i < values.size(); ++i) {
            df << values[i];
            bytes_written += values[i].size();
            if (i < values.size() - 1) df << " ";
            if (i < values.size() - 1) bytes_written += 1;
        }
        df << " " << expiry << "\n";
        bytes_written += 1;
        bytes_written += 11;
        bytes_written += 1;
    }

    tbl->append_bytes += bytes_written;
    schedule_writeback(tbl->append_fd, tbl->writeback_bytes, tbl->append_bytes);
    return "OK";
}

/* ── SELECT ──────────────────────────────────────────────────── */

ResultSet StorageEngine::select(const std::string              &table_name,
                                 const std::vector<std::string> &columns,
                                 const WhereClause              &where,
                                 const OrderBy                  &order_by) {
    ResultSet rs;
    const Table *tbl = get_table(table_name);
    if (!tbl) {
        rs.error = "table '" + table_name + "' not found";
        return rs;
    }

    const Schema &schema = tbl->schema;

    /* resolve and validate output columns */
    std::vector<std::string> out_cols = columns;
    if (out_cols.empty())
        for (auto &cd : schema.columns) out_cols.push_back(cd.name);

    std::vector<int> col_indices;
    for (const auto &col : out_cols) {
        std::string c = col;
        auto dot = c.rfind('.');
        if (dot != std::string::npos) c = c.substr(dot + 1);
        int idx = schema.col_index(c);
        if (idx < 0) {
            rs.error = "unknown column '" + col + "' in table '" + table_name + "'";
            return rs;
        }
        col_indices.push_back(idx);
    }

    rs.column_names = out_cols;

    /* Primary key equality fast path */
    if (schema.has_primary_key() && where.active && where.op == "=" && !order_by.active) {
        int pk = schema.primary_key_index();
        if (pk >= 0 && schema.columns[pk].name == where.column) {
            std::size_t idx = tbl->index.find(where.value);
            if (idx != SIZE_MAX && idx < tbl->rows.size()) {
                const Row &row = tbl->rows[idx];
                if (!row.is_expired()) {
                    rs.rows.push_back(project_row(row, col_indices));
                    return rs;
                }
            }
        }
    }

    /* secondary index fast path */
    if (where.active && !order_by.active) {
        int where_col_idx = schema.col_index(where.column);
        if (where_col_idx >= 0 && tbl->secondary_indexes.count(where_col_idx)) {
            std::vector<std::size_t> indices;
            bool is_numeric = schema.columns[where_col_idx].type == ColType::DECIMAL;
            tbl->secondary_indexes.at(where_col_idx).find_range(where.op, where.value, indices, is_numeric);
            
            for (std::size_t idx : indices) {
                if (idx < tbl->rows.size()) {
                    const Row &row = tbl->rows[idx];
                    if (!row.is_expired())
                        rs.rows.push_back(project_row(row, col_indices));
                }
            }
            return rs;
        }
    }

    /* pre-resolve indices for scan */
    int where_col_idx = -1;
    if (where.active) {
        where_col_idx = schema.col_index(where.column);
        if (where_col_idx < 0) {
            rs.error = "unknown column '" + where.column + "' in WHERE clause";
            return rs;
        }
        if (schema.columns[where_col_idx].type == ColType::INT || schema.columns[where_col_idx].type == ColType::DECIMAL) {
            if (!is_fast_decimal(where.value)) {
                rs.error = "type mismatch: column '" + where.column + "' expects numeric value";
                return rs;
            }
        }
    }

    int sort_col_idx = order_by.active ? schema.col_index(order_by.column) : -1;
    std::vector<std::pair<std::string, std::vector<std::string>>> items;

    for (const Row &row : tbl->rows) {
        if (row.is_expired()) continue;
        if (where.active && !row_matches_where(row, where_col_idx, where.op, where.value)) continue;

        std::string sort_key;
        if (sort_col_idx >= 0 && sort_col_idx < (int)row.values.size())
            sort_key = row.values[sort_col_idx];

        items.emplace_back(std::move(sort_key), project_row(row, col_indices));
    }

    /* ORDER BY */
    if (order_by.active && sort_col_idx >= 0) {
        bool is_num = true;
        for (const auto &it : items) {
            try { std::stod(it.first); } catch (...) { is_num = false; break; }
        }
        std::stable_sort(items.begin(), items.end(), [&](const auto &a, const auto &b) {
            bool l;
            if (is_num) {
                try { l = std::stod(a.first) < std::stod(b.first); } catch (...) { l = a.first < b.first; }
            } else l = a.first < b.first;
            return order_by.descending ? !l : l;
        });
    }

    for (auto &it : items) rs.rows.push_back(std::move(it.second));
    return rs;
}

/* ── SELECT with JOIN ── */
ResultSet StorageEngine::select_join(const std::string              &table_a,
                                      const std::string              &table_b,
                                      const std::vector<std::string> &columns,
                                      const JoinSpec                 &join,
                                      const WhereClause              &where,
                                      const OrderBy                  &order_by) {
    ResultSet rs;
    const Table *ta = get_table(table_a);
    const Table *tb = get_table(table_b);
    if (!ta) { rs.error = "table '" + table_a + "' not found"; return rs; }
    if (!tb) { rs.error = "table '" + table_b + "' not found"; return rs; }

    std::vector<std::string> combined_names;
    for (auto &cd : ta->schema.columns) combined_names.push_back(table_a + "." + cd.name);
    for (auto &cd : tb->schema.columns) combined_names.push_back(table_b + "." + cd.name);

    std::vector<std::string> out_cols = columns;
    if (out_cols.empty()) out_cols = combined_names;
    rs.column_names = out_cols;

    Schema merged_schema;
    merged_schema.table_name = table_a + "_" + table_b;
    for (auto &cd : ta->schema.columns) { ColDef cd2=cd; cd2.name=table_a+"."+cd.name; merged_schema.columns.push_back(cd2); }
    for (auto &cd : tb->schema.columns) { ColDef cd2=cd; cd2.name=table_b+"."+cd.name; merged_schema.columns.push_back(cd2); }

    if (join.active && !join.col_a.empty() && !join.col_b.empty()) {
        int idx_a = ta->schema.col_index(join.col_a);
        int idx_b = tb->schema.col_index(join.col_b);
        if (idx_a >= 0 && idx_b >= 0) {
            std::unordered_multimap<std::string, const Row*> hash_b;
            for (const Row &rb : tb->rows) {
                if (rb.is_expired()) continue;
                if (idx_b < (int)rb.values.size()) hash_b.emplace(rb.values[idx_b], &rb);
            }
            for (const Row &ra : ta->rows) {
                if (ra.is_expired()) continue;
                if (idx_a >= (int)ra.values.size()) continue;
                auto range = hash_b.equal_range(ra.values[idx_a]);
                for (auto it = range.first; it != range.second; ++it) {
                    const Row &rb = *(it->second);
                    Row merged;
                    merged.values.reserve(ra.values.size() + rb.values.size());
                    merged.values.insert(merged.values.end(), ra.values.begin(), ra.values.end());
                    merged.values.insert(merged.values.end(), rb.values.begin(), rb.values.end());
                    if (where.active && !row_matches_where(merged, merged_schema, where)) continue;
                    rs.rows.push_back(project_row(merged, merged_schema, out_cols));
                }
            }
            goto after_join_loop;
        }
    }

    for (const Row &ra : ta->rows) {
        if (ra.is_expired()) continue;
        for (const Row &rb : tb->rows) {
            if (rb.is_expired()) continue;
            Row merged;
            merged.values.insert(merged.values.end(), ra.values.begin(), ra.values.end());
            merged.values.insert(merged.values.end(), rb.values.begin(), rb.values.end());
            if (where.active && !row_matches_where(merged, merged_schema, where)) continue;
            rs.rows.push_back(project_row(merged, merged_schema, out_cols));
        }
    }

after_join_loop:;
    if (order_by.active) {
        int sort_col_idx = -1;
        for (int i = 0; i < (int)out_cols.size(); ++i) {
            std::string c = out_cols[i];
            auto dot = c.rfind('.'); if (dot != std::string::npos) c = c.substr(dot+1);
            if (c == order_by.column) { sort_col_idx = i; break; }
        }
        if (sort_col_idx >= 0) {
            bool is_numeric = true;
            for (auto &r : rs.rows) { try { std::stod(r[sort_col_idx]); } catch(...){ is_numeric=false; break; } }
            std::stable_sort(rs.rows.begin(), rs.rows.end(), [&](const auto &a, const auto &b) {
                bool lt = is_numeric ? (std::stod(a[sort_col_idx]) < std::stod(b[sort_col_idx])) : (a[sort_col_idx] < b[sort_col_idx]);
                return order_by.descending ? !lt : lt;
            });
        }
    }
    return rs;
}

/* ── helpers ─────────────────────────────────────────────────── */
bool StorageEngine::table_exists(const std::string &name) const { return tables_.count(name) > 0; }
StorageEngine::Table *StorageEngine::get_table(const std::string &name) {
    auto it = tables_.find(name); return it == tables_.end() ? nullptr : &it->second;
}
const StorageEngine::Table *StorageEngine::get_table(const std::string &name) const {
    auto it = tables_.find(name); return it == tables_.end() ? nullptr : &it->second;
}

bool StorageEngine::row_matches_where(const Row &row, int col_idx, const std::string &op, const std::string &value) const {
    if (col_idx < 0 || col_idx >= (int)row.values.size()) return false;
    const std::string &cell = row.values[col_idx];
    if (op == "=") return cell == value;
    
    bool cell_is_num = false;
    double cd, wd;
    try {
        cd = std::stod(cell);
        wd = std::stod(value);
        cell_is_num = true;
    } catch (...) {}

    if (cell_is_num) {
        if (op == ">")  return cd >  wd;
        if (op == ">=") return cd >= wd;
        if (op == "<")  return cd <  wd;
        if (op == "<=") return cd <= wd;
        return false;
    }

    if (op == ">")  return cell >  value;
    if (op == ">=") return cell >= value;
    if (op == "<")  return cell <  value;
    if (op == "<=") return cell <= value;
    return false;
}

bool StorageEngine::row_matches_where(const Row &row, const Schema &schema, const WhereClause &where) const {
    return row_matches_where(row, schema.col_index(where.column), where.op, where.value);
}

std::vector<std::string> StorageEngine::project_row(const Row &row, const Schema &schema, const std::vector<std::string> &cols) const {
    std::vector<int> col_indices;
    for (const auto &col : cols) col_indices.push_back(schema.col_index(col));
    return project_row(row, col_indices);
}

std::vector<std::string> StorageEngine::project_row(const Row &row, const std::vector<int> &col_indices) const {
    std::vector<std::string> out; out.reserve(col_indices.size());
    for (int idx : col_indices) {
        if (idx >= 0 && idx < (int)row.values.size()) out.push_back(row.values[idx]);
        else out.push_back("NULL");
    }
    return out;
}

/* ── meta queries ──────────────────────────────────────────────── */
ResultSet StorageEngine::show_tables() const {
    ResultSet rs; rs.column_names = {"TABLE_NAME", "ROW_COUNT", "COLUMN_COUNT"};
    for (auto &kv : tables_) {
        const Table &t = kv.second; std::size_t live = 0;
        for (auto &row : t.rows) if (!row.is_expired()) ++live;
        rs.rows.push_back({t.schema.table_name, std::to_string(live), std::to_string(t.schema.columns.size())});
    }
    return rs;
}

ResultSet StorageEngine::show_databases() const {
    ResultSet rs; rs.column_names = {"DATABASE_NAME", "TABLE_COUNT"};
    rs.rows.push_back({"flexql", std::to_string(tables_.size())});
    return rs;
}

ResultSet StorageEngine::describe(const std::string &table_name) const {
    ResultSet rs; const Table *tbl = get_table(table_name);
    if (!tbl) { rs.error = "table '" + table_name + "' not found"; return rs; }
    rs.column_names = {"COLUMN_NAME", "TYPE", "PRIMARY_KEY", "NOT_NULL"};
    for (auto &cd : tbl->schema.columns) rs.rows.push_back({cd.name, coltype_to_str(cd.type), cd.primary_key ? "YES" : "NO", cd.not_null ? "YES" : "NO"});
    return rs;
}

} /* namespace flexql */
