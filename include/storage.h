#pragma once
#include "common.h"
#include "index.h"
#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
 #include <fstream>

namespace flexql {

/*
 * StorageEngine
 * -------------
 * Holds all tables in memory using row-major format.
 * Each table is a Schema + a vector of Rows.
 * A primary-key hash index is maintained per table.
 *
 * Design choice – row-major:
 *   The workload is OLTP-style (INSERT + point lookups).
 *   Row-major keeps all fields of a record contiguous,
 *   making inserts O(1) and full-row retrieval cache-friendly.
 */
class StorageEngine {
public:
    /* DDL */
    std::string create_table(const Schema &schema, bool if_not_exists = false);

    /* DML */
    std::string insert(const std::string              &table_name,
                       const std::vector<std::string> &values,
                       std::time_t                     expiry);

    std::string insert_batch(const std::string                           &table_name,
                             const std::vector<std::vector<std::string>> &rows_values,
                             std::time_t                                  expiry);

    std::string insert_batch(const std::string                    &table_name,
                             std::vector<std::vector<std::string>> &&rows_values,
                             std::time_t                           expiry);

    std::string delete_rows(const std::string &table_name);

    /* Queries – return a ResultSet ready to send to the client */
    ResultSet select(const std::string              &table_name,
                     const std::vector<std::string> &columns,
                     const WhereClause              &where,
                     const OrderBy                  &order_by = {});

    ResultSet select_join(const std::string              &table_a,
                          const std::string              &table_b,
                          const std::vector<std::string> &columns,
                          const JoinSpec                 &join,
                          const WhereClause              &where,
                          const OrderBy                  &order_by = {});

    bool table_exists(const std::string &name) const;

    /* ── meta queries ── */
    ResultSet show_tables()                          const;
    ResultSet show_databases()                       const;
    ResultSet describe(const std::string &table_name) const;

    /* Persistence */
    void save_to_disk(const std::string &directory = "data/tables") const;
    void load_from_disk(const std::string &directory = "data/tables");
    void save_table(const std::string &table_name, const std::string &directory = "data/tables") const;
    void save_schema(const std::string &table_name, const std::string &directory = "data/tables") const;

private:
    struct Table {
        Schema               schema;
        std::vector<Row>     rows;
        PrimaryIndex         index;   /* hash index on primary key column */
        std::unordered_map<int, SecondaryIndex> secondary_indexes;
        std::ofstream        append_df;
        std::vector<char>    append_buf;
        int                  append_fd = -1;
        std::size_t          append_bytes = 0;
        std::size_t          writeback_bytes = 0;
    };

    std::unordered_map<std::string, Table> tables_;

    /* helpers */
    Table       *get_table(const std::string &name);
    const Table *get_table(const std::string &name) const;

    bool row_matches_where(const Row         &row,
                           const Schema      &schema,
                           const WhereClause &where) const;

    bool row_matches_where(const Row &row, int col_idx, const std::string &op, const std::string &value) const;

    std::vector<std::string> project_row(const Row                    &row,
                                         const Schema                 &schema,
                                         const std::vector<std::string> &cols) const;

    std::vector<std::string> project_row(const Row &row, const std::vector<int> &col_indices) const;
};

} /* namespace flexql */
