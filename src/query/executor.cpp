#include "../include/executor.h"
#include <sstream>

namespace flexql {

Executor::Executor() : cache_(256) {
    load_db();
}

ResultSet Executor::execute(const std::string &sql) {
    /* Handle Pipelined Batches (client sends 5000 queries separated by \n) */
    if (sql.find('\n') != std::string::npos) {
        std::istringstream stream(sql);
        std::string line;
        ParsedQuery combined;
        bool first = true;
        
        while (std::getline(stream, line)) {
            if (line.empty() || line == "\r") continue;
            ParsedQuery q = parser_.parse(line);
            if (!q.error.empty()) {
                ResultSet err_rs; err_rs.error = q.error; return err_rs;
            }
            if (first) {
                combined = std::move(q);
                first = false;
            } else {
                if (!q.insert_rows.empty()) {
                    for (auto &r : q.insert_rows) combined.insert_rows.push_back(std::move(r));
                }
            }
        }
        
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        return exec_insert(std::move(combined));
    }

    ParsedQuery q = parser_.parse(sql);

    if (!q.error.empty()) {
        ResultSet rs;
        rs.error = q.error;
        return rs;
    }

    ResultSet rs;
    bool is_write = (q.type == QueryType::CREATE_TABLE ||
                     q.type == QueryType::INSERT       ||
                     q.type == QueryType::DELETE);

    if (is_write) {
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        switch (q.type) {
            case QueryType::CREATE_TABLE: rs = exec_create(q); break;
            case QueryType::INSERT:       rs = exec_insert(std::move(q)); break;
            case QueryType::DELETE:       rs = exec_delete(q); break;
            default: break;
        }
    } else {
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        switch (q.type) {
            case QueryType::SELECT:         rs = exec_select(q);  break;
            case QueryType::SHOW_TABLES:    rs = storage_.show_tables();   break;
            case QueryType::SHOW_DATABASES: rs = storage_.show_databases(); break;
            case QueryType::DESCRIBE:       rs = storage_.describe(q.select_table); break;
            default:
                rs.error = "Unsupported query type";
        }
    }
    return rs;
}

void Executor::save_db() {
    storage_.save_to_disk();
}

void Executor::save_schema(const std::string &table_name) {
    storage_.save_schema(table_name);
}

void Executor::load_db() {
    storage_.load_from_disk();
}

ResultSet Executor::exec_create(const ParsedQuery &q) {
    ResultSet rs;
    std::string msg = storage_.create_table(q.schema, q.if_not_exists);
    if (msg != "OK") {
        rs.error = msg;
    } else {
        save_schema(q.schema.table_name);
    }
    return rs;
}

ResultSet Executor::exec_delete(const ParsedQuery &q) {
    ResultSet rs;
    std::string msg = storage_.delete_rows(q.delete_table);
    if (msg != "OK") rs.error = msg;
    return rs;
}

ResultSet Executor::exec_insert(const ParsedQuery &q) {
    ResultSet rs;
    /* multi-row insert: bulk write all value groups directly to disk batch */
    const auto &rows = q.insert_rows;

    std::string msg = storage_.insert_batch(q.insert_table, rows, q.insert_expiry);
    if (msg != "OK") { rs.error = msg; return rs; }

    /* DISABLED: cache invalidation for performance during bulk inserts */
    /* cache_.invalidate_table(q.insert_table); */
    return rs;
}

ResultSet Executor::exec_insert(ParsedQuery &&q) {
    ResultSet rs;
    std::string msg = storage_.insert_batch(q.insert_table, std::move(q.insert_rows), q.insert_expiry);
    if (msg != "OK") { rs.error = msg; return rs; }

    /* DISABLED: cache invalidation for performance during bulk inserts */
    /* cache_.invalidate_table(q.insert_table); */
    return rs;
}

ResultSet Executor::exec_select(const ParsedQuery &q) {
    /* build cache key including ORDER BY */
    std::string cache_key = q.select_table + "::"
        + (q.where.active ? q.where.column + q.where.op + q.where.value : "*");
    if (q.join.active)
        cache_key += "::JOIN::" + q.join.table_b + "::" + q.join.col_a + "=" + q.join.col_b;
    if (q.order_by.active)
        cache_key += "::ORDER::" + q.order_by.column + (q.order_by.descending ? ":D" : ":A");

    // ResultSet cached;
    // if (cache_.get(cache_key, cached)) return cached;

    ResultSet rs;
    if (q.join.active) {
        rs = storage_.select_join(q.select_table,
                                   q.join.table_b,
                                   q.select_columns,
                                   q.join,
                                   q.where,
                                   q.order_by);
    } else {
        rs = storage_.select(q.select_table,
                              q.select_columns,
                              q.where,
                              q.order_by);
    }

    if (rs.ok()) {
        cache_.put(cache_key, rs);
    }

    return rs;
}

} /* namespace flexql */
