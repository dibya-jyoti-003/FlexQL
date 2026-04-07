#pragma once
#include "common.h"
#include "parser.h"
#include "storage.h"
#include "cache.h"
#include <string>

#include <shared_mutex>

namespace flexql {

/*
 * Executor
 * --------
 * Accepts a raw SQL string, parses it, and dispatches it to the
 * StorageEngine.  The LRUCache is updated on every SELECT.
 */
class Executor {
public:
    Executor();

    /*
     * Execute one SQL statement.
     * Returns a ResultSet; check result.ok() for success/failure.
     */
    ResultSet execute(const std::string &sql);

    /* Persistence */
    void save_db();
    void load_db();
    void save_schema(const std::string &table_name);

private:
    Parser        parser_;
    StorageEngine storage_;
    LRUCache      cache_;
    mutable std::shared_mutex rw_mutex_;

    ResultSet exec_create (const ParsedQuery &q);
    ResultSet exec_insert (const ParsedQuery &q);
    ResultSet exec_insert (ParsedQuery &&q);
    ResultSet exec_select (const ParsedQuery &q);
    ResultSet exec_delete (const ParsedQuery &q);
};

} /* namespace flexql */
