#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <ctime>
#include <algorithm>

namespace flexql {

/* ─────────────────────────────────────────────
   Column data types
   (INT and DATETIME are intentionally omitted)
   ───────────────────────────────────────────── */
enum class ColType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

inline std::string coltype_to_str(ColType t) {
    switch (t) {
        case ColType::INT:     return "INT";
        case ColType::DECIMAL: return "DECIMAL";
        case ColType::VARCHAR: return "VARCHAR";
        case ColType::DATETIME: return "DATETIME";
    }
    return "UNKNOWN";
}

/* ─────────────────────────────────────────────
   Column definition (part of a table schema)
   ───────────────────────────────────────────── */
struct ColDef {
    std::string name;
    ColType     type;
    bool        not_null    = false;
    bool        primary_key = false;
};

/* ─────────────────────────────────────────────
   Table schema
   ───────────────────────────────────────────── */
struct Schema {
    std::string           table_name;
    std::vector<ColDef>   columns;

    bool has_primary_key() const {
        for (int i = 0; i < (int)columns.size(); ++i)
            if (columns[i].primary_key) return true;
        return false;
    }

    int primary_key_index() const {
        for (int i = 0; i < (int)columns.size(); ++i)
            if (columns[i].primary_key) return i;
        return 0;   /* default: first column */
    }

    int col_index(const std::string &name) const {
        /* case-insensitive match so "student.id" == "STUDENT.ID" */
        std::string n = name;
        std::transform(n.begin(), n.end(), n.begin(), ::toupper);
        /* strip caller's table prefix if present: TEST_ORDERS.AMOUNT → AMOUNT */
        auto dot_n = n.rfind('.');
        std::string bare_n = (dot_n != std::string::npos) ? n.substr(dot_n + 1) : n;

        for (int i = 0; i < (int)columns.size(); ++i) {
            std::string cn = columns[i].name;
            std::transform(cn.begin(), cn.end(), cn.begin(), ::toupper);
            /* strip schema column prefix: TEST_ORDERS.AMOUNT → AMOUNT */
            auto dot_cn = cn.rfind('.');
            std::string bare_cn = (dot_cn != std::string::npos) ? cn.substr(dot_cn + 1) : cn;

            if (cn == n || bare_cn == bare_n) return i;
        }
        return -1;
    }
};

/* ─────────────────────────────────────────────
   A single row: values stored as strings.
   expiry == 0 means "never expires".
   ───────────────────────────────────────────── */
struct Row {
    std::vector<std::string> values;
    std::time_t              expiry = 0;

    bool is_expired() const {
        if (expiry == 0) return false;
        return std::time(nullptr) >= expiry;
    }
};

/* ─────────────────────────────────────────────
   Query types (result of parsing)
   ───────────────────────────────────────────── */
enum class QueryType {
    CREATE_TABLE,
    INSERT,
    SELECT,
    DELETE,           /* DELETE FROM <table>;       */
    SHOW_TABLES,      /* SHOW TABLES;              */
    SHOW_DATABASES,   /* SHOW DATABASES;            */
    DESCRIBE,         /* DESCRIBE <table>;          */
    UNKNOWN
};

/* ─────────────────────────────────────────────
   WHERE condition (single condition only)
   ───────────────────────────────────────────── */
struct WhereClause {
    bool        active = false;
    std::string column;
    std::string value;
    std::string op = "=";  /* "=", ">", ">=", "<", "<="  */
};

/* ─────────────────────────────────────────────
   ORDER BY spec
   ───────────────────────────────────────────── */
struct OrderBy {
    bool        active     = false;
    std::string column;
    bool        descending = false;
};

/* ─────────────────────────────────────────────
   JOIN spec
   ───────────────────────────────────────────── */
struct JoinSpec {
    bool        active = false;
    std::string table_b;
    std::string col_a;       /* tableA.col */
    std::string col_b;       /* tableB.col */
};

/* ─────────────────────────────────────────────
   Parsed query — a union-like structure that
   carries all fields regardless of type.
   ───────────────────────────────────────────── */
struct ParsedQuery {
    QueryType   type    = QueryType::UNKNOWN;
    std::string error;          /* non-empty if parsing failed */

    /* CREATE TABLE */
    Schema      schema;
    bool        if_not_exists = false;

    /* INSERT — single or multi-row */
    std::string                           insert_table;
    std::vector<std::vector<std::string>> insert_rows;    /* multi-row */
    std::time_t                           insert_expiry = 0;

    /* DELETE */
    std::string delete_table;

    /* SELECT */
    std::string              select_table;
    std::vector<std::string> select_columns; /* empty = SELECT * */
    WhereClause              where;
    JoinSpec                 join;
    OrderBy                  order_by;
};

/* ─────────────────────────────────────────────
   Result set returned to the client
   ───────────────────────────────────────────── */
struct ResultSet {
    std::vector<std::string>              column_names;
    std::vector<std::vector<std::string>> rows;
    std::string                           error;

    bool ok() const { return error.empty(); }
};

} /* namespace flexql */
