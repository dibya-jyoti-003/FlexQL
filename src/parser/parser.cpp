#include "../include/parser.h"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace flexql {

/* ── public entry point ──────────────────────────────────────── */

ParsedQuery Parser::parse(const std::string &raw_sql) {
    std::string sql = trim(raw_sql);
    /* strip trailing semicolon */
    if (!sql.empty() && sql.back() == ';') sql.pop_back();
    sql = trim(sql);

    std::string up = upper(sql);

    if (up.rfind("SHOW TABLES", 0) == 0) {
        ParsedQuery q;
        q.type = QueryType::SHOW_TABLES;
        return q;
    }
    if (up.rfind("SHOW DATABASES", 0) == 0) {
        ParsedQuery q;
        q.type = QueryType::SHOW_DATABASES;
        return q;
    }
    if (up.rfind("DESCRIBE", 0) == 0 || up.rfind("DESC ", 0) == 0) {
        ParsedQuery q;
        q.type = QueryType::DESCRIBE;
        std::vector<std::string> parts = split(sql, ' ');
        if (parts.size() >= 2) {
            std::string tbl = parts[1];
            if (!tbl.empty() && tbl.back() == ';') tbl.pop_back();
            q.select_table = upper(trim(tbl));
        }
        return q;
    }

    if (up.rfind("CREATE", 0) == 0)  return parse_create(sql);
    if (up.rfind("INSERT", 0) == 0)  return parse_insert(sql);
    if (up.rfind("SELECT", 0) == 0)  return parse_select(sql);
    if (up.rfind("DELETE", 0) == 0)  return parse_delete(sql);

    ParsedQuery q;
    q.error = "Unknown command: " + sql;
    return q;
}

/* ── CREATE TABLE ────────────────────────────────────────────── */

ParsedQuery Parser::parse_create(const std::string &sql) {
    ParsedQuery q;
    q.type = QueryType::CREATE_TABLE;

    std::string up = upper(sql);

    /* detect IF NOT EXISTS */
    q.if_not_exists = (up.find("IF NOT EXISTS") != std::string::npos);

    auto paren_open  = sql.find('(');
    auto paren_close = sql.rfind(')');
    if (paren_open == std::string::npos || paren_close == std::string::npos) {
        q.error = "CREATE TABLE: missing parentheses";
        return q;
    }

    /* extract table name between "TABLE" (or "EXISTS") and '(' */
    auto table_kw = up.find("TABLE");
    if (table_kw == std::string::npos) {
        q.error = "CREATE TABLE: missing TABLE keyword";
        return q;
    }
    /* skip past IF NOT EXISTS if present */
    auto name_start = table_kw + 5;
    if (q.if_not_exists) {
        auto ine = up.find("EXISTS", table_kw);
        if (ine != std::string::npos) name_start = ine + 6;
    }
    std::string name_part = trim(sql.substr(name_start, paren_open - name_start));
    q.schema.table_name = upper(name_part);

    /* extract column definitions */
    std::string cols_str = sql.substr(paren_open + 1, paren_close - paren_open - 1);
    auto col_defs = split(cols_str, ',');

    for (auto &def : col_defs) {
        def = trim(def);
        if (def.empty()) continue;

        auto tokens = split(def, ' ');
        if (tokens.size() < 2) {
            q.error = "CREATE TABLE: bad column definition: " + def;
            return q;
        }

        ColDef cd;
        cd.name = upper(tokens[0]);
        cd.type = parse_type(upper(tokens[1]));

        std::string rest = upper(def);
        cd.not_null    = (rest.find("NOT NULL")    != std::string::npos);
        cd.primary_key = (rest.find("PRIMARY KEY") != std::string::npos);

        q.schema.columns.push_back(cd);
    }

    if (q.schema.columns.empty()) {
        q.error = "CREATE TABLE: no columns defined";
    }
    return q;
}

/* ── INSERT ──────────────────────────────────────────────────── */

static std::vector<std::string> parse_value_group(const std::string &group, bool respect_quotes) {
    /* split by comma, strip quotes */
    std::vector<std::string> result;
    std::string cur;
    bool in_quote = false;
    for (char c : group) {
        if (c == '\'' ) { in_quote = !in_quote; if (respect_quotes) cur += c; }
        else if (!in_quote && c == ',') { result.push_back(cur); cur.clear(); }
        else cur += c;
    }
    if (!cur.empty()) result.push_back(cur);
    for (auto &v : result) {
        /* trim whitespace */
        auto b = v.find_first_not_of(" \t\r\n");
        auto e = v.find_last_not_of(" \t\r\n");
        v = (b == std::string::npos) ? "" : v.substr(b, e - b + 1);
        /* strip surrounding single quotes */
        if (v.size() >= 2 && v.front() == '\'' && v.back() == '\'')
            v = v.substr(1, v.size() - 2);
    }
    return result;
}

ParsedQuery Parser::parse_insert(const std::string &sql) {
    ParsedQuery q;
    q.type = QueryType::INSERT;

    std::string up = upper(sql);

    auto into_pos = up.find("INTO");
    if (into_pos == std::string::npos) { q.error = "INSERT: missing INTO"; return q; }

    auto values_pos = up.find("VALUES");
    if (values_pos == std::string::npos) { q.error = "INSERT: missing VALUES"; return q; }

    q.insert_table = upper(trim(sql.substr(into_pos + 4, values_pos - into_pos - 4)));

    /* optional EXPIRE= after the last ')' */
    auto last_paren = sql.rfind(')');
    if (last_paren != std::string::npos) {
        std::string after_paren = trim(sql.substr(last_paren + 1));
        std::string up_after    = upper(after_paren);
        auto exp_pos = up_after.find("EXPIRE=");
        if (exp_pos != std::string::npos) {
            try {
                q.insert_expiry = (std::time_t)std::stoll(trim(after_paren.substr(exp_pos + 7)));
            } catch (...) {
                q.error = "INSERT: invalid EXPIRE value"; return q;
            }
        }
    }

    /* collect all value groups: VALUES (g1), (g2), ... */
    std::string rest = sql.substr(values_pos + 6);
    std::size_t pos = 0;
    while (pos < rest.size()) {
        auto open = rest.find('(', pos);
        if (open == std::string::npos) break;
        auto close = rest.find(')', open);
        if (close == std::string::npos) break;
        std::string group = rest.substr(open + 1, close - open - 1);
        auto vals = parse_value_group(group, false);
        q.insert_rows.push_back(vals);
        
        /* find comma after closing paren to see if there are more rows */
        pos = close + 1;
        while (pos < rest.size() && (std::isspace(rest[pos]) || rest[pos] == '\r' || rest[pos] == '\n')) pos++;
        if (pos < rest.size() && rest[pos] == ',') {
            pos++;
        } else {
            break; 
        }
    }

    return q;
}

/* ── DELETE ──────────────────────────────────────────────────── */

ParsedQuery Parser::parse_delete(const std::string &sql) {
    ParsedQuery q;
    q.type = QueryType::DELETE;

    /* DELETE FROM <table> */
    std::string up = upper(sql);
    auto from_pos = up.find("FROM");
    if (from_pos == std::string::npos) { q.error = "DELETE: missing FROM"; return q; }

    std::string tbl = trim(sql.substr(from_pos + 4));
    /* strip trailing semicolon */
    if (!tbl.empty() && tbl.back() == ';') tbl.pop_back();
    q.delete_table = upper(trim(tbl));
    return q;
}

/* ── SELECT ──────────────────────────────────────────────────── */

ParsedQuery Parser::parse_select(const std::string &sql) {
    ParsedQuery q;
    q.type = QueryType::SELECT;

    std::string up = upper(sql);

    /* locate FROM */
    auto from_pos = up.find(" FROM ");
    if (from_pos == std::string::npos) { q.error = "SELECT: missing FROM"; return q; }

    /* columns between SELECT and FROM */
    std::string cols_str = trim(sql.substr(6, from_pos - 6));
    if (cols_str != "*") {
        for (auto &c : split(cols_str, ','))
            q.select_columns.push_back(upper(trim(c)));
    }

    /* everything after FROM */
    std::string after_from = trim(sql.substr(from_pos + 6));
    std::string up_af      = upper(after_from);

    /* strip ORDER BY if present before further parsing */
    std::string order_fragment;
    auto order_pos = up_af.find(" ORDER BY ");
    if (order_pos != std::string::npos) {
        order_fragment = trim(after_from.substr(order_pos + 10));
        after_from = trim(after_from.substr(0, order_pos));
        up_af      = upper(after_from);
        /* parse ORDER BY */
        auto ob_parts = split(order_fragment, ' ');
        if (!ob_parts.empty()) {
            q.order_by.active = true;
            q.order_by.column = upper(trim(ob_parts[0]));
            if (ob_parts.size() >= 2 && upper(trim(ob_parts[1])) == "DESC")
                q.order_by.descending = true;
        }
    }

    /* check for INNER JOIN */
    auto join_pos       = up_af.find(" INNER JOIN ");
    auto where_pos_main = up_af.find(" WHERE ");

    if (join_pos != std::string::npos) {
        q.select_table = upper(trim(after_from.substr(0, join_pos)));
        std::string rest = trim(after_from.substr(join_pos + 12));
        q.join = parse_join(rest, q.select_table);
        std::string up_rest = upper(rest);
        auto wp = up_rest.find(" WHERE ");
        if (wp != std::string::npos)
            q.where = parse_where(trim(rest.substr(wp + 7)));
    } else if (where_pos_main != std::string::npos) {
        q.select_table = upper(trim(after_from.substr(0, where_pos_main)));
        q.where        = parse_where(trim(after_from.substr(where_pos_main + 7)));
        if (!q.where.active) {
            q.error = "SELECT: invalid WHERE clause";
            return q;
        }
    } else {
        q.select_table = upper(trim(after_from));
    }

    return q;
}

/* ── WHERE ───────────────────────────────────────────────────── */

WhereClause Parser::parse_where(const std::string &fragment) {
    WhereClause w;

    /* detect operator: >=, <=, >, < before = */
    std::size_t op_pos   = std::string::npos;
    std::string op       = "=";
    std::size_t eq       = fragment.find('=');

    if (eq != std::string::npos && eq > 0) {
        char prev = fragment[eq - 1];
        if (prev == '>') { op = ">="; op_pos = eq - 1; }
        else if (prev == '<') { op = "<="; op_pos = eq - 1; }
        else { op = "="; op_pos = eq; }
    } else if (eq != std::string::npos) {
        op = "="; op_pos = eq;
    }

    /* also check for standalone > or < (no following =) */
    if (eq == std::string::npos) {
        auto gt = fragment.find('>');
        auto lt = fragment.find('<');
        if (gt != std::string::npos) { op = ">"; op_pos = gt; }
        else if (lt != std::string::npos) { op = "<"; op_pos = lt; }
        else {
            w.active = false;
            return w;  /* indicate no operator found */
        }
    }

    w.active = true;
    w.op     = op;

    std::string col_part = fragment.substr(0, op_pos);
    std::string val_part = fragment.substr(op_pos + op.size());

    std::string col = upper(trim(col_part));
    w.column = col;

    w.value = trim(val_part);
    if (w.value.size() >= 2 && w.value.front() == '\'' && w.value.back() == '\'')
        w.value = w.value.substr(1, w.value.size() - 2);
    return w;
}

/* ── JOIN ────────────────────────────────────────────────────── */

JoinSpec Parser::parse_join(const std::string &fragment,
                             const std::string & /*main_table*/) {
    JoinSpec j;
    std::string up = upper(fragment);

    auto on_pos = up.find(" ON ");
    if (on_pos == std::string::npos) {
        j.active  = true;
        j.table_b = upper(trim(fragment));
        return j;
    }

    j.active  = true;
    j.table_b = upper(trim(fragment.substr(0, on_pos)));

    std::string cond = trim(fragment.substr(on_pos + 4));
    std::string up_cond = upper(cond);
    auto wp = up_cond.find(" WHERE ");
    if (wp != std::string::npos) cond = trim(cond.substr(0, wp));

    auto eq = cond.find('=');
    if (eq == std::string::npos) return j;

    auto left  = upper(trim(cond.substr(0, eq)));
    auto right = upper(trim(cond.substr(eq + 1)));

    auto dot_l = left.find('.');
    auto dot_r = right.find('.');
    j.col_a = (dot_l != std::string::npos) ? left.substr(dot_l + 1)  : left;
    j.col_b = (dot_r != std::string::npos) ? right.substr(dot_r + 1) : right;

    return j;
}

/* ── utilities ───────────────────────────────────────────────── */

std::string Parser::upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

std::string Parser::trim(const std::string &s) {
    auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

std::vector<std::string> Parser::split(const std::string &s,
                                        char delim,
                                        bool respect_quotes) {
    std::vector<std::string> result;
    std::string cur;
    bool in_quote = false;

    for (char c : s) {
        if (respect_quotes && c == '\'') {
            in_quote = !in_quote;
            cur += c;
        } else if (!in_quote && c == delim) {
            result.push_back(cur);
            cur.clear();
        } else {
            cur += c;
        }
    }
    if (!cur.empty()) result.push_back(cur);
    return result;
}

ColType Parser::parse_type(const std::string &token) {
    if (token == "VARCHAR" || token.rfind("VARCHAR", 0) == 0) return ColType::VARCHAR;
    if (token == "TEXT") return ColType::VARCHAR;
    if (token == "INT") return ColType::INT;
    if (token == "DATETIME") return ColType::DATETIME;
    if (token == "DECIMAL") return ColType::DECIMAL;
    return ColType::DECIMAL;
}

} /* namespace flexql */
