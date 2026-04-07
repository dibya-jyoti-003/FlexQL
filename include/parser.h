#pragma once
#include "common.h"
#include <string>

namespace flexql {

/*
 * Parser
 * ------
 * Converts a raw SQL string into a ParsedQuery struct.
 * Supports: CREATE TABLE, INSERT INTO, SELECT (with optional WHERE / INNER JOIN).
 * Everything is case-insensitive for keywords.
 */
class Parser {
public:
    ParsedQuery parse(const std::string &sql);

private:
    /* helpers */
    ParsedQuery parse_create(const std::string &sql);
    ParsedQuery parse_insert(const std::string &sql);
    ParsedQuery parse_select(const std::string &sql);
    ParsedQuery parse_delete(const std::string &sql);

    WhereClause parse_where(const std::string &fragment);
    JoinSpec    parse_join (const std::string &fragment,
                            const std::string &main_table);

    /* low-level string utilities */
    static std::string upper(std::string s);
    static std::string trim (const std::string &s);
    static std::vector<std::string> split(const std::string &s,
                                          char delim,
                                          bool respect_quotes = false);

    static ColType parse_type(const std::string &token);
};

} /* namespace flexql */
