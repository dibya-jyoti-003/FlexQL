#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

/* ─────────────────────────────────────────────
   Error codes
   ───────────────────────────────────────────── */
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

/* ─────────────────────────────────────────────
   Opaque database handle
   The internal structure is hidden from the user.
   ───────────────────────────────────────────── */
typedef struct FlexQL FlexQL;

/* ─────────────────────────────────────────────
   Callback signature:
     data        – user pointer passed through flexql_exec
     columnCount – number of columns in this row
     values      – array of column value strings
     columnNames – array of column name strings
   Return 0 to continue, 1 to abort.
   ───────────────────────────────────────────── */
typedef int (*flexql_callback)(void *data,
                               int   columnCount,
                               char **values,
                               char **columnNames);

/* ─────────────────────────────────────────────
   API
   ───────────────────────────────────────────── */

/*
 * flexql_open – connect to a FlexQL server.
 *   host   : "127.0.0.1" / "localhost" / IP string
 *   port   : port number the server is listening on
 *   db     : output pointer; receives the allocated handle
 *   returns: FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_open(const char *host, int port, FlexQL **db);

/*
 * flexql_close – close connection and free all resources.
 *   db     : handle returned by flexql_open
 *   returns: FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_close(FlexQL *db);

/*
 * flexql_exec – send an SQL statement to the server.
 *   db       : open connection handle
 *   sql      : null-terminated SQL string
 *   callback : called once per result row (may be NULL)
 *   arg      : forwarded as first argument to callback
 *   errmsg   : set to an allocated error string on failure (free with flexql_free)
 *   returns  : FLEXQL_OK or FLEXQL_ERROR
 */
int flexql_exec(FlexQL        *db,
                const char    *sql,
                flexql_callback callback,
                void          *arg,
                char         **errmsg);

/*
 * flexql_free – free memory allocated by the FlexQL API
 *   (e.g. error message strings written into *errmsg).
 */
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif /* FLEXQL_H */
