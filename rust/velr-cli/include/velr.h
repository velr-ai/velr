#ifndef VELR_H
#define VELR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---------- Opaque handles (mirror Rust types) ---------- */

typedef struct velr_db     velr_db;     /* Rust: Velr */
typedef struct velr_stream velr_stream; /* Rust: ExecTables<'c> */
typedef struct velr_table  velr_table;  /* Rust: TableResult<'c> */
typedef struct velr_rows   velr_rows;   /* Rust: RowIter<'s> */

/* ---------- Status codes ---------- */

typedef enum velr_code {
  VELR_OK     = 0,
  VELR_EARG   = -1, /* null/invalid arg */
  VELR_EUTF   = -2, /* not UTF-8 */
  VELR_ESTATE = -3, /* wrong state (e.g., buffer too small) */
  VELR_EERR   = -4, /* driver/internal error */
} velr_code;

/* ---------- Cell & string views ---------- */

typedef enum velr_cell_type {
  VELR_NULL   = 0,
  VELR_BOOL   = 1,
  VELR_INT64  = 2,
  VELR_DOUBLE = 3,
  VELR_TEXT   = 4,
  VELR_JSON   = 5,
} velr_cell_type;

typedef struct velr_cell {
  velr_cell_type  ty;
  int64_t         i64_;   /* BOOL: 0/1; INT64: value */
  double          f64_;   /* DOUBLE value */
  const uint8_t*  ptr;    /* TEXT/JSON bytes (NOT NUL-terminated) */
  size_t          len;    /* TEXT/JSON length */
} velr_cell;

typedef struct velr_strview {
  const uint8_t* ptr;     /* NOT NUL-terminated */
  size_t         len;
} velr_strview;

typedef enum velr_migration_status {
  VELR_MIGRATION_ALREADY_CURRENT = 0,
  VELR_MIGRATION_MIGRATED        = 1,
} velr_migration_status;

typedef struct velr_migration_report {
  int32_t                from_version;
  int32_t                to_version;
  velr_migration_status status;
  size_t                 step_count;
  char*                  steps;        /* comma-separated UTF-8; free with velr_migration_report_clear() */
} velr_migration_report;

/* Free error strings returned via out_err. */
void velr_string_free(char* s);

/* ---------- Velr (connection) ---------- */

/* Open a connection, creating/initializing a file-backed DB when needed. */
velr_code velr_open (const char* path_or_null, velr_db** out_db, char** out_err);

/*
 * Open an existing file-backed DB read-only.
 * Does not create files, initialize schema, repair, or migrate.
 * Supported older schema versions may be opened for reads, but writes and
 * schema-version-5-only features require explicit migration.
 */
velr_code velr_open_existing_readonly(const char* path, velr_db** out_db, char** out_err);
void      velr_close(velr_db* db);

/* Cached database schema version for this connection. */
velr_code velr_schema_version(velr_db* db, int32_t* out_version, char** out_err);

/* Current schema version supported by this runtime. */
velr_code velr_current_schema_version(velr_db* db, int32_t* out_version, char** out_err);

/* 1 when the database is on an older supported schema version, else 0. */
velr_code velr_needs_migration(velr_db* db, int* out_needs_migration, char** out_err);

/*
 * Explicitly migrate the database to the current schema version.
 * Opening a supported older database does not migrate it.
 *
 * On success, out_report->steps is NULL or an allocated string. Clear it with
 * velr_migration_report_clear().
 */
velr_code velr_migrate(velr_db* db, velr_migration_report* out_report, char** out_err);

/* Free owned memory inside a migration report. Safe to call with NULL. */
void velr_migration_report_clear(velr_migration_report* report);

/* ---------- ExecTables (planning & streaming) ---------- */

/* Start an execution stream (Velr::exec). */
velr_code velr_exec_start(velr_db* db, const char* cypher, velr_stream** out_stream, char** out_err);

/* Convenience: expect exactly one table (Velr::exec_one). */
velr_code velr_exec_one(velr_db* db, const char* cypher, velr_table** out_table, char** out_err);

/* Advance to next table, if any: sets *out_has = 1 (table) or 0 (EOF). */
velr_code velr_stream_next_table(velr_stream* stream, velr_table** out_table, int* out_has, char** out_err);

/* Close the stream; must be called before closing db. */
void      velr_exec_close(velr_stream* stream);

/* ---------- TableResult (metadata & rows) ---------- */

size_t    velr_table_column_count(velr_table* table);

/* Column name as (ptr,len) view (NOT NUL-terminated); valid while table is alive. */
velr_code velr_table_column_name(velr_table* table, size_t idx,
                                 const uint8_t** out_ptr, size_t* out_len);

/* Open row iterator (RowIter::rows). */
velr_code velr_table_rows_open(velr_table* table, velr_rows** out_rows, char** out_err);

/* Close table handle. */
void      velr_table_close(velr_table* table);

/* ---------- RowIter (fetch rows) ---------- */

/*
 * Fetch next row into caller-provided buffer of velr_cell.
 * Returns: 1=row written, 0=EOF, <0=error.
 * Requires: buf_len >= column_count.
 * NOTE: For TEXT/JSON cells, `ptr` points into internal scratch storage that remains valid
 *       until the next velr_rows_next call or velr_rows_close.
 */
int       velr_rows_next(velr_rows* rows, struct velr_cell* buf, size_t buf_len,
                         size_t* out_written, char** out_err);

/* Close row iterator. */
void      velr_rows_close(velr_rows* rows);

#ifdef __cplusplus
}
#endif
#endif /* VELR_H */
