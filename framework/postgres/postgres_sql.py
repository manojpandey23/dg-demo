# # postgres_sql_gen.py
# from psycopg2 import sql
#
#
# # -------------------------
# # Identifiers
# # -------------------------
#
#
# def table_ident(table_fqn: str) -> sql.Identifier:
#     return sql.Identifier(*table_fqn.split("."))
#
#
# def column_ident(col: str) -> sql.Identifier:
#     return sql.Identifier(col)
#
#
# def column_list(cols):
#     return sql.SQL(", ").join(map(column_ident, cols))
#
#
# def placeholders(n: int):
#     return sql.SQL(", ").join(sql.Placeholder() * n)
#
#
# # -------------------------
# # Transaction / existence
# # -------------------------
#
#
# def begin_txn():
#     return sql.SQL("BEGIN;")
#
#
# def commit_txn():
#     return sql.SQL("COMMIT;")
#
#
# def rollback_txn():
#     return sql.SQL("ROLLBACK;")
#
#
# def table_exists_sql():
#     return sql.SQL("SELECT to_regclass(%s)")
#
#
# # -------------------------
# # DDL
# # -------------------------
#
#
# def drop_table_sql(table_fqn):
#     return sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(table_fqn))
#
#
# def create_table_sql(table_fqn, column_defs):
#     return sql.SQL("CREATE TABLE {} ({})").format(
#         table_ident(table_fqn),
#         sql.SQL(", ").join(column_defs),
#     )
#
#
# def add_column_sql(table_fqn, col, col_type):
#     return sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
#         table_ident(table_fqn),
#         column_ident(col),
#         sql.SQL(col_type),
#     )
#
#
# def drop_column_sql(table_fqn, col):
#     return sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
#         table_ident(table_fqn),
#         column_ident(col),
#     )
#
#
# def alter_column_type_sql(table_fqn, col, col_type):
#     return sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {}").format(
#         table_ident(table_fqn),
#         column_ident(col),
#         sql.SQL(col_type),
#     )
#
#
# # -------------------------
# # INSERT / DELETE
# # -------------------------
#
#
# def insert_bulk_sql(table_fqn, cols):
#     return sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
#         table_ident(table_fqn),
#         column_list(cols),
#     )
#
#
# def delete_by_keys_sql(table_fqn, key_col):
#     return sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
#         table_ident(table_fqn),
#         column_ident(key_col),
#     )
#
#
# # -------------------------
# # Snapshot queries
# # -------------------------
#
#
# def select_current_snapshot_sql(table_fqn):
#     return sql.SQL("SELECT * FROM {} WHERE is_current = TRUE").format(
#         table_ident(table_fqn)
#     )
#
#
# def select_current_by_key_sql(table_fqn, key_col):
#     if isinstance(key_col, str):
#         where = sql.SQL("{} = %s").format(column_ident(key_col))
#     elif isinstance(key_col, (list, tuple)):
#         where = sql.SQL(" AND ").join(
#             sql.SQL("{} = %s").format(column_ident(c)) for c in key_col
#         )
#     else:
#         raise TypeError(f"unique_key must be str or list[str], got {type(key_col)}")
#
#     return sql.SQL("SELECT * FROM {} WHERE {} AND is_current = TRUE").format(
#         table_ident(table_fqn),
#         where,
#     )
#
#
# def close_snapshot_sql(table_fqn, key_col):
#     return sql.SQL(
#         """
#         UPDATE {}
#         SET valid_to = %s,
#             is_current = FALSE
#         WHERE {} = %s
#           AND is_current = TRUE
#         """
#     ).format(
#         table_ident(table_fqn),
#         column_ident(key_col),
#     )
#
#
# def invalidate_snapshot_sql(table_fqn, key_col):
#     return sql.SQL(
#         """
#         UPDATE {}
#         SET valid_to = %s,
#             is_current = FALSE,
#             is_deleted = TRUE
#         WHERE {} = %s
#           AND is_current = TRUE
#         """
#     ).format(
#         table_ident(table_fqn),
#         column_ident(key_col),
#     )
#
#
# def insert_snapshot_row_sql(table_fqn, cols):
#     return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#         table_ident(table_fqn),
#         column_list(cols),
#         placeholders(len(cols)),
#     )
#
#
# def get_table_schema_sql():
#     return sql.SQL(
#         """
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_schema = %s AND table_name = %s
#         """
#     )
#
#
# def incremental_merge_sql(table_fqn, cols, unique_key):
#     updates = sql.SQL(", ").join(
#         sql.SQL("{} = EXCLUDED.{}").format(column_ident(c), column_ident(c))
#         for c in cols
#         if c != unique_key
#     )
#
#     return sql.SQL(
#         "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
#     ).format(
#         table_ident(table_fqn),
#         column_list(cols),
#         placeholders(len(cols)),
#         column_ident(unique_key),
#         updates,
#     )
