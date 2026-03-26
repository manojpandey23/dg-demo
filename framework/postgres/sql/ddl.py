from psycopg2 import sql
from psycopg2.sql import Composed


def get_pk_constraint_name_sql():
    return sql.SQL(
        """
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = con.connamespace
        WHERE con.contype = 'p'
          AND nsp.nspname = %s
          AND rel.relname = %s
    """
    )


def get_table_schema_sql():
    return sql.SQL(
        """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            COALESCE(i.indisprimary AND a.attnum = ANY(i.indkey),FALSE) AS is_primary_key,
            NOT a.attnotnull AS nullable
        FROM pg_attribute a
        JOIN pg_class t ON t.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_index i ON i.indrelid = t.oid AND i.indisprimary
        WHERE n.nspname = %s AND t.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """
    )


# -------------------------
# Identifiers
# -------------------------


def table_ident(table_fqn: str) -> Composed:
    schema, table = table_fqn.split(".")
    return sql.SQL(".").join([sql.Identifier(schema), sql.Identifier(table)])


def column_ident(col: str) -> sql.Identifier:
    return sql.Identifier(col)


def column_list(cols):
    return sql.SQL(", ").join(map(column_ident, cols))


def placeholders(n: int):
    return sql.SQL(", ").join(sql.Placeholder() * n)


# -------------------------
# Transaction / existence
# -------------------------
def begin_txn():
    return sql.SQL("BEGIN;")


def commit_txn():
    return sql.SQL("COMMIT;")


def rollback_txn():
    return sql.SQL("ROLLBACK;")


def table_exists_sql():
    return sql.SQL("SELECT to_regclass(%s)")


# -------------------------
# DDL
# -------------------------


def drop_table_sql(table_fqn):
    return sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident(table_fqn))


def create_table_sql(table_fqn, cols, keys):
    if keys:
        return sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}))").format(
            table_ident(table_fqn),
            sql.SQL(", ").join(cols),
            sql.SQL(", ").join(keys),
        )
    return sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        table_ident(table_fqn),
        sql.SQL(", ").join(cols),
    )


def drop_primary_key_sql(table_fqn, constraint_name):
    return sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
        table_ident(table_fqn),
        sql.Identifier(constraint_name),
    )


def add_primary_key_sql(table_fqn, keys):
    return sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
        table_ident(table_fqn),
        sql.SQL(", ").join(keys),
    )


def set_column_not_null_sql(table_fqn, col):
    return sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL").format(
        table_ident(table_fqn),
        column_ident(col),
    )


def drop_column_not_null_sql(table_fqn, col):
    return sql.SQL("ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL").format(
        table_ident(table_fqn),
        column_ident(col),
    )


def add_column_sql(table_fqn, col, col_type):
    return sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
        table_ident(table_fqn),
        column_ident(col),
        sql.SQL(col_type),
    )


def drop_column_sql(table_fqn, col):
    return sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
        table_ident(table_fqn),
        column_ident(col),
    )


def alter_column_type_sql(table_fqn, col, col_type):
    return sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {}").format(
        table_ident(table_fqn),
        column_ident(col),
        sql.SQL(col_type),
    )


# -------------------------
# INSERT / DELETE
# -------------------------
def insert_bulk_sql(table_fqn, cols):
    return sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        table_ident(table_fqn),
        column_list(cols),
    )


def delete_by_keys_sql(table_fqn, key_col):
    return sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
        table_ident(table_fqn),
        column_ident(key_col),
    )


# -------------------------
# Snapshot queries
# -------------------------
def select_current_snapshot_sql(table_fqn):
    return sql.SQL("SELECT * FROM {} WHERE is_current = TRUE").format(
        table_ident(table_fqn)
    )


def select_current_by_key_sql(table_fqn, key_col):
    """
    Select current snapshot record by key (single or composite).

    Args:
        table_fqn: fully qualified table name
        key_col: single column name (str) or list of column names (list[str])
    """
    if isinstance(key_col, str):
        where = sql.SQL("{} = %s").format(column_ident(key_col))
    elif isinstance(key_col, (list, tuple)):
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(column_ident(c)) for c in key_col
        )
    else:
        raise TypeError(f"key_col must be str or list[str], got {type(key_col)}")

    return sql.SQL("SELECT * FROM {} WHERE {} AND is_current = TRUE").format(
        table_ident(table_fqn),
        where,
    )


def select_current_snapshot_by_key_sql(table_fqn, key_col):
    """Legacy alias for select_current_by_key_sql"""
    return select_current_by_key_sql(table_fqn, key_col)


def close_snapshot_sql(table_fqn, key_col):
    """
    Close current snapshot record by setting valid_to and is_current=FALSE.
    Handles single or composite keys.
    """
    if isinstance(key_col, str):
        where = sql.SQL("{} = %s").format(column_ident(key_col))
    elif isinstance(key_col, (list, tuple)):
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(column_ident(c)) for c in key_col
        )
    else:
        raise TypeError(f"key_col must be str or list[str], got {type(key_col)}")

    return sql.SQL(
        """UPDATE {}
        SET valid_to = %s,
            is_current = FALSE
        WHERE {} AND is_current = TRUE"""
    ).format(
        table_ident(table_fqn),
        where,
    )


def invalidate_snapshot_sql(table_fqn, key_col):
    """
    Invalidate snapshot record (hard delete SCD2).
    Sets valid_to, is_current=FALSE, and is_deleted=TRUE.
    Handles single or composite keys.
    """
    if isinstance(key_col, str):
        where = sql.SQL("{} = %s").format(column_ident(key_col))
    elif isinstance(key_col, (list, tuple)):
        where = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(column_ident(c)) for c in key_col
        )
    else:
        raise TypeError(f"key_col must be str or list[str], got {type(key_col)}")

    return sql.SQL(
        """UPDATE {}
        SET valid_to = %s,
            is_current = FALSE,
            is_deleted = TRUE
        WHERE {} AND is_current = TRUE"""
    ).format(
        table_ident(table_fqn),
        where,
    )


def insert_snapshot_row_sql(table_fqn, cols):
    return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        table_ident(table_fqn),
        column_list(cols),
        placeholders(len(cols)),
    )


def incremental_merge_sql(table_fqn, cols, unique_key):
    updates = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(column_ident(c), column_ident(c))
        for c in cols
        if c != unique_key
    )

    return sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        table_ident(table_fqn),
        column_list(cols),
        placeholders(len(cols)),
        column_ident(unique_key),
        updates,
    )
