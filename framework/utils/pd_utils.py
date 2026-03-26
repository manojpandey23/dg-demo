import pandas as pd

DTYPE_COERCERS = {
    "int": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "float": lambda s: pd.to_numeric(s, errors="coerce"),
    "string": lambda s: s.astype("string"),
    "datetime": lambda s: pd.to_datetime(s, errors="coerce", utc=True),
    "date": lambda s: pd.to_datetime(s, errors="coerce").dt.date,
    "bool": lambda s: s.astype("boolean"),
    "timedelta": lambda s: pd.to_timedelta(s, errors="coerce"),
}

def coerce_dataframe_types(df: pd.DataFrame, schema: list) -> pd.DataFrame:
    """
    Coerce dataframe columns based on schema dtype definitions
    using the _DTYPE_COERCERS map.
    """
    df = df.copy()

    if not schema:
        return df

    for col in schema:
        dtype = col.dtype
        col_name = col.name

        if not dtype:
            continue

        if col_name not in df.columns:
            continue

        coercer = DTYPE_COERCERS.get(dtype)

        if not coercer:
            continue

        try:
            df[col_name] = coercer(df[col_name])
        except Exception as e:
            raise ValueError(
                f"Failed to coerce column '{col_name}' to dtype '{dtype}'"
            ) from e

    return df
