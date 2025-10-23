import re
from typing import Dict, Any

import numpy as np
import pandas as pd
from grist_api import GristDocAPI
from sqlalchemy.dialects.postgresql import JSONB, TEXT

from .grist_helper import list_grist_tables
from ..postgres_helper import (
    dump_data_to_postgres,
    get_postgres_connection,
)


def _sanitize_identifier(name: str) -> str:
    """
    Convert an arbitrary string into a safe SQL identifier (for table names).

    Parameters
    ----------
    name : str
        Original identifier.

    Returns
    -------
    str
        Lowercase identifier with only [a-z0-9_] characters.
    """
    slug = re.sub(r"\W+", "_", name.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "table"

def _infer_psql_dtypes_and_normalize(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Build a dtype mapping for pandas.to_sql that avoids mixed-type failures.

    Rules:
    - If a column contains any list/tuple/dict -> store as JSONB (preserves structure).
    - If a column mixes strings and numbers -> coerce to TEXT (stringify) and set dtype=TEXT.
    - Else, let pandas/SQLAlchemy infer (no explicit dtype).

    Returns
    -------
    df_out : pd.DataFrame
        Possibly normalized copy (stringified mixed columns).
    dtype_map : Dict[str, sqlalchemy type]
        Mapping to pass to pandas.to_sql(dtype=...).
    """
    df_out = df.copy(deep=False)
    dtype_map: Dict[str, Any] = {}

    # Small sample to keep it cheap but representative
    SAMPLE_N = 200

    for col in df_out.columns:
        s = df_out[col]
        sample = s.dropna().head(SAMPLE_N)

        # Detect structural types
        has_listlike = sample.map(lambda v: isinstance(v, (list, tuple))).any()
        has_mapping  = sample.map(lambda v: isinstance(v, dict)).any()

        if has_listlike or has_mapping:
            dtype_map[col] = JSONB
            # Ensure values are JSONB-serializable (lists/dicts are fine, scalars too)
            # No changes needed unless you have non-JSON-serializable objects.
            continue

        # Detect mixed scalar types that would confuse INSERT casting
        has_str  = sample.map(lambda v: isinstance(v, str)).any()
        has_num  = sample.map(lambda v: isinstance(v, (int, float, np.integer, np.floating))).any()

        if has_str and has_num:
            # Force to TEXT, stringify everything non-null
            df_out[col] = df_out[col].map(lambda v: None if pd.isna(v) else str(v))
            dtype_map[col] = TEXT


    return df_out, dtype_map


def dump_document_to_postgres(
    api: GristDocAPI,
    connection_name: str,
    database: str,
    prefix: str,
    schema: str = "grist",
    if_exists = "replace",
    include_metadata: bool = False,
):
    engine = get_postgres_connection(connection_name, database)
    connection = engine.connect()
    try:
        grist_tables = list_grist_tables(api, include_metadata=include_metadata)

        for table_id in grist_tables:
            data = api.fetch_table(table_id)
            df = pd.DataFrame(data or [])
            table_name = f"{_sanitize_identifier(prefix)}_{_sanitize_identifier(table_id)}"

            # Normalize & build a safe dtype map
            df_safe, dtype_map = _infer_psql_dtypes_and_normalize(df)

            dump_data_to_postgres(
                connection=connection,
                data=df_safe,
                table_name=table_name,
                schema=schema,
                if_exists=if_exists,
                dtype=dtype_map,
            )
    finally:
        connection.close()

