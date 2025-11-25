from __future__ import annotations

import re
from typing import List, Literal

import pandas as pd
from airflow.hooks.base import BaseHook
from grist_api import GristDocAPI


def get_grist_api(connection_name, doc_id):
    connection = BaseHook.get_connection(connection_name)
    grist_api_key = connection.password
    grist_server = connection.host
    if grist_server is None:
        raise ValueError("`grist_osp` connection does not define host")
    return GristDocAPI(doc_id, grist_api_key, grist_server)


def sanitize_identifier(name: str) -> str:
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


def fetch_grist_table_data(
    doc_api: GristDocAPI, table_name, errors: Literal["raise", "coerce"] = "coerce"
):
    """
    Fetch data from a Grist table and return it as a pandas DataFrame with type validation.

    Args:
        doc_api: The Grist Doc API initialized with both the ID to fetch data from and the Grist connection
        table_name (str): Name of the Grist table to fetch data from

    Returns:
        pandas.DataFrame: The fetched data from Grist with validated data types
    """

    # Fetch data from Grist table
    data = doc_api.fetch_table(table_name)
    df = pd.DataFrame(data)

    for column in df.columns:
        # Handle numeric columns - convert invalid values to NaN
        if df[column].dtype == "object":
            # Try to convert to numeric, replacing non-numeric values with NaN
            df[column] = pd.to_numeric(df[column], errors=errors)

    return df


def list_grist_tables(
    grist_api: GristDocAPI, include_metadata: bool = False
) -> List[str]:
    """
    List table IDs for a given Grist document.

    This handles the actual return type of GristDocAPI.tables(), which is a
    `requests.Response`. We call `.json()` and accept multiple shapes:
    - list[dict] of tables
    - dict with a "tables" key containing list[dict]
    """
    resp = grist_api.tables()  # -> requests.Response
    payload = resp.json()  # IMPORTANT: parse JSON

    # Normalize to a list of dicts (tolerate both common shapes)
    if isinstance(payload, dict) and "tables" in payload:
        table_descs = payload["tables"]
    elif isinstance(payload, list):
        table_descs = payload
    else:
        raise RuntimeError(f"Unexpected payload when listing tables: {type(payload)}")

    table_ids = []
    for t in table_descs:
        # Be tolerant to different field names
        if isinstance(t, dict):
            tid = t.get("id") or t.get("tableId") or t.get("name")
        else:
            tid = str(t)
        if tid:
            table_ids.append(tid)

    if not include_metadata:
        table_ids = [tid for tid in table_ids if not str(tid).startswith("_grist_")]

    return table_ids
