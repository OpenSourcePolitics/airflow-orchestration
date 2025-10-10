from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime
from typing import Iterable, List, Optional
import base64
import json
import yaml
import re
import pandas as pd
from airflow.hooks.base import BaseHook
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.dynamic import DynamicClient

import requests

from grist_api import GristDocAPI

from dataclasses import dataclass

@dataclass
class K8sDecidimConfig:
    """
    Configuration for fetching Decidim custom resources and pushing to Grist.

    Parameters
    ----------
    kube_conn_id : str
        Airflow connection id from which to read the kubeconfig.
        The kubeconfig YAML must be stored in `connection.password`.
    grist_conn_id : str
        Airflow connection id for Grist.
    grist_doc_var : str
        Airflow Variable name that contains the Grist doc id.
    grist_table_var : str
        Airflow Variable name that contains the Grist table name.
    api_version : str
        Kubernetes apiVersion of the Decidim CRD.
    kind : str
        Kubernetes Kind of the Decidim CRD.
    """

    kube_conn_id: str
    grist_conn_id: str
    grist_doc_var: str
    grist_table_var: str
    api_version: str = "apps.libre.sh/v1alpha1"
    kind: str = "Decidim"


# -------------------------
# Kubernetes helpers
# -------------------------
def load_kube_api_client_from_connection(conn_id: str):
    """
    Load a Kubernetes ApiClient using a kubeconfig stored as base64 in an Airflow connection.

    Accepted locations:
      - connection.password: "BASE64:<one-line-base64>" OR just "<one-line-base64>"
      - connection.extra (JSON): {"kubeconfig_b64": "<one-line-base64>"}
    """

    conn = BaseHook.get_connection(conn_id)

    # 1) Prefer password
    raw = (conn.password or "").strip()

    if not raw:
        raise RuntimeError(
            f"Connection '{conn_id}' does not contain base64 kubeconfig. "
            "Put a one-line base64 in password (optionally with 'BASE64:' prefix) "
            "or in extras.kubeconfig_b64."
        )

    # Strip quotes if the UI saved them
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1]

    # Remove optional prefix
    if raw.startswith("BASE64:"):
        raw = raw[len("BASE64:"):]

    # Remove *all* whitespace just in case UI wrapped it
    raw = re.sub(r"\s+", "", raw)

    # Add padding if missing
    missing = (-len(raw)) % 4
    if missing:
        raw += "=" * missing

    def _try_decode(b64s: str) -> str:
        # Try standard base64 then url-safe
        try:
            return base64.b64decode(b64s, validate=False).decode("utf-8")
        except Exception:
            # try urlsafe variant
            try:
                return base64.urlsafe_b64decode(b64s).decode("utf-8")
            except Exception as e:
                raise RuntimeError("Provided kubeconfig is not valid base64 (even after padding).") from e

    decoded_yaml = _try_decode(raw)

    # Validate YAML (don’t log content)
    try:
        parsed = yaml.safe_load(decoded_yaml)
        if not isinstance(parsed, dict) or "apiVersion" not in parsed or "kind" not in parsed:
            raise RuntimeError("Decoded kubeconfig YAML missing required keys.")
    except Exception as e:
        raise RuntimeError("Decoded kubeconfig is not valid YAML.") from e

    # Write file for k8s client
    tmp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        tmp_file.write(decoded_yaml)
        tmp_file.flush()
        tmp_file.close()
        k8s_config.load_kube_config(config_file=tmp_file.name)
        api_client = k8s_client.ApiClient()
        return api_client, tmp_file.name
    except Exception:
        try:
            os.unlink(tmp_file.name)
        except Exception:
            pass
        raise



def discover_decidim_resource(dynamic: DynamicClient, api_version: str, kind: str):
    """
    Discover the Decidim CRD resource.

    Parameters
    ----------
    dynamic : DynamicClient
        Initialized Kubernetes dynamic client.
    api_version : str
        e.g., "apps.libre.sh/v1alpha1"
    kind : str
        e.g., "Decidim"

    Returns
    -------
    ResourceInstance
        A dynamic resource handle suitable for list/get operations.
    """
    return dynamic.resources.get(api_version=api_version, kind=kind)


def list_decidim_items_as_dicts(resource) -> List[dict]:
    """
    List Decidim custom resources across all namespaces and return items as dictionaries.

    Parameters
    ----------
    resource : Any
        The dynamic resource handle returned by `discover_decidim_resource`.

    Returns
    -------
    list of dict
        A list of items converted to plain dict via `.to_dict()`.
    """
    resp = resource.get(namespace=None)  # all namespaces
    dicts: List[dict] = []
    for it in resp.items:
        dicts.append(it.to_dict())
    return dicts


# -------------------------
# Transformation logic
# -------------------------
def extract_ready_condition(conditions: Optional[Iterable[dict]]) -> dict:
    """
    Extract the last 'Ready' condition from a conditions list.

    Parameters
    ----------
    conditions : Iterable[dict] or None
        List of condition objects (each a dict with `type`, `status`, etc.).

    Returns
    -------
    dict
        The last condition with `type == 'Ready'`, or empty dict if none.
    """
    if not conditions:
        return {}
    ready = [c for c in conditions if c.get("type") == "Ready"]
    return ready[-1] if ready else {}


def version_from_status_or_image(obj: dict) -> str:
    """
    Extract a version string, preferring `.status.version`, falling back to tag of `.spec.image`.

    Parameters
    ----------
    obj : dict
        A Decidim CR as a dictionary.

    Returns
    -------
    str
        Version string or empty string if not found.
    """
    status = obj.get("status") or {}
    version = status.get("version")
    if version:
        return str(version)

    image = (obj.get("spec") or {}).get("image", "")
    if ":" in image:
        return image.split(":")[-1]
    return ""

def parse_image_repo_name(image_str: str) -> str:
    """
    Return the repository name (last path segment before the optional ':tag').

    Examples
    --------
    rg.fr-par.scw.cloud/decidim-imt/decidim-imt:feat-sso-saml -> 'decidim-imt'
    ghcr.io/org/app:1.2.3                                     -> 'app'
    busybox                                                    -> 'busybox'
    """
    if not image_str:
        return ""
    # Strip tag if present
    path = image_str.split(":", 1)[0]
    # Keep last segment after slash, else the whole thing
    return path.rsplit("/", 1)[-1] if "/" in path else path



def build_dataframe_from_decidim_dicts(items: List[dict]) -> pd.DataFrame:
    rows = []
    for obj in items:
        meta = obj.get("metadata") or {}
        spec = obj.get("spec") or {}
        status = obj.get("status") or {}

        ns = meta.get("namespace", "")
        name = meta.get("name", "")
        host = spec.get("host", "")

        cond = extract_ready_condition(status.get("conditions"))
        ready_status = cond.get("status", "Unknown")
        ready_msg = cond.get("message", "")
        last_transition = cond.get("lastTransitionTime", "")

        ver = version_from_status_or_image(obj)

        full_image = spec.get("image", "")
        image_repo = parse_image_repo_name(full_image)

        rows.append(
            {
                "Namespace": ns,
                "Name": name,
                "Host": host,
                "Ready": ready_status,
                "Status": ready_msg,
                "Version": ver,
                "LastTransitionTime": last_transition,
                "Image": image_repo,
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Namespace", "Name"], kind="stable").reset_index(drop=True)
    return df



# -------------------------
# Grist push
# -------------------------
def dump_df_to_grist_table(
    api: GristDocAPI,
    table_name: str,
    df: pd.DataFrame,
    date_provider: callable = lambda: datetime.now().date(),
    chunk_size: int = 200,
):
    if df.empty:
        logging.info("No rows to push to Grist (empty DataFrame).")
        return

    # Ensure required columns exist (robustness)
    for col in ["Namespace", "Name", "Host", "Ready", "Version", "Image"]:
        if col not in df.columns:
            df[col] = ""

    df = df.copy()

    def _parse_ltt_to_date(val):
        if val is None or val == "":
            return None
        ts = pd.to_datetime(val, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()

    if "LastTransitionTime" in df.columns:
        last_update_series = df["LastTransitionTime"].apply(_parse_ltt_to_date)
    else:
        last_update_series = pd.Series([None] * len(df))

    default_date = date_provider()
    df["Last_Update"] = last_update_series.fillna(default_date)

    key_cols = [
        ["Namespace", "namespace", "Text"],
        ["Name", "name", "Text"],
    ]
    other_cols = [
        ("Host", "host", "Text"),
        ("Ready", "ready", "Text"),
        ("Version", "version", "Text"),
        ("Image", "image", "Text"),
        ("Last_Update", "last_update", "Date"),
    ]

    mapping = {
        "namespace": "Namespace",
        "name": "Name",
        "host": "Host",
        "ready": "Ready",
        "version": "Version",
        "image": "Image",
        "last_update": "Last_Update",
    }

    from types import SimpleNamespace
    safe_df = df.replace({pd.NA: None})
    records = []
    for _, row in safe_df.iterrows():
        payload = {attr: row.get(src) for attr, src in mapping.items()}
        records.append(SimpleNamespace(**payload))

    api.sync_table(
        table_name,
        records,
        key_cols=key_cols,
        other_cols=other_cols,
        grist_fetch=None,
        chunk_size=chunk_size,
        filters=None,
    )



def fetch_existing_grist_platforms(api: GristDocAPI, table_name: str) -> pd.DataFrame:
    """
    Fetch current rows from a Grist table and return a DataFrame with:
      Namespace, Name, Version [, Host]
    Missing columns are filled with defaults.

    Returns
    -------
    pandas.DataFrame with columns ['Namespace','Name','Version','Host']
    """
    rows = api.fetch_table(table_name)
    df = pd.DataFrame(rows) if rows else pd.DataFrame()

    # Normalize / fill columns
    if "Namespace" not in df.columns:
        df["Namespace"] = ""
    if "Name" not in df.columns:
        df["Name"] = ""
    if "Version" not in df.columns:
        df["Version"] = ""
    if "Host" not in df.columns:
        df["Host"] = ""

    return df[["Namespace", "Name", "Version", "Host"]]


def find_version_changes(df_new: pd.DataFrame, df_old: pd.DataFrame, notify_on_new: bool = True) -> pd.DataFrame:
    """
    Compare new snapshot (from K8s) with existing Grist rows and return
    a DataFrame of rows that changed version (and optionally new rows).

    Matching key: (Namespace, Name)

    Returns columns:
      Namespace, Name, Host, Old_Version, New_Version, Ready, Status, LastTransitionTime
    """
    # Normalize keys and versions to strings
    for d in (df_new, df_old):
        for c in ("Namespace", "Name", "Version", "Host"):
            if c in d.columns:
                d[c] = d[c].astype(str).fillna("").str.strip()
            else:
                d[c] = ""

    merged = df_new.merge(
        df_old[["Namespace", "Name", "Version"]].rename(columns={"Version": "Old_Version"}),
        on=["Namespace", "Name"],
        how="left",
        indicator=True,
    )

    merged["Old_Version"] = merged["Old_Version"].fillna("")
    merged["New_Version"] = merged["Version"].fillna("")

    # Changed versions
    changed = merged[(merged["_merge"] == "both") & (merged["Old_Version"] != merged["New_Version"])]

    # Optionally include brand new platforms
    if notify_on_new:
        new_rows = merged[merged["_merge"] == "left_only"].copy()
        new_rows["Old_Version"] = ""
        new_rows["New_Version"] = new_rows["Version"].fillna("")
        changed = pd.concat([changed, new_rows], ignore_index=True)

    out_cols = [
        "Namespace",
        "Name",
        "Host",
        "Old_Version",
        "New_Version",
        "Ready",
        "Status",
        "LastTransitionTime",
    ]
    # Ensure presence of fields from df_new
    for c in ["Host", "Ready", "Status", "LastTransitionTime"]:
        if c not in merged.columns:
            merged[c] = ""

    return changed[out_cols].reset_index(drop=True)


def send_version_changes_to_n8n(webhook_url: str, changes_df: pd.DataFrame, timeout: int = 10):
    """
    Send a POST per platform with version change information to an n8n webhook.

    Body example:
    {
      "namespace": "...",
      "name": "...",
      "host": "...",
      "old_version": "v1.2.3",
      "new_version": "v1.3.0",
      "ready": "True",
      "status": "Ready message",
      "last_transition_time": "2025-09-30T10:20:30Z",
      "change_type": "updated" | "new"
    }
    """
    if changes_df.empty:
        return

    for _, row in changes_df.iterrows():
        old_v = (row.get("Old_Version") or "").strip()
        new_v = (row.get("New_Version") or "").strip()
        change_type = "new" if not old_v else "updated"

        payload = {
            "namespace": row.get("Namespace"),
            "name": row.get("Name"),
            "host": row.get("Host"),
            "old_version": old_v,
            "new_version": new_v,
            "ready": row.get("Ready"),
            "status": row.get("Status"),
            "last_transition_time": row.get("LastTransitionTime"),
            "change_type": change_type,
        }

        r = requests.post(webhook_url, json=payload, timeout=timeout)
        r.raise_for_status()



# -------------------------
# Orchestration (callable)
# -------------------------
def collect_and_push_to_grist(cfg: K8sDecidimConfig):
    """
    Load kubeconfig, query K8s, detect version changes vs Grist, notify n8n per platform,
    then push the fresh snapshot into Grist.
    """
    from airflow.models import Variable

    # Load K8s
    api_client, tmp_path = load_kube_api_client_from_connection(cfg.kube_conn_id)
    try:
        dyn = DynamicClient(api_client)
        res = discover_decidim_resource(dyn, cfg.api_version, cfg.kind)
        items = list_decidim_items_as_dicts(res)
        df_new = build_dataframe_from_decidim_dicts(items)

        # Build Grist client
        grist_conn = BaseHook.get_connection(cfg.grist_conn_id)
        grist_api_key = grist_conn.password
        grist_server = grist_conn.host

        doc_id = Variable.get(cfg.grist_doc_var)
        table_name = Variable.get(cfg.grist_table_var)

        api = GristDocAPI(doc_id, server=grist_server, api_key=grist_api_key)

        try:
            df_existing = fetch_existing_grist_platforms(api, table_name)
        except Exception as e:
            logging.warning("Failed to fetch existing Grist table; assuming empty. Err=%s", e)
            df_existing = pd.DataFrame(columns=["Namespace", "Name", "Version", "Host"])

        changes = find_version_changes(df_new, df_existing, notify_on_new=True)
        logging.info("Detected %d version change(s).", len(changes))

        webhook_url = Variable.get("n8n_webhook_decidim_version_change", default_var="")
        if webhook_url and not changes.empty:
            send_version_changes_to_n8n(webhook_url, changes)
            logging.info("Sent %d notification(s) to n8n.", len(changes))
        elif not webhook_url and not changes.empty:
            logging.warning("n8n_webhook_decidim_version_change not set; skipping notifications.")

        # --- Push updated snapshot to Grist (so next run compares against it) ---
        dump_df_to_grist_table(api, table_name, df_new)

        logging.info("Pushed %d Decidim rows to Grist table '%s'.", len(df_new), table_name)
        if not df_new.empty:
            logging.debug("Sample rows: %s", json.dumps(df_new.head(5).to_dict(orient="records"), ensure_ascii=False))

        return df_new
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                logging.warning("Failed to delete temporary kubeconfig file: %s", tmp_path)

