import os
import base64
from types import SimpleNamespace
import pandas as pd
from unittest.mock import patch

from decidim_info import (
    find_version_changes,
    send_version_changes_to_n8n,
    parse_image_repo_name,
)


import pytest

from decidim_info import load_kube_api_client_from_connection, build_dataframe_from_decidim_dicts

@pytest.fixture
def sample_kubeconfig_yaml():
    # Minimal valid kubeconfig
    return (
        "apiVersion: v1\n"
        "kind: Config\n"
        "clusters: []\n"
        "contexts: []\n"
        "users: []\n"
    )


def _make_conn(password="", extra=""):
    # Simple stub matching the attributes used by BaseHook.get_connection
    return SimpleNamespace(password=password, extra=extra)


@patch("decidim_info.k8s_client.ApiClient")
@patch("decidim_info.k8s_config.load_kube_config")
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_loader_accepts_password_prefix_base64(get_conn, load_cfg, api_client_cls, sample_kubeconfig_yaml, tmp_path):
    """
    Should decode when password is 'BASE64:<b64>' and feed a temp file to load_kube_config.
    """
    b64 = base64.b64encode(sample_kubeconfig_yaml.encode("utf-8")).decode("ascii")
    get_conn.return_value = _make_conn(password=f"BASE64:{b64}", extra="")

    api_client_cls.return_value = "API_CLIENT_SENTINEL"

    api, path = load_kube_api_client_from_connection("k8s_config")

    load_cfg.assert_called_once()
    assert os.path.exists(path)
    assert api == "API_CLIENT_SENTINEL"

    # Sanity: file contains our YAML
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == sample_kubeconfig_yaml

    # Cleanup
    os.unlink(path)

### DECIDIM TRANSFORMATION

def _decidim_obj(
    ns="ns1",
    name="app1",
    host="app1.example",
    ready_status="True",
    message="ok",
    ltt="2025-09-30T10:20:30Z",
    status_version="v1.2.3",
    image=None,
):
    cond = {
        "type": "Ready",
        "status": ready_status,
        "message": message,
        "lastTransitionTime": ltt,
    }
    obj = {
        "apiVersion": "apps.libre.sh/v1alpha1",
        "kind": "Decidim",
        "metadata": {"namespace": ns, "name": name},
        "spec": {"host": host},
        "status": {"conditions": [cond]},
    }
    if status_version is not None:
        obj["status"]["version"] = status_version
    if image is not None:
        obj["spec"]["image"] = image
        # Optionally remove status.version so image fallback is used
        obj["status"].pop("version", None)
    return obj


def test_build_dataframe_happy_path():
    items = [
        _decidim_obj(ns="a", name="x", host="x.example", ready_status="True", message="ok", ltt="t1", status_version="v1"),
        _decidim_obj(ns="a", name="y", host="y.example", ready_status="False", message="fail", ltt="t2", status_version="v2"),
        _decidim_obj(ns="b", name="z", host="z.example", ready_status="True", message="ok", ltt="t3", status_version=None, image="repo/app:2025-09-01"),
    ]
    df = build_dataframe_from_decidim_dicts(items)

    assert list(df.columns) == [
        "Namespace",
        "Name",
        "Host",
        "Ready",
        "Status",
        "Version",
        "LastTransitionTime",
    ]

    # Sorted by (Namespace, Name): (a,x), (a,y), (b,z)
    assert df.iloc[0]["Namespace"] == "a" and df.iloc[0]["Name"] == "x"
    assert df.iloc[1]["Namespace"] == "a" and df.iloc[1]["Name"] == "y"
    assert df.iloc[2]["Namespace"] == "b" and df.iloc[2]["Name"] == "z"

    # Readiness and version extraction
    assert df.iloc[0]["Ready"] == "True" and df.iloc[0]["Version"] == "v1"
    assert df.iloc[1]["Ready"] == "False" and df.iloc[1]["Version"] == "v2"
    # Image fallback
    assert df.iloc[2]["Version"] == "2025-09-01"


def test_build_dataframe_missing_fields_and_no_conditions():
    # Missing spec.host and no conditions -> defaults kick in
    obj = {
        "apiVersion": "apps.libre.sh/v1alpha1",
        "kind": "Decidim",
        "metadata": {"namespace": "ns", "name": "app"},
        "spec": {},
        "status": {},  # no conditions, no version
    }
    df = build_dataframe_from_decidim_dicts([obj])
    row = df.iloc[0].to_dict()

    assert row["Host"] == ""
    assert row["Ready"] == "Unknown"
    assert row["Status"] == ""
    assert row["Version"] == ""
    assert row["LastTransitionTime"] == ""


def test_find_version_changes_updates_and_new():
    df_old = pd.DataFrame(
        [
            {"Namespace": "a", "Name": "x", "Version": "v1", "Host": "x.example"},
            {"Namespace": "a", "Name": "y", "Version": "v2", "Host": "y.example"},
        ]
    )

    df_new = pd.DataFrame(
        [
            {"Namespace": "a", "Name": "x", "Version": "v1", "Host": "x.example", "Ready": "True", "Status": "ok", "LastTransitionTime": "t1"},
            {"Namespace": "a", "Name": "y", "Version": "v3", "Host": "y.example", "Ready": "True", "Status": "rollout", "LastTransitionTime": "t2"},  # changed
            {"Namespace": "b", "Name": "z", "Version": "v0", "Host": "z.example", "Ready": "False", "Status": "new", "LastTransitionTime": "t3"},     # new
        ]
    )

    changes = find_version_changes(df_new, df_old, notify_on_new=True)
    # Expect 2 rows: y(updated), z(new)
    assert len(changes) == 2

    # y updated
    y = changes[(changes["Namespace"] == "a") & (changes["Name"] == "y")].iloc[0]
    assert y["Old_Version"] == "v2"
    assert y["New_Version"] == "v3"

    # z new
    z = changes[(changes["Namespace"] == "b") & (changes["Name"] == "z")].iloc[0]
    assert z["Old_Version"] == ""
    assert z["New_Version"] == "v0"


@patch("decidim_info.requests.post")
def test_send_version_changes_to_n8n(posts):
    df = pd.DataFrame(
        [
            {"Namespace": "a", "Name": "y", "Host": "y.example", "Old_Version": "v2", "New_Version": "v3", "Ready": "True", "Status": "rollout", "LastTransitionTime": "t2"},
            {"Namespace": "b", "Name": "z", "Host": "z.example", "Old_Version": "",   "New_Version": "v0", "Ready": "False", "Status": "new",     "LastTransitionTime": "t3"},
        ]
    )

    send_version_changes_to_n8n("https://n8n.example/webhook", df)

    assert posts.call_count == 2
    body1 = posts.call_args_list[0].kwargs["json"]
    assert body1["namespace"] == "a" and body1["name"] == "y"
    assert body1["old_version"] == "v2" and body1["new_version"] == "v3"
    assert body1["change_type"] == "updated"

    body2 = posts.call_args_list[1].kwargs["json"]
    assert body2["namespace"] == "b" and body2["name"] == "z"
    assert body2["old_version"] == "" and body2["new_version"] == "v0"
    assert body2["change_type"] == "new"


def test_parse_image_repo_name():
    """
    Ensure parse_image_repo_name extracts the last repository segment correctly,
    handling various registry, path, and tag formats.
    """
    cases = {
        "rg.fr-par.scw.cloud/decidim-imt/decidim-imt:feat-sso-saml": "decidim-imt",
        "ghcr.io/org/app:1.2.3": "app",
        "busybox": "busybox",
        "registry.io/foo/bar/baz:latest": "baz",
        "registry.io/foo/bar": "bar",
        "": "",
        None: "",
    }

    for input_str, expected in cases.items():
        result = parse_image_repo_name(input_str)
        assert result == expected, f"Expected '{expected}' for '{input_str}', got '{result}'"