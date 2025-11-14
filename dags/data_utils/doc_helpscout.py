import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from requests.auth import HTTPBasicAuth

from .grist.grist_helper import osp_grist_api

connection_helpscout = BaseHook.get_connection("helpscout")
assert connection_helpscout.login is not None
basic = HTTPBasicAuth(connection_helpscout.login, "X")

grist_doc_id = Variable.get("helpscout_documentation_grist_doc_id")


def get_categories(collection_id):
    response = requests.get(
        f"https://docsapi.helpscout.net/v1/collections/{collection_id}/categories",
        auth=basic,
    )
    return pd.DataFrame(response.json()["categories"]["items"])


def get_articles(category_id):
    response = requests.get(
        f"https://docsapi.helpscout.net/v1/categories/{category_id}/articles",
        auth=basic,
    )
    result = pd.DataFrame(response.json()["articles"]["items"])
    result["category_id"] = category_id
    return result


def dump_helpscout_collection_to_grist(collection_id):
    categories = get_categories(collection_id)

    all_pages = pd.concat([get_articles(c_id) for c_id in categories["id"]])
    final_table = pd.merge(
        all_pages,
        categories[["id", "name"]],
        left_on="category_id",
        right_on="id",
        suffixes=("", "_category"),
    )
    final_table["published"] = final_table["status"] == "published"

    api = osp_grist_api("grist_doc_id")

    api.sync_table(
        "Articles",
        final_table.itertuples(),
        [("helpscout_id", "id")],
        [
            ("Category", "name_category"),
            ("updated", "updatedAt"),
            ("Published", "published"),
            ("Url", "publicUrl"),
            ("Name", "name"),
        ],
    )
