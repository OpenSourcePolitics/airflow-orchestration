from dags.data_utils.crossclient_aggregation.crossclient_pull import create_aggregated_tables
from dags.client_list import clients

queries = {
    "all_users": "SELECT id AS decidim_user_id, email, created_at, confirmed, sign_in_count, deleted_at, blocked, spam, date_of_birth, gender FROM prod.users",
    "budgets": "SELECT id AS decidim_budget_id, title, amount, is_selected, categories, ps_title, FROM prod.budgets",
}

create_aggregated_tables(queries, clients)