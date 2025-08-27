import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
import pandas as pd

# ---- Patch Airflow BEFORE importing your module so top-level code doesn't blow up ----
def _fake_get_connection(conn_id):
    # Minimal fake connections used by your module globals
    if conn_id == "grist_osp":
        return SimpleNamespace(password="grist_api_key", host="https://grist.example")
    if conn_id == "keycloak_demo_suite":
        return SimpleNamespace(host="https://kc.example", login="kc_user", password="kc_pass")
    raise KeyError(conn_id)

def _fake_variable_get(key):
    # Minimal fake variables used at import-time
    mapping = {
        "grist_commercial_doc_id": "doc-123",
        "n8n_webhook_prospect_demo_suite_coop": "https://webhook.example/n8n",
        "prospects_demo_suite_coop": "Prospects",
    }
    return mapping.get(key, "")

with patch("airflow.hooks.base.BaseHook.get_connection", side_effect=_fake_get_connection), \
     patch("airflow.models.Variable.get", side_effect=_fake_variable_get):
    # Import AFTER the patches so the module-level Airflow calls use the fakes
    from demo_suite_keycloak import find_new_prospects, dump_df_to_grist_table


class TestFindNewProspects(unittest.TestCase):
    def test_returns_only_new_rows(self):
        """Should return only prospects present in Keycloak and missing in Grist (match on email/prenom/nom)."""
        df_keycloak = pd.DataFrame(
            [
                {"email": "a@ex.com", "prenom": "alice", "nom": "dupont"},
                {"email": "b@ex.com", "prenom": "bob", "nom": "martin"},
                {"email": "c@ex.com", "prenom": "chris", "nom": "durand"},
            ]
        )
        df_grist = pd.DataFrame(
            [
                {"email": "a@ex.com", "prenom": "alice", "nom": "dupont"},
                {"email": "b@ex.com", "prenom": "bob", "nom": "martin"},
            ]
        )

        out = find_new_prospects(df_keycloak, df_grist)
        self.assertEqual(len(out), 1)
        self.assertEqual(out.iloc[0].to_dict(), {"email": "c@ex.com", "prenom": "chris", "nom": "durand"})
        # Index should be reset to start at 0
        self.assertListEqual(list(out.index), [0])


class TestDumpDfToGristTable(unittest.TestCase):
    def setUp(self):
        """Prepare a fake Grist API and a tiny DataFrame."""
        self.api = MagicMock()
        self.table_name = "Prospects"
        self.df = pd.DataFrame(
            [
                {
                    "email": "a@ex.com",
                    "prenom": "alice",
                    "nom": "dupont",
                    "pseudo": "ali",
                    "email_verifie": True,
                    "date_de_creation": pd.Timestamp("2024-01-02"),
                }
            ]
        )

    def test_sync_table_is_called_with_expected_signature(self):
        """Ensure sync_table is called with proper records, key/other cols, and chunk_size."""
        chunk_size = 123
        dump_df_to_grist_table(self.api, self.table_name, self.df, chunk_size=chunk_size)

        self.api.sync_table.assert_called_once()
        args, kwargs = self.api.sync_table.call_args

        # Table name
        self.assertEqual(args[0], self.table_name)

        # Records list built from DataFrame rows (as attribute-bearing objects)
        records = args[1]
        self.assertEqual(len(records), len(self.df))
        expected_attrs = {"email", "prenom", "nom", "pseudo", "email_verifie", "date_de_creation"}
        for rec in records:
            self.assertTrue(all(hasattr(rec, attr) for attr in expected_attrs))

        # First record content
        r0 = records[0]
        self.assertEqual(r0.email, "a@ex.com")
        self.assertEqual(r0.prenom, "alice")
        self.assertEqual(r0.nom, "dupont")
        self.assertEqual(r0.pseudo, "ali")
        self.assertEqual(r0.email_verifie, True)
        self.assertTrue(hasattr(r0, "date_de_creation"))

        # key_cols / other_cols as expected by py_grist_api.sync_table
        expected_key_cols = [
            ["Email", "email", "Text"],
            ["Prenom", "prenom", "Text"],
            ["Nom", "nom", "Text"],
        ]
        expected_other_cols = [
            ("Email_verifie", "email_verifie", "Toggle"),
            ("Date_de_creation", "date_de_creation", "Date"),
        ]
        self.assertEqual(args[2], expected_key_cols)
        self.assertEqual(args[3], expected_other_cols)

        # kwargs
        self.assertIsNone(kwargs.get("grist_fetch"))
        self.assertIsNone(kwargs.get("filters"))
        self.assertEqual(kwargs.get("chunk_size"), chunk_size)


if __name__ == "__main__":
    unittest.main()
