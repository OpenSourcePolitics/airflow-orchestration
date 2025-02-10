from odoo_helper import fetch_invoices, process_invoices, export_csv_and_send_webhook
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime



class TestOdooInvoicesAutomation(unittest.TestCase):

    def setUp(self):
        """
        Setup mocked data
        """
        self.mock_models = MagicMock()
        self.db = 'test_db'
        self.uid = 1
        self.api_key = 'test_api_key'


        self.mock_invoices = [
            {
                'id': 1,
                'name': 'INV001',
                'move_type': 'out_invoice',
                'partner_id': [1, 'Client A'],
                'invoice_date': '2023-01-01',
                'invoice_date_due': '2023-01-15',
                'create_date': '2023-01-01',
                'line_ids': [10, 11],
                'status_in_payment': 'posted',
                'x_studio_csv_gnr': False
            },
            {
                'id': 2,
                'name': 'INV002',
                'move_type': 'out_refund',
                'partner_id': [2, 'Client B'],
                'invoice_date': '2023-01-05',
                'invoice_date_due': '2023-01-20',
                'create_date': '2023-01-05',
                'line_ids': [12],
                'status_in_payment': 'draft',
                'x_studio_csv_gnr': False
            },
            {
                'id': 3,
                'name': 'INV003',
                'move_type': 'out_invoice',
                'partner_id': [3, 'Client C'],
                'invoice_date': '2023-01-10',
                'invoice_date_due': '2023-01-25',
                'create_date': '2023-01-10',
                'line_ids': [13],
                'status_in_payment': 'posted',
                'x_studio_csv_gnr': True
            },
        ]

    def test_fetch_invoices(self):
        """
        Tester fetch_invoices filter the correct invoices
        """

        self.mock_models.execute_kw.return_value = [1, 2, 3]  # IDs returned
        self.mock_models.execute_kw.side_effect = [
            [invoice['id'] for invoice in self.mock_invoices],  # IDs for the search
            self.mock_invoices
        ]

        invoices_to_keep = fetch_invoices(self.mock_models, self.db, self.uid, self.api_key)

        self.assertEqual(len(invoices_to_keep), 1)
        self.assertEqual(invoices_to_keep[0]['id'], 1)

    @patch('odoo_helper.fetch_lines')
    def test_process_invoices(self, mock_fetch_lines):
        """
        Test that processing invoices works
        """
        invoices_to_keep = [self.mock_invoices[0]]
        mock_fetch_lines.return_value = [
            {
                'name': 'Product A',
                'debit': 100.0,
                'credit': 0.0,
                'account_id': [101, '411 Client A']
            }
        ]

        df = process_invoices(self.mock_models, invoices_to_keep, self.db, self.uid, self.api_key)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['Journal'], 'VTE')
        self.assertEqual(df.iloc[0]['Date'], '01/01/2023')
        self.assertEqual(df.iloc[0]['N piece'], 'INV001')
        self.assertEqual(df.iloc[0]['Code'], '411')
        self.assertEqual(df.iloc[0]['Libelle Compte'], 'Client A')
        self.assertEqual(df.iloc[0]['Debit'], 100.0)
        self.assertEqual(df.iloc[0]['Credit'], 0.0)


class TestExportCSVAndSendWebhook(unittest.TestCase):

    @patch("odoo_helper.requests.post")
    @patch("odoo_helper.Variable.get")
    def test_export_csv_and_send_webhook(self, mock_variable_get, mock_requests_post):
        """
        Test that the export_csv_and_send_webhook function correctly generates
        two separate CSV files when the data spans multiple months.
        """

        # Simulate a webhook URL
        mock_variable_get.return_value = "https://webhook.example.com"

        # Simulate a DataFrame with data from two different months
        data = [
            {'Journal': 'VTE', 'Date': '01/01/2023', 'N piece': 'INV001', 'Code': '411', 'Libelle Compte': 'Client A', 'Libelle': 'Invoice - Client A', 'Debit': 100.0, 'Credit': 0.0},
            {'Journal': 'VTE', 'Date': '15/01/2023', 'N piece': 'INV002', 'Code': '411', 'Libelle Compte': 'Client B', 'Libelle': 'Invoice - Client B', 'Debit': 200.0, 'Credit': 0.0},
            {'Journal': 'VTE', 'Date': '05/02/2023', 'N piece': 'INV003', 'Code': '411', 'Libelle Compte': 'Client C', 'Libelle': 'Invoice - Client C', 'Debit': 300.0, 'Credit': 0.0},
        ]

        df = pd.DataFrame(data)

        # Call the function with the mock data
        with patch("builtins.open", MagicMock()), patch("pandas.DataFrame.to_csv", MagicMock()):
            export_csv_and_send_webhook(df)

        # Verify that the function generates two CSV files with distinct names based on the month
        current_date = datetime.now().strftime('%Y-%m-%d')
        expected_filenames = [
            f"{current_date}_for_2023-01_output_invoices.csv",
            f"{current_date}_for_2023-02_output_invoices.csv"
        ]

        # Check if requests.post was called twice (one for each month)
        self.assertEqual(mock_requests_post.call_count, 2)

        # Extract the filenames used in the mock call arguments
        actual_filenames = [
            call_args[1]['files']['file'][0] for call_args in mock_requests_post.call_args_list
        ]

        # Ensure that both expected filenames are in the actual filenames list
        self.assertTrue(all(filename in actual_filenames for filename in expected_filenames))


if __name__ == '__main__':
    unittest.main()
