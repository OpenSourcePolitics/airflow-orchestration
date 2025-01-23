import unittest
from unittest.mock import MagicMock, patch

from odoo_helper import fetch_invoices, process_invoices



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
                'x_studio_boolean_field_24i_1ii6rf2v6': False
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
                'x_studio_boolean_field_24i_1ii6rf2v6': False
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
                'x_studio_boolean_field_24i_1ii6rf2v6': True
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

        # Vérifier le contenu du DataFrame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['Journal'], 'VTE')
        self.assertEqual(df.iloc[0]['Date'], '2023-01-01')
        self.assertEqual(df.iloc[0]['N piece'], 'INV001')
        self.assertEqual(df.iloc[0]['Code'], '411')
        self.assertEqual(df.iloc[0]['Libelle Compte'], 'Client A')
        self.assertEqual(df.iloc[0]['Debit'], 100.0)
        self.assertEqual(df.iloc[0]['Credit'], 0.0)

if __name__ == '__main__':
    unittest.main()
