from matomo_url import get_matomo_base_url, construct_url
import unittest
from unittest.mock import patch, MagicMock
from airflow.hooks.base import BaseHook


class TestMatomoURLFunctions(unittest.TestCase):

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_get_matomo_base_url(self, mock_get_connection):
        """
        Test that get_matomo_base_url retrieves the URL properly.
        """

        # Mocking connection 
        mock_connection = MagicMock()
        mock_connection.host = 'https://matomo.example.com/'
        mock_connection.password = 'test_token'
        mock_get_connection.return_value = mock_connection

        # Expected URL
        matomo_site_id = 1
        expected_url = 'https://matomo.example.com/index.php?module=API&format=JSON&token_auth=test_token&idSite=1'

        
        retrieved_url = get_matomo_base_url(matomo_site_id)

        # Check that the retreived URL matches de expected URL
        self.assertEqual(retrieved_url, expected_url)

    @patch('airflow.hooks.base.BaseHook.get_connection')
    def test_construct_url(self, mock_get_connection):
        """
        Test that construct_url correctly constructs the URL with the proper parameters.
        """

        # Mocking connection
        mock_connection = MagicMock()
        mock_connection.host = 'https://matomo.example.com/'
        mock_connection.password = 'test_token'
        mock_get_connection.return_value = mock_connection

        # Setup the base_url and config
        
        base_url = 'https://matomo.example.com/index.php?module=API&format=JSON&token_auth=test_token&idSite=1'
        
        config = {
            'date': '2023-03-20',      # Date for the API query
            'period': 'day',           # Period of the query
            'expanded': 1,             # Expanded query for detailed data
            'filter_limit': -1,        # No limit on the number of results
        }

        day = '2023-03-20'

        result_url = construct_url(base_url, config, day)

        # Expected URL with the real set of parameters
        expected_url = (
            'https://matomo.example.com/index.php?module=API&format=JSON&token_auth=test_token&idSite=1'
            '&date=2023-03-20&period=day&expanded=1&filter_limit=-1'
        )

        # Check that the constructed URL matches the expected URL
        self.assertEqual(result_url, expected_url)



if __name__ == '__main__':
    unittest.main()