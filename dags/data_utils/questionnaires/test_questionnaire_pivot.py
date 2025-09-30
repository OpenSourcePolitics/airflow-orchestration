from dags.data_utils.questionnaires.questionnaire_pivot import pivot_filters, concat_multiple_answers, get_questionnaires_ids
import unittest
from unittest.mock import patch
import pandas as pd

class TestDataframeProcessing(unittest.TestCase):

    def setUp(self):
        self.client_name = "paris"
        self.df = pd.DataFrame({
            'session_token': ['1038918792771835475219703', '1038918792771835475219703', '77819727802057982331067', '77819727802057982331067'],
            'question_type': ['single_option', 'single_option', 'single_option', 'single_option'],
            'question_title': ['Qu\'avez-vous contre les Caractères spéciaux ?', 'Pourquoi ?', 'Qu\'avez-vous contre les Caractères spéciaux ?', 'Pourquoi ?'],
            'answer': ['Oui', 'Non', 'Rien du tout', 'NaN'],
            'decidim_questionnaire_id': ['1', '1', '1', '1'],
            'position': ['1', '2', '1', '3']
        })
        self.df_with_multiple_answers = pd.DataFrame({
            'session_token': ['1038918792771835475219703', '1038918792771835475219703'],
            'question_type': ['single_option', 'single_option'],
            'question_title': ['Voulez-vous participer ?', 'Voulez-vous participer ?'],
            'answer': ['Oui', 'Salarié'],
            'decidim_questionnaire_id': ['1', '1'],
            'position': ['1', '1']
        })

    @patch('dags.data_utils.questionnaires.questionnaire_pivot.Variable.get')
    def test_questionnaires_ids_parsing(self, mock_variable_get):
        """Test the proper parsing of the string variable stored in Airflow"""
        mock_variable_get.return_value = '{"paris": [98, 167, 2, 404], "marseille": [456,559,560,614], "lyon": [611,653]}'
        questionnaires_ids = get_questionnaires_ids(self.client_name)

        expected_result = [98, 167, 2, 404]
        self.assertEqual(expected_result, questionnaires_ids)

    @patch('dags.data_utils.questionnaires.questionnaire_pivot.Variable.get')
    def test_questionnaires_ids_parsing_when_failed(self, mock_variable_get):
        """Test the proper parsing of the string variable stored in Airflow"""
        mock_variable_get.return_value = '{"paris": [98, 167, 2, 404], "marseille": [456,559,560,614], "lyon": [611,653]}'
        questionnaires_ids = get_questionnaires_ids("angers")

        expected_result = []
        self.assertEqual(expected_result, questionnaires_ids)

    def test_pivoting(self):
        """Test that the form_answers dataframe is correctly pivoted and aggregated."""
        form_filters = pivot_filters(self.df)

        expected_columns = ['session_token','1._qu_avez-vous_contre_les_caractères_sp', '2._pourquoi_?', '3._pourquoi_?']
        self.assertEqual(list(form_filters.columns), expected_columns)

    def test_handle_multiple_answers_to_single_option_questions(self):
        """Test that multiple answers to a single_option question are concatenated as expected."""
        form_answers = concat_multiple_answers(self.df_with_multiple_answers)

        expected_answer = "Oui Salarié"
        actual_answer = form_answers.reset_index()['answer'][0]

        self.assertEqual(len(form_answers), 1)
        self.assertEqual(expected_answer, actual_answer)

if __name__ == '__main__':
    unittest.main()