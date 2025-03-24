from matomo_campaign_helper import process_dataframe_for_campaign
import unittest
import pandas as pd

class TestDataframeProcessing(unittest.TestCase):

    def setUp(self):
        """Setting up mocked data"""
        # Sample data with raw columns that match the ones the function will process
        self.mapping_raw = pd.DataFrame({
            'MarketingCampaignsReporting_CombinedKeywordContent': ['Content A', 'Content B'],
            'MarketingCampaignsReporting_CampaignName': ['Campaign 1', 'Campaign 2'],
            'MarketingCampaignsReporting_CampaignSource': ['Source A', 'Source B'],
            'MarketingCampaignsReporting_CampaignMedium': ['Medium A', 'Medium B'],
            'MarketingCampaignsReporting_CampaignSourceMedium': ['Source A - Medium A', 'Source B - Medium B']
        })

    def test_column_renaming(self):
        """Test that the columns are properly renamed."""
       
        renamed_df = process_dataframe_for_campaign(self.mapping_raw)
    
        # Expected renamed columns
        renamed_columns = ['campaign_content', 'campaign_name', 'campaign_source', 'campaign_medium']
    
        self.assertEqual(list(renamed_df.columns), renamed_columns)

    def test_combined_column_split(self):
        """Test that the 'CampaignSourceMedium' column is properly split."""
    
        renamed_df = process_dataframe_for_campaign(self.mapping_raw)
        
        # Check if the 'campaign_source' and 'campaign_medium' columns are correctly split
        self.assertEqual(renamed_df['campaign_source'][0], 'Source A')
        self.assertEqual(renamed_df['campaign_medium'][0], 'Medium A')
        self.assertEqual(renamed_df['campaign_source'][1], 'Source B')
        self.assertEqual(renamed_df['campaign_medium'][1], 'Medium B')

    def test_column_dropped(self):
        """Test that the 'CampaignSourceMedium' column is dropped after splitting."""

        renamed_df = process_dataframe_for_campaign(self.mapping_raw)
        
        # Ensure that 'MarketingCampaignsReporting_CampaignSourceMedium' is no longer in the columns
        self.assertNotIn('MarketingCampaignsReporting_CampaignSourceMedium', renamed_df.columns)

    def test_no_combined_column(self):
        """Test when there is no 'CampaignSourceMedium' column."""
        # Create a new DataFrame without the 'CampaignSourceMedium' column
        mapping_no_combined = self.mapping_raw.drop(columns=['MarketingCampaignsReporting_CampaignSourceMedium'])

        renamed_df = process_dataframe_for_campaign(mapping_no_combined)
        
        # Ensure that 'campaign_source' and 'campaign_medium' columns are not added
        self.assertNotIn('campaign_source', renamed_df.columns)
        self.assertNotIn('campaign_medium', renamed_df.columns)


if __name__ == '__main__':
    unittest.main()
