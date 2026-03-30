from matomo_campaign_helper import process_dataframe_for_campaign
import unittest
import pandas as pd

class TestDataframeProcessing(unittest.TestCase):

    def setUp(self):
        """Setting up mocked data"""
        # Sample data with raw columns that match the ones the function will process
        self.mapping_raw = pd.DataFrame({
            'MarketingCampaignsReporting_CombinedKeywordContent': ['Content A'],
            'MarketingCampaignsReporting_CampaignName': ['Campaign 1'],
            'MarketingCampaignsReporting_CampaignSource': ['Source A'],
            'MarketingCampaignsReporting_CampaignMedium': ['Medium A'],
            'MarketingCampaignsReporting_CampaignSourceMedium': ['Source A - Medium A']
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
        
    #Reduntant but kept as documentation
    def test_column_dropped(self):
        """Test that the 'CampaignSourceMedium' column is dropped after splitting."""

        renamed_df = process_dataframe_for_campaign(self.mapping_raw)
        
        # Ensure that 'MarketingCampaignsReporting_CampaignSourceMedium' is no longer in the columns
        self.assertNotIn('MarketingCampaignsReporting_CampaignSourceMedium', renamed_df.columns)

    
if __name__ == '__main__':
    unittest.main()
