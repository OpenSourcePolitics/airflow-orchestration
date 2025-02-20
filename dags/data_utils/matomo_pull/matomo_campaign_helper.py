def process_dataframe_for_campaign(df):
    """
    Prepares a DataFrame for PostgreSQL by renaming and restructuring columns.

    Args:
        df (pd.DataFrame): The input DataFrame with raw data.

    Returns:
        pd.DataFrame: The processed DataFrame ready for dumping into PostgreSQL.
    """
    # Mapping of original column names to their desired new names
    rename_map = {
        'MarketingCampaignsReporting_CombinedKeywordContent': 'campaign_content',
        'MarketingCampaignsReporting_CampaignName': 'campaign_name',
        'MarketingCampaignsReporting_CampaignSource': 'campaign_source',
        'MarketingCampaignsReporting_CampaignMedium': 'campaign_medium',
    }

    # Rename columns only if they exist in the DataFrame
    df = df.rename(columns={col: new_col for col, new_col in rename_map.items() if col in df.columns})
    # Check if the combined source-medium column exists
    if 'MarketingCampaignsReporting_CampaignSourceMedium' in df.columns:
        # Split the combined source-medium column into two separate columns
        try:
            source_medium_split = df['MarketingCampaignsReporting_CampaignSourceMedium'].str.split(' - ', expand=True)
            df['campaign_source'] = source_medium_split[0]  # Assign the first part as campaign_source
            df['campaign_medium'] = source_medium_split[1]  # Assign the second part as campaign_medium
            # Drop the original combined column as it's no longer needed
            df = df.drop(columns=['MarketingCampaignsReporting_CampaignSourceMedium'])
        except KeyError as e:
            pass
    return df
