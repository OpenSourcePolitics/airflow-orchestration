import json
import urllib3
import pandas as pd
from .matomo_request_config import matomo_requests_config
from .matomo_postgres_dump import get_postgres_connection, clean_data_in_postgres, dump_data_to_postgres
from .matomo_url import get_matomo_base_url, construct_url


START_DATE = '2024-09-01'  # Specify the start date
END_DATE = '2024-09-23'    # Specify the end date

# Initialize HTTP manager
http = urllib3.PoolManager()


def parse_range_data(raw_data):
    for entry in raw_data:
        if entry.get('subtable'):
            for sub_entry in entry['subtable']:
                sub_entry['sub_type'] = entry["label"]
                raw_data.append(sub_entry)
            entry.pop('subtable')
        if entry.get('goals'):
            entry.pop('goals')
    return raw_data


# Fetch data for a specific day from Matomo and return it as a DataFrame
def fetch_data_for_day(base_url, report_name, config, day):
    """Fetches data from Matomo for a specific day and returns it as a DataFrame."""
    url = construct_url(base_url, config, day)
    try:
        response = http.request('GET', url)
        raw_data = json.loads(response.data.decode('utf-8'))

        # Check if the response contains errors
        if isinstance(raw_data, dict) and raw_data.get('result') == 'error':
            error_message = f"Error fetching data for {report_name} on {day}: {raw_data.get('message')}"
            raise Exception(error_message)

            # Convert the response to DataFrame
        if isinstance(raw_data, list):
            parsed_raw_data = parse_range_data(raw_data)
            data = pd.DataFrame(parsed_raw_data)
        elif isinstance(raw_data, dict):
            data = pd.json_normalize(raw_data)
        else:
            print(f"Unexpected data format for {report_name} on {day}: {type(raw_data)}")
            return pd.DataFrame()
        # Add the date field to each row
        data['date'] = day
        return data

    except Exception as e:
        error_message = f"Error fetching data for {report_name} on {day}: {str(e)}"
        raise Exception(error_message)

    # Fetch data from Matomo for each day in the date range and merge into a single DataFrame
def fetch_data_from_matomo(base_url, report_name, config):
    """Fetches data from Matomo for each day in the specified range and merges it into a single DataFrame."""
    date_range = pd.date_range(start=START_DATE, end=END_DATE).strftime('%Y-%m-%d').tolist()
    all_data = [fetch_data_for_day(base_url, report_name, config, day) for day in date_range]

    # Combine all non-empty DataFrames into a single DataFrame
    valid_data = [df for df in all_data if not df.empty]
    if valid_data:
        return pd.concat(valid_data, ignore_index=True)
    else:
        error_message = f"No data fetched for {report_name}."
        raise Exception(error_message)

# Main function to fetch and dump data
def fetch_and_dump_data(matomo_site_id, database):
    base_url = get_matomo_base_url(matomo_site_id)
    connection = get_postgres_connection(database)

    if not connection:
        error_message = "Cannot proceed without database connection."
        raise Exception(error_message)

    for report_name, config in matomo_requests_config.items():
        print(f"Fetching data for {report_name}...")
        data = fetch_data_from_matomo(base_url, report_name, config)

        if data is not None and not data.empty:
            # Clean existing data in the table before dumping new data
            clean_data_in_postgres(connection, report_name, START_DATE, END_DATE)
            # Dump fetched data into PostgreSQL
            dump_data_to_postgres(connection, data, report_name)
        else:
            print(f"No data fetched for {report_name}, skipping clean and dump.")

    connection.close()
