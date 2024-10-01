from airflow.hooks.base import BaseHook


def get_matomo_base_url(matomo_site_id):
    connection = BaseHook.get_connection('matomo_host_connection')
    host = connection.host
    token = connection.password
    return f"{host}index.php?module=API&format=JSON&token_auth={token}&idSite={matomo_site_id}"


#  Function to construct the URL for API calls for a specific day
def construct_url(base_url, config, day):
    url_parameters = {
        'date': day,  # Query the API for a specific day
        'period': 'day',  # Set the period to 'day'
        'expanded': 1,
        'filter_limit': -1,
    }
    url_parameters.update(config)
    url = base_url + ''.join([f"&{key}={value}" for key, value in url_parameters.items()])
    return url
