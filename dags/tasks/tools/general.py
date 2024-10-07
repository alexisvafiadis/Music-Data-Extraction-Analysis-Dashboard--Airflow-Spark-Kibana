import configparser
import os

def get_config():
    # Construct the path to the config.ini file
    current_dir = os.path.dirname(__file__)  # gets the directory of the current script
    parent_dir = os.path.dirname(current_dir)  # navigates to the parent directory (dags)
    parent_dir = os.path.dirname(parent_dir)  # navigates to the parent directory (dags)
    airflow_dir = os.path.dirname(parent_dir)  # navigates to the airflow directory
    config_path = os.path.join(airflow_dir, 'config.ini')  # constructs the full path to config.ini

    # Read the config file
    config = configparser.ConfigParser()
    try:
        config.read(config_path)
    except Exception as e:
        print(f"Failed to read config file: {e}")
        return None

    return config

import requests
def authenticate_spotify(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {"grant_type": "client_credentials"}
    response = requests.post(url, headers=headers, data=payload, auth=(client_id, client_secret))
    return response.json().get('access_token')

def request_lastfm_data(params, base_url="http://ws.audioscrobbler.com/2.0/"):
    response = requests.get(base_url, params=params)
    data = response.json()
    return data

def set_pandas_settings(pd):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

def check_status_code(response):
    if response.status_code != 200:
        print(f"API error: {response.json()}")

import time
def make_api_request(url, headers, params, time_between_calls=0):
    if time_between_calls > 0: time.sleep(time_between_calls)
    response = requests.get(url, headers=headers, params=params)
    check_status_code(response)
    return response.json()

def finalize_pipeline_step(data, path_to_save, action_past_principle, dataset_name, debug=False, with_pandas=True,
                           compression='snappy', quick=False):
    if data is not None:
        # Save the data to Parquet
        try:
            if with_pandas:
                data.to_parquet(path_to_save, index=False,compression=compression)
            else:#with spark
                data.write.mode('overwrite').parquet(path_to_save)
        except Exception as e:
            print(f"Failed to save {dataset_name} data to Parquet: {e}")
            return
        print(f"{dataset_name} data {action_past_principle} successfully!")

        # Display the shape of the dataframe
        if with_pandas:
            total_rows, total_columns = data.shape
        else:
            total_rows = data.count()
            total_columns = len(data.columns)
        print(f"{dataset_name} data now has {total_rows} rows and {total_columns} columns")

        # Display the dataframe
        if not quick:
            print(f"First row of the {dataset_name} DataFrame as a dict: ")
            if with_pandas: print(data.iloc[0].to_dict())
            else: print(data.first().asDict())
            nrows_to_display = total_rows if debug else 5
            print(f"First {nrows_to_display} rows of the {action_past_principle} DataFrame :")
            if with_pandas:
                print(data.head(nrows_to_display))
            else:
                data.show(nrows_to_display)

    return data

