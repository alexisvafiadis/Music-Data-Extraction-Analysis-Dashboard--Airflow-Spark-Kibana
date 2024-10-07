import pandas as pd
import time

def extract_lastfm_data(config, countries, continents, api_key, tools, limit=10, time_between_lastfm_api_calls=0.25):
    print("Extracting Last.fm data...")
    print(f"Taking {limit} tracks from {len(countries)} countries")
    #base_url = "http://ws.audioscrobbler.com/2.0/"
    all_tracks = []
    method = 'geo.gettoptracks'

    for j, country in enumerate(countries):
        params = {
            'method': method,
            'country': country,
            'api_key': api_key,
            'format': 'json',
            'limit': limit
        }
        print(params)
        data = tools.request_lastfm_data(params)
        #response = requests.get(base_url, params=params)
        #data = response.json()
        print(data)

        # Extract track details
        for i, track in enumerate(data.get('tracks', {}).get('track', [])):
            track_info = {
                'rank': i+1, #track.get('@attr', {}).get('rank'),
                'country': country,
                'continent': continents[j],
                'track_name': track.get('name'),
                'artist': track.get('artist', {}).get('name'),
                'duration_ms': track.get('duration'),
                'lastfm_listeners': track.get('listeners'),
                #'tags': [tag.get('name') for tag in track.get('toptags', {}).get('tag', [])],
                'lastfm_url': track.get('url'),
            }
            all_tracks.append(track_info)
            if i == limit:
                break
        time.sleep(time_between_lastfm_api_calls)

    # Convert list to DataFrame
    df = pd.DataFrame(all_tracks)
    datalake_dir = config['paths']['datalake_dir']
    file_dir = f"{datalake_dir}raw/lastfm.parquet"
    tools.finalize_pipeline_step(df, file_dir, 'extracted', 'Last.fm')

if __name__ == '__main__':
    from dags.tasks.tools.general import *
    import dags.tasks.tools.general as tools
    from dags import data_params

    set_pandas_settings(pd)
    config = get_config()
    print(data_params.continents, data_params.country_names)
    extract_lastfm_data(config, data_params.country_names, data_params.continents, data_params.lastfm_api_key, tools,
                        limit=data_params.ntracks_per_country, time_between_lastfm_api_calls=data_params.lastfm_api_call_delay)