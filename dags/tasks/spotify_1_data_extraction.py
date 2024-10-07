import requests
import pandas as pd
import requests
def get_top_tracks(access_token, playlist_id, limit, country, continent, spotify_tools):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers, params={"limit": limit})
    track_data = spotify_tools.get_tracks_from_json(response.json(), country, limit)
    for track in track_data:
        track.update({'continent': continent})
    return track_data

def extract_spotify_data(config, access_token, data_params, tools, spotify_tools, limit=50,debug=False):
    country_names = data_params.country_names
    playlist_ids = data_params.country_playlist_ids
    continents = data_params.continents

    print("Extracting Spotify data...")
    print(f"Taking {limit} tracks from {len(country_names)} countries ")
    all_tracks = []
    for i, country in enumerate(country_names):
        tracks = get_top_tracks(access_token, playlist_ids[i], limit, country, continents[i], spotify_tools)
        track_ids = [track['track_id'] for track in tracks]
        audio_features = spotify_tools.get_audio_features(access_token, track_ids, data_params)
        all_tracks.extend(spotify_tools.consolidate_data(tracks, audio_features))
        print(f"Processed {country}")

    # Convert list of track dictionaries to DataFrame
    df = pd.DataFrame(all_tracks)
    print(f'Loaded a total of {len(df)} tracks with {len(df.columns)} columns into a DataFrame')
    if debug:
        print(f'First track in DataFrame as a dictionary:\n{df.iloc[0].to_dict()}')
        print(f'First 5 tracks in DataFrame:\n{df.head()}')

    # Save the extracted data to a parquet
    parquet_path = config['paths']['datalake_dir']
    file_path = f'{parquet_path}/raw/spotify.parquet'
    tools.finalize_pipeline_step(df, file_path, 'extracted', 'Spotify', debug=debug)


if __name__ == '__main__':
    from dags.tasks.tools.general import *
    from dags.tasks.tools.spotify import *
    import dags.tasks.tools.spotify as spotify_tools
    import dags.tasks.tools.general as tools
    from dags import data_params
    config = get_config()
    set_pandas_settings(pd)
    access_token = authenticate_spotify(data_params.spotify_client_id, data_params.spotify_client_secret)
    extract_spotify_data(config, access_token, data_params,tools,
                         spotify_tools, limit=data_params.ntracks_per_country,debug=True)
