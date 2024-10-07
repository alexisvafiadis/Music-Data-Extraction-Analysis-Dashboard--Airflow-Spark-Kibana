import pandas as pd
import requests

def enrich_lastfm_data(config, access_token, tools, spotify_tools, data_params, debug=False, limit=-1):
    datalake_dir = config['paths']['datalake_dir']
    print("Enhancing data...")
    common_exclude_columns = []

    df_spotify = pd.read_parquet(f"{datalake_dir}raw/spotify.parquet")
    df_lastfm = pd.read_parquet(f"{datalake_dir}raw/lastfm.parquet")

    # take only first 10 tracks if debug is True
    if limit != -1:
        df_lastfm = df_lastfm.head(limit)
        df_spotify = df_spotify.head(limit)

    # Finding unique tracks from LastFM that aren't in Spotify
    unique_tracks = df_lastfm.merge(df_spotify, on=["track_name", "artist"], how="left", indicator=True)
    unique_tracks = unique_tracks[unique_tracks['_merge'] == 'left_only']
    unique_tracks = unique_tracks[['track_name', 'artist']].drop_duplicates()

    # Convert to Pandas for easier API interaction
    track_data = []

    # Fetch track information from Spotify API
    for index, row in unique_tracks.iterrows():
        track_name = row['track_name']
        # remove "'"
        track_name = track_name.replace("'", "")
        # remove anything that is inside paranthesis
        search_url = f"https://api.spotify.com/v1/search?q=track:{track_name}%20artist:{row['artist']}&type=track"
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(search_url, headers=headers)
        search_results = response.json()['tracks']['items']
        if search_results:
            # Get the most relevant result which is usually the first one
            # if the current track contains Remastered, Live, Edition, etc. then take the next one until it doesn't
            track_info = spotify_tools.filter_search_results(search_results)
            data_dict = {
                "track_id": track_info['id'],
                "album": track_info['album']['name'],
                "spotify_popularity": track_info['popularity'],
                "explicit": track_info['explicit'],
                "release_date": track_info['album']['release_date'],
            }
            if debug:
                # print("Track Info : ", track_info)
                print(f"Found {track_info['name']} by {track_info['artists'][0]['name']} that matches {row['track_name']} by {row['artist']}")
                #print(f"Same name ? {track_info['name'] == row['track_name']}, Same artist ? {track_info['artists'][0]['name'] == row['artist']}")
            # make sure to replace the track name and artist by the original ones
            data_dict['track_name'] = row['track_name']
            track_data.append(data_dict)
        else:
            print(f"No results found for {row['track_name']} by {row['artist']}")
    print(f"Found {len(track_data)} tracks from Spotify (out of {len(unique_tracks)} unique tracks)")

    # Get audio features for the track ids
    #track_ids = track_df['track_id'].tolist()
    track_ids = [track['track_id'] for track in track_data]
    # get audio features for 50 tracks by 50 tracks
    audio_features_data = {}
    for i in range(0, len(track_ids), 50):
        audio_features_data.update(spotify_tools.get_audio_features(access_token, track_ids[i:i+50], data_params))
    track_data = spotify_tools.consolidate_data(track_data, audio_features_data)

    track_df = pd.DataFrame(track_data)

    # Remove common columns
    track_df.drop(columns=common_exclude_columns, inplace=True)

    # Merge the fetched data back to the original LastFM DataFrame
    df_lastfm_enriched = df_lastfm.merge(track_df, on=['track_name'], how="left")

    # Save the enhanced dataframe
    save_dir = f"{datalake_dir}enriched/lastfm.parquet"
    tools.finalize_pipeline_step(df_lastfm_enriched, save_dir, "enriched", "Lastfm track", debug=debug)



if __name__ == '__main__':
    from dags import data_params
    from dags.tasks.tools.spotify import *
    from dags.tasks.tools.general import *
    import dags.tasks.tools.general as tools
    import dags.tasks.tools.spotify as spotify_tools
    set_pandas_settings(pd)

    config = get_config()
    access_token_from_spotify = authenticate_spotify(data_params.spotify_client_id, data_params.spotify_client_secret)
    enrich_lastfm_data(config, access_token_from_spotify, tools, spotify_tools, data_params, debug=True)