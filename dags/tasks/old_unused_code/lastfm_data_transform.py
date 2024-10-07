import pandas as pd
from dags.tasks.tools import get_config

def map_genre(genre_dict, tags, default='Unidentified'):
    for tag in tags:
        for genre, keywords in genre_dict.items():
            if tag in keywords:
                return genre
    return default  # Default genre if no match is found


def transform_lastfm_data(config):
    datalake_dir = config['paths']['datalake_dir']
    top_tracks = pd.read_parquet(f'{datalake_dir}raw/lastfm.parquet')

    # Convert duration from seconds to minutes for better readability
    top_tracks['duration_ms'] = pd.to_numeric(top_tracks['duration_ms'], errors='coerce')
    top_tracks['duration_min'] = top_tracks['duration_ms'] / 60

    # Convert listeners to numeric for potential aggregation and analysis
    top_tracks['listeners'] = pd.to_numeric(top_tracks['listeners'], errors='coerce')
    top_tracks.rename(columns={'listeners': 'lastfm_listeners'}, inplace=True)

    # Create a normalized popularity index based on listeners (range 0-1)
    if not top_tracks['lastfm_listeners'].isna().all():  # Avoid division by zero if all are NaN
        max_listeners = top_tracks['lastfm_listeners'].max()
        top_tracks['lastfm_popularity'] = top_tracks['lastfm_listeners'] / max_listeners

    # Drop original duration in seconds column if no longer needed
    top_tracks.drop(columns=['duration_ms'], inplace=True)

    print(f"Last.fm data transformed successfully!")
    print(f"Last.fm data now has {top_tracks.shape[0]} rows and {top_tracks.shape[1]} columns")
    print(f"First row of the processed DataFrame: {top_tracks.iloc[0].to_dict()}")
    print(top_tracks)