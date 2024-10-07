import pandas as pd
def load_spotify_data(path):
    try:
        df = pd.read_parquet(f'{path}/raw/spotify.parquet')
        #df = pd.read_csv(f'{path}/raw/spotify.csv')
    except Exception as e:
        print(f"Failed to load Spotify data from Parquet: {e}")
        return None
    print(f"Loaded Spotify data with {df.shape[0]} rows and {df.shape[1]} columns")
    return df

def transform_spotify_data(config):
    datalake_dir = config['paths']['datalake_dir']
    print("Transforming Spotify data...")
    # Load the Spotify data
    df = load_spotify_data(datalake_dir)

    # Calculate the age of the song in days
    df['track_age_days'] = (pd.Timestamp.now() - pd.to_datetime(df['release_date'])).dt.days

    # Convert duration from ms to minutes and round to two decimal places
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)

    # Rename columns for clarity before normalization
    df.rename(columns={'loudness': 'loudness_dB', 'tempo': 'tempo_bpm'}, inplace=True)

    # Normalize loudness and tempo to 0-1 scale
    df['loudness'] = (df['loudness_dB'] - df['loudness_dB'].min()) / (df['loudness_dB'].max() - df['loudness_dB'].min())
    df['tempo'] = (df['tempo_bpm'] - df['tempo_bpm'].min()) / (df['tempo_bpm'].max() - df['tempo_bpm'].min())

    # Bin popularity into categories
    df['popularity_category'] = pd.cut(df['popularity'], bins=[0, 33, 66, 100], labels=['Low', 'Medium', 'High'])

    # New feature: Energy Efficiency Index
    df['energy_efficiency'] = df['energy'] * df['danceability'] / (df['loudness'] + 1)  # Avoid division by zero

    # New feature: Emotional Impact
    df['emotional_impact'] = df['valence'] * df['energy'] * df['liveness']

    # Drop unnecessary columns
    df.drop(columns=['type', 'uri', 'track_href', 'analysis_url', 'duration_ms'], inplace=True)

    # Display the result
    print(f"Spotify Data now has {df.shape[0]} rows and {df.shape[1]} columns")
    print(f"First row of the processed DataFrame: {df.iloc[0].to_dict()}")

    # Save the processed data to parquet
    try:
        df.to_parquet(f'{datalake_dir}formatted/spotify.parquet', index=False)
        #df.to_csv(f'{datalake_dir}formatted/spotify.csv', index=False)
    except Exception as e:
        print(f"Failed to save processed Spotify data to Parquet: {e}")
        return
    print(f"Spotify data transformed successfully!")