import requests
import pandas as pd

import requests

def enhance_data(config,access_token, max_artists=None, debug=False, get_albums=True):
    if debug and max_artists is None: max_artists = 1
    print("Extracting artist data...")
    datalake_dir = config['paths']['datalake_dir']
    spotify_tracks_df = pd.read_parquet(f"{datalake_dir}formatted/spotify.parquet")
    lastfm_tracks_df = pd.read_parquet(f"{datalake_dir}formatted/lastfm.parquet")
    tracks_df = pd.concat([spotify_tracks_df, lastfm_tracks_df])

    artists = tracks_df[['artist']].drop_duplicates()
    if max_artists is not None:
        artists = artists.head(max_artists)
    print(f"Fetching data from {len(artists)} artists...")
    artists['artist_id'], artists['genres'] = zip(*artists['artist'].apply(lambda x: get_artist_info(x, access_token)))
    # print names of artists we couldn't find and that have None as value
    artists_not_found = artists[artists['artist_id'].isnull()]
    if len(artists_not_found) > 0:
        print("Artists not found:")
        print(artists_not_found['artist'])

    #artists['artist_id'] = artists['artist'].apply(lambda x: get_artist_id(x, access_token))
    if get_albums: artists['albums'] = artists['artist_id'].apply(lambda x: get_artist_albums(x, access_token))
    # get all unique genres from all tracks
    all_genres = set()
    for genres in artists['genres']:
        all_genres.update(genres)
    print("Unique genres found:")
    print(all_genres)
    artists['genre'] = artists['genres'].apply(extract_main_genre_from_genres)

    if debug:
        print("Genre value counts:")
        print(artists['genre'].value_counts())
        # print rows where genre is Unidentified
        unidentified_genres = artists[artists['genre'] == 'Unidentified']
        if len(unidentified_genres) > 0:
            print("Unidentified genres:")
            print(unidentified_genres.head(1000))

    if not get_albums:
        return
    expanded_album_data = pd.concat([pd.DataFrame(x) for x in artists['albums']], keys=artists['artist'])
    expanded_album_data.reset_index(level=0, inplace=True)
    expanded_album_data.rename(columns={'level_0': 'artist'}, inplace=True)

    # Merge genres into the expanded album data
    expanded_album_data = expanded_album_data.merge(artists[['artist', 'genre']], on='artist', how='left')
    try:
        expanded_album_data.to_parquet(f"{datalake_dir}formatted/artist_albums.parquet", index=False)
    except Exception as e:
        print(f"Failed to save artist album data to Parquet: {e}")
        return
    print(f"Artist album data extracted successfully!")
    print(f"Artist album data now has {len(expanded_album_data)} rows and {len(expanded_album_data.columns)} columns")
    print(f"First row of the extracted DataFrame : ")
    print(expanded_album_data.iloc[0].to_dict())

    # attach the genre to each track based on the genre of the artist
    spotify_tracks_df = spotify_tracks_df.merge(artists[['artist', 'genre']], on='artist', how='left')
    lastfm_tracks_df = lastfm_tracks_df.merge(artists[['artist', 'genre']], on='artist', how='left')

def get_artist_info(artist_name, access_token):
    """Fetch Spotify artist ID based on name."""
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    items = data.get('artists', {}).get('items', [])
    if items is None:
        return None, None
    first_items = items[0]
    return first_items['id'], first_items['genres']


def get_artist_albums(artist_id, access_token, debug=True):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"include_groups": "album,single", "limit": 50}  # Adjust as needed
    response = requests.get(url, headers=headers, params=params)
    response_json = response.json()

    if response.status_code != 200:
        print(f"API error: {response_json}")
        return []  # Return an empty list in case of API error

    albums = []
    for album in response_json.get('items', []):
        album_data = {
            'album_name': album['name'],
            'release_date': album['release_date'],
            'total_tracks': album['total_tracks'],
            'n_artists': len(album['artists']),
            'n_words_in_name': len(album['name'].split()),
            'available_markets': len(album['available_markets']),
            'album_type': album['album_type'],
        }
        album_details = get_album_details(album['id'], access_token)
        album_data.update(album_details)
        albums.append(album_data)
    if debug: print(f"Processed {len(albums)} albums for artist ID {artist_id}")
    return albums


def get_album_details(album_id, access_token):
    """Fetch detailed album data including popularity."""
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    album_info = response.json()

    return {
        'album_popularity': album_info.get('popularity', 0),
    }


dict_genre_to_words_it_must_contain = {
    'World Music': ['mahraganat', 'shaabi', 'egyptian alternative', 'amapiano', 'afrobeats'],
    'Pop': ['pop', 'new romantic', 'disco','neo', 'easy', 'adult standards','dance','reggaeton','funk','bollywood'],
    'Rock': ['rock', 'punk', 'grunge', 'punk blues', 'permanent wave','metal'],
    'Hip-Hop': ['rap', 'hip','hop','drill'],
    'Country': ['country', 'americana','corrido','sierreno','chihuahuense', 'norteno','ranchera','tejano','sertanejo','tex mex','regional mexican','banda','duranguense','grupero'],
    'Electronic': ['electronic','electro', 'indietronica', 'shoegaze','french touch','french house', 'filter house', 'indie','bassline','edm','techno','house','dubstep','gaming'],
    'Folk': ['folk','maskandi','forro','vaqueiro'],
    'Jazz': ['jazz'],
    'Blues': ['blues'],
    'Latin': ['reggaeton', 'latin', 'salsa', 'tango', 'flamenco', 'samba', 'bossa nova','urbano'],
    'Soul': ['soul', 'motown', 'r&b'],
}

def extract_main_genre_from_genres(genres):
    for genre in genres[:5]:
        for main_genre, keywords in dict_genre_to_words_it_must_contain.items():
            for keyword in keywords:
                if keyword in genre:
                    return main_genre
    return "Unidentified" # Default genre if no match is found

if __name__ == "__main__":
    from dags.tasks.tools import *
    from dags import data_params
    config = get_config()
    set_pandas_settings(pd)
    access_token = authenticate_spotify(data_params.spotify_client_id, data_params.spotify_client_secret)
    enhance_data(get_config(), access_token, debug=True, max_artists=-1)