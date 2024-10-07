import requests
import pandas as pd
import time
import requests
import os
def load_if_exists(path,dataset_name):
    if os.path.exists(path):
        print(f"Loaded existing {dataset_name} data.")
        return pd.read_parquet(path)
    else:
        return pd.DataFrame()

def augment_data(config, access_token, tools, data_params, max_artists=None, debug=False, get_albums=True, force_refetch=False):
    if debug and max_artists is None: max_artists = 3
    print("Extracting artist data...")
    datalake_dir = config['paths']['datalake_dir']
    artists_path = f"{datalake_dir}formatted/artists.parquet"
    albums_path = f"{datalake_dir}formatted/artists_albums.parquet"

    #Load data
    if force_refetch:
        artists = pd.DataFrame()
        expanded_album_data = pd.DataFrame()
    else:
        artists = load_if_exists(artists_path, "artist")
        if get_albums: expanded_album_data = load_if_exists(albums_path, "album")
    spotify_tracks_df = pd.read_parquet(f"{datalake_dir}formatted/spotify.parquet")
    lastfm_tracks_df = pd.read_parquet(f"{datalake_dir}formatted/lastfm.parquet")
    tracks_df = pd.concat([spotify_tracks_df, lastfm_tracks_df])

    # Get artists we need to fetch data for
    new_artists = tracks_df[['artist']].drop_duplicates()
    if max_artists is not None:
        new_artists = new_artists.head(max_artists)
    print(f"{len(new_artists)} unique artists found.")
    if artists.shape[0] > 0:
        # Filter out artists already processed in artists
        new_artists = new_artists[(~new_artists['artist'].isin(artists['artist']))]
        #new_artists = new_artists[(~new_artists['artist'].isin(artists['artist'])) | (~new_artists['artist'].isin(expanded_album_data['artist']))]
    print(f"{len(new_artists)} new artists to process.")

    # Fetch data
    if len(new_artists) == 0:
        print("Nothing to fetch, ending pipeline step")
        return
    print(f"Fetching artist data...")
    # show all artists
    #print(new_artists)
    new_artists['artist_id'], new_artists['genres'] = zip(*new_artists['artist'].apply(lambda x: get_artist_info(x, access_token, tools, data_params)))

    # Debug : Print names of eventual artists we couldn't find and that have None as value
    artists_not_found = new_artists[new_artists['artist_id'].isnull()]
    if len(artists_not_found) > 0:
        print("Artists not found:")
        print(artists_not_found['artist'])

    new_artists['genre'] = new_artists['genres'].apply(extract_main_genre_from_genres)
    # Debug : get all unique genres from all tracks, genre counts and rows from unidentified genres
    if debug:
        all_genres = set()
        for genres in new_artists['genres']:
            all_genres.update(genres)
        print("Unique genres found:")
        print(all_genres)
        print("Genre value counts:")
        print(new_artists['genre'].value_counts())
        # print rows where genre is Unidentified
        unidentified_genres = new_artists[new_artists['genre'] == 'Unidentified']
        if len(unidentified_genres) > 0:
            print("Unidentified genres:")
            print(unidentified_genres.head(1000))
    # Remove the now useless "genres" column
    new_artists.drop(columns=['genres'], inplace=True)

    # Update the main artists DataFrame
    artists = pd.concat([artists, new_artists])

    # Save and print important info
    tools.finalize_pipeline_step(artists, artists_path, "extracted and processed", "Artist")

    # attach the genre to each track based on the genre of the artist and save the datasets again
    # unneeded with the combination step..
    #spotify_tracks_df = spotify_tracks_df.merge(artists[['artist', 'genre']], on='artist', how='left')
    #lastfm_tracks_df = lastfm_tracks_df.merge(artists[['artist', 'genre']], on='artist', how='left')
    #finalize_pipeline_step(spotify_tracks_df, f"{datalake_dir}enhanced/spotify.parquet", "enhanced", "Spotify track")
    #finalize_pipeline_step(lastfm_tracks_df, f"{datalake_dir}enhanced/lastfm.parquet", "enhanced", "Lastfm track")

    if not get_albums:
        return
    # Fetch albums for the new artists
    new_artists['albums'] = new_artists['artist_id'].apply(lambda x: get_artist_albums(x, access_token, tools, data_params))
    new_expanded_album_data = pd.concat([pd.DataFrame(x) for x in new_artists['albums']], keys=new_artists['artist'])
    new_expanded_album_data.reset_index(level=0, inplace=True)
    new_expanded_album_data.rename(columns={'level_0': 'artist'}, inplace=True)

    # Merge genres into the expanded album data
    new_expanded_album_data = new_expanded_album_data.merge(new_artists[['artist', 'genre']], on='artist', how='left')

    # Update the main albums DataFrame
    expanded_album_data = pd.concat([expanded_album_data, new_expanded_album_data])

    # Save and print important info
    tools.finalize_pipeline_step(expanded_album_data, albums_path, "extracted and processed", "Album", debug=debug)
def get_artist_info(artist_name, access_token, tools, data_params):
    """Fetch Spotify artist ID based on name."""
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    data = tools.make_api_request(url, headers, params, data_params.spotify_api_call_delay)
    items = data.get('artists', {}).get('items', [])
    if items is None:
        return None, None
    first_items = items[0]
    return first_items['id'], first_items['genres']


def get_artist_albums(artist_id, access_token, tools, data_params, debug=True):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"include_groups": "album,single", "limit": 50}  # Adjust as needed
    response_json = tools.make_api_request(url, headers, params, time_between_calls=data_params.spotify_api_call_delay)

    albums = []
    for album in response_json.get('items', []):
        if album['album_type'] != 'album': #only consider actual albums and not singles, compilations, etc.
            continue
        if "(" in album['name']: #avoid live, remastered, etc.
            continue
        album_data = {
            'album_name': album['name'],
            'release_date': album['release_date'],
            'total_tracks': album['total_tracks'],
            'n_artists': len(album['artists']),
            'n_words_in_name': len(album['name'].split()),
            'available_markets': len(album['available_markets']),
            'is_album_name_capitalized': album['name'].isupper(),
        }
        album_details = get_album_details(album['id'], access_token, tools, data_params)
        album_data.update(album_details)
        albums.append(album_data)
    if debug: print(f"Processed {len(albums)} albums for artist ID {artist_id}")
    return albums


def get_album_details(album_id, access_token, tools, data_params):
    """Fetch detailed album data including popularity."""
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    album_info = tools.make_api_request(url, headers, {}, data_params.spotify_api_call_delay)

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
    from dags.tasks.tools.general import *
    from dags.tasks.tools.spotify import *
    import dags.tasks.tools.general as tools
    from dags import data_params
    config = get_config()
    set_pandas_settings(pd)
    access_token = authenticate_spotify(data_params.spotify_client_id, data_params.spotify_client_secret)
    augment_data(get_config(), access_token, tools, data_params, debug=True, max_artists=-1, force_refetch=False, get_albums=True)