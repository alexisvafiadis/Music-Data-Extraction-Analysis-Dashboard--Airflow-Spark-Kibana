import requests
import time

wrong_keywords = ['Remaster', 'Live', 'Edition', 'Instrumental', 'Radio Edit', 'Acoustic']
def filter_search_results(search_results):
    for track_info in search_results:
        if not any(word in track_info['name'] for word in wrong_keywords):
            break
    return track_info
def consolidate_data(track_data, audio_features_data):
    for track in track_data:
        track_id = track['track_id']
        features = audio_features_data.get(track_id, {})
        track.update(features)  # Merge audio features into track data
    return track_data

def get_tracks_from_json(tracks_json, country, limit):
    track_data = []
    for i, item in enumerate(tracks_json.get('items', [])):
        track = item['track']
        if track:
            track_data.append({
                'country': country,
                'rank': i + 1,
                'track_id': track['id'],
                'track_name': track['name'],
                'artist': track['artists'][0]['name'] if track['artists'] else None,
                'artist_id': track['artists'][0]['id'] if track['artists'] else None,
                'album': track['album']['name'] if track['album'] else None,
                'duration_ms': track['duration_ms'],
                'spotify_popularity': track['popularity'],
                'explicit': track['explicit'],
                'release_date': track['album']['release_date'] if track['album'] else None,
            })
    return track_data

def get_audio_features(access_token, track_ids, data_params):
    url = f"https://api.spotify.com/v1/audio-features?ids={','.join(track_ids)}"
    headers = {"Authorization": f"Bearer {access_token}"}
    time.sleep(data_params.spotify_api_call_delay)
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch audio features: ({response.status_code}) - {response.text}")
        # throw exception !!
    output = {}
    # only get energy, danceability, etc and don't change the type
    audio_features = response.json()['audio_features']
    for feature in audio_features:
        output[feature['id']] = {
            'energy': feature['energy'],
            'danceability': feature['danceability'],
            'key': feature['key'],
            'loudness': feature['loudness'],
            'mode': feature['mode'],
            'speechiness': feature['speechiness'],
            'acousticness': feature['acousticness'],
            'instrumentalness': feature['instrumentalness'],
            'liveness': feature['liveness'],
            'valence': feature['valence'],
            'tempo': feature['tempo'],
            'time_signature': feature['time_signature'],
        }
    return output