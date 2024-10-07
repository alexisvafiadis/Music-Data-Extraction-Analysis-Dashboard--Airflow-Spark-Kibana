########################################
# SET RUN AND DATA FETCH SETTINGS
debug = False
must_be_fast = True
account = 3
ntracks_per_country = 50 if not debug else 10
country_limit = None if not debug else 4
########################################

# API INFO
lastfm_api_key = '74875f44a22a8931c89b3f3495ba6c61'
lastfm_shared_secret = '477ff083d9b9dadd2cbf71611b8ed213'

if account == 1:
    spotify_client_id = 'e747146f2d1b4a5fa47893ed8b2400c8'
    spotify_client_secret = 'a49d33c0767c4cbc9413d3c7c8e60834'
elif account == 2:
    spotify_client_id = 'ba5a4e6478234282a966c30050e20cf8'
    spotify_client_secret = '463190736993417bb4741d0f0a11f7e8'
elif account == 3:
    spotify_client_id = 'a4e7fc95284648089140e2ec634035e3'
    spotify_client_secret = '41f644ce7c934fae8483be685c369998'
if must_be_fast:
    lastfm_api_call_delay = 0
    spotify_api_call_delay = 0
else:
    lastfm_api_call_delay = 0.05 if not debug else 0.08
    spotify_api_call_delay = 0.08 if not debug else 0.08


# COUNTRIES TO FETCH
country_codes_to_name_id_and_continent = {
    ### AMERICA
    "US": ("United States", "37i9dQZEVXbLRQDuF5jeBp", "America"),
    "CA": ("Canada", "37i9dQZEVXbKj23U1GF4IR", "America"),
    "MX": ("Mexico", "37i9dQZEVXbO3qyFxbkOE1", "America"),
    "BR": ("Brazil", "37i9dQZEVXbMXbN3EUUhlg", "America"),
    "AR": ("Argentina", "37i9dQZEVXbMMy2roB9myp", "America"),
    ### EUROPE
    "GB": ("United Kingdom", "37i9dQZEVXbLnolsZ8PSNw", "Europe"),
    "DE": ("Germany", "37i9dQZEVXbJiZcmkrIHGU", "Europe"),
    "FR": ("France", "37i9dQZEVXbIPWwFssbupI", "Europe"),
    "IT": ("Italy", "37i9dQZEVXbIQnj7RRhdSX", "Europe"),
    "ES": ("Spain", "37i9dQZEVXbNFJfN1Vw8d9", "Europe"),
    ### AFRICA
    "NG": ("Nigeria", "37i9dQZEVXbKY7jLzlJ11V", "Africa"),
    "ZA": ("South Africa", "37i9dQZEVXbMH2jvi6jvjk", "Africa"),
    "EG": ("Egypt", "37i9dQZEVXbLn7RQmT5Xv2", "Africa"),
    ### ASIA
    "IN": ("India", "37i9dQZEVXbLZ52XmnySJg", "Asia"),
    "ID": ("Indonesia", "37i9dQZEVXbObFQZ3JLcXt", "Asia"),
}


if country_limit is not None:
    #country_codes_to_name = {k: v for k, v in list(country_codes_to_name.items())[:country_limit]}
    country_codes_to_name_id_and_continent = {k: v for k, v in list(country_codes_to_name_id_and_continent.items())[:country_limit]}
print(f"{len(country_codes_to_name_id_and_continent)} countries were chosen because the debug state is {debug}")
country_names_to_code = {v[0]: k for k, v in country_codes_to_name_id_and_continent.items()}
#print(country_names_to_code)
country_codes = list(country_codes_to_name_id_and_continent.keys())
country_playlist_ids, country_names, continents = [], [], []
for value in list(country_codes_to_name_id_and_continent.values()):
    country_playlist_ids.append(value[1])
    country_names.append(value[0])
    continents.append(value[2])

