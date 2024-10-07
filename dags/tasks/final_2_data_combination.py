#import findspark
#findspark.init("/opt/spark-3.5.1")

from pyspark.sql import SQLContext
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, desc, when, lit, coalesce, lead, months_between, udf
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
from pyspark.sql import Window


def combine_data_sources(config, spark, country_names_to_code, debug=True):
    if spark is None: spark = SparkSession.builder.appName("CombineData").getOrCreate()
    datalake_dir = config['paths']['datalake_dir']
    print("Combining data sources...")

    # Load the formatted data
    spotify_df = spark.read.parquet(f"{datalake_dir}formatted/spotify.parquet")
    lastfm_df = spark.read.parquet(f"{datalake_dir}formatted/lastfm.parquet")
    artists_df = spark.read.parquet(f"{datalake_dir}formatted/artists.parquet")
    artists_albums_df = spark.read.parquet(f"{datalake_dir}formatted/artists_albums.parquet")
    #print(f"Number of total continents : {spotify_df.select('continent').distinct().count()}")

    # Adding country_code column to Spotify and Last.fm DataFrames
    bc_country_codes = spark.sparkContext.broadcast(country_names_to_code)
    def get_country_code(country_name):
        return bc_country_codes.value.get(country_name, None)

    udf_country_code = udf(get_country_code, StringType())
    spotify_df = spotify_df.withColumn("country_code", udf_country_code(col("country")))
    lastfm_df = lastfm_df.withColumn("country_code", udf_country_code(col("country")))

    #if debug: spotify_df.select("country", "country_code").show(1000)

    lastfm_df, spotify_df = align_union_dfs(lastfm_df, spotify_df)
    artists_albums_df = calc_months_between_album_releases(artists_albums_df)

    # Add the genre column to both dataframes before making more complex joins
    #spotify_df = spotify_df.join(artists_albums_df.select("artist", "genre"), on="artist", how="left")
    #lastfm_df = lastfm_df.join(artists_albums_df.select("artist", "genre"), on="artist", how="left")
    #spotify_df = spotify_df.withColumn("genre", coalesce(col("genre"), lit("Unidentified")))
    #lastfm_df = lastfm_df.withColumn("genre", coalesce(col("genre"), lit("Unidentified")))

    # Some debug info to check data validity
    if debug:
        print("Spotify columns:", spotify_df.columns)
        print("Last.fm columns:", lastfm_df.columns)
        print("Artists albums columns:", artists_albums_df.columns)
        print(f"Spotify columns that are not in Last.fm: {set(spotify_df.columns) - set(lastfm_df.columns)}")
        print(f"Last.fm columns that are not in Spotify: {set(lastfm_df.columns) - set(spotify_df.columns)}")
        # get minimum and maximum of rank
        #print(f"Minimum rank in Spotify data: {spotify_df.select(min('rank')).collect()[0][0]}")
        # check data types
        #print("Spotify data types:")
        #spotify_df.printSchema()
        #print("Last.fm data types:")
        #lastfm_df.printSchema()
        # print the genre column of artists_albums_df
        #artists_albums_df.select("genre").show(artists_albums_df.count(), False)
        #print("Number of countries in Spotify data:", spotify_df.select("country").distinct().count())

    # Register DataFrames as SQL temporary views
    spotify_df.createOrReplaceTempView("spotify")
    lastfm_df.createOrReplaceTempView("lastfm")
    artists_df.createOrReplaceTempView("artists")
    artists_albums_df.createOrReplaceTempView("artists_albums")

    ### Create the dataframes by combining our data sources

    # Combine spotify and lastfm songs to use when needed for overall analysis
    unioned_top_charts = spotify_df.unionByName(lastfm_df)
    unioned_top_charts.createOrReplaceTempView("unioned")

    # Unique songs with genre and ranking stats for genre specificities analysis
    unique_songs = spark.sql("""
        SELECT u.track_id, 
        COUNT(u.track_name) as num_countries,
        MIN(u.rank) as best_rank,
        AVG(u.rank) as avg_rank,
        FIRST(u.track_name) as track_name,
        FIRST(u.artist) as artist,
        FIRST(u.lastfm_listeners) as lastfm_listeners,
        FIRST(u.album) as album,
        FIRST(u.explicit) as explicit,
        FIRST(u.release_date) as release_date,
        FIRST(u.energy) as energy,
        FIRST(u.danceability) as danceability,
        FIRST(u.key) as key,
        FIRST(u.loudness_dB) as loudness_dB,
        FIRST(u.mode) as mode,
        FIRST(u.speechiness) as speechiness,
        FIRST(u.acousticness) as acousticness,
        FIRST(u.instrumentalness) as instrumentalness,
        FIRST(u.liveness) as liveness,
        FIRST(u.valence) as valence,
        FIRST(u.tempo_bpm) as tempo_bpm,
        FIRST(u.time_signature) as time_signature,
        FIRST(u.lastfm_popularity) as lastfm_popularity,
        FIRST(u.lastfm_popularity_category) as lastfm_popularity_category,
        FIRST(u.track_age_days) as track_age_days,
        FIRST(u.duration_min) as duration_min,
        FIRST(u.loudness) as loudness,
        FIRST(u.tempo) as tempo,
        FIRST(u.energy_efficiency) as energy_efficiency,
        FIRST(u.emotional_impact) as emotional_impact,
        FIRST(u.country_code) as country_code,
        FIRST(u.spotify_popularity) as spotify_popularity,
        FIRST(u.spotify_popularity_category) as spotify_popularity_category,
        FIRST(a.genre) as genre
        FROM unioned u
        LEFT JOIN artists a ON u.artist = a.artist
        WHERE genre <> 'Unidentified'
        GROUP BY u.track_id
    """)
    if debug:
        print(unique_songs.count())
        # check if a track with a single name has multiple entries
        unique_songs.select("track_name").groupBy("track_name").count().filter(col("count") > 1).show(100)
        # pritn all columns in unioned df that aren't in unique songs df
        print(f"Columns in unioned df that aren't in unique songs df: {set(unioned_top_charts.columns) - set(unique_songs.columns)}")

    # Performance of each genre across platforms
    genres_performance = spark.sql("""
        SELECT a.genre,
               AVG(u.rank) as avg_ranking, MIN(u.rank) as best_ranking, 
               AVG(u.spotify_popularity) as avg_song_popularity_on_spotify, MAX(u.spotify_popularity) as max_song_popularity_on_spotify,
               AVG(u.lastfm_listeners) as avg_listeners_on_lastfm, MAX(u.lastfm_listeners) as max_listeners_on_lastfm,
               SUM(CASE WHEN u.spotify_popularity is null THEN 0 ELSE 1 END) as num_top_songs_on_spotify, 
               SUM(CASE WHEN u.lastfm_popularity is null THEN 0 ELSE 1 END) as num_top_songs_on_lastfm,
               COUNT(*) as total_songs_in_genre
        FROM unioned u
        INNER JOIN artists a ON u.artist = a.artist
        WHERE (a.genre <> 'Unidentified')
        GROUP BY a.genre
    """)

    # Global and recent performance of each artist
    artists_performance = spark.sql("""
         SELECT a.artist, FIRST(a.genre) as genre, COUNT(DISTINCT a.album_name) AS total_albums,
                        SUM(a.album_popularity) as total_album_popularity, SUM(s.spotify_popularity) as total_spotify_popularity,
                        AVG(a.album_popularity) as avg_album_popularity, MAX(a.album_popularity) AS max_album_popularity, MIN(a.album_popularity) AS min_album_popularity,
                        AVG(s.rank) as avg_song_rank, MIN(s.rank) as best_song_rank,
                        AVG(s.spotify_popularity) as avg_song_popularity_on_spotify, MAX(s.spotify_popularity) as max_song_popularity_on_spotify,
                        AVG(a.total_tracks) as avg_total_tracks, 
                        AVG(a.available_markets) as avg_available_markets,
                        AVG(a.n_artists) AS avg_n_artists,
                        AVG(a.n_words_in_name) AS avg_words_in_album_name,
                        AVG(a.months_between_album_releases) AS avg_months_between_releases
        FROM artists_albums a
        JOIN spotify s ON a.artist = s.artist
        GROUP BY a.artist
    """)

    # Most viral spotify songs released last week
    viral_spotify_songs = spark.sql("""
        SELECT * FROM spotify
        WHERE track_age_days <= 14
        ORDER BY spotify_popularity DESC
    """)

    # Top 1 spotify song per country
    top1_spotify_songs = spark.sql("""
        SELECT country, FIRST(continent) as continent, FIRST(track_name) as top_track, FIRST(a.genre) as genre, FIRST(s.artist) as artist,
               FIRST(danceability) as danceability, FIRST(energy) as energy, FIRST(loudness_dB) as loudness_dB,
               FIRST(speechiness) as speechiness, FIRST(acousticness) as acousticness, FIRST(instrumentalness) as instrumentalness,
               FIRST(liveness) as liveness, FIRST(valence) as valence, FIRST(tempo_bpm) as tempo_bpm, FIRST(duration_min) as duration_min,
               FIRST(spotify_popularity) as popularity
        FROM spotify s
        LEFT  JOIN artists a ON s.artist = a.artist
        GROUP BY country
        ORDER BY MAX(spotify_popularity) DESC
    """)

    if debug:
        print("Unioned Top Charts:")
        display_dataset_info(unioned_top_charts)
        print("Unique Songs for Genre Analysis:")
        display_dataset_info(unique_songs)
        print("Genres Performance:")
        display_dataset_info(genres_performance)
        print("Artists Performance:")
        display_dataset_info(artists_performance)
        print("Viral Spotify Songs:")
        display_dataset_info(viral_spotify_songs)
        print("Top 1 Spotify Songs per Country:")
        display_dataset_info(top1_spotify_songs)
        #print("Max of num_countries of unique songs :")
        #unique_songs.select("num_countries").agg(max("num_countries")).show()

    # cast lastfm_listeners column of unique_songs to double in case it is void if too few rows from lastfm
    for col_name in ['lastfm_listeners','lastfm_url','lastfm_popularity','lastfm_popularity_category']:
        if col_name in unique_songs.columns:
            unique_songs = unique_songs.withColumn(col_name, col(col_name).cast(DoubleType()))
        if col_name in viral_spotify_songs.columns:
            viral_spotify_songs = viral_spotify_songs.withColumn(col_name, col(col_name).cast(DoubleType()))

    save_dir = f"{datalake_dir}combined/"
    #spark.sparkContext.setLogLevel("INFO")
    #print("Number of rows in DataFrame:", unioned_top_charts.count())
    #test_df = spark.createDataFrame([("test", 1)], ["name", "value"])
    #test_df.write.save(f"{datalake_dir}test_output.parquet", format="parquet", mode="overwrite")
    combined_dfs_dict = {
        "unioned_top_charts": unioned_top_charts,
        "unique_songs": unique_songs,
        "genres_performance": genres_performance,
        "artists_performance": artists_performance,
        "viral_spotify_songs": viral_spotify_songs,
        "top1_spotify_songs": top1_spotify_songs
    }
    for name, df in combined_dfs_dict.items():
        #df.printSchema()
        try:
            df.write.save(f"{save_dir}{name}.parquet", format="parquet", mode="overwrite")
        except Exception as e:
            print(f"Failed to save {name} data to Parquet: {e}")
            return

    print("Data sources combined successfully!")
    spark.stop()

def display_dataset_info(dataset):
    print(f"Shape of the dataset : {dataset.count()} rows and {len(dataset.columns)} columns")
    dataset.show(10)
def align_union_dfs(df1, df2):
    # Create a list of all columns in the order they first appear across both DataFrames
    combined_columns = list(dict.fromkeys(df1.columns + df2.columns))

    # Align df1 columns
    df1_aligned_columns = [
        df1[col].alias(col) if col in df1.columns else lit(None).alias(col)
        for col in combined_columns
    ]
    df1_aligned = df1.select(*df1_aligned_columns)

    # Align df2 columns
    df2_aligned_columns = [
        df2[col].alias(col) if col in df2.columns else lit(None).alias(col)
        for col in combined_columns
    ]
    df2_aligned = df2.select(*df2_aligned_columns)

    return df1_aligned, df2_aligned

def calc_months_between_album_releases(df):
    # Create a window specification
    window = Window.partitionBy("artist").orderBy("release_date")

    # Calculate the difference in months between the current and previous release
    df = df.withColumn("previous_release_date", lead("release_date", 1).over(window))
    df = df.withColumn("months_between_album_releases", - months_between(col("release_date"), col("previous_release_date")))
    df = df.drop("previous_release_date")
    return df

if __name__ == "__main__":
    from dags.tasks.tools.general import *
    from dags import data_params
    spark = SparkSession.builder.appName("CombineData").getOrCreate()
    config = get_config()
    combine_data_sources(config, spark, data_params.country_names_to_code, debug=True)