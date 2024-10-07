from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, when

def transform_lastfm_data(config, tools, transformation_tools, spark, debug=False):
    ###
    ### THIS USES THE TRANSFORMATION TOOLS FROM THE TOOLS PACKAGE
    ###
    if spark is None: spark = SparkSession.builder.appName("TransformLastFMData").getOrCreate()
    datalake_dir = config['paths']['datalake_dir']

    # Load data using Spark
    top_tracks = spark.read.parquet(f'{datalake_dir}enriched/lastfm.parquet')
    print(f"Loaded Lastfm track data with {top_tracks.count()} rows and {len(top_tracks.columns)} columns")

    # Set data types
    numerical_columns = ['duration_ms', 'lastfm_listeners']
    integer_columns = ['rank', 'mode', 'time_signature','key']
    boolean_columns = ['explicit']
    date_col = 'release_date'
    top_tracks = transformation_tools.set_data_types(top_tracks, numerical_columns, integer_columns, boolean_columns, date_col=date_col)
    if debug:
        print("LastFM data types:")
        top_tracks.printSchema()

    # Create artificial popularity column based on listeners
    max_listeners = top_tracks.agg(spark_max(col('lastfm_listeners')).alias('max_listeners')).collect()[0]['max_listeners']
    print(f"Max listeners found : {max_listeners}")
    if max_listeners is not None:
        top_tracks = top_tracks.withColumn('lastfm_popularity', (col('lastfm_listeners') / max_listeners) * 100)

    # New columns and transformations
    top_tracks = transformation_tools.create_popularity_category(top_tracks, 'lastfm')
    top_tracks = transformation_tools.transform_audio_features(top_tracks)

    # Drop unnecessary columns
    top_tracks = top_tracks.drop('duration_ms','spotify_popularity')

    # For debugging and checking transformations
    file_dir = f'{datalake_dir}formatted/lastfm.parquet'
    tools.finalize_pipeline_step(top_tracks, file_dir,'transformed','Lastfm track', debug, with_pandas=False)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    import dags.tasks.tools.general as tools
    from dags.tasks.tools import transformation as transformation_tools
    spark = SparkSession.builder.appName("TransformLastFMData").getOrCreate()
    config = tools.get_config()
    transform_lastfm_data(config, tools, transformation_tools, spark, debug=True)



# not used anymore because we got the genre from the artist thanks to the spotify api, and getting the genre with lastfm
# is not always successful + would require to make a request to lastfm api for each spotify track which is long and unnecessary
def map_genre(genre_dict,tags, default='Unidentified'):
    for tag in tags:
        for genre, keywords in genre_dict.items():
            if tag in keywords:
                return genre
    return default  # Default genre if no match is found

genre_dict = {
    'Pop': ['pop', 'dance-pop', 'synthpop', 'alt-pop', 'indie pop', 'Progressive Pop', 'art pop', 'electropop',
            'chamber pop', 'baroque pop'],
    'Rock': ['pop rock', 'indie rock', 'psychedelic pop', 'alternative rock'],
    'Hip-Hop': ['hip hop', 'Hip-Hop', 'trap', 'rap'],
    'Country': ['country', 'country pop', 'americana', 'bro-country'],
    'Soul': ['soul', 'alternative rnb', 'rnb'],
    'Electronic': ['indietronica', 'deep house', 'dubstep'],
    'Folk': ['folk pop', 'singer-songwriter'],
    'New Wave': ['new wave', 'coldwave']
}

def generate_genre_column(top_tracks, genre_dict):
    top_tracks['genre'] = top_tracks['tags'].apply(lambda x: map_genre(genre_dict, x))