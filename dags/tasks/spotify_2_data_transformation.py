from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, to_date, col, round as spark_round, when, min, max
def load_spotify_data(spark, path):
    try:
        top_tracks = spark.read.parquet(f'{path}/raw/spotify.parquet')
    except Exception as e:
        print(f"Failed to load Spotify data from Parquet: {e}")
        return None
    print(f"Loaded Spotify data with {top_tracks.count()} rows and {len(top_tracks.columns)} columns")
    return top_tracks

def transform_spotify_data(config, tools, transformation_tools, spark, debug=False):
    if spark is None: spark = SparkSession.builder.appName("TransformSpotifyData").getOrCreate()

    datalake_dir = config['paths']['datalake_dir']
    print("Transforming Spotify data...")
    top_tracks = load_spotify_data(spark, datalake_dir)
    if top_tracks is None:
        return

    # Set data types
    numerical_columns = ['duration_ms']
    integer_columns = ['rank', 'mode', 'time_signature', 'key']
    boolean_columns = ['explicit']
    date_col = 'release_date'
    top_tracks = transformation_tools.set_data_types(top_tracks, numerical_columns, integer_columns, boolean_columns,
                                                     date_col=date_col)
    if debug:
        print("Spotify data types:")
        top_tracks.printSchema()

    # New columns and transformations
    top_tracks = transformation_tools.create_popularity_category(top_tracks, 'spotify')
    top_tracks = transformation_tools.transform_audio_features(top_tracks)

    # Drop columns that are now unnecessary
    top_tracks = top_tracks.drop('duration_ms','artist_id')

    # Save the processed data to parquet
    file_dir = f'{datalake_dir}formatted/spotify.parquet'
    tools.finalize_pipeline_step(top_tracks, file_dir, 'transformed', 'Spotify track', debug=debug, with_pandas=False)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    from dags.tasks.tools.general import *
    import dags.tasks.tools.general as tools
    from dags.tasks.tools import transformation as transformation_tools
    config = get_config()
    # Initialize Spark session
    spark = SparkSession.builder.appName("TransformSpotifyData").getOrCreate()
    transform_spotify_data(config,tools,transformation_tools,spark,debug=True)
