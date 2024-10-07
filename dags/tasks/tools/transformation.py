from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark.sql.functions import current_date, to_date, col, round as spark_round, when, min, max, length, concat, lit
def adjust_date_format(df, col_name):
    return df.withColumn(
        col_name,
        when(length(col(col_name)) == 4, concat(col(col_name), lit("-01-01"))) \
        .when(length(col(col_name)) == 7, concat(col(col_name), lit("-01"))) \
        .otherwise(col(col_name))
    )
def set_data_types(top_tracks,numerical_columns, integer_columns, boolean_columns, date_col=None):

    for column in numerical_columns:
        top_tracks = top_tracks.withColumn(column, col(column).cast(DoubleType()))
    for column in integer_columns:
        top_tracks = top_tracks.withColumn(column, col(column).cast(IntegerType()))
    for column in boolean_columns:
        top_tracks = top_tracks.withColumn(column, col(column).cast(BooleanType()))
    if date_col is not None:
        top_tracks = adjust_date_format(top_tracks, date_col)
        top_tracks = top_tracks.withColumn(date_col, to_date(col(date_col), "yyyy-MM-dd"))
    return top_tracks

def transform_audio_features(df):
    # Calculate the age of the song in days
    df = df.withColumn("track_age_days", (current_date() - col("release_date")).cast("int"))

    # Convert duration from ms to minutes and round to two decimal places
    df = df.withColumn("duration_min", spark_round(col("duration_ms") / 60000, 2))

    # Rename columns for clarity before normalization
    df = df.withColumnRenamed("loudness", "loudness_dB").withColumnRenamed("tempo", "tempo_bpm")

    # Calculate normalization parameters first
    loudness_stats = \
        df.agg(min(col("loudness_dB")).alias("min_loudness"), max(col("loudness_dB")).alias("max_loudness")).collect()[
            0]
    tempo_stats = df.agg(min(col("tempo_bpm")).alias("min_tempo"), max(col("tempo_bpm")).alias("max_tempo")).collect()[
        0]

    # Normalize loudness and tempo to 0-1 scale
    df = df.withColumn("loudness", (col("loudness_dB") - loudness_stats["min_loudness"]) /
                       (loudness_stats["max_loudness"] - loudness_stats["min_loudness"]))
    df = df.withColumn("tempo", (col("tempo_bpm") - tempo_stats["min_tempo"]) /
                       (tempo_stats["max_tempo"] - tempo_stats["min_tempo"]))

    # New feature: Energy Efficiency Index
    df = df.withColumn("energy_efficiency", col("energy") * col("danceability") / (col("loudness") + 1))

    # New feature: Emotional Impact
    df = df.withColumn("emotional_impact", col("valence") * col("energy") * col("liveness"))
    return df

def create_popularity_category(df,platform_str):
    # Bin popularity into categories
    popularity_column = f"{platform_str}_popularity"
    df = df.withColumn(f"{popularity_column}_category", when(col(popularity_column) <= 33, "Low")
                       .when((col(popularity_column) > 33) & (col(popularity_column) <= 66), "Medium")
                       .otherwise("High"))
    return df