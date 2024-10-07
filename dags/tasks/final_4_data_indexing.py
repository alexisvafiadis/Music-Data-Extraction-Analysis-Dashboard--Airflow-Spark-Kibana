import os
import time
from datetime import datetime, timedelta
import random
from elasticsearch import Elasticsearch, helpers
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

def delete_all_documents(client, index_name):
    client.delete_by_query(index=index_name, body={
        "query": {"match_all": {}}
    })
    print(f"All documents deleted from {index_name}.")


def index_data(config, debug=False):
    print("Indexing data...")

    # Elasticsearch setup
    user = 'elastic'
    ELASTIC_PASSWORD = "PooVMD0i5NwyhKajF82ftmFc"
    CLOUD_ID = "BigDataProject:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NmZiYjE2ZGY0M2Q0Y2QxODk1MWRjOWRlZjgyZmNlOCQ5NTZlOWM0NTA0ZDk0NjkyOWYyMzc1MjYxNzQyM2MwYQ=="
    client = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=(user, ELASTIC_PASSWORD)
    )

    # Spark setup
    sc = SparkContext(appName="IndexData")
    sqlContext = SQLContext(sc)

    # Reading data from Spark
    datalake_dir = config['paths']['datalake_dir']
    save_dir = f"{datalake_dir}combined/"
    dataframes = {
    "all_top_charts": sqlContext.read.parquet(f"{save_dir}/unioned_top_charts.parquet"),
    "unique_songs": sqlContext.read.parquet(f"{save_dir}/unique_songs_ml_enhanced.parquet"),
    "genres_performance": sqlContext.read.parquet(f"{save_dir}/genres_performance.parquet"),
    "artists_performance": sqlContext.read.parquet(f"{save_dir}/artists_performance.parquet"),
    "viral_spotify_songs": sqlContext.read.parquet(f"{save_dir}/viral_spotify_songs.parquet"),
    "top1_spotify_songs": sqlContext.read.parquet(f"{save_dir}/top1_spotify_songs.parquet")
    }

    # check that the data is complete and not resulting from debug processes by checking if top charts has at least 500 rows
    if dataframes['all_top_charts'].count() < 500:
        print("Data is not complete, ending indexing process to avoid overwriting existing data")
        return

    # Index each DataFrame into Elasticsearch
    for index_name, df in dataframes.items():
        # Collect data in the driver (be cautious with memory usage)
        data = df.collect()
        # check columns of desired dataframe
        #if index_name == 'unique_songs': print(f"Columns of {index_name}: {df.columns}")

        # Reduce data for debugging
        #if debug: data = data[:2]

        # Prepare documents for bulk indexing
        docs = []
        for record in data:
            doc = record.asDict()  # Convert Row to dictionary
            doc['timestamp'] = datetime.utcnow().isoformat()  # Add a timestamp field
            if debug: print(doc)  # Print the document for debugging
            docs.append(doc)

        # Delete all documents from the index before indexing new ones
        delete_all_documents(client, index_name)

        # Bulk index the documents to Elasticsearch
        helpers.bulk(client, docs, index=index_name)
        print(f"Total {len(docs)} documents indexed in {index_name}!")

    # Clean up
    sc.stop()

if __name__ == "__main__":
    from dags.tasks.tools.general import *
    from dags import data_params
    config = get_config()
    index_data(config, debug=False)