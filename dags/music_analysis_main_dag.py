# Import project files
from tasks.spotify_1_data_extraction import extract_spotify_data
from tasks.spotify_2_data_transformation import transform_spotify_data
from tasks.lastfm_1_data_extraction import extract_lastfm_data
from tasks.lastfm_2_data_enrichment import enrich_lastfm_data
from tasks.lastfm_3_data_transformation import transform_lastfm_data
from tasks.final_1_data_cross_source_augmentation import augment_data
from tasks.final_2_data_combination import combine_data_sources
from tasks.final_4_data_indexing import index_data
from tasks.final_3_data_ML_enhancement import enhance_unique_songs_with_ml
from tasks import tools
import tasks.tools.general as tools
from tasks.tools.general import *
from tasks.tools import transformation as transformation_tools
from tasks.tools.spotify import *
import tasks.tools.spotify as spotify_tools
import data_params


# Import libraries
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime
import time
import os

import requests
from pyspark.sql import SparkSession

config = get_config()

# Initialize connection to APIs and spark session because we will use them in multiple tasks
spotify_access_token = authenticate_spotify(data_params.spotify_client_id, data_params.spotify_client_secret)
#spark = SparkSession.builder.appName("MusicAnalysisApp").getOrCreate()
spark = None
set_pandas_settings(pd)
retries = 1

# Define default arguments for your DAG
start_date = datetime.today()
start_date = (start_date.replace(hour=0, minute=0, second=0) + timedelta(days=1))
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # set start date as 2024 january
    'start_date': datetime(2024, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': retries,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG('music_data_analysis',
          default_args=default_args,
          description='Analyze music data from Spotify and Last.fm',
          schedule_interval='@daily',  # This can be adjusted to your needs
          catchup=False)

# Define tasks
extract_spotify_task = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    op_kwargs={'data_params': data_params,
               'access_token': spotify_access_token,
               'limit': data_params.ntracks_per_country,
               'config': config,
               'debug': data_params.debug,
               'tools': tools,
               'spotify_tools': spotify_tools
               },
    dag=dag)

transform_spotify_task = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=transform_spotify_data,
    op_kwargs={
        'config': config,
        'tools': tools,
        'transformation_tools': transformation_tools,
        'spark': spark,
        'debug': data_params.debug,
               },
    dag=dag)

extract_lastfm_task = PythonOperator(
    task_id='extract_lastfm_data',
    python_callable=extract_lastfm_data,
    op_kwargs={'countries': data_params.country_names,
               'continents': data_params.continents,
               'api_key': data_params.lastfm_api_key,
               'limit': data_params.ntracks_per_country,
               'config': config,
               'tools': tools,
               'debug': data_params.debug,
               },
    dag=dag)

enrich_lastfm_task = PythonOperator(
    task_id='enrich_lastfm_data',
    python_callable=enrich_lastfm_data,
    op_kwargs={'config': config,
               'access_token': spotify_access_token,
               'debug': data_params.debug,
               'tools': tools,
               'transformation_tools': transformation_tools,
               'spotify_tools': spotify_tools,
               'data_params': data_params,
               },
    dag=dag)

transform_lastfm_task = PythonOperator(
    task_id='transform_lastfm_data',
    python_callable=transform_lastfm_data,
    op_kwargs={
        'config': config,
        'tools': tools,
        'transformation_tools': transformation_tools,
        'spark': spark,
        'debug': data_params.debug,
    },
    dag=dag)

cross_source_augment_task = PythonOperator(
    task_id='enhance_data',
    python_callable=augment_data,
    op_kwargs={'config': config,
               'debug': data_params.debug,
               'access_token': spotify_access_token,
               'tools': tools,
               'data_params': data_params,
               },
    dag=dag)

combine_data_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data_sources,
    op_kwargs={
        'config': config,
        'spark': spark,
        'country_names_to_code': data_params.country_names_to_code,
    },
    dag=dag)
ml_enhance_data_task = PythonOperator(
    task_id='ml_enhance_data',
    python_callable=enhance_unique_songs_with_ml,
    op_kwargs={
        'config': config,
    },
    dag=dag)
index_data_task = PythonOperator(
    task_id='index_data',
    python_callable=index_data,
    op_kwargs={'config': config},
    dag=dag)


# Set task dependencies
extract_spotify_task >> transform_spotify_task
extract_lastfm_task >> enrich_lastfm_task
enrich_lastfm_task >> transform_lastfm_task

# Join the 2 branches
[transform_spotify_task, transform_lastfm_task] >> cross_source_augment_task
cross_source_augment_task >> combine_data_task
combine_data_task >> ml_enhance_data_task
ml_enhance_data_task >> index_data_task
