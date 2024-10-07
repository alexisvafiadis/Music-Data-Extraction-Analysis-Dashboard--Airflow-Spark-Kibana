import os
import time
from datetime import datetime, timedelta
import random
from elasticsearch import Elasticsearch, helpers

# ⚠️Be careful to put Elastic URL and not Kibana Url !! On the Cloud version, you should see “es” and not “kb” in the URL.
# ⚠️On Cloud, don’t forget the port ! The host should be something like https://deployment-es.us-central1.gcp.cloud.es.io:9243
user='elastic'
ELASTIC_PASSWORD = "PooVMD0i5NwyhKajF82ftmFc"
CLOUD_ID = "BigDataProject:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NmZiYjE2ZGY0M2Q0Y2QxODk1MWRjOWRlZjgyZmNlOCQ5NTZlOWM0NTA0ZDk0NjkyOWYyMzc1MjYxNzQyM2MwYQ=="
client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
)
print(client.info())

cities = ["New York", "Tokyo", "London", "Paris", "Sydney", "Beijing", "Mumbai", "Rio de Janeiro", "Cairo", "Moscow",
         "Toronto", "Dubai", "Los Angeles", "Berlin", "Rome", "Seoul", "Cape Town", "Bangkok", "Istanbul",
         "Mexico City", "Amsterdam", "Singapore", "Stockholm", "Lagos", "Buenos Aires", "Jakarta", "New Delhi",
         "Toronto", "Chicago", "Madrid", "Shanghai", "Hong Kong", "Dublin", "Vienna", "Barcelona", "Athens",
         "Copenhagen", "Nairobi", "Lima", "Warsaw", "Oslo", "Budapest", "Prague", "Auckland", "Hanoi", "Kuala Lumpur",
         "Johannesburg", "San Francisco"]


while True:
   docs = []
   log_date = datetime.utcnow()
   docs.append({"city": random.choice(cities), "co2": random.randint(10000, 1000000), "timestamp": log_date})
   helpers.bulk(client, docs, index="carbone_co2_worldwide_realtime")
   print(f"{len(docs)} documents indexed !")
   print("Waiting 5seconds before next index ...")
   time.sleep(1)
