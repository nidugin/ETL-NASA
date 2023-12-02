"""
producer_nikita.py
Fetching data from NASA API
and sending to the topic.

Created by Nikita Roldugins 08/06/2023.
"""


import requests
import json
import sys
from kafka import KafkaProducer
from configparser import ConfigParser


config = ConfigParser()
config.read("/home/airflow_usr/scripts/nikita/config.ini")

env_name = sys.argv[1]
api_name = sys.argv[2]
key_api = sys.argv[3]

kafka_ip = config[env_name]["kafka_ip"]
topic_name = config[env_name][f"topic_{api_name}"]

url_map = {
    "apod": "https://api.nasa.gov/planetary/apod?api_key=",
    "neo": "https://api.nasa.gov/neo/rest/v1/neo/browse?api_key="
}


def fetch_api():
    """
    Fetches specific NASA API according to api_name.
    :return: fetched NASA API in json format
    """
    try:
        url = url_map.get(api_name) + key_api
        response = requests.request("GET", url, data={}, cert=False)
        max_limit = int(response.headers["X-Ratelimit-Limit"])
        remaining_limit = int(response.headers["X-Ratelimit-Remaining"])
        return response.json()
    except requests.exceptions.HTTPError as e:
        if remaining_limit <= 0:
            raise Exception("Exceeding request limit for 1 hour (limit " + str(max_limit) + ")")
        elif response.status_code == 413:
            raise Exception("Request entity is larger than limits defined by server")
        else:
            raise Exception("Bad Request")


if __name__ == "__main__":
    data = fetch_api()
    producer = KafkaProducer(
        bootstrap_servers=kafka_ip,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    producer.send(topic_name, data)
    producer.flush()
    producer.close()
    print("Producer is successfully completed")
