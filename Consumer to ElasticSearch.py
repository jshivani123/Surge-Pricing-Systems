from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time
import pygeohash as pgh

es = Elasticsearch(["http://localhost:9200"])

consumer = KafkaConsumer(
    'surge_pricing_demo_output',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

index = "rides_demo"

mapping = {
  "mappings": {
    "properties": {
      "geohash": {"type": "geo_point"},
      "surge": {"type": "float"},
      "ts": {"type": "date"}
    }
  }
}

if not es.indices.exists(index=index):
    es.indices.create(index=index, body=mapping)

esid = 0

for message in consumer:
    esid += 1
    msg = message.value

    if "surge_ride" not in msg or "geohash" not in msg or "window" not in msg:
        continue

    lat, lon = pgh.decode(msg["geohash"])

    doc = {
        "geohash": f"{lat},{lon}",
        "surge": msg["surge_ride"],
        "ts": msg["window"]["start"]
    }

    es.index(index=index, id=esid, body=doc)

    if esid % 100 == 0:
        print(f"Indexed {esid} documents")

