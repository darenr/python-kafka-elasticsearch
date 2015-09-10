from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
import avro.schema
from avro.io import DatumWriter
import io
import requests
import sys
import csv
import json
import time
import datetime
import re

# To send messages synchronously
kafka = KafkaClient('slc08use.us.oracle.com:9092')
producer = SimpleProducer(kafka)

topic = 'iss'

schema = {
    "type": "record", "name": "iss",
    "fields": [
      {"name": "latlon", "type": "string"},
      {"name": "timestamp", "type": "long"}
    ]
}

avro_schema = avro.schema.parse(json.dumps(schema))

# put the schema in the avro registry
print requests.put("http://slc08use.us.oracle.com:8080/ingest/v1/set_avro_schema.json/" + topic, data=json.dumps(schema))

for x in range(10):
  time.sleep(1)
  j = requests.get("http://api.open-notify.org/iss-now.json").json()

  m = { "timestamp": j['timestamp'],
        "latlon": str(j['iss_position']['latitude']) \
          + ','  \
          + str(j['iss_position']['longitude']) }

  print m
  writer = avro.io.DatumWriter(avro_schema)
  bytes_writer = io.BytesIO()
  encoder = avro.io.BinaryEncoder(bytes_writer)
    
  writer.write(m, encoder)
  raw_bytes = bytes_writer.getvalue()
  producer.send_messages(topic, raw_bytes)
