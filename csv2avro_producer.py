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

if not len(sys.argv) is 3:
  print "usage:", sys.argv[0], "<csv file> <index>"
  sys.exit(-1)

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

topic = 'test'

with open(sys.argv[1]) as fp:
  for i, m in enumerate(csv.DictReader(fp, delimiter='|'), 1):

    doc_type = sys.argv[1].split('.')[0]
    index = sys.argv[2]

    # for the first line we do poor man's type discovery
    if i == 1:

      schema = {
        "type": "record",
        "name": doc_type,
        "fields": []
      }

      for k in m.keys():
        schema['fields'].append({"name": k, "type": "string"})

      print json.dumps(schema)

      # put the schema in the avro registry
      print requests.put("http://localhost:8080/ingest/v1/set_avro_schema.json/" + topic, data=json.dumps(schema))

    avro_schema = avro.schema.parse(json.dumps(schema))
    writer = avro.io.DatumWriter(avro_schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    
    writer.write(m, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send_messages(topic, raw_bytes)
