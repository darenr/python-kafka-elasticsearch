from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import requests
import json
import sys

topic = 'test'
 
# To consume messages
consumer = KafkaConsumer(topic,
                         group_id='avro_group',
                         bootstrap_servers=['localhost:9092'])
 
schema = None
for msg in consumer:
    if not schema:
      r = requests.get("http://localhost:8080/ingest/v1/get_avro_schema.json/" + topic)
      if not r.status_code == 200:
        print "no schema"
        sys.exit(-1)

      schema = avro.schema.parse(r.json()['schema'])
      print schema
 
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    print  reader.read(decoder)
