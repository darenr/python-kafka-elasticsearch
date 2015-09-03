from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import requests
import json

topic = 'test'
 
# To consume messages
consumer = KafkaConsumer(topic,
                         group_id='avro_group',
                         bootstrap_servers=['localhost:9092'])
 
schema = avro.schema.parse(requests.get("http://localhost:8080/ingest/v1/get_avro_schema.json/" + topic).content)

print schema
 
for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    print  reader.read(decoder)
