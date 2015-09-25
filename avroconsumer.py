from kafka import KafkaConsumer
from kafka.common import ConsumerTimeout
import avro.schema
import avro.io
import io
import requests
import json
import sys

topic = 'simple'
 
# To consume messages
consumer = KafkaConsumer(topic,
                         group_id='test_group',
                         consumer_timeout_ms=1000,
                         auto_commit_enable=True,
                         auto_commit_interval_ms=30 * 1000,
                         auto_offset_reset='smallest',
                         bootstrap_servers=['slc08use.us.oracle.com:9092'])
schema = avro.schema.parse('''
{"type":"record","name":"test8","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}
''')
print schema

while True:
  try:
    for msg in consumer:
      print msg
      #bytes_reader = io.BytesIO(msg.value)
      #decoder = avro.io.BinaryDecoder(bytes_reader)
      #reader = avro.io.DatumReader(schema)
      #print("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,
       #                                  msg.offset, msg.key,
        #                                 reader.read(decoder)))
      #consumer.commit()

  except ConsumerTimeout:
    print 'nothing...'
