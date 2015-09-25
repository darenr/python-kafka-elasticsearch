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
import time
import re

# To send messages synchronously
kafka = KafkaClient('slc08use.us.oracle.com:9092')
producer = SimpleProducer(kafka)

time_offset = 1443217773*1000 # 9/25/15 

topic = 'iss'

for x in range(100):
  time.sleep(1)
  j = requests.get("http://api.open-notify.org/iss-now.json").json()

  m = { "id": int(time.time()*1000) - time_offset,
        "timestamp": j['timestamp'],
        "latlon": str(j['iss_position']['latitude']) \
          + ','  \
          + str(j['iss_position']['longitude']) }

  print json.dumps(m)
  producer.send_messages(topic, json.dumps(m))
