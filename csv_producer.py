from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError
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
#kafka = KafkaClient('mkressirer.us.oracle.com:32770')
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

topic = 'test'

with open(sys.argv[1]) as fp:
  for i, m in enumerate(csv.DictReader(fp, delimiter='|'), 1):
    # hack to turn lat/lon into geo
    m['timestamp'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S')

    doc_type = sys.argv[1].split('.')[0]
    index = sys.argv[2]

    wrapper = {
      'doc_type': doc_type,
      'index': index,
      'body': m,
    }

    # for the first line we do poor man's type discovery
    if i == 1:
      # add schema info to recreate the index and apply mappings
      schema = {
        'mappings': { 
          doc_type: {
            "properties" : {
            }
          } 
        }
      }

      properties = schema['mappings'][doc_type]['properties']
      for k in m.keys():
        print "type discovering: ", k, m[k]
        # look for lat/lon
        if not m[k]:
          properties[k] = {"type": "string", "store": "yes", "enabled": "true"}
        elif re.match(r'^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$', m[k].strip()):
          print "detected", k, "is a geo_point type"
          properties[k] = {"type": "geo_point", "store": "yes", "enabled": "true"}
        else:
          try:
            x = float(m[k])
            properties[k] = {"type": "float", "store": "yes", "enabled": "true"}
          except ValueError:
            if m[k].isdigit():
              properties[k] = {"type": "integer", "store": "yes", "enabled": "true"}
            elif k == 'timestamp':
              properties[k] = {"type": "date", "enabled" : "true", "store" : "yes", "format": "dateOptionalTime"}
            else:
              properties[k] = {"type": "string", "store": "yes", "enabled": "true"}

      print json.dumps(schema, sort_keys=True, indent=2, separators=(',', ': '))
      wrapper['schema'] = schema

    try:
      producer.send_messages(bytes(topic), json.dumps(wrapper))
    except LeaderNotAvailableError:
      print "pausing to allow Kafka time to create topic"
      time.sleep(1)
      producer.send_messages(bytes(topic), json.dumps(wrapper))

