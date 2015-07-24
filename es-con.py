from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch
import json

es = Elasticsearch()

# To consume messages
consumer = KafkaConsumer('test',
                         bootstrap_servers=['localhost:9092'])
esid = 0

for message in consumer:
    esid += 1
    if esid % 1000 == 0:
      print esid

    msg = json.loads(message.value)

    if not 'index' in msg:
      print "you must specify the index name in the json wrapper"
      sys.exit(-1)

    index = msg['index']

    if 'schema' in msg:
      esid = 1
      print "Switching to Index", index
      if es.indices.exists(index=index):
        print es.indices.delete(index=index)
      print es.indices.create(index=index )
      print es.indices.put_mapping(index=index, doc_type= msg['doc_type'], body=msg['schema']['mappings'] )

    es.index(index=index, doc_type= msg['doc_type'], id=esid, body=msg['body'])
