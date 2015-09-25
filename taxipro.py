from kafka import SimpleProducer, KafkaClient
import sys
import csv
import json
import time
import datetime
import re

# To send messages synchronously
#kafka = KafkaClient('slc08use.us.oracle.com:9092')
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

topic = 'test0730'
table = 'taxi_200K'

lines = [x.strip() for x in open(table + '.csv').readlines()]

print 'create external table', table, '('
columns =  lines[0].split('|')
print ',\n'.join(['  ' + col + ' string' for col in columns])
print ')'
print 'ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"'
print "location '/user/jiezhen/camus/topics/test0730/daily/2015/08/04';"
for line in lines[1:]:
  if line:
    try:
      producer.send_messages(bytes(topic), line)
    except LeaderNotAvailableError:
      print "pausing to allow Kafka time to create topic"
      time.sleep(1)
      producer.send_messages(bytes(topic), line)

