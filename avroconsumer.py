from kafka import KafkaConsumer
import avro.schema
import avro.io
import io

topic = 'test'
 
# To consume messages
consumer = KafkaConsumer(topic,
                         group_id='avro_group',
                         bootstrap_servers=['localhost:9092'])
 
schema = avro.schema.parse('{"fields": [{"type": "string", "name": "cdatetime"}, {"type": "string", "name": "district"}, {"type": "string", "name": "crimedescr"}, {"type": "string", "name": "beat"}, {"type": "string", "name": "grid"}, {"type": "string", "name": "address"}, {"type": "string", "name": "ucr_ncic_code"}, {"type": "string", "name": "geo"}], "type": "record", "name": "SacramentocrimeJanuary2006"}')

 
for msg in consumer:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    print  reader.read(decoder)
