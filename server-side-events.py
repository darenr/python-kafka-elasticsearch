# Make sure your gevent version is >= 1.0
import gevent
from gevent.wsgi import WSGIServer
from gevent.queue import Queue

from flask import Flask, Response
import time
from kafka import KafkaConsumer
import json



class ServerSentEvent(object):
    def __init__(self, data):
        self.data = data
        self.event = None
        self.id = None
        self.desc_map = {
            self.data : "data",
            self.event : "event",
            self.id : "id"
        }

    def encode(self):
        if not self.data:
            return ""
        lines = ["%s: %s" % (v, k) 
                 for k, v in self.desc_map.iteritems() if k]
        
        return "%s\n\n" % "\n".join(lines)

app = Flask(__name__)
subscriptions = []
consumer = KafkaConsumer('test', group_id="es_group",
                          auto_commit_enable=True,
                          auto_commit_interval_ms=30 * 1000,
                          auto_offset_reset='smallest',
                          consumer_timeout_ms=1000,
                          bootstrap_servers=['localhost:9092'])

@app.route("/")
def index():
    template = """
     <html>
       <head>
       </head>
       <body>
         <h1>Server sent events:</h1>
         <ul id="ul">
         <script type="text/javascript">

        
         var evtSrc = new EventSource("/subscribe");

         evtSrc.onmessage = function(e) {
            ul.insertBefore(document.createTextNode(e.data), ul.childNodes[0]);
         };

         </script>
       </body>
     </html>
    """
    return(template)

@app.route("/subscribe")
def subscribe():
    def gen():
        q = Queue()
        subscriptions.append(q)
        try:
            while True:
                result = q.get()
                ev = ServerSentEvent(str(result))
                yield ev.encode()
        except GeneratorExit: # Or maybe use flask signals
            subscriptions.remove(q)

    return Response(gen(), mimetype="text/event-stream")

def kafka():
  gevent.sleep(0.5)
  for message in consumer:
    for sub in subscriptions[:]:
      sub.put(message)
    gevent.sleep(0.1)

gevent.spawn(kafka)

if __name__ == "__main__":
    app.debug = True
    server = WSGIServer(("", 5000), app)
    server.serve_forever()
    # Then visit http://localhost:5000 to subscribe 
    # and send messages by visiting http://localhost:5000/publish
