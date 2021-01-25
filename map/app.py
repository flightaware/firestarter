"""Forward flight events via SSE"""

import os
import random

from confluent_kafka import KafkaException, Consumer  # type: ignore
from flask import Flask, Response, render_template, request


app = Flask(__name__, template_folder="frontend", static_folder="frontend/static")

@app.route("/")
def index():
    return render_template(
            "index.html",
            google_maps_api_key=os.environ.get("GOOGLE_MAPS_API_KEY", ""),
            startlive='live' in request.args)

@app.route("/listen")
def listen():
    group = request.headers.get("Last-Event-ID", f"{os.environ['KAFKA_GROUP_NAME']}{random.randint(0, 10**9)}")
    live = 'live' in request.args
    def stream():
        innergroup = group
        consumer = None
        while True:
            try:
                # Handle case where we initialized the consumer but failed to
                # subscribe. Don't want to keep initializing.
                if consumer is None:
                    consumer = Consumer(
                        {
                            "bootstrap.servers": "kafka:9092",
                            "group.id": innergroup,
                            "auto.offset.reset": "latest" if live else "earliest",
                            "enable.auto.commit": True,
                            "auto.commit.interval.ms": 1000,
                        }
                    )
                consumer.subscribe([os.environ["KAFKA_TOPIC_NAME"]])
                break
            except (KafkaException, OSError) as error:
                # Consider providing some sort of status update to listener
                time.sleep(3)
        while True:
            # Polling will mask SIGINT, just fyi
            messagestr = consumer.poll(timeout=1.0)
            if messagestr is None:
                # poll timed out
                continue
            if messagestr.error():
                # Consider providing to listener
                print(f"Encountered kafka error: {messagestr.error()}")
                # They continue in the examples, so let's do it as well
                continue
            yield as_sse(messagestr.value().decode(), id=innergroup)
            # Only send the id for the first message. We're kind of hijacking
            # the ID's purpose since it's really supposed to be unique per
            # message. We just need one ID for handling the group membership,
            # though.
            innergroup = None
    return Response(stream(), mimetype="text/event-stream")


def as_sse(data, event=None, id=None):
    message = f"data: {data}"
    if event is not None:
        message = f"event: {event}\n{message}"
    if id is not None:
        message = f"{message}\nid: {id}"
    message = message + "\n\n"
    return message

app.run(host="0.0.0.0", port=5001, debug=True)
