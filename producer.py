"""
This is a simple kafka producer in python
"""
import json
from time import sleep

from kafka import KafkaProducer

if __name__ == "__main__":
    ## write a kafka producer in python

    # create a kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        client_id="kitkat",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with open("messages.json", "r", encoding="utf-8") as f:
        # read the data from the file
        for data in f.readlines():
            # send a message to kafka
            producer.send("test", data)
            print(f"Sent: {data}")
            # flush the producer
            producer.flush()
            sleep(10)
    # close the producer
    producer.close()
