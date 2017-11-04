from kafka import KafkaProducer
from itertools import islice
import time
import sys

infile_path = "/Users/swaite/Stirling/CSIE-63/assignment-9/data/orders.txt"
chunk_size = 1000
topic = "p3-new-topic"
producer = KafkaProducer(bootstrap_servers="localhost:9092")

message_count = 0
with open(infile_path) as f:
    while True:
        chunks = list(islice(f, chunk_size))
        message_count += 1
        print("message_count: "+ str(message_count) + "\n" + "chunk_size: " + str(len(chunks)))
        chunked_message = "".join(chunks)
        producer.send(topic, chunked_message)
        if not chunks:
            break
        time.sleep(1)


# python2.7 kafka-orders-producer.py localhost:9092 p3-new-topic