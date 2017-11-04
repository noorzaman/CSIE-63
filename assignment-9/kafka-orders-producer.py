from kafka import KafkaProducer
from itertools import islice
import time
import sys

infile_path = "/Users/swaite/Stirling/CSIE-63/assignment-9/data/orders.txt"
chunk_size = 1000
producer = KafkaProducer(bootstrap_servers=str(sys.argv[1]))
topic = str(sys.argv[2])
# topic = "p3-topic-1"
# producer = KafkaProducer(bootstrap_servers="localhost:9092")

order_count = 0
with open(infile_path) as f:
    while True:
        chunks = list(islice(f, chunk_size))
        order_count += 1
        print("message_count: " + str(order_count) + "\n" + "chunk_size: " + str(len(chunks)))
        chunked_message = "".join(chunks)
        print(chunked_message)
        producer.send(topic, chunked_message)
        if not chunks:
            break
        time.sleep(1)

# python2.7 kafka-orders-producer.py localhost:9092 p3-topic-1