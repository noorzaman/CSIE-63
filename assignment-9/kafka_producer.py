from kafka import KafkaProducer
import time
import sys

if len(sys.argv) != 4:
    print "Please pass 3 arguments to script ex: python2.7 kafka_producer.py localhost 9092 topic1"
    exit(1)

producer = KafkaProducer(bootstrap_servers=str(sys.argv[1]) + ':' + str(sys.argv[2]))
kafka_topic = str(sys.argv[3])

inp = raw_input("\nPlease type a message or EXIT to exit script:\n")
while inp != "EXIT":
    inp = raw_input()
    producer.send(sys.argv[3], b"" + str(inp))