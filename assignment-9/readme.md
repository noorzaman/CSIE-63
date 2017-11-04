cd /usr/local/Cellar/kafka/0.11.0.1/bin

zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties 
kafka-server-start /usr/local/etc/kafka/server.properties


netstat -vanp tcp | grep 2181
kill -9 63813

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic p3-topic-1

python2.7 kafka-orders-consumer.py localhost:2821 p3-topic-1
python2.7 kafka-orders-producer.py localhost 9092 p3-topic-1