Run:  ./spark-submit --master local --class com.rr.SparkSqlTest  '/home/notroot/test_jars/sparkTest.jar' '/home/notroot/test_jars/data/'

Multiple Kafka Broker:
1)By default the ZooKeeper server will listen on *:2181/tcp
cp config/server.properties config/server1.properties
cp config/server.properties config/server2.properties
2)first broker: Edit config/server1.properties and replace the existing config values as follows:
		broker.id=1
		port=9092
		log.dir=/tmp/kafka-logs-1
3) Second Broker :Edit config/server2.properties and replace the existing config values as follows:
		broker.id=2
		port=9093
		log.dir=/tmp/kafka-logs-2

 

## Running Kakfka for Spark streamig:
1) Go to kafka bin directory and run below commands in seperate putty session
2) ./zookeeper-server-start.sh ../config/zookeeper.properties
3) ./kafka-server-start.sh ../config/server.properties
4) ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic spark-topic

# List the available topics in the Kafka cluster
$ bin/kafka-topics.sh --zookeeper localhost:2181 --list
# Describe kafka topic details
$ bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic spark-topic

# Start a console producer in sync mode
$ bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --sync \
    --topic spark-topic
	
# Start a console consumer
$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic spark-topic --from-beginning

submitting spark stream job
5) ./spark-submit --jars  '/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/spark-streaming-kafka_2.10-1.6.2.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/kafka-clients-0.8.2.1.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/kafka_2.10-0.8.2.1.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/metrics-core-2.2.0.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/zkclient-0.3.jar'  --class com.rr.KafkaStream  '/home/notroot/test_jars/sparkDemo.jar'

File data producer to kafka topic
6) java -cp sparkDemo.jar:/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/kafka-clients-0.8.2.1.jar:/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/rrlib/slf4j-api-1.7.2.jar com.rr.KafkaFileProducer

## Running Spark TCP streaming
first run server linux command -->  nc -lk 8585 
./spark-submit --master local  --jars  '/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/spark-streaming-kafka_2.10-1.6.2.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/kafka-clients-0.8.2.1.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/kafka_2.10-0.8.2.1.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/metrics-core-2.2.0.jar,/home/notroot/lab/software/spark-1.6.0-bin-hadoop2.6/lib/zkclient-0.3.jar'  --class com.rr.SparkTcpStream  '/home/notroot/test_jars/sparkTest.jar'

