# Access the Cassandra container and execute the CQL script
docker exec -dit cassandra cqlsh -f /root/cassandraTables.sh  

# Access the Spark consumer worker and run the Spark job
docker exec -dit spark-consumer-worker bash /spark/bin/spark-submit --master spark://spark-master:7077 --class spark.consumer.SparkStreamConsumer --deploy-mode client /root/consumer2.jar

# Access the Spark master
docker exec -dit spark-master bash java -jar /root/producer2.jar
