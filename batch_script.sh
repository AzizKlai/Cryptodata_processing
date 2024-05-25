# Access the Cassandra container and execute the CQL script
docker exec -dit spark-master bash  bash /spark/bin/spark-submit --master spark://spark-master:7077  --class spark.batch.CryptoSummary --master local /root/batch.jar coin.txt out-spark

docker exec -dit spark-master bash  bash /spark/bin/spark-submit --master spark://spark-master:7077  --class spark.batch.DataAggregation --master local /root/batch.jar coin.txt out-spark

docker exec -dit spark-master bash  bash /spark/bin/spark-submit --master spark://spark-master:7077  --class spark.batch.TopMarketCap --master local /root/batch.jar coin.txt out-spark

