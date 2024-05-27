# Cryptodata Processing

End-to-end big data pipeline that deals with cryptocurrency data with batch and real-time stream processing fetching data from [Coinranking API](https://developers.coinranking.com/api) v2.

## Architecture

![Architecture](/img/projet_bigdata_202232024.png)


- **Batch Processing:** Process cryptocurrency data in batches to calculate various metrics.
- **Stream Processing:** Stream cryptocurrency data in real-time to perform analysis and monitoring.
- **Data Sources:** Utilize data from various sources such as CSV files and Kafka topics.
- **Containerized:** Docker-compose files provided for easy setup and deployment.

## Functionalities

- **Stream Processing:**
  - Windows aggregation: Calculate mean price value.
  - Calculate missing field like total_supply.
  - Calculate percentage: Volume and market_cap market share percentage.
  - Price tracking.

- **Batch Processing:**
  - Data aggregation by month and by year with the calculation of other fields like highest_high, lowest_low, first_open, last_close.
  - Market_cap ranking top10.
  - Crypto data summary for all time (min max price).

## visualization
![Dashboard](img/img1.png)

![price](img/img2.png)

![marketshare](img/img4.png)
## Usage

**1. Run Docker Compose:**

   ```bash
   docker-compose up -d
   ```
**2. Build JARs and move them to the container:**

 - **Cassandra Tables:**
   ```bash
   docker cp ./cassandraTables.cql cassandra:/root/
   ```
 - **Producer:**
   ```bash
   cd ./crypto_stream/stream-kafka-producer/
   mvn clean compile assembly:single
   docker cp ./target/stream-kafka-producer-1.0-SNAPSHOT-jar-with-dependencies.jar spark-master:/root/producer2.jar
   ```
 - **Consumer:**
   ```bash
   cd ./crypto_stream/stream-spark-consumer/
   mvn clean compile assembly:single
   docker cp ./target/stream-spark-consumer-1-jar-with-dependencies.jar spark-consumer-worker:/root/consumer2.jar
   ```
**3. start stream:**

   ```bash
   bash start-services.sh
   ```
