package spark.consumer;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.math.BigDecimal;

import javax.xml.crypto.Data;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;



public class SparkStreamConsumer {

    
    private static final String TOPIC_NAME = "crypto";
    private static final String GROUP_ID = "myconsumergroup";
    private static final String BOOTSTRAP_SERVER= "kafka:9092";//"localhost:9092";
    //private static final Integer TIME_STAMP = 10000;

    private static final String WATERMARK_THRESHOLD = "300 seconds"; // Maximum allowed time after event time before a record is considered late
    private static final String WINDOW_DURATION = "300 seconds"; // Duration of the window for aggregations
    private static final String SLIDE_DURATION = "30 seconds"; // Duration for updating the window and triggering computations
    public static void main(String[] args) throws Exception {
        System.out.println("starting consumer");
  

        String master = "local[4]";
        
        SparkSession spark = SparkSession
                .builder()
                .appName(SparkStreamConsumer.class.getName())
                .config("spark.cassandra.connection.host", "cassandra")//"localhost")
                .config("spark.cassandra.connection.port","9042" )
                .master(master)//for local dev
                .getOrCreate();
                
        // Create input DataFrame representing the stream of input lines from Kafka
        Dataset<Row> inputDF = spark
                        .readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                        .option("subscribe", TOPIC_NAME)
                       // .option("kafka.group.id", GROUP_ID)
                        .load();
                
        // Parse the data into schema
        Dataset<Row> parsedInput = inputDF.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).as("data"))
        .select("data.*");
        
        
        

                
        //percent calculation       
        // Calculate percent_volume
                
        Dataset<Row> calculatedData = parsedInput.withColumn("percent_volume", expr("100 * volume / total_24_volume"));

        // Calculate percent_market_cap
                
        calculatedData = calculatedData.withColumn("percent_market_cap", expr("100 * market_cap / total_market_cap"));
                        
        // Calculate percent_change
        calculatedData = calculatedData.withColumn("percent_change", expr("100 * (price - price_old) / price_old"));

        // Calculate percent_change_24hr

        //circulating supply calculation
        calculatedData = calculatedData.withColumn("cir_supply", expr("market_cap / price"));


        //calculate total supply
        Dataset<Row> droped = calculatedData.drop("id", "price_old", "percent_change_24hr", "total_24_volume", "total_market_cap");
              
              
        StreamingQuery query = droped
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                        batchDF.write()
                        .format("org.apache.spark.sql.cassandra")
                        .option("keyspace", "crypto")
                        .option("table", "crypto_realtime_prices")
                        .mode("append")
                        .save();
                })
                .outputMode("update")
                .start();


                //windows aggregation: Calculate rolling means and standard deviation for each coin
                Dataset<Row> windowedDF = parsedInput
                        .withWatermark("time_stamp", WATERMARK_THRESHOLD)
                        .groupBy(
                                window(col("time_stamp"), WINDOW_DURATION, SLIDE_DURATION),
                                col("symbol_coin"))
                        .agg(
                                avg(col("price")).as("arithmetic_mean"),   // calculate arithmetic mean
                                expr("exp(avg(log(price)))").as("geometric_mean"), // Calculate geometric mean
                               // expr("1 / avg(1 / price)").as("harmonic_mean"), // Calculate harmonic mean
                                stddev(col("price")).as("standard_deviation")) // Calculate standard deviation
                        .withColumn("start_time", col("window").getField("start"))
                        .withColumn("end_time", col("window").getField("end"))
                        .drop("window");

                        StreamingQuery queryAggregate = windowedDF
                        .writeStream()
                        .foreachBatch((batchDF, batchId) -> {
                                batchDF.write()
                                .format("org.apache.spark.sql.cassandra")
                                .option("keyspace", "crypto")
                                .option("table", "crypto_rolling_means")
                                .mode("append")
                                .save();
                        })
                        .outputMode("update")
                        .start();

        
/*
 * // Assume 'spark' is your SparkSession and 'tableName' is the name of the table you want to purge
val tableName = "your_table_name"

// Get the CassandraConnector
val connector = CassandraConnector(spark.sparkContext.getConf)

// Truncate the table
connector.withSessionDo(session => session.execute(s"TRUNCATE $tableName"))
 */

  /// save biggest winners and biggest losers
  // Assuming biggestWinners and biggestLosers are Datasets
//WindowSpec windowSpec = Window.orderBy(col("percent_change").desc());

Dataset<Row> biggestdf = calculatedData.groupBy("name_coin")
        .agg(max("price").as("price"),
                max("percent_change").alias("percent_change"),
                max("time_stamp").as("time_stamp"))     
                .select("name_coin","time_stamp","price", "percent_change");

Dataset<Row> biggestWinners = biggestdf.orderBy(col("percent_change").desc()).limit(10);

Dataset<Row> biggestLosers = biggestdf.orderBy(col("percent_change").asc()).limit(10);


    
        
    // Save the top 10 biggest winners and losers to the database
    StreamingQuery queryBiggestWinners = biggestWinners
    .writeStream()
    .foreachBatch((batchDF, batchId) -> {
    batchDF.write()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "crypto")
            .option("table", "biggest_winners")
            .mode("append")
            .save();
    })
    .outputMode("complete")
    .start();

    // Save the top 10 biggest winners and losers to the database
    StreamingQuery queryBiggestLosers = biggestLosers
    .writeStream()
    .foreachBatch((batchDF, batchId) -> {
    batchDF.write()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "crypto")
            .option("table", "biggest_losers")
            .mode("append")
            .save();
    })
    .outputMode("complete")
    .start();


// market share

Dataset<Row> marketSharedf = calculatedData .groupBy("name_coin")
.agg(   max("percent_volume").as("percent_volume"),       
        max("percent_market_cap").alias("percent_market_cap"),
        max("time_stamp").as("time_stamp"))     
        .select("name_coin","time_stamp","percent_volume", "percent_market_cap");

Dataset<Row> marketShare=marketSharedf.orderBy(col("percent_market_cap").desc()).limit(10);
    // Save to the database
    StreamingQuery queryMarketShare = marketShare
    .writeStream()
    .foreachBatch((batchDF, batchId) -> {
    batchDF.write()
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", "crypto")
            .option("table", "market_share")
            .mode("append")
            .save();
    })
    .outputMode("complete")
    .start();


// Wait for all queries to terminate
query.awaitTermination();


queryAggregate.awaitTermination();
queryBiggestWinners.awaitTermination();
queryBiggestLosers.awaitTermination();
queryMarketShare.awaitTermination();
    }



    
        static StructType schema = new StructType()
        .add("name_coin", DataTypes.StringType)
        .add("symbol_coin", DataTypes.StringType)
        .add("id", DataTypes.StringType)

        .add("volume", DataTypes.createDecimalType(38, 18)) // 38 digits with up to 18 decimal places
        .add("market_cap", DataTypes.createDecimalType(38, 18))

        .add("price", DataTypes.createDecimalType(38, 18))
        .add("price_old", DataTypes.createDecimalType(38, 18))
        .add("percent_change_24hr", DataTypes.DoubleType)

        .add("total_24_volume", DataTypes.createDecimalType(38, 18))
        .add("total_market_cap", DataTypes.createDecimalType(38, 18))
        
        .add("time_stamp", DataTypes.TimestampType);
        



}

                              /* 
                              to show the data
                              StreamingQuery queryshow3 = parsedInput
                                .writeStream()
                                .outputMode("append")
                                .format("console")
                                .start();*/
