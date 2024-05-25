
    package spark.batch;
    import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
    //top marketcap of all time
    //take a csv with price field price marketCap
    public class CryptoSummary {
        public static void main(String[] args) {
            // Create Spark configuration

        String inputFilePath="../csv/currencies_data.csv";
         // Spark configuration
        String master = "local[*]"; 
        
        SparkSession spark = SparkSession
            .builder()
            .appName(CryptoSummary.class.getName())
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port","9042" )
            .master(master)//for local dev
            .getOrCreate();   

            // Load CSV dataset as a DataFrame
        Dataset<Row> cryptoDataDF = spark.read()
                                       .option("header", true)
                                       .option("inferSchema", true)
                                       .csv(inputFilePath)
                                       .select("name1", "price", "marketCap")
                                       .filter((Row row) -> !row.anyNull());
        cryptoDataDF.show();
                                     
        Dataset<Row> summary = cryptoDataDF.groupBy("name1")
        .agg(
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
                max("marketCap").alias("max_market_cap")
        ).withColumnRenamed("name1","name");

        summary.show();
        summary.write()
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "crypto_batch")
        .option("table", "crypto_summary")
        .mode("append")
        .save();
        /* Dataset<Row> formattedSummary = summary.withColumn("AveragePriceFormatted",
                functions.format_number(summary.col("AveragePrice"), 2));

        formattedSummary.show();*/
    

        spark.close();
        
      }
    }
    