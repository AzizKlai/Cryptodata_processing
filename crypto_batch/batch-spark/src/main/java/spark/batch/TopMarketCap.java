
    package spark.batch;
    import static org.apache.spark.sql.functions.col;
    import static org.apache.spark.sql.functions.rank;
    
    import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.*;
    //top marketcap of all time
    public class TopMarketCap {
        public static void main(String[] args) {
            // Create Spark configuration

        String inputFilePath="../csv/coins/coin_BinanceCoin.csv";
         // Spark configuration
        String master = "local[*]"; 
        
        SparkSession spark = SparkSession
            .builder()
            .appName(TopMarketCap.class.getName())
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port","9042" )
            .master(master)//for local dev
            .getOrCreate();   
        StructType schema = new StructType()
                .add("SNo", DataTypes.IntegerType)
                .add("Name",  DataTypes.StringType)
                .add("Symbol",  DataTypes.StringType)
                .add("Date",  DataTypes.DateType)
                .add("High",  DataTypes.DoubleType)
                .add("Low",  DataTypes.DoubleType)
                .add("Open",  DataTypes.DoubleType)
                .add("Close",  DataTypes.DoubleType)
                .add("Volume",  DataTypes.DoubleType)
                .add("Marketcap",  DataTypes.DoubleType);

            // Load CSV dataset as a DataFrame
        Dataset<Row> cryptoDataDF = spark.read()
                                       .schema(schema)
                                       .option("header", "false")
                                       .csv(inputFilePath)
                                       .filter((Row row) -> !row.anyNull())
                                       .withColumnRenamed("Name", "name"); 

        Dataset<Row> top10MarketCap = cryptoDataDF.withColumn("rank", rank().over(Window.orderBy(col("Marketcap").desc())))
                                           .filter(col("rank").leq(10))
                                           .drop("rank")
                                           .withColumnRenamed("Marketcap", "market_cap")
                                           .withColumnRenamed("Name", "name_coin")
                                           .withColumnRenamed("Symbol", "symbol_coin")
                                           .withColumnRenamed("Date", "date")
                                           .withColumnRenamed("High", "high")
                                           .withColumnRenamed("Low", "low")
                                           .withColumnRenamed("Open", "open")
                                           .withColumnRenamed("Close", "close")
                                           .withColumnRenamed("Volume", "volume");
                                           ;
        top10MarketCap.show();   
        
        top10MarketCap.write()
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "crypto_batch")
        .option("table", "crypto_year_agg")
        .mode("append")
        .save();

spark.close();
        }
    }
    