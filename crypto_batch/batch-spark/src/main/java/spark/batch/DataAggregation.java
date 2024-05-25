
    package spark.batch;
   // import java.sql.Date;
import java.text.SimpleDateFormat;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Locale;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
    /*
     * data aggregation by month and by year
     */
    public class DataAggregation implements Serializable{
        /**
         * 
         * @param args
         * 
         */
        public static void main(String[] args) {   
        // Preconditions.checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        
        String inpath1="../csv/coins/coin_BinanceCoin.csv";
        String inpath2="crypto_batch/csv/coins/coin_Cardano.csv";
        String inpath3="crypto_batch/csv/coins/coin_Ethereum.csv";
        String inpath="crypto_batch/csv/coins/coin_Polkadot.csv";
        String outpath="outputdataaggregation";
        new DataAggregation().run(inpath, outpath);
        
        }

        void run(String inputFilePath, String outputDir){
            DecimalFormat df = new DecimalFormat("#.############");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
            
            // Spark configuration
            String master = "local[*]"; 
            
            SparkSession spark = SparkSession
                .builder()
                .appName(DataAggregation.class.getName())
                .config("spark.cassandra.connection.host", "localhost")
                .config("spark.cassandra.connection.port","9042" )
                .master(master)//for local dev
                .getOrCreate();
                    // Define the schema for the CSV file
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

            
            cryptoDataDF.show();   

            Dataset<Row> dataByMonthDF = cryptoDataDF.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
                .withColumn("month", date_format(col("Date"), "yyyy-MM"))
                .groupBy("name", "month")
                .agg(
                    max("High").alias("highest_high"),
                    min("Low").alias("lowest_low"),
                    first("Open").alias("first_open"),
                    last("Close").alias("last_close"),
                    sum("Volume").alias("total_volume"),
                    sum("Marketcap").alias("total_marketcap")
                );
    
            Dataset<Row> dataByYearDF = cryptoDataDF.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
                .withColumn("year", date_format(col("Date"), "yyyy"))
                .groupBy("name", "year")
                .agg(
                    max("High").alias("highest_high"),
                    min("Low").alias("lowest_low"),
                    first("Open").alias("first_open"),
                    last("Close").alias("last_close"),
                    sum("Volume").alias("total_volume"),
                    sum("Marketcap").alias("total_marketcap")
                )
                /*.withColumn("TotalMarketcap", lit(df.format(col("TotalMarketcap"))))
                .withColumn("TotalVolume", lit(df.format(col("TotalVolume"))))
                .withColumn("LastClose", lit(df.format(col("LastClose"))))
                .withColumn("FirstOpen", lit(df.format(col("FirstOpen"))))
                .withColumn("LowestLow", lit(df.format(col("LowestLow"))))
                .withColumn("HighestHigh", lit(df.format(col("HighestHigh"))))*/;
                dataByYearDF.show(); 
                
                dataByMonthDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "crypto_batch")
                .option("table", "crypto_month_agg")
                .mode("append")
                .save();

                dataByYearDF.write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "crypto_batch")
                .option("table", "crypto_year_agg")
                .mode("append")
                .save();


       spark.close();
    }

    }