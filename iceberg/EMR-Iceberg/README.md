# Quick Start


spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.1   \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions    \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog    \
--conf spark.sql.catalog.spark_catalog.type=hive    \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog   \
--conf spark.sql.catalog.local.type=hadoop   \
--conf spark.sql.catalog.local.warehouse=s3://akshaya-firehose-test/iceberg

val data = spark.range(0, 5)
data.writeTo("local.default.first_iceberg").create()


# Spark Streaming

## Spark Submit
spark-submit \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions    \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog    \
--conf spark.sql.catalog.spark_catalog.type=hive    \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog   \
--conf spark.sql.catalog.local.type=hadoop   \
--conf spark.sql.catalog.local.warehouse=s3://akshaya-firehose-test/iceberg \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.hive.convertMetastoreParquet=false \
--packages org.apache.iceberg:iceberg-spark3-runtime:0.12.1,org.apache.iceberg:iceberg-spark3-extensions:0.12.1,org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.1,com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0 \
--class kinesis.iceberg.latefile.SparkKinesisConsumerIcebergProcessor spark-structured-streaming-kinesis-hudi_2.12-1.0.jar \
aksh-firehose-test hudi-stream-ingest ap-south-1 iceberg_trade_event_late_simulation


## Spark Shell
com.qubole.spark:spark-sql-kinesis_2.12':1.2.0_spark-3.0


spark-shell --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.1,org.apache.iceberg:iceberg-spark3-extensions:0.12.1,org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.1,com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0    \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions    \
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog    \
--conf spark.sql.catalog.spark_catalog.type=hive    \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog   \
--conf spark.sql.catalog.local.type=hadoop   \
--conf spark.sql.catalog.local.warehouse=s3://akshaya-firehose-test/iceberg \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.hive.convertMetastoreParquet=false 

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions
import java.util.Date
import java.text.SimpleDateFormat
import spark.implicits._
import spark.sql
import org.apache.spark.sql.streaming.Trigger
import java.util.concurrent.TimeUnit

val s3_bucket="akshaya-firehose-test"
val streamName="hudi-stream-ingest"//
val region="ap-south-1"
val tableName="iceberg_trade_info"
val endpointUrl=s"https://kinesis.$region.amazonaws.com"
val hudiTablePartitionKey = "partition_key"

val streamingInputDF = (spark
                          .readStream .format("kinesis") 
                          .option("streamName", streamName) 
                          .option("startingposition", "TRIM_HORIZON")
                          .option("endpointUrl", endpointUrl)
                          .load())

val decimalType = DataTypes.createDecimalType(38, 10)
val dataSchema=StructType(Array(
        StructField("tradeId",StringType,true),
        StructField("symbol",StringType,true),
        StructField("quantity",StringType,true),
        StructField("price",StringType,true),
        StructField("timestamp",StringType,true),
        StructField("description",StringType,true),
        StructField("traderName",StringType,true),
        StructField("traderFirm",StringType,true)
      ))
var jsonDF=(streamingInputDF.selectExpr("CAST(data AS STRING)").as[(String)]
                .withColumn("jsonData",from_json(col("data"),dataSchema))
                .select(col("jsonData.*")))

val hudiTableRecordKey = "record_key"

jsonDF.printSchema()

vat creatTableString= """CREATE TABLE IF NOT EXISTS local.default.iceberg_trade_info_late 
        (  tradeid string,  
            symbol string, 
            quantity string, 
            price string, 
            timestamp string, 
            description string,    
            tradername string, 
            traderfirm String, 
            record_key string , 
            trade_datetime string, 
            day string, 
            hour string,    
            partition_key string 
        )    
        USING iceberg    
        PARTITIONED BY (day,hour)"""

val table=sql(creatTableString)
table.printSchema()

val checkpoint_path=s"s3://$s3_bucket/kinesis-stream-data-checkpoint/iceberg_trade_info_new/"

jsonDF=jsonDF.select(jsonDF.columns.map(x => col(x).as(x.toLowerCase)): _*)
jsonDF=jsonDF.filter(jsonDF.col("tradeid").isNotNull) 
jsonDF = jsonDF.withColumn(hudiTableRecordKey, concat(col("tradeid"), lit("#"), col("timestamp")))
jsonDF = jsonDF.withColumn("trade_datetime", from_unixtime(jsonDF.col("timestamp")))
jsonDF = jsonDF.withColumn("day",dayofmonth($"trade_datetime").cast(StringType)).withColumn("hour",hour($"trade_datetime").cast(StringType))
jsonDF = jsonDF.withColumn(hudiTablePartitionKey,concat(lit("day="),$"day",lit("/hour="),$"hour"))
jsonDF.printSchema()


(jsonDF.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
    .option("path", "local.default.iceberg_trade_info_late")
    .option("fanout-enabled", "true")
    .option("checkpointLocation", checkpoint_path)
    .start())

