from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.window import Window
import time

TH1_topic = 'TH1'
TH2_topic = 'TH2'
HVAC1_topic = 'HVAC1'
HVAC2_topic = 'HVAC2'
MiAC1_topic = 'MiAC1'
MiAC2_topic = 'MiAC2'
Mov1_topic = 'Mov1'
W1_topic = 'W1'
Etot_topic = 'Etot'
Wtot_topic = 'Wtot'

binary_to_string = udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())
jsonFormatSchema = open("path_to/avro_sch.avsc", "r").read()

kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession.builder.appName("Sensor data streaming").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

topics = [TH1_topic, TH2_topic, HVAC1_topic, HVAC2_topic, MiAC1_topic, MiAC2_topic, Mov1_topic, W1_topic, Etot_topic, Wtot_topic]
sensor_names = ['TH1_sensor', 'TH2_sensor', 'HVAC1_sensor', 'HVAC2_sensor', 'MiAC1_sensor', 'MiAC2_sensor', 'Mov1_sensor', 'W1_sensor', 'Etot_sensor', 'Wtot_sensor']

dfs = []

for i in range(len(topics)):
    dfs.append(
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topics[i])
    .load()
    .withColumn('key', col("key").cast(StringType()))
    .withColumn('fixedValue', (expr("substring(value, 6, length(value)-5)")))
    .withColumn('valueSchemaId', binary_to_string(expr("substring(value, 2, 4)")))
    .select('key', 'valueSchemaId','fixedValue')
    )

    dfs[i] = dfs[i].select(from_avro("fixedValue", jsonFormatSchema).alias(sensor_names[i]))
    dfs[i] = dfs[i].select(f"{sensor_names[i]}.*")

TH1_df = dfs[0]
TH2_df = dfs[1]
HVAC1_df = dfs[2]
HVAC2_df = dfs[3]
MiAC1_df = dfs[4]
MiAC2_df = dfs[5]
Mov1_df = dfs[6]
W1_df = dfs[7]
Etot_df = dfs[8]
Wtot_df = dfs[9]


# TH1 Avg Aggregation
TH1_df = TH1_df.withColumn('timestamp', to_timestamp(TH1_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_TH1_df = TH1_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(avg("value").alias('value'))
tumbling_TH1_df = tumbling_TH1_df.select('name', tumbling_TH1_df.window.end.cast("string").alias("timestamp"), "value")

# TH2 Avg Aggregation
TH2_df = TH2_df.withColumn('timestamp', to_timestamp(TH2_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_TH2_df = TH2_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(avg("value").alias('value'))
tumbling_TH2_df = tumbling_TH2_df.select('name', tumbling_TH2_df.window.end.cast("string").alias("timestamp"), "value")

# HVAC1 Sum Aggregation
HVAC1_df = HVAC1_df.withColumn('timestamp', to_timestamp(HVAC1_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_HVAC1_df = HVAC1_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(sum("value").alias('value'))
tumbling_HVAC1_df = tumbling_HVAC1_df.select('name', tumbling_HVAC1_df.window.end.cast("string").alias("timestamp"), "value")

# HVAC2 Sum Aggregation
HVAC2_df = HVAC2_df.withColumn('timestamp', to_timestamp(HVAC2_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_HVAC2_df = HVAC2_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(sum("value").alias('value'))
tumbling_HVAC2_df = tumbling_HVAC2_df.select('name', tumbling_HVAC2_df.window.end.cast("string").alias("timestamp"), "value")

# MiAC1 Sum Aggregation
MiAC1_df = MiAC1_df.withColumn('timestamp', to_timestamp(MiAC1_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_MiAC1_df = MiAC1_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(sum("value").alias('value'))
tumbling_MiAC1_df = tumbling_MiAC1_df.select('name', tumbling_MiAC1_df.window.end.cast("string").alias("timestamp"), "value")

# MiAC2 Sum Aggregation
MiAC2_df = MiAC2_df.withColumn('timestamp', to_timestamp(MiAC2_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_MiAC2_df = MiAC2_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(sum("value").alias('value'))
tumbling_MiAC2_df = tumbling_MiAC2_df.select('name', tumbling_MiAC2_df.window.end.cast("string").alias("timestamp"), "value")

# Mov1 Sum Aggregation
Mov1_df = Mov1_df.withColumn('timestamp', to_timestamp(Mov1_df.timestamp, 'yyyy-MM-dd HH:mm:ss'))
tumbling_Mov1_df = Mov1_df.withWatermark('timestamp', '1 second').groupBy(window('timestamp', '1 day', startTime='22 hours'), 'name').agg(sum("value").alias('value'))
tumbling_Mov1_df = tumbling_Mov1_df.select('name', tumbling_Mov1_df.window.end.cast("string").alias("timestamp"), "value")


TH1_stream = tumbling_TH1_df.select(to_avro(struct([col(c).alias(c) for c in tumbling_TH1_df.columns]), jsonFormatSchema).alias("value"))\
.writeStream\
.format("kafka")\
.outputMode("append")\
.option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
.option("topic", "t")\
.option("checkpointLocation", "/tmp/peldrak/checkpoint")\
.start()


# write streams to console
#TH1_stream = tumbling_TH1_df.writeStream.format("console").start()
TH2_stream = tumbling_TH2_df.writeStream.format("console").start()
HVAC1_stream = tumbling_HVAC1_df.writeStream.format("console").start()
HVAC2_stream = tumbling_HVAC2_df.writeStream.format("console").start()
MiAC1_stream = tumbling_MiAC1_df.writeStream.format("console").start()
MiAC2_stream = tumbling_MiAC2_df.writeStream.format("console").start()
Mov1_stream = tumbling_Mov1_df.writeStream.format("console").start()

# wait until they terminate
TH1_stream.awaitTermination()
TH2_stream.awaitTermination()
HVAC1_stream.awaitTermination()
HVAC2_stream.awaitTermination()
MiAC1_stream.awaitTermination()
MiAC2_stream.awaitTermination()
Mov1_stream.awaitTermination()
