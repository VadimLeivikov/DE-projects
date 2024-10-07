from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# explicitly define the structure of the JSON content
schema = StructType().add("id",IntegerType()).add("action", StringType())

users_schema = StructType().add("id",IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()

# static dataset - emulation of an external data source

input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", "netology-spark") \
  .option("failOnDataLoss", False) \
  .load()

#the incoming content:
#input_stream.writeStream.format("console").outputMode("append").start().awaitTermination()
#input_stream = input_stream.writeStream.format("console").outputMode("append").start()

json_stream = input_stream.select(col("timestamp").cast("string"), from_json(col("value").cast("string"), schema).alias("parsed_value"))
#json_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#highlighting the elements of interest:
clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))
#clean_data.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#adding a join with a static dataset - creating data
users_data = [(1,"Jimmy",18),(2,"Hank",48),(3,"Johnny",9),(4,"Erle",40)]
users = spark.createDataFrame(data=users_data,schema=users_schema)


#join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name, users.user_age, clean_data.action, clean_data.timestamp)
join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name, users.user_age, clean_data.action, clean_data.timestamp)
#join_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#removing terminate:
#res_checkpoints= join_stream.writeStream.\
#format("console").\
#outputMode("append").\
#option("truncate", False).\
#option("checkpointLocation", "checkpoint/target").\
#start()


#sleep(10)
#res_checkpoints.stop()


#adding an aggregate - to display the number of unique IDs:
stat_stream = clean_data.groupBy("id").count()
#stat_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()

join_stream_agg = stat_stream.join(users, stat_stream.id == users.id, "left_outer").select(users.user_name, users.user_age, col('count'))

res_checkpoints_agg= join_stream_agg.writeStream.\
format("console").\
outputMode("complete").\
option("truncate", False).\
option("checkpointLocation", "checkpoint/target").\
start()\




sleep(10)
res_checkpoints_agg.stop()
