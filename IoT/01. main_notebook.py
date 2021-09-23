# Databricks notebook source
# MAGIC %md ---
# MAGIC # Internet of Things Analytics
# MAGIC ### A live-demo showcasing the lakehouse architecture
# MAGIC 
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Delta medallion icon.png" alt='Make all your data ready for BI and ML' width=800/>
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install Folium

# COMMAND ----------

# MAGIC %md
# MAGIC ###Install Event Hub Spark connector

# COMMAND ----------

cntx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = cntx.apiUrl().getOrElse(None)
token = cntx.apiToken().getOrElse(None)
post_body = {
  "cluster_id": cntx.clusterId().getOrElse(None),
  "libraries": [
    {
      "maven": {
        "coordinates": "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
      }   
    }
  ]
}

# COMMAND ----------

import requests

response = requests.post(
  host + '/api/2.0/libraries/install',
  headers={"Authorization": "Bearer " + token},
  json = post_body
)

if response.status_code == 200:
  print(response.json())
else:
  raise Exception(f'Error: {response.status_code} {response.reason}')

# COMMAND ----------

# wait until library is installed
import json, time

while True:
  response = requests.get(
    host + f'/api/2.0/libraries/cluster-status?cluster_id={cntx.clusterId().getOrElse(None)}',
    headers={"Authorization": "Bearer " + token},
  )
  status = [library['status'] for library in response.json()['library_statuses'] if 'eventhubs-spark' in json.dumps(library['library'])][0]
  if status != "INSTALLING":
    break
  time.sleep(2)
  print("Waiting for library to install")

# COMMAND ----------

# MAGIC %run "./functions/demo_init"

# COMMAND ----------

# == Managed Delta on Databricks: automatically compact small files during write:
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Step 0: Set-up Sensor data stream 
# MAGIC - Configure parameter *runtime_in_min* in demo_init notebook for stream generation run-time<br>
# MAGIC - <a href="$./02. gen_stream">Link to notebook</a><br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 1: Set-up Stream for raw data und write them into Bronze-Table

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json

sensor_schema = StructType()\
  .add('timestamp',TimestampType()) \
  .add('deviceId',IntegerType()) \
  .add('temperature',DoubleType())

sensor_readings = (spark
  .readStream
  .format("eventhubs")
  .options(**ehConf)
  .load()
  .withColumn('payload',col("body").cast(StringType()))
  .withColumn('json',from_json(col('payload'),sensor_schema))
  )

# COMMAND ----------

# DBTITLE 1,Let us have a first look 
display(sensor_readings.select(col('payload'),col('json.*')))

# COMMAND ----------

# DBTITLE 1,Create temporary view for SQL-Querying
sensor_readings.createOrReplaceTempView('sensor_readings')

# COMMAND ----------

# DBTITLE 1,Let us have a first look at the data (in SQL)
# MAGIC %sql
# MAGIC 
# MAGIC select json.deviceId, min(json.temperature) as min, max(json.temperature) as max, avg(json.temperature) as mean
# MAGIC from sensor_readings
# MAGIC group by deviceId
# MAGIC order by deviceId asc

# COMMAND ----------

# MAGIC %md
# MAGIC One sensor seems to have much higher readings than all the others. But to determine whether these readings are abnormal, we will need more contextual data to put into perspective

# COMMAND ----------

# DBTITLE 1,Write data to bronze table filtering out erroneous data to separate error queue
def to_bronze_table(df,epoch_id):
  df.persist()
  # Write correct data to bronze path
  (df
   .filter('json.temperature is not null')
   .select('json.*')
   .write
   .mode("append")
   .format('delta')
   .save(bronze_path)
  )
  
  # Write erroneous data to separate delta table as error queue
  (df
   .filter('json.temperature is null')
   .write
   .mode("append")
   .format('delta')
   .save(bad_records_path)
  )
  
  df.unpersist()

sensor_readings.writeStream.queryName("bronze_query").foreachBatch(to_bronze_table).start()
untilStreamIsReady("bronze_query")

# COMMAND ----------

# MAGIC %md
# MAGIC # Let us have a look at Delta under the cover
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/image7.png" alt="Transaction Log" width="800"/>

# COMMAND ----------

# DBTITLE 1,A look into the bronze-path
display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# DBTITLE 1,All changes are persisted in Delta log
display(dbutils.fs.ls(f"{bronze_path}/_delta_log"))

# COMMAND ----------

# DBTITLE 1,Let us have a look at the error queue as well
display(dbutils.fs.ls(bad_records_path))

# COMMAND ----------

display(spark.read.format('delta').load(bad_records_path))

# COMMAND ----------

# DBTITLE 1,Register the Bronze Table in Hive-Metastore...
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS iot_bronze
  USING DELTA
  LOCATION '{bronze_path}'
  """
)

# COMMAND ----------

# DBTITLE 1,...and write a simple query on the data
# MAGIC %sql
# MAGIC select deviceId, count(*)
# MAGIC from iot_bronze
# MAGIC group by deviceId
# MAGIC order by deviceId

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 2: Create the Silver Table thorugh adding metadata info

# COMMAND ----------

locations = spark.sql('SELECT * from LOCATIONS')
locations_pd = locations.toPandas()
display(locations)

# COMMAND ----------

# DBTITLE 1,Let us look at plant locations on a map
import folium
from folium import plugins

# Bounds
min_lat, max_lat = 48.77, 60
min_lon, max_lon = -9.05, 5

plant_map = folium.Map(
  location=[(min_lat+max_lat)/2, (min_lon+max_lon)/2],
  zoom_start=6,
  min_lat=min_lat, 
  max_lat=max_lat,
  min_lon=min_lon, 
  max_lon=max_lon,
  detect_retina=True)

for index,row in locations_pd.iterrows():
  folium.Marker(
    [row['latitude'],row['longitude']], 
    popup=row['Name'],
    icon=folium.Icon(icon='building', prefix='fa')
  ).add_to(plant_map)
  
displayHTML(plant_map._repr_html_())

# COMMAND ----------

# DBTITLE 1,What additional information on sensor do we have?
sensors = spark.sql('SELECT * FROM sensors ORDER BY deviceId')

display(sensors)

# COMMAND ----------

# DBTITLE 1,Live-Join on the Stream und adding additional columns
from pyspark.sql.functions import col

silver_stream = (spark.readStream
                 .format('Delta')
                 .load(bronze_path)
                 .join(sensors, on=['deviceId'])
                 .join(locations, on=['l_id'])
                 .withColumn('Temp_high_alert', col('temperature') > col('max_accept_temp'))
                 .withColumn('Temp_low_alert', col('temperature') < col('min_accept_temp'))
                 .writeStream
                 .format("delta")
                   .option("path",silver_path)
                   .option("checkpointLocation",checkpointPathSilver)
                 .outputMode("append")
                 .queryName('silver_query')
                 .start()
                )
untilStreamIsReady("silver_query")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS iot_silver
  USING DELTA
  LOCATION '{silver_path}'
  """
)

# COMMAND ----------

# DBTITLE 1,Describe table allows accessing meta data on silver-table
# MAGIC %sql
# MAGIC describe table iot_silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 3: As a last part we create a Gold Table for a simple Monitoring Dashboard

# COMMAND ----------

from pyspark.sql.functions import window

gold_stream = (spark.readStream
               .format('delta')
               .load(silver_path)
               .withWatermark('timestamp','30 minutes')
               .filter((col('Temp_high_alert') == True) | (col('Temp_low_alert') == True))
               .groupBy(window('timestamp','300 seconds'),'deviceId')
               .count()
              )

gold_stream.createOrReplaceTempView("gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select deviceID as Sensor_No, window.start as interval_beginning, window.end as interval_end, count as No_of_alarms from gold
# MAGIC order by window.start

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Run cleanup
# MAGIC Will
# MAGIC - stop all streams
# MAGIC - drop databases and clean-up storage

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %run "./functions/cleanup"
