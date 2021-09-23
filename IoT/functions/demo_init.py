# Databricks notebook source
# MAGIC %md
# MAGIC ###Set paths & parameters

# COMMAND ----------

workingDir = "/tmp/home_track_webinar"

raw_path = workingDir + "/raw"
bad_records_path = workingDir + "/bad_records"

bronze_path = workingDir + "/iot/bronze.delta" #Path to bronze table
checkpointPathBronze = workingDir + "/iot/bronze.checkpoint" #

silver_path = workingDir + "/iot/silver.delta" #Silver table
checkpointPathSilver = workingDir + "/iot/silver.checkpoint"

gold_path = workingDir + "/iot/gold.delta"  #Gold table
checkpointPathGold = workingDir + "/iot/gold.delta"

meta_path = workingDir + "/meta" # Path to meta data on sensors

#database name
db_name = 'vn_iot_demo'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure EventHub

# COMMAND ----------

import json
connectionString = dbutils.secrets.get("vn-demo", "eventhub-conn")
eventhub_name = 'vn_test_hub'

ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
startingEventPosition = {
  "offset": "@latest",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {db_name}')
spark.sql(f'USE {db_name}')

# COMMAND ----------

# define demo parameters
params = {}
params['n_sensors'] = 20 # the number of simulated sensors
params['seed'] = 754 # seed for the random number generator
params['temp_mu'] = 15 # mean temperature for normally distributed sensor readings
params['temp_sigma'] = 5 #standard deviation for normally distributed sensor readings
params['runtime_in_min'] = 30 #runtime for stream generator 
params['hot sensor id'] = 3 # id for sensor which runs hot
params['n_locations'] = 4 # number of plant locations

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create metadata tables

# COMMAND ----------

tables_collection = spark.catalog.listTables()
table_names = [table.name for table in tables_collection]

# COMMAND ----------

import pandas as pd
# locations
if 'locations' not in table_names:
  locations = [
    {'l_id': 0, 'Name' : 'Manchester Office', 'City': 'Manchester', 'latitude': 53.4808, 'longitude': -2.2426},
    {'l_id': 1, 'Name' : 'London Office', 'City': 'London', 'latitude': 51.5074, 'longitude': -0.1278},
    {'l_id': 2, 'Name' : 'Birmingham Office', 'City': 'Birmingham', 'latitude': 52.4862 ,'longitude': -1.8904},
    {'l_id': 3, 'Name' : 'Edinburgh Office', 'City': 'Edinburgh', 'latitude': 55.9533, 'longitude': -3.1883},
    {'l_id': 4, 'Name' : 'Cardiff Office', 'City': 'Cardiff', 'latitude': 51.4816, 'longitude': -3.1791}
  ]

  loc_pdf = pd.DataFrame.from_records(locations)

  spark.createDataFrame(loc_pdf).write.mode("overwrite").saveAsTable('Locations')

# COMMAND ----------

# sensors
if 'sensors' not in table_names:
  from scipy.stats import norm
  from random import randint, choice

  min_a = norm.ppf(0.01, loc=params['temp_mu'], scale=params['temp_sigma'])
  max_a = norm.ppf(0.99, loc=params['temp_mu'], scale=params['temp_sigma'])

  sensor_types = pd.DataFrame.from_records(
    [
      {'st_id' : 0, 'Sensor_Type': 'Type A Sensor', 'Manufacturer': 'BAE', 'min_accept_temp': min_a,'max_accept_temp': max_a},
      {'st_id' : 1, 'Sensor_Type': 'Type B Sensor', 'Manufacturer': 'Dyson ', 'min_accept_temp': min_a,'max_accept_temp': max_a}
    ]
  )

  data = {'deviceId' : [i for i in range(params['n_sensors'])],
          's_st_id' : [randint(0,1) for i in range(params['n_sensors'])],
          'l_id' : [randint(0,params['n_locations']) for i in range(params['n_sensors'])]
         }
  sensor_pdf = pd.DataFrame(data)
  sensor_pdf = sensor_pdf.merge(sensor_types,left_on='s_st_id',right_on='st_id').drop(['s_st_id','st_id'],axis=1).sort_values('deviceId')

  spark.createDataFrame(sensor_pdf).write.mode('overwrite').saveAsTable('Sensors')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define helper functions

# COMMAND ----------

import string

def get_random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(choice(letters) for i in range(length))
    return result_str
  
def getActiveStreams():
  try:
    return spark.streams.active
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("Unable to iterate over all active streams - using an empty set instead.")
    return []

def stopStream(s):
  try:
    print("Stopping the stream {}.".format(s.name))
    s.stop()
    print("The stream {} was stopped.".format(s.name))
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("An [ignorable] error has occured while stoping the stream.")

def stopAllStreams():
  streams = getActiveStreams()
  while len(streams) > 0:
    stopStream(streams[0])
    streams = getActiveStreams()
    
def untilStreamIsReady(name):
  queries = list(filter(lambda query: query.name == name, spark.streams.active))
  if len(queries) == 0:
    print("The stream is not active.")
  else:
    while (queries[0].isActive and len(queries[0].recentProgress) == 0):
      pass # wait until there is any type of progress
    if queries[0].isActive:
      queries[0].awaitTermination(5)
      print("The stream is active and ready.")
    else:
      print("The stream is not active.")
