# Databricks notebook source
# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %run "./demo_init"

# COMMAND ----------

from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData

client = EventHubProducerClient.from_connection_string(conn_str=connectionString, eventhub_name=eventhub_name)

# COMMAND ----------

import json
import datetime
import time
from random import randint, gauss, random

rt = datetime.timedelta(minutes=params['runtime_in_min'])

i=0
t0 = datetime.datetime.now()
t1 = datetime.datetime.now()
while t1-t0 < rt:
  i+=1
  rand = random()
  sid = randint(0,params['n_sensors'])
  if rand >= 0.05: # 5% of readings are faulty
    if sid != params['hot sensor id']:
      msg = {
        "timestamp" : str(datetime.datetime.now()),
        "deviceId" : sid,
        "temperature" : gauss(params['temp_mu'],params['temp_sigma'])
      }
    else:
      msg = {
        "timestamp" : str(datetime.datetime.now()),
        "deviceId" : sid,
        "temperature" : gauss(params['temp_mu'],params['temp_sigma']) + 60.6
      }
  else:
    msg = {
    "timestamp" : str(datetime.datetime.now()),
    "deviceId" : randint(0,params['n_sensors']),
    "temperature" : 'ERROR'
  }
  
  event_data_batch = client.create_batch()
  event_data_batch.add(EventData(json.dumps(msg)))
  client.send_batch(event_data_batch)  
    
  
  if i < 10: print(json.dumps(msg))
    
  time.sleep(0.05)
  t1 = datetime.datetime.now()
