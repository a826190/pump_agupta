import json
#import logging
#from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
#from iotfunctions import bif
#from auto_ai.functions import Auto_AI_Model
from ai_prediction.functions import PredictionModel_agupta
#from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
#from iotfunctions.base import BaseTransformer
#from iotfunctions.bif import EntityDataGenerator
#from iotfunctions.enginelog import EngineLogging
#import datetime as dt

#import sys
#import pandas as pd
#import numpy as np

with open('credentials_as.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

#with open('../Monitor-Demo-Credentials.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

#db.unregister_functions(["PredictionModel"])
# exit()
db.register_functions([PredictionModel_agupta])
# exit()
print("Function registered or unregistered")
