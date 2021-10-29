import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from auto_ai.functions import Auto_AI_Model
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer
from iotfunctions.bif import EntityDataGenerator
#from iotfunctions.enginelog import EngineLogging
import datetime as dt

import sys
import pandas as pd
import numpy as np

with open('../beta-1_credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

#with open('../Monitor-Demo-Credentials.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

#db.unregister_functions(["IsolationForestModel"])
# exit()
db.register_functions([Auto_AI_Model])
# exit()
print("Function registered or unregistered")
