import json
import logging
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from ai_prediction.functions import PredictionModel

EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

with open('../credentials_as.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = 'DEMO_MAM'
#db_schema = 'public'
db = Database(credentials=credentials, entity_type="124")
entity_type_name = 'pump_co_li'
deviceid ='111137F8'
entityType = entity_type_name
logger.debug("entityType")
# Retrieve a singe entity df
entity_name = 'iot_pump_co_li'
df = db.read_table(table_name=entity_name, schema=db_schema)
logger.debug(df)
for col in df.columns:
    logger.debug(col)

# Show the df structure and indexes
orig_indexes_names = df.index.names
logger.debug(orig_indexes_names)

# Reset the df indexes and columns required by execute method
# In the pipeline index levels will always be = ['entity_id', 'timestamp']
entity_index_name = "entity_id"
time_index_name = "evt_timestamp"
df[entity_index_name] = df['deviceid']
df[time_index_name] = df['rcv_timestamp_utc']

# Trim columns required  execute method
model_columns = ['speed', 'flow', 'voltage', 'CURRENT', 'POWER']
df.set_index([entity_index_name, time_index_name], inplace=True)
t_df = df.loc[df['deviceid'] == deviceid, :][model_columns]

logger.debug("t_df")
logger.debug(t_df.head())

#logger.debug("Set Index")
#ex_df = t_df.reset_index()
#logger.debug("Done resetting Index needed for execute method")
#logger.debug(ex_df)

# metric_value = "1.170199,-19.711880,-309526912.0,-2000.026978, 6.359863"
vector = ['speed', 'flow', 'voltage', 'CURRENT']
name = 'power_random_forest.mod'

# Call Custom Function using IOTFUnction SDK
'''
fn = ExtraTreesPredictModel(model_name=name, features=vector)
df = fn.execute_local_test(db=db, db_schema=db_schema, generate_days=1,to_csv=True)
print(df)
'''

# Unit Test Function Locally build df
fn = PredictionModel(model_name=name, features=vector)
fn.execute(t_df)
