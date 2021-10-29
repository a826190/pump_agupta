import logging
import json
from iotfunctions.base import BaseTransformer
from iotfunctions import ui
from iotfunctions.ui import UISingle, UIMulti, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://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@github.com/a826190/pump_agupta.git@mas851'

from iotfunctions.dbtables import DBModelStore
from iotfunctions.db import Database

#  Remove this comment to run locally.    Dummy datbase back before deploying.
#'''
class DatabaseDummy:
    tenant_id = 'demo'
    entity_type_id = '1'
    db_type = 'db2'
    #db_type = 'ibm'
    #db_schema = "public"
    db_schema = "DEMO_MAM"
    try:
        with open('../credentials_as.json', encoding='utf-8') as F:
            credentials = json.loads(F.read())
        db_connection = Database(credentials=credentials, entity_type="124").native_connection
        #model_store = FileModelStore()
        model_store = DBModelStore(tenant_id=tenant_id, entity_type_id=entity_type_id, schema=db_schema, db_connection=db_connection, db_type=db_type)
    except Exception as e:
        logging.debug('Database Dummy failed because running in Service ' + str(e))
        db_connection = None
        model_store = None
        pass
#'''

class PredictionModel_agupta(BaseTransformer):
    # _allow_empty_df = True  # allow this task to run even if it receives no incoming data
    # produces_output_items = False  # this task does not contribute new data items
    # requires model_name, features  = True  # this task does not require dependent data items

    # def __init__(self, ):
    def __init__(self, model_name, features,
                 output_items='PredictionModel_agupta'):
        logging.debug("in init function")
        super().__init__()
        self.output_items = output_items
        self._output_list = [output_items]
        logging.debug('output_items %s', output_items)
        self.model_name = model_name  # .replace(' ', '').split(',')
        logging.debug('model_name %s', model_name)
        logging.debug("features recieved in init")
        logging.debug(features)
        self.features = features
        logging.debug("finished init")
        self.whoami = 'Regressor Model Function'

    def check_feature_vector(self, model_feature_vector=None):
        """
        Checks if input feature vector is the same as pre-trained model's feature vector
        :param model_feature_vector
        """
        model_required_input = [f.lower() for f in model_feature_vector]
        user_given_input = [f.lower() for f in self.features]
        if not set(model_required_input) == set(user_given_input):
            logger.debug(f'Pre-trained model features {model_feature_vector} are different from input features '
                         f'{self.features}')
            return False
        return True

    def parse_features(self, comma_string):
        logging.debug("Parse features model")
        metric_names =[]
        try:
            metric_names = comma_string.split(',')
            logging.debug("metric_names")
            logging.debug(metric_names)
        except(RuntimeError, TypeError, NameError, ValueError):
            logging.debug("Error parsing features for  Extra Trees model")
        return metric_names

    def invoke_model(self, df):
        # Calls trained model stored in Monitor DB using parsed feature metrics.
        logging.debug("Oosting entity data features to Extra Trees model %s" %self.model_name)

        #metric_names = self.parse_features(features)

        # In the pipeline index levels will always be = ['entity_id', 'timestamp']
        logger.debug(locals())
        indexes = df.index.names
        timestamp_index = indexes[1]
        #dfe = df.reset_index()   we don't use it

        logging.debug('Calling model for prediction')
        # retrieve the model from monitor
        try:
            logging.debug("self._entity_type.db is set %s" % self._entity_type.db)
            db = self._entity_type.db
            logging.debug("db is set %s" %db)
            model_dict = db.model_store.retrieve_model(self.model_name)
            logging.debug('load model')
            monitor_model = model_dict['model']
            logging.debug(monitor_model)
            feature_vector = model_dict['feature_vector']
            logging.debug(feature_vector)
        except Exception as e:
            logging.debug('Online Model retrieval failed with ' + str(e))
            # See if running local and retrieve db that way.
            # Try a second time to retrieve the model from monitor
            #  Remove this comment to run locally
            try:
                logging.debug('second attempt to load model')
                db = DatabaseDummy()
                logging.debug(db)
                model_dict = db.model_store.retrieve_model(self.model_name)
                logging.debug('load model')
                monitor_model = model_dict['model']
                logging.debug(monitor_model)
                feature_vector = model_dict['feature_vector']
                logging.debug(feature_vector)
            except Exception as e:
                print('Local Model retrieval failed with ' + str(e))
                pass


        # Check that input feature vector must be the same as pre-trained models feature vector
        # Check feature order needs to match order that model was trained for.
        if not self.check_feature_vector(feature_vector):
            logging.error("Wrong model")
            #df[self.target] = 0  treat df as read-only, self.target is undef'd
            #return df
            return None

        # Test that it is still able to score by df

        #try:
        logging.debug("Check columns that exist")
        for col in df.columns:
            logger.debug(col)

        s_df = df[feature_vector].copy()
        logging.debug("s_df")
        logging.debug(s_df.head())
        logging.debug(s_df.info)
        logging.debug("s_df to_numpy")
        logging.debug(s_df.to_numpy())
        s_df = s_df.fillna(0)
        #s_df.columns = s_df.columns.str.lower()
        x1current_list = monitor_model.predict(s_df.to_numpy())
        logging.debug("score %s" %x1current_list)
        return x1current_list
        #except Exception as e:
        #    logging.error("Model scoring failed with " + str(e))
        #    return None


    '''
    Execute Method df
    in execution method
    df.columns Index(['wss1speed', 'x1torque', 'x1position', 'x1load', 'deviceid',
           '_timestamp'],
          dtype='object')
    self.output_items PredictionModel
    1446 rows
    id    evt_timestamp  wss1speed  ... _timestamp
    73003 2021-01-21 10:45:11.091319   1.759622  ... 2021-01-21 10:45:11.091319
          2021-01-21 10:46:11.091319   0.777343  ... 2021-01-21 10:46:11.091319
    73004 2021-01-21 10:47:11.091319   0.391523  ... 2021-01-21 10:47:11.091319
          2021-01-21 10:48:11.091319   1.547923  ... 2021-01-21 10:48:11.091319
    73001 2021-01-21 10:49:11.091319   0.558748  ... 2021-01-21 10:49:11.091319
    ...                                     ...  ...                        ...
    73002 2021-01-22 10:46:11.091319   2.517702  ... 2021-01-22 10:46:11.091319
    73000 2021-01-22 10:47:11.091319   0.663354  ... 2021-01-22 10:47:11.091319
    73001 2021-01-22 10:48:11.091319   0.760289  ... 2021-01-22 10:48:11.091319
    73003 2021-01-22 10:49:11.091319   0.529209  ... 2021-01-22 10:49:11.091319
    73004 2021-01-22 10:50:11.091319   1.281836  ... 2021-01-22 10:50:11.091319
    [1446 rows x 6 columns]
    '''
    def execute(self, df):  # , force_overwrite=True, start_ts = None,end_ts=None):
        # BaseTransformer()
        logging.debug('in execution method')
        logging.debug('df.columns %s', df.columns)
        # rename Pandas columns to lower case
        # df.columns = df.columns.str.lower()
        logging.debug('df.columns %s', df.columns)
        logging.debug('self.output_items %s', self.output_items)
        logging.debug('processing %s rows', len(df))
        logging.debug(df)
        #df_copy = df.copy()   # unused

        results = self.invoke_model(df)

        logging.debug('setting to 0')

        df[self.output_items] = 0
        #if not results.any():
        if results is not None:
            logging.debug('results received')
            # df.loc[:, self.output_items] = results['values']
            # df[self.output_items] = results['values']
            #df[self.output_items] = [i[0] for i in results['values']]
            #column_result = results.reshape(-1, 1).to_list()
            df[self.output_items] = results
            # Remove NaN
            df[self.output_items] = df[self.output_items].fillna(0)
            logging.debug(df.head())

            #df[self.output_items] = results.to_list()
        else:
            logging.error('error invoking external model')
            #logging.debug(df[self.output_items].dtype.name)
            #df[self.output_items] = 0

        logger.info('Columns: ' + str(df.columns))
        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []

        inputs.append(ui.UISingle(name='model_name',
                                  datatype=str,
                                  description='Endpoint to WML service where model is hosted',
                                  tags=['TEXT'],
                                  required=True
                                  ))

        inputs.append(UIMultiItem(name='features',
                                  datatype=float,
                                  description='Metrics inputs to calculate a prediction',
                                  required=True
                                  ))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UISingle(name='output_items', datatype=float))
        return (inputs, outputs)

        outputs = []
        return (inputs, outputs)
