import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd
import json
import base64
import requests

# from iotfunctions.base import BaseTransformer
from iotfunctions.base import BasePreload
from iotfunctions.base import BaseTransformer
from iotfunctions import ui
#from iotfunctions import util
from iotfunctions.db import Database
from iotfunctions import bif
# import datetime as dt
import datetime
import urllib3
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://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@github.com/a826190/maximo_autoai_agupta.git@mas851'

class Auto_AI_Model(BaseTransformer):
    # _allow_empty_df = True  # allow this task to run even if it receives no incoming data
    # produces_output_items = False  # this task does not contribute new data items
    # requires_input_items = True  # this task does not require dependent data items

    # def __init__(self, ):
    def __init__(self, wml_endpoint, model_name, token, input_items,
                 output_items='http_preload_done'):
        logging.debug("in init function")
        super().__init__()
        self.input_items = input_items
        self.output_items = output_items
        self._output_list = [output_items]
        logging.debug('output_items %s', output_items)
        logging.debug('input_items %s', input_items)
        input_items.sort()
        logging.debug('sorted input_items %s', input_items)
        self.input_columns = input_items  # .replace(' ', '').split(',')
        self.wml_endpoint = wml_endpoint
        # https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/ml-authentication.html
        self.model_name = model_name
        self.token = token
        logging.debug("finished init")

    def invoke_model(self, df, input_items, wml_endpoint, token, input_columns=[]):
        # Trim df to just the columns needed
        logger.debug(df.head())

        ### Loop for each variate to crate json payload for WML
        # payload_scoring = {
        #     "input_data": [{"fields": ["MET1 - Average Air Temperature", "Smooth_last3"], "values": [[30.33, 28.2], [40.33, 38.2] ]}]}

        col = input_items[0]
        logger.debug("column to use for predictions %s" %col )
        data_array = df[col].to_numpy()
        data_list = data_array.tolist()
        input_list = [[float(el)] for el in data_list]
        logger.debug("input_list")
        logger.debug(input_list)
        token = token
        str1 = 'Bearer ' + token
        logger.debug(str1)
        logger.debug(wml_endpoint)

        header = {'Content-Type': 'application/json', 'Authorization': str1}
        payload = {"input_data":[{"fields":[col],"values":input_list}]}
        logger.debug(
            "\npayload: " + str(type(payload)) + "\ndata_list: " + str(
                type(data_list)) + "\ndata_array" + str(type(
                data_array)))
        logger.debug("Payload")
        logger.debug(payload)
        r = requests.post(wml_endpoint, json=payload, headers=header)
        logger.debug("Prediction response")
        logger.debug(r.status_code)
        data_scores = json.loads(r.text)
        #logger.debug(data_scores)

        # Clean Null values and replace with NaN values
        for idx, value in enumerate(np.array(data_scores['predictions'][0]['values'])):
            if value.any() == "NULL":
                logger.debug('Null was found %s' %str(idx) )
                data_scores['predictions'][0]['values'][idx] = [np.NaN]

        logging.debug("model response code: " + str(r.status_code))
        if r.status_code == 200:
            logging.debug("model response")
            #logging.debug(r.text)
            j = r.json()
            #logging.debug("json")
            #logging.debug(j)
            return data_scores['predictions'][0]['values']
        else:
            logging.error("error invoking model")
            logging.error(r.status_code)
            logging.error(r.text)
            return None

    def execute(self, df):  # , force_overwrite=True, start_ts = None,end_ts=None):
        logging.debug('in execution method')
        logging.debug('df.columns %s', df.columns)
        logging.debug('self.input_items %s', self.input_items)
        logging.debug('self.output_items %s', self.output_items)
        logging.debug('processing %s rows', len(df))

        logger.debug("Debugging")
        #  Set DF to a single level.   Initially DF index is at 2 levels
        #  Saving original index column names
        entity_index_name = df.index.names[0]
        time_index_name = df.index.names[1]
        #  Reset to a single index level
        df.reset_index(inplace=True)

        logging.debug("DF After Reset_Index")
        logging.debug(df)
        logger.debug('Here are entity_index_name ')
        logger.debug(entity_index_name)
        logger.debug('time_index_name ')
        logger.debug(time_index_name)
        logger.debug('df.columns')
        logger.debug(df.columns)

        entity_list = df[entity_index_name].unique().tolist()
        logger.debug("List of unique entities")
        logger.debug(entity_list)

        results = self.invoke_model(df, self.input_items, self.wml_endpoint, self.token, self.input_items)

        if results == None :
            logging.error('error invoking external model')
            logging.debug(df[self.output_items].dtype.name)
        else:
            logging.debug("type of result")
            logging.debug(type(results))
            logging.debug('Results received. Create a DF Column from a list of lists')
            logging.debug(results)
            logging.debug('self.output_items')
            logging.debug(self.output_items)
            logging.debug('df head in ')
            logging.debug(df.head())
            logging.debug("df.info")
            logging.debug(df.info)
            logging.debug("df.columns")
            logging.debug(df.columns)
            logging.debug('df.types')
            logging.debug(df.dtypes)
            logging.debug("df.shape")
            logging.debug(df.shape)
            logging.debug("len results")
            logging.debug(len(results))

            '''
            for i in results:
                logger.debug(' i[0] Type and Value  ')
                try:
                    logger.debug(type(i[0]))
                    logger.debug(i[0])
                except:
                    # Map string Null to np.nan
                    logger.debug( i[0] )
            '''


            # df = df.reindex(columns=list(df.columns) + self.output_items)  # if this was a multivariate
            df = df.reindex(columns=list(df.columns) + [self.output_items])
            df.loc[:, self.output_items] = results
            logging.debug("df after reindex")
            logging.debug(df.head())

            '''
            logging.debug("Map to DF out column")
            #util.log_data_frame('util df head', df.head())
            #df[self.output_items] = [i[0] for i in results]
            df[self.output_items[0]] = [i[0] for i in results]
            logging.debug("df head out")
            '''
            df.set_index([entity_index_name, time_index_name], inplace=True)
            #df.reset_index([entity_index_name, time_index_name], inplace=True)
            logging.debug("df after reset_index")
            logging.debug(df)
            #df2 = pd.DataFrame(results, columns=[self.output_items])

        return df

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(
                                    name='input_items',
                                    datatype=float,
                                    description="Data items adjust",
                                    # output_item = 'output_item',
                                    is_output_datatype_derived=True
                                    ))
        inputs.append(ui.UISingle(name='wml_endpoint',
                                  datatype=str,
                                  description='Endpoint to WML service where model is hosted',
                                  tags=['TEXT'],
                                  required=True
                                  ))
        inputs.append(ui.UISingle(name='model_name',
                                  datatype=str,
                                  description='Name of the WML model',
                                  tags=['TEXT'],
                                  required=False
                                  ))
        inputs.append(ui.UISingle(name='token',
                                  datatype=str,
                                  description='IBM Cloud API Token',
                                  tags=['TEXT'],
                                  required=True
                                  ))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UISingle(name='output_items', datatype=float))
        return (inputs, outputs)

        outputs = []
        return (inputs, outputs)
