import numpy as np
import pandas as pd
import sys
import logging
import requests
import json
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def invoke_model(df, input_items, wml_endpoint, token, input_columns=[]):
    ### Trim df to just the columns needed
    ### Loop for each variate to crate json payload for WML
    #  payload_scoring = {
    #      "input_data": [{"fields": ["MET1 - Average Air Temperature", "Smooth_last3"], "values": [[30.33, 28.2], [40.33, 38.2] ]}]}

    col = input_items[0]
    logger.debug("column to use for predictions %s" % col)
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
    payload = {"input_data": [{"fields": ["INV1A - Module 2 AC Power Smoothed"], "values": input_list}]}
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
    # logger.debug(data_scores)

    # Clean not a number values
    for idx, value in enumerate(np.array(data_scores['predictions'][0]['values'])):
        if value == "NULL":
            logger.debug('Null was found %s' % str(idx))
            data_scores['predictions'][0]['values'][idx] = [np.NaN]

    logging.debug("model response code: " + str(r.status_code))
    if r.status_code == 200:
        logging.debug("model response")
        # logging.debug(r.text)
        j = r.json()
        # logging.debug("json")
        # logging.debug(j)
        return data_scores['predictions'][0]['values']
    else:
        logging.error("error invoking model")
        logging.error(r.status_code)
        logging.error(r.text)
        return None

def auto_ai(df, parameters=None):

    import numpy as np
    import requests
    import json

    #  Initialize to simulate incoming DF
    # input_items is a ist of Column Names
    input_items = ['mdcalc_met1_poa_pyranometer_cor_smoothed']
    # output_items list of Column Names being added to the DF.
    output_items = ['mod_02_power_ac_smoothed_wml_prediction']
    wml_endpoint = "https://zen-cpd-zen.maximodev-14cc747866faab74c9640c8ac367af7f-0000.us-south.containers.appdomain.cloud/v4/deployments/4887e338-4718-4d6c-98f4-973cad74954f/predictions"
    token ="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImRhdHRhcmFvIiwicm9sZSI6IlVzZXIiLCJwZXJtaXNzaW9ucyI6WyJhY2Nlc3NfY2F0YWxvZyIsImNhbl9wcm92aXNpb24iLCJzaWduX2luX29ubHkiXSwic3ViIjoiZGF0dGFyYW8iLCJpc3MiOiJLTk9YU1NPIiwiYXVkIjoiRFNYIiwidWlkIjoiMTAwMDMzMTAwMiIsImF1dGhlbnRpY2F0b3IiOiJkZWZhdWx0IiwiaWF0IjoxNjA1MTc0Mjc2LCJleHAiOjE2MDUyMTc0NDB9.qErW7B2t7owp01GtATDAZf0ReQ75FK7ehPiBma-VP6o5SnrHpbTUKIwyILPtWQJppT4-PTXW4ixtz07t-_At5CtCEfZ3ibWZgqUTdH68tl1Tu-eQKsN_QmDtoW-FnzWElFB4BBQFGORoLFcEQDc4-BoPb6KkCpw14ytBBLLtAYmAkTxXCUFi3JSbt9I61UrQGoO3jbypU2M6IRwiP2ze5rtbyBDKcZwdJt6MqTFGUgUKiYRHvJ0daS_58_0dwmIZrxXskX0zcrcDXjHqU419AUFCwVveCT0oFvNsUcEM_4XXfsjXoroqdGxi0Fp8RXmeOKxrcEPubE5XLUgaKeeITQ"
    input_columns = []

    df = pd.DataFrame({'mod_02_power_ac_smoothed': [0, 0, 0, 0,
                                          3.618865967, 5.647155762000001, 8.648925781, 15.66516113,
                                          25.64648438, 38.78417969]},
                        index=[('73001', pd.Timestamp('2020-04-10 07:31')), ('73001', pd.Timestamp('2020-04-10 07:46')), ('73001', pd.Timestamp('2020-04-10 07:51')), ('73001', pd.Timestamp('2020-04-10 08:01')),
                               ('73000', pd.Timestamp('2020-04-05 21:37')), ('73000', pd.Timestamp('2020-04-05 21:42')),('73000', pd.Timestamp('2020-04-05 21:47')),('73000', pd.Timestamp('2020-04-05 21:52')),
                               ('73001', pd.Timestamp('2020-04-10 08:06')), ('73001', pd.Timestamp('2020-04-10 08:11'))])

    df.index = pd.MultiIndex.from_tuples(df.index, names=['id', 'evt_timestamp'])
    logging.debug("MultiIndex")
    logging.debug(df)

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

    results = invoke_model(df, input_items, wml_endpoint, token, input_columns=[])

    if results == None:
        logging.error('error invoking external model')
        logging.debug(df[output_items].dtype.name)
    else:
        logging.debug("type of result")
        logging.debug(type(results))
        logging.debug('Results received. Create a DF Column from a list of lists')
        logging.debug(results)
        logging.debug('output_items')
        #logging.debug(output_items)
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

        for i in results:
            logger.debug(' i[0]   ')
            logger.debug(i[0])

        # util.log_data_frame('util df head', df.head())
        #df[output_items] = [i[0] for i in results]
        df[output_items[0]] = [i[0] for i in results]
        logging.debug("df head")
        logging.debug(df)
        # df2 = pd.DataFrame(results, columns=[output_items])
        #logging.debug("df2 head")
        #logging.debug(df2.head())

    return df

    #return np.array(data_scores['predictions'][0]['values'])


def main(args):
    inputfile = args
    logger.debug('Reading args %s', inputfile)

    #  Check for how long an asset is running in a specific state
    logger.debug("Calculate auto_AI regression")
    val = auto_ai(df=None, parameters=None)
    logger.debug('Done testing ')

if __name__ == "__main__":
    logger.debug("here")
    main(sys.argv[1:])


    '''
    df columns  = Index(['power_ac_smoothed', 'mod_02_power_ac_smoothed',
           'mdcalc_met1_air_temp_avg', 'mod_01_3_phase_grid_pwr_kw',
           'mod_02_out_heatsink_temp_c', 'a_3_phase_grid_pwr_sys_w', 'pratio_cor',
           'mod_01_inp_heatsink_temp_c', 'mdcalc_met1_poa_pyranometer_avg',
           'mdcalc_met1_precip_avg', 'pratio_cor_smoothed',
           'mod_01_power_ac_smoothed', 'mod_01_pwr_supply_temp_c',
           'mdcalc_met1_wind_speed_avg', 'mod_01_inp_air_temp_c',
           'mod_03_power_ac_smoothed', 'mod_02_inp_air_temp_c',
           'mdcalc_met1_poa_pyranometer_cor', 'mod_02_3_phase_grid_pwr_kw',
           'mod_03_3_phase_grid_pwr_kw', 'mod_02_inp_vltg_v',
           'mod_02_pwr_supply_temp_c', 'mod_02_inp_heatsink_temp_c', 'online',
           'mod_02_inp_pwr_kw', 'mod_02_inp_amps_a', 'mod_03_inp_air_temp_c',
           'mod_01_out_heatsink_temp_c',
           'mdcalc_met1_poa_pyranometer_cor_smoothed', 'site', 'asset',
           'predictions_mod_02_inp_air_temp_c_2',
           'deviations_mdcalc_met1_air_temp_avg_2',
           'mdcalc_met1_air_temp_avg_fft_score',
           'mdcalc_met1_air_temp_avg_ga_score',
           'mdcalc_met1_air_temp_avg_kmeans_score',
           'mdcalc_met1_air_temp_avg_saliency_score',
           'mdcalc_met1_air_temp_avg_spectral_score',
           'met1_poa_pyranometer_cor_smoothed_ga_score', 'mod2_solar_irradiance',
           'mod_02_inp_air_temp_c_fft_score', 'mod_02_inp_air_temp_c_ga_score',
           'predicted_mod_02_inp_air_temp_c_kmeans_score',
           'mod_02_inp_air_temp_c_saliency_score',
           'mod_02_inp_air_temp_c_spectral_score',
           'mod_02_power_ac_smoothed_ga_score'])
    '''
