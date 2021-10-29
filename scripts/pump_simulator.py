# *****************************************************************************
# Copyright (c) 2021 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
# *****************************************************************************

import pandas as pd
import time
import sys
import uuid
import argparse
import settings
import logging

if __name__ == "__main__":

    # Read in environment variables
    ORGANIZATION = settings.ORGANIZATION
    DOMAIN = settings.DOMAIN
    DEVICE_TYPE_ID = settings.DEVICE_TYPE_ID
    logging.info("DEVICE_TYPE_ID %s " %settings.DEVICE_TYPE_ID)
    SOURCE_DEVICE_ID = settings.SOURCE_DEVICE_ID
    logging.info("SOURCE_DEVICE_ID %s " %SOURCE_DEVICE_ID)
    DEVICE_ID = settings.DEVICE_ID
    logging.info("DEVICE_ID %s " %settings.DEVICE_ID)
    TOKEN = settings.TOKEN

    '''
    # Read the device data in and select just device id 11111096
    df = pd.read_csv('../data/PumpData.csv',
                     index_col=False)
    print("Loading Pump Simulation Data ")
    print(df.head())
    # Select rows of pump you need

    device_df = df.loc[df.device_id == SOURCE_DEVICE_ID].sort_values(by=['rcv_timestamp_utc'])
    logging.info("Trimming Pump Simulation Data to  %s" %SOURCE_DEVICE_ID)
    logging.info(device_df.head())

    # Save only the columns you need
    columns = ['updated_utc', 'speed', 'head', 'device_id', 'pump_mode', 'flow', 'voltage', 'POWER', 'CURRENT']
    drop_cols = ['Unnamed: 0', 'id', 'warn', 'kw_hour', 'alarm', 'TIMESTAMP',
                 'devicetype', 'deviceid', 'temperature', 'logicalinterface_id', 'eventtype', 'accel_speed',
                 'format', 'vibrations_xaxis', 'vibrations_yaxis', 'vibrations_zaxis',
                 'rmsn_z', 'rmsn_x', 'rmsn_y', 'rms_x_avg', 'pwr', 'hw_ver', 'dq', 'fw_ver',
                 'accel_power', 'rms_x', 'rms_y', 'rms_z', 'VERSION', 'run_qty', 'tag_number', 'design_head',
                 'perf_option',
                 'design_flow', 'pts_count', 'serial_number', 'firmware_ver', 'reated_power', 'vibration_n_yaxis',
                 'vibration_n_xaxis', 'vibration_n_zaxis', 'rated_current', 'rated_speed', 'hardware_ver', 'pts']

    ac_df = device_df.drop(columns=drop_cols).loc[device_df['device_id'] == SOURCE_DEVICE_ID, :]
    print('ac_df.shape 1')
    print(ac_df.shape)
    print(ac_df)
    ac_df.dropna().to_csv('../data/maximo-auto_ai_pump_data_' + DEVICE_ID + '.csv')
    '''
    # Read and check the values
    c_df = pd.read_csv('../data/maximo-auto_ai_pump_data_' + DEVICE_ID + '.csv', parse_dates=['rcv_timestamp_utc']).drop(
        columns=['Unnamed: 0'])
    logging.info(c_df.head())
    logging.info(c_df.columns)
    logging.info(c_df.columns)
    # Reformat timestamp
    c_df['evt_timestamp'] = c_df['rcv_timestamp_utc'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4])
    print('ac_df Reformat timestamps')
    print(c_df.head())

    try:
        import wiotp.sdk
    except ImportError:
        import os
        import inspect
        cmd_subfolder = os.path.realpath(
            os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], "../../src"))
        )
        if cmd_subfolder not in sys.path:
            sys.path.insert(0, cmd_subfolder)
        import wiotp.sdk

    def commandProcessor(cmd):
        print("Command received: %s" % cmd.data)

    authMethod = None

    # Initialize the properties we need
    parser = argparse.ArgumentParser()

    # Set Arguements for your Tenant for the IOTP SDK
    parser.add_argument("-c", "--cfg", required=False, default=None, help="configuration file")
    parser.add_argument("-E", "--event", required=False, default="event", help="type of event to send")
    parser.add_argument(
        "-N", "--nummsgs", required=False, type=int, default=3, help="send this many messages before disconnecting"
    )
    parser.add_argument("-D", "--delay", required=False, type=float, default=10, help="number of seconds between msgs")
    args, unknown = parser.parse_known_args()
    authMethod = "token"

    # Initialize the device client.
    try:
        deviceOptions = {
            "identity": {"orgId": ORGANIZATION,
                         "typeId": DEVICE_TYPE_ID,
                         "deviceId": DEVICE_ID
            },
            "auth": {
                    "token": TOKEN
            },
            "options": {
                        "domain": DOMAIN
                        ,
                        "mqtt": {
                            "caFile": "messaging.pem",
                            "transport": "tcp",
                            "port": 443
                        }
            }
        }
        logging.info("deviceOptions")
        logging.info(deviceOptions)
        deviceCli = wiotp.sdk.device.DeviceClient(deviceOptions)
        deviceCli.commandCallback = commandProcessor
    except Exception as e:
        print("Caught exception connecting device: %s" % str(e))
        sys.exit()

    # Connect and send datapoint(s) into the cloud
    deviceCli.connect()

    for index, row in c_df.iterrows():

        # data = {"simpledev": "ok", "x": x}
        # Timestamp 2021-03-29T14:08:05Z  or 2020-05-07-14.17.26.5
        data = {'evt_timestamp': row['evt_timestamp'][:-1] + 'Z', 'speed': row['speed'], 'head': row['head'],
                'pump_mode': row['pump_mode'], 'flow': row['flow'], 'voltage': row['voltage'], 'POWER': row['POWER'],
                'CURRENT': row['CURRENT']}
        print(row['evt_timestamp'][:-1] + 'Z')

        print("data sent")
        print(data)

        def myOnPublishCallback():
            print(
                "Confirmed event %s, speed: %s , head: %s, pump_mode: %s, flow: %s, voltage: %s, POWER: %s, CURRENT: %s" % (
                    row['evt_timestamp'][:-1] + 'Z', row['speed'], row['head'], row['pump_mode'], row['flow'],
                    row['voltage'],
                    row['POWER'], row['CURRENT']))


        success = deviceCli.publishEvent(args.event, "json", data, qos=0, onPublish=myOnPublishCallback)
        if not success:
            print("Not connected to WIoTP")

        time.sleep(args.delay)


    # Disconnect the device and application from the cloud
    deviceCli.disconnect()