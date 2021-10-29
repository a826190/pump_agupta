import os
import json
import logging
from dotenv import load_dotenv
from os.path import join, dirname

def set_log_level(logger_level):
    logging.getLogger().setLevel(logger_level)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logger_level)

    # create formatter
    formatter = logging.Formatter('\n%(asctime)s - %(name)s - %(levelname)s - \n%(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logging.getLogger().addHandler(ch)


# Configure Logging Default
set_log_level(logging.INFO)

# If on bluemix load env differently
# Load Environment variables set via VCAP variables in Bluemix
#if 'VCAP_SERVICES' in os.environ:
    # print("On Bluemix...")

# Check for existance of .env file
env_path = join(dirname(__file__), '../.env')
logging.info("env_path Set To: " + env_path)

# Load .env file into os.environ
load_dotenv(env_path)

# ===================
# Logging Settings
# ===================
try:
    # API Key for bot analytics
    LOG_LEVEL = os.environ.get("LOG_LEVEL").upper()
    log_level_str = LOG_LEVEL

    if LOG_LEVEL == 'INFO':
        LOG_LEVEL = logging.INFO
    elif LOG_LEVEL == 'DEBUG':
        LOG_LEVEL = logging.DEBUG
    elif LOG_LEVEL == 'WARNING':
        LOG_LEVEL = logging.WARNING
    elif LOG_LEVEL == 'ERROR':
        LOG_LEVEL = logging.ERROR
    else:
        LOG_LEVEL = logging.WARNING

    logging.info("Logging Set To: " + log_level_str)

    if LOG_LEVEL != logging.WARNING:
        set_log_level(LOG_LEVEL)
except Exception as ex:
    template = 'Error: {0} Problem reading Logging Level string from environment variables. Logging set to WARNING LEVEL. Arguments: \n{1!r}'
    message = template.format(type(ex).__name__, ex.args)
    ANALYTICS_ENABLED = False
    logging.warning(message)


# ===================
# Device Settings
# ===================
try:
    # Get Device settings
    DEVICE_TYPE_ID = os.environ.get("DEVICE_TYPE_ID")
    logging.info("DEVICE_TYPE_ID set from env: " + str(DEVICE_TYPE_ID))
    DEVICE_ID = os.environ.get("DEVICE_ID")
    logging.info("DEVICE_ID set from env: " + str(DEVICE_ID))
    SOURCE_DEVICE_ID = os.environ.get("SOURCE_DEVICE_ID")
    logging.info("DEVICE_ID set from env: " + str(SOURCE_DEVICE_ID))
    TOKEN = os.environ.get("TOKEN")
    logging.info("TOKEN set from env: " + str(TOKEN))
    ORGANIZATION = os.environ.get("ORGANIZATION")
    logging.info("Organization set from env: " + str(ORGANIZATION))
    DOMAIN = os.environ.get("DOMAIN")
    logging.info("Organization set from env: " + str(DOMAIN))
except Exception as ex:
    template = 'Error: {0} Problem reading device settings from environment variables. Arguments: \n{1!r}'
    message = template.format(type(ex).__name__, ex.args)
    logging.warning(message)

# ===================
#  Prediction Model Settings
# ===================
try:
    # Get Prediction Model  settings
    ENTITY_TYPE_ID = os.environ.get("ENTITY_TYPE_ID")
    logging.info("ENTITY_TYPE_ID set from env: " + str(ENTITY_TYPE_ID))
except Exception as ex:
    template = 'Error: {0} Problem reading Model settings from environment variables. Arguments: \n{1!r}'
    message = template.format(type(ex).__name__, ex.args)
    logging.warning(message)