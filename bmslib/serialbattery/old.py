
import configparser
import logging
from pathlib import Path
import sys
from time import sleep
from .error import errors_in_config

# LOGGING
logging.basicConfig()
logger = logging.getLogger("SerialBattery")

PATH_CONFIG_DEFAULT: str = "config.default.ini"
PATH_CONFIG_USER: str = "config.ini"

config = configparser.ConfigParser()
path = Path(__file__).parents[0]
default_config_file_path = str(path.joinpath(PATH_CONFIG_DEFAULT).absolute())
custom_config_file_path = str(path.joinpath(PATH_CONFIG_USER).absolute())
try:
    config.read([default_config_file_path, custom_config_file_path])

    # Ensure the [DEFAULT] section exists and is uppercase
    if "DEFAULT" not in config:
        logger.error(f'The custom config file "{custom_config_file_path}" is missing the [DEFAULT] section.')
        logger.error("Make sure the first line of the file is exactly (case-sensitive): [DEFAULT]")
        sleep(60)
        sys.exit(1)

except configparser.MissingSectionHeaderError as error_message:
    logger.error(f'Error reading "{custom_config_file_path}"')
    logger.error("Make sure the first line is exactly: [DEFAULT]")
    logger.error(f"{error_message}\n")
    sleep(60)
    sys.exit(1)

# Map config logging levels to logging module levels
LOGGING_LEVELS = {
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

# Set logging level from config file
logger.setLevel(LOGGING_LEVELS.get(config["DEFAULT"].get("LOGGING").upper()))



# Check if there are any options in the custom config file that are not in the default config file
default_config = configparser.ConfigParser()
custom_config = configparser.ConfigParser()
# Ensure that option names are treated as case-sensitive
default_config.optionxform = str
custom_config.optionxform = str
# Read the default and custom config files
default_config.read(default_config_file_path)
custom_config.read(custom_config_file_path)

for section in custom_config.sections() + ["DEFAULT"]:
    if section not in default_config.sections() + ["DEFAULT"]:
        errors_in_config.append(f'Section "{section}" in config.ini is not valid.')
    else:
        for option in custom_config[section]:
            if option not in default_config[section]:
                errors_in_config.append(f'Option "{option}" in config.ini is not valid.')

# Free up memory
del default_config, custom_config, section, option