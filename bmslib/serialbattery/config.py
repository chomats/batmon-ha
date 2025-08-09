from bmslib.util import dotdict
from .error import errors_in_config
from bmslib.util import get_logger
from typing import List, Any, Callable
import configparser
import sys
from pathlib import Path
from time import sleep

logger = get_logger()

class Config(object):
    user_config = dotdict()
    config = configparser.ConfigParser()
    
    def __init__(self, config_dict: dict):
        self.user_config.update(config_dict)
        self.load_config()
        self.user_config.update({'DEFAULT': dict(self.config.defaults())})
        self.user_config.update({section: dict(self.config.items(section)) for section in self.config.sections()})

    def load_config(self):
        PATH_CONFIG_DEFAULT: str = "config.default.ini"
        PATH_CONFIG_USER: str = "config.ini"
        
        path = Path(__file__).parents[2]
        default_config_file_path = str(path.joinpath(PATH_CONFIG_DEFAULT).absolute())
        custom_config_file_path = str(path.joinpath(PATH_CONFIG_USER).absolute())
        try:
            self.config.read([default_config_file_path, custom_config_file_path])

            # Ensure the [DEFAULT] section exists and is uppercase
            if "DEFAULT" not in self.config:
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
    
    def get_user_config(self) -> dict:
        return self.user_config
    
    def set_user_config(self, config_dict: dict):
        self.user_config.update(config_dict)
    
    def get_group(self, group: str) -> dict:
        return self.user_config.get(group, {})
    
    def get_option(self, group: str, option: str) -> Any:
        return self.get_group(group).get(option)
    
    # --------- Helper Functions ---------
    def get_bool(self, group: str, option: str) -> bool:
        """
        Get a boolean value from the config file.
    
        :param group: Group in the config file
        :param option: Option in the config file
        :return: Boolean value
        """
        return self.get_group(group).get(option, "False").lower() == "true"
    
    
    def get_float(self, group: str, option: str, default_value: float = 0) -> float:
        """
        Get a float value from the config file.
    
        :param group: Group in the config file
        :param option: Option in the config file
        :return: Float value
        """
        value = self.get_group(group).get(option, default_value)
        if value == "":
            return default_value
        try:
            return float(value)
        except ValueError:
            errors_in_config.append(f"Invalid value '{value}' for option '{option}' in group '{group}'.")
            return default_value
    
    
    def get_int(self, group: str, option: str, default_value: int = 0) -> int:
        """
        Get an integer value from the config file.
    
        :param group: Group in the config file
        :param option: Option in the config file
        :return: Integer value
        """
        value = self.get_group(group).get(option, default_value)
        if value == "":
            return default_value
        try:
            return int(value)
        except ValueError:
            errors_in_config.append(f"Invalid value '{value}' for option '{option}' in group '{group}'.")
            return default_value
    
    def get_list(self, group: str, option: str, mapper: Callable[[Any], Any] = lambda v: v) -> List[Any]:
        """
        Get a string with comma-separated values from the config file and return a list of values.
    
        :param group: Group in the config file
        :param option: Option in the config file
        :param mapper: Function to map the values to the correct type
        :return: List of values
        """
        try:
            raw_list = self.get_group(group).get(option.lower()).split(",")
            return [mapper(item.strip()) for item in raw_list if item.strip()]
        except KeyError:
            logger.error(f"Missing config option '{option}' in group '{group}'")
            errors_in_config.append(f"Missing config option '{option}' in group '{group}'")
            return []
        except ValueError:
            errors_in_config.append(f"Invalid value '{mapper}' for option '{option}' in group '{group}'.")
            return []
        
config = Config({})