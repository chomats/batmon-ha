# -*- coding: utf-8 -*-
# Standard library imports
import bisect
from typing import List, Any, Union

# Third-party imports
import serial

from .config import Config, logger, errors_in_config

# CONSTANTS
DRIVER_VERSION: str = "2.1errors_in_config.202507230dev"
"""
current version of the driver
"""

ZERO_CHAR: str = chr(48)
"""

number zero (`0`)
"""

DEGREE_SIGN: str = "\N{DEGREE SIGN}"
"""
degree sign (`°`)
"""


def check_config_issue(condition: bool, message: str):
    """
    Check a condition and append a message to the errors_in_config list if the condition is True.

    :param condition: The condition to check
    :param message: The message to append if the condition is True
    """
    if condition:
        errors_in_config.append(f"{message}")


class UtilsConfig:
    def __init__(self, config: Config):
        # SAVE CONFIG VALUES to constants
        # --------- Battery Current Limits ---------
        self.MAX_BATTERY_CHARGE_CURRENT = config.get_float("DEFAULT", "MAX_BATTERY_CHARGE_CURRENT")
        """
        Defines the maximum charge current that the battery can accept.
        """
        self.MAX_BATTERY_DISCHARGE_CURRENT: float = config.get_float("DEFAULT", "MAX_BATTERY_DISCHARGE_CURRENT")
        """
        Defines the maximum discharge current that the battery can deliver.
        """
        
            
        
        # --------- Cell Voltages ---------
        self.MIN_CELL_VOLTAGE: float = config.get_float("DEFAULT", "MIN_CELL_VOLTAGE")
        """
        Defines the minimum cell voltage that the battery can have.
        Used for:
        - Limit CVL range
        - SoC calculation (if enabled)
        """
        self.MAX_CELL_VOLTAGE: float = config.get_float("DEFAULT", "MAX_CELL_VOLTAGE")
        """
        Defines the maximum cell voltage that the battery can have.
        Used for:
        - Limit CVL range
        - SoC calculation (if enabled)
        """
        self.FLOAT_CELL_VOLTAGE: float = config.get_float("DEFAULT", "FLOAT_CELL_VOLTAGE")
        """
        Defines the cell voltage that the battery should have when it is fully charged.
        """
        
        # make some checks for most common misconfigurations
        if self.FLOAT_CELL_VOLTAGE > self.MAX_CELL_VOLTAGE:
            check_config_issue(
                True,
                f"FLOAT_CELL_VOLTAGE ({self.FLOAT_CELL_VOLTAGE} V) is greater than MAX_CELL_VOLTAGE ({self.MAX_CELL_VOLTAGE} V). "
                + "To ensure that the driver still works correctly, FLOAT_CELL_VOLTAGE was set to MAX_CELL_VOLTAGE. Please check the configuration.",
                )
            self.FLOAT_CELL_VOLTAGE = self.MAX_CELL_VOLTAGE
        elif self.FLOAT_CELL_VOLTAGE < self.MIN_CELL_VOLTAGE:
            check_config_issue(
                True,
                "FLOAT_CELL_VOLTAGE ({FLOAT_CELL_VOLTAGE} V) is less than MIN_CELL_VOLTAGE ({MIN_CELL_VOLTAGE} V). "
                + "To ensure that the driver still works correctly, FLOAT_CELL_VOLTAGE was set to MIN_CELL_VOLTAGE. Please check the configuration.",
                )
            self.FLOAT_CELL_VOLTAGE = self.MIN_CELL_VOLTAGE
        
        
        # --------- SoC Reset Voltage (must match BMS settings) ---------
        self.SOC_RESET_CELL_VOLTAGE: float = config.get_float("DEFAULT", "SOC_RESET_CELL_VOLTAGE")
        self.SOC_RESET_AFTER_DAYS: Union[int, bool] = config.get_int("DEFAULT", "SOC_RESET_AFTER_DAYS") if config.get_option("DEFAULT", "SOC_RESET_AFTER_DAYS") != "" else False
        
        # make some checks for most common misconfigurations
        if self.SOC_RESET_AFTER_DAYS and self.SOC_RESET_CELL_VOLTAGE < self.MAX_CELL_VOLTAGE:
            check_config_issue(
                True,
                f"SOC_RESET_CELL_VOLTAGE ({self.SOC_RESET_CELL_VOLTAGE} V) is less than MAX_CELL_VOLTAGE ({self.MAX_CELL_VOLTAGE} V). "
                "To ensure that the driver still works correctly, SOC_RESET_CELL_VOLTAGE was set to MAX_CELL_VOLTAGE. Please check the configuration.",
            )
            self.SOC_RESET_CELL_VOLTAGE = self.MAX_CELL_VOLTAGE
        
        
        # --------- SoC Calculation ---------
        self.SOC_CALCULATION: bool = config.get_bool("DEFAULT", "SOC_CALCULATION")
        
        # --------- Current correction --------
        self.CURRENT_REPORTED_BY_BMS: list = config.get_list("DEFAULT", "CURRENT_REPORTED_BY_BMS", float)
        self.CURRENT_MEASURED_BY_USER: list = config.get_list("DEFAULT", "CURRENT_MEASURED_BY_USER", float)
        
        # check if lists are different
        # this allows to calculate linear relationship between the two lists only if needed
        self.CURRENT_CORRECTION: bool = self.CURRENT_REPORTED_BY_BMS != self.CURRENT_MEASURED_BY_USER
        
        # --------- Bluetooth BMS ---------
        self.BLUETOOTH_USE_POLLING = config.get_bool("DEFAULT", "BLUETOOTH_USE_POLLING")
        self.BLUETOOTH_FORCE_RESET_BLE_STACK = config.get_bool("DEFAULT", "BLUETOOTH_FORCE_RESET_BLE_STACK")
        
        # --------- Daisy Chain Configuration (Multiple BMS on one cable) ---------
        self.BATTERY_ADDRESSES: list = config.get_list("DEFAULT", "BATTERY_ADDRESSES", str)
        
        # --------- BMS Disconnect Behavior ---------
        self.BLOCK_ON_DISCONNECT: bool = config.get_bool("DEFAULT", "BLOCK_ON_DISCONNECT")
        self.BLOCK_ON_DISCONNECT_TIMEOUT_MINUTES: float = config.get_float("DEFAULT", "BLOCK_ON_DISCONNECT_TIMEOUT_MINUTES")
        self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN: float = config.get_float("DEFAULT", "BLOCK_ON_DISCONNECT_VOLTAGE_MIN")
        self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX: float = config.get_float("DEFAULT", "BLOCK_ON_DISCONNECT_VOLTAGE_MAX")
        
        # make some checks for most common misconfigurations
        if not self.BLOCK_ON_DISCONNECT:
            if self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN < self.MIN_CELL_VOLTAGE:
                check_config_issue(
                    True,
                    f"BLOCK_ON_DISCONNECT_VOLTAGE_MIN ({self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN} V) is less than MIN_CELL_VOLTAGE ({self.MIN_CELL_VOLTAGE} V). "
                    "To ensure that the driver still works correctly, BLOCK_ON_DISCONNECT_VOLTAGE_MIN was set to MIN_CELL_VOLTAGE. Please check the configuration.",
                )
                self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN = self.MIN_CELL_VOLTAGE
        
            if self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX > self.MAX_CELL_VOLTAGE:
                check_config_issue(
                    True,
                    f"BLOCK_ON_DISCONNECT_VOLTAGE_MAX ({self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX} V) is greater than MAX_CELL_VOLTAGE ({self.MAX_CELL_VOLTAGE} V). "
                    "To ensure that the driver still works correctly, BLOCK_ON_DISCONNECT_VOLTAGE_MAX was set to MAX_CELL_VOLTAGE. Please check the configuration.",
                )
                self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX = self.MAX_CELL_VOLTAGE
        
            if self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN >= self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX:
                check_config_issue(
                    True,
                    f"BLOCK_ON_DISCONNECT_VOLTAGE_MIN ({self.BLOCK_ON_DISCONNECT_VOLTAGE_MIN} V) "
                    f"is greater or equal to BLOCK_ON_DISCONNECT_VOLTAGE_MAX ({self.BLOCK_ON_DISCONNECT_VOLTAGE_MAX} V). "
                    "For safety reasons BLOCK_ON_DISCONNECT was set to True. Please check the configuration.",
                )
                self.BLOCK_ON_DISCONNECT = True
        
        
        # --------- External Sensor for Current and/or SoC ---------
        # EXTERNAL_SENSOR_DBUS_DEVICE: Union[str, None] = config["DEFAULT"]["EXTERNAL_SENSOR_DBUS_DEVICE"] or None
        # EXTERNAL_SENSOR_DBUS_PATH_CURRENT: Union[str, None] = config["DEFAULT"]["EXTERNAL_SENSOR_DBUS_PATH_CURRENT"] or None
        self.EXTERNAL_SENSOR_DBUS_PATH_SOC: Union[str, None] = config.get_option("DEFAULT", "EXTERNAL_SENSOR_DBUS_PATH_SOC") or None
        
        
        # Common configuration checks
        check_config_issue(
            self.SOC_CALCULATION and self.EXTERNAL_SENSOR_DBUS_PATH_SOC is not None,
            "SOC_CALCULATION and EXTERNAL_SENSOR_DBUS_PATH_SOC are both enabled. This will lead to a conflict. Please disable one of them in the configuration.",
            )
        
        
        # --------- Charge mode ---------
        self.CHARGE_MODE: int = config.get_int("DEFAULT", "CHARGE_MODE")
        self.CVL_RECALCULATION_EVERY: int = config.get_int("DEFAULT", "CVL_RECALCULATION_EVERY")
        self.CVL_RECALCULATION_ON_MAX_PERCENTAGE_CHANGE: int = config.get_int("DEFAULT", "CVL_RECALCULATION_ON_MAX_PERCENTAGE_CHANGE")
        
        
        # --------- Charge Voltage Limitation (affecting CVL) ---------
        self.CVCM_ENABLE: bool = config.get_bool("DEFAULT", "CVCM_ENABLE")
        """
        Charge voltage control management
        
        Limits max charging voltage (CVL). Switch from max to float voltage and back.
        """
        self.SWITCH_TO_FLOAT_WAIT_FOR_SEC: int = config.get_int("DEFAULT", "SWITCH_TO_FLOAT_WAIT_FOR_SEC", 0)
        self.SWITCH_TO_FLOAT_CELL_VOLTAGE_DIFF: float = config.get_float("DEFAULT", "SWITCH_TO_FLOAT_CELL_VOLTAGE_DIFF", 10)
        self.SWITCH_TO_FLOAT_CELL_VOLTAGE_DEVIATION: float = config.get_float("DEFAULT", "SWITCH_TO_FLOAT_CELL_VOLTAGE_DEVIATION", 0)

        self.SWITCH_TO_BULK_SOC_THRESHOLD: int = config.get_int("DEFAULT", "SWITCH_TO_BULK_SOC_THRESHOLD", 0)
        self.SWITCH_TO_BULK_CELL_VOLTAGE_DIFF: float = config.get_float("DEFAULT", "SWITCH_TO_BULK_CELL_VOLTAGE_DIFF", 10)
        
        
        # Common configuration checks
        if self.SWITCH_TO_BULK_SOC_THRESHOLD <= 0 and self.SWITCH_TO_BULK_CELL_VOLTAGE_DIFF >= 0.101:
            logger.warning(
                "Your current configuration very likely prevents the switch from FLOAT to BULK."
                f"SWITCH_TO_BULK_SOC_THRESHOLD is set to {self.SWITCH_TO_BULK_SOC_THRESHOLD} and "
                f"SWITCH_TO_BULK_CELL_VOLTAGE_DIFF is set to {self.SWITCH_TO_BULK_CELL_VOLTAGE_DIFF}."
            )
            logger.warning("Please check the configuration and adjust the values accordingly.")
        
        
        # --------- Cell Voltage Limitation (affecting CVL) ---------
        self.CVL_CONTROLLER_MODE: int = config.get_int("DEFAULT", "CVL_CONTROLLER_MODE")
        self.CVL_ICONTROLLER_FACTOR: float = config.get_float("DEFAULT", "CVL_ICONTROLLER_FACTOR")
        
        
        # --------- Cell Voltage Current Limitation (affecting CCL/DCL) ---------
        self.CCCM_CV_ENABLE: bool = config.get_bool("DEFAULT", "CCCM_CV_ENABLE")
        """
        Charge current control management referring to cell-voltage
        """
        self.DCCM_CV_ENABLE: bool = config.get_bool("DEFAULT", "DCCM_CV_ENABLE")
        """
        Discharge current control management referring to cell-voltage
        """
        self.CELL_VOLTAGES_WHILE_CHARGING: List[float] = config.get_list("DEFAULT", "CELL_VOLTAGES_WHILE_CHARGING", float)
        self.MAX_CHARGE_CURRENT_CV: List[float] = config.get_list("DEFAULT", "MAX_CHARGE_CURRENT_CV_FRACTION", lambda v: self.MAX_BATTERY_CHARGE_CURRENT * float(v))
        
        
        # Common configuration checks
        check_config_issue(
            self.CELL_VOLTAGES_WHILE_CHARGING[0] < self.MAX_CELL_VOLTAGE and self.MAX_CHARGE_CURRENT_CV[0] == 0,
            f"Maximum value of CELL_VOLTAGES_WHILE_CHARGING ({self.CELL_VOLTAGES_WHILE_CHARGING[0]} V) is lower than MAX_CELL_VOLTAGE ({self.MAX_CELL_VOLTAGE} V). "
            "MAX_CELL_VOLTAGE will never be reached this way and battery will not change to float. Please check the configuration.",
            )
        
        check_config_issue(
            self.SOC_RESET_AFTER_DAYS and self.CELL_VOLTAGES_WHILE_CHARGING[0] < self.SOC_RESET_CELL_VOLTAGE and self.MAX_CHARGE_CURRENT_CV[0] == 0,
            f"Maximum value of CELL_VOLTAGES_WHILE_CHARGING ({self.CELL_VOLTAGES_WHILE_CHARGING[0]} V) is lower than SOC_RESET_CELL_VOLTAGE ({self.SOC_RESET_CELL_VOLTAGE} V). "
            "SOC_RESET_CELL_VOLTAGE will never be reached this way and battery will not change to float. Please check the configuration.",
            )
        
        check_config_issue(
            self.MAX_BATTERY_CHARGE_CURRENT not in self.MAX_CHARGE_CURRENT_CV,
            f"In MAX_CHARGE_CURRENT_CV_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_CHARGE_CURRENT_CV_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum charge current. Please check the configuration.",
            )

        self.CELL_VOLTAGES_WHILE_DISCHARGING: List[float] = config.get_list("DEFAULT", "CELL_VOLTAGES_WHILE_DISCHARGING", float)
        self.MAX_DISCHARGE_CURRENT_CV: List[float] = config.get_list("DEFAULT", "MAX_DISCHARGE_CURRENT_CV_FRACTION", lambda v: self.MAX_BATTERY_DISCHARGE_CURRENT * float(v))
        
        check_config_issue(
            self.CELL_VOLTAGES_WHILE_DISCHARGING[0] > self.MIN_CELL_VOLTAGE and self.MAX_DISCHARGE_CURRENT_CV[0] == 0,
            f"Minimum value of CELL_VOLTAGES_WHILE_DISCHARGING ({self.CELL_VOLTAGES_WHILE_DISCHARGING[0]} V) is higher than MIN_CELL_VOLTAGE ({self.MIN_CELL_VOLTAGE} V). "
            "MIN_CELL_VOLTAGE will never be reached this way. Please check the configuration.",
            )
        
        check_config_issue(
            self.MAX_BATTERY_DISCHARGE_CURRENT not in self.MAX_DISCHARGE_CURRENT_CV,
            f"In MAX_DISCHARGE_CURRENT_CV_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_DISCHARGE_CURRENT_CV_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum discharge current. Please check the configuration.",
            )
        
        # --------- Temperature Limitation (affecting CCL/DCL) ---------
        self.CCCM_T_ENABLE: bool = config.get_bool("DEFAULT", "CCCM_T_ENABLE")
        """
        Charge current control management referring to temperature
        """
        self.DCCM_T_ENABLE: bool = config.get_bool("DEFAULT", "DCCM_T_ENABLE")
        """
        Discharge current control management referring to temperature
        """
        self.TEMPERATURES_WHILE_CHARGING: List[float] = config.get_list("DEFAULT", "TEMPERATURES_WHILE_CHARGING", float)
        self.MAX_CHARGE_CURRENT_T: List[float] = config.get_list("DEFAULT", "MAX_CHARGE_CURRENT_T_FRACTION", lambda v: self.MAX_BATTERY_CHARGE_CURRENT * float(v))
        
        check_config_issue(
            self.MAX_BATTERY_CHARGE_CURRENT not in self.MAX_CHARGE_CURRENT_T,
            f"In MAX_CHARGE_CURRENT_T_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_CHARGE_CURRENT_T_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum charge current. Please check the configuration.",
            )

        self.TEMPERATURES_WHILE_DISCHARGING: List[float] = config.get_list("DEFAULT", "TEMPERATURES_WHILE_DISCHARGING", float)
        self.MAX_DISCHARGE_CURRENT_T: List[float] = config.get_list("DEFAULT", "MAX_DISCHARGE_CURRENT_T_FRACTION", lambda v: self.MAX_BATTERY_DISCHARGE_CURRENT * float(v))
        
        check_config_issue(
            self.MAX_BATTERY_DISCHARGE_CURRENT not in self.MAX_DISCHARGE_CURRENT_T,
            f"In MAX_DISCHARGE_CURRENT_T_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_DISCHARGE_CURRENT_T_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum discharge current. Please check the configuration.",
            )
        
        # --------- MOSFET Temperature Current Limitation (affecting CCL/DCL) ---------
        self.CCCM_T_MOSFET_ENABLE: bool = config.get_bool("DEFAULT", "CCCM_T_MOSFET_ENABLE")
        """
        Charge current control management referring to MOSFET temperature
        """
        self.DCCM_T_MOSFET_ENABLE: bool = config.get_bool("DEFAULT", "DCCM_T_MOSFET_ENABLE")
        """
        Discharge current control management referring to MOSFET temperature
        """
        self.MOSFET_TEMPERATURES_WHILE_CHARGING: List[float] = config.get_list("DEFAULT", "MOSFET_TEMPERATURES_WHILE_CHARGING", float)
        self.MAX_CHARGE_CURRENT_T_MOSFET: List[float] = config.get_list(
            "DEFAULT", "MAX_CHARGE_CURRENT_T_MOSFET_FRACTION", lambda v: self.MAX_BATTERY_CHARGE_CURRENT * float(v)
        )
        
        check_config_issue(
            self.MAX_BATTERY_CHARGE_CURRENT not in self.MAX_CHARGE_CURRENT_T_MOSFET,
            f"In MAX_CHARGE_CURRENT_T_MOSFET_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_CHARGE_CURRENT_T_MOSFET_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum charge current. Please check the configuration.",
            )

        self.MOSFET_TEMPERATURES_WHILE_DISCHARGING: List[float] = config.get_list("DEFAULT", "MOSFET_TEMPERATURES_WHILE_DISCHARGING", float)
        self.MAX_DISCHARGE_CURRENT_T_MOSFET: List[float] = config.get_list(
            "DEFAULT", "MAX_DISCHARGE_CURRENT_T_MOSFET_FRACTION", lambda v: self.MAX_BATTERY_DISCHARGE_CURRENT * float(v)
        )
        
        check_config_issue(
            self.MAX_BATTERY_DISCHARGE_CURRENT not in self.MAX_DISCHARGE_CURRENT_T_MOSFET,
            f"In MAX_DISCHARGE_CURRENT_T_MOSFET_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_DISCHARGE_CURRENT_T_MOSFET_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum discharge current. Please check the configuration.",
            )
        
        # --------- SoC Limitation (affecting CCL/DCL) ---------
        self.CCCM_SOC_ENABLE: bool = config.get_bool("DEFAULT", "CCCM_SOC_ENABLE")
        """
        Charge current control management referring to SoC
        """
        self.DCCM_SOC_ENABLE: bool = config.get_bool("DEFAULT", "DCCM_SOC_ENABLE")
        """
        Discharge current control management referring to SoC
        """
        self.SOC_WHILE_CHARGING: List[float] = config.get_list("DEFAULT", "SOC_WHILE_CHARGING", float)
        self.MAX_CHARGE_CURRENT_SOC: List[float] = config.get_list("DEFAULT", "MAX_CHARGE_CURRENT_SOC_FRACTION", lambda v: self.MAX_BATTERY_CHARGE_CURRENT * float(v))
        
        check_config_issue(
            self.MAX_BATTERY_CHARGE_CURRENT not in self.MAX_CHARGE_CURRENT_SOC,
            f"In MAX_CHARGE_CURRENT_SOC_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_CHARGE_CURRENT_SOC_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum charge current. Please check the configuration.",
            )

        self.SOC_WHILE_DISCHARGING: List[float] = config.get_list("DEFAULT", "SOC_WHILE_DISCHARGING", float)
        self.MAX_DISCHARGE_CURRENT_SOC: List[float] = config.get_list(
            "DEFAULT", "MAX_DISCHARGE_CURRENT_SOC_FRACTION", lambda v: self.MAX_BATTERY_DISCHARGE_CURRENT * float(v)
        )
        
        check_config_issue(
            self.MAX_BATTERY_DISCHARGE_CURRENT not in self.MAX_DISCHARGE_CURRENT_SOC,
            f"In MAX_DISCHARGE_CURRENT_SOC_FRACTION ({', '.join(map(str, config.get_list('DEFAULT', 'MAX_DISCHARGE_CURRENT_SOC_FRACTION', float)))}) "
            "there is no value set to 1. This means that the battery will never use the maximum discharge current. Please check the configuration.",
            )
        
        
        # --------- CCL/DCL Recovery Threshold ---------
        self.CHARGE_CURRENT_RECOVERY_THRESHOLD_PERCENT: float = config.get_float("DEFAULT", "CHARGE_CURRENT_RECOVERY_THRESHOLD_PERCENT")
        """
        Defines the percentage of the maximum charge current that the battery has to reach to recover from a limitation.
        """
        self.DISCHARGE_CURRENT_RECOVERY_THRESHOLD_PERCENT: float = config.get_float("DEFAULT", "DISCHARGE_CURRENT_RECOVERY_THRESHOLD_PERCENT")
        """
        Defines the percentage of the maximum discharge current that the battery has to reach to recover from a limitation.
        """
        
        
        # --------- Time-To-Go ---------
        self.TIME_TO_GO_ENABLE: bool = config.get_bool("DEFAULT", "TIME_TO_GO_ENABLE")
        
        # --------- Time-To-Soc ---------
        self.TIME_TO_SOC_POINTS: List[int] = config.get_list("DEFAULT", "TIME_TO_SOC_POINTS", int)
        self.TIME_TO_SOC_VALUE_TYPE: int = config.get_int("DEFAULT", "TIME_TO_SOC_VALUE_TYPE")
        self.TIME_TO_SOC_RECALCULATE_EVERY: int = max(config.get_int("DEFAULT", "TIME_TO_SOC_RECALCULATE_EVERY"), 5)
        self.TIME_TO_SOC_INC_FROM: bool = config.get_bool("DEFAULT", "TIME_TO_SOC_INC_FROM")
        
        # --------- History ---------
        self.HISTORY_ENABLE: bool = config.get_bool("DEFAULT", "HISTORY_ENABLE")
        
        # --------- Additional settings ---------
        self.BMS_TYPE: List[str] = config.get_list("DEFAULT", "BMS_TYPE", str)
        self.EXCLUDED_DEVICES: List[str] = config.get_list("DEFAULT", "EXCLUDED_DEVICES", str)
        self.POLL_INTERVAL: Union[float, None] = config.get_float("DEFAULT", "POLL_INTERVAL") * 1000 if config.get_int("DEFAULT", "POLL_INTERVAL") else None
        """
        Poll interval in milliseconds
        """
        self.PUBLISH_CONFIG_VALUES: bool = config.get_bool("DEFAULT", "PUBLISH_CONFIG_VALUES")
        self.PUBLISH_BATTERY_DATA_AS_JSON: bool = config.get_bool("DEFAULT", "PUBLISH_BATTERY_DATA_AS_JSON")
        self.BATTERY_CELL_DATA_FORMAT: int = config.get_int("DEFAULT", "BATTERY_CELL_DATA_FORMAT")
        self.MIDPOINT_ENABLE: bool = config.get_bool("DEFAULT", "MIDPOINT_ENABLE")
        self.TEMPERATURE_SOURCE_BATTERY: List[int] = config.get_list("DEFAULT", "TEMPERATURE_SOURCE_BATTERY", int)
        self.TEMPERATURE_1_NAME: str = config.get_option("DEFAULT","TEMPERATURE_1_NAME")
        self.TEMPERATURE_2_NAME: str = config.get_option("DEFAULT","TEMPERATURE_2_NAME")
        self.TEMPERATURE_3_NAME: str = config.get_option("DEFAULT","TEMPERATURE_3_NAME")
        self.TEMPERATURE_4_NAME: str = config.get_option("DEFAULT","TEMPERATURE_4_NAME")
        self.TEMPERATURE_NAMES: dict = {
            1: self.TEMPERATURE_1_NAME,
            2: self.TEMPERATURE_2_NAME,
            3: self.TEMPERATURE_3_NAME,
            4: self.TEMPERATURE_4_NAME,
        }
        self.GUI_PARAMETERS_SHOW_ADDITIONAL_INFO: bool = config.get_bool("DEFAULT", "GUI_PARAMETERS_SHOW_ADDITIONAL_INFO")
        self.TELEMETRY: bool = config.get_bool("DEFAULT", "TELEMETRY")
        
        
        # --------- Voltage drop ---------
        self.VOLTAGE_DROP: float = config.get_float("DEFAULT", "VOLTAGE_DROP")
        
        # --------- BMS specific settings ---------
        self.USE_PORT_AS_UNIQUE_ID: bool = config.get_bool("DEFAULT", "USE_PORT_AS_UNIQUE_ID")
        self.BATTERY_CAPACITY: float = config.get_float("DEFAULT", "BATTERY_CAPACITY")
        self.AUTO_RESET_SOC: bool = config.get_bool("DEFAULT", "AUTO_RESET_SOC")
        self.USE_BMS_DVCC_VALUES: bool = config.get_bool("DEFAULT", "USE_BMS_DVCC_VALUES")
        
        # -- LltJbd settings
        self.SOC_LOW_WARNING: float = config.get_float("DEFAULT", "SOC_LOW_WARNING")
        self.SOC_LOW_ALARM: float = config.get_float("DEFAULT", "SOC_LOW_ALARM")
        
        # -- Daly settings
        self.INVERT_CURRENT_MEASUREMENT: int = config.get_int("DEFAULT", "INVERT_CURRENT_MEASUREMENT")
        
        # -- ESC GreenMeter and Lipro device settings
        self.GREENMETER_ADDRESS: int = config.get_int("DEFAULT", "GREENMETER_ADDRESS")
        self.LIPRO_START_ADDRESS: int = config.get_int("DEFAULT", "LIPRO_START_ADDRESS")
        self.LIPRO_END_ADDRESS: int = config.get_int("DEFAULT", "LIPRO_END_ADDRESS")
        self.LIPRO_CELL_COUNT: int = config.get_int("DEFAULT", "LIPRO_CELL_COUNT")
        
        # -- UBMS settings
        self.UBMS_CAN_MODULE_SERIES: int = config.get_int("DEFAULT", "UBMS_CAN_MODULE_SERIES")
        self.UBMS_CAN_MODULE_PARALLEL: int = config.get_int("DEFAULT", "UBMS_CAN_MODULE_PARALLEL")



def set_pref_config(config: Config):
    global PREF_CONFIG
    PREF_CONFIG = UtilsConfig(config)

def get_pref_config() -> UtilsConfig:
    return PREF_CONFIG

# FUNCTIONS
def constrain(val: float, min_val: float, max_val: float) -> float:
    """
    Constrain a value between a minimum and maximum value.

    :param val: Value to constrain
    :param min_val: Minimum value
    :param max_val: Maximum value
    :return: Constrained value
    """
    if min_val > max_val:
        min_val, max_val = max_val, min_val
    return min(max_val, max(min_val, val))


def map_range(in_value: float, in_min: float, in_max: float, out_min: float, out_max: float) -> float:
    """
    Map a value from one range to another.

    :param in_value: Input value
    :param in_min: Minimum value of the input range
    :param in_max: Maximum value of the input range
    :param out_min: Minimum value of the output range
    :param out_max: Maximum value of the output range
    :return: Mapped value
    """
    return out_min + (((in_value - in_min) / (in_max - in_min)) * (out_max - out_min))


def map_range_constrain(in_value: float, in_min: float, in_max: float, out_min: float, out_max: float) -> float:
    """
    Map a value from one range to another and constrain it between the output range.

    :param in_value: Input value
    :param in_min: Minimum value of the input range
    :param in_max: Maximum value of the input range
    :param out_min: Minimum value of the output range
    :param out_max: Maximum value of the output range
    :return: Mapped and constrained value
    """
    return constrain(map_range(in_value, in_min, in_max, out_min, out_max), out_min, out_max)


def calc_linear_relationship(in_value: float, in_array: List[float], out_array: List[float]) -> float:
    """
    Calculate a linear relationship between two arrays.

    :param in_value: Input value
    :param in_array: Input array
    :param out_array: Output array
    :return: Calculated value
    """
    # Change compare-direction in array
    if in_array[0] > in_array[-1]:
        return calc_linear_relationship(in_value, in_array[::-1], out_array[::-1])

    # Handle out of bounds
    if in_value <= in_array[0]:
        return out_array[0]
    if in_value >= in_array[-1]:
        return out_array[-1]

    # Calculate linear current between the setpoints
    idx = bisect.bisect(in_array, in_value)
    upper_in = in_array[idx - 1]
    upper_out = out_array[idx - 1]
    lower_in = in_array[idx]
    lower_out = out_array[idx]
    return map_range_constrain(in_value, lower_in, upper_in, lower_out, upper_out)


def calc_step_relationship(in_value: float, in_array: List[float], out_array: List[float], return_lower: bool) -> float:
    """
    Calculate a step relationship between two arrays.

    :param in_value: Input value
    :param in_array: Input array
    :param out_array: Output array
    :param return_lower: Return lower value if True, else return higher value
    :return: Calculated value
    """
    # Change compare-direction in array
    if in_array[0] > in_array[-1]:
        return calc_step_relationship(in_value, in_array[::-1], out_array[::-1], return_lower)

    # Handle out of bounds
    if in_value <= in_array[0]:
        return out_array[0]
    if in_value >= in_array[-1]:
        return out_array[-1]

    # Get index between the setpoints
    idx = bisect.bisect(in_array, in_value)
    return out_array[idx] if return_lower else out_array[idx - 1]


def is_bit_set(value: Any) -> bool:
    """
    Check if a bit is set high or low.

    :param value: Value to check
    :return: True if bit is set, False if not
    """
    return value != ZERO_CHAR


def kelvin_to_celsius(temperature: float) -> float:
    """
    Convert Kelvin to Celsius.

    :param temperature: Temperature in Kelvin
    :return: Temperature in Celsius
    """
    return temperature - 273.15


def bytearray_to_string(data: bytearray) -> str:
    """
    Convert a bytearray to a string.

    :param data: Data to convert
    :return: Converted string
    """
    return "".join(f"\\x{byte:02x}" for byte in data)


def get_connection_error_message(battery_online: bool, suffix: str = None) -> None:
    """
    This method is used to check if the connection to the BMS is successful.
    It returns True if the connection is successful, otherwise False.
    It also handles the error logging if the connection is lost.

    :battery_online: Boolean indicating if the battery is online
    :suffix: Optional suffix to add to the error message
    :return: True if the connection is successful, otherwise False
    """
    if battery_online is None:
        logger.info("  |- No battery recognized")
        return

    if battery_online:
        logger.error(">>> No response from battery. Connection lost or battery not recognized. Check cabeling!" + (" " + suffix if suffix else ""))
        return


def open_serial_port(port: str, baud: int) -> Union[serial.Serial, None]:
    """
    Open a serial port.

    :param port: Serial port
    :param baud: Baud rate
    :return: Opened serial port or None if failed
    """
    tries = 3
    while tries > 0:
        try:
            return serial.Serial(port, baudrate=baud, timeout=0.1)
        except serial.SerialException as e:
            logger.error(e)
            tries -= 1
    return None


def safe_number_format(value: float, fmt: str = "{:.2f}", default=None) -> str:
    """
    Format a value safely, returning a default value if the value is None.

    :param value: Value to format
    :param fmt: Format string (default: "{:.2f}")
    :param default: Default value to return if value is None
    :return: Formatted value or default value
    """
    return fmt.format(value) if value is not None else default


def validate_config_values() -> bool:
    """
    Validate the config values and log any issues.
    Has to be called in a function, otherwise the error messages are not instantly visible.

    :return: True if there are no errors else False
    """
    # Add empty line for better readability
    if len(errors_in_config) > 0:
        logger.error("")
        logger.error("*** CONFIG ISSUES DETECTED ***")

    # loop through all errors and log them
    for error in errors_in_config:
        logger.error("- " + error)

    # return True if there are no errors
    if len(errors_in_config) == 0:
        return True
    else:
        logger.error("The driver may not behave as expected due to the above issues.")
        logger.error(">>> Please check the CHANGELOG.md for option changes and the config.default.ini for all available options!")
        logger.error("")
        return False
