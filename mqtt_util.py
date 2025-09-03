"""

HA mdi: icons https://pictogrammers.com/library/mdi/


"""
import asyncio
import json
import math
import queue
import statistics
import time
import traceback

import paho.mqtt.client as paho

from bmslib.bms import BmsSample, DeviceInfo, MIN_VALUE_EXPIRY, BmsSetSwitch
from bmslib.util import get_logger_child, get_logger_err

logger = get_logger_child("mqtt")
logger_err = get_logger_err()

no_publish_fail_warn = False


def round_to_n(x, n):
    """
    Rounds a given number `x` to `n` significant digits. Returns the formatted number
    as a string. It gracefully handles cases when `x` is non-finite, a string, 
    or zero. If the provided `n` is zero, the number is rounded to its nearest 
    integer as a string.

    :param x: The number to round. Must be a float or integer.
    :type x: float | int
    :param n: The number of significant digits to which `x` will be rounded.
    :type n: int
    :return: A string representation of `x`, rounded to `n` significant digits.
    :rtype: str
    :raises ValueError: If an invalid value is encountered during rounding.
    """
    if isinstance(x, str) or not math.isfinite(x) or not x:
        return x

    if n == 0:
        return str(round(x, None))

    digits = -int(math.floor(math.log10(abs(x)))) + (n - 1)

    try:
        # return ('%.*f' % (digits, x))
        return str(round(x, digits or None))  # digits=0 will output 12.0, digits=None => 12
    except ValueError as e:
        print('error', x, n, e)
        raise e


def disable_warnings():
    """
    Disable warning notifications for publishing failures.

    This function sets the global flag ``no_publish_fail_warn``
    to ``True``, which disables warning notifications regarding
    publishing failures. 

    :return: None
    """
    global no_publish_fail_warn
    no_publish_fail_warn = True


def remove_none_values(fields: dict):
    """
    Removes keys from a dictionary where values are None, non-finite floats (NaN or
    infinity), or empty strings. This function mutates the provided dictionary
    in-place and eliminates entries with invalid or undesired values.

    :param fields: A dictionary where keys map to values that will be evaluated 
        for removal based on their validity.
    :type fields: dict

    :return: None
    """
    for k in list(fields.keys()):
        v = fields[k]
        if v is None:
            del fields[k]
        elif isinstance(v, float):
            if math.isnan(v) or not math.isfinite(v):
                del fields[k]
        elif isinstance(v, str):
            if not v:
                del fields[k]

_last_values = {}
_last_publish_time = 0.


def mqtt_single_out(client: paho.Client, topic, data, retain=True):
    # logger.debug(f'Send data: {data} on topic: {topic}, retain flag: {retain}')
    # print('mqtt: ' + topic, data)
    # return
    if client is None:
        logger.warning('mqtt publish %s no client', topic)
        return False

    lv = _last_values.get(topic, None)
    now = time.time()
    if lv and lv[1] == data and (now - lv[0]) < (MIN_VALUE_EXPIRY / 2):
        return False

    mqi: paho.MQTTMessageInfo = client.publish(topic, data, retain=retain)
    if mqi.rc != paho.MQTT_ERR_SUCCESS:
        if not no_publish_fail_warn:
            logger.warning('mqtt publish %s failed: %s %s', topic, mqi.rc, mqi)
        return False

    _last_values[topic] = now, data
    global _last_publish_time
    _last_publish_time = now
    return True


def mqtt_last_publish_time():
    global _last_publish_time
    return _last_publish_time


def is_none_or_nan(val):
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or not math.isfinite(val)):
        return True
    return False


# units: https://github.com/home-assistant/core/blob/d7ac4bd65379e11461c7ce0893d3533d8d8b8cbf/homeassistant/const.py#L384
sample_desc = {
    "soc/total_voltage": {
        "field": "voltage",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 4,
        "accuracy_decimals": 2,
        "icon": "meter-electric"},
    "soc/current": {
        "field": "current",
        "device_class": "current",
        "state_class": "measurement",
        "unit_of_measurement": "A",
        "precision": 4},
    "soc/balance_current": {
        "field": "balance_current",
        "device_class": "current",
        "state_class": "measurement",
        "unit_of_measurement": "A",
        "precision": 4,
        "icon": "scale-unbalanced"},
    "soc/soc_percent": {
        "field": "soc",
        "device_class": "battery",
        "state_class": None,
        "unit_of_measurement": "%",
        "precision": 4,
        "icon": "battery"},
    "soc/power": {
        "field": "power",
        "device_class": "power",
        "state_class": "measurement",
        "unit_of_measurement": "W",
        "precision": 3,
        "icon": "flash"},
    "soc/power_ui": {
        "field": "power_ui",
        "device_class": "power",
        "state_class": "measurement",
        "unit_of_measurement": "W",
        "precision": 3,
        "icon": "flash"},
    "soc/capacity": {
        "field": "capacity",
        "device_class": None,
        "state_class": None,
        "unit_of_measurement": "Ah"
    },
    "soc/cycle_capacity": {
        "field": "cycle_capacity",
        "device_class": None,
        "state_class": None,
        "unit_of_measurement": "Ah"},
    "soc/num_cycles": {
        "field": "num_cycles",
        "device_class": None,
        "state_class": "measurement",
        "unit_of_measurement": "N",
        "icon": "battery-sync"},
    "mosfet_status/capacity_ah": {
        "field": "charge",
        "device_class": None,
        "state_class": None,
        "unit_of_measurement": "Ah"},
    "mosfet_status/temperature": {
        "field": "mos_temperature",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "icon": "thermometer"},
    "bms/uptime": {
        "field": "uptime",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "icon": "clock"},
    "bms/alarm": {
        "field": "alarm",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "precision": 0},
    "bms/balance_line_resistance_status": {
        "field": "balance_line_resistance_status",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "precision": 0},
    "meter/sample_count": {
        "field": "num_samples",
        "device_class": None,
        "state_class": "measurement",
        "unit_of_measurement": "N",
        "icon": "counter"},
    "temperatures/min": {
        "field": "temp_min",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "icon": "thermometer"},
    "temperatures/moyenne": {
        "field": "temp_moyenne",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "icon": "thermometer"},
    "temperatures/max": {
        "field": "temp_max",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "icon": "thermometer"},
}
sample_setting_desc = {
    "setting/vol_smart_sleep": {
        "field": "vol_smart_sleep",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_cell_uv": {
        "field": "vol_cell_uv",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_cell_uvpr": {
        "field": "vol_cell_uvpr",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_cell_ov": {
        "field": "vol_cell_ov",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_cell_ovpr": {
        "field": "vol_cell_ovpr",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_balan_trig": {
        "field": "vol_balan_trig",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_soc_full": {
        "field": "vol_soc_full",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_soc_empty": {
        "field": "vol_soc_empty",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_rcv": {
        "field": "vol_rcv",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_rfv": {
        "field": "vol_rfv",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },
    "setting/vol_sys_pwr_off": {
        "field": "vol_sys_pwr_off",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    },

    "setting/cell_count": {
        "field": "cell_count",
        "device_class": None,
        "state_class": "measurement",
        "unit_of_measurement": "N",
        "icon": "counter"},
    "setting/capacity": {
        "field": "capacity",
        "device_class": None,
        "state_class": None,
        "unit_of_measurement": "Ah"
    },
    "setting/max_battery_charge_current": {
        "field": "max_battery_charge_current",
        "device_class": "current",
        "state_class": "measurement",
        "unit_of_measurement": "A",
        "precision": 2,
        "accuracy_decimals": 2
    },
    "setting/max_battery_discharge_current": {
        "field": "max_battery_discharge_current",
        "device_class": "current",
        "state_class": "measurement",
        "unit_of_measurement": "A",
        "precision": 2,
        "accuracy_decimals": 2
    },
    "setting/cur_balan_max": {
        "field": "cur_balan_max",
        "device_class": "current",
        "state_class": "measurement",
        "unit_of_measurement": "A",
        "precision": 2,
        "accuracy_decimals": 2
    },
    "setting/tmp_bat_cot": {
        "name": "Charging over-temperature protection",
        "field": "tmp_bat_cot",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_bat_cotpr": {
        "name": "Charging over-temperature recovery",
        "field": "tmp_bat_cotpr",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_bat_dc_ot": {
        "name": "Discharging over-temperature protection",
        "field": "tmp_bat_dc_ot",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_bat_dc_otpr": {
        "name": "Discharging over-temperature recovery",
        "field": "tmp_bat_dc_otpr",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_bat_cut": {
        "name": "Charging low-temperature protection",
        "field": "tmp_bat_cut",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_bat_cutpr": {
        "name": "Charging low-temperature recovery",
        "field": "tmp_bat_cutpr",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_mos_ot": {
        "name": "Mosfet over-temperature protection",
        "field": "tmp_mos_ot",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tmp_mos_otpr": {
        "name": "Mosfet over-temperature recovery",
        "field": "tmp_mos_otpr",
        "device_class": "temperature",
        "state_class": "measurement",
        "unit_of_measurement": "°C",
        "precision": 1,
        "accuracy_decimals": 1
    },
    "setting/tim_bat_cocp_dly": {
        "field": "tim_bat_cocp_dly",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/tim_bat_cocpr_dly": {
        "field": "tim_bat_cocpr_dly",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/tim_bat_dc_ocp_dly": {
        "field": "tim_bat_dc_ocp_dly",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/tim_bat_dc_ocpr_dly": {
        "field": "tim_bat_dc_ocpr_dly",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/tim_bat_scpr_dly": {
        "field": "tim_bat_scpr_dly",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/scp_delay": {
        "field": "scp_delay",
        "device_class": "duration",
        "state_class": "measurement",
        "unit_of_measurement": "s",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "setting/start_bal_vol": {
        "field": "start_bal_vol",
        "device_class": "voltage",
        "state_class": "measurement",
        "unit_of_measurement": "V",
        "precision": 3,
        "accuracy_decimals": 2
    }
}

alarm_desc = {
    "alarm/cell_imbalance": {
        "field": "cell_imbalance",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/low_soc": {
        "field": "low_soc",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_internal_temperature": {
        "field": "high_internal_temperature",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_voltage": {
        "field": "high_voltage",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/low_voltage": {
        "field": "low_voltage",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_charge_current": {
        "field": "high_charge_current",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_discharge_current": {
        "field": "high_discharge_current",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_cell_voltage": {
        "field": "high_cell_voltage",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/low_cell_voltage": {
        "field": "low_cell_voltage",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_charge_temperature": {
        "field": "high_charge_temperature",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/low_charge_temperature": {
        "field": "low_charge_temperature",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/high_temperature": {
        "field": "high_temperature",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
    "alarm/low_temperature": {
        "field": "low_temperature",
        "device_class": "problem",
        "state_class": None,
        "unit_of_measurement": "",
        "entity_category": "diagnostic",
        "precision": 0,
        "accuracy_decimals": 0
    },
}

def publish_sample_with_desc(client, device_topic, desc, sample, is_problem=False):
    """
    Publishes a sample with a detailed description to the specified device topic.

    The method iterates over the provided description dictionary and constructs
    a topic for each key by appending it to the base device topic. The sample
    is processed and rounded according to the precision specified in the
    description. Non-NaN and non-None values are then sent to the MQTT broker
    via a single outgoing message.

    :param is_problem: if is a problem
    :param client: The MQTT client instance used for publishing messages.
    :param device_topic: The base topic name for the device.
    :param desc: A dictionary containing key-value pairs specifying the details
        for publishing. Each key maps to a dictionary that must include the
        sample field to extract (indicated by 'field') and optionally the
        rounding precision (indicated by 'precision').
    :param sample: The sample object containing the data to be published.
    :return: None
    """
    for k, v in desc.items():
        topic = f"{device_topic}/{k}"
        val = getattr(sample, v['field'])

        if is_problem or v.get('device_class', '') == 'problem':
            s = 'OFF' if val==0 else 'Unknown' if val is None else 'ON'
        else:
            s = round_to_n(val, v.get('precision', 5))

        if not is_none_or_nan(s):
            if mqtt_single_out(client, topic, s):
                logger.debug("publish_sample %s: %s", topic, f"{s}")

def publish_sample(client, device_topic, sample: BmsSample):
    """
    Publishes a BmsSample object to an MQTT client along with its switches and settings.

    This function handles publishing the provided `BmsSample` object to a specified
    MQTT client and topic. It publishes the sample description, switch states, and 
    settings as applicable.

    :param client: The MQTT client to publish messages to.
    :type client: MQTT client object
    :param device_topic: The base topic for the MQTT device.
    :type device_topic: str
    :param sample: The `BmsSample` object containing the data to be published.
    :type sample: BmsSample
    :return: None
    """
    publish_sample_with_desc(client, device_topic, sample_desc, sample)
    setting = sample.setting
    switch_states = sample.switches
    if setting:
        publish_sample_with_desc(client, device_topic, sample_setting_desc, setting)
        if not switch_states:
            switch_states= setting.switches

    if switch_states:
        for switch_name, switch_state in switch_states.items():
            assert isinstance(switch_state, bool)
            topic = f"{device_topic}/switch/{switch_name}"
            logger.debug("publish_sample %s: %s", topic, f"{switch_state}")
            mqtt_single_out(client, topic, 'ON' if switch_state else 'OFF')
    protection = sample.protection
    if protection:
        publish_sample_with_desc(client, device_topic, alarm_desc, protection, True)

def publish_cell_voltages(client, device_topic, voltages, publish_index, bms_name):
    """
    Publishes cell voltage data to specific MQTT topics for a given battery management system (BMS). 

    The function processes and publishes cell voltage information including individual cell voltages, 
    minimum and maximum voltages, voltage delta, average, total, and median values. Additionally, 
    it optionally publishes the index positions of cells with the minimum and maximum voltages. 
    The voltages are expressed in volts by converting the input values provided in millivolts.

    :param client: The MQTT client instance used for publishing messages.
    :type client: Any
    :param device_topic: The base topic string identifying the device in the MQTT system.
    :type device_topic: str
    :param voltages: A list of integers representing cell voltages in millivolts.
    :type voltages: list[float]
    :param publish_index: A boolean indicating whether to publish the indices of the cells 
                          with minimum and maximum voltages.
    :type publish_index: bool
    :param bms_name: The name or identifier for the battery management system used in debug logging.
    :type bms_name: str
    :return: None
    :rtype: NoneType
    """

    if not voltages:
        return

    for i in range(0, len(voltages)):
        topic = f"{device_topic}/cell_voltages/{i + 1}"
        mqtt_single_out(client, topic, voltages[i] / 1000)

    if len(voltages) > 1:
        x = range(len(voltages))
        high_i = max(x, key=lambda i: voltages[i])
        low_i = min(x, key=lambda i: voltages[i])
        voltage_min = voltages[low_i] / 1000
        voltage_max = voltages[high_i] / 1000
        voltage_delta = (voltages[high_i] - voltages[low_i]) / 1000
        logger.debug("%s publish_cell_voltages (%s, %s, %s)", bms_name, f"{voltage_min}", f"{voltage_max}", f"{voltage_delta}")

        if publish_index:
            mqtt_single_out(client, f"{device_topic}/cell_voltages/min_index", low_i + 1)
            mqtt_single_out(client, f"{device_topic}/cell_voltages/max_index", high_i + 1)

        mqtt_single_out(client, f"{device_topic}/cell_voltages/min", voltage_min)
        mqtt_single_out(client, f"{device_topic}/cell_voltages/max", voltage_max)
        mqtt_single_out(client, f"{device_topic}/cell_voltages/delta", voltage_delta)
        mqtt_single_out(client, f"{device_topic}/cell_voltages/average", round(sum(voltages) / len(voltages)) / 1000)
        mqtt_single_out(client, f"{device_topic}/cell_voltages/total", round(sum(voltages))/1000)
        mqtt_single_out(client, f"{device_topic}/cell_voltages/median", statistics.median(voltages) / 1000)


def publish_temperatures(client, device_topic, temperatures):
    for i in range(0, len(temperatures)):
        topic = f"{device_topic}/temperatures/{i + 1}"
        if not is_none_or_nan(temperatures[i]):
            mqtt_single_out(client, topic, round_to_n(temperatures[i], 4))


def publish_hass_discovery(client, device_topic, expire_after_seconds: int, sample: BmsSample, num_cells,
                           temperatures,
                           device_info: DeviceInfo = None):
    discovery_msg = {}

    device_json = {
        "identifiers": [(device_info and device_info.sn) or device_topic],
        "manufacturer": (device_info and device_info.mnf) or None,
        "name": f"{device_info.name} ({device_topic})" if (device_info and device_info.name) else device_topic,
        "model": (device_info and device_info.model) or None,
        "sw_version": (device_info and device_info.sw_version) or None,
        "hw_version": (device_info and device_info.hw_version) or None,
    }

    def _hass_discovery(k, device_class, unit, state_class=None, icon=None, name=None, long_expiry=False, precision_value=None, type="sensor"):
        dm = {
            "unique_id": f"{device_topic}__{k.replace('/', '_')}",
            "name": name or k.replace('/', ' '),
            "device_class": device_class or None,
            "state_class": state_class or None,
            "unit_of_measurement": unit,
            # "json_attributes_topic": f"{device_topic}/{k}",
            "state_topic": f"{device_topic}/{k}",
            "expire_after": max(expire_after_seconds, 3600 * 2) if long_expiry else expire_after_seconds,
            "device": device_json,
            "suggested_display_precision": precision_value,
        }

        if type=='sensor' and device_class == 'problem':
            type = 'binary_sensor'

        if icon:
            dm['icon'] = 'mdi:' + icon
        remove_none_values(dm)
        remove_none_values(dm['device'])
        discovery_msg[f"homeassistant/{type}/{device_topic}/_{k.replace('/', '_')}/config"] = dm

    def publish_hass_discovery_with_desc(desc, setting, no_name=False, type="sensor"):
        for k, d in desc.items():
            if not is_none_or_nan(getattr(setting, d["field"])):
                device_name = d.get('name', None if no_name else d["field"])
                _hass_discovery(k, d["device_class"], state_class=d["state_class"], unit=d["unit_of_measurement"],
                                icon=d.get('icon', None), name=device_name, precision_value=d.get('precision', None), type=type)

    publish_hass_discovery_with_desc(sample_desc, sample)

    switch_states = sample.switches
    setting = sample.setting
    if setting:
        publish_hass_discovery_with_desc(sample_setting_desc, setting)
        if not switch_states:
            switch_states= setting.switches
    
    protection = sample.protection
    if protection:
        publish_hass_discovery_with_desc(alarm_desc, protection, True, type="binary_sensor")

    for i in range(0, num_cells):
        k = 'cell_voltages/%d' % (i + 1)
        n = 'Cell Volt %0*d' % (1 + int(math.log10(num_cells)), i + 1)
        _hass_discovery(k, device_class="voltage", state_class= "measurement", name=n, unit="V", precision_value=3)

    if num_cells > 1:
        statistic_fields = ["min", "max", "average", "median", "delta", "total"]
        for f in statistic_fields:
            k = 'cell_voltages/%s' % f
            _hass_discovery(k, name="Cell Volt %s" % f, device_class="voltage", state_class= "measurement", unit="V", precision_value=3)

        for f in ["min_index", "max_index"]:
            k = 'cell_voltages/%s' % f
            _hass_discovery(k, name="Cell Index %s" % f[:3], device_class=None, unit="", precision_value=0)

    for i in range(0, len(temperatures)):
        k = 'temperatures/%d' % (i + 1)
        if not is_none_or_nan(temperatures[i]):
            _hass_discovery(k, device_class="temperature", state_class= "measurement", unit="°C", precision_value=2)

    meters = {
        # state_class see https://developers.home-assistant.io/docs/core/entity/sensor/#long-term-statistics
        # this enables the meters to appear in HA Energy Grid
        'total_energy': dict(device_class="energy", unit="kWh", icon="meter-electric"),  # state_class="total",
        'total_energy_charge': dict(device_class="energy", state_class="total_increasing", unit="kWh",
                                    icon="meter-electric"),
        'total_energy_discharge': dict(device_class="energy", state_class="total_increasing", unit="kWh",
                                       icon="meter-electric"),
        'total_charge': dict(device_class=None, unit="Ah"),
        'total_cycles': dict(device_class=None, unit="N", icon="battery-sync"),
    }
    for name, m in meters.items():
        _hass_discovery('meter/%s' % name, **m, name=name.replace('_', ' ') + " meter", long_expiry=True)

    switches = (switch_states and switch_states.keys())
    if switches:
        for switch_name in switches:
            discovery_msg[f"homeassistant/switch/{device_topic}/{switch_name}/config"] = {
                "unique_id": f"{device_topic}__switch_{switch_name}",
                "name": f"{switch_name}",
                "device_class": 'outlet',
                # "json_attributes_topic": f"{device_topic}/{switch_name}",
                "state_topic": f"{device_topic}/switch/{switch_name}",
                "expire_after": expire_after_seconds,
                "device": device_json,
                "command_topic": f"homeassistant/switch/{device_topic}/{switch_name}/set",
            }

            discovery_msg[f"homeassistant/binary_sensor/{device_topic}/{switch_name}/config"] = {
                "unique_id": f"{device_topic}__switch_{switch_name}",
                "name": f"{switch_name} switch",
                "device_class": 'power',
                # "json_attributes_topic": f"{device_topic}/{switch_name}",
                "expire_after": expire_after_seconds,
                "device": device_json,
                "state_topic": f"{device_topic}/switch/{switch_name}",
                "command_topic": f"homeassistant/switch/{device_topic}/{switch_name}/set",
            }

    for topic, data in discovery_msg.items():
        j = json.dumps(data)
        logger.debug('discovery msg %s: %s', topic, j)
        mqtt_single_out(client, topic, j)
        

_switch_callbacks = {}
_message_queue = queue.Queue()


async def mqtt_process_action_queue():
    while not _message_queue.empty():
        callback, arg = _message_queue.get(block=False)
        try:
            await callback(arg)
        except Exception as e:
            logger_err.error('exception in action callback: %s', e)
            logger_err.error('Stack: %s', traceback.format_exc())
            await asyncio.sleep(1)


def subscribe_switches(mqtt_client: paho.Client, device_topic, bms: BmsSetSwitch, switches):
    """
    Subscribe to MQTT topics to control switches and define callbacks for handling 
    incoming MQTT messages. The function establishes the relationship between MQTT 
    and switch operations on the given device.

    :param mqtt_client: MQTT client responsible for broker communication
    :type mqtt_client: paho.Client
    :param device_topic: Base MQTT topic associated with the device
    :type device_topic: str
    :param bms: Battery Management System instance for controlling switches
    :type bms: BmsSetSwitch
    :param switches: List of switch names to be subscribed to
    :type switches: dict_keys[str, bool]
    :return: None
    :rtype: None
    """
    async def set_switch(switch_name: str, state: bool):
        assert isinstance(state, bool)
        logger.debug('Set %s %s switch %s', bms.get_name(), switch_name, state)
        await bms.set_switch(switch_name, state)
        topic = f"{device_topic}/switch/{switch_name}"
        mqtt_single_out(mqtt_client, topic, 'ON' if state else 'OFF')

    for switch_name in switches:
        state_topic = f"homeassistant/switch/{device_topic}/{switch_name}/set"
        logger.debug("subscribe %s", state_topic)
        mqtt_client.subscribe(state_topic, qos=2)
        _switch_callbacks[state_topic] = \
            lambda msg, sn=switch_name: set_switch(sn, msg.lower() == "on")


def mqtt_message_handler(client, userdata, message: paho.MQTTMessage):
    """
    Handles incoming MQTT messages, decodes the payload, and routes the payload to
    a pre-defined callback based on the message topic. If no callback is defined
    for the topic, a warning is logged. This function is part of an MQTT client
    workflow.

    :param client: The MQTT client instance calling this function.
    :type client: Any
    :param userdata: The private user data associated with the MQTT client.
    :type userdata: Any
    :param message: The incoming MQTT message containing topic and payload data.
                    It must be an instance of `paho.MQTTMessage`.
    :type message: paho.MQTTMessage
    :return: None
    """
    payload = message.payload.decode("utf-8")
    logger.info("received msg %s: %s", message.topic, payload)
    callback = _switch_callbacks.get(message.topic, None)
    if callback:
        _message_queue.put((callback, payload))
    else:
        logger.warning("No callback for topic %s (payload %s)", message.topic, payload)


def paho_monkey_patch():
    def _handle_pingresp(self):
        """
        Handles the PINGRESP packet to ensure correct protocol behavior and logs the response.

        This function processes an incoming PINGRESP MQTT control packet, verifying
        that the packet is constructed correctly. If the packet contains an invalid
        remaining length, a protocol error is returned. Otherwise, it logs the PINGRESP
        packet and concludes the ping request process.

        :param self: Reference to the current instance of the class invoking the method.
        :return: MQTT error status code indicating the result of the handling
            process (either `paho.MQTT_ERR_PROTOCOL` or `paho.MQTT_ERR_SUCCESS`).
        :rtype: int
        """
        if self._in_packet['remaining_length'] != 0:
            return paho.MQTT_ERR_PROTOCOL

        # No longer waiting for a PINGRESP.
        # self._ping_t = 0
        self._easy_log(paho.MQTT_LOG_DEBUG, "Received PINGRESP (patched)")
        return paho.MQTT_ERR_SUCCESS

    paho.Client._handle_pingresp = _handle_pingresp

    logger.debug("applied paho monkey patch _handle_pingresp")
