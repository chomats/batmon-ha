import asyncio
import atexit
import os
import random
import signal
import sys
import threading
import time
import traceback
from typing import List, Dict

import paho.mqtt.client
from paho.mqtt.enums import CallbackAPIVersion

import bmslib.bt
import mqtt_util
from bmslib.bms import MIN_VALUE_EXPIRY
from bmslib.group import BmsGroup, VirtualGroupBms
from bmslib.models import construct_bms
from bmslib.models.jikong import s_decode_sample
from bmslib.sampling import BmsSampler
from bmslib.serialbattery.jkbms_pb import Jkbms_pb, COMMAND_STATUS_BYTES
from bmslib.serialbattery.jkserialio2 import JKSerialIO
from bmslib.store import load_user_config
from bmslib.util import get_logger, exit_process
from mqtt_util import mqtt_last_publish_time, mqtt_message_handler, mqtt_process_action_queue

DEV_TTY_USB_ = "/dev/ttyUSB2"

logger = get_logger(verbose=False)

user_config = load_user_config()

from bmslib.serialbattery.config import config
from bmslib.serialbattery.utils import set_pref_config

config.set_user_config(user_config)
set_pref_config(config)

shutdown = False
t_last_store = 0

def bytes_to_printable(data: bytes) -> str:
    return ''.join(chr(b) if chr(b).isprintable() else '.' for b in data)

def mon_callback(data, crc=None):
    be = ' '.join(format(x, '02x') for x in data)
    if len(data) >= 300 and data[4] == 2:
        sample = s_decode_sample(is_new_11fw_32s=True,
                        logger=logger,
                        num_cells=16,
                        buf_set=None,
                        buf=data, t_buf=time.time(), has_float_charger=True)
        logger.info(sample)
        logger.info(be)
        logger.info(sample.trame_str)
    elif len(data) >= 300 and data[4] == 1:
        logger.info(be)
        logger.info(bytes_to_printable(data))
    else:
        logger.debug(be)


jkIo = JKSerialIO(DEV_TTY_USB_, 115200)
jkIo.read_serial_data(None, mon_callback,308)
#55 aa eb 90 02 00 3c 0d 3b 0d 38 0d 38 0d 3c 0d 3b 0d 3b 0d 3b 0d 3b 0d 37 0d 37 0d 3a 0d 3c 0d 37 0d 38 0d 3c 0d 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ff ff 00 00 3a 0d 05 00 00 02 3d 00 38 00 41 00 47 00 57 00 5b 00 64 00 69 00 7f 00 80 00 6b 00 5f 00 5f 00 52 00 49 00 3b 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 eb 00 00 00 00 00 9d d3 00 00 00 00 00 00 00 00 00 00 de 00 e2 00 00 00 00 00 00 00 00 63 92 3a 04 00 c0 45 04 00 09 00 00 00 c7 75 28 00 64 00 00 00 82 e9 b0 00 01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ff 00 01 00 00 00 93 03 00 00 00 00 35 55 3f 40 00 00 00 00 29 15 00 00 00 01 01 01 00 06 01 00 1c 61 d1 04 00 00 00 00 eb 00 de 00 e1 00 90 03 83 89 86 0a 4c 00 00 00 80 51 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 fe ff 7f dc 2f 01 01 b0 cf 07 00 00 35
# 
# jkbms_pb2 = Jkbms_pb(DEV_TTY_USB_, 115200, "2")
# jkbms_pb2.read_serial_data_jkbms_pb(COMMAND_STATUS_BYTES, 308)
# 
# 
# jkbms_pb3 = Jkbms_pb(DEV_TTY_USB_, 115200, "2")
# jkbms_pb3.read_serial_data_jkbms_pb(COMMAND_STATUS_BYTES, 308)
