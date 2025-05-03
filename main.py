import argparse
import asyncio
import atexit
import logging
import os
import signal
import sys
import time
import traceback
from typing import List, Dict

import paho.mqtt.client
from paho.mqtt.enums import CallbackAPIVersion

import mqtt_util
from bmslib.bms import MIN_VALUE_EXPIRY, SettingsData
from bmslib.sampling import BmsSampler
from bmslib.serialbattery.jkserialio import JKSerialIO, s_decode_O1, decode_info, JKSerialIOBmsSetSwitch, \
    s_decode_sample
from bmslib.store import get_user_config
from bmslib.util import get_logger, exit_process, get_logger_err, get_logger_child, set_log_levels
from mqtt_util import mqtt_last_publish_time, mqtt_message_handler, mqtt_process_action_queue

parser = argparse.ArgumentParser(
    prog="jkbms2A16",
    description="A mqtt sending for jkbms via serial",
)
parser.add_argument(
    "-m", "--master", action='store_true', help="The mode master or slave")
parser.add_argument(
    "-p", "--port", default="/dev/ttyUSB0", type=str, help="The TCP port to listen on"
)
parser.add_argument(
    "-c", "--config", default="options.json", type=str, help="The TCP port to listen on"
)
parser.add_argument("-s", "--skip-discovery", action='store_true', help="skip discovery")

parser.add_argument(
    "-l", "--verbose", action='store_true', help="verbose log"
)
args = parser.parse_args()

logger_root = get_logger(verbose=args.verbose)

user_config = get_user_config(args.config)

if user_config.get('console_log', False):
    log_format = '%(asctime)s %(levelname)s %(name)s [%(module)s] %(message)s'
    formatter = logging.Formatter(log_format)
    steamHandlerOut = logging.StreamHandler(sys.stdout)
    steamHandlerOut.setFormatter(formatter)
    logger_root.addHandler(steamHandlerOut)
    steamHandlerErr = logging.StreamHandler(sys.stderr)
    steamHandlerErr.setFormatter(formatter)
    get_logger_err().addHandler(steamHandlerOut)

logger_err = get_logger_err().getChild("main")
logger = get_logger_child("main")
logger_callback = get_logger_child("callback")
logger_callback.setLevel(logging.INFO)

log_levels = user_config.get('log_levels')
if log_levels:
    set_log_levels(log_levels)
logger.info("finish configuration of logging")

verbose_log = user_config.get('verbose_log', args.verbose)

shutdown = False
t_last_store = 0

DEV_TTY_USB_ = user_config.get("serial_port", "/dev/pts/2")
logger.info("Starting main.py with %s", DEV_TTY_USB_)

bms_list_by_ad: Dict[int, BmsSampler] = {}

def store_states(samplers: List[BmsSampler]):
    """
    Store the state of multiple samplers by retrieving their meter states
    and delegating the storage of these states to an external function. It
    aggregates the meter states of all given samplers into a dictionary
    with sampler names as keys.

    :param samplers: List of BmsSampler objects from which meter states
        are to be retrieved.
    :type samplers: List[BmsSampler]
    :return: None. The function does not return any value.
    """
    meter_states = {s.name: s.get_meter_state() for s in samplers}
    from bmslib.store import store_meter_states
    store_meter_states(meter_states)

def bg_checks(sampler_list, timeout, t_start):
    global shutdown

    now = time.time()

    if timeout:
        # compute time since last successful publish
        pdt = now - (mqtt_last_publish_time() or t_start)
        if pdt > timeout:
            if mqtt_last_publish_time():
                logger_err.error("MQTT message publish timeout (last %.0fs ago), exit", pdt)
            else:
                logger_err.error("MQTT never published a message after %.0fs, exit", timeout)
            shutdown = True
            return False

    global t_last_store
    # store persistent states (metering) every 30s
    if now - (t_last_store or t_start) > 30:
        t_last_store = now
        try:
            store_states(sampler_list)
        except Exception as e:
            logger_err.error('Error storing states: %s', e)

    return True


def background_thread(timeout: float, sampler_list: List[BmsSampler]):
    t_start = time.time()
    while not shutdown:
        if not bg_checks(sampler_list, timeout, t_start):
            break
        time.sleep(4)
    logger.info("Background thread ends. shutdown=%s", shutdown)
    time.sleep(10)
    logger.info("Process still alive, suicide")
    exit_process(True, True)


async def background_loop(timeout: float, sampler_list: List[BmsSampler]):
    global shutdown

    t_start = time.time()

    if timeout:
        logger.info("mqtt watchdog loop started with timeout %.1fs", timeout)

    while not shutdown:
        for bms_s in sampler_list:
            await bms_s.action_queue()
        await mqtt_process_action_queue()
        if not bg_checks(sampler_list, timeout, t_start):
            break

        await asyncio.sleep(.1)   

def bytes_to_printable(data: bytes) -> str:
    return ''.join(chr(b) if chr(b).isprintable() else '.' for b in data)

async def mon_callback(data, crc=None):
    be = ' '.join(format(x, '02x') for x in data)
    if len(data) >= 300 and data[4] == 2:
        sample = s_decode_sample(is_new_11fw_32s=True,
                                 logger=logger,
                                 num_cells=16,
                                 buf_set=None,
                                 buf=data, t_buf=time.time(), has_float_charger=True)
        logger_callback.info(sample)
        logger_callback.debug(be)
        logger_callback.debug(sample.trame_str)
        bms_sampler = bms_list_by_ad.get(sample.address)
        if bms_sampler:
            bms_sampler.put(sample) 
    elif len(data) >= 300 and data[4] == 1:
        logger_callback.debug(be)
        logger_callback.debug(bytes_to_printable(data))
        setting:SettingsData = s_decode_O1(data)
        if setting:
            logger_callback.info(setting)
            bms_sampler = bms_list_by_ad.get(setting.address)
            if bms_sampler:
                bms_sampler.set_setting(setting)
    elif len(data) >= 300 and data[4] == 3:
        logger_callback.debug(be)
        logger_callback.debug(bytes_to_printable(data))
        info = decode_info(data, logger)
        logger_callback.info(info)
        if info:
            bms_sampler = bms_list_by_ad.get(info.address)
            if bms_sampler:
                bms_sampler.set_info(info)
    else:
        logger_callback.info("trame? ")
        logger_callback.info(be)


if args.port:
    DEV_TTY_USB_ = args.port

jk_serial_io = JKSerialIO(DEV_TTY_USB_, 115200, count_bat=2, master=args.master)

async def main():
    global shutdown

    if verbose_log:
        logger.info('Verbose logging enabled')

    names = set()

    # import env vars from addon_main.sh
    for k, en in dict(mqtt_broker='MQTT_HOST', mqtt_user='MQTT_USER', mqtt_password='MQTT_PASSWORD').items():
        if not user_config.get(k) and os.environ.get(en):
            user_config[k] = os.environ[en]

    if user_config.get('mqtt_broker'):
        port_idx = user_config.mqtt_broker.rfind(':')
        if port_idx > 0:
            user_config.mqtt_port = user_config.get('mqtt_port', int(user_config.mqtt_broker[(port_idx + 1):]))
            user_config.mqtt_broker = user_config.mqtt_broker[:port_idx]

        logger.info('connecting mqtt %s@%s', user_config.mqtt_user, user_config.mqtt_broker)
        # paho_monkey_patch()
        mqtt_client = paho.mqtt.client.Client(CallbackAPIVersion.VERSION2)
        mqtt_client.enable_logger(logger)
        if user_config.get('mqtt_user', None):
            mqtt_client.username_pw_set(user_config.mqtt_user, user_config.mqtt_password)

        mqtt_client.on_message = mqtt_message_handler

        try:
            mqtt_client.connect(user_config.mqtt_broker, port=int(user_config.get('mqtt_port', 1883)))
            mqtt_client.loop_start()
        except Exception as ex:
            logger_err.error('mqtt connection error %s', ex)

        if not user_config.mqtt_broker:
            mqtt_util.disable_warnings()
    else:
        mqtt_client = None

    from bmslib.store import load_meter_states
    try:
        meter_states = load_meter_states()
    except FileNotFoundError:
        logger.info("Initialize meter states file")
        meter_states = {}
    except Exception as e:
        logger.warning('Failed to load meter states: %s', e)
        meter_states = {}

    sample_period = float(user_config.get('sample_period', 1.0))
    publish_period = float(user_config.get('publish_period', sample_period))
    expire_values_after = float(user_config.get('expire_values_after', MIN_VALUE_EXPIRY))
    ic = user_config.get('invert_current', False)
    publish_index = user_config.get('publish_index', True)

    # serial
    serial_sampler_list = []
    for device in user_config.get('devices', []):
        name = device['name']
        assert name not in names, "duplicate name %s" % name
        names.add(name)

        address= int(device['address_serial'])
        bms_s = BmsSampler(
            name=name, address=address, 
            mqtt_client=mqtt_client,
            verbose_log=verbose_log,
            dt_max_seconds=max(60. * 10, sample_period * 2),
            expire_after_seconds=expire_values_after and max(expire_values_after, int(sample_period * 2 + .5),
                                                             int(publish_period * 2 + .5)),
            invert_current=ic,
            meter_state=meter_states.get(name),
            publish_period=publish_period,
            algorithms=device.get('algorithm') and device.get('algorithm', '').split(";"),
            current_calibration_factor=float(device.get('current_calibration', 1.0)),
            publish_index=publish_index,
            bms_set_switch_delegate=JKSerialIOBmsSetSwitch(address=address, jk_serial_io=jk_serial_io)
        )
        bms_s.bms_set_switch_delegate.set_bms_sampler(bms_s)
    
        bms_list_by_ad[bms_s.address] = bms_s
        serial_sampler_list.append(bms_s)
    
    watchdog_en = user_config.get('watchdog', False)

    wd_timeout = max(5 * 60., sample_period * 4) if watchdog_en else 0
    asyncio.create_task(background_loop(
        timeout=wd_timeout,
        sampler_list=serial_sampler_list
    ))

    jk_serial_io.count_bat = len(serial_sampler_list)
    await jk_serial_io.read_serial_data(None, mon_callback,308)

    logger.info('All fetch loops ended. shutdown is already %s', shutdown)
    shutdown = True

    store_states(serial_sampler_list)

def on_exit(*args, **kwargs):
    global shutdown
    logger.info('exit signal handler... %s, %s, shutdown was %s', args, kwargs, shutdown)
    shutdown += 1
    jk_serial_io.shutdown = True
    if shutdown == 5:
        logging.shutdown()
        sys.exit(1)


atexit.register(on_exit)
# noinspection PyTypeChecker

signal.signal(signal.SIGQUIT, on_exit)
# signal.signal(signal.SIGSTOP, on_exit)
signal.signal(signal.SIGTERM, on_exit)
# noinspection PyTypeChecker
signal.signal(signal.SIGINT, on_exit)

try:
    asyncio.run(main())
except Exception as e:
    logger_err.error("Main loop exception: %s", e)
    logger_err.error("Stack: %s", traceback.format_exc())

logging.shutdown()
sys.exit(1)
