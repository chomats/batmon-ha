import asyncio
import atexit
import os
import signal
import sys
import time
import traceback
from typing import List, Dict

import paho.mqtt.client
from paho.mqtt.enums import CallbackAPIVersion

import bmslib.bt
import mqtt_util
from bmslib.bms import MIN_VALUE_EXPIRY
from bmslib.models.jikong import s_decode_sample
from bmslib.sampling import BmsSampler
from bmslib.serialbattery.jkserialio import JKSerialIO, s_decode_O1
from bmslib.store import load_user_config
from bmslib.util import get_logger, exit_process
from mqtt_util import mqtt_last_publish_time, mqtt_message_handler, mqtt_process_action_queue

logger = get_logger(verbose=False)

user_config = load_user_config()

from bmslib.serialbattery.config import config
from bmslib.serialbattery.utils import set_pref_config

config.set_user_config(user_config)
set_pref_config(config)

shutdown = False
t_last_store = 0

DEV_TTY_USB_ = "/dev/ttyUSB0"

bms_list: Dict[int, BmsSampler] = {}

async def fetch_loop(fn, period, max_errors):
    num_errors_row = 0
    while not shutdown:
        try:
            if await fn():
                num_errors_row = 0
        except Exception as e:
            num_errors_row += 1
            logger.error('Error (num %d, max %d) reading BMS: %s', num_errors_row, max_errors, e)
            logger.error('Stack: %s', traceback.format_exc())
            if max_errors and num_errors_row > max_errors:
                logger.warning('too many errors, abort')
                break
        await asyncio.sleep(period)
    logger.info("fetch_loop %s ends", fn)


def store_states(samplers: List[BmsSampler]):
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
                logger.error("MQTT message publish timeout (last %.0fs ago), exit", pdt)
            else:
                logger.error("MQTT never published a message after %.0fs, exit", timeout)
            shutdown = True
            return False

    global t_last_store
    # store persistent states (metering) every 30s
    if now - (t_last_store or t_start) > 30:
        t_last_store = now
        try:
            store_states(sampler_list)
        except Exception as e:
            logger.error('Error storing states: %s', e)

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
        logger.info(sample)
        logger.info(be)
        logger.info(sample.trame_str)
        bms_sampler = bms_list.get(sample.address)
        if bms_sampler:
            await bms_sampler.put(sample) 
    elif len(data) >= 300 and data[4] == 1:
        logger.info(be)
        logger.info(bytes_to_printable(data))
        sampler = s_decode_O1(data)
        if sampler:
            logger.info(sampler)
    else:
        logger.debug(be)


async def main():
    global shutdown

    verbose_log = user_config.get('verbose_log', False)
    if verbose_log:
        logger.info('Verbose logging enabled')

    logger.info('Bleak version %s, BtBackend version %s', bmslib.bt.bleak_version(), bmslib.bt.bt_stack_version())

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
            logger.error('mqtt connection error %s', ex)

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

    sampler_list = []
    for bms in user_config.get('devices', []):
        name = bms['name']
        assert name not in names, "duplicate name %s" % name
        names.add(name)
        
        bms_s = BmsSampler(
            name=name, address=int(bms['address']), 
            mqtt_client=mqtt_client,
            verbose_log=verbose_log,
            dt_max_seconds=max(60. * 10, sample_period * 2),
            expire_after_seconds=expire_values_after and max(expire_values_after, int(sample_period * 2 + .5),
                                                             int(publish_period * 2 + .5)),
            invert_current=ic,
            meter_state=meter_states.get(name),
            publish_period=publish_period,
            algorithms=bms.get('algorithm') and bms.get('algorithm', '').split(";"),
            current_calibration_factor=float(bms.get('current_calibration', 1.0)),
            sinks=[],
            publish_index=publish_index
        )
        bms_list[bms_s.address] = bms_s
        sampler_list.append(bms_s)

    watchdog_en = user_config.get('watchdog', False)
    max_errors = 200 if watchdog_en else 0

    wd_timeout = max(5 * 60., sample_period * 4) if watchdog_en else 0
    asyncio.create_task(background_loop(
        timeout=wd_timeout,
        sampler_list=sampler_list
    ))

    jkIo = JKSerialIO(DEV_TTY_USB_, 115200)
    await jkIo.read_serial_data(None, mon_callback,308)

    logger.info('All fetch loops ended. shutdown is already %s', shutdown)
    shutdown = True

    store_states(sampler_list)


def on_exit(*args, **kwargs):
    global shutdown
    logger.info('exit signal handler... %s, %s, shutdown was %s', args, kwargs, shutdown)
    shutdown += 1
    bmslib.bt.BtBms.shutdown = True
    if shutdown == 5:
        sys.exit(1)


atexit.register(on_exit)
# noinspection PyTypeChecker
signal.signal(signal.SIGTERM, on_exit)
# noinspection PyTypeChecker
signal.signal(signal.SIGINT, on_exit)

try:
    asyncio.run(main())
except Exception as e:
    logger.error("Main loop exception: %s", e)
    logger.error("Stack: %s", traceback.format_exc())

sys.exit(1)
