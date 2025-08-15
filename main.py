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

import bmslib.bt
import mqtt_util
from bmslib.bms import MIN_VALUE_EXPIRY, SettingsData
from bmslib.group import BmsGroup, VirtualGroupBms
from bmslib.models import construct_bms
from bmslib.models.jikong import s_decode_sample
from bmslib.sampling import BmsSampler
from bmslib.serialbattery.jkserialio import JKSerialIO, s_decode_O1
from bmslib.store import load_user_config
from bmslib.util import get_logger, exit_process
from mqtt_util import mqtt_last_publish_time, mqtt_message_handler, mqtt_process_action_queue
from bmslib.serialbattery.config import config
from bmslib.serialbattery.utils import set_pref_config


logger = get_logger(verbose=False)
user_config = load_user_config()

if user_config.get('console_log', False):
    logger.addHandler(logging.StreamHandler(sys.stdout))

config.set_user_config(user_config)
set_pref_config(config)

verbose_log = user_config.get('verbose_log', False)
if verbose_log:
    logger.setLevel(logging.DEBUG)

shutdown = False
t_last_store = 0

DEV_TTY_USB_ = user_config.get("serial_port", "/dev/pts/2")
logger.info("Starting main.py with %s", DEV_TTY_USB_)

bms_list_by_ad: Dict[int, BmsSampler] = {}

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
        logger.debug(be)
        logger.debug(sample.trame_str)
        bms_sampler = bms_list_by_ad.get(sample.address)
        if bms_sampler:
            bms_sampler.put(sample) 
    elif len(data) >= 300 and data[4] == 1:
        logger.debug(be)
        logger.debug(bytes_to_printable(data))
        setting:SettingsData = s_decode_O1(data)
        if setting:
            logger.info(setting)
            bms_sampler = bms_list_by_ad.get(setting.address)
            if bms_sampler:
                bms_sampler.set_setting(setting)
    else:
        logger.debug(be)

jk_serial_io = JKSerialIO(DEV_TTY_USB_, 115200)

async def main():
    global shutdown

    pair_only = len(sys.argv) > 1 and sys.argv[1] == "pair-only"
    if pair_only:
        logger.info('Started in pair-only mode (bleak %s)', bmslib.bt.bleak_version())
        psks = set(dev.get('psk', None) for dev in user_config.get('devices', []) if dev.get('psk', None))
        if not psks:
            logger.info('No PSK, nothing to pair')
            sys.exit(0)

    bms_list: List[bmslib.bt.BtBms] = []
    extra_tasks = []  # currently unused, add custom coroutines here. must return True on success and can raise

    if user_config.get('bt_power_cycle'):
        try:
            logger.info('Power cycle bluetooth hardware')
            bmslib.bt.bt_power(False)
            await asyncio.sleep(1)
            bmslib.bt.bt_power(True)
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning("Error power cycling BT: %s", e)

    try:
        if len(sys.argv) > 1 and sys.argv[1] == "skip-discovery":
            raise Exception("skip-discovery")
        devices = await asyncio.wait_for(bmslib.bt.bt_discovery(logger), 60)
    except Exception as e:
        devices = []
        logger.error('Error discovering devices: %s', e)

    if verbose_log:
        logger.info('Verbose logging enabled')

    logger.info('Bleak version %s, BtBackend version %s', bmslib.bt.bleak_version(), bmslib.bt.bt_stack_version())

    names = set()
    dev_args: Dict[str, dict] = {}

    for dev in user_config.get('devices', []):

        if dev.get('is_serial', False):
            continue
        bms = construct_bms(dev, verbose_log, devices)

        if bms is None:
            logger.info("Skip %s", dev)
            continue

        name = bms.name
        assert name not in names, "duplicate name %s" % name

        bms_list.append(bms)
        names.add(name)
        dev_args[name] = dev

    bms_by_name: Dict[str, bmslib.bt.BtBms] = {
        **{bms.address: bms for bms in bms_list if not bms.is_virtual},
        **{bms.name: bms for bms in bms_list}}
    groups_by_bms: Dict[str, BmsGroup] = {}

    for bms in bms_list:
        bms.set_keep_alive(user_config.get('keep_alive', False))

        if isinstance(bms, VirtualGroupBms):
            group_bms = bms
            for member_ref in bms.get_member_refs():
                if member_ref not in bms_by_name:
                    logger.warning('Please choose one of these names: %s', set(bms_by_name.keys()))
                    raise Exception("unknown bms '%s' in group %s" % (member_ref, group_bms))

                member_name = bms_by_name[member_ref].name
                if member_name in groups_by_bms:
                    raise Exception("can't add bms %s to multiple groups %s %s", member_name,
                                    groups_by_bms[member_name], group_bms)
                
                groups_by_bms[member_name] = group_bms.group
                bms.add_member(bms_by_name[member_ref])

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

    sinks = []
    if user_config.get('influxdb_host', None):
        try:
            from bmslib.sinks import InfluxDBSink
            sinks.append(InfluxDBSink(**{k[9:]: v for k, v in user_config.items() if k.startswith('influxdb_')}))
        except Exception as e:
            logger.warning('Failed to load influxdb sink: %s', e)

    if user_config.get("telemetry"):
        try:
            from bmslib.sinks import TelemetrySink
            sinks.append(TelemetrySink(bms_by_name=bms_by_name))
        except:
            logger.warning("failed to init telemetry", exc_info=True)

    # serial
    serial_sampler_list = []
    for bms in user_config.get('devices', []):
        name = bms['name']
        if not bms.get('is_serial', False):
            continue
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
        bms_list_by_ad[bms_s.address] = bms_s
        serial_sampler_list.append(bms_s)

    watchdog_en = user_config.get('watchdog', False)
    max_errors = 200 if watchdog_en else 0

    wd_timeout = max(5 * 60., sample_period * 4) if watchdog_en else 0
    asyncio.create_task(background_loop(
        timeout=wd_timeout,
        sampler_list=serial_sampler_list
    ))

    
    
    await jk_serial_io.read_serial_data(None, mon_callback,308)

    logger.info('All fetch loops ended. shutdown is already %s', shutdown)
    shutdown = True

    store_states(serial_sampler_list)


def on_exit(*args, **kwargs):
    global shutdown
    logger.info('exit signal handler... %s, %s, shutdown was %s', args, kwargs, shutdown)
    shutdown += 1
    bmslib.bt.BtBms.shutdown = True
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
    logger.error("Main loop exception: %s", e)
    logger.error("Stack: %s", traceback.format_exc())

logging.shutdown()
sys.exit(1)
