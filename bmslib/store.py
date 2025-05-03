import json
import os
import re
from os import access, R_OK
from os.path import isfile
from threading import Lock

from bmslib.cache import random_str
from bmslib.util import dotdict, get_logger_child

def is_readable(file):
    return isfile(file) and access(file, R_OK)


root_dir = '/data/' if is_readable('/data/options.json') else ''
bms_meter_states_fn = root_dir + 'bms_meter_states.json'

lock = Lock()


def store_file(fn):
    return root_dir + fn


def load_meter_states():
    with lock:
        with open(bms_meter_states_fn) as f:
            meter_states = json.load(f)
        return meter_states


def store_meter_states(meter_states):
    with lock:
        s = f'.{random_str(6)}.tmp'
        with open(bms_meter_states_fn + s, 'w') as f:
            json.dump(meter_states, f, indent=2)
        os.replace(bms_meter_states_fn + s, bms_meter_states_fn)


def store_algorithm_state(bms_name, algorithm_name, state=None):
    fn = root_dir + 'bat_state_' + re.sub(r'[^\w_. -]', '_', bms_name) + '.json'
    with lock:
        with open(fn, 'a+') as f:
            try:
                f.seek(0)
                bms_state = json.load(f)
            except:
                logger = get_logger_child("store")
                logger.info('init %s bms state storage', bms_name)
                bms_state = dict(algorithm_state=dict())

            if state is not None:
                bms_state['algorithm_state'][algorithm_name] = state
                f.seek(0), f.truncate()
                json.dump(bms_state, f, indent=2)

            return bms_state['algorithm_state'].get(algorithm_name, None)

g_conf = None

def get_user_config(file_name='options.json'):
    global g_conf
    if g_conf is None:
        g_conf = _load_user_config(file_name)
    return g_conf

def _load_user_config(file_name='options.json'):
    with open(file_name) as f:
        conf = dotdict(json.load(f))
    return conf


def user_config_migrate_addresses(conf):
    changed = False
    slugs = ["daly", "jbd", "jk", "sok", "victron"]
    conf["devices"] = conf.get('devices') or []
    devices_by_address = {d['address']: d for d in conf["devices"]}
    logger = get_logger_child("store")
    for slug in slugs:
        addr = conf.get(f'{slug}_address')
        if addr and not devices_by_address.get(addr):
            device = dict(
                address=addr.strip('?'),
                type=slug,
                alias=slug + '_bms',
            )
            if addr.endswith('?'):
                device["debug"] = True
            if conf.get(f'{slug}_pin'):
                device['pin'] = conf.get(f'{slug}_pin')
            conf["devices"].append(device)
            del conf[f'{slug}_address']
            logger.info('Migrated %s_address to device %s', slug, device)
            changed = True
    if changed:
        logger.info('Please update add-on configuration manually.')
