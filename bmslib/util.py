import logging
import os
import random
import string
import time
from logging.handlers import TimedRotatingFileHandler


class dotdict(dict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError as e:
            raise AttributeError(e)

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    # __hasattr__ = dict.__contains__

loggers = {}

def get_logger(verbose=False, name="main", log_file_name="batmon-ha.jkbms-app.log"):
    global loggers
    if name in loggers:
        return loggers[name]
    
    # log_format = '%(asctime)s %(levelname)-6s [%(filename)s:%(lineno)d] %(message)s'
    log_format = '%(asctime)s %(levelname)s %(name)s [%(module)s] %(message)s'
    formatter = logging.Formatter(log_format)
    # set TimedRotatingFileHandler for root
    # use very short interval for this example, typical 'when' would be 'midnight' and no explicit interval
    handler = TimedRotatingFileHandler(log_file_name, when="H", interval=1, backupCount=10)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name) # or pass string to give it a name
    logger.addHandler(handler)
    if verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger.setLevel(level)
    loggers[name] = logger
    return logger

def get_logger_child(child_name, verbose=False):
    if child_name == "main":
        return get_logger()
    return get_logger().getChild(child_name)

def get_logger_err():
    return get_logger(name="error", log_file_name="batmon-ha.jkbms-app-err.log")

def set_log_levels(log_levels):
    logger = get_logger_child("logging")
    for name, level in log_levels.items():
        child_log = get_logger_child(name)
        logger.info("Set log level for %s to %s", child_log.name, level)
        child_log.setLevel(logging.getLevelName(level))

def dict_to_short_string(d: dict):
    return '(' + ','.join(f'{k}={v}' for k, v in d.items() if v is not None) + ')'


def to_hex_str(data):
    return " ".join(f"{b:02X}" for b in data)

def exit_process(is_error=True, delayed=False):
    from threading import Thread
    import _thread
    status = 1 if is_error else 0
    Thread(target=lambda: (time.sleep(3), _thread.interrupt_main()), daemon=True).start()
    Thread(target=lambda: (time.sleep(6), os._exit(status)), daemon=True).start()
    if not delayed:
        import sys
        sys.exit(status)


def _id_generator(size=6, chars=string.ascii_uppercase + string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def sid_generator(n=2):
    assert n >= 2
    return _id_generator(n-1, string.ascii_lowercase + string.ascii_uppercase) + _id_generator(1, string.digits)

def read_str(buf, offset, encoding='utf-8'):
    return buf[offset:buf.index(0x00, offset)].decode(encoding=encoding)