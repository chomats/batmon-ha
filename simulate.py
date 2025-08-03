import asyncio
import logging
import sys
import traceback

from bmslib.serialbattery.jksimulateserialio import JKSimulateSerialIO
from bmslib.util import get_logger
import signal

logger = get_logger(verbose=True)
logger.addHandler(logging.StreamHandler(sys.stdout))

shutdown = False
jk_serial_io = JKSimulateSerialIO('/dev/pts/1', 115200)
logger.info('JKSimulateSerialIO over %s', '/dev/pts/1')

async def main():
    global shutdown
    await jk_serial_io.simulate_serial_data()

    logger.info('All fetch loops ended. shutdown is already %s', shutdown)
    shutdown = True

def on_exit(*args, **kwargs):
    global shutdown
    logger.info('exit signal handler... %s, %s, shutdown was %s', args, kwargs, shutdown)
    shutdown = True
    jk_serial_io.shutdown = True


signal.signal(signal.SIGQUIT, on_exit)
# signal.signal(signal.SIGSTOP, on_exit)
signal.signal(signal.SIGTERM, on_exit)
# noinspection PyTypeChecker
signal.signal(signal.SIGINT, on_exit)

try:
    logger.info('Starting simulate.py')   
    asyncio.run(main())
except Exception as e:
    logger.error("Main loop exception: %s", e)
    logger.error("Stack: %s", traceback.format_exc())

logging.shutdown()
sys.exit(1)