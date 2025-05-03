import logging
import sys
import unittest
from time import sleep

from bmslib.sampling import PeriodicBoolSignal
from bmslib.util import get_logger_child

logger = get_logger_child("test_period")
class TestPeriod(unittest.TestCase):
    """ unit tests of jk02 protocol """
    maxDiff = None

    def test_priode(self):
        log_format = '%(asctime)s %(levelname)s %(name)s [%(module)s] %(message)s'
        formatter = logging.Formatter(log_format)
        steamHandlerOut = logging.StreamHandler(sys.stdout)
        steamHandlerOut.setFormatter(formatter)
        logger.addHandler(steamHandlerOut)
        p = PeriodicBoolSignal(period=10)
        while True:
            if p:
                logger.info('period is true')
            if p.counter > 10:
                break
            sleep(5)
            p.set_time()
            