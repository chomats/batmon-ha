import asyncio
import math
import queue
import random
import re
import sys
import time
from collections import defaultdict
from copy import copy
from typing import Optional, List, Dict

import paho.mqtt.client

from bmslib.algorithm import create_algorithm
from bmslib.bms import DeviceInfo, BmsSample, MIN_VALUE_EXPIRY
from bmslib.pwmath import Integrator, DiffAbsSum, LHQ
from bmslib.util import get_logger
from mqtt_util import publish_sample, publish_cell_voltages, publish_temperatures, publish_hass_discovery, \
    mqtt_single_out

logger = get_logger(verbose=False)


class SampleExpiredError(Exception):
    pass


class PeriodicBoolSignal:
    def __init__(self, period):
        self.period = period
        self._last_t = 0
        self.state = True

    def __bool__(self):
        return self.state

    def get(self):
        return self.state

    def set_time(self, t):
        if self._last_t == 0:
            self._last_t = t

        dt = t - self._last_t

        if dt < self.period:
            if self.state:
                self.state = False
        else:
            self._last_t = t
            self.state = True


class BmsSampleSink:
    """ Interface of an arbitrary data sink of battery samples """

    def publish_sample(self, bms_name: str, sample: BmsSample):
        raise NotImplementedError()

    def publish_voltages(self, bms_name: str, voltages: List[int]):
        raise NotImplementedError()

    def publish_meters(self, bms_name: str, readings: Dict[str, float]):
        raise NotImplementedError()


class BmsSampler:
    """
    Samples a single BMS and schedules publishing the samples to MQTT and arbitrary sinks.
    Also updates meters.
    """

    def __init__(self,
                 name:str,
                 address:int,
                 mqtt_client: paho.mqtt.client.Client,
                 dt_max_seconds,
                 expire_after_seconds,
                 invert_current=False,
                 meter_state=None,
                 publish_period=None,
                 sinks: Optional[List[BmsSampleSink]] = None,
                 algorithms: Optional[list] = None,
                 current_calibration_factor=1.0,
                 over_power=None,
                 publish_index=True,
                 verbose_log=False
                 ):

        self.mqtt_topic_prefix = re.sub(r'[^\w_.-/]', '_', name)
        self.mqtt_client = mqtt_client
        self.invert_current = invert_current
        self.expire_after_seconds = expire_after_seconds
        self.device_info: Optional[DeviceInfo] = None
        self.num_samples = 0
        self.current_calibration_factor = current_calibration_factor
        self.over_power = over_power or math.nan
        self.publish_index = publish_index

        self.sinks = sinks or []

        self.downsampler = Downsampler()

        self.period_pub = PeriodicBoolSignal(period=publish_period or 0)
        self.period_discov = PeriodicBoolSignal(60 * 5)
        self.period_30s = PeriodicBoolSignal(period=30)

        self._t_wd_reset = time.time()  # watchdog
        self._last_time_log = 0

        self._last_power = 0
        self._t_last_power_jump = 0

        self._num_errors = 0
        self._time_next_retry = 0
        self.name = name
        self.address = address
        self.verbose_log = verbose_log

        self.algorithm = None
        if algorithms:
            assert len(algorithms) == 1, "currently only 1 algo supported"
            algorithm = algorithms[0]
            self.algorithm = create_algorithm(algorithm, bms_name=self.name)

        dx_max = dt_max_seconds / 3600
        self.current_integrator = Integrator(name="total_charge", dx_max=dx_max)
        self.power_integrator = Integrator(name="total_energy", dx_max=dx_max)
        self.power_integrator_discharge = Integrator(name="total_energy_discharge", dx_max=dx_max)
        self.power_integrator_charge = Integrator(name="total_energy_charge", dx_max=dx_max)

        dx_max_diff = 3600 / 3600  # allow larger gabs for already integrated value
        self.cycle_integrator = DiffAbsSum(name="total_cycles", dx_max=dx_max_diff, dy_max=0.1)
        self.charge_integrator = DiffAbsSum(name="total_abs_diff_charge", dx_max=dx_max_diff, dy_max=0.5)
        # TODO normalize dy_max to capacity                                                         ^^^

        self.meters = [self.current_integrator, self.power_integrator, self.power_integrator_discharge,
                       self.power_integrator_charge, self.cycle_integrator, self.charge_integrator]

        for meter in self.meters:
            if meter_state and meter.name in meter_state:
                meter.restore(meter_state[meter.name]['reading'])

        self.queue = queue.Queue()
        # self.power_stats = EWM(span=120, std_regularisation=0.1)

        temp_step = 0
        temp_smooth = 10
        self._lhq_temp = defaultdict(lambda: LHQ(span=temp_smooth, inp_q=temp_step)) if temp_step else None

    def get_meter_state(self):
        return {meter.name: dict(reading=meter.get()) for meter in self.meters}

    def _filter_temperatures(self, temperatures):
        if not temperatures or self._lhq_temp is None:
            return temperatures
        return [round(self._lhq_temp[i].add(temperatures[i]), 2) for i in range(len(temperatures))]

    def put(self, sample: BmsSample):
        self.queue.put(sample)

    async def action_queue(self):
        while not self.queue.empty():
            sample = self.queue.get(block=False)
            try:
                await self.publish_sample(sample)
            except Exception as e:
                logger.error('exception in action callback: %s', e)
                await asyncio.sleep(1)
        
    async def publish_sample(self, sample):
        
        mqtt_client = self.mqtt_client

        # if not was_connected:
        #    self._num_errors = 0

        t_conn = time.time()

        err = False
        t_fetch = time.time()
        t_now = time.time()
        t_hour = t_now * (1 / 3600)

        if sample.timestamp < t_now - max(self.expire_after_seconds, MIN_VALUE_EXPIRY):
            raise SampleExpiredError("sample %s expired" % sample.timestamp)
            # logger.warning('%s expired sample', bms.name)
            # return

        sample.num_samples = self.num_samples

        if self.current_calibration_factor and self.current_calibration_factor != 1:
            sample = sample.multiply_current(self.current_calibration_factor)

        # discharging P>0
        self.power_integrator_charge += (t_hour, abs(min(0, sample.power)) * 1e-3)  # kWh
        self.power_integrator_discharge += (t_hour, abs(max(0, sample.power)) * 1e-3)  # kWh

        # self.power_stats.add(sample.power)
        sample.temperatures = self._filter_temperatures(sample.temperatures)

        if not math.isnan(sample.mos_temperature) and self._lhq_temp is not None:
            sample.mos_temperature = self._lhq_temp['mos'].add(sample.mos_temperature)

        if self.invert_current:
            sample = sample.invert_current()

        self.current_integrator += (t_hour, sample.current)  # Ah
        self.power_integrator += (t_hour, sample.power * 1e-3)  # kWh

        self.cycle_integrator += (t_hour, sample.soc * (0.01 / 2))  # SoC 100->0 is a half cycle
        self.charge_integrator += (t_hour, sample.charge)  # Ah
        self.downsampler += sample

        log_data = (t_now - self._last_time_log) >= (60 if self.num_samples < 1000 else 300) or self.verbose_log
        if log_data:
            self._last_time_log = t_now

        voltages = sample.voltages

        # z_score = self.power_stats.z_score(sample.power)
        # if abs(z_score) > 12:
        #    logger.info('%s Power z_score %.1f (avg=%.0f std=%.2f last=%.0f)', bms.name, z_score, self.power_stats.avg.value, self.power_stats.stddev, sample.power)

        PWR_CHG_REG = 120  # regularisation to suppress changes when power is low
        PWR_CHG_HOLD = 4
        power_chg = (sample.power - self._last_power) / (abs(self._last_power) + PWR_CHG_REG)
        if abs(power_chg) > 0.15 and abs(sample.power) > abs(self._last_power):
            if self.verbose_log or (
                    not self.period_pub and (t_now - self._t_last_power_jump) > PWR_CHG_HOLD):
                logger.info('%s Power jump %.0f %% (prev=%.0f last=%.0f, REG=%.0f)', self.name, power_chg * 100,
                            self._last_power, sample.power, PWR_CHG_REG)
            self._t_last_power_jump = t_now
        self._last_power = sample.power

        if self.period_discov or self.period_pub or \
                (t_now - self._t_last_power_jump) < PWR_CHG_HOLD or abs(sample.power) > self.over_power:
            self._t_pub = t_now

            sample = self.downsampler.pop()

            publish_sample(mqtt_client, device_topic=self.mqtt_topic_prefix, sample=sample)
            log_data and logger.info('%s: %s', self.name, sample)

            voltages = sample.voltages
            publish_cell_voltages(mqtt_client, device_topic=self.mqtt_topic_prefix, voltages=voltages, publish_index=self.publish_index, bms_name=self.name)

            # temperatures = None
            if self.period_30s or self.period_discov:
                if not sample.temperatures:
                    sample.temperatures = self._filter_temperatures(sample.temperatures)
                publish_temperatures(mqtt_client, device_topic=self.mqtt_topic_prefix,
                                     temperatures=sample.temperatures)

            if log_data and (voltages or sample.temperatures):
                logger.info('%s volt=[%s] temp=%s', self.name,
                            ','.join(map(str, voltages)) if voltages else voltages,
                            sample.temperatures)

        if self.period_discov or self.period_30s:
            self.publish_meters()

        # publish home assistant discovery every 60 samples
        if self.period_discov:
            logger.info("Sending HA discovery for %s (num_samples=%d)", self.name, self.num_samples)
            publish_hass_discovery(
                mqtt_client, device_topic=self.mqtt_topic_prefix,
                expire_after_seconds=self.expire_after_seconds,
                sample=sample,
                num_cells=len(voltages) if voltages else 0,
                temperatures=sample.temperatures,
                device_info=self.device_info,
            )

            # publish sample again after discovery
            if self.period_pub.period > 2:
                await asyncio.sleep(1)
                publish_sample(mqtt_client, device_topic=self.mqtt_topic_prefix, sample=sample)

        self.num_samples += 1
        t_disc = time.time()
        self._t_wd_reset = sample.timestamp or t_disc

        self.period_pub.set_time(t_now)
        self.period_30s.set_time(t_now)
        self.period_discov.set_time(t_now)

        dt_conn = t_fetch - t_conn
        dt_fetch = t_disc - t_fetch
        dt_max = max(dt_conn, dt_fetch)
        if self.verbose_log or (  # or dt_max > 1
                dt_max > 0.01 and random.random() < (0.05 if sample.num_samples < 1e3 else 0.01)
              and log_data):
            logger.info('%s times: connect=%.2fs fetch=%.2fs', self.name, dt_conn, dt_fetch)

        # pass "light" errors to the caller to trigger a re-connect after too many
        return sample if not err else None

    def publish_meters(self):
        device_topic = self.mqtt_topic_prefix
        for meter in self.meters:
            topic = f"{device_topic}/meter/{meter.name}"
            s = round(meter.get(), 3)
            mqtt_single_out(self.mqtt_client, topic, s)

        if self.sinks:
            readings = {m.name: m.get() for m in self.meters}
            for sink in self.sinks:
                try:
                    sink.publish_meters(self.name, readings)
                except NotImplementedError:
                    pass
                except:
                    logger.error(sys.exc_info(), exc_info=True)


class Downsampler:
    """ Averages multiple BmsSamples """

    def __init__(self):
        self._power = 0
        self._current = 0
        self._voltage = 0
        self._num = 0
        self._last: Optional[BmsSample] = None

    def __iadd__(self, s: BmsSample):
        self._power += s.power
        self._current += s.current
        self._voltage += s.voltage
        self._num += 1
        self._last = s
        return self

    def pop(self):
        if self._num == 0:
            return None

        if self._num == 1:
            return self._last

        n = 1 / self._num
        s = copy(self._last)

        if not math.isnan(s._power):
            s._power = self._power * n
        s.current = self._current * n
        s.voltage = self._voltage * n

        self._power = 0
        self._current = 0
        self._voltage = 0
        self._num = 0
        self._last = None

        return s
