import asyncio
import math
import queue
import re
import time
from collections import defaultdict
from copy import copy
from typing import Optional

import paho.mqtt.client

from bmslib.algorithm import create_algorithm, BatterySwitches
from bmslib.bms import DeviceInfo, BmsSample, MIN_VALUE_EXPIRY, BmsSetSwitch
from bmslib.pwmath import Integrator, DiffAbsSum, LHQ
from bmslib.util import get_logger_err, get_logger_child
from mqtt_util import publish_sample, publish_cell_voltages, publish_temperatures, publish_hass_discovery, \
    subscribe_switches, mqtt_single_out

logger = get_logger_child("sampling")
logger_err = get_logger_err()

class SampleExpiredError(Exception):
    pass

class PeriodicBoolSignal:
    """
    Represents a periodic boolean signal that alternates its state over a defined time interval.

    The class is designed to generate a boolean signal that alternates between `True` and `False` 
    based on a specified period. It maintains the consistency of the signal's timing, even when 
    time drifts occur or delays are introduced. Can be used in scenarios requiring periodic 
    triggering or toggling behavior.

    :ivar period: The time interval, in seconds, defining the signal's periodicity.
    :type period: float
    :ivar state: Current state of the signal, `True` or `False`.
    :type state: bool
    :ivar counter: Number of complete periods elapsed since the signal started.
    :type counter: int
    """
    def __init__(self, period):
        self.period = period
        self._last_t = int(time.time())
        self.state = True
        self.counter = 0

    def __bool__(self):
        """
        Converts the state of the object to a boolean representation.

        If the `state` attribute is set to a truthy value, this method will return
        True; otherwise, it will return False.

        :return: Boolean value representing the `state` attribute
        :rtype: bool
        """
        return self.state
    
    def __str__(self):
        return str(self.state)

    def get(self):
        return self.state
    
    async def sleep(self):
        """
        Sleeps asynchronously for a calculated time period maintaining a consistent execution interval.

        The method calculates the time difference between the current time and the last execution
        to determine how much time to sleep, aiming to uphold a steady interval defined by the 
        class's `period` attribute. If the interval has already passed, it adjusts the internal 
        state without incurring a sleep delay.

        :param self: Instance of the class. Requires attributes `period`, `counter`, `_last_t`. 
        :return: None
        """
        diff = (time.time() - self._last_t)
        t = self.period - diff
        
        if t <= 0:
            # diff > period
            nb = int(t/self.period) + 1
            self.counter+=nb
            self._last_t += nb * self.period
            return
        await asyncio.sleep(t)
        self.counter+=1
        self._last_t += self.period

    def set_time(self):
        """
        Calculates the time until the next period and updates the internal state and counter if necessary.

        This method computes the difference between the current time and the last recorded time, adjusts
        the counter and last time based on the defined period, and returns the remaining time until the 
        next period. If the time has already elapsed, it updates the internal state and resets the time 
        to align with the timing period.

        :return: Time in seconds until the next period. Returns 0 if the time period has elapsed.
        :rtype: float
        """
        diff = (time.time() - self._last_t)
        t = self.period - diff
        logger.debug("%s set_time: %s", str(self.period), str(t))
        if t <= 0:
            nb = int(t/self.period) + 1
            self.counter+=nb
            self._last_t += nb * self.period
            self.state = True
            return 0
        if self.state:
            self.state = False
        return t

class BmsSampler(BmsSetSwitch):
    """
    Samples a single BMS and schedules publishing the samples to MQTT.
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
                 algorithms: Optional[list] = None,
                 current_calibration_factor=1.0,
                 over_power=None,
                 publish_index=True,
                 verbose_log=False,
                 bms_set_switch_delegate=Optional[BmsSetSwitch],
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

        self.downsampler = Downsampler()

        self.period_pub = PeriodicBoolSignal(period=publish_period or 1)
        self.period_discov = PeriodicBoolSignal(period=120 * 5)
        self.period_30s = PeriodicBoolSignal(period=30)

        self._last_time_log = 0

        self._last_power = 0
        self._t_last_power_jump = 0

        self._num_errors = 0
        self._time_next_retry = 0
        self.name = name
        self.address = address
        self.verbose_log = verbose_log
        self.subscribe_switches = False

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
        self.setting = None
        self.bms_set_switch_delegate = bms_set_switch_delegate

    def get_meter_state(self):
        return {meter.name: dict(reading=meter.get()) for meter in self.meters}

    def _filter_temperatures(self, temperatures):
        if not temperatures or self._lhq_temp is None:
            return temperatures
        return [round(self._lhq_temp[i].add(temperatures[i]), 2) for i in range(len(temperatures))]

    def put(self, sample: BmsSample):
        if self.setting:
            sample.set_setting(self.setting)
        self.queue.put(sample)

    async def action_queue(self):
        while not self.queue.empty():
            sample = self.queue.get(block=False)
            try:
                logger.debug('action_queue: %s', sample)
                await self.publish_sample(sample)
            except Exception as e:
                logger_err.error('exception in action callback: %s', e)

    async def set_switch(self, switch: str, state: bool):
        """
        Send a switch command to the BMS to control a physical switch, usually a MOSFET or relay.
        :param switch:
        :param state:
        :return:
        """
        if self.bms_set_switch_delegate:
            await self.bms_set_switch_delegate.set_switch(switch, state)

    def get_name(self):
        return self.name

    async def publish_sample(self, sample: BmsSample):
        
        mqtt_client = self.mqtt_client
        t_now = time.time()
        t_hour = t_now * (1 / 3600)

        if sample.timestamp < t_now - max(self.expire_after_seconds, MIN_VALUE_EXPIRY):
            raise SampleExpiredError("sample %s expired" % sample.timestamp)

        sample.num_samples = self.num_samples

        if self.current_calibration_factor and self.current_calibration_factor != 1:
            sample = sample.multiply_current(self.current_calibration_factor)

        # discharging P>0
        self.power_integrator_charge += (t_hour, abs(min(0, sample.power_ui)) * 1e-3)  # kWh
        self.power_integrator_discharge += (t_hour, abs(max(0, sample.power_ui)) * 1e-3)  # kWh

        # self.power_stats.add(sample.power)
        sample.temperatures = self._filter_temperatures(sample.temperatures)

        if not math.isnan(sample.mos_temperature) and self._lhq_temp is not None:
            sample.mos_temperature = self._lhq_temp['mos'].add(sample.mos_temperature)

        if self.invert_current:
            sample = sample.invert_current()

        self.current_integrator += (t_hour, sample.current)  # Ah
        self.power_integrator += (t_hour, sample.power_ui * 1e-3)  # kWh

        self.cycle_integrator += (t_hour, sample.soc * (0.01 / 2))  # SoC 100->0 is a half cycle
        self.charge_integrator += (t_hour, sample.charge)  # Ah

        if self.algorithm:
            res = self.algorithm.update(sample)
            if res or self.verbose_log:
                logger.info('Algo State=%s (bms=%s) -> %s ', self.algorithm.state,
                            BatterySwitches(**sample.switches), res)

            if res:
                from bmslib.store import store_algorithm_state
                state = self.algorithm.state
                if state:
                    store_algorithm_state(self.name, algorithm_name=self.algorithm.name, state=state.__dict__)

            if res and res.switches:
                for swk in sample.switches.keys():
                    if res.switches[swk] is not None:
                        logger_err.error('%s algo set %s switch -> %s', self.name, swk, res.switches[swk])
                        await self.set_switch(swk, res.switches[swk])

        if self.subscribe_switches == False and sample.switches and mqtt_client:
            logger.info("%s subscribing for %s switch change", self.name, sample.switches)
            subscribe_switches(mqtt_client, device_topic=self.mqtt_topic_prefix, bms=self,
                               switches=sample.switches.keys())
            self.subscribe_switches = True

        ## self.downsampler += sample

        log_data = (t_now - self._last_time_log) >= (60 if self.num_samples < 1000 else 300) or self.verbose_log
        if log_data:
            self._last_time_log = t_now

        voltages = sample.voltages

        PWR_CHG_REG = 120  # regularisation to suppress changes when power is low
        PWR_CHG_HOLD = 4
        power_chg = (sample.power_ui - self._last_power) / (abs(self._last_power) + PWR_CHG_REG)
        if abs(power_chg) > 0.15 and abs(sample.power_ui) > abs(self._last_power):
            if self.verbose_log or (
                    not self.period_pub and (t_now - self._t_last_power_jump) > PWR_CHG_HOLD):
                logger.info('%s Power jump %.0f %% (prev=%.0f last=%.0f, REG=%.0f)', self.name, power_chg * 100,
                            self._last_power, sample.power_ui, PWR_CHG_REG)
            self._t_last_power_jump = t_now
        self._last_power = sample.power_ui
        
        # publish home assistant discovery every 120 samples or 10 minutes
        if self.period_discov:
            logger.info("Sending HA discovery for %s (num_samples=%d)", self.name, self.num_samples)
            publish_hass_discovery(
                mqtt_client, device_topic=self.mqtt_topic_prefix,
                expire_after_seconds=1200,
                sample=sample,
                num_cells=len(voltages) if voltages else 0,
                temperatures=sample.temperatures,
                device_info=self.device_info,
            )

        publish_sample(mqtt_client, device_topic=self.mqtt_topic_prefix, sample=sample)
        log_data and logger.info('%s: (num_samples=%d) %s', self.name, self.num_samples, sample)

        publish_cell_voltages(mqtt_client, device_topic=self.mqtt_topic_prefix, voltages=voltages, publish_index=self.publish_index, bms_name=self.name)
        publish_temperatures(mqtt_client, device_topic=self.mqtt_topic_prefix, temperatures=sample.temperatures)

        if log_data and (voltages or sample.temperatures):
            logger.info('%s volt=[%s] temp=%s', self.name,
                        ','.join(map(str, voltages)) if voltages else voltages,
                        sample.temperatures)

        if self.period_discov or self.period_30s:
            self.publish_meters()

        self.num_samples += 1

        self.period_pub.set_time()
        self.period_30s.set_time()
        self.period_discov.set_time()

        # pass "light" errors to the caller to trigger a re-connect after too many
        return sample

    def publish_meters(self):
        device_topic = self.mqtt_topic_prefix
        for meter in self.meters:
            topic = f"{device_topic}/meter/{meter.name}"
            s = round(meter.get(), 3)
            mqtt_single_out(self.mqtt_client, topic, s)

    def set_setting(self, setting):
        self.setting = setting

    def set_info(self, info):
        self.device_info = info
        pass


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
