import math
import time
from copy import copy
from typing import List, Dict, Optional
from dataclasses import dataclass
from bmslib.protection import Protection

MIN_VALUE_EXPIRY = 20


class DeviceInfo:
    def __init__(self, mnf: str, model: str, hw_version: Optional[str], sw_version: Optional[str], name: Optional[str],
                 sn: Optional[str] = None, psk: Optional[str] = None, address: Optional[int] = None):
        self.mnf = mnf
        self.model = model
        self.hw_version = hw_version
        self.sw_version = sw_version
        self.name = name
        self.sn = sn
        self.psk = psk
        self.float_charger = False
        self.address = address

    def __str__(self):
        s = f'DeviceInfo({self.model},hw-{self.hw_version},sw-{self.sw_version}'
        if self.name:
            s += ',' + self.name
        if self.sn:
            s += ',#' + self.sn
        if self.psk:
            s += ',psk=' + self.psk
        if self.float_charger:
            s += ',float_charge=' + str(self.float_charger)
        if self.address:
            s += ',address=' + str(self.address)
        return s + ')'


class PowerMonitorSample:
    # Todo this is a draft
    def __init__(self, voltage, current, power=math.nan, total_energy=math.nan):
        pass


@dataclass
class SettingsData:
    """
    SettingsData represents a configuration structure used for various system settings
    and operational thresholds. This class is primarily utilized in managing power system
    parameters, including voltage, current, temperature, delays, and system functionality.

    This dataclass is designed for precise configuration to ensure safe and optimal operation
    of battery systems. It includes properties for overvoltage, undervoltage, current limits,
    temperature thresholds, and delay timings. Additionally, it governs system controls like
    balancing, charging, and discharging enablement.

    The purpose of the class is to encapsulate and handle the settings reliably, reducing complexity
    and enhancing system management. It can be used as a container for configuration data.

    :ivar address: Address identifier for the system.
    :type address: int
    :ivar vol_smart_sleep: Voltage threshold for smart sleep mode.
    :type vol_smart_sleep: float
    :ivar vol_cell_uv: Cell undervoltage detection threshold.
    :type vol_cell_uv: float
    :ivar vol_cell_uvpr: Cell undervoltage protection release threshold.
    :type vol_cell_uvpr: float
    :ivar vol_cell_ov: Cell overvoltage detection threshold.
    :type vol_cell_ov: float
    :ivar vol_cell_ovpr: Cell overvoltage protection release threshold.
    :type vol_cell_ovpr: float
    :ivar vol_balan_trig: Trigger voltage for cell balancing.
    :type vol_balan_trig: float
    :ivar vol_soc_full: Voltage corresponding to a fully charged state of charge.
    :type vol_soc_full: float
    :ivar vol_soc_empty: Voltage corresponding to an empty state of charge.
    :type vol_soc_empty: float
    :ivar vol_rcv: Recovery voltage for system reactivation.
    :type vol_rcv: float
    :ivar vol_rfv: Reference voltage used for system calibration.
    :type vol_rfv: float
    :ivar vol_sys_pwr_off: Voltage threshold for system power-off.
    :type vol_sys_pwr_off: float
    :ivar max_battery_charge_current: Maximum allowable battery charge current.
    :type max_battery_charge_current: float
    :ivar tim_bat_cocp_dly: Delay time for overcurrent protection during charging.
    :type tim_bat_cocp_dly: int
    :ivar tim_bat_cocpr_dly: Release delay time for overcurrent protection during charging.
    :type tim_bat_cocpr_dly: int
    :ivar max_battery_discharge_current: Maximum allowable battery discharge current.
    :type max_battery_discharge_current: float
    :ivar tim_bat_dc_ocp_dly: Delay time for overcurrent protection during discharging.
    :type tim_bat_dc_ocp_dly: int
    :ivar tim_bat_dc_ocpr_dly: Release delay time for overcurrent protection during discharging.
    :type tim_bat_dc_ocpr_dly: int
    :ivar tim_bat_scpr_dly: Delay time for short-circuit protection response.
    :type tim_bat_scpr_dly: int
    :ivar cur_balan_max: Maximum current threshold for balancing operations.
    :type cur_balan_max: float
    :ivar tmp_bat_cot: Battery critical over-temperature threshold.
    :type tmp_bat_cot: float
    :ivar tmp_bat_cotpr: Battery critical over-temperature protection release threshold.
    :type tmp_bat_cotpr: float
    :ivar tmp_bat_dc_ot: Battery discharge over-temperature threshold.
    :type tmp_bat_dc_ot: float
    :ivar tmp_bat_dc_otpr: Battery discharge over-temperature protection release threshold.
    :type tmp_bat_dc_otpr: float
    :ivar tmp_bat_cut: Battery charge over-temperature threshold.
    :type tmp_bat_cut: float
    :ivar tmp_bat_cutpr: Battery charge over-temperature protection release threshold.
    :type tmp_bat_cutpr: float
    :ivar tmp_mos_ot: MOSFET over-temperature threshold.
    :type tmp_mos_ot: float
    :ivar tmp_mos_otpr: MOSFET over-temperature protection release threshold.
    :type tmp_mos_otpr: float
    :ivar cell_count: Number of cells in the system.
    :type cell_count: int
    :ivar charge: Enable flag for battery charging.
    :type charge: bool
    :ivar discharge: Enable flag for battery discharging.
    :type discharge: bool
    :ivar balance: Enable flag for cell balancing.
    :type balance: bool
    :ivar capacity: Battery capacity in ampere-hours.
    :type capacity: float
    :ivar scp_delay: Delay time for short-circuit protection.
    :type scp_delay: int
    :ivar start_bal_vol: Voltage threshold to start cell balancing.
    :type start_bal_vol: float
    """

    def __init__(self, address: int, vol_smart_sleep: float, vol_cell_uv: float, vol_cell_uvpr: float,
                 vol_cell_ov: float, vol_cell_ovpr: float, vol_balan_trig: float, vol_soc_full: float,
                 vol_soc_empty: float, vol_rcv: float, vol_rfv: float, vol_sys_pwr_off: float,
                 max_battery_charge_current: float, tim_bat_cocp_dly: int, tim_bat_cocpr_dly: int,
                 max_battery_discharge_current: float, tim_bat_dc_ocp_dly: int, tim_bat_dc_ocpr_dly: int,
                 tim_bat_scpr_dly: int, cur_balan_max: float, tmp_bat_cot: float, tmp_bat_cotpr: float,
                 tmp_bat_dc_ot: float, tmp_bat_dc_otpr: float, tmp_bat_cut: float, tmp_bat_cutpr: float,
                 tmp_mos_ot: float, tmp_mos_otpr: float, cell_count: int, charge: bool,
                 discharge: bool, balance: bool,
                 float_charge: bool,
                 capacity: float, scp_delay: int,
                 start_bal_vol: float,
                 status_282: int,
                 tim_prodischarge: float,
                 switches: Dict[str, bool]):
        self.address = address
        self.vol_smart_sleep = vol_smart_sleep
        self.vol_cell_uv = vol_cell_uv
        self.vol_cell_uvpr = vol_cell_uvpr
        self.vol_cell_ov = vol_cell_ov
        self.vol_cell_ovpr = vol_cell_ovpr
        self.vol_balan_trig = vol_balan_trig
        self.vol_soc_full = vol_soc_full
        self.vol_soc_empty = vol_soc_empty
        self.vol_rcv = vol_rcv
        self.vol_rfv = vol_rfv
        self.vol_sys_pwr_off = vol_sys_pwr_off
        self.max_battery_charge_current = max_battery_charge_current
        self.tim_bat_cocp_dly = tim_bat_cocp_dly
        self.tim_bat_cocpr_dly = tim_bat_cocpr_dly
        self.max_battery_discharge_current = max_battery_discharge_current
        self.tim_bat_dc_ocp_dly = tim_bat_dc_ocp_dly
        self.tim_bat_dc_ocpr_dly = tim_bat_dc_ocpr_dly
        self.tim_bat_scpr_dly = tim_bat_scpr_dly
        self.tim_prodischarge = tim_prodischarge
        self.cur_balan_max = cur_balan_max
        self.tmp_bat_cot = tmp_bat_cot
        self.tmp_bat_cotpr = tmp_bat_cotpr
        self.tmp_bat_dc_ot = tmp_bat_dc_ot
        self.tmp_bat_dc_otpr = tmp_bat_dc_otpr
        self.tmp_bat_cut = tmp_bat_cut
        self.tmp_bat_cutpr = tmp_bat_cutpr
        self.tmp_mos_ot = tmp_mos_ot
        self.tmp_mos_otpr = tmp_mos_otpr
        self.cell_count = cell_count
        self.charge = charge
        self.discharge = discharge
        self.balance = balance
        self.float_charge = float_charge
        self.capacity = capacity
        self.scp_delay = scp_delay
        self.start_bal_vol = start_bal_vol
        self.switches = switches
        self.status_282 = status_282

        if switches:
            assert all(map(lambda x: isinstance(x, bool), switches.values())), "non-bool switches values %s" % switches


    def __str__(self):
        s = 'SettingsData('
        attrs = [f"{attr}={getattr(self, attr)}" for attr in dir(self) if not attr.startswith('_')]
        return s + ','.join(attrs) + ')'

    address: int
    vol_smart_sleep: float
    vol_cell_uv: float
    vol_cell_uvpr: float
    vol_cell_ov: float
    vol_cell_ovpr: float
    vol_balan_trig: float
    vol_soc_full: float
    vol_soc_empty: float
    vol_rcv: float
    vol_rfv: float
    vol_sys_pwr_off: float
    max_battery_charge_current: float
    tim_bat_cocp_dly: int
    tim_bat_cocpr_dly: int
    max_battery_discharge_current: float
    tim_bat_dc_ocp_dly: int
    tim_bat_dc_ocpr_dly: int
    tim_bat_scpr_dly: int
    cur_balan_max: float
    tmp_bat_cot: float
    tmp_bat_cotpr: float
    tmp_bat_dc_ot: float
    tmp_bat_dc_otpr: float
    tmp_bat_cut: float
    tmp_bat_cutpr: float
    tmp_mos_ot: float
    tmp_mos_otpr: float
    cell_count: int
    charge: bool
    discharge: bool
    balance: bool
    float_charge: bool
    capacity: float
    scp_delay: int
    start_bal_vol: float
    tim_prodischarge: float
    switches: Dict[str, bool]


class BmsSample:
    def __init__(self, voltage, current, power=math.nan,
                 charge=math.nan, capacity=math.nan, cycle_capacity=math.nan,
                 num_cycles=math.nan, soc=math.nan,
                 balance_current=math.nan,
                 temperatures: List[float] = None,
                 voltages: List[float] = None,
                 resistances: List[float] = None,
                 mos_temperature: float = math.nan,
                 temp_status_flag: list[bool] = None,
                 switches: Optional[Dict[str, bool]] = None,
                 uptime=math.nan, timestamp: Optional[float] = None,
                 ad=None,
                 minimum_voltage_cell_index=None,
                 maximum_voltage_cell_index=None,
                 maximum_voltage_difference=None,
                 cell_average_voltage=None,
                 battery_status=None,
                 alarm=None,
                 balance_line_resistance_status=None,
                 balance_state=None,
                 trame_str=None,
                 temp_moyenne=None,
                 temp_max=None,
                 temp_min=None, ):
        """

        :param voltage:
        :param current: Current out of the battery (negative=charging, positive=discharging)
        :param charge: The charge available in Ah, aka remaining capacity, between 0 and `capacity`
        :param capacity: The capacity of the battery in Ah
        :param cycle_capacity: Total absolute charge meter (coulomb counter). Increases during charge and discharge. Can tell you the battery cycles (num_cycles = cycle_capacity/2/capacity). A better name would be cycle_charge. This is not well defined.
        :param num_cycles:
        :param soc: in % (0-100)
        :param balance_current:
        :param temperatures:
        :param mos_temperature:
        :param uptime: BMS uptime in seconds
        :param timestamp: seconds since epoch (unix timestamp from time.time())
        """
        self.address = ad
        self.voltage: float = voltage
        self.current: float = current or 0  # -
        self._power = power  # 0 -> +0
        self.balance_current = balance_current

        # infer soc from capacity if soc is nan or type(soc)==int (for higher precision)
        if capacity > 0 and (math.isnan(soc) or (isinstance(soc, int) and charge > 0)):
            soc = round(charge / capacity * 100, 2)
        elif math.isnan(capacity) and soc > .2:
            capacity = round(charge / soc * 100)

        # assert math.isfinite(soc)

        self.charge: float = charge
        self.capacity: float = capacity
        self.soc: float = soc
        self.cycle_capacity: float = cycle_capacity
        self.num_cycles: float = num_cycles
        self.temperatures = temperatures
        self.mos_temperature = mos_temperature
        self.switches = switches
        self.uptime = uptime
        self.timestamp = timestamp or time.time()
        self.voltages = voltages
        self.resistances = resistances
        self.temp_status_flag = temp_status_flag
        self.minimum_voltage_cell_index = minimum_voltage_cell_index
        self.maximum_voltage_cell_index = maximum_voltage_cell_index
        self.maximum_voltage_difference = maximum_voltage_difference
        self.cell_average_voltage = cell_average_voltage
        self.battery_status = battery_status
        self.num_samples = 0
        self.alarm = alarm
        self.balance_line_resistance_status = balance_line_resistance_status
        self.balance_state = balance_state
        self.trame_str = trame_str
        self.temp_moyenne = temp_moyenne
        self.temp_max = temp_max
        self.temp_min = temp_min
        self.setting : Optional[SettingsData]= None
        if alarm is not None:
            self.protection = Protection()
            self.to_protection_bits(self.alarm)

        if switches:
            assert all(map(lambda x: isinstance(x, bool), switches.values())), "non-bool switches values %s" % switches

    @property
    def power(self):
        """
        :return: Power (P=U*I) in W
        """
        return 0 if math.isnan(self._power) else self._power
    
    @property
    def power_ui(self):
        """
        :return: Power (P=U*I) in W
        """
        return self.voltage * self.current

    def values(self):
        return {**self.__dict__, "power": self.power}

    def __str__(self):
        # noinspection PyStringFormat
        s = 'BmsSampl('
        s += 'ad=%s, ' % self.address
        if not math.isnan(self.soc):
            s += '%.1f%%,' % self.soc
        vals = self.values()
        s += ', U=%(voltage).1fV,I=%(current).2fA,P=%(power).0fW,' % vals
        if not math.isnan(self.charge):
            s += 'Q=%(charge).0f/' % vals
        s += 'capacity=%(capacity).0fAh,mos=%(mos_temperature).0f°C' % vals
        s += 'maximum_voltage_difference= %(maximum_voltage_difference).0fV,cell_average_voltage=%(cell_average_voltage).0fV' % vals
        if self.temperatures:
            s += ',temp=[%s]' % ','.join(map(str, self.temperatures))
        if self.voltages:
            s += ',V=[%s]' % ','.join(map(str, self.voltages))
        return s.rstrip(',') + ')'

    def invert_current(self):
        return self.multiply_current(-1)

    def multiply_current(self, x):
        res = copy(self)
        if res.current != 0:  # prevent -0 values
            res.current *= x
        if not math.isnan(res._power) and res._power != 0:
            res._power *= x
        return res

    def set_setting(self, setting: SettingsData):
        self.setting = setting
        self.switches = setting.switches

    def to_protection_bits(self, byte_data):
        """
        Bit 0x00000001: Wire resistance alarm: 1 warning only, 0 nomal -> OK
        Bit 0x00000002: MOS overtemperature alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000004: Cell quantity alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000008: Current sensor error alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000010: Cell OVP alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000020: Bat OVP alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000040: Charge Over current alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000080: Charge SCP alarm: 1 alarm, 0 nomal -> OK
        Bit 0x00000100: Charge OTP: 1 alarm, 0 nomal -> OK
        Bit 0x00000200: Charge UTP: 1 alarm, 0 nomal -> OK
        Bit 0x00000400: CPU Aux Communication: 1 alarm, 0 nomal -> OK
        Bit 0x00000800: Cell UVP: 1 alarm, 0 nomal -> OK
        Bit 0x00001000: Batt UVP: 1 alarm, 0 nomal
        Bit 0x00002000: Discharge Over current: 1 alarm, 0 nomal
        Bit 0x00004000: Discharge SCP: 1 alarm, 0 nomal
        Bit 0x00008000: Discharge OTP: 1 alarm, 0 nomal
        Bit 0x00010000: Charge MOS: 1 alarm, 0 nomal
        Bit 0x00020000: Discharge MOS: 1 alarm, 0 nomal
        Bit 0x00040000: GPS disconnected: 1 alarm, 0 nomal
        Bit 0x00080000: Modify PWD in time: 1 alarm, 0 nomal
        Bit 0x00100000: Discharg on Faied: 1 alarm, 0 nomal
        Bit 0x00200000: Battery over Temp: 1 alarm, 0 nomal
        """

        # low capacity alarm
        self.protection.low_soc = (byte_data & 0x00001000) * 2
        # MOSFET temperature alarm
        self.protection.high_internal_temperature = (byte_data & 0x00000002) * 2
        # charge over voltage alarm
        self.protection.high_voltage = (byte_data & 0x00000020) * 2
        # discharge under voltage alarm
        self.protection.low_voltage = (byte_data & 0x00000800) * 2
        # charge overcurrent alarm
        self.protection.high_charge_current = (byte_data & 0x00000040) * 2
        # discharge over current alarm
        self.protection.high_discharge_current = (byte_data & 0x00002000) * 2
        # core differential pressure alarm OR unit overvoltage alarm
        self.protection.cell_imbalance = 0
        # cell overvoltage alarm
        self.protection.high_cell_voltage = (byte_data & 0x00000010) * 2
        # cell undervoltage alarm
        self.protection.low_cell_voltage = (byte_data & 0x00001000) * 2
        # battery overtemperature alarm OR overtemperature alarm in the battery box
        self.protection.high_charge_temperature = (byte_data & 0x00000100) * 2
        self.protection.low_charge_temperature = (byte_data & 0x00000200) * 2
        # check if low/high temp alarm arise during discharging
        self.protection.high_temperature = (byte_data & 0x00008000) * 2
        self.protection.low_temperature = 0

class BmsSetSwitch:
    def get_name(self):
        return self.__class__.__name__

    async def set_switch(self, switch: str, state: bool):
        """
        Send a switch command to the BMS to control a physical switch, usually a MOSFET or relay.
        :param switch:
        :param state:
        :return:
        """
        raise NotImplementedError()