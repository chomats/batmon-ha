# -*- coding: utf-8 -*-
import math
import threading
from time import sleep, time

# Notes
# Added by https://github.com/KoljaWindeler

from .battery import Battery, Cell
from .deviceInfoSer import DeviceInfoSer
from struct import unpack_from
import sys
from .config import config, logger
from .jkserialio import JKSerialIO
from ..bms import BmsSample
from ..bms_ble.plugins.basebms import crc16_modbus2



class Jkbms_pb(Battery):
    def __init__(self, port: str, baud: int, address: str):
        super(Jkbms_pb, self).__init__(port, baud, address)
        self.type = self.BATTERYTYPE
        self.unique_identifier_tmp = ""
        self.cell_count = 0
        self.address = address
        self.jkIo = JKSerialIO(port, baud)
        #01 10 16 20 00 01 02 00 00 D6 F1
        
        self.history.exclude_values_to_calculate = ["charge_cycles"]
        self.USE_PORT_AS_UNIQUE_ID: bool = config.get_bool("DEFAULT", "USE_PORT_AS_UNIQUE_ID")
        self.mos_temperature = 0.0
        self.temperatures = [0.0, 0.0, 0.0, 0.0, 0.0]

    def command_status(self):
        return self.command_status

    BATTERYTYPE = "JKBMS PB Model"
    LENGTH_CHECK = 0  # ignored
    LENGTH_POS = 2  # ignored
    LENGTH_SIZE = "H"  # ignored

    def test_connection(self):
        """
        call a function that will connect to the battery, send a command and retrieve the result.
        The result or call should be unique to this BMS. Battery name or version, etc.
        Return True if success, False for failure
        """
        result = False
        try:
            # get settings to check if the data is valid and the connection is working
            result = self.get_settings()
            # get the rest of the data to be sure, that all data is valid and the correct battery type is recognized
            # only read next data if the first one was successful, this saves time when checking multiple battery types
            if result != False:
                result = self.refresh_data()
        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
            result = False

        return result

    def get_settings(self) -> DeviceInfoSer|bool: 
        # After successful connection get_settings() will be called to set up the battery
        # Set the current limits, populate cell count, etc
        # Return True if success, False for failure
        status_data = self.read_serial_data_jkbms_pb(self.jkIo.command_settings, 300)
        if status_data is False:
            return False

        

        status_data = self.read_serial_data_jkbms_pb(self.jkIo.command_about, 300)
        serial_nr = status_data[86:97].decode("utf-8")
        vendor_id = status_data[6:18].decode("utf-8")
        hw_version = (status_data[22:26].decode("utf-8") + " / " + status_data[30:35].decode("utf-8")).replace("\x00", "")
        sw_version = status_data[30:34].decode("utf-8")  # will be overridden

        self.unique_identifier_tmp = serial_nr
        self.version = sw_version
        self.hardware_version = hw_version

        logger.debug("Serial Nr: " + str(serial_nr))
        logger.debug("Vendor ID: " + str(vendor_id))
        logger.debug("HW Version: " + str(hw_version))
        logger.debug("SW Version: " + str(sw_version))

        # init the cell array
        for _ in range(self.cell_count):
            self.cells.append(Cell(False))

        return DeviceInfoSer(mnf=str(vendor_id),
                             name=str(serial_nr),
                              model=self.type,
                              sw_version=self.version,
                              hw_version=self.hardware_version)

    async def fetch(self) -> BmsSample|None:
        if (self.read_status_data() is False):
            return None
        return BmsSample(voltage=self.voltage, current=self.current, power=self.voltage*self.current,
                         charge=self.charge_charged, capacity=self.capacity, cycle_capacity=0,
                         num_cycles=self.history.charge_cycles, soc=self.soc, balance_current=0,
                         temperatures = self.temperatures,
                         mos_temperature= self.mos_temperature,
                         switches={},
                         uptime=math.nan)
    
    def refresh_data(self):
        # call all functions that will refresh the battery data.
        # This will be called for every iteration (1 second)
        # Return True if success, False for failure
        return self.read_status_data()

    def read_status_data(self):
        status_data = self.read_serial_data_jkbms_pb(self.command_status, 308)
        # check if connection success
        if status_data is False:
            return False

        #        logger.error("sucess we have data")
        #        be = ''.join(format(x, ' 02X') for x in status_data)
        #        logger.error(be)

        # cell voltages
        for c in range(self.cell_count):
            if (unpack_from("<H", status_data, c * 2 + 6)[0] / 1000) != 0:
                self.cells[c].voltage = unpack_from("<H", status_data, c * 2 + 6)[0] / 1000

        # MOSFET temperature
        temperature_mos = unpack_from("<h", status_data, 144)[0] / 10
        self.mos_temperature = temperature_mos if temperature_mos < 99 else (100 - temperature_mos)
        self.to_temperature(0, self.mos_temperature)

        # Temperature sensors
        #         0x009C  156+6 INT16   2   R   Battery temperature                                 TempBat1            0.1°C
        #         0x009E  158+6 INT16   2   R   Battery temperature                                 TempBat2            0.1°C
        #         0x00F8  248+6 INT16   2   R   Battery temperature                                 TempBat3            0.1°C
        #         0x00FA  250+6 INT16   2   R   Battery temperature                                 TempBat4            0.1°C
        #         0x00FC  252+6 INT16   2   R   Battery temperature                                 TempBat5            0.1°C
        temperature_1 = unpack_from("<h", status_data, 162)[0] / 10
        temperature_2 = unpack_from("<h", status_data, 164)[0] / 10
        temperature_3 = unpack_from("<h", status_data, 254)[0] / 10
        temperature_4 = unpack_from("<h", status_data, 256)[0] / 10
        temperature_5 = unpack_from("<h", status_data, 258)[0] / 10
        
        ## 0x00D0  208+6 UINT8   2   R   MOS temperature sensor                              MOSTempSensorAbsent                             BIT0
        # Battery temperature sensor 1                        BATTempSensor1Absent    1: Normal; 0: Missing   BIT1
        # Battery temperature sensor 2                        BATTempSensor2Absent    1: Normal; 0: Missing   BIT2
        # Battery temperature sensor 3                        BATTempSensor3Absent    1: Normal; 0: Missing   BIT3
        # Battery temperature sensor 4                        BATTempSensor4Absent    1: Normal; 0: Missing   BIT4
        # Battery temperature sensor 5                        BATTempSensor5Absent    1: Normal; 0: Missing   BIT5
        # Heating status                                      Heating                 1: On; 0: Off

        temp_status = unpack_from("<B", status_data, 214)[0]
        if temp_status & 0x02:
            self.temperatures[0] = temperature_1 if temperature_1 < 99 else (100 - temperature_1)
            self.to_temperature(1, self.temperatures[0])
        if temp_status & 0x04:
            self.temperatures[1] = temperature_2 if temperature_2 < 99 else (100 - temperature_2)
            self.to_temperature(2, self.temperatures[1])
        if temp_status & 0x08:
            self.temperatures[2] = temperature_3 if temperature_3 < 99 else (100 - temperature_3)
            self.to_temperature(3, self.temperatures[2])
        if temp_status & 0x10:
            self.temperatures[3] = temperature_4 if temperature_4 < 99 else (100 - temperature_4)
            self.to_temperature(3, self.temperatures[2])
        if temp_status & 0x20:
            self.temperatures[4] = temperature_5 if temperature_5 < 99 else (100 - temperature_5)
            self.to_temperature(4, self.temperatures[3])

        # Battery voltage
        self.voltage = unpack_from("<I", status_data, 150)[0] / 1000

        # Battery ampere
        self.current = unpack_from("<i", status_data, 158)[0] / 1000

        # SOC
        self.soc = unpack_from("<B", status_data, 173)[0]

        # cycles
        self.history.charge_cycles = unpack_from("<i", status_data, 182)[0]

        # capacity
        self.capacity_remain = unpack_from("<i", status_data, 174)[0] / 1000

        # fuses
        self.to_protection_bits(unpack_from("<I", status_data, 166)[0])

        # bits
        bal = unpack_from("<B", status_data, 172)[0]
        charge = unpack_from("<B", status_data, 198)[0]
        discharge = unpack_from("<B", status_data, 199)[0]
        self.charge_fet = 1 if charge != 0 else 0
        self.discharge_fet = 1 if discharge != 0 else 0
        self.balancing = 1 if bal != 0 else 0

        # show wich cells are balancing
        if self.get_min_cell() is not None and self.get_max_cell() is not None:
            for c in range(self.cell_count):
                if self.balancing and (self.get_min_cell() == c or self.get_max_cell() == c):
                    self.cells[c].balance = True
                else:
                    self.cells[c].balance = False

        # logging
        """
        for c in range(self.cell_count):
                logger.error("Cell "+str(c)+" voltage: "+str(self.cells[c].voltage)+"V")
        logger.error("Temperature 2: "+str(temperature_1))
        logger.error("Temperature 3: "+str(temperature_2))
        logger.error("voltage: "+str(self.voltage)+"V")
        logger.error("Current: "+str(self.current))
        logger.error("SOC: "+str(self.soc)+"%")
        logger.error("Mos Temperature: "+str(temperature_mos))
        """

        return True

    def unique_identifier(self) -> str:
        """
        Used to identify a BMS when multiple BMS are connected
        """
        # TODO: Temporary solution, since the serial number is not correctly read
        if self.USE_PORT_AS_UNIQUE_ID:
            return self.port + ("__" + self.address if self.address is not None else "")
        else:
            return self.unique_identifier_tmp

    def get_balancing(self):
        return 1 if self.balancing else 0

    def get_min_cell(self):
        min_voltage = 9999
        min_cell = None
        for c in range(min(len(self.cells), self.cell_count)):
            if self.cells[c].voltage is not None and min_voltage > self.cells[c].voltage:
                min_voltage = self.cells[c].voltage
                min_cell = c
        return min_cell

    def get_max_cell(self):
        max_voltage = 0
        max_cell = None
        for c in range(min(len(self.cells), self.cell_count)):
            if self.cells[c].voltage is not None and max_voltage < self.cells[c].voltage:
                max_voltage = self.cells[c].voltage
                max_cell = c
        return max_cell

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

    def read_serial_data_jkbms_pb(self, command: bytearray|None, callback, length: int) -> bytearray|bool:
        """
        use the read_serial_data() function to read the data and then do BMS specific checks (crc, start bytes, etc)
        :param command: the command to be sent to the bms
        :return: True if everything is fine, else False
        """
        modbus_msg = None
        if command is not None:
            modbus_msg = self.jkIo.generateCmd(command, self.address)

        self.jkIo.read_serial_data(
            modbus_msg,
            callback,
            length
        )


def crcJK232(byteData):
    """
    Generate JK RS232 / RS485 CRC
    - 2 bytes, the verification field is "command code + length byte + data segment content",
    the verification method is thesum of the above fields and then the inverse plus 1, the high bit is in the front and the low bit is in the back.
    """
    CRC = 0
    for b in byteData:
        CRC += b
    crc_low = CRC & 0xFF
    crc_high = (CRC >> 8) & 0xFF
    return [crc_high, crc_low]    
