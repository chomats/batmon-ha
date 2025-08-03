import asyncio
import sys
import time
from typing import Union

import serial

from mqtt_util import is_none_or_nan
from ..bms import BmsSample, SettingsData, DeviceInfo
from ..bms_ble.plugins.basebms import crc16_modbus2
from ..sampling import PeriodicBoolSignal
from ..util import get_logger, to_hex_str, read_str

logger = get_logger().getChild("JKSerialIO")

class JKSerialIO:
    def __init__(self, port: str, baud: int, count_bat:int = 3, master:bool = False) -> None:
        self._serial_port = port
        self._serial_baud = baud
        self.no_data_counter = 0
        self.command_status =   b"\x10\x16\x20\x00\x01\x02\x00\x00"
        self.command_settings = b"\x10\x16\x1e\x00\x01\x02\x00\x00"
        self.command_about =    b"\x10\x16\x1c\x00\x01\x02\x00\x00"
        self.shutdown = False
        self.cmd_line: bytearray = bytearray()
        self.count_bat = count_bat
        self.master = master

    def generate_cmd(self, command:bytes, address:int=1) -> bytearray:
        """
        Generate a Modbus RTU command with appended CRC checksum.

        This method constructs a Modbus RTU message by adding the provided command to
        a bytearray starting with the given address. It then calculates and appends
        the CRC checksum of the resulting message.

        :param command: The command to be sent in the Modbus RTU protocol.
        :param address: The address of the Modbus slave device. Defaults to 1.
        :type address: int
        :return: A Modbus RTU formatted message including the CRC checksum.
        :rtype: bytearray
        """
        modbus_msg = bytearray([address])
        modbus_msg += command
        modbus_msg += crc16_modbus2(modbus_msg)
        return modbus_msg

    def sendBMSCommand(self, ser, cmd_bytes):
        for cmd_byte in cmd_bytes:
            hex_byte = ("{0:02x}".format(cmd_byte))
            ser.write(bytearray.fromhex(hex_byte))
        return        

    async def read_serial_data(
            self,
            command: any,
            callback,
            length_fixed: Union[int, None] = 308,
    ):
        """
        Read data from a serial port
    
        :param callback: 
        :param command: Command to send
        :param length_fixed: Fixed length of the data, if not set it will be read from the data
        :return: Data read from the serial port
        """
        ser = None  # Initialize ser to None
        try:
            with serial.Serial(self._serial_port, baudrate=self._serial_baud, timeout=0.2) as ser:
                if self.master:
                    await self.read_serialport_data_mode_master_all_slave(ser, callback, length_fixed)
                else:
                    await self.read_serialport_data(ser, command, callback, length_fixed)

        except serial.SerialException as e:
            logger.error(e)
            if ser is not None:
                # close the serial port if it was opened
                ser.close()
                logger.error("Serial port closed")
            else:
                logger.error("Serial port could not be opened")

        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")

    async def read_trame_55_aa(self, ser: serial.Serial, callback, length_fixed: int = 308, timeout=0.5):
        """

        """
        bms = ser
        t_now = time.time_ns()
        ns_timeout= timeout*1e9
        while not self.shutdown:
            t_current = time.time_ns() - t_now
            if t_current > ns_timeout:
                logger.info(f"Timeout to read trame {t_current}")
                return False
                
            if ser.in_waiting >= 4 :
                b = bms.read(1)
                logger.debug(f"BMS data received {b.hex()}")
                if b.hex() == '55' : # header byte 1
                    b = bms.read(1)
                    logger.debug(f"BMS data received {b.hex()}")
                    if b.hex() == 'aa' : # header byte 2
                        if self.cmd_line.__len__()>0:
                            await self.cmd_line_11(callback)
                            self.cmd_line = bytearray()
    
                        # next two bytes is the length of the data package, including the two length bytes
                        length = length_fixed - 2
                        logger.debug(f"BMS data length {length}")
                        response_line = None
                        no_data_counter = 0
                        while no_data_counter < 5:  # Try up to 5 times with no new data before exiting
                            if bms.in_waiting > 0:
                                if response_line is None:
                                    response_line = bytearray()
                                l = bms.in_waiting
                                if response_line.__len__()+l > length:
                                    l = length-response_line.__len__()
                                response_line.extend(bms.read(l))
                                if (response_line.__len__()) == length:
                                    break
                                else:
                                    logger.debug(f"BMS data length {response_line.__len__()} < {length}")
                                no_data_counter = 0  # Reset counter if data was received
                            else:
                                no_data_counter += 1
                                await asyncio.sleep(0.01)
    
                        # Reconstruct the header and length field
                        data = bytearray.fromhex("55aa")
                        data += response_line
    
                        trame1 = data[0:300]
                        crc = sum(trame1[:-1]) & 0xFF
                        trame2 = data[300:]
                        logger.debug(f"trame1 {to_hex_str(data)}")
    
                        if crc != trame1[-1] & 0xFF:
                            logger.error(f"CRC error in trame1 {crc:02X} != {trame1[-1]:02X}")
                            return False
    
                        crc_cmd = crc16_modbus2(trame2[:-2])
                        if crc_cmd != trame2[-2:]:
                            logger.error(f"CRC error in trame2 {crc_cmd[0]:02X} {crc_cmd[1]:02X} != {trame2[-2]:02X} {trame2[-1]:02X}")
                            return False

                        t_current = time.time_ns() - t_now
                        logger.debug(f"Read trame in {t_current}")
                        await callback(data)
                        return True
                    else:
                        await self.cmd_line_11(callback)
                        self.cmd_line.extend(b)
                else:
                    await self.cmd_line_11(callback)
                    self.cmd_line.extend(b)
            else:
                await asyncio.sleep(0.01)
        return False


    async def read_serialport_data_mode_master_all_slave(
            self,
            ser: serial.Serial,
            callback,
            length_fixed: int = 308
    ):
        """
        Read data from a serial port
    
        :param callback: 
        :param ser: Serial port
        :param command: Command to send
        :param length_fixed: Fixed length of the data, if not set it will be read from the data
        :return: Data read from the serial port
        """
        try:
            counter = 0
            period_status = PeriodicBoolSignal(period=5)
            period_setting = PeriodicBoolSignal(period=30)
            period_about = PeriodicBoolSignal(period=600)

            counter = await self.send_cmd_and_read_all_slave(callback, self.command_about, counter, length_fixed, ser)
            counter = await self.send_cmd_and_read_all_slave(callback, self.command_settings, counter, length_fixed, ser)

            while not self.shutdown:
                t_now = time.time()
                period_status.set_time(t_now)
                period_setting.set_time(t_now)
                period_about.set_time(t_now)
                if period_about:
                    counter = await self.send_cmd_and_read_all_slave(callback, self.command_about, counter, length_fixed, ser)
                if period_setting:
                    counter = await self.send_cmd_and_read_all_slave(callback, self.command_settings, counter, length_fixed, ser)
                if period_status:
                    counter = await self.send_cmd_and_read_all_slave(callback, self.command_status, counter, length_fixed, ser)

                await asyncio.sleep(0.1)
                counter += 1
                if counter>800:
                    await callback(self.cmd_line)
                    return

        except serial.SerialException as e:
            logger.error(e)
            return

        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
            return

    async def send_cmd_and_read_all_slave(self, callback, command, counter, length_fixed, ser):
        for i in range(1, self.count_bat + 1):
            cmd = self.generate_cmd(command, i)
            logger.info(f"cmd {i}/{to_hex_str(cmd)}")
            self.sendBMSCommand(ser, cmd)
            if await self.read_trame_55_aa(ser, callback, length_fixed, 0.5):
                counter = 0
            await asyncio.sleep(0.1)

        return counter

    async def read_serialport_data(
            self,
            ser: serial.Serial,
            command: bytearray,
            callback,
            length_fixed: int = 308
    ):
        """
        Read data from a serial port
    
        :param callback: 
        :param ser: Serial port
        :param command: Command to send
        :param length_fixed: Fixed length of the data, if not set it will be read from the data
        :return: Data read from the serial port
        """
        try:
            if command:
                self.sendBMSCommand(ser, command)
    
            counter = 0
            while not self.shutdown:
                counter += 1
                if await self.read_trame_55_aa(ser, callback, length_fixed, 5):
                    counter = 0
                if counter>800:
                    callback(self.cmd_line)
                    return
    
        except serial.SerialException as e:
            logger.error(e)
            return
    
        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
            return

    async def cmd_line_11(self, callback):
        if len(self.cmd_line) == 11:
            crc = crc16_modbus2(self.cmd_line[:-2])
            crcGood = self.cmd_line[-2] == crc[0] and self.cmd_line[-1] == crc[1]
            await callback(self.cmd_line, crcGood)
            self.cmd_line = bytearray()


def s_decode_sample(is_new_11fw_32s, logger,
                    num_cells,
                    buf: bytearray, buf_set: bytearray|None,
                    t_buf: float, has_float_charger:bool) -> BmsSample:

    offset = 0
    if is_new_11fw_32s is None:
        is_new_11fw_32s = True

    if is_new_11fw_32s:
        offset = 32
        logger.debug('New 11.x firmware, offset=%s', offset)

    i16 = lambda i: int.from_bytes(buf[i:(i + 2)], byteorder='little', signed=True)
    u8 = lambda i: int.from_bytes(buf[i:(i + 1)], byteorder='little', signed=True)
    u16 = lambda i: int.from_bytes(buf[i:(i + 2)], byteorder='little', signed=False)
    u32 = lambda i: int.from_bytes(buf[i:(i + 4)], byteorder='little', signed=False)
    f16u = lambda i: u16(i) * 1e-3
    f32u = lambda i: u32(i) * 1e-3
    f32s = lambda i: int.from_bytes(buf[i:(i + 4)], byteorder='little', signed=True) * 1e-3

    temp = lambda x: float('nan') if x == -2000 else (x / 10)

    trame_str = ' '.join(format(x, '02x') for x in buf[0:6])
    voltages = [u16(6 + i * 2) for i in range(num_cells)]
    trame_str += ' ' + ''.join(f"{x}mV" for x in voltages)
    trame_str += ' ' + ''.join(f"{x}mV" for x in [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])

    #162
    #164
    # Temperature sensors
    #         0x009C  156+6 INT16   2   R   Battery temperature                                 TempBat1            0.1°C
    #         0x009E  158+6 INT16   2   R   Battery temperature                                 TempBat2            0.1°C
    #         0x00F8  248+6 INT16   2   R   Battery temperature                                 TempBat3            0.1°C
    #         0x00FA  250+6 INT16   2   R   Battery temperature                                 TempBat4            0.1°C
    #         0x00FC  252+6 INT16   2   R   Battery temperature                                 TempBat5            0.1°C

    temperatures = [temp(i16(130 + offset)), temp(i16(132 + offset))]
    if is_new_11fw_32s:
        temperatures += [temp(i16(222 + offset)), temp(i16(224 + offset)), temp(i16(226 + offset))]
        #252,258
    ## 0x00D0  208+6 UINT8   2   R   MOS temperature sensor                              MOSTempSensorAbsent                             BIT0
    # Battery temperature sensor 1                        BATTempSensor1Absent    1: Normal; 0: Missing   BIT1
    # Battery temperature sensor 2                        BATTempSensor2Absent    1: Normal; 0: Missing   BIT2
    # Battery temperature sensor 3                        BATTempSensor3Absent    1: Normal; 0: Missing   BIT3
    # Battery temperature sensor 4                        BATTempSensor4Absent    1: Normal; 0: Missing   BIT4
    # Battery temperature sensor 5                        BATTempSensor5Absent    1: Normal; 0: Missing   BIT5
    # Heating status                                      Heating                 1: On; 0: Off
    mos_temperature = i16((112 if is_new_11fw_32s else 134) + offset) / 10
    
    temp_somme = mos_temperature
    temp_count = 1
    temp_min = mos_temperature
    temp_max = mos_temperature
    for i in range(0, len(temperatures)):
        if not is_none_or_nan(temperatures[i]):
            if temperatures[i]<temp_min:
                temp_min=temperatures[i]
            if temperatures[i]>temp_max:
                temp_max=temperatures[i]
            temp_somme+=temperatures[i]
            temp_count+=1
    temp_moyenne = temp_somme/temp_count
    
    temp_status = u8(214)
    temp_status_flag = [
        temp_status & 0x01 == 0x01 ,
        temp_status & 0x02 == 0x02 ,
        temp_status & 0x04 == 0x04 ,
        temp_status & 0x08 == 0x08 ,
        temp_status & 0x10 == 0x10,
        temp_status & 0x20 == 0x20,
        temp_status & 0x40 == 0x40,
        temp_status & 0x80 == 0x80
    ]
    # 70 -> 80
    # 0x0040  64  UINT32  4   R   Battery status                                      CellSta                 BIT[n] is 1, indicating that the battery exists
    # 0x0044  68  UINT16  2   R   Cell average voltage                                CellVolAve          mV
    # 0x0046  70  UINT16  2   R   Maximum voltage difference                          CellVdifMax         mV
    # 0x0048  72  UINT8   2   R   Maximum voltage cell number                         MaxVolCellNbr
    # UINT8       R   Minimum voltage cell number                         MinVolCellNbr

    #         0x0040  64  UINT32  4   R   Battery status                                      CellSta                 BIT[n] is 1, indicating that the battery exists
    #         0x0044  68  UINT16  2   R   Cell average voltage                                CellVolAve          mV
    #         0x0046  70  UINT16  2   R   Maximum voltage difference                          CellVdifMax         mV
    #         0x0048  72  UINT8   2   R   Maximum voltage cell number                         MaxVolCellNbr
    #                 73  UINT8       R   Minimum voltage cell number                         MinVolCellNbr

    return BmsSample(
        trame_str=trame_str,
        ad=u8(300),
        battery_status=u32(70),
        cell_average_voltage=f16u(74),
        maximum_voltage_difference=f16u(76),
        maximum_voltage_cell_index=u8(78),
        minimum_voltage_cell_index=u8(79),
        voltages = voltages,
        resistances = [u16(80 + i * 2) for i in range(num_cells)],
        #144
        mos_temperature=mos_temperature,
        temp_status_flag=temp_status_flag,
        #146
        balance_line_resistance_status=f32u(146),
        #150
        voltage=f32u(118 + offset), #150
        power=f32u(122 + offset), #152
        current=-f32s(126 + offset), #158
        #162 T1
        #164 T2
        #166 protection bit
        alarm=u32(166),
        #170
        balance_current=i16(138 + offset) / 1000, #170
        #172 discard
        balance_state=u8(172),
        #173
        soc=buf[141 + offset],
        #174
        charge=f32u(142 + offset),  # "remaining capacity"
        #178
        capacity=f32u(146 + offset),  # computed capacity (starts at self.capacity, which is user-defined),
        #182
        num_cycles=u32(150 + offset),
        #186
        cycle_capacity=f32u(154 + offset),  # total charge TODO rename cycle charge
        temperatures=temperatures,
        temp_min=temp_min,
        temp_max=temp_max,
        temp_moyenne=temp_moyenne,


        # 146 charge_full (see above)

        switches=dict(
            charge=bool(buf_set[118]),
            discharge=bool(buf_set[122]),
            balance=bool(buf_set[126]),
            **(dict(float_charge=bool(buf_set[283] & 2)) if has_float_charger else {}),
        ) if buf_set else {},
        #  #buf[166 + offset]),  charge FET state
        # buf[167 + offset]), discharge FET state
        # 184
        uptime=float(u32(162 + offset)),  # seconds
        timestamp=t_buf,
    )

def s_decode_O1(status_data: bytearray) -> SettingsData:
    """
    Decodes the status data received from a device into a ``SettingsData`` object. The function extracts various 
    settings and operational parameters from the provided binary data. Each of the extracted parameters 
    represents a specific operating condition or configuration for the system.

    :param status_data: Binary data representing the device's status and settings.
    :type status_data: bytes
    :return: A ``SettingsData`` object containing all the decoded parameters.
    :rtype: SettingsData
    """


    i32 = lambda i: int.from_bytes(status_data[i:(i + 4)], byteorder='little', signed=True)
    u32 = lambda i: int.from_bytes(status_data[i:(i + 4)], byteorder='little', signed=False)

    vol_smart_sleep = i32(6) / 1000
    vol_cell_uv = i32(10) / 1000
    vol_cell_uvpr = i32(14) / 1000
    vol_cell_ov = i32(18) / 1000
    vol_cell_ovpr = i32(22) / 1000
    vol_balan_trig = i32(26) / 1000
    vol_soc_full = i32(30) / 1000
    vol_soc_empty = i32(34) / 1000
    vol_rcv = i32(38) / 1000  # Voltage Cell Request Charge Voltage (RCV)
    vol_rfv = i32(42) / 1000  # Voltage Cell Request Float Voltage (RFV)
    vol_sys_pwr_off = i32(46) / 1000
    max_battery_charge_current = i32(50) / 1000
    tim_bat_cocp_dly = i32(54)
    tim_bat_cocpr_dly = i32(58)
    max_battery_discharge_current = i32(62) / 1000
    tim_bat_dc_ocp_dly = i32(66)
    tim_bat_dc_ocpr_dly = i32(70)
    tim_bat_scpr_dly = i32(74)
    cur_balan_max = i32(78) / 1000
    tmp_bat_cot = u32(82) / 10
    tmp_bat_cotpr = u32(86) / 10
    tmp_bat_dc_ot = u32(90) / 10
    tmp_bat_dc_otpr = u32(94) / 10
    tmp_bat_cut = u32(98) / 10
    tmp_bat_cutpr = u32(102) / 10
    tmp_mos_ot = u32(106) / 10
    tmp_mos_otpr = u32(110) / 10
    cell_count = i32(114)
    bat_charge_en = i32(118)
    bat_dis_charge_en = i32(122)
    balan_en = i32(126)
    
    capacity = i32(130) / 1000
    scp_delay = i32(134)
    start_bal_vol = i32(138) / 1000  # Start Balance Voltage

    charge= bool(bat_charge_en)
    discharge= bool(bat_dis_charge_en)
    balance=bool(balan_en)
    float_charge=bool(status_data[283] & 2)
    
    # balancer enabled
    address = int(status_data[270])

    logger.debug("vol_smart_sleep: " + str(vol_smart_sleep))
    logger.debug("vol_cell_uv: " + str(vol_cell_uv))
    logger.debug("vol_cell_uvpr: " + str(vol_cell_uvpr))
    logger.debug("vol_cell_ov: " + str(vol_cell_ov))
    logger.debug("vol_cell_ovpr: " + str(vol_cell_ovpr))
    logger.debug("vol_balan_trig: " + str(vol_balan_trig))
    logger.debug("vol_soc_full: " + str(vol_soc_full))
    logger.debug("vol_soc_empty: " + str(vol_soc_empty))
    logger.debug("vol_rcv: " + str(vol_rcv))
    logger.debug("vol_rfv: " + str(vol_rfv))
    logger.debug("vol_sys_pwr_off: " + str(vol_sys_pwr_off))
    logger.debug("cur_bat_coc: " + str(max_battery_charge_current))
    logger.debug("tim_bat_cocp_dly: " + str(tim_bat_cocp_dly))
    logger.debug("tim_bat_cocpr_dly: " + str(tim_bat_cocpr_dly))
    logger.debug("cur_bat_dc_oc: " + str(max_battery_discharge_current))
    logger.debug("tim_bat_dc_ocp_dly: " + str(tim_bat_dc_ocp_dly))
    logger.debug("tim_bat_dc_ocpr_dly: " + str(tim_bat_dc_ocpr_dly))
    logger.debug("tim_bat_scpr_dly: " + str(tim_bat_scpr_dly))
    logger.debug("cur_balan_max: " + str(cur_balan_max))
    logger.debug("tmp_bat_cot: " + str(tmp_bat_cot))
    logger.debug("tmp_bat_cotpr: " + str(tmp_bat_cotpr))
    logger.debug("tmp_bat_dc_ot: " + str(tmp_bat_dc_ot))
    logger.debug("tmp_bat_dc_otpr: " + str(tmp_bat_dc_otpr))
    logger.debug("tmp_bat_cut: " + str(tmp_bat_cut))
    logger.debug("tmp_bat_cutpr: " + str(tmp_bat_cutpr))
    logger.debug("tmp_mos_ot: " + str(tmp_mos_ot))
    logger.debug("tmp_mos_otpr: " + str(tmp_mos_otpr))
    logger.debug("cell_count: " + str(cell_count))
    logger.debug("charge: " + str(charge))
    logger.debug("discharge: " + str(discharge))
    logger.debug("balance: " + str(balance))
    logger.debug("float_charge: " + str(float_charge))
    logger.debug("cap_bat_cell: " + str(capacity))
    logger.debug("scp_delay: " + str(scp_delay))
    logger.debug("start_bal_vol: " + str(start_bal_vol))
    # 55 aa eb 90 01 05 ac 0d 00 00 14 0a 00 00 be 0a 00 00 42 0e 00 00 ac 0d 00 00 05 00 00 00 06 0e 00 00 8c 0a 00 00 10 0e 00 00 ac 0d 00 00 c4 09 00 00 f0 49 02 00 03 00 00 00 3c 00 00 00 f0 49 02 00 2c 01 00 00 3c 00 00 00 05 00 00 00 d0 07 00 00 bc 02 00 00 58 02 00 00 bc 02 00 00 58 02 00 00 38 ff ff ff 9c ff ff ff e8 03 00 00 20 03 00 00 10 00 00 00 01 00 00 00 01 00 00 00 01 00 00 00 68 a7 04 00 dc 05 00 00 7a 0d 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 60 e3 16 00 11 32 3c 32 18 fe ff ff ff 9f e9 1d 02 00 00 00 00 d4 01 10 16 1e 00 01 65 87
    # 55 aa eb 90 01 05 ac 0d 00 00 14 0a 00 00 be 0a 00 00 42 0e 00 00 ac 0d 00 00 05 00 00 00 06 0e 00 00 8c 0a 00 00 10 0e 00 00 ac 0d 00 00 c4 09 00 00 f0 49 02 00 03 00 00 00 3c 00 00 00 f0 49 02 00 2c 01 00 00 3c 00 00 00 05 00 00 00 d0 07 00 00 bc 02 00 00 58 02 00 00 bc 02 00 00 58 02 00 00 14 00 00 00 46 00 00 00 20 03 00 00 bc 02 00 00 10 00 00 00 01 00 00 00 01 00 00 00 01 00 00 00 68 a7 04 00 dc 05 00 00 e4 0c 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00 60 e3 16 00 10 32 3c 32 18 fe ff ff ff 9f e9 1d 02 00 00 00 00 9c 02 10 16 1e 00 01 65 b4
    return SettingsData(
        address=address,
        vol_smart_sleep=vol_smart_sleep,
        vol_cell_uv=vol_cell_uv,
        vol_cell_uvpr=vol_cell_uvpr,
        vol_cell_ov=vol_cell_ov,
        vol_cell_ovpr=vol_cell_ovpr,
        vol_balan_trig=vol_balan_trig,
        vol_soc_full=vol_soc_full,
        vol_soc_empty=vol_soc_empty,
        vol_rcv=vol_rcv,
        vol_rfv=vol_rfv,
        vol_sys_pwr_off=vol_sys_pwr_off,
        max_battery_charge_current=max_battery_charge_current,
        tim_bat_cocp_dly=tim_bat_cocp_dly,
        tim_bat_cocpr_dly=tim_bat_cocpr_dly,
        max_battery_discharge_current=max_battery_discharge_current,
        tim_bat_dc_ocp_dly=tim_bat_dc_ocp_dly,
        tim_bat_dc_ocpr_dly=tim_bat_dc_ocpr_dly,
        tim_bat_scpr_dly=tim_bat_scpr_dly,
        cur_balan_max=cur_balan_max,
        tmp_bat_cot=tmp_bat_cot,
        tmp_bat_cotpr=tmp_bat_cotpr,
        tmp_bat_dc_ot=tmp_bat_dc_ot,
        tmp_bat_dc_otpr=tmp_bat_dc_otpr,
        tmp_bat_cut=tmp_bat_cut,
        tmp_bat_cutpr=tmp_bat_cutpr,
        tmp_mos_ot=tmp_mos_ot,
        tmp_mos_otpr=tmp_mos_otpr,
        cell_count=cell_count,
        charge=charge,
        discharge=discharge,
        balance=balance,
        float_charge=float_charge,
        capacity=capacity,
        scp_delay=scp_delay,
        start_bal_vol=start_bal_vol,
        switches=dict(
            charge=bool(status_data[118]),
            discharge=bool(status_data[122]),
            balance=bool(status_data[126]),
            float_charge=bool(status_data[283] & 2)
        )
    )


def decode_info(buf, logger):
    psk = read_str(buf, 6 + 16 + 8 + 16 + 40 + 11)
    if psk:
        logger.info("PSK = '%s' (Note that anyone within BLE range can read this!)", psk)
    u8 = lambda i: int.from_bytes(buf[i:(i + 1)], byteorder='little', signed=True)

    di = DeviceInfo(mnf="JK",
                    model=read_str(buf, 6),
                    hw_version=read_str(buf, 6 + 16),
                    sw_version=read_str(buf, 6 + 16 + 8),
                    name=read_str(buf, 6 + 16 + 8 + 16),
                    sn=read_str(buf, 6 + 16 + 8 + 16 + 40),
                    psk=psk,
                    address=u8(300)
                    )
    has_float_charger = ('PB2A16S' in di.model) or ('PB1A16S' in di.model)
    di.float_charger = has_float_charger
    return di