import asyncio
import logging
import sys
from struct import unpack_from
from typing import Union

import serial

from ..bms import BmsSample
from ..bms_ble.plugins.basebms import crc16_modbus2

logger = logging.getLogger("JKSerialIO")

class JKSerialIO:
    def __init__(self, port: str, baud: int) -> None:
        self._serial_port = port
        self._serial_baud = baud
        self.no_data_counter = 0
        self.command_status =   b"\x10\x16\x20\x00\x01\x02\x00\x00"
        self.command_settings = b"\x10\x16\x1e\x00\x01\x02\x00\x00"
        self.command_about =    b"\x10\x16\x1c\x00\x01\x02\x00\x00"
        self.shutdown = False
        self.cmd_line: bytearray = bytearray()

    def generateCmd(self, command, address):
        modbus_msg = bytes(address, "utf-8")
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
    
    def pattern_matched(self, data):
        if len(data) >= 3:
            return (
                data[-3] == 0x01 and
                data[-2] == 0x04 and
                data[-1] == 0x4B
            )
        return False

    async def read_serialport_data(
            self,
            ser: serial.Serial,
            command: bytearray,
            callback,
            length_fixed: Union[int, None] = 308,
    
    
    ):
        """
        Read data from a serial port
    
        :param callback: 
        :param ser: Serial port
        :param command: Command to send
        :param length_pos: Position of the length byte
        :param length_check: Length of the checksum
        :param length_fixed: Fixed length of the data, if not set it will be read from the data
        :param length_size: Size of the length byte, can be "B", "H", "I" or "L"
        :param battery_online: Boolean indicating if the battery is online
        :return: Data read from the serial port
        """
        try:
            if command:
                self.sendBMSCommand(ser, command)
    
            cmd_line = bytearray()
            counter = 0
            while not self.shutdown:
                counter += 1
                bms = ser
                if ser.inWaiting() >= 4 :
                    b = bms.read(1)
                    logger.debug(f"BMS data received {b.hex()}")
                    if b.hex() == '55' : # header byte 1
                        b = bms.read(1)
                        logger.debug(f"BMS data received {b.hex()}")
                        if b.hex() == 'aa' : # header byte 2
                            if cmd_line.__len__()>0:
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
                                    if response_line.__len__()+l > 306:
                                        l = 306-response_line.__len__()
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
    
                            # The actual data we need
                            await callback(data)
                            counter = 0
                        else:
                            await self.cmd_line_11(callback)
                            self.cmd_line.extend(b)
                    else:
                        await self.cmd_line_11(callback)
                        self.cmd_line.extend(b)
                else:
                    await asyncio.sleep(0.01)
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
    u16 = lambda i: int.from_bytes(buf[i:(i + 2)], byteorder='little', signed=True)
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
        mos_temperature=i16((112 if is_new_11fw_32s else 134) + offset) / 10,
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

def s_decode_O1(status_data):
    vol_smart_sleep = unpack_from("<i", status_data, 6)[0] / 1000
    vol_cell_uv = unpack_from("<i", status_data, 10)[0] / 1000
    vol_cell_uvpr = unpack_from("<i", status_data, 14)[0] / 1000
    vol_cell_ov = unpack_from("<i", status_data, 18)[0] / 1000
    vol_cell_ovpr = unpack_from("<i", status_data, 22)[0] / 1000
    vol_balan_trig = unpack_from("<i", status_data, 26)[0] / 1000
    vol_soc_full = unpack_from("<i", status_data, 30)[0] / 1000
    vol_soc_empty = unpack_from("<i", status_data, 34)[0] / 1000
    vol_rcv = unpack_from("<i", status_data, 38)[0] / 1000  # Voltage Cell Request Charge Voltage (RCV)
    vol_rfv = unpack_from("<i", status_data, 42)[0] / 1000  # Voltage Cell Request Float Voltage (RFV)
    vol_sys_pwr_off = unpack_from("<i", status_data, 46)[0] / 1000
    max_battery_charge_current = unpack_from("<i", status_data, 50)[0] / 1000
    tim_bat_cocp_dly = unpack_from("<i", status_data, 54)[0]
    tim_bat_cocpr_dly = unpack_from("<i", status_data, 58)[0]
    max_battery_discharge_current = unpack_from("<i", status_data, 62)[0] / 1000
    tim_bat_dc_ocp_dly = unpack_from("<i", status_data, 66)[0]
    tim_bat_dc_ocpr_dly = unpack_from("<i", status_data, 70)[0]
    tim_bat_scpr_dly = unpack_from("<i", status_data, 74)[0]
    cur_balan_max = unpack_from("<i", status_data, 78)[0] / 1000
    tmp_bat_cot = unpack_from("<I", status_data, 82)[0] / 10
    tmp_bat_cotpr = unpack_from("<I", status_data, 86)[0] / 10
    tmp_bat_dc_ot = unpack_from("<I", status_data, 90)[0] / 10
    tmp_bat_dc_otpr = unpack_from("<I", status_data, 94)[0] / 10
    tmp_bat_cut = unpack_from("<I", status_data, 98)[0] / 10
    tmp_bat_cutpr = unpack_from("<I", status_data, 102)[0] / 10
    tmp_mos_ot = unpack_from("<I", status_data, 106)[0] / 10
    tmp_mos_otpr = unpack_from("<I", status_data, 110)[0] / 10
    cell_count = unpack_from("<i", status_data, 114)[0]
    bat_charge_en = unpack_from("<i", status_data, 118)[0]
    bat_dis_charge_en = unpack_from("<i", status_data, 122)[0]
    balan_en = unpack_from("<i", status_data, 126)[0]
    capacity = unpack_from("<i", status_data, 130)[0] / 1000
    scp_delay = unpack_from("<i", status_data, 134)[0]
    start_bal_vol = unpack_from("<i", status_data, 138)[0] / 1000  # Start Balance Voltage

    # balancer enabled
    balance_fet = True if balan_en != 0 else False

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
    logger.debug("bat_charge_en: " + str(bat_charge_en))
    logger.debug("bat_dis_charge_en: " + str(bat_dis_charge_en))
    logger.debug("balan_en: " + str(balan_en))
    logger.debug("cap_bat_cell: " + str(capacity))
    logger.debug("scp_delay: " + str(scp_delay))
    logger.debug("start_bal_vol: " + str(start_bal_vol))
    
    return dict({
        "vol_smart_sleep": vol_smart_sleep,
        "vol_cell_uv": vol_cell_uv,
        "vol_cell_uvpr": vol_cell_uvpr,
        "vol_cell_ov": vol_cell_ov,
        "vol_cell_ovpr": vol_cell_ovpr,
        "vol_balan_trig": vol_balan_trig,
        "vol_soc_full": vol_soc_full,
        "vol_soc_empty": vol_soc_empty,
        "vol_rcv": vol_rcv,
        "vol_rfv": vol_rfv,
        "vol_sys_pwr_off": vol_sys_pwr_off,
        "max_battery_charge_current": max_battery_charge_current,
        "tim_bat_cocp_dly": tim_bat_cocp_dly,
        "tim_bat_cocpr_dly": tim_bat_cocpr_dly,
        "max_battery_discharge_current": max_battery_discharge_current,
        "tim_bat_dc_ocp_dly": tim_bat_dc_ocp_dly,
        "tim_bat_dc_ocpr_dly": tim_bat_dc_ocpr_dly,
        "tim_bat_scpr_dly": tim_bat_scpr_dly,
        "cur_balan_max": cur_balan_max,
        "tmp_bat_cot": tmp_bat_cot,
        "tmp_bat_cotpr": tmp_bat_cotpr,
        "tmp_bat_dc_ot": tmp_bat_dc_ot,
        "tmp_bat_dc_otpr": tmp_bat_dc_otpr,
        "tmp_bat_cut": tmp_bat_cut,
        "tmp_bat_cutpr": tmp_bat_cutpr,
        "tmp_mos_ot": tmp_mos_ot,
        "tmp_mos_otpr": tmp_mos_otpr,
        "cell_count": cell_count,
        "bat_charge_en": bat_charge_en,
        "bat_dis_charge_en": bat_dis_charge_en,
        "balan_en": balan_en,
        "capacity": capacity,
        "scp_delay": scp_delay,
        "start_bal_vol": start_bal_vol,
        "balance_fet": balance_fet,
        
    })