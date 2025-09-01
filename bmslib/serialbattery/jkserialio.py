import asyncio
import queue
import sys
import time
from typing import Union, Optional

import serial
from crcmod import crcmod

from mqtt_util import is_none_or_nan
from ..bms import BmsSample, SettingsData, DeviceInfo, BmsSetSwitch
from ..sampling import PeriodicBoolSignal, BmsSampler
from ..util import to_hex_str, read_str, get_logger_err, get_logger_child

def crc16_modbus2(data: bytes) -> bytearray:
    i = crc16_modbus(data)
    return bytearray([i & 0xff, (i >> 8) & 0xff])

crc16_modbus = crcmod.mkCrcFun(0x18005, rev=True, initCrc=0xFFFF, xorOut=0x0000)

logger = get_logger_child("JKSerialIO")
logger_err = get_logger_err()

float_charge_flag = 0x0200
heating_flag = 0x0001
display_flag = 0x0010

class JKSerialIO:
    """
    Manages the serial communication for JK BMS (Battery Management System) using user-defined
    parameters such as port, baud rate, number of batteries, and master-slave configuration. This
    class provides functionality for handling commands and interacting with the connected system.

    :ivar command_status: A predefined command for fetching the status of the system.
    :type command_status: bytes
    :ivar command_settings: A predefined command for fetching the settings of the system.
    :type command_settings: bytes
    :ivar command_about: A predefined command for fetching information about the system.
    :type command_about: bytes
    :ivar shutdown: Indicates whether the system is in a shutdown state.
    :type shutdown: bool
    :ivar cmd_line: A dynamic buffer used for assembling command data to be transmitted.
    :type cmd_line: bytearray
    :ivar count_bat: Number of batteries managed by the BMS.
    :type count_bat: int
    :ivar master: Determines if the system is configured as a master.
    :type master: bool
    :ivar cmd: A queue for storing commands to be executed in master mode.
    :type cmd: queue.Queue
    """
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
        self.cmd = queue.Queue()

    def get_command_switch(self, switch: str, state: bool, sampler: BmsSampler) -> bytearray | None:
        """
        self.command_switch_on = switches=dict(
            charge=      "10 10 70 00 02 04 00 00 00 01",
            discharge=   "10 10 74 00 02 04 00 00 00 01",
            balance=     "10 10 78 00 02 04 00 00 00 01",
            float_charge="10 11 14 00 01 02 32 51",  # 0200 not work it's common flag with heating
            heating=     "10 11 14 00 01 02 32 51",  # 0001 not work it's common flag with heating
            display=     "10 11 14 00 01 02 32 51"   # 0010 not work it's common flag with heating'
        )
        self.command_switch_off = switches=dict(
            charge=      "10 10 70 00 02 04 00 00 00 00",
            discharge=   "10 10 74 00 02 04 00 00 00 00",
            balance=     "10 10 78 00 02 04 00 00 00 00",
            float_charge="10 11 14 00 01 02 30 51", # not work it's common flag with heating
            heating=     "10 11 14 00 01 02 32 5O",  # not work it's common flag with heating
            display=     "10 11 14 00 01 02 32 41"
        )
        01 10 11 14 00 01 02 32 51 70 D9
01 10 16 1E 00 01 02 00 00 D2 2F
01 10 16 1C 00 01 02 00 00 D3 CD
01 10 11 14 00 01 02 32 51 70 D9

charge off
[20:10:09,291] 01 10 10 70 00 02 04 00 00 00 00 39 4B
[20:10:09,303] 01 10 10 70 00 02 44 D3

charge on
[20:10:35,337] 01 10 10 70 00 02 04 00 00 00 01 F8 8B
reponse
[20:10:35,358] 01 10 10 70 00 02 44 D3

discharge

[20:12:54,052] 01 10 10 74 00 02 04 00 00 00 00 38 B8
[20:12:54,072] 01 10 10 74 00 02 05 12


[20:13:09,531] 01 10 10 74 00 02 04 00 00 00 01 F9 78
[20:13:09,550] 01 10 10 74 00 02 05 12

balance

[20:16:31,322] 01 10 10 78 00 02 04 00 00 00 00 38 ED
[20:16:31,349] 01 10 10 78 00 02 C5 11

[20:16:33,536] 01 10 10 78 00 02 04 00 00 00 01 F9 2D
[20:16:33,551] 01 10 10 78 00 02 C5 11


floating

[20:18:45,695] 01 10 11 14 00 01 02 30 51 71 B9
[20:18:45,713] 01 10 11 14 00 01 44 F1


[20:18:46,651] 01 10 11 14 00 01 02 32 51 70 D9
[20:18:46,668] 01 10 11 14 00 01 44 F1


heating

[20:22:18,940] 01 10 11 14 00 01 02 32 50 B1 19
[20:22:21,626] 01 10 11 14 00 01 02 32 51 70 D9

display

off
[20:24:04,824] 01 10 11 14 00 01 02 32 41 71 15
[20:24:04,849] 01 10 11 14 00 01 44 F1
on
[20:24:20,208] 01 10 11 14 00 01 02 32 51 70 D9
[20:24:20,228] 01 10 11 14 00 01 44 F1


loggin
01 10 16 24 00 01 02 00 00 D7 75
55 AA EB 90 05 05 35 01 00 00 08 EA 8B 41 01 3B 78 D8 5B 01 03 92 D8 5B 01 04 1A D9 5B 01 05 2E D9 5B 01 06 36 DB 5B 01 03 37 DB 5B 01 04 3C DB 5B 01 03 3D DB 5B 01 04 8E 0E 92 00 06 D3 0E 92 00 04 D1 F0 95 00 64 78 F7 95 00 12 F0 50 98 00 65 0E 52 98 00 12 2A 52 98 00 64 A7 54 98 00 12 D1 54 98 00 64 14 5F 98 00 12 CD 87 99 00 64 92 88 99 00 12 B2 88 99 00 64 53 8A 99 00 12 75 8A 99 00 64 B7 92 99 00 12 E4 92 99 00 64 2A 97 99 00 12 5D 97 99 00 64 32 CC 99 00 03 38 CC 99 00 04 91 D3 99 00 12 80 D8 99 00 03 81 D8 99 00 05 38 D9 99 00 04 39 D9 99 00 06 62 C3 A2 00 64 4C 07 A3 00 12 CB 99 B2 00 64 F9 A0 B2 00 12 12 B4 C3 00 6B D6 B4 C3 00 12 5F BE D8 00 71 0E C0 D8 00 12 88 D7 DC 00 6D 46 D8 DC 00 12 44 0A 1C 01 65 E9 0A 1C 01 12 9D E4 1F 01 6C 64 E8 1F 01 12 EA 8B 41 01 3B 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 9E 01 10 16 24 00 01 45 8A


        :param sampler: 
        :param switch: 
        :param state: 
        :return: 
        """
        # "10 10 70 00 02 04 00 00 00 01",
        def cmd_10(c):
            return bytearray([0x10, 0x10, c, 0x00, 0x02, 0x04, 0x00, 0x00, 0x00, 0x01 if state else 0x00])

#10 11 14 00 01 02 30 51
        def cmd_11(f):
            v = sampler.setting.status_282 | f if state else sampler.setting.status_282 & f
            return bytearray([0x10, 0x11, 0x14, 0x00, 0x02, 0x02, v>>8 & 0xFF, v & 0xFF])
        
        match switch:
            case 'charge':
                return cmd_10(0x70)

            case 'discharge':
                return cmd_10(0x74)

            case 'balance':
                return cmd_10(0x78)
            
            case 'float_charge':
                return cmd_11(float_charge_flag)
            case 'heating':
                return cmd_11(heating_flag)
            case 'display':
                return cmd_11(display_flag)
        return None

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

    def send_bms_command(self, ser, cmd_bytes: bytearray):
        """
        Sends a Battery Management System (BMS) command via a serial interface. 

        This function takes a serialized command represented by a sequence of 
        bytes and transmits it over the given serial communication channel. Each 
        byte is formatted as a hexadecimal string before being sent.

        :param ser: Serial interface object used for communication.
        :type ser: object
        :param cmd_bytes: Sequence of bytes representing the command to be sent.
        :type cmd_bytes: bytearray
        :return: None
        """
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
            logger_err.error(e)
            if ser is not None:
                # close the serial port if it was opened
                ser.close()
                logger_err.error("Serial port closed")
            else:
                logger_err.error("Serial port could not be opened")

        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger_err.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")

    async def read_trame_55_aa(self, ser: serial.Serial, callback, length_fixed: int = 308, timeout=0.5):
        """
        Read and process data frames of a specific format from a serial device asynchronously.

        This function reads data frames beginning with a specific header ('55aa') from a serial
        device. It verifies the data's integrity using CRC checks and then processes it using
        the provided callback function. The length of the data frame is either specified or
        determined during runtime. The function operates with a timeout and ensures continuous
        reading until the timeout is reached or the `shutdown` attribute becomes `True`.

        :param ser: The serial port object for reading data.
        :type ser: serial.Serial
        :param callback: The asynchronous callback function to process the read data.
        :param length_fixed: The fixed length of data to be read (default is 308 bytes, including
            header and length bytes).
        :type length_fixed: int
        :param timeout: The timeout for the read operation in seconds (default is 0.5 seconds).
        :type timeout: float
        :return: A boolean indicating whether the data was successfully read and processed.
        :rtype: bool
        """
        bms = ser
        t_now = time.time_ns()
        ns_timeout= timeout*1e9
        while not self.shutdown:
            t_current = time.time_ns() - t_now
            if t_current > ns_timeout:
                logger_err.error(f"Timeout to read trame {t_current}")
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
                        while no_data_counter < 20:  # Try up to 20 times with no new data before exiting wait 0.2
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

                        if no_data_counter == 20:
                            logger_err.error(f"Missing received in {t_current}: {len(data)}")
                            logger_err.error(f"trame {to_hex_str(data)}")
    
                        trame1 = data[0:300]
                        crc = sum(trame1[:-1]) & 0xFF
                        trame2 = data[300:]
                        logger.debug(f"trame1 {to_hex_str(data)}")

                        t_current = time.time_ns() - t_now
                        
                        if crc != trame1[-1] & 0xFF:
                            logger_err.error(f"CRC error in trame1 {crc:02X} != {trame1[-1]:02X}")
                            logger_err.error(f"Read trame in {t_current}")
                            logger_err.error(f"trame {to_hex_str(data)}")
                            return False
    
                        crc_cmd = crc16_modbus2(trame2[:-2])
                        if crc_cmd != trame2[-2:]:
                            logger_err.error(f"CRC error in trame2 {crc_cmd[0]:02X} {crc_cmd[1]:02X} != {trame2[-2]:02X} {trame2[-1]:02X}")
                            logger_err.error(f"Read trame in {t_current}")
                            logger_err.error(f"trame {to_hex_str(data)}")
                            return False

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

    async def read_cmd_answer(self, ser: serial.Serial, cmd_answer: bytes, timeout=0.5):
        """
        Asynchronously reads and checks if the incoming data matches the expected command answer within a specified timeout.

        This function continuously reads and compares data from the serial port with the expected command answer, until
        either the timeout is reached or a matching response is found. If the timeout is exceeded without a match, it
        returns `False`.

        :param ser: Serial port object used to read the incoming data
        :type ser: serial.Serial
        :param cmd_answer: Expected response bytes to be matched
        :type cmd_answer: bytes
        :param timeout: Timeout value in seconds for waiting for the command response, defaults to 0.5
        :type timeout: float, optional
        :return: Returns False if the timeout was reached, or the function is interrupted by shutdown
        :rtype: bool
        """
        t_now = time.time_ns()
        ns_timeout= timeout*1e9
        while not self.shutdown:
            t_current = time.time_ns() - t_now
            if t_current > ns_timeout:
                logger_err.error(f"Timeout to read trame {t_current}")
                return False
    
            if ser.in_waiting >= len(cmd_answer) :
                b = ser.read(len(cmd_answer))
                resp = b == cmd_answer
                if not resp:
                    logger_err.error(f"Missing response in {t_current}: {len(b)} != {len(cmd_answer)}")
                    logger_err.error(f"cmd_answer {to_hex_str(cmd_answer)}")
                    logger_err.error(f"receive answer {to_hex_str(b)}")
                return resp
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
        Asynchronously reads data from a serial port in master mode and handles commands for 
        all slaves, using specific periodic intervals for different command types, such as 
        status, settings, and general information.

        The function reads data based on predefined periodic signals and processes commands. 
        It ensures proper communication with all connected slaves, sending and reading commands 
        in a synchronized manner. 

        :param ser: Serial port object to communicate with the slaves.
        :type ser: serial.Serial
        :param callback: Callback function to handle the processed data from the slaves.
        :param length_fixed: Fixed length of data packets to be read from the serial port, 
            defaults to 308.
        :type length_fixed: int
        :return: None
        :rtype: None
        :raises serial.SerialException: If an error occurs during serial port communication.
        :raises Exception: If any unexpected error is encountered during operation.
        """
        try:
            counter = 0
            period_status = PeriodicBoolSignal(period=5)

            while not self.shutdown:
                logger.info(f" period_status")

                if period_status.counter % 720 == 0 :
                    logger.info(f" period_about")
                    counter = await self.send_cmd_and_read_all_slave(callback, self.command_about, counter, length_fixed, ser)
                
                if period_status.counter % 6 == 0:
                    logger.info(f" period_setting")
                    counter = await self.send_cmd_and_read_all_slave(callback, self.command_settings, counter, length_fixed, ser)
               
                counter = await self.send_cmd_and_read_all_slave(callback, self.command_status, counter, length_fixed, ser)

                counter = await self.send_command_from_queue(callback, counter, length_fixed, ser)
                counter += 1
                
                if counter>800:
                    logger_err.error(f"counter {counter} > 800")
                    await callback(self.cmd_line)
                    return
                await period_status.sleep()

        except serial.SerialException as e:
            logger_err.error(e)
            return

        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger_err.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
            return

    async def send_command_from_queue(self, callback, counter, length_fixed: int, ser):
        """
        Asynchronous method to send commands from a queue to a specific device. This 
        function processes commands retrieved from the command queue and sends them to the 
        corresponding target, while handling retries, fixed-length command settings, and response 
        handling via a callback.

        The method loops until the `shutdown` attribute becomes True, enabling termination. Commands 
        from the queue are processed and sent using the appropriate protocol specified in the 
        implementation.

        :param callback: A callable object or function that is triggered or invoked when specific 
             operations are complete or responses are received.
        :param counter: An integer counter is used to track the number of errors, 
            typically reset after sending commands successfully.
        :param length_fixed: A fixed length of commands used for maintaining consistency in 
            communication protocol.
        :param ser: A serial communication interface used for sending data to targeted devices or systems.
        :return: Returns the updated counter, indicating the progress or state of the sent commands 
            after processing the queue.
        """
        while not self.shutdown:
            try:
                (cmd, address) = self.cmd.get_nowait()
                logger.debug(f"Send command {cmd} from queue to device {address}")
                counter = await self.send_cmd_one_slave(address, cmd, counter, length_fixed, ser, callback, 6)
                await asyncio.sleep(0.01)
                counter = await self.send_cmd_one_slave(address, self.command_settings, counter, length_fixed,
                                                        ser, callback)
            except queue.Empty:
                return counter
        return counter

    async def send_cmd_and_read_all_slave(self, callback, command:bytes, counter:int, length_fixed: int, ser):
        """
        Sends a command to all slaves and reads their responses.

        This method iterates through the slaves, sends the specified command to each,
        and processes their responses. The `callback` function is triggered upon
        completion of each slave's command execution. 

        :param callback: A callable function to be triggered after processing the 
            command for each slave.
        :param command: The command to be sent to each slave as bytes.
        :param counter: An integer counter is used to track the number of errors, 
            typically reset after sending commands successfully.
        :param length_fixed: The fixed length of the expected response from the 
            slaves.
        :param ser: The serial connection instance facilitating communication with 
            slaves.
        :return: The updated counter's value after processing all slaves if it's reset.
        """
        for i in range(1, self.count_bat + 1):
            counter = await self.send_cmd_one_slave(i, command, counter, length_fixed, ser, callback)

        return counter

    async def send_cmd_one_slave(self, address: int, command:bytes, counter:int, length_fixed: int, ser, callback, cmd_answer_index: Optional[int] = None):
        """
        Sends a command to a single slave device, with the option to match a specific
        response or read a general response.

        This function generates a command using the specified address and command 
         bytes and sends it to the slave device through the provided serial interface. 
        Optionally, it can wait for a specific answer or handle a generic response.

        :param address: The integer address of the slave device.
        :param command: The bytes represent the command to be sent.
        :param counter: An integer counter is used to track the number of errors.
        :param length_fixed: The fixed length of the frame to be expected in the 
            response.
        :param ser: The serial interface used for communication with the device.
        :param callback: A callback function to handle the received data.
        :param cmd_answer_index: Optional expected response length from command and from the slave device 
            used for validation.
        :return: An integer reflecting the retry counter after command execution.
        """
        cmd = self.generate_cmd(command, address)
        logger.debug(f"cmd {address}/{to_hex_str(cmd)}")
        self.send_bms_command(ser, cmd)
        if cmd_answer_index is not None:
            await asyncio.sleep(0.1)
            cmd_answer =  cmd[0:cmd_answer_index]
            cmd_answer_modbus_msg = bytearray(cmd_answer)
            cmd_answer_modbus_msg += crc16_modbus2(cmd_answer_modbus_msg)
            logger.debug(f"Receive cmd_answer {address}/{to_hex_str(cmd_answer_modbus_msg)}")
            if not await self.read_cmd_answer(ser, bytes(cmd_answer_modbus_msg)):
                logger_err.error(f"cmd_answer {address} not received")
                return counter
        elif await self.read_trame_55_aa(ser, callback, length_fixed, 0.5):
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
        Mode slaves receive data from the serial port and process them.
        Reads data from a serial port asynchronously, processes the data using a
        callback function, and manages communication with a predefined serial command.

        This method sends a predefined command (must be removed) to the serial port and continuously
        monitors incoming data. It waits for data to be read using a callback and ensures
        the read operation adheres to a certain length. If data reception is stalled beyond 
        a threshold, the command line state is returned via the callback.

        :param ser: The serial connection object used for communication.
        :param command: The bytearray command to be sent via the serial port.
        :param callback: The callback function to process or handle the received data.
        :param length_fixed: The fixed length (default is 308) to be used for reading data
                             from the serial port.
        :return: None
        """
        try:
            if command:
                self.send_bms_command(ser, command)
    
            counter = 0
            while not self.shutdown:
                counter += 1
                if await self.read_trame_55_aa(ser, callback, length_fixed, 5):
                    counter = 0
                if counter>800:
                    callback(self.cmd_line)
                    return
    
        except serial.SerialException as e:
            logger_err.error(e)
            return
    
        except Exception:
            (
                exception_type,
                exception_object,
                exception_traceback,
            ) = sys.exc_info()
            file = exception_traceback.tb_frame.f_code.co_filename
            line = exception_traceback.tb_lineno
            logger_err.error(f"Exception occurred: {repr(exception_object)} of type {exception_type} in {file} line #{line}")
            return

    async def cmd_line_11(self, callback):
        """
        Executes a command when the `cmd_line` buffer contains exactly 11 bytes. It calculates
        the CRC-16/Modbus checksum for the first 9 bytes of the `cmd_line` and compares it
        with the last two bytes to verify its integrity. After verification, it invokes the
        provided callback function with the command line data and the result of the checksum
        verification.

        :param callback: A coroutine function to be executed after checksum verification.
            It takes two arguments:
            - The command line data (of type bytearray).
            - A boolean flag indicating whether the checksum matches (True) or not (False).
        :return: None
        """
        if len(self.cmd_line) == 11:
            crc = crc16_modbus2(self.cmd_line[:-2])
            crcGood = self.cmd_line[-2] == crc[0] and self.cmd_line[-1] == crc[1]
            await callback(self.cmd_line, crcGood)
            self.cmd_line = bytearray()
    
    def send_cmd_in_queue(self, cmd:bytes, address):
        """
        Send a command to a specified address by adding it to the internal queue.

        This method enqueues the command and associated address for later processing.
        The queue ensures that commands are handled in the order they are added without
        blocking the caller.

        :param cmd: Command in bytes format to be sent.
        :type cmd: bytes
        :param address: The target address associated with the command.
        :return: None
        """
        self.cmd.put_nowait((cmd, address))


def s_decode_sample(is_new_11fw_32s, logger,
                    num_cells,
                    buf: bytearray, buf_set: bytearray|None,
                    t_buf: float, has_float_charger:bool) -> BmsSample:
    """
    Decodes a sample from a battery management system (BMS). Processes various metrics 
    and parameters related to battery states, voltages, temperatures, and other derived 
    statistics from given buffers.

    :param is_new_11fw_32s: Specifies if the firmware is the new version 11.x format for 32-cell 
        configurations. Defaults to True if not provided.
    :type is_new_11fw_32s: bool or None

    :param logger: Logger instance used for debugging or informational output.
    :type logger: logging.Logger

    :param num_cells: Number of battery cells being monitored in the system.
    :type num_cells: int

    :param buf: Byte array containing the raw BMS data to decode.
    :type buf: bytearray

    :param buf_set: Optional byte array containing additional data related to system-specific 
        parameters, such as switches or flags.
    :type buf_set: bytearray or None

    :param t_buf: Timestamp associated with the current buffer data for temporal tracking.
    :type t_buf: float

    :param has_float_charger: Boolean indicating if the battery system has a float charger 
        present.
    :type has_float_charger: bool

    :return: A populated BmsSample object containing the decoded BMS data, including details 
        about voltages, temperatures, alarms, states, and other battery metrics.
    :rtype: BmsSample
    """
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
# Register Map
# Start address code offset Index Data type Length R/W Data content Content           Unit Note Note
# Address
# Field HEX DEC Type Length               Unit
# 0x1000  0x0000  0   UINT32  4   RW  Entering sleep voltage                              VolSmartSleep       mV
#         0x0004  4   UINT32  4   RW  Cell undervoltage protection                        VolCellUV           mV
#         0x0008  8   UINT32  4   RW  Cell undervoltage protection recovery               VolCellUVPR         mV
#         0x000C  12  UINT32  4   RW  Cell overcharge protection                          VolCellOV           mV
#         0x0010  16  UINT32  4   RW  Cell overcharge protection recovery voltage         VolCellOVPR         mV
#         0x0014  20  UINT32  4   RW  Trigger balanced voltage difference                 VolBalanTrig        mV
#         0x0018  24  UINT32  4   RW  SOC-100% voltage                                    VolSOC100%          mV
#         0x001C  28  UINT32  4   RW  SOC-0% voltage                                      VolSOC0%            mV
#         0x0020  32  UINT32  4   RW  Recommended charging voltage                        VolCellRCV          mV
#         0x0024  36  UINT32  4   RW  Float charge voltage                                VolCellRFV          mV
#         0x0028  40  UINT32  4   RW  Automatic shutdown voltage                          VolSysPwrOff        mV
#         0x002C  44  UINT32  4   RW  Continuous charging current                         CurBatCOC           mA
#         0x0030  48  UINT32  4   RW  Charge overcurrent protection delay                 TIMBatCOCPDly       S
#         0x0034  52  UINT32  4   RW  Charge overcurrent protection release               TIMBatCOCPRDly      S
#         0x0038  56  UINT32  4   RW  Continuous discharge current                        CurBatDcOC          mA
#         0x003C  60  UINT32  4   RW  Discharge overcurrent protection delay              TIMBatDcOCPDly      S
#         0x0040  64  UINT32  4   RW  Discharge overcurrent protection release            TIMBatDcOCPRDly     S
#         0x0044  68  UINT32  4   RW  Short circuit protection release                    TIMBatSCPRDly       S
#         0x0048  72  UINT32  4   RW  Maximum balancing current                           CurBalanMax         mA
#         0x004C  76  INT32   4   RW  Charging over-temperature protection                TMPBatCOT           0.1°C
#         0x0050  80  INT32   4   RW  Charge over temperature recovery                    TMPBatCOTPR         0.1°C
#         0x0054  84  INT32   4   RW  Discharge over temperature protection               TMPBatDcOT          0.1°C
#         0x0058  88  INT32   4   RW  Discharge over temperature recovery                 TMPBatDcOTPR        0.1°C
#         0x005C  92  INT32   4   RW  Charging low temperature protection                 TMPBatCUT           0.1°C
#         0x0060  96  INT32   4   RW  Charging low temperature recovery                   TMPBatCUTPR         0.1°C
#         0x0064  100 INT32   4   RW  MOS over temperature protection                     TMPMosOT            0.1°C
#         0x0068  104 INT32   4   RW  MOS over temperature protection recovery            TMPMosOTPR          0.1°C
#         0x006C  108 UINT32  4   RW  CellCount                                           CellCount           string
#         0x0070  112 UINT32  4   RW  Charging switch                                     BatChargeEN             1: open; 0: close
#         0x0074  116 UINT32  4   RW  Discharge switch                                    BatDisChargeEN          1: open; 0: close
#         0x0078  120 UINT32  4   RW  Balance switch                                      BalanEN                 1: open; 0: close
#         0x007C  124 UINT32  4   RW  Battery design capacity                             CapBatCell          mAH
#         0x0080  128 UINT32  4   RW  Short circuit protection delay                      SCPDelay            us
#         0x0084  132 UINT32  4   RW  Balanced start voltage                              VolStartBalan       mV
#         0x0088  136 UINT32  4   RW  Connection line internal resistance 0               CellConWireRes0     uΩ
#         0x008C  140 UINT32  4   RW  Connection line internal resistance 1               CellConWireRes1     uΩ
#         0x0090  144 UINT32  4   RW  Connection line internal resistance 2               CellConWireRes2     uΩ
#         0x0094  148 UINT32  4   RW  Connection line internal resistance 3               CellConWireRes3     uΩ
#         0x0098  152 UINT32  4   RW  Connection line internal resistance 4               CellConWireRes4     uΩ
#         0x009C  156 UINT32  4   RW  Connection line internal resistance 5               CellConWireRes5     uΩ
#         0x00A0  160 UINT32  4   RW  Connection line internal resistance 6               CellConWireRes6     uΩ
#         0x00A4  164 UINT32  4   RW  Connection line internal resistance 7               CellConWireRes7     uΩ
#         0x00A8  168 UINT32  4   RW  Connection line internal resistance 8               CellConWireRes8     uΩ
#         0x00AC  172 UINT32  4   RW  Connection line internal resistance 9               CellConWireRes9     uΩ
#         0x00B0  176 UINT32  4   RW  Connection line internal resistance 10              CellConWireRes10    uΩ
#         0x00B4  180 UINT32  4   RW  Connection line internal resistance 11              CellConWireRes11    uΩ
#         0x00B8  184 UINT32  4   RW  Connection line internal resistance 12              CellConWireRes12    uΩ
#         0x00BC  188 UINT32  4   RW  Connection line internal resistance 13              CellConWireRes13    uΩ
#         0x00C0  192 UINT32  4   RW  Connection line internal resistance 14              CellConWireRes14    uΩ
#         0x00C4  196 UINT32  4   RW  Connection line internal resistance 15              CellConWireRes15    uΩ
#         0x00C8  200 UINT32  4   RW  Connection line internal resistance 16              CellConWireRes16    uΩ
#         0x00CC  204 UINT32  4   RW  Connection line internal resistance 17              CellConWireRes17    uΩ
#         0x00D0  208 UINT32  4   RW  Connection line internal resistance 18              CellConWireRes18    uΩ
#         0x00D4  212 UINT32  4   RW  Connection line internal resistance 19              CellConWireRes19    uΩ
#         0x00D8  216 UINT32  4   RW  Connection line internal resistance 20              CellConWireRes20    uΩ
#         0x00DC  220 UINT32  4   RW  Connection line internal resistance 21              CellConWireRes21    uΩ
#         0x00E0  224 UINT32  4   RW  Connection line internal resistance 22              CellConWireRes22    uΩ
#         0x00E4  228 UINT32  4   RW  Connection line internal resistance 23              CellConWireRes23    uΩ
#         0x00E8  232 UINT32  4   RW  Connection line internal resistance 24              CellConWireRes24    uΩ
#         0x00EC  236 UINT32  4   RW  Connection line internal resistance 25              CellConWireRes25    uΩ
#         0x00F0  240 UINT32  4   RW  Connection line internal resistance 26              CellConWireRes26    uΩ
#         0x00F4  244 UINT32  4   RW  Connection line internal resistance 27              CellConWireRes27    uΩ
#         0x00F8  248 UINT32  4   RW  Connection line internal resistance 28              CellConWireRes28    uΩ
#         0x00FC  252 UINT32  4   RW  Connection line internal resistance 29              CellConWireRes29    uΩ
#         0x0100  256 UINT32  4   RW  Connection line internal resistance 30              CellConWireRes30    uΩ
#         0x0104  260 UINT32  4   RW  Connection line internal resistance 31              CellConWireRes31    uΩ
#         0x0108  264 UINT32  4   RW  Device address                                      DevAddr H
#         0x010C  268 UINT32  4   RW  Discharge precharge time                            TIMProdischarge     S
#         0x0114  276 UINT16  2   RW  Heating switch                                      HeatEN                  1: open; 0: close BIT0
#                                 RW  Temperature sensor shield                           Disable temp-sensor     1: open; 0: close BIT1
#                                 RW  GPS Heartbeat detection                             GPS Heartbeat           1: open; 0: close BIT2
#                                 RW  Multiplex port function                             Port Switch             1: RS485; 0: CAN BIT3
#                                 RW  The display is always on                            LCD Always On           1: On; 0: Off BIT4
#                                 RW  Dedicated charger identification                    Special Charger         1: open; 0: close BIT5
#                                 RW  Smart sleep                                         SmartSleep              1: open; 0: close BIT6
#                                 RW  Disable parallel current limiting                   DisablePCLModule        1: open; 0: close BIT7
#                                 RW  Data timing storage                                 TimedStoredData         1: open; 0: close BIT8
#                                 RW  Charging floating mode                              ChargingFloatMode       1: open; 0: close BIT9
#         0x0118  280 UINT8   2   RW  Intelligent sleep time                              TIMSmartSleep       H
#                     UINT8       R   Data field enable control 0                         
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
    u16 = lambda i: int.from_bytes(status_data[i:(i + 2)], byteorder='little', signed=False)

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
    cur_balan_max = i32(0x48 + 6) / 1000
    #         0x0048  72  UINT32  4   RW  Maximum balancing current                           CurBalanMax         mA
    #         0x004C  76  INT32   4   RW  Charging over-temperature protection                TMPBatCOT           0.1°C
    #         0x0050  80  INT32   4   RW  Charge over temperature recovery                    TMPBatCOTPR         0.1°C
    #         0x0054  84  INT32   4   RW  Discharge over temperature protection               TMPBatDcOT          0.1°C
    #         0x0058  88  INT32   4   RW  Discharge over temperature recovery                 TMPBatDcOTPR        0.1°C
    #         0x005C  92  INT32   4   RW  Charging low temperature protection                 TMPBatCUT           0.1°C
    #         0x0060  96  INT32   4   RW  Charging low temperature recovery                   TMPBatCUTPR         0.1°C
    #         0x0064  100 INT32   4   RW  MOS over temperature protection                     TMPMosOT            0.1°C
    #         0x0068  104 INT32   4   RW  MOS over temperature protection recovery            TMPMosOTPR          0.1°C
    #         0x006C  108 UINT32  4   RW  CellCount                                           CellCount           string
    tmp_bat_cot = u32(0x4C + 6 ) / 10 # Charging over-temperature protection
    tmp_bat_cotpr = i32(0x50 + 6) / 10 # Charging over-temperature recovery
    tmp_bat_dc_ot = i32(0x54 + 6) / 10 # Discharge over temperature protection
    tmp_bat_dc_otpr = i32(0x58 + 6) / 10 # Discharge over temperature recovery
    tmp_bat_cut = i32(0x5C + 6) / 10 # Charging low temperature protection
    tmp_bat_cutpr = i32(0x60 + 6) / 10 # Charging low temperature recovery
    tmp_mos_ot = i32(0x64 + 6) / 10 # MOS over temperature protection
    tmp_mos_otpr = i32(0x68 + 6) / 10 # MOS over temperature recovery
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

    tim_prodischarge = u32(274)
    
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

    status_282 = u16(282)
    
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
        tim_prodischarge=tim_prodischarge,
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
        status_282 = status_282,
        switches=dict(
            charge=bool(status_data[118]),
            discharge=bool(status_data[122]),
            balance=bool(status_data[126]),
            float_charge=bool(status_282 & float_charge_flag)
        )
    )


def decode_info(buf, logger):
    """
    Decode device information from a given buffer and log the PSK.

    This function extracts various pieces of device information such as model name,
    hardware and software version, device name, serial number, and PSK from the
    input buffer. It also determines whether the device has a float charger 
    based on its model and assigns this information to the returned device object.

    The PSK is logged by the logger provided to the function.

    :rtype: DeviceInfo
    :param buf: The byte buffer containing the encoded device information.
    :type buf: bytes
    :param logger: Logger instance used to log the PSK information.
    :type logger: logging.Logger
    :return: A `DeviceInfo` instance populated with the decoded device information.
    """
    psk = read_str(buf, 6 + 16 + 8 + 16 + 40 + 11)
    if psk:
        logger.debug("PSK = '%s' (Note that anyone within BLE range can read this!)", psk)
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

class JKSerialIOBmsSetSwitch(BmsSetSwitch):
    """
    Handles serial communication with a BMS to set the state of one device slave. 

    This class acts as a bridge between the JKSerialIO communication layer and the
    control logic for managing BMS switches. It allows sending commands to operate
    physical JKBMS, using an associated JKSerialIO object and an BmsSampler (adding by set_bms_sampler).

    :ivar address: The address identifier for communication with the BMS.
    :type address: int
    :ivar jk_serial_io: The JKSerialIO object that manages the serial communication interface.
    :type jk_serial_io: JKSerialIO
    :ivar bms_sampler: Optional reference to a BmsSampler instance for sampling operations.
    :type bms_sampler: Optional[BmsSampler]
    """
    def __init__(self, address: int, jk_serial_io:JKSerialIO ) -> None:
        """
        Initializes an instance of the class with the specified address and a JKSerialIO
        object. This constructor is responsible for setting up the address and managing
        the provided JKSerialIO communication layer. Additionally, it includes an optional
        reference to a BmsSampler instance.

        :param address: The address identifier for the instance.
        :type address: int
        :param jk_serial_io: The JKSerialIO object responsible for serial communication.
        :type jk_serial_io: JKSerialIO
        """
        self.address = address
        self.jk_serial_io = jk_serial_io
        self.bms_sampler: Optional[BmsSampler] = None

    def set_bms_sampler(self, bms_sampler: BmsSampler):
        self.bms_sampler = bms_sampler

    def get_name(self):
        return self.bms_sampler.get_name()

    async def set_switch(self, switch: str, state: bool):
        """
        Send a switch command to the BMS to control a physical switch, usually a MOSFET or relay.
        :param switch:
        :param state:
        :return:
        """
        cmd= self.jk_serial_io.get_command_switch(switch, state, self.bms_sampler)
        if cmd:
            self.jk_serial_io.send_cmd_in_queue(bytes(cmd), self.address)