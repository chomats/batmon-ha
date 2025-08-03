# This script reads the data from a JB BMS over RS-485 and formats
# it for use with https://github.com/BarkinSpider/SolarShed/
import logging
import time
import sys, os, io
import struct

from bmslib.models.jikong import JKBt_32s
# Plain serial... Modbus would have been nice, but oh well. 
from bmslib.serialbattery.jkserialio2 import JKSerialIO4B
#from mppsolar.protocols.jk02_32 import jk02_32

log = logging.getLogger("")
FORMAT = "%(asctime)-15s:%(levelname)s:%(module)s:%(funcName)s@%(lineno)d: %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.DEBUG)
sleepTime = 10

protocol = JKBt_32s(address="test_jk11",name="test_jk11")
protocol.decode_sample(bytearray.fromhex('55 AA EB 90 02 05 03 0E 03 0E 04 0E 05 0E 04 0E 05 0E 03 0E 03 0E 03 0E 05 0E 04 0E 05 0E 03 0E 04 0E 05 0E 04 0E 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 FF FF 00 00 04 0E 02 00 09 00 3D 00 38 00 41 00 47 00 57 00 5B 00 64 00 69 00 7F 00 80 00 6B 00 5F 00 5F 00 52 00 49 00 3B 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 E6 00 00 00 00 00 3C E0 00 00 00 00 00 00 00 00 00 00 DB 00 DF 00 00 00 00 00 00 00 00 63 D1 3A 04 00 C0 45 04 00 09 00 00 00 A6 69 28 00 64 00 00 00 D1 80 AF 00 01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 FF 00 01 00 00 00 93 03 00 00 00 00 35 55 3F 40 00 00 00 00 6C 16 00 00 00 01 01 01 00 06 01 00 33 4A C3 04 00 00 00 00 E6 00 DB 00 DE 00 90 03 4C 23 85 0A 48 00 00 00 80 51 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 FE FF 7F DC 2F 01 01 B0 0F 07 00 00 1B 01 10 16 20 00 01 04 4B'))


try:
    bms = JKSerialIO4B(device_path='/dev/ttyUSB1',serial_baud=115200)
    bms.timeout  = 0.2
except:
    print("BMS not found.")

# The hex string composing the command, including CRC check etc.
# See also: 
# - https://github.com/syssi/esphome-jk-bms
# - https://github.com/NEEY-electronic/JK/tree/JK-BMS
# - https://github.com/Louisvdw/dbus-serialbattery

def sendBMSCommand(cmd_string):
    cmd_bytes = bytearray.fromhex(cmd_string)
    for cmd_byte in cmd_bytes:
        hex_byte = ("{0:02x}".format(cmd_byte))
        bms.write(bytearray.fromhex(hex_byte))
    return

# This could be much better, but it works.
def readBMS():
    try:

        # Read all command
        response = bms.send_and_receive(full_command=bytearray.fromhex('01 10 16 20 00 01 02 00 00 D6 F1'))
#
        result = protocol.decode_sample(response)
        log.debug(result)
# 55 AA EB 90 02 05 03 0E 03 0E 04 0E 05 0E 04 0E 05 0E 03 0E 03 0E 03 0E 05 0E 04 0E 05 0E 03 0E 04 0E 05 0E 04 0E 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 FF FF 00 00 04 0E 02 00 09 00 3D 00 38 00 41 00 47 00 57 00 5B 00 64 00 69 00 7F 00 80 00 6B 00 5F 00 5F 00 52 00 49 00 3B 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 E6 00 00 00 00 00 3C E0 00 00 00 00 00 00 00 00 00 00 DB 00 DF 00 00 00 00 00 00 00 00 63 D1 3A 04 00 C0 45 04 00 09 00 00 00 A6 69 28 00 64 00 00 00 D1 80 AF 00 01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 FF 00 01 00 00 00 93 03 00 00 00 00 35 55 3F 40 00 00 00 00 6C 16 00 00 00 01 01 01 00 06 01 00 33 4A C3 04 00 00 00 00 E6 00 DB 00 DE 00 90 03 4C 23 85 0A 48 00 00 00 80 51 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 FE FF 7F DC 2F 01 01 B0 0F 07 00 00 1B 01 10 16 20 00 01 04 4B
        time.sleep(.1)
        # 
        # if bms.inWaiting() >= 4 :
        #     b = bms.read(1).hex()
        #     log.debug(f"BMS data received {b}")
        #     if b == '55' : # header byte 1
        #         b = bms.read(1).hex()
        #         log.debug(f"BMS data received {b}")
        #         if b == 'aa' : # header byte 2
        #             # next two bytes is the length of the data package, including the two length bytes
        #             length = int.from_bytes(bms.read(2),byteorder='big')
        #             length -= 2 # Remaining after length bytes
        #             log.debug(f"BMS data length {length}")
        # 
        #             # Lets wait until all the data that should be there, really is present.
        #             # If not, something went wrong. Flush and exit
        #             available = bms.inWaiting()
        #             if available != length :
        #                 time.sleep(0.1)
        #                 available = bms.inWaiting()
        #                 # if it's not here by now, exit
        #                 if available != length :
        #                     bms.reset_input_buffer()
        #                     raise Exception("Something went wrong reading the data...")
        # 
        #             # Reconstruct the header and length field
        #             b = bytearray.fromhex("55aa")
        #             b += (length+2).to_bytes(2, byteorder='big')
        # 
        #             # Read all the data
        #             data = bytearray(bms.read(available))
        #             # And re-attach the header (needed for CRC calculation)
        #             data = b + data
        # 
        #             # Calculate the CRC sum
        #             crc_calc = sum(data[0:-4])
        #             # Extract the CRC value from the data
        #             crc_lo = struct.unpack_from('>H', data[-2:])[0]
        # 
        #             # Exit if CRC doesn't match
        #             if crc_calc != crc_lo :
        #                 bms.reset_input_buffer()
        #                 raise Exception("CRC Wrong")
        # 
        #             # The actual data we need
        #             data = data[11:length-19] # at location 0 we have 0x79
        # 
        #             # The byte at location 1 is the length count for the cell data bytes
        #             # Each cell has 3 bytes representing the voltage per cell in mV
        #             bytecount = data[1]
        # 
        #             # We can use this number to determine the total amount of cells we have
        #             cellcount = int(bytecount/3)
        # 
        #             # Voltages start at index 2, in groups of 3
        #             for i in range(cellcount) :
        #                 voltage = struct.unpack_from('>xH', data, i * 3 + 2)[0]/1000
        #                 valName  = "mode=\"cell"+str(i+1)+"_BMS\""
        #                 valName  = "{" + valName + "}"
        #                 dataStr  = f"JK_BMS{valName} {voltage}"
        #                 print(dataStr)
        # 
        #             # Temperatures are in the next nine bytes (MOSFET, Probe 1 and Probe 2), register id + two bytes each for data
        #             # Anything over 100 is negative, so 110 == -10
        #             temp_fet = struct.unpack_from('>H', data, bytecount + 3)[0]
        #             if temp_fet > 100 :
        #                 temp_fet = -(temp_fet - 100)
        #             temp_1 = struct.unpack_from('>H', data, bytecount + 6)[0]
        #             if temp_1 > 100 :
        #                 temp_1 = -(temp_1 - 100)
        #             temp_2 = struct.unpack_from('>H', data, bytecount + 9)[0]
        #             if temp_2 > 100 :
        #                 temp_2 = -(temp_2 - 100)
        # 
        #             # For now we just show the average between the two probes in Grafana
        #             valName  = "mode=\"temp_BMS\""
        #             valName  = "{" + valName + "}"
        #             dataStr  = f"JK_BMS{valName} {(temp_1+temp_2)/2}"
        #             print(dataStr)
        # 
        #             # Battery voltage
        #             voltage = struct.unpack_from('>H', data, bytecount + 12)[0]/100
        # 
        #             # Current
        #             current = struct.unpack_from('>H', data, bytecount + 15)[0]/100
        # 
        #             # Remaining capacity, %
        #             capacity = struct.unpack_from('>B', data, bytecount + 18)[0]
        # 
        # 
        # bms.reset_input_buffer()

    except Exception as e :
        log.error(e)

while True:
    readBMS()
    time.sleep(sleepTime)