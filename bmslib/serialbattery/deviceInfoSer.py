from bmslib.bms import DeviceInfo
from typing import Optional

class DeviceInfoSer (DeviceInfo):
    def __init__(self, mnf: str, model: str, hw_version: Optional[str], sw_version: Optional[str], name: Optional[str],
                 sn: Optional[str] = None):
        super().__init__(mnf, model, hw_version, sw_version, name, sn)
        self.device_type = 'serial'
        
        