from typing import Union


class Protection(object):
    """
    This class holds warning and alarm states for different types of checks.
    The alarm name in the GUI is the same as the variable name.

    They are of type integer

    2 = alarm
    1 = warning
    0 = ok, everything is fine
    """

    ALARM = 2
    WARNING = 1
    OK = 0

    def __init__(self):
        # current values
        self.high_voltage: Union[int, None] = None
        self.high_cell_voltage: Union[int, None] = None
        self.low_voltage: Union[int, None] = None
        self.low_cell_voltage: Union[int, None] = None
        self.low_soc: Union[int, None] = None
        self.high_charge_current: Union[int, None] = None
        self.high_discharge_current: Union[int, None] = None
        self.cell_imbalance: Union[int, None] = None
        self.high_charge_temperature: Union[int, None] = None
        self.low_charge_temperature: Union[int, None] = None
        self.high_temperature: Union[int, None] = None
        self.low_temperature: Union[int, None] = None
        self.high_internal_temperature: Union[int, None] = None
