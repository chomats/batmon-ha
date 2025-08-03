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
        self.internal_failure: Union[int, None] = None
        self.high_charge_temperature: Union[int, None] = None
        self.low_charge_temperature: Union[int, None] = None
        self.high_temperature: Union[int, None] = None
        self.low_temperature: Union[int, None] = None
        self.high_internal_temperature: Union[int, None] = None
        self.fuse_blown: Union[int, None] = None

        # previous values to check if the value has changed
        self.previous_high_voltage: Union[int, None] = None
        self.previous_high_cell_voltage: Union[int, None] = None
        self.previous_low_voltage: Union[int, None] = None
        self.previous_low_cell_voltage: Union[int, None] = None
        self.previous_low_soc: Union[int, None] = None
        self.previous_high_charge_current: Union[int, None] = None
        self.previous_high_discharge_current: Union[int, None] = None
        self.previous_cell_imbalance: Union[int, None] = None
        self.previous_internal_failure: Union[int, None] = None
        self.previous_high_charge_temperature: Union[int, None] = None
        self.previous_low_charge_temperature: Union[int, None] = None
        self.previous_high_temperature: Union[int, None] = None
        self.previous_low_temperature: Union[int, None] = None
        self.previous_high_internal_temperature: Union[int, None] = None
        self.previous_fuse_blown: Union[int, None] = None

    def set_previous(self) -> None:
        """
        Set the previous values to the current values.

        :return: None
        """
        self.previous_high_voltage = self.high_voltage
        self.previous_high_cell_voltage = self.high_cell_voltage
        self.previous_low_voltage = self.low_voltage
        self.previous_low_cell_voltage = self.low_cell_voltage
        self.previous_low_soc = self.low_soc
        self.previous_high_charge_current = self.high_charge_current
        self.previous_high_discharge_current = self.high_discharge_current
        self.previous_cell_imbalance = self.cell_imbalance
        self.previous_internal_failure = self.internal_failure
        self.previous_high_charge_temperature = self.high_charge_temperature
        self.previous_low_charge_temperature = self.low_charge_temperature
        self.previous_high_temperature = self.high_temperature
        self.previous_low_temperature = self.low_temperature
        self.previous_high_internal_temperature = self.high_internal_temperature
        self.previous_fuse_blown = self.fuse_blown
