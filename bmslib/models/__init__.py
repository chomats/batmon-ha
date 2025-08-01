from functools import partial

from bmslib.util import get_logger

logger = get_logger()


def get_bms_model_class(name):
    import bmslib.models.ant
    import bmslib.models.daly
    import bmslib.models.daly2
    import bmslib.models.dummy
    import bmslib.models.jbd
    import bmslib.models.jikong
    import bmslib.models.sok
    import bmslib.models.supervolt
    import bmslib.models.victron
    import bmslib.models.BLE_BMS_wrap

    import bmslib.group

    from bmslib import models

    from bmslib.bms_ble import plugins
    import bmslib.bms_ble.plugins.seplos_bms
    import  bmslib.bms_ble.plugins.seplos_v2_bms
    import bmslib.bms_ble.plugins.daly_bms
    import bmslib.bms_ble.plugins.tdt_bms
    import bmslib.bms_ble.plugins.ej_bms
    import bmslib.bms_ble.plugins.abc_bms
    import bmslib.bms_ble.plugins.cbtpwr_bms
    import bmslib.bms_ble.plugins.dpwrcore_bms
    import bmslib.bms_ble.plugins.ecoworthy_bms
    import bmslib.bms_ble.plugins.ective_bms
    import bmslib.bms_ble.plugins.felicity_bms
    import bmslib.bms_ble.plugins.ogt_bms
    import bmslib.bms_ble.plugins.redodo_bms
    import bmslib.bms_ble.plugins.roypow_bms

    #for k in dir(plugins):
    #    print(k)

    bms_registry = dict(
        daly=models.daly.DalyBt,
        daly2=models.daly2.Daly2Bt,
        jbd=models.jbd.JbdBt,
        jk=models.jikong.JKBt,  # auto detect
        jk_24s=models.jikong.JKBt_24s,  # https://github.com/syssi/esphome-jk-bms/blob/main/esp32-ble-example.yaml#L6
        jk_32s=models.jikong.JKBt_32s,
        ant=models.ant.AntBt,
        victron=models.victron.SmartShuntBt,
        group_parallel=bmslib.group.VirtualGroupBms,
        # group_serial=bmslib.group.VirtualGroupBms, # TODO
        supervolt=models.supervolt.SuperVoltBt,
        sok=models.sok.SokBt,
        daly_ble=partial(models.BLE_BMS_wrap.BMS, module=plugins.daly_bms, type='daly_ble'),
        dummy=models.dummy.DummyBt,
    )

    for k in dir(plugins):
        if k.startswith('_') or not k.endswith('_bms'):
            continue
        if k[:-4] in bms_registry:
            continue
        # print(k)
        logger.info('Bms2 device %s', k[:-4])
        bms_registry[k[:-4]] = partial(models.BLE_BMS_wrap.BMS, type=k, module=getattr(plugins, k))

    return bms_registry.get(name)


def construct_bms(dev, verbose_log, bt_discovered_devices):
    addr: str = dev['address']

    if not addr or addr.startswith('#'):
        return None

    bms_class = get_bms_model_class(dev['type'])

    if bms_class is None:
        logger.warning('Unknown device type %s', dev)
        return None

    if dev.get('debug'):
        logger.info('Verbose log for %s enabled', addr)

    def name2addr(name: str):
        return next((d.address for d in bt_discovered_devices if (d.name or "").strip() == name.strip()), name)

    def dev_by_addr(address: str):
        dev = next((d for d in bt_discovered_devices if d.address == address), None)
        if not dev:
            raise Exception("Can't resolve device name %s, not discovered" % address)
        return dev

    addr = name2addr(addr)

    name: str = dev.get('alias') or dev_by_addr(addr).name

    return bms_class(address=addr,
                     name=name,
                     verbose_log=verbose_log or dev.get('debug'),
                     psk=dev.get('pin'),
                     adapter=dev.get('adapter'),
                     )
