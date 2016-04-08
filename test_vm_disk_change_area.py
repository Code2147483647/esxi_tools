#! /usr/bin/python

import json, time, math
import pyvddk
from pyVmomi import vim, vmodl
from vsdk.VsdkUtil import getLatestGenId, createSpecList, getOvfPath,\
getBackupFile, getVMName, getDatastoreName, setBackupFile, setDatastoreName,\
getDiskCapacity, getPrevChangeId, setGeneration
from vsdk.Connection import Connection
from vsdk.VmManager import VmManager
from vsdk.ESXInventory import ESXInventory
from vsdk.Constants import Constants
import os
import vsdk.VsdkUtil
import argparse
import traceback


def getArgs():
    desc = 'test and fix cbt'

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-a', '--host-name', required=True, help='host ip')
    parser.add_argument('-u', '--user', required=True, help='host account')
    parser.add_argument('-p', '--password', required=True, help='host password')
    parser.add_argument('-t', '--http-port', required=False, default=443, help='host port')
    parser.add_argument('-v', '--vm-id', required=True, help='vm id')
    parser.add_argument('-c', '--test-cbt', required=False, action='store_true', help='Only do test could get changeId or not.')
    parser.add_argument('-e', '--change-id', default='*', help='test query disk change area api with given changeId(default: *). Note that need to single quote around the input value')

    return parser.parse_args()

if __name__ == '__main__':
    print 'Start tet cb and try to fix it'
    args = getArgs()
    conn_param = dict(
        host=args.host_name,
        user=args.user,
        pwd=args.password,
        port=args.http_port
    )
    print conn_param
    conn = Connection(**conn_param)
    vmss = None
    snapshot = None
    try:
        conn.connect()
        sii = conn.si
        esx = ESXInventory(conn)
        uuid = args.vm_id
        vm = esx.getVM(uuid)
        vmm = VmManager(conn, vm)
        if not args.test_cbt:
            print '-----do reset cbt-----'
            vmm.setCBT(False)
            snapshot = vmm.createSnapshot(name='clean_cbt', memory=False, quiesce=False)

            if snapshot is None:
                raise RuntimeError('Can not create snapshot')

            vmm.deleteSnapshot(snapshot)
            snapshot = None
            print '-----reset cbt done-----'

        vmm.setCBT(True)
        vmss = vmm.createSnapshot(name='test_disk_change_area', memory=False, quiesce=False)
        disks = [d for d in vmm.vmss.config.hardware.device if isinstance(d, vim.VirtualDisk)]
        print 'disk keys: {}'.format([d.key for d in disks])
        for disk in disks:
            print 'this changeId:', disk.backing.changeId
            change_id = args.change_id
            disk_key = disk.key
            pos = 0
            query_method = vm.QueryChangedDiskAreas
            print 'query disk change area with disk key: {}, changeId: {}\n'.format(disk_key, change_id)
            changes = query_method(vmss, disk_key, pos, change_id)

        print 'Fix done successfully'
    except Exception as e:
        print 'exception occurred: {}'.format(e.message)
        traceback.print_exc()
    finally:
        if snapshot:
            vmm.deleteSnapshot(snapshot)
        if vmss:
            vmm.deleteSnapshot(vmss)

        conn.disconnect()
