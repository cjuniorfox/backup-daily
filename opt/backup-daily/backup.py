#!/bin/python3

import sys
import os
import subprocess
import re
import time
import socket
import logging
from datetime import datetime
from glob import glob
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('/var', 'log', 'backup.log')),
        logging.StreamHandler()
    ]
)

def get_fs_type(path):
    try:
        fstype = subprocess.check_output(['df', '--output=fstype', path], stderr=subprocess.PIPE).decode().split('\n')[1]
        return fstype.strip()
    except Exception as e:
        logging.exception('Error getting filesystem type: %s', str(e))
        return None

def zfs_check_tag(tag_name):
    zfs_list = ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', tag_name]
    try:
        process = subprocess.check_output(zfs_list, stderr=subprocess.PIPE)
        return process.decode('utf-8').strip()
    except subprocess.CalledProcessError:
        return None

def btrfs_check_tag(tag_name):
    btrfs_list = ['btrfs', 'subvolume', 'list', '/', '-tsr']
    try:
        process = subprocess.check_output(btrfs_list, stderr=subprocess.PIPE)
        subvolumes = process.decode('utf-8').split('\n')
        match_item = [item for item in subvolumes if tag_name in item]
        if match_item:
            subvol = (match_item[0].split()[6]).split('/')
            if subvol:
                return subvol[-1]
        return ''
    except subprocess.CalledProcessError:
        return None

def from_tag_incremental(name, file_title, dest_path):
    files = glob(os.path.join(dest_path, f'{file_title}*.{fs_type}.gz'))
    if not files:
        return None
    files.sort(reverse=True)
    for i in files:
        tag = i.split("@")[1].replace(".incremental", "").replace(f'.{fs_type}.gz', "")
        tag_name = f'{name}@{tag}' if is_zfs else f'{file_title}@{tag}'
        if glob(os.path.join(dest_path, f'{file_title}@{tag}.doing.txt')):
            return {'tag': '', 'source_file': ''}
        snapshot = zfs_check_tag(tag_name) if is_zfs else btrfs_check_tag(tag_name)
        if snapshot:
            return {'tag': snapshot, 'source_file': i}
    return None

def snapshot_data(name):
    file_title = name.replace('/', '--')
    formatted_date = datetime.now().strftime("%Y-%m-%d_T%H-%M-%S")
    file_name = f'{file_title}@{formatted_date}.{fs_type}.gz'
    file_name_incremental = file_name.replace(fs_type, f'incremental.{fs_type}')
    dest_path = os.path.join(DEST_PATH, file_title)
    from_tag = from_tag_incremental(name, file_title, dest_path)

    incremental = {
        "file_name": file_name_incremental,
        "target": os.path.join(dest_path, file_name_incremental),
        "from_tag": from_tag
    } if from_tag and from_tag.get("tag") else {}

    return {
        "file_title": file_title,
        "tag": f'{name}@{formatted_date}' if is_zfs else f'{file_title}@{formatted_date}',
        "file_name": file_name,
        "incremental": incremental,
        "dest_path": dest_path,
        "target": os.path.join(dest_path, file_name),
    }

def mountpoint_data(i):
    name, mounted, snapshot = i
    return {
        "type": fs_type,
        "name": name,
        "mountpoint": mounted,
        "snap": os.path.join(mounted, '.snap'),
        "mounted": snapshot,
        "snapshot": snapshot_data(name)
    }

def zfs_list():
    zfs_list = ['zfs', 'list', '-t', 'filesystem', '-o', 'name,mountpoint,mounted']
    lines = [i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3]
    filesystem = [mountpoint_data(i.split()) for i in lines if i.split()[1].lower() != 'legacy' and 'tmp' not in i.split()[0].lower() and i.split()[2] == 'yes']
    zfs_list[3] = 'volume'
    lines = [i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3]
    volume = [mountpoint_data(i.split()) for i in lines if 'swap' not in i.split()[0].lower() and i.split()[0].lower() != 'name']
    return filesystem + volume

def btrfs_list():
    try:
        btrfs_list_cmd = ['btrfs', 'subvolume', 'list', '/']
        list_mounts_cmd = ['mount']
        subvolumes = subprocess.check_output(btrfs_list_cmd).decode('utf-8').split('\n')
        mountpoints = subprocess.check_output(list_mounts_cmd).decode('utf-8').split('\n')
        writable_subvolumes = []
        for subvol in subvolumes:
            if 'path' in subvol:
                path = subvol.split('path')[1].strip()
                mountpoint_match = [item for item in mountpoints if f'subvol=/{path})' in item]
                if mountpoint_match and 'tmp' not in mountpoint_match[0] and 'snap' not in mountpoint_match[0]:
                    mountpoint = mountpoint_match[0].split(' ')[2]
                    mp_data = mountpoint_data(f'{path} {mountpoint} yes'.split())
                    writable_subvolumes.append(mp_data)
        return writable_subvolumes
    except subprocess.CalledProcessError as e:
        logging.exception('Error listing BTRFS subvolumes: %s', str(e))
        return []

def take_zfs_snapshot(snapshot_tag):
    try:
        snap = subprocess.Popen(['zfs', 'snapshot', snapshot_tag])
        _, _ = snap.communicate()
        return snap.returncode == 0
    except subprocess.CalledProcessError:
        logging.exception('Error taking snapshot: "%s"', snapshot_tag)
        return False

def take_btrfs_snapshot(snapshot_tag, snap_subvol, mountpoint):
    if not os.path.exists(snap_subvol):
        try:
            proc = subprocess.Popen(['btrfs', 'subvolume', 'create', snap_subvol])
            _, _ = proc.communicate()
        except subprocess.CalledProcessError:
            logging.exception('Error creating .snap subvolume for "%s"', snapshot_tag)
            return False
    try:
        proc = subprocess.Popen(['btrfs', 'subvolume', 'snapshot', '-r', mountpoint, os.path.join(snap_subvol, snapshot_tag)])
        _, _ = proc.communicate()
        return proc.returncode == 0
    except subprocess.CalledProcessError:
        logging.exception('Error creating .snap subvolume for "%s"', snapshot_tag)
    return False

def take_snapshot(i):
    snapshot_tag = i['snapshot']['tag']
    snap_subvol = i['snap']
    mountpoint = i['mountpoint']
    logging.info('Taking snapshot for "%s"', snapshot_tag)
    take_snapshot = take_zfs_snapshot(snapshot_tag) if is_zfs else take_btrfs_snapshot(snapshot_tag, snap_subvol, mountpoint)
    if take_snapshot:
        logging.debug('Snapshot "%s" successfully taken', snapshot_tag)
        return True
    else:
        logging.error('ERROR While taking snapshot "%s"', snapshot_tag)
        return False

def send_backup_using_bash(i, incremental=False):
    snap = i.get('snapshot', {})
    tag = snap.get('tag')
    snapshot_dir = i.get('snap')

    from_tag_incremental = snap.get('incremental', {}).get('from_tag', {}).get('tag')
    dest_file = snap.get('target')
    if incremental and snap.get('incremental', {}).get('target'):
        dest_file = snap.get('incremental', {}).get('target')
    doing_file = dest_file.replace(".incremental", "").replace(f'.{fs_type}.gz', ".doing.txt")
    incr_txt_file = dest_file.replace(f'.{fs_type}.gz', ".txt")

    with open(doing_file, 'w') as f:
        f.write("writing file")

    btrfs_snapshot = os.path.join(snapshot_dir, tag)

    zfs_cmd = f'zfs send {tag} | pv -B 512M | pigz -c > {dest_file}'
    btrfs_cmd = f'btrfs send "{btrfs_snapshot}" | pv -B 512M | pigz -c > {dest_file}'
    if incremental and from_tag_incremental:
        with open(incr_txt_file, "w") as f:
            f.write(dest_file)
        logging.info('Sending snapshot tag "%s" to "%s" incrementally from "%s".', tag, dest_file, from_tag_incremental)
        zfs_cmd = f'zfs send -i {from_tag_incremental} {tag} | pv -B 512M | pigz -c > {dest_file}'
        btrfs_cmd = f'btrfs send -p "{from_tag_incremental}" "{btrfs_snapshot}" | pv -B 512M | pigz -c > {dest_file}'
    else:
        logging.info('Sending snapshot tag "%s" to "%s".', tag, dest_file)
    try:
        if is_zfs:
            zfs = subprocess.Popen(['bash', '-c', zfs_cmd])
            zfs.wait()
        else:
            btrfs = subprocess.Popen(['bash', '-c', btrfs_cmd])
            btrfs.wait()
        if os.path.exists(doing_file):
            os.remove(doing_file)
    except subprocess.CalledProcessError:
        logging.exception('Error doing the backup of "%s" for "%s"', tag, dest_file)

def backup(i):
    logging.info('Starting backup of "%s"', i['snapshot']['tag'])
    logging.debug('Creating directory: "%s"', i['snapshot']['tag'])
    os.makedirs(i.get('snapshot', {}).get('dest_path', ''), exist_ok=True)
    incremental_tag = i.get('snapshot', {}).get('incremental', {}).get('from_tag', {}).get('tag')
    if incremental_tag:
        send_backup_using_bash(i, True)
    else:
        send_backup_using_bash(i)

def do_the_job(fs_list):
    logging.info('Starting backup job for "%s"', workname)
    for i in fs_list:
        if take_snapshot(i):
            time.sleep(0.5)
            backup(i)

def mount_shares(block_device: str, mountpoint: str, options: str) -> bool:
    try:
        if not os.path.exists(mountpoint):
            logging.info('Mountpoint "%s" does not exist. Creating it.', mountpoint)
            os.makedirs(mountpoint)
        command = ['mount', block_device, mountpoint]
        if options:
            command.extend(['-o', options])
        logging.info('Mounting backup "%s" at mountpoint "%s" with options "%s"', block_device, mountpoint, options)
        mount = subprocess.Popen(command)
        mount.wait()
        if mount.returncode == 0:
            return True
        else:
            logging.error('ERROR: Mount command failed with return code %d', mount.returncode)
            return False
    except subprocess.CalledProcessError as e:
        logging.exception('Error while mounting backup %s at mountpoint "%s"', block_device, mountpoint)
        logging.debug('Error: %s', str(e))
        return False

def umount_shares(mountpoint):
    logging.info('Unmounting backup at mountpoint "%s"', mountpoint)
    umount = subprocess.Popen(['umount', mountpoint])
    umount.wait()
    if umount.returncode == 0:
        return True
    else:
        logging.error('ERROR: Umount command failed with return code %d', umount.returncode)
        return False

if __name__ == '__main__':
    block_device = 'lacie-d2.local:/srv/Files'
    mountpoint = '/mnt'
    options = ''
    workname = socket.gethostname()

    fs_type = get_fs_type("/")
    is_zfs = fs_type == 'zfs'
    parser = argparse.ArgumentParser(description='Backup script for ZFS and BTRFS filesystems.')
    parser.add_argument('--print-fs-list', '-p', action='store_true', help='Does not do the backup. Just print the list of filesystems to backup.')
    parser.add_argument('--block-device', '-b', type=str, required=True, help='Backup script for ZFS and BTRFS filesystems.')
    parser.add_argument('--mountpoint', '-m', type=str, required=True, help='Mountpoint where the backup will be stored.')
    parser.add_argument('--options', '-o', type=str, required=False, help='Mounter options.')
    args = parser.parse_args()
    mountpoint = args.mountpoint
    options = args.options
    block_device = args.block_device
    print_fs_list = args.print_fs_list
    DEST_PATH = os.path.join(mountpoint, workname)
    if mount_shares(block_device, mountpoint, options):
        if print_fs_list:
            fs_list = zfs_list() if is_zfs else btrfs_list()
            print(str(fs_list).replace("'", '"'))
        else:
            do_the_job(zfs_list() if is_zfs else btrfs_list())
        sys.exit(0 if umount_shares(mountpoint) else 1)
