#!/bin/python3

import os, subprocess, re, time
from datetime import datetime
from glob import glob
from operator import contains

# Determine the filesystem type
def get_fs_type(path):
    try:
        fstype = subprocess.check_output(['df', '--output=fstype', path], stderr=subprocess.PIPE).decode().split('\n')[1]
        return fstype.strip()
    except Exception as e:
        print(f"Error determining filesystem type: {e}")
        return None

# Modified from_tag_incremental to support both ZFS and BTRFS
def from_tag_incremental(name, file_title, directory, fs_type):
    extension = 'zfs.gz' if fs_type == 'zfs' else 'btrfs.gz'
    files = glob(os.path.join(directory, f'{file_title}*.{extension}'))
    if len(files) == 0:
        return {'tag': '', 'file_name': ''}
    files.sort(reverse=True)
    for i in files:
        tag = i.split("@")[1].replace(".incremental", "").replace(f".{extension}", "")
        tag_name = f'{name}@{tag}'
        if glob(os.path.join(directory, f'{file_title}@{tag}.doing.txt')):
            return {'tag': '', 'file_name': ''}
        if fs_type == 'zfs':
            list_cmd = ['zfs', 'list', '-t', 'snapshot', '-o', 'name', '-H', tag_name]
        elif fs_type == 'btrfs':
            list_cmd = ['btrfs', 'subvolume', 'list', directory]
        try:
            process = subprocess.check_output(list_cmd, stderr=subprocess.PIPE)
            snapshot_list = process.decode('utf-8')
            if fs_type == 'btrfs' and tag_name in snapshot_list or fs_type == 'zfs' and len(snapshot_list) > 0:
                return {'tag': tag_name, 'file_name': i}
        except:
            pass
    return {'tag': '', 'file_name': ''}

# Updated get_snapshot_data to support both ZFS and BTRFS
def get_snapshot_data(name, fs_type):
    file_title = name.replace('/', '--')
    formatted_date = datetime.now().strftime(f"{workname}_%Y-%m-%d_T%H-%M-%S")
    extension = 'zfs.gz' if fs_type == 'zfs' else 'btrfs.gz'
    file_name = f'{file_title}@{formatted_date}.{extension}'
    file_name_incremental = file_name.replace(extension, f"incremental.{extension}")
    directory = os.path.join(path, file_title)
    from_tag_incr = from_tag_incremental(name, file_title, directory, fs_type)
    return {
        "file_title": file_title,
        "tag": f'{name}@{formatted_date}',
        "file_name": file_name,
        "file_name_incremental": file_name_incremental if from_tag_incr['tag'] else "",
        "from_tag_incremental": from_tag_incr,
        "directory": directory,
    }

# Function to take a snapshot, now supports both ZFS and BTRFS
def take_snapshot(i, fs_type):
    snapshot_tag = i['snapshot']['tag']
    try:
        print(f'Creating snapshot "{snapshot_tag}"')
        if fs_type == 'zfs':
            snap_cmd = ['zfs', 'snapshot', snapshot_tag]
        elif fs_type == 'btrfs':
            snap_cmd = ['btrfs', 'subvolume', 'snapshot', i['snapshot']['directory'], snapshot_tag]
        subprocess.check_call(snap_cmd)
        return True
    except subprocess.CalledProcessError as e:
        print(f'ERROR While taking snapshot "{snapshot_tag}"')
        print(f'Error: {e}')
        return False

# Main function to list filesystems and volumes, now checks FS type
def fs_list():
    filesystem = []
    volume = []
    # Assuming mountpoint path is known, replace '/mnt' with your mountpoint if different
    fs_type = get_fs_type('/mnt')
    if fs_type == 'zfs':
        filesystem, volume = zfs_list()
    elif fs_type == 'btrfs':
        # Implement BTRFS equivalent if needed
        pass
    return filesystem + volume

# You will need to implement BTRFS equivalent functions for zfs_send_file_using_bash, backup, do_the_job, mount_shares, and umount_shares
# based on the BTRFS commands and your backup strategy.

if __name__ == '__main__':
    block_device = 'lacie-d2.local:/srv/Files'
    mountpoint = '/mnt'
    options = ''
    workname = "backup_macmini"
    path = os.path.join(mountpoint, workname)
    fs_type = get_fs_type(mountpoint)
    if mount_shares(block_device, mountpoint):
        do_the_job(fs_list(), fs_type)
        umount_shares(mountpoint)