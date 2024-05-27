#!/bin/python3

import os,subprocess,re,time, socket
from datetime import datetime
from glob import glob
from operator import contains

def get_fs_type(path):
    try:
        fstype = subprocess.check_output(['df', '--output=fstype', path], stderr=subprocess.PIPE).decode().split('\n')[1]
        return fstype.strip()
    except Exception as e:
        print(f"Error determining filesystem type: {e}")
        return None

def zfs_check_tag(tag_name):
    zfs_list = ['zfs','list','-t','snapshot','-o','name','-H',tag_name]
    try:
        process=subprocess.check_output(zfs_list,stderr=subprocess.PIPE)
        return process.decode('utf-8').split[0]
    except subprocess.CalledProcessError:
        None

def btrfs_check_tag(tag_name):
    btrfs_list = ['btrfs','subvolume','list','/','-ats']
    try:
        process=subprocess.check_output(btrfs_list,stderr=subprocess.PIPE)
        subvolumes=process.decode('utf-8').split('\n')
        match_item = [item for item in subvolumes if tag_name in item]
        if match_item:
            return match_item[3].replace('<FS_TREE>/','')
        return ''
    except subprocess.CalledProcessError:
        None

def from_tag_incremental(name,file_title,directory):
    files = glob(os.path.join( directory, f'{file_title}*.{fs_type}.gz'))
    if len(files) == 0:
        return {'tag': '', 'file_name' : ''}
    files.sort()
    files.reverse()
    for i in files:
        tag=i.split("@")[1].replace(".incremental","").replace(f'.{fs_type}.gz',"")
        tag_name= f'{name}@{tag}' if fs_type=='zfs' else f'{file_title}@{tag}'
        if glob(os.path.join(directory,f'{file_title}@{tag}.doing.txt')) :
            return {'tag':'','file_name':''}
        snapshot = zfs_check_tag(tag_name) if is_zfs else btrfs_check_tag(tag_name)
        if len(snapshot) > 0:
            return {'tag' : snapshot, 'file_name': i }
    return {'tag': '', 'file_name' : ''}

def snapshot_data(name):
    file_title=name.replace('/','--')
    formatted_date = datetime.now().strftime("%Y-%m-%d_T%H-%M-%S")
    file_name = f'{file_title}@{formatted_date}.{fs_type}.gz'
    file_name_incremental = file_name.replace(fs_type,f'incremental.{fs_type}')
    directory = os.path.join(path,file_title)
    from_tag_incr=from_tag_incremental(name,file_title,directory)
    return {
            "file_title" : file_title, 
            "tag" : f'{name}@{formatted_date}' if is_zfs else f'{file_title}@{formatted_date}',
            "file_name" : file_name,
            "file_name_incremental" : file_name_incremental if from_tag_incr != None and from_tag_incr != "" else "",
            "from_tag_incremental": from_tag_incr,
            "directory" : directory,
    }

def mountpoint_data(i):
    name = i[0]
    mounted = i[1]
    snapshot = i[2]
    return {
        "type" : fs_type,
        "name" : name , 
        "mountpoint" : mounted , 
        "snap" : os.path.join(mounted,'.snap'),
        "mounted": snapshot ,
        "snapshot": snapshot_data(name)
    }

def zfs_list():  
    zfs_list=['zfs', 'list', '-t', 'filesystem', '-o' ,'name,mountpoint,mounted']
    #Filesystem
    lines = [ i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3 ]
    filesystem=[ mountpoint_data(i.split()) for i in lines if i.split()[1].lower() != 'legacy' and not contains(i.split()[0].lower(),'tmp') and i.split()[2].lower() == 'yes']
    #Volumes
    zfs_list[3]='volume'
    lines = [ i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3 ]
    volume=[mountpoint_data(i.split()) for i in lines if not contains(i.split()[0].lower(),'swap') and i.split()[0].lower() != 'name']
    return filesystem + volume

def btrfs_list():
    try:
        # List all subvolumes
        btrfs_list_cmd = ['btrfs', 'subvolume', 'list', '/']
        list_mounts_cmd = ['mount']
        subvolumes = subprocess.check_output(btrfs_list_cmd).decode('utf-8').split('\n')
        mountpoints = subprocess.check_output(list_mounts_cmd).decode('utf-8').split('\n')        
        # Filter out read-only subvolumes
        writable_subvolumes = []
        for subvol in subvolumes:
            if 'path' in subvol:
                path = subvol.split('path')[1].strip()
                mountpoint_match = [item for item in mountpoints if f'subvol=/{path})' in item]
                #Check if subvolume is mounted
                if mountpoint_match and 'tmp' not in mountpoint_match[0] and 'snap' not in mountpoint_match[0]:
                    mountpoint=mountpoint_match[0].split(' ')[2]
                    writable_subvolumes.append(mountpoint_data(f'{path} {mountpoint} yes'.split()))
        
        return writable_subvolumes
    except subprocess.CalledProcessError as e:
        print(f"Error listing BTRFS subvolumes: {e}")
        return []

def take_zfs_snapshot(snapshot_tag):
    try:
        snap = subprocess.Popen(['zfs', 'snapshot', snapshot_tag])
        _, _ = snap.communicate()
        return snap.returncode == 0
    except subprocess.CalledProcessError:
        return False

def take_btrfs_snapshot(snapshot_tag,snap_subvol,mountpoint):
    #Check if the .snap exists inside the mountpoint. If not, create subvolume with name .snap
    if not os.path.exists(snap_subvol):
        try:
            proc = subprocess.Popen(['btrfs','subvolume','create',snap_subvol])
            _, _ = proc.communicate()
        except subprocess.CalledProcessError:
            print(f'Error creating .snap subvolume for {snapshot_tag}')
            return False
    try:
        proc = subprocess.Popen(['btrfs','subvolume','snapshot','-r',mountpoint,os.path.join(snap_subvol,snapshot_tag)])
        _, _ = proc.communicate()
        return proc.returncode == 0
    except subprocess.CalledProcessError:
        print(f'Error creating snapshot for {snapshot_tag}')
        return False
    return False
        
def take_snapshot(i):
    snapshot_tag= i['snapshot']['tag']
    snap_subvol=i['snap']
    mountpoint=i['mountpoint']
    print(f'Creating snapshot "{snapshot_tag}"')
    take_snapshot = take_zfs_snapshot(snapshot_tag) if is_zfs else take_btrfs_snapshot(snapshot_tag,snap_subvol,mountpoint)
    if take_snapshot:
        return True
    else:
        print(f'ERROR While taking snapshot "{snapshot_tag}"')
        return False
    

def send_backup_using_bash(i,incremental=False):
    snap=i['snapshot']
    tag=snap['tag']
    snapshot_dir=i['snap']
    btrfs_snapshot=os.path.join(snapshot_dir,tag)
    from_tag_incremental=snap['from_tag_incremental']['tag']
    dest_file=os.path.join(snap['directory'],(snap['file_name_incremental'] if incremental else snap['file_name'] ))
    doing_file=dest_file.replace(".incremental","").replace(f'.{fs_type}.gz',".doing.txt")
    incr_txt_file=dest_file.replace(f'.{fs_type}.gz',".txt")
    with open(doing_file,'w') as f:
        f.write("writing file")
    if incremental:
        with open(incr_txt_file,"w") as f:
            f.write(snap['from_tag_incremental']['file_name'])
    zfs_cmd=f'zfs send {tag} | pv -B 512M -s $(zfs send -nP {tag} | tail -n1 | awk \'{{print $2}}\') | pigz -c > {dest_file}'
    zfs_cmd_incr=f'zfs send -i {from_tag_incremental} {tag} | pv -B 512M -s $(zfs send -i {from_tag_incremental} -nP {tag} | awk \'{{print 2}}\' | tail -n1) | pigz -c > {dest_file}'
    btrfs_cmd=f'btrfs send "{btrfs_snapshot}" | pv -B 512M | pigz -c > {dest_file}'
    btrfs_cmd_incr=f'btrfs send -p "{from_tag_incremental}" "{btrfs_snapshot}" | pv -B 512M | pigz -c > {dest_file}'
    try:
        if is_zfs:
            zfs=subprocess.Popen(['bash','-c',zfs_cmd_incr if incremental else zfs_cmd])
            zfs.wait()
        else:
            btrfs=subprocess.Popen(['bash','-c',btrfs_cmd_incr if incremental else btrfs_cmd])
            btrfs.wait()
        if os.path.exists(doing_file):
            os.remove(doing_file)
    except subprocess.CalledProcessError:
        print (f'Error doing the backup of {tag} for {dest_file}')
        
def backup(i):
    os.makedirs(i['snapshot']['directory'], exist_ok=True)
    if i['snapshot']['from_tag_incremental']['tag'] != '':
        print("Sending snapshot " + i['snapshot']['tag'] + ' to the file ' + i['snapshot']['file_name_incremental'])
        send_backup_using_bash(i,True) if is_zfs else btrfs_send_file_using_bash(i,True)
    else:
        print("Sending snapshot " + i['snapshot']['tag'] + ' to the file ' + i['snapshot']['file_name'])
        send_backup_using_bash(i)

def do_the_job(fs_list):
    for i in fs_list:
        if take_snapshot(i):
            time.sleep(0.5)
            backup(i)

def mount_shares(block_device,mountpoint):
    try:
        mount = subprocess.Popen(['mount',block_device,mountpoint])
        mount.wait()
        return True
    except subprocess.CalledProcessError as e:
        print(f'ERROR While mounting backup {block_device} at mountpoint "{mountpoint}"')
        print(f'Error: {e}')
        return False

def umount_shares(mountpoint):
    umount = subprocess.Popen(['umount',mountpoint])
    umount.wait()

if __name__ == '__main__':
    block_device='lacie-d2.local:/srv/Files'
    mountpoint='/mnt'
    options=''
    workname = socket.gethostname()

    fs_type = get_fs_type("/")
    is_zfs = fs_type == 'zfs'
    if mount_shares(block_device,mountpoint):
        path=os.path.join(mountpoint,workname)
        #print(zfs_list() if is_zfs else btrfs_list())
        do_the_job(zfs_list() if is_zfs else btrfs_list())
        umount_shares(mountpoint)