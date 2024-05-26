#!/bin/python3

import os,subprocess,re,time
from datetime import datetime
from glob import glob
from operator import contains

def from_tag_incremental(name,file_title,part_type,directory):
    files = glob(os.path.join( directory, f'{file_title}*.{part_type}.gz'))
    if len(files) == 0:
        return {'tag': '', 'file_name' : ''}
    files.sort()
    files.reverse()
    for i in files:
        tag=i.split("@")[1].replace(".incremental","").replace(f".{part_type}.gz","")
        tag_name= f'{name}@{tag}'
        if glob(os.path.join(directory,f'{file_title}@{tag}.doing.txt')) :
            return {'tag':'','file_name':''}
        zfs_list = ['zfs','list','-t','snapshot','-o','name','-H',tag_name]
        try:
            process=subprocess.check_output(zfs_list,stderr=subprocess.PIPE)
            zfs_snapshot = process.decode('utf-8')
            if len(zfs_snapshot) > 0:
                return {'tag' : zfs_snapshot.split()[0], 'file_name': i }
        except:
            None
    return {'tag': '', 'file_name' : ''}

def get_snapshot_data(name,part_type):
    file_title=name.replace('/','--')
    formatted_date = datetime.now().strftime(f"{workname}_%Y-%m-%d_T%H-%M-%S")
    file_name = f'{file_title}@{formatted_date}.zfs.gz'
    file_name_incremental = file_name.replace(part_type,f"incremental.{part_type}")
    directory = os.path.join(path,file_title)
    from_tag_incr=from_tag_incremental(name,file_title,part_type, directory)
    return {
            "file_title" : file_title, 
            "tag" : f'{name}@{formatted_date}',
            "file_name" : file_name,
            "file_name_incremental" : file_name_incremental if from_tag_incr != None and from_tag_incr != "" else "",
            "from_tag_incremental": from_tag_incr,
            "directory" : directory,
    }

def get_snapshot(i,part_type):
    name = i[0]
    mounted = i[1]
    snapshot = i[2]
    return {
        "name" : name , 
        "mountpoint" : mounted , 
        "mounted": snapshot ,
        "snapshot": get_snapshot_data(name,part_type)
    }

def zfs_list():
    part_type='zfs'
    zfs_list=['zfs', 'list', '-t', 'filesystem', '-o' ,'name,mountpoint,mounted']
    #Filesystem
    lines = [ i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3 ]
    filesystem=[ get_snapshot(i.split(),part_type) for i in lines if i.split()[1].lower() != 'legacy' and not contains(i.split()[0].lower(),'tmp') and i.split()[2].lower() == 'yes']
    #Volumes
    zfs_list[3]='volume'
    lines = [ i for i in subprocess.check_output(zfs_list).decode('utf-8').split('\n') if len(i.split()) == 3 ]
    volume=[get_snapshot(i.split(),part_type) for i in lines if not contains(i.split()[0].lower(),'swap') and i.split()[0].lower() != 'name']
    return filesystem + volume

def take_snapshot(i):
    snapshot_tag= i['snapshot']['tag']
    try:
        print(f'Creating snapshot "{snapshot_tag}"')
        snap = subprocess.Popen(['zfs', 'snapshot', snapshot_tag])
        return True
    except subprocess.CalledProcessError as e:
        print(f'ERROR While taking snapshot "{snapshot_tag}"')
        print(f'Error: {e}')
        return False

def zfs_send_file_using_bash(i,incremental=False):
    snap=i['snapshot']
    tag=snap['tag']
    from_tag_incremental=snap['from_tag_incremental']['tag']
    dest_file=os.path.join(snap['directory'],(snap['file_name_incremental'] if incremental else snap['file_name'] ))
    doing_file=dest_file.replace(".incremental","").replace(".zfs.gz",".doing.txt")
    incr_txt_file=dest_file.replace(".zfs.gz",".txt")
    with open(doing_file,'w') as f:
        f.write("writing file")
    if incremental:
        with open(incr_txt_file,"w") as f:
            f.write(snap['from_tag_incremental']['file_name'])
    cmd=f'zfs send {tag} | pv -B 512M -s $(zfs send -nP {tag} | tail -n1 | awk \'{{print $2}}\') | pigz -c > {dest_file}'
    cmd_incr=f'zfs send -i {from_tag_incremental} {tag} | pv -B 512M -s $(zfs send -i {from_tag_incremental} -nP {tag} | awk \'{{print 2}}\' | tail -n1) | pigz -c > {dest_file}'
    zfs=subprocess.Popen(['bash','-c',cmd_incr if incremental else cmd])
    zfs.wait()
    if os.path.exists(doing_file):
        os.remove(doing_file)

def backup(i):
    os.makedirs(i['snapshot']['directory'], exist_ok=True)
    if i['snapshot']['from_tag_incremental']['tag'] != '':
        print("Sending snapshot " + i['snapshot']['tag'] + ' to the file ' + i['snapshot']['file_name_incremental'])
        zfs_send_file_using_bash(i,True)
    else:
        print("Sending snapshot " + i['snapshot']['tag'] + ' to the file ' + i['snapshot']['file_name'])
        zfs_send_file_using_bash(i)

def do_the_job(zfs_list):
    for i in zfs_list:
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
    workname="backup_macmini"
    path=os.path.join(mountpoint,workname)
    if mount_shares(block_device,mountpoint):
    #print(zfs_list())
        do_the_job(fs_list())
        umount_shares(mountpoint)