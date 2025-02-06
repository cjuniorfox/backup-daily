import os
import subprocess
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler(os.path.join('/var', 'log', 'backup.log')),
        logging.StreamHandler()
    ]
)
def find_previous_backup(filename):
    """Find the previous backup file by reading the content of the .txt file."""
    txt_file = filename.replace(".zfs.gz", ".txt").replace(".btrfs.gz",".txt")
    if os.path.isfile(txt_file):
        with open(txt_file, 'r') as f:
            previous_file = f.read().strip()
            return os.path.basename(previous_file)
    return None

def restore_recursively(filename):
    filename_before = find_previous_backup(filename)
    if filename_before :
        restore_recursively(os.path.join(os.path.dirname(filename),filename_before))
    logging.info('Restoring "%s"...',os.path.basename(filename))
    if not test:
        btrfs_cmd = f'pigz -dc "{filename}" | pv -B 512M | btrfs receive {destiny}'
        zfs_cmd = f'pigz -dc "{filename}" | pv -B 512M | zfs receive -F "{destiny}"'
        restore = subprocess.Popen(['bash', '-c', zfs_cmd if filesystem == 'zfs' else btrfs_cmd ])
        restore.wait()

def main():
    logging.info('Restoring to "%s"',destiny)
    restore_recursively(backup_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Restore BTRFS Backup.')
    parser.add_argument('--filesystem' ,'-f', type=str, choices=['btrfs','zfs'], default='btrfs', help='Backup/partition type. "btrfs" or "zfs"')
    parser.add_argument('--backup-file', '-b', type=str, required=True, help='Backup file to be restored')
    parser.add_argument('--destiny', '-d', type=str, required=True, help='Destiny Dataset')
    parser.add_argument('--test', '-t', action='store_true', required=False, help='Do not restore. Just test')
    args = parser.parse_args()
    backup_file = args.backup_file
    destiny = args.destiny
    test = args.test
    filesystem=args.filesystem
    main()
