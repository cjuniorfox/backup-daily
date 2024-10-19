# Backup Daily Script

This script is designed to perform backups of ZFS and BTRFS filesystems. It supports both full and incremental backups and logs its operations to a specified log file.

## Prerequisites

- Python 3.x
- ZFS and/or BTRFS utilities installed on your system
- `pv` and `pigz` utilities for efficient data transfer and compression

## Usage

To run the backup script, use the following command:

```bash
python3 backup.py --block-device <BLOCK_DEVICE> --mountpoint <MOUNTPOINT> [--options <OPTIONS>] [--print-fs-list]
```

- `--block-device`: The block device or network share to back up.
- `--mountpoint`: The directory where the backup will be stored.
- `--options`: (Optional) Mount options for the block device.
- `--print-fs-list`: (Optional) Print the list of filesystems to be backed up without performing the backup.

## Example

```bash
python3 backup.py --block-device nfs_server:/srv/Backup --mountpoint /mnt
```

## Logging

The script logs its operations to `/var/log/backup.log`. Ensure that the script has the necessary permissions to write to this file.

## Setting Up a Daily Backup with systemd

To automate the backup process, you can create a systemd service and timer. Below is an example of how to set this up.

### Create a systemd Service

Create a file named `/etc/systemd/system/daily-backup.service` with the following content:

```ini
[Unit]
Description=Daily Backup Service

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /path/to/backup.py --block-device nfs_server:/srv/Backup --mountpoint /tmp/daily-backup --options <OPTIONS>
```

### Create a systemd Timer

Create a file named `/etc/systemd/system/daily-backup.timer` with the following content:

```ini
[Unit]
Description=Run daily-backup.service daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

### Enable and Start the Timer

Enable and start the timer with the following commands:

```bash
sudo systemctl enable daily-backup.timer
sudo systemctl start daily-backup.timer
```

This setup will run the backup script daily, using a mountpoint in the `/tmp` directory with the current date as part of the directory name.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.