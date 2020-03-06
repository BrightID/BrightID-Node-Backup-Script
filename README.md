# BrightID-Node Backup Script

This script is used by the official brightid nodes to upload daily backups to the google storage service.
Other nodes that want to join the network restore the last backup to reach the state of the backup time. Nodes read the operations sent after the backup time from the blockchain and apply them to sync to the current state of the network.

Latest BrightID backup can always be downloaded from:

`https://storage.googleapis.com/brightid-backups/brightid.tar.gz`

Available backup files can be found:

`https://storage.googleapis.com/brightid-backups/`

There are backup files avilable for following times:

- one file per hour for a day
- one file per day for a week
- one file per week for a month
- one file per month forever

### Prerequisites:

- python3
- pip3
- arangodump

* You can find out how to install `arangodump` from arango client tools on your os from [here](https://www.arangodb.com/download-major/).

### Install

`pip3 install -r requirements.txt`

### Run

- Place your google credentials json file beside the `backup.py` with the `google.json` as name.

- Add a cron job to run the script every hour.

```
# m h  dom mon dow   command
0 * * * * python3 /path/to/script/backup.py
```
