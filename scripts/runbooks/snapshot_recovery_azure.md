# Recovering snapshot backups on Microsoft Azure

## Overview

This runbook describes the steps that must be followed in order to recover a snapshot backup made using Azure.

## Prerequisites and limitations

The following assumptions are made about the recovery scenario:

1. A recent snapshot backup has been taken using either [`barman backup`][barman-snapshot-backups] or [`barman-cloud-backup`][barman-cloud-snapshot-backups].
2. A recovery VM has been provisioned and PostgreSQL has been installed.

The example commands given are the bare minimum required to perform the recovery.
It is highly recommended that you consult the Azure documentation and consider whether the default options are suitable for your environment and whether any additional options are required.

## Snapshot recovery steps

In order to recover the snapshot backup the following steps must be taken:

1. Review the necessary metadata for recovering the snapshot backup.
2. Create a new Managed Disk for each snapshot in the backup.
3. Attach each disk to the recovery VM.
4. Mount each attached disk at the expected mount point for your PostgreSQL installation.
5. Finalize the recovery with Barman.

### Review the necessary metadata for recovering the snapshot backup

The information required to recover the snapshots can be found in the backup metadata managed by Barman.
For example, for backup `20230614T130700` made with `barman backup`:

```bash
barman@barman:~$ barman show-backup primary 20230614T130700
Backup 20230614T130700:
  Server Name            : primary
  System Id              : 7244478807899904061
  Status                 : DONE
  PostgreSQL Version     : 140008
  PGDATA directory       : /opt/postgres/data

  Snapshot information:
    provider             : azure
    subscription_id      : SUBSCRIPTION_ID
    resource_group       : barman-test-rg

    location             : uksouth
    lun                  : 1
    snapshot_name        : barman-test-primary-pgdata-20230614t130700
    Mount point          : /opt/postgres
    Mount options        : rw,noatime

    location             : uksouth
    lun                  : 2
    snapshot_name        : barman-test-primary-tbs1-20230614t130700
    Mount point          : /opt/postgres/tablespaces/tbs1
    Mount options        : rw,noatime
...
```

Alternatively, for backup `20230614T103507` made with `barman-cloud-backup`:

```bash
postgres@primary:~ $ barman-cloud-backup-show --cloud-provider=azure-blob-storage https://barmanteststorage.blob.core.windows.net/barman-test-container primary 20230614T103507
Backup 20230614T103507:
  Server Name            : primary
  System Id              : 7244478807899904061
  Status                 : DONE
  PostgreSQL Version     : 140008
  PGDATA directory       : /opt/postgres/data

  Snapshot information:
    provider             : azure
    subscription_id      : SUBSCRIPTION_ID
    resource_group       : barman-test-rg

    location             : uksouth
    lun                  : 1
    snapshot_name        : barman-test-primary-pgdata-20230614t103507
    Mount point          : /opt/postgres
    Mount options        : rw,noatime

    location             : uksouth
    lun                  : 2
    snapshot_name        : barman-test-primary-tbs1-20230614t103507
    Mount point          : /opt/postgres/tablespaces/tbs1
    Mount options        : rw,noatime
```

The `--format=json` option can be used with either command to view the metadata as a JSON object.
Snapshot metadata will be available under the `snapshots_info` key and will have the following structure:

```json
"snapshots_info": {
  "provider": "azure",
  "provider_info": {
    "resource_group": "barman-test-rg",
    "subscription_id": "SUBSCRIPTION_ID"
  },
  "snapshots": [
    {
      "mount": {
        "mount_options": "rw,noatime",
        "mount_point": "/opt/postgres"
      },
      "provider": {
        "location": "uksouth",
        "lun": 1,
        "snapshot_name": "barman-test-primary-pgdata-20230614t130700"
      }
    },
    {
      "mount": {
        "mount_options": "rw,noatime",
        "mount_point": "/opt/postgres/tablespaces/tbs1"
      },
      "provider": {
        "location": "uksouth",
        "lun": 2,
        "snapshot_name": "barman-test-primary-tbs1-20230614t130700"
      }
    }
  ]
},
```

Note the following values for use in the recovery process:

1. `snapshots_info/provider_info/subscription_id`
2. `snapshots_info/provider_info/resource_group`

Additionally, the following values will need to be known for each snapshot:

1. `mount/mount_point`
2. `mount/mount_options`
3. `provider/snapshot_name`

### Create a new Managed Disk for each snapshot in the backup

A new disk must be created for each snapshot listed in the backup metadata.
New disks can be created using the [`az disk` command][az-disk-create] and the snapshot can be specified using the `--source` option.

For example, for backup `20230614T130700`, the following commands should be run:

```bash
az disk create --resource-group barman-test-rg --name recovery-pgdata --sku StandardSSD_LRS --source barman-test-primary-pgdata-20230614t130700
az disk create --resource-group barman-test-rg --name recovery-tbs1 --sku StandardSSD_LRS --source barman-test-primary-tbs1-20230614t130700
```

The name given to each disk is required in order to attach the disks to the recovery VM in the following step.

### Attach each disk to the recovery VM

Each disk must be attached to the recovery VM so that it can be mounted at the correct location.
This can be achieved using the [`az-vm-disk-attach` command][az-vm-disk-attach].

To recover the backup `20230614T130700` onto a recovery instance named `barman-test-recovery`, the following commands should be run to attach the disks created in the previous step:

```bash
az vm disk attach --resource-group barman-test-rg --vm-name barman-test-recovery --name recovery-pgdata --lun 5
az vm disk attach --resource-group barman-test-rg --vm-name barman-test-recovery --name recovery-tbs1 --lun 6
```

The lun used to attach each disk is required in order to mount the disks on the recovery VM in the following step.
Any available lun can be used, or the option can be omitted in which case Azure will assign the lun itself.
If the lun is omitted then you will need to query the VM metadata to find its value, for example by using `az vm show`.

For more details see the [Azure documentation][add-a-disk-to-a-linux-vm].

### Mount each attached disk at the expected mount point for your PostgreSQL installation

Mounting each attached disk must be carried out on the recovery VM.
There are [multiple documented ways to find each attached disk][format-and-mount-the-disk] however it is recommended that the symlinks created by the Azure linux agent are used. These symlinks are structured as follows, where `${LUN}` is the lun value used when attaching the disk to the VM:

```bash
/dev/disk/azure/scsi1/lun${LUN}
```

Barman expects the disks to be attached at the same mount point at which the disk used to create the original snapshot was mounted - this information is available in the metadata Barman stores about the backup.

For the example recovery of backup `20230614T130700`, we know the following:

- The disk used to create snapshot `barman-test-primary-pgdata-20230614t130700` was mounted at `/opt/postgres` with the options `rw,noatime`.
- A new disk was created from this snapshot named `recovery-pgdata` and it is attached with lun `5`.
- The disk used to create snapshot `barman-test-primary-tbs1-20230614t130700` was mounted at `/opt/postgres/tablespaces/tbs1` with the options `rw,noatime`.
- A new disk was created from this snapshot named `recovery-tbs1` and it is attached with lun `6`.

The following commands should therefore be run on the recovery instance:

```bash
mount -o rw,noatime /dev/disk/azure/scsi1/lun5 /opt/postgres
mount -o rw,noatime /dev/disk/azure/scsi1/lun6 /opt/postgres/tablespaces/tbs1
```

The recovered data is now available on the recovery VM and the recovery is ready to be finalized.

### Finalize the recovery with Barman

The final step is to run `barman recover` (if the backup was made with `barman backup`) or `barman-backup-restore` (if the backup was made with `barman-cloud-backup`).
This will copy the backup label into the PGDATA directory on the recovery VM and, in the case of `barman recover`, prepare PostgreSQL for recovery by adding any requested recovery options to `postgresql.auto.conf` and optionally copying any WALs into place.

More details about this step of the recovery can be found [in the Barman documentation][recovering-from-a-snapshot-backup].

[add-a-disk-to-a-linux-vm]: https://learn.microsoft.com/en-us/azure/virtual-machines/linux/add-disk
[az-disk-create]: https://learn.microsoft.com/en-us/cli/azure/disk?view=azure-cli-latest#az-disk-create
[az-vm-disk-attach]: https://learn.microsoft.com/en-us/cli/azure/vm/disk?view=azure-cli-latest#az-vm-disk-attach
[barman-cloud-snapshot-backups]: https://docs.pgbarman.org/release/latest/#barman-cloud-and-snapshot-backups
[barman-snapshot-backups]: https://docs.pgbarman.org/release/latest/#backup-with-cloud-snapshots
[format-and-mount-the-disk]: https://learn.microsoft.com/en-us/azure/virtual-machines/linux/add-disk?tabs=ubuntu#format-and-mount-the-disk
[recovering-from-a-snapshot-backup]: https://docs.pgbarman.org/release/latest/#recovering-from-a-snapshot-backup
