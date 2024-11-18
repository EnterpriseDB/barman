# Recovering snapshot backups on AWS EC2

## Overview

This runbook describes the steps that must be followed in order to recover a snapshot backup made using AWS EBS volume snapshots on EC2.

## Prerequisites and limitations

The following assumptions are made about the recovery scenario:

1. A recent snapshot backup has been taken using either [`barman backup`][barman-snapshot-backups] or [`barman-cloud-backup`][barman-cloud-snapshot-backups].
2. A recovery VM has been provisioned and PostgreSQL has been installed.

The example commands given are the bare minimum required to perform the recovery.
It is highly recommended that you consult the AWS documentation and consider whether the default options are suitable for your environment and whether any additional options are required.

## Snapshot recovery steps

In order to recover the snapshot backup the following steps must be taken:

1. Review the necessary metadata for recovering the snapshot backup.
2. Create a new EBS Volume for each snapshot in the backup.
3. Attach each disk to the recovery VM.
4. Mount each attached disk at the expected mount point for your PostgreSQL installation.
5. Finalize the recovery with Barman.

### Review the necessary metadata for recovering the snapshot backup.

The information required to recover the snapshots can be found in the backup metadata managed by Barman.
For example, for backup `20230719T111532` made with `barman backup`:

```
barman@barman:~ $ barman show-backup primary 20230719T111532
Backup 20230719T111532:
  Server Name            : primary
  System Id              : 7257451984620623351
  Status                 : DONE
  PostgreSQL Version     : 140008
  PGDATA directory       : /opt/postgres/data

  Snapshot information:
    provider             : aws
    account_id           : AWS_ACCOUNT_ID
    region               : eu-west-1

    device_name          : /dev/sdf
    snapshot_id          : snap-00726674e0e859757
    snapshot_name        : barman-test-primary-pgdata-20230719t111532
    Mount point          : /opt/postgres
    Mount options        : rw,noatime

    device_name          : /dev/sdg
    snapshot_id          : snap-005176dd63fa66ccc
    snapshot_name        : barman-test-primary-tbs1-20230719t111532
    Mount point          : /opt/postgres/tablespaces/tbs1
    Mount options        : rw,noatime
...
```

Alternatively, for backup `20230719T091506` made with `barman-cloud-backup`:

```
postgres@primary:~ $ barman-cloud-backup-show s3://barman-test primary 20230719T091506
Backup 20230719T091506:
  Server Name            : primary
  System Id              : 7257451984620623351
  Status                 : DONE
  PostgreSQL Version     : 140008
  PGDATA directory       : /opt/postgres/data

  Snapshot information:
    provider             : aws
    account_id           : AWS_ACCOUNT_ID
    region               : eu-west-1

    device_name          : /dev/sdf
    snapshot_id          : snap-0851ae9a67b4d5f42
    snapshot_name        : barman-test-primary-pgdata-20230719t091506
    Mount point          : /opt/postgres
    Mount options        : rw,noatime

    device_name          : /dev/sdg
    snapshot_id          : snap-0646e91967434cd5b
    snapshot_name        : barman-test-primary-tbs1-20230719t091506
    Mount point          : /opt/postgres/tablespaces/tbs1
    Mount options        : rw,noatime
...
```

The `--format=json` option can be used with either command to view the metadata as a JSON object.
Snapshot metadata will be available under the `snapshots_info` key and will have the following structure:

```
"snapshots_info": {
  "provider": "aws",
  "provider_info": {
    "account_id": "AWS_ACCOUNT_ID",
    "region": "AWS_REGION"
  },
  "snapshots": [
    {
      "mount": {
        "mount_options": "rw,noatime",
        "mount_point": "/opt/postgres"
      },
      "provider": {
        "device_name": "/dev/sdf",
        "snapshot_id": "snap-00726674e0e859757",
        "snapshot_name": "barman-test-primary-pgdata-20230719t111532"
      }
    },
    {
      "mount": {
        "mount_options": "rw,noatime",
        "mount_point": "/opt/postgres/tablespaces/tbs1"
      },
      "provider": {
        "device_name": "/dev/sdg",
        "snapshot_id": "snap-005176dd63fa66ccc",
        "snapshot_name": "barman-test-primary-tbs1-20230719t111532"
      }
    }
  ]
},
```

Note the following values for each snapshot as they will be required later in the process:

1. `mount/mount_point`
2. `mount/mount_options`
3. `provider/snapshot_id`

### Create a new EBS Volume for each snapshot in the backup

A new disk must be created for each snapshot listed in the backup metadata.
New disks can be created using the [`aws ec2 create-volume` command][aws-create-volume] and the snapshot can be specified using the `--snapshot-id` option.

For example, for backup `20230719T111532`, the following commands should be run:

```
barman@barman:~ $ aws ec2 create-volume --availability-zone eu-west-1a --snapshot-id snap-00726674e0e859757
{
    "AvailabilityZone": "eu-west-1a",
    "CreateTime": "2023-07-19T13:05:17+00:00",
    "Encrypted": false,
    "Size": 10,
    "SnapshotId": "snap-00726674e0e859757",
    "State": "creating",
    "VolumeId": "vol-02f4de6148c1bca91",
    "Iops": 100,
    "Tags": [],
    "VolumeType": "gp2",
    "MultiAttachEnabled": false
}
barman@barman:~ $ aws ec2 create-volume --availability-zone eu-west-1a --snapshot-id snap-005176dd63fa66ccc
{
    "AvailabilityZone": "eu-west-1a",
    "CreateTime": "2023-07-19T13:06:56+00:00",
    "Encrypted": false,
    "Size": 10,
    "SnapshotId": "snap-005176dd63fa66ccc",
    "State": "creating",
    "VolumeId": "vol-0836b3cc3e37d39fc",
    "Iops": 100,
    "Tags": [],
    "VolumeType": "gp2",
    "MultiAttachEnabled": false
}
```

The `VolumeId` for each volume will be required in the next step when attaching the volumes to the recovery instance.

### Attach each disk to the recovery VM

Each disk must be attached to the recovery VM so that it can be mounted at the correct location.
This can be achieved using the [`aws ec2 attach-volume` command][aws-attach-volume].

To recover the backup `20230719T111532` onto a recovery instance named `barman-test-recovery` with an instance ID of `i-0ab99cab451990eeb`, the following commands should be run to attach the disks created in the previous step:

```
barman@barman:~ $ aws ec2 attach-volume --instance-id i-0ab99cab451990eeb --volume-id vol-02f4de6148c1bca91 --device /dev/sdf
{
    "AttachTime": "2023-07-19T13:16:33.790000+00:00",
    "Device": "/dev/sdf",
    "InstanceId": "i-0ab99cab451990eeb",
    "State": "attaching",
    "VolumeId": "vol-02f4de6148c1bca91"
}
barman@barman:~ $ aws ec2 attach-volume --instance-id i-0ab99cab451990eeb --volume-id vol-0836b3cc3e37d39fc --device /dev/sdg
{
    "AttachTime": "2023-07-19T13:16:56.653000+00:00",
    "Device": "/dev/sdg",
    "InstanceId": "i-0ab99cab451990eeb",
    "State": "attaching",
    "VolumeId": "vol-0836b3cc3e37d39fc"
}
```

The device name assigned to the attached device is required in the next step when mounting the disks on the recovery VM.
Note that the device name specified here may be remapped to a different name when it is attached to the instance.
The possible re-mappings and the rules regarding device name are specified in the [AWS documentation][aws-device-naming].

### Mount each attached disk at the expected mount point for your PostgreSQL installation.

Mounting each attached disk must be carried out on the recovery VM.
Barman expects the disks to be attached at the same mount point at which the disk used to create the original snapshot was mounted - this information is available in the metadata Barman stores about the backup.

Note that Barman stores the device name assigned to the volume attachment, not the final device name given when attaching the volume.
You will therefore need to consider any possible changes to the device name when the volume is attached to the instance.
For example, if your recovery instance is using hardware virtualization, a volume with a device name of `/dev/sdf` will appear as `/dev/xvdf` on the instance.

For the example recovery of backup `20230719T111532`, the following information can be used to determine how to mount the volumes:

- The disk used to create snapshot `snap-00726674e0e859757` was mounted at `/opt/postgres` with the options `rw,noatime`.
- A new disk was created from this snapshot with the volume ID `vol-02f4de6148c1bca91` and it is attached with device name `/dev/sdf`.
- The disk used to create snapshot `snap-005176dd63fa66ccc` was mounted at `/opt/postgres/tablespaces/tbs1` with the options `rw,noatime`.
- A new disk was created from this snapshot with the volume ID `vol-0836b3cc3e37d39fc` and it is attached with device name `/dev/sdg`.

In this scenario the recovery instance is using hardware virtualization so the devices `/dev/sdf` and `/dev/sdg` will be renamed as `/dev/xvdf` and `/dev/xvdg`.

The following commands should therefore be run on the recovery instance:

```
mount -o rw,noatime /dev/xvdf /opt/postgres
mount -o rw,noatime /dev/xvdg /opt/postgres/tablespaces/tbs1
```

The recovered data is now available on the recovery VM and the recovery is ready to be finalized.

### Finalize the recovery with Barman.

The final step is to run `barman recover` (if the backup was made with `barman backup`) or `barman-backup-restore` (if the backup was made with `barman-cloud-backup`).
This will copy the backup label into the PGDATA directory on the recovery VM and, in the case of `barman recover`, prepare PostgreSQL for recovery by adding any requested recovery options to `postgresql.auto.conf` and optionally copying any WALs into place.

More details about this step of the recovery can be found [in the Barman documentation][recovering-from-a-snapshot-backup].

[barman-cloud-snapshot-backups]: https://docs.pgbarman.org/release/latest/#barman-cloud-and-snapshot-backups
[barman-snapshot-backups]: https://docs.pgbarman.org/release/latest/#backup-with-cloud-snapshots
[aws-create-volume]: https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ec2/create-volume.html
[aws-attach-volume]: https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ec2/attach-volume.html
[aws-device-naming]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
[recovering-from-a-snapshot-backup]: https://docs.pgbarman.org/release/latest/#recovering-from-a-snapshot-backup
