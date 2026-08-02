.. _cloud-snapshot-backups:

Cloud Snapshot Backups
======================

Barman can perform backups of Postgres servers deployed in specific cloud environments
by utilizing snapshots of storage volumes. In this setup, Postgres file backups are
represented as volume snapshots stored in the cloud, while Barman functions as the
storage server for Write-Ahead Logs (WALs) and the backup catalog. Despite the backup
data being stored in the cloud, Barman manages these backups similarly to traditional
ones created with ``rsync`` or ``postgres`` backup methods.

.. note::
    Additionally, snapshot backups can be created without a Barman server by using the
    ``barman-cloud-backup`` command directly on the Postgres server. Refer to the
    :ref:`barman cloud client package <barman-cloud-barman-client-package>` section for
    more information on how to properly work with this option.

.. important::
    The following configuration options and equivalent command arguments (if applicable)
    are not available when using ``backup_method=snapshot``:

    * ``backup_compression`` 
    * ``bandwidth_limit`` (``--bwlimit``)
    * ``parallel_jobs`` (``--jobs``)
    * ``network_compression``
    * ``reuse_backup`` (``--reuse-backup``)

To configure snapshot backups, include the following parameters in the Barman
configuration file for the specific Postgres server:

.. code-block:: text

    backup_method = snapshot
    snapshot_provider = CLOUD_PROVIDER
    snapshot_instance = INSTANCE_NAME
    snapshot_disks = DISK_NAME1,DISK_NAME2

.. important::
    Ensure ``snapshot_disks`` includes all disks that store Postgres data. Any data
    stored on a disk not listed will not be backed up and will be unavailable during
    recovery.

Requirements and Configuration
------------------------------

To use the snapshot backup method with Barman, your deployment must meet these
requirements:

1. Postgres must be running on a compute instance provided by a supported cloud
   provider.
2. All critical data, including PGDATA and tablespace data, must be stored on storage
   volumes that support snapshots.
3. The ``findmnt`` command must be available on the Postgres host.

.. important::
    Configuration files stored outside of ``PGDATA`` will not be included in the snapshots.
    You will need to manage these files separately, using a configuration management
    system or other mechanisms.

Google Cloud Platform
---------------------

To use snapshot backups on :term:`GCP` with Barman, please ensure the following:

1. **Python Libraries**

Install the ``google-cloud-compute`` and ``grpcio`` libraries for the Python
distribution used by Barman. These libraries are optional and not included by default.

Install them using pip:

.. code:: bash
  
    pip3 install grpcio google-cloud-compute

.. note::
    The ``google-cloud-compute`` library requires Python 3.7 or newer. GCP snapshots are
    not compatible with earlier Python versions.

2. **Disk Requirements**

The disks used in the ``snapshot`` backup must be zonal persistent disks. Regional
persistent disks are not supported at this time.

3. **Access Control**

Barman needs a service account with specific permissions. You can either attach this
account to the compute instance running Barman (recommended) or use the
``GOOGLE_APPLICATION_CREDENTIALS`` environment variable to specify a credentials
file.

.. important::
    Ensure the service account has the permissions listed below:

    * ``compute.disks.createSnapshot``
    * ``compute.disks.get``
    * ``compute.globalOperations.get``
    * ``compute.instances.get``
    * ``compute.snapshots.create``
    * ``compute.snapshots.delete``
    * ``compute.snapshots.list``

For provider specific credentials configurations, refer to the
`Google authentication methods <https://cloud.google.com/docs/authentication>`_ and
`service account impersonation <https://cloud.google.com/docs/authentication/use-service-account-impersonation>`_.

4. **Specific Configuration**

The fields ``gcp_project`` and ``gcp_zone`` are configuration options specific to GCP.

.. code-block:: text

    gcp_project = GCP_PROJECT_ID
    gcp_zone = ZONE

Microsoft Azure
---------------

To use snapshot backups on Azure with Barman, ensure the following:

1. **Python Libraries**

The ``azure-mgmt-compute`` and ``azure-identity`` libraries must be available for the
Python distribution used by Barman. These libraries are optional and not included by
default.

Install them using pip:

.. code:: bash

    pip3 install azure-mgmt-compute azure-identity

.. note::
    The ``azure-mgmt-compute`` library requires Python 3.7 or later. Azure snapshots are
    not compatible with earlier Python versions.

2. **Disk Requirements**

All disks involved in the snapshot backup must be managed disks attached to the VM
instance as data disks.

3. **Access Control**

Barman needs to access Azure using credentials obtained via managed identity or CLI
login. 

The following environment variables are supported: ``AZURE_STORAGE_CONNECTION_STRING``,
``AZURE_STORAGE_KEY`` and ``AZURE_STORAGE_SAS_TOKEN``. You can also use the
``--credential`` option to specify either ``default``, ``azure-cli`` or
``managed-identity`` credentials in order to authenticate via Azure Active Directory.

.. important::
    Ensure the credential has the permissions listed below:

    * ``Microsoft.Compute/disks/read``
    * ``Microsoft.Compute/virtualMachines/read``
    * ``Microsoft.Compute/snapshots/read``
    * ``Microsoft.Compute/snapshots/write``
    * ``Microsoft.Compute/snapshots/delete``

For provider specific credential configurations, refer to the
`Azure environment variables configurations <https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters>`_,
`Identity Package <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity?view=azure-python>`_ and 
`DefaultAzureCredential documentation <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python>`_.

4. **Specific Configuration**

The fields ``azure_subscription_id`` and ``azure_resource_group`` are configuration
options specific to Azure.

.. code-block:: text

    azure_subscription_id = AZURE_SUBSCRIPTION_ID
    azure_resource_group = AZURE_RESOURCE_GROUP
    
Amazon Web Services
-------------------

To use snapshot backups on :term:`AWS` with Barman, please ensure the following:

1. **Python Libraries**

The ``boto3`` library must be available for the Python distribution used by Barman. This
library is optional and not included by default.

Install it using pip:

.. code:: bash

    pip3 install boto3

2. **Disk Requirements**

All disks involved in the snapshot backup must be non-root EBS volumes attached to the
same VM instance.

.. note::
    If the VM instance is attached to an AWS Outpost, Barman automatically detects
    this and creates the snapshots locally on the Outpost instead of copying them
    back to the parent AWS Region. No additional configuration is required.

    AWS does not support locking local EBS snapshots on Outposts. Because of this,
    ``aws_snapshot_lock_mode`` cannot be used together with an instance attached to
    an Outpost; Barman will raise an error rather than attempt an unsupported lock.

3. **Access Control**

Barman needs to access AWS so you must configure the AWS credentials with the ``awscli``
tool as the postgres user, by entering the Access Key and Secret Key that must be
previously created in the IAM section of the AWS console.

.. important::
    Ensure you have the permissions listed below:

    * ``ec2:CreateSnapshot``
    * ``ec2:CreateTags``
    * ``ec2:DeleteSnapshot``
    * ``ec2:DescribeSnapshots``
    * ``ec2:DescribeInstances``
    * ``ec2:DescribeVolumes``

For provider specific credentials configurations, refer to the
`AWS boto3 configurations <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`_.

4. **Specific Configuration**

The fields ``aws_region``, ``aws_profile`` and ``aws_await_snapshots_timeout`` are
configuration options specific to AWS.

``aws_profile`` is the name of the AWS profile in the credentials file. If not used, the
default profile will be applied. If no credentials file exists, credentials will come from
the environment.

``aws_region`` overrides any region defined in the AWS profile.

``aws_await_snapshots_timeout`` is the timeout for waiting for snapshots to be created
(default is ``3600`` seconds).

When specifying ``snapshot_instance`` or ``snapshot_disks``, Barman accepts either the
instance/volume ID or the name of the resource. If you use a name, Barman will query AWS
for resources with a matching ``Name`` tag. If zero or multiple matches are found,
Barman will return an error.

.. code-block:: text

    aws_region = AWS_REGION
    aws_profile = AWS_PROFILE_NAME
    aws_await_snapshots_timeout = TIMEOUT_IN_SECONDS

5. **Ransomware Protection**

Ransomware protection is essential to secure data and maintain operational stability.
With Amazon EBS Snapshot Lock, snapshots are protected from deletion, providing an
immutable backup that safeguards against ransomware attacks. By locking snapshots,
unwanted deletions are prevented, ensuring reliable recovery options in case of
compromise. Barman can prevent unwanted deletion of backups by locking the snapshots
when creating the backup.

.. note::
    To delete a locked backup, you must first manually remove the lock in the AWS
    console.

To lock a snapshot during backup creation, you need to configure the following
options:

a. Choose the snapshot lock mode: either ``compliance`` or ``governance``.
b. Set either the lock duration or the expiration date (not both). Lock duration
   is specified in days, ranging from 1 to 36,500. If you choose an expiration
   date, it must be at least 1 day after the snapshot creation date and time,
   using the format ``YYYY-MM-DDTHH:MM:SS.sssZ``.
c. Optionally, set a cool-off period (in hours), from 1 to 72. This option only
   applies when the lock mode is set to ``compliance``.

.. code-block:: text

    aws_snapshot_lock_mode = compliance | governance
    aws_snapshot_lock_duration = 1
    aws_snapshot_lock_cool_off_period = 1
    aws_snapshot_lock_expiration_date = "2024-10-07T21:53:00.606Z"

.. important::
    Ensure you have the permission listed below:

    * ``ec2:LockSnapshot``

For the concepts behind AWS Snapshot Lock, refer to the
`Amazon EBS snapshot lock concepts <https://docs.aws.amazon.com/ebs/latest/userguide/snapshot-lock-concepts.html>`_.

6. **AWS Outposts**

If the target instance is attached to an AWS Outpost, Barman automatically
detects this and creates the EBS snapshots locally on the Outpost instead of
the parent AWS Region, with no additional configuration required. Refer to
:ref:`AWS Outposts <aws-outposts-ebs-snapshots>` for the AWS-side
prerequisites this requires, including a limitation on combining it with
snapshot lock.

Backup Process
--------------

Here is an overview of the snapshot backup process:

1. Barman performs checks to validate the snapshot options, instance, and disks.
    Before each backup and during the ``barman check`` command, the following checks are
    performed:

    * The compute instance specified by ``snapshot_instance`` and any provider-specific
      arguments exists.
    * The disks listed in ``snapshot_disks`` are present.
    * The disks listed in ``snapshot_disks`` are attached to the ``snapshot_instance``.
    * The disks listed in ``snapshot_disks`` are mounted on the ``snapshot_instance``.

2. Barman initiates the backup using the Postgres backup API.
3. The cloud provider API is used to create a snapshot for each specified disk. Barman
   waits until each snapshot reaches a state that guarantees application consistency
   before proceeding to the next disk.
4. Additional provider-specific details, such as the device name for each disk, and the
   mount point and options for each disk are recorded in the backup metadata.

Metadata
^^^^^^^^

Regardless of whether you provision recovery disks and instances using
infrastructure-as-code, ad-hoc automation, or manually, you will need to use Barman to
identify the necessary snapshots for a specific backup. You can do this with the barman
``show-backup`` command, which provides details for each snapshot included in the
backup.

For example:

.. code-block:: text

    Backup 20240813T200506:
      Server Name            : snapshot
      System Id              : 7402620047885836080
      Status                 : DONE
      PostgreSQL Version     : 160004
      PGDATA directory       : /opt/postgres/data
      Estimated Cluster Size : 22.7 MiB

      Server information:
        Checksums            : on

      Snapshot information:
        provider             : aws
        account_id           : 714574844897
        region               : sa-east-1

        device_name          : /dev/sdf
        snapshot_id          : snap-0d2288b4f30e3f9e3
        snapshot_name        : Barman_AWS:1:/dev/sdf-20240813t200506
        Mount point          : /opt/postgres
        Mount options        : rw,noatime,seclabel

      Base backup information:
        Backup Method        : snapshot-concurrent
        Backup Size          : 1.0 KiB (16.0 MiB with WALs)
        WAL Size             : 16.0 MiB
        Timeline             : 1
        Begin WAL            : 00000001000000000000001A
        End WAL              : 00000001000000000000001A
        Number of WALs       : 1
        Begin time           : 2024-08-14 16:21:50.820618+00:00
        End time             : 2024-08-14 16:22:38.264726+00:00
        Copy time            : 47 seconds
        Estimated throughput : 22 B/s
        Begin Offset         : 40
        End Offset           : 312
        Begin LSN            : 0/1A000028
        End LSN              : 0/1A000138

      WAL information:
        Number of files      : 1
        Disk usage           : 16.0 MiB
        WAL rate             : 5048.32/hour
        Last available       : 00000001000000000000001B

      Catalog information:
        Retention Policy     : not enforced
        Previous Backup      : - (this is the oldest base backup)
        Next Backup          : - (this is the latest base backup)

The ``--format=json`` option can be used when integrating with external tooling.

.. code-block:: json

    {
      "snapshots_info": {
        "provider": "gcp",
        "provider_info": {
          "project": "project_id"
        },
        "snapshots": [
          {
            "mount": {
              "mount_options": "rw,noatime",
              "mount_point": "/opt/postgres"
            },
            "provider": {
              "device_name": "pgdata",
              "snapshot_name": "barman-av-ubuntu20-primary-pgdata-20230123t131430",
              "snapshot_project": "project_id"
            }
          },
          {
            "mount": {
              "mount_options": "rw,noatime",
              "mount_point": "/opt/postgres/tablespaces/tbs1"
            },
            "provider": {
              "device_name": "tbs1",
              "snapshot_name": "barman-av-ubuntu20-primary-tbs1-20230123t131430",
              "snapshot_project": "project_id",
            }
          }
        ]
      }
    }

The metadata found in ``snapshots_info/provider_info`` and
``snapshots_info/snapshots/*/provider`` varies depending on the cloud provider, as
detailed in the following sections.

**GCP**

``snapshots_info/provider_info``

* ``project``: The GCP project ID of the project which owns the resources involved
  in backup and recovery.

``snapshots_info/snapshots/*/provider``

* ``device_name``: The short device name with which the source disk for the snapshot
  was attached to the backup VM at the time of the backup.
* ``snapshot_name``: The name of the snapshot.
* ``snapshot_project``: The GCP project ID which owns the snapshot.

**Azure**

``snapshots_info/provider_info``

* ``subscription_id``: The Azure subscription ID which owns the resources involved
  in backup and recovery.
* ``resource_group``: The Azure resource group to which the resources involved in
  the backup belong.

``snapshots_info/snapshots/*/provider``

* ``location``: The Azure location of the disk from which the snapshot was taken.
* ``lun``: The LUN identifying the disk from which the snapshot was taken at the
  time of the backup.
* ``snapshot_name``: The name of the snapshot.

**AWS**

``snapshots_info/provider_info``

* ``account_id``: The ID of the AWS account which owns the resources used to make
  the backup.
* ``region``: The AWS region in which the resources involved in backup are located.

``snapshots_info/snapshots/*/provider``

* ``device_name``: The device to which the source disk was mapped on the backup VM
  at the time of the backup.
* ``snapshot_id``: The ID of the snapshot as assigned by AWS.
* ``snapshot_name``: The name of the snapshot.
