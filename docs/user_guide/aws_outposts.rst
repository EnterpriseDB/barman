.. _aws-outposts:

AWS Outposts
============

`AWS Outposts <https://aws.amazon.com/outposts/>`_ extends AWS infrastructure,
services, and APIs to on-premises hardware. Barman can use an Outpost as a
target for both cloud object storage backups and EBS volume snapshots, but
each of these uses a different Outpost capability with its own AWS-side
setup. This page covers that setup; it does not replace AWS's own Outposts
documentation, which should be treated as authoritative if the two ever
disagree.

Which section applies depends on the backup method in use:

* :ref:`S3 on Outposts <aws-outposts-s3>` applies when using cloud object
  storage backups, i.e. :ref:`streaming backups to the cloud
  <backup-streaming-backup-cloud>` or :ref:`local-to-cloud backups
  <backup-local-to-cloud-backup>` with a destination on the Outpost.
* :ref:`EBS local snapshots <aws-outposts-ebs-snapshots>` applies when using
  :ref:`cloud snapshot backups <cloud-snapshot-backups>` for an instance
  attached to the Outpost.

A single Barman server does not have to pick one or the other: an
Outposts-attached instance can use local EBS snapshots for its volumes while
still archiving WALs to an S3 on Outposts bucket, provided both are
configured as described below.

.. _aws-outposts-s3:

S3 on Outposts
--------------

S3 on Outposts provides object storage physically located on the Outpost,
addressed through the same ``s3://`` destination URLs used for regular S3
backups, but with an access point ARN in place of a bucket name (see
:ref:`local-to-cloud backups <backup-local-to-cloud-backup>` for the URL
format). Before pointing a Barman configuration at one, make sure the
following are in place on the AWS side:

* **S3 on Outposts storage capacity.** This is a dedicated storage pool
  ordered as part of, or added to, the Outpost. It does not scale
  elastically the way regular S3 does, so capacity should be sized and
  monitored ahead of Barman's expected backup and WAL volume.
* **An S3 on Outposts bucket**, created on that capacity through the
  ``S3Control`` API. Regular S3 buckets and S3 on Outposts buckets are
  managed through different AWS APIs and are not interchangeable.
* **An access point for that bucket.** S3 on Outposts has no direct bucket
  addressing: every object operation, including the ones Barman performs,
  must go through an access point. The access point ARN is what goes into
  Barman's destination URL.

  .. important::
      An access point ARN, not a bucket name, is required in Barman's
      ``s3://`` destination URLs when using S3 on Outposts.

* **An access point policy** granting the IAM principal Barman runs as the
  ``s3-outposts:*`` actions it needs (at minimum ``GetObject``,
  ``PutObject``, ``ListBucket`` and ``DeleteObject``). These are a distinct
  IAM action namespace from the ``s3:*`` actions used for regular S3, so an
  existing regular-S3 policy will not cover Outposts access.
* **Network connectivity to the S3 on Outposts endpoint.** S3 on Outposts is
  reachable only from within the VPC associated with the Outpost's subnet,
  through an S3 on Outposts endpoint created in that VPC; there is no public
  internet access. The host running Barman (or ``barman-cloud-*`` commands)
  needs a network path to that endpoint, which typically means running on
  the Outpost itself or having private connectivity to it.
* **A reasonably current version of** ``boto3``. Barman relies on the
  underlying AWS SDK to recognize an S3 on Outposts access point ARN and
  route requests to the correct endpoint automatically.

.. note::
    Encryption support for S3 on Outposts has historically differed from
    regular S3 (for example, in which server-side encryption options are
    available). Check the current
    `Amazon S3 on Outposts documentation
    <https://docs.aws.amazon.com/AmazonS3/latest/userguide/S3onOutposts.html>`_
    for the encryption options supported by your Outpost before relying on
    them.

.. _aws-outposts-ebs-snapshots:

EBS local snapshots on Outposts
--------------------------------

By default, a snapshot of an EBS volume attached to an Outposts instance is
stored back in the parent AWS Region. Barman instead creates these snapshots
locally on the Outpost automatically when it detects that the target
instance is Outposts-attached, with no additional configuration required.
Before relying on this, make sure the following are in place on the AWS
side:

* **The EC2 instance and its EBS volumes must be running on the Outpost**,
  not merely in the same account or Region as one.
* **Dedicated local EBS snapshot storage capacity** must be provisioned on
  the Outpost, separate from the volume capacity itself. If this pool is
  full, snapshot creation fails outright, so it should be monitored as part
  of normal Outpost capacity planning.
* **IAM permissions that cover Outpost resource ARNs.** The
  ``ec2:CreateSnapshot`` and ``ec2:DeleteSnapshot`` permissions already
  required for :ref:`cloud snapshot backups <cloud-snapshot-backups>` must
  extend to Outpost resources; a policy written only against regional EBS
  ARNs may not be sufficient.
* **Local EBS snapshot support on the specific Outpost configuration.**
  Availability can vary by Outpost rack or server form factor, so confirm
  the deployed Outpost supports this feature.

.. warning::
    Snapshot locking (``aws_snapshot_lock_mode`` and related options) is not
    supported for local snapshots on Outposts. Do not combine
    Outposts-attached instances with snapshot lock configuration.

.. note::
    A local snapshot cannot be copied directly to another AWS Region. It
    must first be copied to a Region-based snapshot before any further
    cross-Region copy.
