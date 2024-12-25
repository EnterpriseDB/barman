# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2025
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import json
import logging
import math
import shutil
from datetime import datetime
from io import RawIOBase

from barman.clients.cloud_compression import decompress_to_file
from barman.cloud import (
    DEFAULT_DELIMITER,
    CloudInterface,
    CloudProviderError,
    CloudSnapshotInterface,
    DecompressingStreamingIO,
    SnapshotMetadata,
    SnapshotsInfo,
    VolumeMetadata,
)
from barman.exceptions import (
    CommandException,
    SnapshotBackupException,
    SnapshotInstanceNotFoundException,
)

try:
    # Python 3.x
    from urllib.parse import urlencode, urlparse
except ImportError:
    # Python 2.x
    from urllib import urlencode

    from urlparse import urlparse

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:
    raise SystemExit("Missing required python module: boto3")


class StreamingBodyIO(RawIOBase):
    """
    Wrap a boto StreamingBody in the IOBase API.
    """

    def __init__(self, body):
        self.body = body

    def readable(self):
        return True

    def read(self, n=-1):
        n = None if n < 0 else n
        return self.body.read(n)


class S3CloudInterface(CloudInterface):
    # S3 multipart upload limitations
    # http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html
    MAX_CHUNKS_PER_FILE = 10000
    MIN_CHUNK_SIZE = 5 << 20

    # S3 permit a maximum of 5TB per file
    # https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    MAX_DELETE_BATCH_SIZE = 1000

    # The minimum size for a file to be uploaded using multipart upload in upload_fileobj
    # 100MB is the AWS recommendation for when to start considering using multipart upload
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    MULTIPART_THRESHOLD = 104857600

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove boto3 client reference from the state as it cannot be pickled
        # in Python >= 3.8 and multiprocessing will pickle the object when the
        # worker processes are created.
        # The worker processes create their own boto3 sessions so do not need
        # the boto3 session from the parent process.
        del state["s3"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __init__(
        self,
        url,
        encryption=None,
        jobs=2,
        profile_name=None,
        endpoint_url=None,
        tags=None,
        delete_batch_size=None,
        read_timeout=None,
        sse_kms_key_id=None,
    ):
        """
        Create a new S3 interface given the S3 destination url and the profile
        name

        :param str url: Full URL of the cloud destination/source
        :param str|None encryption: Encryption type string
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        :param str profile_name: Amazon auth profile identifier
        :param str endpoint_url: override default endpoint detection strategy
          with this one
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        :param int|None read_timeout: the time in seconds until a timeout is
          raised when waiting to read from a connection
        :param str|None sse_kms_key_id: the AWS KMS key ID that should be used
          for encrypting uploaded data in S3
        """
        super(S3CloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
            delete_batch_size=delete_batch_size,
        )
        self.profile_name = profile_name
        self.encryption = encryption
        self.endpoint_url = endpoint_url
        self.read_timeout = read_timeout
        self.sse_kms_key_id = sse_kms_key_id

        # Extract information from the destination URL
        parsed_url = urlparse(url)
        # If netloc is not present, the s3 url is badly formatted.
        if parsed_url.netloc == "" or parsed_url.scheme != "s3":
            raise ValueError("Invalid s3 URL address: %s" % url)
        self.bucket_name = parsed_url.netloc
        self.bucket_exists = None
        self.path = parsed_url.path.lstrip("/")

        # initialize the config object to be used in uploads
        self.config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD)

        # Build a session, so we can extract the correct resource
        self._reinit_session()

    def _reinit_session(self):
        """
        Create a new session
        """
        config_kwargs = {}
        if self.read_timeout is not None:
            config_kwargs["read_timeout"] = self.read_timeout
        config = Config(**config_kwargs)

        session = boto3.Session(profile_name=self.profile_name)
        self.s3 = session.resource("s3", endpoint_url=self.endpoint_url, config=config)

    @property
    def _extra_upload_args(self):
        """
        Return a dict containing ExtraArgs to be passed to certain boto3 calls

        Because some boto3 calls accept `ExtraArgs: {}` and others do not, we
        return a nested dict which can be expanded with `**` in the boto3 call.
        """
        additional_args = {}
        if self.encryption:
            additional_args["ServerSideEncryption"] = self.encryption
        if self.sse_kms_key_id:
            additional_args["SSEKMSKeyId"] = self.sse_kms_key_id
        return additional_args

    def test_connectivity(self):
        """
        Test AWS connectivity by trying to access a bucket
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to try if aws is reachable
            self.bucket_exists = self._check_bucket_existence()
            return True
        except EndpointConnectionError as exc:
            logging.error("Can't connect to cloud provider: %s", exc)
            return False

    def _check_bucket_existence(self):
        """
        Check cloud storage for the target bucket

        :return: True if the bucket exists, False otherwise
        :rtype: bool
        """
        try:
            # Search the bucket on s3
            self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as exc:
            # If a client error is thrown, then check the error code.
            # If code was 404, then the bucket does not exist
            error_code = exc.response["Error"]["Code"]
            if error_code == "404":
                return False
            # Otherwise there is nothing else to do than re-raise the original
            # exception
            raise

    def _create_bucket(self):
        """
        Create the bucket in cloud storage
        """
        # Get the current region from client.
        # Do not use session.region_name here because it may be None
        region = self.s3.meta.client.meta.region_name
        logging.info(
            "Bucket '%s' does not exist, creating it on region '%s'",
            self.bucket_name,
            region,
        )
        create_bucket_config = {
            "ACL": "private",
        }
        # The location constraint is required during bucket creation
        # for all regions outside of us-east-1. This constraint cannot
        # be specified in us-east-1; specifying it in this region
        # results in a failure, so we will only
        # add it if we are deploying outside of us-east-1.
        # See https://github.com/boto/boto3/issues/125
        if region != "us-east-1":
            create_bucket_config["CreateBucketConfiguration"] = {
                "LocationConstraint": region,
            }
        self.s3.Bucket(self.bucket_name).create(**create_bucket_config)

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        if prefix.startswith(delimiter):
            prefix = prefix.lstrip(delimiter)

        paginator = self.s3.meta.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter
        )

        for page in pages:
            # List "folders"
            keys = page.get("CommonPrefixes")
            if keys is not None:
                for k in keys:
                    yield k.get("Prefix")

            # List "files"
            objects = page.get("Contents")
            if objects is not None:
                for o in objects:
                    yield o.get("Key")

    def download_file(self, key, dest_path, decompress):
        """
        Download a file from S3

        :param str key: The S3 key to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        # Open the remote file
        obj = self.s3.Object(self.bucket_name, key)
        remote_file = obj.get()["Body"]

        # Write the dest file in binary mode
        with open(dest_path, "wb") as dest_file:
            # If the file is not compressed, just copy its content
            if decompress is None:
                shutil.copyfileobj(remote_file, dest_file)
                return

            decompress_to_file(remote_file, dest_file, decompress)

    def remote_open(self, key, decompressor=None):
        """
        Open a remote S3 object and returns a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """
        try:
            obj = self.s3.Object(self.bucket_name, key)
            resp = StreamingBodyIO(obj.get()["Body"])
            if decompressor:
                return DecompressingStreamingIO(resp, decompressor)
            else:
                return resp
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                return None
            else:
                raise

    def upload_fileobj(self, fileobj, key, override_tags=None):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of k,v tuples which should override any
          tags already defined in the cloud interface
        """
        extra_args = self._extra_upload_args.copy()
        tags = override_tags or self.tags
        if tags is not None:
            extra_args["Tagging"] = urlencode(tags)
        self.s3.meta.client.upload_fileobj(
            Fileobj=fileobj,
            Bucket=self.bucket_name,
            Key=key,
            ExtraArgs=extra_args,
            Config=self.config,
        )

    def create_multipart_upload(self, key):
        """
        Create a new multipart upload

        :param key: The key to use in the cloud service
        :return: The multipart upload handle
        :rtype: dict[str, str]
        """
        extra_args = self._extra_upload_args.copy()
        if self.tags is not None:
            extra_args["Tagging"] = urlencode(self.tags)
        return self.s3.meta.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=key, **extra_args
        )

    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a part into this multipart upload

        :param dict upload_metadata: The multipart upload handle
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part handle
        :rtype: dict[str, None|str]
        """
        part = self.s3.meta.client.upload_part(
            Body=body,
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_metadata["UploadId"],
            PartNumber=part_number,
        )
        return {
            "PartNumber": part_number,
            "ETag": part["ETag"],
        }

    def _complete_multipart_upload(self, upload_metadata, key, parts):
        """
        Finish a certain multipart upload

        :param dict upload_metadata:  The multipart upload handle
        :param str key: The key to use in the cloud service
        :param parts: The list of parts composing the multipart upload
        """
        self.s3.meta.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=upload_metadata["UploadId"],
            MultipartUpload={"Parts": parts},
        )

    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        :param dict upload_metadata:  The multipart upload handle
        :param str key: The key to use in the cloud service
        """
        self.s3.meta.client.abort_multipart_upload(
            Bucket=self.bucket_name, Key=key, UploadId=upload_metadata["UploadId"]
        )

    def _delete_objects_batch(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        super(S3CloudInterface, self)._delete_objects_batch(paths)

        resp = self.s3.meta.client.delete_objects(
            Bucket=self.bucket_name,
            Delete={
                "Objects": [{"Key": path} for path in paths],
                "Quiet": True,
            },
        )
        if "Errors" in resp:
            for error_dict in resp["Errors"]:
                logging.error(
                    'Deletion of object %s failed with error code: "%s", message: "%s"'
                    % (error_dict["Key"], error_dict["Code"], error_dict["Message"])
                )
            raise CloudProviderError()

    def get_prefixes(self, prefix):
        """
        Return only the common prefixes under the supplied prefix.

        :param str prefix: The object key prefix under which the common prefixes
            will be found.
        :rtype: Iterator[str]
        :return: A list of unique prefixes immediately under the supplied prefix.
        """
        for wal_prefix in self.list_bucket(prefix + "/", delimiter="/"):
            if wal_prefix.endswith("/"):
                yield wal_prefix

    def delete_under_prefix(self, prefix):
        """
        Delete all objects under the specified prefix.

        :param str prefix: The object key prefix under which all objects should be
            deleted.
        """
        if len(prefix) == 0 or prefix == "/" or not prefix.endswith("/"):
            raise ValueError(
                "Deleting all objects under prefix %s is not allowed" % prefix
            )
        bucket = self.s3.Bucket(self.bucket_name)
        for resp in bucket.objects.filter(Prefix=prefix).delete():
            response_metadata = resp["ResponseMetadata"]
            if response_metadata["HTTPStatusCode"] != 200:
                logging.error(
                    'Deletion of objects under %s failed with error code: "%s"'
                    % (prefix, response_metadata["HTTPStatusCode"])
                )
                raise CloudProviderError()


class AwsCloudSnapshotInterface(CloudSnapshotInterface):
    """
    Implementation of CloudSnapshotInterface for EBS snapshots as implemented in AWS
    as documented at:

        https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-snapshot.html
    """

    def __init__(
        self,
        profile_name=None,
        region=None,
        await_snapshots_timeout=3600,
        lock_mode=None,
        lock_duration=None,
        lock_cool_off_period=None,
        lock_expiration_date=None,
        tags=None,
    ):
        """
        Creates the client necessary for creating and managing snapshots.

        :param str profile_name: AWS auth profile identifier.
        :param str region: The AWS region in which snapshot resources are located.
        :param int await_snapshots_timeout: The maximum time in seconds to wait for
            snapshots to complete.
        :param str lock_mode: The lock mode to apply to the snapshot.
        :param int lock_duration: The duration (in days) for which the snapshot
            should be locked.
        :param int lock_cool_off_period: The cool-off period (in hours) for the snapshot.
        :param str lock_expiration_date: The expiration date for the snapshot in the format
            ``YYYY-MM-DDThh:mm:ss.sssZ``.
        :param List[Tuple[str, str]] tags: Key value pairs for tags to be applied.
        """

        self.session = boto3.Session(profile_name=profile_name)
        # If a specific region was provided then this overrides any region which may be
        # defined in the profile
        self.region = region or self.session.region_name
        self.ec2_client = self.session.client("ec2", region_name=self.region)
        self.await_snapshots_timeout = await_snapshots_timeout
        self.tags = tags
        self.lock_mode = lock_mode
        self.lock_duration = lock_duration
        self.lock_cool_off_period = lock_cool_off_period
        self.lock_expiration_date = lock_expiration_date

    def _get_waiter_config(self):
        delay = 15
        # Use ceil so that we always wait for at least the specified timeout
        max_attempts = math.ceil(self.await_snapshots_timeout / delay)
        return {
            "Delay": delay,
            # Ensure we always try waiting at least once
            "MaxAttempts": max(max_attempts, 1),
        }

    def _get_instance_metadata(self, instance_identifier):
        """
        Retrieve the boto3 describe_instances metadata for the specified instance.

        The supplied instance_identifier can be either an AWS instance ID or a name.
        If an instance ID is supplied then this function will look it up directly. If
        a name is supplied then the `tag:Name` filter will be used to query the AWS
        API for instances with the matching `Name` tag.

        :param str instance_identifier: The instance ID or name of the VM instance.
        :rtype: dict
        :return: A dict containing the describe_instances metadata for the specified
            VM instance.
        """
        # Consider all states other than `terminated` as valid instances
        allowed_states = ["pending", "running", "shutting-down", "stopping", "stopped"]
        # If the identifier looks like an instance ID then we attempt to look it up
        resp = None
        if instance_identifier.startswith("i-"):
            try:
                resp = self.ec2_client.describe_instances(
                    InstanceIds=[instance_identifier],
                    Filters=[
                        {"Name": "instance-state-name", "Values": allowed_states},
                    ],
                )
            except ClientError as exc:
                error_code = exc.response["Error"]["Code"]
                # If we have a malformed instance ID then continue and treat it
                # like a name, otherwise re-raise the original error
                if error_code != "InvalidInstanceID.Malformed":
                    raise
        # If we do not have a response then try looking up by name
        if resp is None:
            resp = self.ec2_client.describe_instances(
                Filters=[
                    {"Name": "tag:Name", "Values": [instance_identifier]},
                    {"Name": "instance-state-name", "Values": allowed_states},
                ]
            )
        # Check for non-unique reservations and instances before returning the instance
        # because tag uniqueness is not a thing
        reservations = resp["Reservations"]
        if len(reservations) == 1:
            if len(reservations[0]["Instances"]) == 1:
                return reservations[0]["Instances"][0]
            elif len(reservations[0]["Instances"]) > 1:
                raise CloudProviderError(
                    "Cannot find a unique EC2 instance matching {}".format(
                        instance_identifier
                    )
                )
        elif len(reservations) > 1:
            raise CloudProviderError(
                "Cannot find a unique EC2 reservation containing instance {}".format(
                    instance_identifier
                )
            )
        raise SnapshotInstanceNotFoundException(
            "Cannot find instance {}".format(instance_identifier)
        )

    def _has_tag(self, resource, tag_name, tag_value):
        """
        Determine whether the resource metadata contains a specified tag.

        :param dict resource: Metadata describing an AWS resource.
        :parma str tag_name: The name of the tag to be checked.
        :param str tag_value: The value of the tag to be checked.
        :rtype: bool
        :return: True if a tag with the specified name and value was found, False
            otherwise.
        """
        if "Tags" in resource:
            for tag in resource["Tags"]:
                if tag["Key"] == tag_name and tag["Value"] == tag_value:
                    return True
        return False

    def _lookup_volume(self, attached_volumes, volume_identifier):
        """
        Searches a supplied list of describe_volumes metadata for the specified volume.

        :param list[dict] attached_volumes: A list of volumes in the format provided by
            the boto3 describe_volumes function.
        :param str volume_identifier: The volume ID or name of the volume to be looked
            up.
        :rtype: dict|None
        :return: describe_volume metadata for the volume matching the supplied
            identifier.
        """
        # Check whether volume_identifier matches a VolumeId
        matching_volumes = [
            volume
            for volume in attached_volumes
            if volume["VolumeId"] == volume_identifier
        ]
        # If we do not have a match, try again but search for a matching Name tag
        if not matching_volumes:
            matching_volumes = [
                volume
                for volume in attached_volumes
                if self._has_tag(volume, "Name", volume_identifier)
            ]
        # If there is more than one matching volume then it's an error condition
        if len(matching_volumes) > 1:
            raise CloudProviderError(
                "Duplicate volumes found matching {}: {}".format(
                    volume_identifier,
                    ", ".join(v["VolumeId"] for v in matching_volumes),
                )
            )
        # If no matching volumes were found then return None - it is up to the calling
        # code to decide if this is an error
        elif len(matching_volumes) == 0:
            return None
        # Otherwise, we found exactly one matching volume and return its metadata
        else:
            return matching_volumes[0]

    def _get_requested_volumes(self, instance_metadata, disks=None):
        """
        Fetch describe_volumes metadata for disks attached to a specified VM instance.

        Queries the AWS API for metadata describing the volumes attached to the
        instance described in instance_metadata.

        If `disks` is specified then metadata is only returned for the volumes that are
        included in the list and attached to the instance. Volumes which are requested
        in the `disks` list but not attached to the instance are not included in the
        response - it is up to calling code to decide whether this is an error
        condition.

        Entries in `disks` can be either volume IDs or names. The value provided for
        each volume will be included in the response under the key `identifier`.

        If `disks` is not provided then every non-root volume attached to the instance
        will be included in the response.

        :param dict instance_metadata: A dict containing the describe_instances metadata
            for a VM instance.
        :param list[str] disks: A list of volume IDs or volume names. If specified then
            only volumes in this list which are attached to the instance described by
            instance_metadata will be included in the response.
        :rtype: list[dict[str,str|dict]]
        :return: A list of dicts containing identifiers and describe_volumes metadata
            for the requested volumes.
        """
        # Pre-fetch the describe_volumes output for all volumes attached to the instance
        attached_volumes = self.ec2_client.describe_volumes(
            Filters=[
                {
                    "Name": "attachment.instance-id",
                    "Values": [instance_metadata["InstanceId"]],
                },
            ]
        )["Volumes"]
        # If disks is None then use a list of all Ebs volumes attached to the instance
        requested_volumes = []
        if disks is None:
            disks = [
                device["Ebs"]["VolumeId"]
                for device in instance_metadata["BlockDeviceMappings"]
                if "Ebs" in device
            ]
        # For each requested volume, look it up in the describe_volumes output using
        # _lookup_volume which will handle both volume IDs and volume names
        for volume_identifier in disks:
            volume = self._lookup_volume(attached_volumes, volume_identifier)
            if volume is not None:
                attachment_metadata = None
                for attachment in volume["Attachments"]:
                    if attachment["InstanceId"] == instance_metadata["InstanceId"]:
                        attachment_metadata = attachment
                        break
                if attachment_metadata is not None:
                    # Ignore the root volume
                    if (
                        attachment_metadata["Device"]
                        == instance_metadata["RootDeviceName"]
                    ):
                        continue
                    snapshot_id = None
                    if "SnapshotId" in volume and volume["SnapshotId"] != "":
                        snapshot_id = volume["SnapshotId"]
                    requested_volumes.append(
                        {
                            "identifier": volume_identifier,
                            "attachment_metadata": attachment_metadata,
                            "source_snapshot": snapshot_id,
                        }
                    )
        return requested_volumes

    def _create_snapshot(self, backup_info, volume_name, volume_id):
        """
        Create a snapshot of an EBS volume in AWS.

        Unlike its counterparts in AzureCloudSnapshotInterface and
        GcpCloudSnapshotInterface, this function does not wait for the snapshot to
        enter a successful completed state and instead relies on the calling code
        to perform any necessary waiting.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str volume_name: The user-supplied identifier for the volume. Used
            when creating the snapshot name.
        :param str volume_id: The AWS volume ID. Used when calling the AWS API to
            create the snapshot.
        :rtype: (str, dict)
        :return: The snapshot name and the snapshot metadata returned by AWS.
        """
        snapshot_name = "%s-%s" % (
            volume_name,
            backup_info.backup_id.lower(),
        )
        logging.info(
            "Taking snapshot '%s' of disk '%s' (%s)",
            snapshot_name,
            volume_name,
            volume_id,
        )
        tags = [
            {"Key": "Name", "Value": snapshot_name},
        ]

        if self.tags is not None:
            for key, value in self.tags:
                tags.append({"Key": key, "Value": value})

        resp = self.ec2_client.create_snapshot(
            TagSpecifications=[
                {
                    "ResourceType": "snapshot",
                    "Tags": tags,
                }
            ],
            VolumeId=volume_id,
        )

        if resp["State"] == "error":
            raise CloudProviderError(
                "Snapshot '{}' failed: {}".format(snapshot_name, resp)
            )

        return snapshot_name, resp

    def take_snapshot_backup(self, backup_info, instance_identifier, volumes):
        """
        Take a snapshot backup for the named instance.

        Creates a snapshot for each named disk and saves the required metadata
        to backup_info.snapshots_info as an AwsSnapshotsInfo object.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_identifier: The instance ID or name of the VM instance to
            which the disks to be backed up are attached.
        :param dict[str,barman.cloud_providers.aws_s3.AwsVolumeMetadata] volumes:
            Metadata describing the volumes to be backed up.
        """
        instance_metadata = self._get_instance_metadata(instance_identifier)
        attachment_metadata = instance_metadata["BlockDeviceMappings"]
        snapshots = []
        for volume_identifier, volume_metadata in volumes.items():
            attached_volumes = [
                v
                for v in attachment_metadata
                if v["Ebs"]["VolumeId"] == volume_metadata.id
            ]
            if len(attached_volumes) == 0:
                raise SnapshotBackupException(
                    "Disk %s not attached to instance %s"
                    % (volume_identifier, instance_identifier)
                )
            assert len(attached_volumes) == 1

            snapshot_name, snapshot_resp = self._create_snapshot(
                backup_info, volume_identifier, volume_metadata.id
            )
            # Apply lock on snapshot if lock mode is specified
            if self.lock_mode:
                self._lock_snapshot(
                    snapshot_resp["SnapshotId"],
                    self.lock_mode,
                    self.lock_duration,
                    self.lock_cool_off_period,
                    self.lock_expiration_date,
                )

            snapshots.append(
                AwsSnapshotMetadata(
                    snapshot_id=snapshot_resp["SnapshotId"],
                    snapshot_name=snapshot_name,
                    snapshot_lock_mode=self.lock_mode,
                    device_name=attached_volumes[0]["DeviceName"],
                    mount_options=volume_metadata.mount_options,
                    mount_point=volume_metadata.mount_point,
                )
            )

        # Await completion of all snapshots using a boto3 waiter. This will call
        # `describe_snapshots` every 15 seconds until all snapshot IDs are in a
        # successful state. If the successful state is not reached after the maximum
        # number of attempts (default: 40) then a WaiterError is raised.
        snapshot_ids = [snapshot.identifier for snapshot in snapshots]
        logging.info("Waiting for completion of snapshots: %s", ", ".join(snapshot_ids))
        waiter = self.ec2_client.get_waiter("snapshot_completed")
        waiter.wait(
            SnapshotIds=snapshot_ids,
            WaiterConfig=self._get_waiter_config(),
        )
        backup_info.snapshots_info = AwsSnapshotsInfo(
            snapshots=snapshots,
            region=self.region,
            # All snapshots will have the same OwnerId so we get it from the last
            # snapshot response.
            account_id=snapshot_resp["OwnerId"],
        )

    def _lock_snapshot(
        self,
        snapshot_id,
        lock_mode,
        lock_duration,
        lock_cool_off_period,
        lock_expiration_date,
    ):
        lock_snapshot_default_args = {"LockMode": lock_mode, "SnapshotId": snapshot_id}

        if lock_duration:
            lock_snapshot_default_args["LockDuration"] = lock_duration

        if lock_cool_off_period:
            lock_snapshot_default_args["CoolOffPeriod"] = lock_cool_off_period

        if lock_expiration_date:
            lock_snapshot_default_args["ExpirationDate"] = lock_expiration_date

        resp = self.ec2_client.lock_snapshot(**lock_snapshot_default_args)

        _output = {}
        for key, value in resp.items():
            if key != "ResponseMetadata":
                if isinstance(value, datetime):
                    value = value.isoformat()
                _output[key] = value

        logging.info("Snapshot locked: \n%s" % json.dumps(_output, indent=4))

    def _delete_snapshot(self, snapshot_id):
        """
        Delete the specified snapshot.

        :param str snapshot_id: The ID of the snapshot to be deleted.
        """
        try:
            self.ec2_client.delete_snapshot(SnapshotId=snapshot_id)
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            # If the snapshot could not be found then deletion is considered successful
            # otherwise we raise a CloudProviderError
            if error_code == "InvalidSnapshot.NotFound":
                logging.warning("Snapshot {} could not be found".format(snapshot_id))
            elif error_code == "SnapshotLocked":
                raise SystemExit(
                    "Locked snapshot: %s.\n"
                    "Before deleting a snapshot, please ensure that it is not locked "
                    "or that the lock has expired." % snapshot_id,
                )
            else:
                raise CloudProviderError(
                    "Deletion of snapshot %s failed with error code %s: %s"
                    % (snapshot_id, error_code, exc.response["Error"])
                )
        logging.info("Snapshot %s deleted", snapshot_id)

    def delete_snapshot_backup(self, backup_info):
        """
        Delete all snapshots for the supplied backup.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        for snapshot in backup_info.snapshots_info.snapshots:
            logging.info(
                "Deleting snapshot '%s' for backup %s",
                snapshot.identifier,
                backup_info.backup_id,
            )
            self._delete_snapshot(snapshot.identifier)

    def get_attached_volumes(
        self, instance_identifier, disks=None, fail_on_missing=True
    ):
        """
        Returns metadata for the non-root volumes attached to this instance.

        Queries AWS for metadata relating to the volumes attached to the named instance
        and returns a dict of `VolumeMetadata` objects, keyed by volume identifier.

        The volume identifier will be either:
        - The value supplied in the disks parameter, which can be either the AWS
          assigned volume ID or a name which corresponds to a unique `Name` tag assigned
          to a volume.
        - The AWS assigned volume ID, if the disks parameter is unused.

        If the optional disks parameter is supplied then this method returns metadata
        for the disks in the supplied list only. If fail_on_missing is set to True then
        a SnapshotBackupException is raised if any of the supplied disks are not found
        to be attached to the instance.

        If the disks parameter is not supplied then this method returns a
        VolumeMetadata object for every non-root disk attached to this instance.

        :param str instance_identifier: Either an instance ID or the name of the VM
            instance to which the disks are attached.
        :param list[str]|None disks: A list containing either the volume IDs or names
            of disks backed up.
        :param bool fail_on_missing: Fail with a SnapshotBackupException if any
            specified disks are not attached to the instance.
        :rtype: dict[str, VolumeMetadata]
        :return: A dict where the key is the volume identifier and the value is the
            device path for that disk on the specified instance.
        """
        instance_metadata = self._get_instance_metadata(instance_identifier)
        requested_volumes = self._get_requested_volumes(instance_metadata, disks)

        attached_volumes = {}
        for requested_volume in requested_volumes:
            attached_volumes[requested_volume["identifier"]] = AwsVolumeMetadata(
                requested_volume["attachment_metadata"],
                virtualization_type=instance_metadata["VirtualizationType"],
                source_snapshot=requested_volume["source_snapshot"],
            )

        if disks is not None and fail_on_missing:
            unattached_volumes = []
            for disk_identifier in disks:
                if disk_identifier not in attached_volumes:
                    unattached_volumes.append(disk_identifier)
            if len(unattached_volumes) > 0:
                raise SnapshotBackupException(
                    "Disks not attached to instance {}: {}".format(
                        instance_identifier, ", ".join(unattached_volumes)
                    )
                )
        return attached_volumes

    def instance_exists(self, instance_identifier):
        """
        Determine whether the instance exists.

        :param str instance_identifier: A string identifying the VM instance to be
            checked. Can be either an instance ID or a name. If a name is provided
            it is expected to match the value of a `Name` tag for a single EC2
            instance.
        :rtype: bool
        :return: True if the named instance exists, False otherwise.
        """
        try:
            self._get_instance_metadata(instance_identifier)
        except SnapshotInstanceNotFoundException:
            return False
        return True


class AwsVolumeMetadata(VolumeMetadata):
    """
    Specialization of VolumeMetadata for AWS EBS volumes.

    This class uses the device name obtained from the AWS API together with the
    virtualization type of the VM to which it is attached in order to resolve the
    mount point and mount options for the volume.
    """

    def __init__(
        self, attachment_metadata=None, virtualization_type=None, source_snapshot=None
    ):
        """
        Creates an AwsVolumeMetadata instance using metadata obtained from the AWS API.

        :param dict attachment_metadata: An `Attachments` entry in the describe_volumes
            metadata for this volume.
        :param str virtualization_type: The type of virtualzation used by the VM to
            which this volume is attached - either "hvm" or "paravirtual".
        :param str source_snapshot: The snapshot ID of the source snapshot from which
            volume was created.
        """
        super(AwsVolumeMetadata, self).__init__()
        # The `id` property is used to store the volume ID so that we always have a
        # reference to the canonical ID of the volume. This is essential when creating
        # snapshots via the AWS API.
        self.id = None
        self._device_name = None
        self._virtualization_type = virtualization_type
        self._source_snapshot = source_snapshot
        if attachment_metadata:
            if "Device" in attachment_metadata:
                self._device_name = attachment_metadata["Device"]
            if "VolumeId" in attachment_metadata:
                self.id = attachment_metadata["VolumeId"]

    def resolve_mounted_volume(self, cmd):
        """
        Resolve the mount point and mount options using shell commands.

        Uses `findmnt` to find the mount point and options for this volume by building
        a list of candidate device names and checking each one. Candidate device names
        are:

        - The device name reported by the AWS API.
        - A subsitution of the device name depending on virtualization type, with the
          same trailing letter.

        This is based on information provided by AWS about device renaming in EC2:
            https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html

        :param UnixLocalCommand cmd: An object which can be used to run shell commands
            on a local (or remote, via the UnixRemoteCommand subclass) instance.
        """
        if self._device_name is None:
            raise SnapshotBackupException(
                "Cannot resolve mounted volume: device name unknown"
            )
        # Determine a list of candidate device names
        device_names = [self._device_name]
        device_prefix = "/dev/sd"
        if self._virtualization_type == "hvm":
            if self._device_name.startswith(device_prefix):
                device_names.append(
                    self._device_name.replace(device_prefix, "/dev/xvd")
                )
        elif self._virtualization_type == "paravirtual":
            if self._device_name.startswith(device_prefix):
                device_names.append(self._device_name.replace(device_prefix, "/dev/hd"))

        # Try to find the device name reported by the EC2 API
        for candidate_device in device_names:
            try:
                mount_point, mount_options = cmd.findmnt(candidate_device)
                if mount_point is not None:
                    self._mount_point = mount_point
                    self._mount_options = mount_options
                    return
            except CommandException as e:
                raise SnapshotBackupException(
                    "Error finding mount point for device path %s: %s"
                    % (self._device_name, e)
                )
        raise SnapshotBackupException(
            "Could not find device %s at any mount point" % self._device_name
        )

    @property
    def source_snapshot(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot ID
        """
        return self._source_snapshot


class AwsSnapshotMetadata(SnapshotMetadata):
    """
    Specialization of SnapshotMetadata for AWS EBS snapshots.

    Stores the device_name, snapshot_id, snapshot_name and snapshot_lock_mode in the provider-specific
    field.
    """

    _provider_fields = (
        "device_name",
        "snapshot_id",
        "snapshot_name",
        "snapshot_lock_mode",
    )

    def __init__(
        self,
        mount_options=None,
        mount_point=None,
        device_name=None,
        snapshot_id=None,
        snapshot_name=None,
        snapshot_lock_mode=None,
    ):
        """
        Constructor saves additional metadata for AWS snapshots.

        :param str mount_options: The mount options used for the source disk at the
            time of the backup.
        :param str mount_point: The mount point of the source disk at the time of
            the backup.
        :param str device_name: The device name used in the AWS API.
        :param str snapshot_id: The snapshot ID used in the AWS API.
        :param str snapshot_name: The snapshot name stored in the `Name` tag.
        :param str snapshot_lock_mode: The mode with which the snapshot has been locked
            (``governance`` or ``compliance``), if set.
        :param str project: The AWS project name.
        """
        super(AwsSnapshotMetadata, self).__init__(mount_options, mount_point)
        self.device_name = device_name
        self.snapshot_id = snapshot_id
        self.snapshot_name = snapshot_name
        self.snapshot_lock_mode = snapshot_lock_mode

    @property
    def identifier(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot ID.
        """
        return self.snapshot_id


class AwsSnapshotsInfo(SnapshotsInfo):
    """
    Represents the snapshots_info field for AWS EBS snapshots.
    """

    _provider_fields = (
        "account_id",
        "region",
    )
    _snapshot_metadata_cls = AwsSnapshotMetadata

    def __init__(self, snapshots=None, account_id=None, region=None):
        """
        Constructor saves the list of snapshots if it is provided.

        :param list[SnapshotMetadata] snapshots: A list of metadata objects for each
            snapshot.
        :param str account_id: The AWS account to which the snapshots belong, as
            reported by the `OwnerId` field in the snapshots metadata returned by AWS
            at snapshot creation time.
        :param str region: The AWS region in which snapshot resources are located.
        """
        super(AwsSnapshotsInfo, self).__init__(snapshots)
        self.provider = "aws"
        self.account_id = account_id
        self.region = region
