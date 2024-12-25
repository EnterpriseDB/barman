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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

import logging
import os
import posixpath

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
from barman.exceptions import CommandException, SnapshotBackupException

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from google.api_core.exceptions import Conflict, GoogleAPIError, NotFound
    from google.cloud import storage
except ImportError:
    raise SystemExit("Missing required python module: google-cloud-storage")

_logger = logging.getLogger(__name__)

BASE_URL = "https://console.cloud.google.com/storage/browser/"


class GoogleCloudInterface(CloudInterface):
    """
    This class implements CloudInterface for GCS with the scope of using JSON API

    storage client documentation:  https://googleapis.dev/python/storage/latest/client.html
    JSON API documentation: https://cloud.google.com/storage/docs/json_api/v1/objects
    """

    # This implementation uses JSON API . does not support real parallel upload.
    # <<Within the JSON API, there is an unrelated type of upload also called a "multipart upload".>>
    MAX_CHUNKS_PER_FILE = 1

    # Since there is only on chunk  min size is the same as max archive size
    MIN_CHUNK_SIZE = 1 << 40

    # https://cloud.google.com/storage/docs/json_api/v1/objects/insert
    # Google json api  permit a maximum of 5TB per file
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    MAX_DELETE_BATCH_SIZE = 100

    def __init__(
        self, url, jobs=1, tags=None, delete_batch_size=None, kms_key_name=None
    ):
        """
        Create a new Google cloud Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 1.
        :param List[tuple] tags: List of tags as k,v tuples to be added to all
          uploaded objects
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        :param str|None kms_key_name: the name of the KMS key which should be used for
          encrypting the uploaded data in GCS
        """
        self.bucket_name, self.path = self._parse_url(url)
        super(GoogleCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
            delete_batch_size=delete_batch_size,
        )
        self.kms_key_name = kms_key_name
        self.bucket_exists = None
        self._reinit_session()

    @staticmethod
    def _parse_url(url):
        """
        Parse url and return bucket name and path. Raise ValueError otherwise.
        """
        if not url.startswith(BASE_URL) and not url.startswith("gs://"):
            msg = "Google cloud storage URL {} is malformed. Expected format are '{}' or '{}'".format(
                url,
                os.path.join(BASE_URL, "bucket-name/some/path"),
                "gs://bucket-name/some/path",
            )
            raise ValueError(msg)
        gs_url = url.replace(BASE_URL, "gs://")
        parsed_url = urlparse(gs_url)
        if not parsed_url.netloc:
            raise ValueError(
                "Google cloud storage URL {} is malformed. Bucket name not found".format(
                    url
                )
            )
        return parsed_url.netloc, parsed_url.path.strip("/")

    def _reinit_session(self):
        """
        Create a new session
        Creates a client using "GOOGLE_APPLICATION_CREDENTIALS" env.
        An error will be raised if the variable is missing.
        """
        self.client = storage.Client()
        self.container_client = self.client.bucket(self.bucket_name)

    def test_connectivity(self):
        """
        Test gcs connectivity by trying to access a container
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to see if google cloud storage is reachable.
            self.bucket_exists = self._check_bucket_existence()
            return True
        except GoogleAPIError as exc:
            logging.error("Can't connect to cloud provider: %s", exc)
            return False

    def _check_bucket_existence(self):
        """
        Check google bucket

        :return: True if the container exists, False otherwise
        :rtype: bool
        """
        return self.container_client.exists()

    def _create_bucket(self):
        """
        Create the bucket in cloud storage
        It will try to create the bucket according to credential provided with 'GOOGLE_APPLICATION_CREDENTIALS' env. This imply the
        Bucket creation requires following gcsBucket access: 'storage.buckets.create'. Storage Admin role is suited for that.

        It is advised to have the bucket already created. Bucket creation can use a lot of parameters (region, project, dataclass, access control ...).
        Barman cloud does not provide a way to customise this creation and will use only bucket for creation .
        You can check detailed documentation here to learn more about default values
        https://googleapis.dev/python/storage/latest/client.html -> create_bucket
        """
        try:
            self.client.create_bucket(self.container_client)
        except Conflict as e:
            logging.warning("It seems there was a Conflict creating bucket.")
            logging.warning(e.message)
            logging.warning("The bucket already exist, so we continue.")

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix: Prefix used to filter blobs
        :param str delimiter: Delimiter, used with prefix to emulate hierarchy
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        logging.debug("list_bucket: {}, {}".format(prefix, delimiter))
        blobs = self.client.list_blobs(
            self.container_client, prefix=prefix, delimiter=delimiter
        )
        objects = list(map(lambda blob: blob.name, blobs))
        dirs = list(blobs.prefixes)
        logging.debug("objects {}".format(objects))
        logging.debug("dirs {}".format(dirs))
        return objects + dirs

    def download_file(self, key, dest_path, decompress):
        """
        Download a file from cloud storage

        :param str key: The key identifying the file to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        logging.debug("GCS.download_file")
        blob = storage.Blob(key, self.container_client)
        with open(dest_path, "wb") as dest_file:
            if decompress is None:
                self.client.download_blob_to_file(blob, dest_file)
                return
            with blob.open(mode="rb") as blob_reader:
                decompress_to_file(blob_reader, dest_file, decompress)

    def remote_open(self, key, decompressor=None):
        """
        Open a remote object in cloud storage and returns a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: google.cloud.storage.fileio.BlobReader | DecompressingStreamingIO | None A file-like object from which
          the stream can be read or None if the key does not exist
        """
        logging.debug("GCS.remote_open")
        blob = storage.Blob(key, self.container_client)
        if not blob.exists():
            logging.debug("Key: {} does not exist".format(key))
            return None
        blob_reader = blob.open("rb")
        if decompressor:
            return DecompressingStreamingIO(blob_reader, decompressor)
        return blob_reader

    def upload_fileobj(self, fileobj, key, override_tags=None):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of tags as k,v tuples to be added to the
          uploaded object
        """
        tags = override_tags or self.tags
        logging.debug("upload_fileobj to {}".format(key))
        extra_args = {}
        if self.kms_key_name is not None:
            extra_args["kms_key_name"] = self.kms_key_name
        blob = self.container_client.blob(key, **extra_args)
        if tags is not None:
            blob.metadata = dict(tags)
        logging.debug("blob initiated")
        try:
            blob.upload_from_file(fileobj)
        except GoogleAPIError as e:
            logging.error(type(e))
            logging.error(e)
            raise e

    def create_multipart_upload(self, key):
        """
        JSON API does not allow this kind of multipart.
        https://cloud.google.com/storage/docs/uploads-downloads#uploads
        Closest solution is Parallel composite uploads. It is implemented in gsutil.
        It basically behave as follow:
            * file to upload is split in chunks
            * each chunk is sent to a specific path
            * when all chunks ar uploaded, compose call will assemble them into one file
            * chunk files can then be deleted

        For now parallel upload is a simple upload.

        :param key: The key to use in the cloud service
        :return: The multipart upload metadata
        :rtype: dict[str, str]|None
        """
        return []

    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a file

        The part metadata will included in a list of metadata for all parts of
        the upload which is passed to the _complete_multipart_upload method.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part metadata
        :rtype: dict[str, None|str]
        """
        self.upload_fileobj(body, key)
        return {
            "PartNumber": part_number,
        }

    def _complete_multipart_upload(self, upload_metadata, key, parts_metadata):
        """
        Finish a certain multipart upload
        There is nothing to do here as we are not using multipart.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param List[dict] parts_metadata: The list of metadata for the parts
          composing the multipart upload. Each part is guaranteed to provide a
          PartNumber and may optionally contain additional metadata returned by
          the cloud provider such as ETags.
        """
        pass

    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        The implementation of this method should clean up any dangling resources
        left by the incomplete upload.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        """
        # Probably delete things here in case it has already been uploaded ?
        # Maybe catch some exceptions like file not found (equivalent)
        try:
            self.delete_objects(key)
        except GoogleAPIError as e:
            logging.error(e)
            raise e

    def _delete_objects_batch(self, paths):
        """
        Delete the objects at the specified paths.
        The maximum possible number of calls in a batch is 100.
        :param List[str] paths:
        """
        super(GoogleCloudInterface, self)._delete_objects_batch(paths)

        failures = {}

        with self.client.batch():
            for path in list(set(paths)):
                try:
                    blob = self.container_client.blob(path)
                    blob.delete()
                except GoogleAPIError as e:
                    failures[path] = [str(e.__class__), e.__str__()]

        if failures:
            logging.error(failures)
            raise CloudProviderError()

    def get_prefixes(self, prefix):
        """
        Return only the common prefixes under the supplied prefix.

        :param str prefix: The object key prefix under which the common prefixes
            will be found.
        :rtype: Iterator[str]
        :return: A list of unique prefixes immediately under the supplied prefix.
        """
        raise NotImplementedError()

    def delete_under_prefix(self, prefix):
        """
        Delete all objects under the specified prefix.

        :param str prefix: The object key prefix under which all objects should be
            deleted.
        """
        raise NotImplementedError()


def import_google_cloud_compute():
    """
    Import and return the google.cloud.compute module.

    This particular import happens in a function so that it can be deferred until
    needed while still allowing tests to easily mock the library.
    """
    try:
        from google.cloud import compute
    except ImportError:
        raise SystemExit("Missing required python module: google-cloud-compute")
    return compute


class GcpCloudSnapshotInterface(CloudSnapshotInterface):
    """
    Implementation of ClourSnapshotInterface for persistend disk snapshots as
    implemented in Google Cloud Platform as documented at:

        https://cloud.google.com/compute/docs/disks/create-snapshots
    """

    _required_config_for_backup = CloudSnapshotInterface._required_config_for_backup + (
        "gcp_zone",
    )

    _required_config_for_restore = (
        CloudSnapshotInterface._required_config_for_restore + ("gcp_zone",)
    )

    DEVICE_PREFIX = "/dev/disk/by-id/google-"

    def __init__(self, project, zone=None):
        """
        Imports the google cloud compute library and creates the clients necessary for
        creating and managing snapshots.

        :param str project: The name of the GCP project to which all resources related
            to the snapshot backups belong.
        :param str|None zone: The zone in which resources accessed through this
            snapshot interface reside.
        """
        if project is None:
            raise TypeError("project cannot be None")
        self.project = project
        self.zone = zone

        # The import of this module is deferred until this constructor so that it
        # does not become a spurious dependency of the main cloud interface. Doing
        # so would break backup to GCS for anyone unable to install
        # google-cloud-compute (which includes anyone using python 2.7).
        compute = import_google_cloud_compute()

        self.client = compute.SnapshotsClient()
        self.disks_client = compute.DisksClient()
        self.instances_client = compute.InstancesClient()

    def _get_instance_metadata(self, instance_name):
        """
        Retrieve the metadata for the named instance in the specified zone.

        :rtype: google.cloud.compute_v1.types.Instance
        :return: An object representing the compute instance.
        """
        try:
            return self.instances_client.get(
                instance=instance_name,
                zone=self.zone,
                project=self.project,
            )
        except NotFound:
            raise SnapshotBackupException(
                "Cannot find instance with name %s in zone %s for project %s"
                % (instance_name, self.zone, self.project)
            )

    def _get_disk_metadata(self, disk_name):
        """
        Retrieve the metadata for the named disk in the specified zone.

        :rtype: google.cloud.compute_v1.types.Disk
        :return: An object representing the disk.
        """
        try:
            return self.disks_client.get(
                disk=disk_name, zone=self.zone, project=self.project
            )
        except NotFound:
            raise SnapshotBackupException(
                "Cannot find disk with name %s in zone %s for project %s"
                % (disk_name, self.zone, self.project)
            )

    def _take_snapshot(self, backup_info, disk_zone, disk_name):
        """
        Take a snapshot of a persistent disk in GCP.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str disk_zone: The zone in which the disk resides.
        :param str disk_name: The name of the source disk for the snapshot.
        :rtype: str
        :return: The name used to reference the snapshot with GCP.
        """
        snapshot_name = "%s-%s" % (
            disk_name,
            backup_info.backup_id.lower(),
        )
        _logger.info("Taking snapshot '%s' of disk '%s'", snapshot_name, disk_name)
        resp = self.client.insert(
            {
                "project": self.project,
                "snapshot_resource": {
                    "name": snapshot_name,
                    "source_disk": "projects/%s/zones/%s/disks/%s"
                    % (
                        self.project,
                        disk_zone,
                        disk_name,
                    ),
                },
            }
        )

        _logger.info("Waiting for snapshot '%s' completion", snapshot_name)
        resp.result()

        if resp.error_code:
            raise CloudProviderError(
                "Snapshot '%s' failed with error code %s: %s"
                % (snapshot_name, resp.error_code, resp.error_message)
            )

        if resp.warnings:
            prefix = "Warnings encountered during snapshot %s: " % snapshot_name
            _logger.warning(
                prefix
                + ", ".join(
                    "%s:%s" % (warning.code, warning.message)
                    for warning in resp.warnings
                )
            )

        _logger.info("Snapshot '%s' completed", snapshot_name)
        return snapshot_name

    def take_snapshot_backup(self, backup_info, instance_name, volumes):
        """
        Take a snapshot backup for the named instance.

        Creates a snapshot for each named disk and saves the required metadata
        to backup_info.snapshots_info as a GcpSnapshotsInfo object.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param dict[str,barman.cloud.VolumeMetadata] volumes: Metadata for the volumes
            to be backed up.
        """
        instance_metadata = self._get_instance_metadata(instance_name)
        snapshots = []
        for disk_name, volume_metadata in volumes.items():
            snapshot_name = self._take_snapshot(backup_info, self.zone, disk_name)

            # Save useful metadata
            attachment_metadata = [
                d for d in instance_metadata.disks if d.source.endswith(disk_name)
            ][0]
            snapshots.append(
                GcpSnapshotMetadata(
                    snapshot_name=snapshot_name,
                    snapshot_project=self.project,
                    device_name=attachment_metadata.device_name,
                    mount_options=volume_metadata.mount_options,
                    mount_point=volume_metadata.mount_point,
                )
            )

        # Add snapshot metadata to BackupInfo
        backup_info.snapshots_info = GcpSnapshotsInfo(
            project=self.project, snapshots=snapshots
        )

    def _delete_snapshot(self, snapshot_name):
        """
        Delete the specified snapshot.

        :param str snapshot_name: The short name used to reference the snapshot within GCP.
        """
        try:
            resp = self.client.delete(
                {
                    "project": self.project,
                    "snapshot": snapshot_name,
                }
            )
        except NotFound:
            # If the snapshot cannot be found then deletion is considered successful
            return
        resp.result()

        if resp.error_code:
            raise CloudProviderError(
                "Deletion of snapshot %s failed with error code %s: %s"
                % (snapshot_name, resp.error_code, resp.error_message)
            )

        if resp.warnings:
            prefix = "Warnings encountered during deletion of %s: " % snapshot_name
            _logger.warning(
                prefix
                + ", ".join(
                    "%s:%s" % (warning.code, warning.message)
                    for warning in resp.warnings
                )
            )

        _logger.info("Snapshot %s deleted", snapshot_name)

    def delete_snapshot_backup(self, backup_info):
        """
        Delete all snapshots for the supplied backup.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        """
        for snapshot in backup_info.snapshots_info.snapshots:
            _logger.info(
                "Deleting snapshot '%s' for backup %s",
                snapshot.identifier,
                backup_info.backup_id,
            )
            self._delete_snapshot(snapshot.identifier)

    def get_attached_volumes(self, instance_name, disks=None, fail_on_missing=True):
        """
        Returns metadata for the volumes attached to this instance.

        Queries GCP for metadata relating to the volumes attached to the named instance
        and returns a dict of `VolumeMetadata` objects, keyed by disk name.

        If the optional disks parameter is supplied then this method returns metadata
        for the disks in the supplied list only. If fail_on_missing is set to True then
        a SnapshotBackupException is raised if any of the supplied disks are not found
        to be attached to the instance.

        If the disks parameter is not supplied then this method returns a
        VolumeMetadata for all disks attached to this instance.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param list[str]|None disks: A list containing the names of disks to be
            backed up.
        :param bool fail_on_missing: Fail with a SnapshotBackupException if any
            specified disks are not attached to the instance.
        :rtype: dict[str, VolumeMetadata]
        :return: A dict of VolumeMetadata objects representing each volume
            attached to the instance, keyed by volume identifier.
        """
        instance_metadata = self._get_instance_metadata(instance_name)
        attached_volumes = {}
        for attachment_metadata in instance_metadata.disks:
            disk_name = posixpath.split(urlparse(attachment_metadata.source).path)[-1]
            if disks and disk_name not in disks:
                continue
            if disk_name == "":
                raise SnapshotBackupException(
                    "Could not parse disk name for source %s attached to instance %s"
                    % (attachment_metadata.source, instance_name)
                )
            assert disk_name not in attached_volumes
            disk_metadata = self._get_disk_metadata(disk_name)
            attached_volumes[disk_name] = GcpVolumeMetadata(
                attachment_metadata,
                disk_metadata,
            )
        # Check all requested disks were found and complain if necessary
        if disks is not None and fail_on_missing:
            unattached_disks = []
            for disk_name in disks:
                if disk_name not in attached_volumes:
                    # Verify the disk definitely exists by fetching the metadata
                    self._get_disk_metadata(disk_name)
                    # Append to list of unattached disks
                    unattached_disks.append(disk_name)
            if len(unattached_disks) > 0:
                raise SnapshotBackupException(
                    "Disks not attached to instance %s: %s"
                    % (instance_name, ", ".join(unattached_disks))
                )
        return attached_volumes

    def instance_exists(self, instance_name):
        """
        Determine whether the named instance exists.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :rtype: bool
        :return: True if the named instance exists, False otherwise.
        """
        try:
            self.instances_client.get(
                instance=instance_name,
                zone=self.zone,
                project=self.project,
            )
        except NotFound:
            return False
        return True


class GcpVolumeMetadata(VolumeMetadata):
    """
    Specialization of VolumeMetadata for GCP persistent disks.

    This class uses the device name obtained from the GCP API to determine the full
    path to the device on the compute instance. This path is then resolved to the
    mount point using findmnt.
    """

    def __init__(self, attachment_metadata=None, disk_metadata=None):
        """
        Creates a GcpVolumeMetadata instance using metadata obtained from the GCP API.

        Uses attachment_metadata to obtain the device name and resolves this to the
        full device path on the instance using a documented prefix.

        Uses disk_metadata to obtain the source snapshot name, if such a snapshot
        exists.

        :param google.cloud.compute_v1.types.AttachedDisk attachment_metadata: An
            object representing the disk as attached to the instance.
        :param google.cloud.compute_v1.types.Disk disk_metadata: An object representing
            the disk.
        """
        super(GcpVolumeMetadata, self).__init__()
        self._snapshot_name = None
        self._device_path = None
        if (
            attachment_metadata is not None
            and attachment_metadata.device_name is not None
        ):
            self._device_path = (
                GcpCloudSnapshotInterface.DEVICE_PREFIX
                + attachment_metadata.device_name
            )
        if disk_metadata is not None:
            if disk_metadata.source_snapshot is not None:
                attached_snapshot_name = posixpath.split(
                    urlparse(disk_metadata.source_snapshot).path
                )[-1]
            else:
                attached_snapshot_name = ""
            if attached_snapshot_name != "":
                self._snapshot_name = attached_snapshot_name

    def resolve_mounted_volume(self, cmd):
        """
        Resolve the mount point and mount options using shell commands.

        Uses findmnt to retrieve the mount point and mount options for the device
        path at which this volume is mounted.
        """
        if self._device_path is None:
            raise SnapshotBackupException(
                "Cannot resolve mounted volume: Device path unknown"
            )
        try:
            mount_point, mount_options = cmd.findmnt(self._device_path)
        except CommandException as e:
            raise SnapshotBackupException(
                "Error finding mount point for device %s: %s" % (self._device_path, e)
            )
        if mount_point is None:
            raise SnapshotBackupException(
                "Could not find device %s at any mount point" % self._device_path
            )
        self._mount_point = mount_point
        self._mount_options = mount_options

    @property
    def source_snapshot(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot short name.
        """
        return self._snapshot_name


class GcpSnapshotMetadata(SnapshotMetadata):
    """
    Specialization of SnapshotMetadata for GCP persistent disk snapshots.

    Stores the device_name, snapshot_name and snapshot_project in the provider-specific
    field and uses the short snapshot name as the identifier.
    """

    _provider_fields = ("device_name", "snapshot_name", "snapshot_project")

    def __init__(
        self,
        mount_options=None,
        mount_point=None,
        device_name=None,
        snapshot_name=None,
        snapshot_project=None,
    ):
        """
        Constructor saves additional metadata for GCP snapshots.

        :param str mount_options: The mount options used for the source disk at the
            time of the backup.
        :param str mount_point: The mount point of the source disk at the time of
            the backup.
        :param str device_name: The short device name used in the GCP API.
        :param str snapshot_name: The short snapshot name used in the GCP API.
        :param str snapshot_project: The GCP project name.
        """
        super(GcpSnapshotMetadata, self).__init__(mount_options, mount_point)
        self.device_name = device_name
        self.snapshot_name = snapshot_name
        self.snapshot_project = snapshot_project

    @property
    def identifier(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot short name.
        """
        return self.snapshot_name


class GcpSnapshotsInfo(SnapshotsInfo):
    """
    Represents the snapshots_info field for GCP persistent disk snapshots.
    """

    _provider_fields = ("project",)
    _snapshot_metadata_cls = GcpSnapshotMetadata

    def __init__(self, snapshots=None, project=None):
        """
        Constructor saves the list of snapshots if it is provided.

        :param list[SnapshotMetadata] snapshots: A list of metadata objects for each
            snapshot.
        :param str project: The GCP project name.
        """
        super(GcpSnapshotsInfo, self).__init__(snapshots)
        self.provider = "gcp"
        self.project = project
