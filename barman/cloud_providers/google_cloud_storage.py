# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2023
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
    CloudInterface,
    CloudProviderError,
    CloudSnapshotInterface,
    DecompressingStreamingIO,
    DEFAULT_DELIMITER,
    SnapshotMetadata,
    SnapshotsInfo,
)
from barman.exceptions import SnapshotBackupException

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from google.cloud import storage
    from google.api_core.exceptions import GoogleAPIError, Conflict, NotFound
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

    def __init__(self, url, jobs=1, tags=None, delete_batch_size=None):
        """
        Create a new Google cloud Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 1.
        :param List[tuple] tags: List of tags as k,v tuples to be added to all
          uploaded objects
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        """
        self.bucket_name, self.path = self._parse_url(url)
        super(GoogleCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
            delete_batch_size=delete_batch_size,
        )
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
        blob = self.container_client.blob(key)
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

    DEVICE_PREFIX = "/dev/disk/by-id/google-"

    def __init__(self, project):
        """
        Imports the google cloud compute library and creates the clients necessary for
        creating and managing snapshots.

        :param str project: The name of the GCP project to which all resources related
            to the snapshot backups belong.
        """
        if project is None:
            raise TypeError("project cannot be None")
        self.project = project

        # The import of this module is deferred until this constructor so that it
        # does not become a spurious dependency of the main cloud interface. Doing
        # so would break backup to GCS for anyone unable to install
        # google-cloud-compute (which includes anyone using python 2.7).
        compute = import_google_cloud_compute()

        self.client = compute.SnapshotsClient()
        self.disks_client = compute.DisksClient()
        self.instances_client = compute.InstancesClient()

    def _get_instance_metadata(self, instance_name, zone):
        """
        Retrieve the metadata for the named instance in the specified zone.

        :rtype: google.cloud.compute_v1.types.Instance
        :return: An object representing the compute instance.
        """
        try:
            return self.instances_client.get(
                instance=instance_name,
                zone=zone,
                project=self.project,
            )
        except NotFound:
            raise SnapshotBackupException(
                "Cannot find instance with name %s in zone %s for project %s"
                % (instance_name, zone, self.project)
            )

    def _get_disk_metadata(self, disk_name, zone):
        """
        Retrieve the metadata for the named disk in the specified zone.

        :rtype: google.cloud.compute_v1.types.Disk
        :return: An object representing the disk.
        """
        try:
            return self.disks_client.get(
                disk=disk_name, zone=zone, project=self.project
            )
        except NotFound:
            raise SnapshotBackupException(
                "Cannot find disk with name %s in zone %s for project %s"
                % (disk_name, zone, self.project)
            )

    def take_snapshot(self, backup_info, disk_zone, disk_name):
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

    def take_snapshot_backup(self, backup_info, instance_name, zone, disks):
        """
        Take a snapshot backup for the named instance.

        Creates a snapshot for each named disk and saves the required metadata
        to backup_info.snapshots_info as a GcpSnapshotsInfo object.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :param list[str] disks: A list containing the names of the source disks.
        """
        instance_metadata = self._get_instance_metadata(instance_name, zone)
        snapshots = []
        for disk_name in disks:
            disk_metadata = self._get_disk_metadata(disk_name, zone)
            # Check disk is attached and find device name
            attached_disks = [
                d
                for d in instance_metadata.disks
                if d.source == disk_metadata.self_link
            ]
            if len(attached_disks) == 0:
                raise SnapshotBackupException(
                    "Disk %s not attached to instance %s" % (disk_name, instance_name)
                )
            # We should always have exactly one attached disk matching the name
            assert len(attached_disks) == 1

            snapshot_name = self.take_snapshot(backup_info, zone, disk_name)
            snapshots.append(
                GcpSnapshotMetadata(
                    device_name=attached_disks[0].device_name,
                    snapshot_name=snapshot_name,
                    snapshot_project=self.project,
                )
            )

        # Add snapshot metadata to BackupInfo
        backup_info.snapshots_info = GcpSnapshotsInfo(
            project=self.project, snapshots=snapshots
        )

    def delete_snapshot(self, snapshot_name):
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
            self.delete_snapshot(snapshot.identifier)

    def get_attached_devices(self, instance_name, zone):
        """
        Returns the non-boot devices attached to instance_name in zone.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: dict[str,str]
        :return: A dict where the key is the disk name and the value is the device
            path for that disk on the specified instance.
        """
        instance_metadata = self._get_instance_metadata(instance_name, zone)
        attached_devices = {}
        for attached_disk in instance_metadata.disks:
            disk_name = posixpath.split(urlparse(attached_disk.source).path)[-1]
            if disk_name == "":
                raise SnapshotBackupException(
                    "Could not parse disk name for source %s attached to instance %s"
                    % (attached_disk.source, instance_name)
                )
            full_device_name = self.DEVICE_PREFIX + attached_disk.device_name
            if disk_name in attached_devices:
                raise SnapshotBackupException(
                    "Disk %s appears to be attached with name %s as devices %s and %s"
                    % (
                        attached_disk.source,
                        disk_name,
                        full_device_name,
                        attached_devices[disk_name],
                    )
                )
            attached_devices[disk_name] = full_device_name

        return attached_devices

    def get_attached_snapshots(self, instance_name, zone):
        """
        Returns the snapshots which are sources for disks attached to instance.

        Queries the instance metadata to determine which disks are attached and
        then queries the disk metadata for each disk to determine whether it was
        cloned from a snapshot. If it was cloned then the snapshot is added to the
        dict which is returned once all attached devices have been checked.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: dict[str,str]
        :return: A dict where the key is the snapshot name and the value is the
            device path for the source disk for that snapshot on the specified
            instance.
        """
        attached_devices = self.get_attached_devices(instance_name, zone)
        attached_snapshots = {}
        for disk_name, device_name in attached_devices.items():
            disk_metadata = self._get_disk_metadata(disk_name, zone)
            if disk_metadata.source_snapshot is not None:
                attached_snapshot_name = posixpath.split(
                    urlparse(disk_metadata.source_snapshot).path
                )[-1]
            else:
                attached_snapshot_name = ""
            if attached_snapshot_name != "":
                attached_snapshots[attached_snapshot_name] = device_name
        return attached_snapshots

    def instance_exists(self, instance_name, zone):
        """
        Determine whether the named instance exists in the specified zone.

        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param str zone: The zone in which the snapshot disks and instance reside.
        :rtype: bool
        :return: True if the named instance exists in zone, False otherwise.
        """
        try:
            self.instances_client.get(
                instance=instance_name,
                zone=zone,
                project=self.project,
            )
        except NotFound:
            return False
        return True


class GcpSnapshotMetadata(SnapshotMetadata):
    """
    Specialization of SnapshotMetadata for GCP persistent disk snapshots.

    Stores the device_name, snapshot_name and snapshot_project in the provider-specific
    field, uses the short snapshot name as the identifier, and forges the device path
    using the hardcoded (but documented in the API docs) prefix.
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
        :param str project: The GCP project name.
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

    @property
    def device(self):
        """
        The device path to the source disk on the compute instance at the time the
        backup was taken.

        :rtype: str
        :return: The full path to the source disk device.
        """
        return GcpCloudSnapshotInterface.DEVICE_PREFIX + self.device_name


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
