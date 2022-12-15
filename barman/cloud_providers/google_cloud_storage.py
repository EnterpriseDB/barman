# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2022
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
    DEVICE_PREFIX = "/dev/disk/by-id/google-"

    def __init__(self, project):
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

    def take_snapshot(self, backup_info, disk_zone, disk_name):
        snapshot_name = "%s-%s-%s" % (
            backup_info.server_name.lower(),
            backup_info.backup_id.lower(),
            disk_name,
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

        if resp.done():
            _logger.info("Snapshot '%s' completed", snapshot_name)
            return snapshot_name
        else:
            raise CloudProviderError(
                "Snapshot '%s' failed with error code %s: %s"
                % (snapshot_name, resp.error_code, resp.error_message)
            )

    def take_snapshot_backup(self, backup_info, instance_name, zone, disks):
        """Take a snapshot backup for the named instance."""
        instance_metadata = self.instances_client.get(
            instance=instance_name,
            zone=zone,
            project=self.project,
        )
        snapshots = {}
        for disk_name in disks:
            try:
                disk_metadata = self.disks_client.get(
                    disk=disk_name, zone=zone, project=self.project
                )
            except NotFound:
                raise SnapshotBackupException(
                    "Cannot find disk with name %s in zone %s for project %s"
                    % (disk_name, zone, self.project)
                )
            metadata = {
                "block_size": disk_metadata.physical_block_size_bytes,
                "size": disk_metadata.size_gb,
            }
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
            elif len(attached_disks) > 1:
                raise SnapshotBackupException(
                    "Multiple disks matching name %s found attached to instance %s"
                    % (instance_name, disk_name)
                )
            metadata["device"] = self.DEVICE_PREFIX + attached_disks[0].device_name
            snapshot_name = self.take_snapshot(backup_info, zone, disk_name)
            snapshots[snapshot_name] = metadata

        # Add snapshot metadata to BackupInfo
        backup_info.snapshots_info = {
            "gcp_project": self.project,
            "provider": "gcp",
            "snapshots": snapshots,
        }

    def delete_snapshot(self, snapshot_name):
        """Delete the specified snapshot."""
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
        if resp.done():
            _logger.info("Snapshot %s deleted", snapshot_name)
            return
        else:
            raise CloudProviderError(
                "Deletion of snapshot %s failed with error code %s: %s"
                % (snapshot_name, resp.error_code, resp.error_message)
            )

    def delete_snapshot_backup(self, backup_info):
        """Delete all snapshots for the supplied backup."""
        for snapshot_name in backup_info.snapshots_info["snapshots"]:
            _logger.info(
                "Deleting snapshot '%s' for backup %s",
                snapshot_name,
                backup_info.backup_id,
            )
            self.delete_snapshot(snapshot_name)

    def get_attached_devices(self, instance_name, zone):
        """
        Returns the non-boot devices attached to instance_name in zone.
        """
        instance_metadata = self.instances_client.get(
            instance=instance_name,
            zone=zone,
            project=self.project,
        )
        attached_devices = {}
        for attached_disk in instance_metadata.disks:
            disk_name = posixpath.split(urlparse(attached_disk.source).path)[-1]
            attached_devices[disk_name] = self.DEVICE_PREFIX + attached_disk.device_name

        return attached_devices

    def get_attached_snapshots(self, instance_name, zone):
        """
        Returns the snapshots which are sources for disks attached to instance.
        """
        attached_devices = self.get_attached_devices(instance_name, zone)
        attached_snapshots = {}
        for disk_name, device_name in attached_devices.items():
            disk_metadata = self.disks_client.get(
                disk=disk_name, zone=zone, project=self.project
            )
            attached_snapshot_name = posixpath.split(
                urlparse(disk_metadata.source_snapshot).path
            )[-1]
            if attached_snapshot_name != "":
                attached_snapshots[attached_snapshot_name] = device_name
        return attached_snapshots

    def instance_exists(self, instance_name, zone):
        """
        Returns true if the named instance exists in zone, false otherwise.
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
