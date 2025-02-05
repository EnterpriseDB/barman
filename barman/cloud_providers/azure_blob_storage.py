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

import logging
import os
import re
from io import SEEK_END, BytesIO, RawIOBase

import requests

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
    from azure.core.exceptions import (
        HttpResponseError,
        ResourceNotFoundError,
        ServiceRequestError,
    )
    from azure.storage.blob import ContainerClient, PartialBatchErrorException
except ImportError:
    raise SystemExit("Missing required python module: azure-storage-blob")

# Domain for azure blob URIs
# See https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#resource-uri-syntax
AZURE_BLOB_STORAGE_DOMAIN = "blob.core.windows.net"


class StreamingBlobIO(RawIOBase):
    """
    Wrap an azure-storage-blob StorageStreamDownloader in the IOBase API.

    Inherits the IOBase defaults of seekable() -> False and writable() -> False.
    """

    def __init__(self, blob):
        self._chunks = blob.chunks()
        self._current_chunk = BytesIO()

    def readable(self):
        return True

    def read(self, n=1):
        """
        Read at most n bytes from the stream.

        Fetches new chunks from the StorageStreamDownloader until the requested
        number of bytes have been read.

        :param int n: Number of bytes to read from the stream
        :return: Up to n bytes from the stream
        :rtype: bytes
        """
        n = None if n < 0 else n
        blob_bytes = self._current_chunk.read(n)
        bytes_count = len(blob_bytes)
        try:
            while bytes_count < n:
                self._current_chunk = BytesIO(self._chunks.next())
                new_blob_bytes = self._current_chunk.read(n - bytes_count)
                bytes_count += len(new_blob_bytes)
                blob_bytes += new_blob_bytes
        except StopIteration:
            pass
        return blob_bytes


class AzureCloudInterface(CloudInterface):
    # Azure block blob limitations
    # https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
    MAX_CHUNKS_PER_FILE = 50000
    # Minimum block size allowed in Azure Blob Storage is 64KB
    MIN_CHUNK_SIZE = 64 << 10

    # Azure Blob Storage permit a maximum of 4.75TB per file
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    MAX_DELETE_BATCH_SIZE = 256

    # The size of each chunk in a single object upload when the size of the
    # object exceeds max_single_put_size. We default to 2MB in order to
    # allow the default max_concurrency of 8 to be achieved when uploading
    # uncompressed WAL segments of the default 16MB size.
    DEFAULT_MAX_BLOCK_SIZE = 2 << 20

    # The maximum amount of concurrent chunks allowed in a single object upload
    # where the size exceeds max_single_put_size. We default to 8 based on
    # experiments with in-region and inter-region transfers within Azure.
    DEFAULT_MAX_CONCURRENCY = 8

    # The largest file size which will be uploaded in a single PUT request. This
    # should be lower than the size of the compressed WAL segment in order to
    # force the Azure client to use concurrent chunk upload for archiving WAL files.
    DEFAULT_MAX_SINGLE_PUT_SIZE = 4 << 20

    # The maximum size of the requests connection pool used by the Azure client
    # to upload objects.
    REQUESTS_POOL_MAXSIZE = 32

    def __init__(
        self,
        url,
        jobs=2,
        encryption_scope=None,
        credential=None,
        tags=None,
        delete_batch_size=None,
        max_block_size=DEFAULT_MAX_BLOCK_SIZE,
        max_concurrency=DEFAULT_MAX_CONCURRENCY,
        max_single_put_size=DEFAULT_MAX_SINGLE_PUT_SIZE,
    ):
        """
        Create a new Azure Blob Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        :param int|None delete_batch_size: the maximum number of objects to be
          deleted in a single request
        """
        super(AzureCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
            delete_batch_size=delete_batch_size,
        )
        self.encryption_scope = encryption_scope
        self.credential = credential
        self.max_block_size = max_block_size
        self.max_concurrency = max_concurrency
        self.max_single_put_size = max_single_put_size

        parsed_url = urlparse(url)
        if parsed_url.netloc.endswith(AZURE_BLOB_STORAGE_DOMAIN):
            # We have an Azure Storage URI so we use the following form:
            # <http|https>://<account-name>.<service-name>.core.windows.net/<resource-path>
            # where <resource-path> is <container>/<blob>.
            # Note that although Azure supports an implicit root container, we require
            # that the container is always included.
            self.account_url = parsed_url.netloc
            try:
                self.bucket_name = parsed_url.path.split("/")[1]
            except IndexError:
                raise ValueError("azure blob storage URL %s is malformed" % url)
            path = parsed_url.path.split("/")[2:]
        else:
            # We are dealing with emulated storage so we use the following form:
            # http://<local-machine-address>:<port>/<account-name>/<resource-path>
            logging.info("Using emulated storage URL: %s " % url)
            if "AZURE_STORAGE_CONNECTION_STRING" not in os.environ:
                raise ValueError(
                    "A connection string must be provided when using emulated storage"
                )
            try:
                self.bucket_name = parsed_url.path.split("/")[2]
            except IndexError:
                raise ValueError("emulated storage URL %s is malformed" % url)
            path = parsed_url.path.split("/")[3:]

        self.path = "/".join(path)

        self.bucket_exists = None
        self._reinit_session()

    def _reinit_session(self):
        """
        Create a new session
        """
        if self.credential:
            # Any supplied credential takes precedence over the environment
            credential = self.credential
        elif "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
            logging.info("Authenticating to Azure with connection string")
            self.container_client = ContainerClient.from_connection_string(
                conn_str=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
                container_name=self.bucket_name,
            )
            return
        else:
            if "AZURE_STORAGE_SAS_TOKEN" in os.environ:
                logging.info("Authenticating to Azure with SAS token")
                credential = os.getenv("AZURE_STORAGE_SAS_TOKEN")
            elif "AZURE_STORAGE_KEY" in os.environ:
                logging.info("Authenticating to Azure with shared key")
                credential = os.getenv("AZURE_STORAGE_KEY")
            else:
                logging.info("Authenticating to Azure with default credentials")
                # azure-identity is not part of azure-storage-blob so only import
                # it if needed
                try:
                    from azure.identity import DefaultAzureCredential
                except ImportError:
                    raise SystemExit("Missing required python module: azure-identity")
                credential = DefaultAzureCredential()
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=self.REQUESTS_POOL_MAXSIZE)
        session.mount("https://", adapter)
        self.container_client = ContainerClient(
            account_url=self.account_url,
            container_name=self.bucket_name,
            credential=credential,
            max_single_put_size=self.max_single_put_size,
            max_block_size=self.max_block_size,
            session=session,
        )

    @property
    def _extra_upload_args(self):
        optional_args = {}
        if self.encryption_scope:
            optional_args["encryption_scope"] = self.encryption_scope
        return optional_args

    def test_connectivity(self):
        """
        Test Azure connectivity by trying to access a container
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to see if Azure blob service is reachable.
            self.bucket_exists = self._check_bucket_existence()
            return True
        except (HttpResponseError, ServiceRequestError) as exc:
            logging.error("Can't connect to cloud provider: %s", exc)
            return False

    def _check_bucket_existence(self):
        """
        Chck Azure Blob Storage for the target container

        Although there is an `exists` function it cannot be called by container-level
        shared access tokens. We therefore check for existence by calling list_blobs
        on the container.

        :return: True if the container exists, False otherwise
        :rtype: bool
        """
        try:
            self.container_client.list_blobs().next()
        except ResourceNotFoundError:
            return False
        except StopIteration:
            # The bucket is empty but it does exist
            pass
        return True

    def _create_bucket(self):
        """
        Create the container in cloud storage
        """
        # By default public access is disabled for newly created containers.
        # Unlike S3 there is no concept of regions for containers (this is at
        # the storage account level in Azure)
        self.container_client.create_container()

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        res = self.container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        )

        for item in res:
            yield item.name

    def download_file(self, key, dest_path, decompress=None):
        """
        Download a file from Azure Blob Storage

        :param str key: The key to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        obj = self.container_client.download_blob(key)
        with open(dest_path, "wb") as dest_file:
            if decompress is None:
                obj.download_to_stream(dest_file)
                return
            blob = StreamingBlobIO(obj)
            decompress_to_file(blob, dest_file, decompress)

    def remote_open(self, key, decompressor=None):
        """
        Open a remote Azure Blob Storage object and return a readable stream

        :param str key: The key identifying the object to open
        :param barman.clients.cloud_compression.ChunkedCompressor decompressor:
          A ChunkedCompressor object which will be used to decompress chunks of bytes
          as they are read from the stream
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """
        try:
            obj = self.container_client.download_blob(key)
            resp = StreamingBlobIO(obj)
            if decompressor:
                return DecompressingStreamingIO(resp, decompressor)
            else:
                return resp
        except ResourceNotFoundError:
            return None

    def upload_fileobj(
        self,
        fileobj,
        key,
        override_tags=None,
    ):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        :param List[tuple] override_tags: List of tags as k,v tuples to be added to the
          uploaded object
        """
        # Find length of the file so we can pass it to the Azure client
        fileobj.seek(0, SEEK_END)
        length = fileobj.tell()
        fileobj.seek(0)

        extra_args = self._extra_upload_args.copy()
        tags = override_tags or self.tags
        if tags is not None:
            extra_args["tags"] = dict(tags)
        self.container_client.upload_blob(
            name=key,
            data=fileobj,
            overwrite=True,
            length=length,
            max_concurrency=self.max_concurrency,
            **extra_args
        )

    def create_multipart_upload(self, key):
        """No-op method because Azure has no concept of multipart uploads

        Instead of multipart upload, blob blocks are staged and then committed.
        However this does not require anything to be created up front.
        This method therefore does nothing.
        """
        pass

    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a single block of this block blob.

        Uses the supplied part number to generate the block ID and returns it
        as the "PartNumber" in the part metadata.

        :param dict upload_metadata: Provider-specific metadata about the upload
          (not used in Azure)
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part metadata
        :rtype: dict[str, None|str]
        """
        # Block IDs must be the same length for all bocks in the blob
        # and no greater than 64 characters. Given there is a limit of
        # 50000 blocks per blob we zero-pad the part_number to five
        # places.
        block_id = str(part_number).zfill(5)
        blob_client = self.container_client.get_blob_client(key)
        blob_client.stage_block(block_id, body, **self._extra_upload_args)
        return {"PartNumber": block_id}

    def _complete_multipart_upload(self, upload_metadata, key, parts):
        """
        Finish a "multipart upload" by committing all blocks in the blob.

        :param dict upload_metadata: Provider-specific metadata about the upload
          (not used in Azure)
        :param str key: The key to use in the cloud service
        :param parts: The list of block IDs for the blocks which compose this blob
        """
        blob_client = self.container_client.get_blob_client(key)
        block_list = [part["PartNumber"] for part in parts]
        extra_args = self._extra_upload_args.copy()
        if self.tags is not None:
            extra_args["tags"] = dict(self.tags)
        blob_client.commit_block_list(block_list, **extra_args)

    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort the upload of a block blob

        The objective of this method is to clean up any dangling resources - in
        this case those resources are uncommitted blocks.

        :param dict upload_metadata: Provider-specific metadata about the upload
          (not used in Azure)
        :param str key: The key to use in the cloud service
        """
        # Ideally we would clean up uncommitted blocks at this point
        # however there is no way of doing that.
        # Uncommitted blocks will be discarded after 7 days or when
        # the blob is committed (if they're not included in the commit).
        # We therefore create an empty blob (thereby discarding all uploaded
        # blocks for that blob) and then delete it.
        blob_client = self.container_client.get_blob_client(key)
        blob_client.commit_block_list([], **self._extra_upload_args)
        blob_client.delete_blob()

    def _delete_objects_batch(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        super(AzureCloudInterface, self)._delete_objects_batch(paths)

        try:
            # If paths is empty because the files have already been deleted then
            # delete_blobs will return successfully so we just call it with whatever
            # we were given
            responses = self.container_client.delete_blobs(*paths)
        except PartialBatchErrorException as exc:
            # Although the docs imply any errors will be returned in the response
            # object, in practice a PartialBatchErrorException is raised which contains
            # the response objects in its `parts` attribute.
            # We therefore set responses to reference the response in the exception and
            # treat it the same way we would a regular response.
            logging.warning(
                "PartialBatchErrorException received from Azure: %s" % exc.message
            )
            responses = exc.parts

        # resp is an iterator of HttpResponse objects so we check the status codes
        # which should all be 202 if successful
        errors = False
        for resp in responses:
            if resp.status_code == 404:
                logging.warning(
                    "Deletion of object %s failed because it could not be found"
                    % resp.request.url
                )
            elif resp.status_code != 202:
                errors = True
                logging.error(
                    'Deletion of object %s failed with error code: "%s"'
                    % (resp.request.url, resp.status_code)
                )

        if errors:
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


def import_azure_mgmt_compute():
    """
    Import and return the azure.mgmt.compute module.

    This particular import happens in a function so that it can be deferred until
    needed while still allowing tests to easily mock the library.
    """
    try:
        import azure.mgmt.compute as compute
    except ImportError:
        raise SystemExit("Missing required python module: azure-mgmt-compute")
    return compute


def import_azure_identity():
    """
    Import and return the azure.identity module.

    This particular import happens in a function so that it can be deferred until
    needed while still allowing tests to easily mock the library.
    """
    try:
        import azure.identity as identity
    except ImportError:
        raise SystemExit("Missing required python module: azure-identity")
    return identity


class AzureCloudSnapshotInterface(CloudSnapshotInterface):
    """
    Implementation of CloudSnapshotInterface for managed disk snapshots in Azure, as
    described at:

        https://learn.microsoft.com/en-us/azure/virtual-machines/snapshot-copy-managed-disk
    """

    _required_config_for_backup = CloudSnapshotInterface._required_config_for_backup + (
        "azure_resource_group",
    )

    _required_config_for_restore = (
        CloudSnapshotInterface._required_config_for_restore + ("azure_resource_group",)
    )

    def __init__(self, subscription_id, resource_group=None, credential=None):
        """
        Imports the azure-mgmt-compute library and creates the clients necessary for
        creating and managing snapshots.

        :param str subscription_id: A Microsoft Azure subscription ID to which all
            resources accessed through this interface belong.
        :param str resource_group|None: The resource_group to which the resources
            accessed through this interface belong.
        :param azure.identity.AzureCliCredential|azure.identity.ManagedIdentityCredential|
            azure.identity.DefaultAzureCredential
            The Azure credential to be used when authenticating against the Azure API.
            If omitted then a DefaultAzureCredential will be created and used.
        """
        if subscription_id is None:
            raise TypeError("subscription_id cannot be None")
        self.subscription_id = subscription_id

        self.resource_group = resource_group

        if credential is None:
            identity = import_azure_identity()
            credential = identity.DefaultAzureCredential

        self.credential = credential()

        # Import of azure-mgmt-compute is deferred until this point so that it does not
        # become a hard dependency of this module.
        compute = import_azure_mgmt_compute()

        self.client = compute.ComputeManagementClient(
            self.credential, self.subscription_id
        )

    def _get_instance_metadata(self, instance_name):
        """
        Retrieve the metadata for the named instance.

        :rtype: azure.mgmt.compute.v2022_11_01.models.VirtualMachine
        :return: An object representing the named compute instance.
        """
        try:
            return self.client.virtual_machines.get(self.resource_group, instance_name)
        except ResourceNotFoundError:
            raise SnapshotBackupException(
                "Cannot find instance with name %s in resource group %s "
                "in subscription %s"
                % (instance_name, self.resource_group, self.subscription_id)
            )

    def _get_disk_metadata(self, disk_name):
        """
        Retrieve the metadata for the named disk in the specified zone.

        :rtype: azure.mgmt.compute.v2022_11_01.models.Disk
        :return: An object representing the disk.
        """
        try:
            return self.client.disks.get(self.resource_group, disk_name)
        except ResourceNotFoundError:
            raise SnapshotBackupException(
                "Cannot find disk with name %s in resource group %s "
                "in subscription %s"
                % (disk_name, self.resource_group, self.subscription_id)
            )

    def _take_snapshot(self, backup_info, resource_group, location, disk_name, disk_id):
        """
        Take a snapshot of a managed disk in Azure.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str resource_group: The resource_group to which the snapshot disks and
            instance belong.
        :param str location: The location of the source disk for the snapshot.
        :param str disk_name: The name of the source disk for the snapshot.
        :param str disk_id: The Azure identifier for the source disk.
        :rtype: str
        :return: The name used to reference the snapshot with Azure.
        """
        snapshot_name = "%s-%s" % (disk_name, backup_info.backup_id.lower())
        logging.info("Taking snapshot '%s' of disk '%s'", snapshot_name, disk_name)
        resp = self.client.snapshots.begin_create_or_update(
            resource_group,
            snapshot_name,
            {
                "location": location,
                "incremental": True,
                "creation_data": {"create_option": "Copy", "source_uri": disk_id},
            },
        )

        logging.info("Waiting for snapshot '%s' completion", snapshot_name)
        resp.wait()

        if (
            resp.status().lower() != "succeeded"
            or resp.result().provisioning_state.lower() != "succeeded"
        ):
            raise CloudProviderError(
                "Snapshot '%s' failed with error code %s: %s"
                % (snapshot_name, resp.status(), resp.result())
            )

        logging.info("Snapshot '%s' completed", snapshot_name)
        return snapshot_name

    def take_snapshot_backup(self, backup_info, instance_name, volumes):
        """
        Take a snapshot backup for the named instance.

        Creates a snapshot for each named disk and saves the required metadata
        to backup_info.snapshots_info as an AzureSnapshotsInfo object.

        :param barman.infofile.LocalBackupInfo backup_info: Backup information.
        :param str instance_name: The name of the VM instance to which the disks
            to be backed up are attached.
        :param dict[str,barman.cloud.VolumeMetadata] volumes: Metadata describing
            the volumes to be backed up.
        """
        instance_metadata = self._get_instance_metadata(instance_name)
        snapshots = []
        for disk_name, volume_metadata in volumes.items():
            attached_disks = [
                d
                for d in instance_metadata.storage_profile.data_disks
                if d.name == disk_name
            ]
            if len(attached_disks) == 0:
                raise SnapshotBackupException(
                    "Disk %s not attached to instance %s" % (disk_name, instance_name)
                )
            # We should always have exactly one attached disk matching the name
            assert len(attached_disks) == 1

            snapshot_name = self._take_snapshot(
                backup_info,
                self.resource_group,
                volume_metadata.location,
                disk_name,
                attached_disks[0].managed_disk.id,
            )
            snapshots.append(
                AzureSnapshotMetadata(
                    lun=attached_disks[0].lun,
                    snapshot_name=snapshot_name,
                    location=volume_metadata.location,
                    mount_point=volume_metadata.mount_point,
                    mount_options=volume_metadata.mount_options,
                )
            )

        backup_info.snapshots_info = AzureSnapshotsInfo(
            snapshots=snapshots,
            subscription_id=self.subscription_id,
            resource_group=self.resource_group,
        )

    def _delete_snapshot(self, snapshot_name, resource_group):
        """
        Delete the specified snapshot.

        :param str snapshot_name: The short name used to reference the snapshot within
            Azure.
        :param str resource_group: The resource_group to which the snapshot belongs.
        """
        # The call to begin_delete will raise a ResourceNotFoundError if the resource
        # group cannot be found. This is deliberately not caught here because it is
        # an error condition which we cannot do anything about.
        # If the snapshot itself cannot be found then the response status will be
        # `succeeded`, exactly as if it did exist and was successfully deleted.
        resp = self.client.snapshots.begin_delete(
            resource_group,
            snapshot_name,
        )

        resp.wait()
        if resp.status().lower() != "succeeded":
            raise CloudProviderError(
                "Deletion of snapshot %s failed with error code %s: %s"
                % (snapshot_name, resp.status(), resp.result())
            )

        logging.info("Snapshot %s deleted", snapshot_name)

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
            self._delete_snapshot(
                snapshot.identifier, backup_info.snapshots_info.resource_group
            )

    def get_attached_volumes(self, instance_name, disks=None, fail_on_missing=True):
        """
        Returns metadata for the volumes attached to this instance.

        Queries Azure for metadata relating to the volumes attached to the named
        instance and returns a dict of `VolumeMetadata` objects, keyed by disk name.

        If the optional disks parameter is supplied then this method returns metadata
        for the disks in the supplied list only. If fail_on_missing is set to True then
        a SnapshotBackupException is raised if any of the supplied disks are not found
        to be attached to the instance.

        If the disks parameter is not supplied then this method returns a
        VolumeMetadata object for every disk attached to this instance.

        :param str instance_name: The name of the VM instance to which the disks
            are attached.
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
        for attachment_metadata in instance_metadata.storage_profile.data_disks:
            disk_name = attachment_metadata.name
            if disks and disk_name not in disks:
                continue
            assert disk_name not in attached_volumes
            disk_metadata = self._get_disk_metadata(disk_name)
            attached_volumes[disk_name] = AzureVolumeMetadata(
                attachment_metadata, disk_metadata
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
            self.client.virtual_machines.get(self.resource_group, instance_name)
        except ResourceNotFoundError:
            return False
        return True


class AzureVolumeMetadata(VolumeMetadata):
    """
    Specialization of VolumeMetadata for Azure managed disks.

    This class uses the LUN obtained from the Azure API in order to resolve the mount
    point and options via using a documented symlink.
    """

    def __init__(self, attachment_metadata=None, disk_metadata=None):
        """
        Creates an AzureVolumeMetadata instance using metadata obtained from the Azure
        API.

        Uses attachment_metadata to obtain the LUN of the attached volume and
        disk_metadata to obtain the location of the disk.

        :param azure.mgmt.compute.v2022_11_01.models.DataDisk|None attachment_metadata:
            Metadata for the attached volume.
        :param azure.mgmt.compute.v2022_11_01.models.Disk|None disk_metadata:
            Metadata for the managed disk.
        """
        super(AzureVolumeMetadata, self).__init__()
        self.location = None
        self._lun = None
        self._snapshot_name = None
        if attachment_metadata is not None:
            self._lun = attachment_metadata.lun
        if disk_metadata is not None:
            # Record the location because this is needed when creating snapshots
            # (even though snapshots can only be created in the same location as the
            # source disk, Azure requires us to specify the location anyway).
            self.location = disk_metadata.location
            # Figure out whether this disk was cloned from a snapshot.
            if (
                disk_metadata.creation_data.create_option == "Copy"
                and "providers/Microsoft.Compute/snapshots"
                in disk_metadata.creation_data.source_resource_id
            ):
                # Extract the snapshot name from the source_resource_id in the disk
                # metadata. We do not care about the source subscription or resource
                # group - these may vary depending on whether the user has copied the
                # snapshot between resource groups or subscriptions. We only care about
                # the name because this is the part of the resource ID which Barman
                # associates with backups.
                resource_regex = (
                    r"/subscriptions/(?!/).*/resourceGroups/(?!/).*"
                    "/providers/Microsoft.Compute"
                    r"/snapshots/(?P<snapshot_name>.*)"
                )
                match = re.search(
                    resource_regex, disk_metadata.creation_data.source_resource_id
                )
                if match is None or match.group("snapshot_name") == "":
                    raise SnapshotBackupException(
                        "Could not determine source snapshot for disk %s with source resource ID %s"
                        % (
                            disk_metadata.name,
                            disk_metadata.creation_data.source_resource_id,
                        )
                    )
                self._snapshot_name = match.group("snapshot_name")

    def resolve_mounted_volume(self, cmd):
        """
        Resolve the mount point and mount options using shell commands.

        Uses findmnt to retrieve the mount point and mount options for the device
        path at which this volume is mounted.

        :param UnixLocalCommand cmd: An object which can be used to run shell commands
            on a local (or remote, via the UnixRemoteCommand subclass) instance.
        """
        if self._lun is None:
            raise SnapshotBackupException("Cannot resolve mounted volume: LUN unknown")
        try:
            # This symlink path is created by the Azure linux agent on boot. It is a
            # direct symlink to the actual device path of the attached volume. This
            # symlink will be consistent across reboots of the VM but the device path
            # will not. We therefore call findmnt directly on this symlink.
            # See the following documentation for more context:
            #   - https://learn.microsoft.com/en-us/troubleshoot/azure/virtual-machines/troubleshoot-device-names-problems#identify-disk-luns
            lun_symlink = "/dev/disk/azure/scsi1/lun{}".format(self._lun)
            mount_point, mount_options = cmd.findmnt(lun_symlink)
        except CommandException as e:
            raise SnapshotBackupException(
                "Error finding mount point for volume with lun %s: %s" % (self._lun, e)
            )
        if mount_point is None:
            raise SnapshotBackupException(
                "Could not find volume with lun %s at any mount point" % self._lun
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


class AzureSnapshotMetadata(SnapshotMetadata):
    """
    Specialization of SnapshotMetadata for Azure managed disk snapshots.

    Stores the location, lun and snapshot_name in the provider-specific field.
    """

    _provider_fields = ("location", "lun", "snapshot_name")

    def __init__(
        self,
        mount_options=None,
        mount_point=None,
        lun=None,
        snapshot_name=None,
        location=None,
    ):
        """
        Constructor saves additional metadata for Azure snapshots.

        :param str mount_options: The mount options used for the source disk at the
            time of the backup.
        :param str mount_point: The mount point of the source disk at the time of
            the backup.
        :param int lun: The lun identifying the disk from which the snapshot was taken
            on the instance it was attached to at the time of the backup.
        :param str snapshot_name: The snapshot name used in the Azure API.
        :param str location: The location of the disk from which the snapshot was taken
            at the time of the backup.
        """
        super(AzureSnapshotMetadata, self).__init__(mount_options, mount_point)
        self.lun = lun
        self.snapshot_name = snapshot_name
        self.location = location

    @property
    def identifier(self):
        """
        An identifier which can reference the snapshot via the cloud provider.

        :rtype: str
        :return: The snapshot short name.
        """
        return self.snapshot_name


class AzureSnapshotsInfo(SnapshotsInfo):
    """
    Represents the snapshots_info field for Azure managed disk snapshots.
    """

    _provider_fields = ("subscription_id", "resource_group")
    _snapshot_metadata_cls = AzureSnapshotMetadata

    def __init__(self, snapshots=None, subscription_id=None, resource_group=None):
        """
        Constructor saves the list of snapshots if it is provided.

        :param list[SnapshotMetadata] snapshots: A list of metadata objects for each
            snapshot.
        """
        super(AzureSnapshotsInfo, self).__init__(snapshots)
        self.provider = "azure"
        self.subscription_id = subscription_id
        self.resource_group = resource_group
