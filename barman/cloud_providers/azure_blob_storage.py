# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2021
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

import bz2
import gzip
import logging
import os
import shutil
from io import BytesIO, RawIOBase

from barman.cloud import CloudInterface

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from azure.storage.blob import BlobPrefix, BlobServiceClient
    from azure.core.exceptions import (
        HttpResponseError,
        ResourceNotFoundError,
        ServiceRequestError,
    )
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

    def __init__(self, url, jobs=2, **kwargs):
        """
        Create a new Azure Blob Storage interface given the supplied acccount url

        :param str url: Full URL of the cloud destination/source
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        """
        super(AzureCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
        )

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
            logging.warn("Using emulated storage URL: %s " % url)
            if "AZURE_STORAGE_CONNECTION_STRING" not in os.environ:
                raise ValueError(
                    "A connection string must be povided when using emulated storage"
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
        if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
            logging.info("Authenticating to Azure with connection string")
            client = BlobServiceClient.from_connection_string(
                conn_str=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
                container_name=self.bucket_name,
            )
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
            client = BlobServiceClient(
                account_url=self.account_url,
                credential=credential,
                container_name=self.bucket_name,
            )
        self.container_client = client.get_container_client(self.bucket_name)

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

        :return: True if the container exists, False otherwise
        :rtype: bool
        """
        return self.container_client.exists()

    def _create_bucket(self):
        """
        Create the container in cloud storage
        """
        # By default public access is disabled for newly created containers.
        # Unlike S3 there is no concept of regions for containers (this is at
        # the storage account level in Azure)
        self.container_client.create_container()

    def _walk_blob_tree(self, obj, ignore=None):
        """
        Walk a blob tree in a directory manner and return a list of directories
        and files.

        :param ItemPaged[BlobProperties] obj: Iterable response of BlobProperties
          obtained from ContainerClient.walk_blobs
        :param str|None ignore: An entry to be excluded from the returned list,
          typically the top level prefix
        :return: List of objects and directories in the tree
        :rtype: List[str]
        """
        if obj.name != ignore:
            yield obj.name
        if isinstance(obj, BlobPrefix):
            # We are a prefix and not a leaf so iterate children
            for child in obj:
                for v in self._walk_blob_tree(child):
                    yield v

    def list_bucket(self, prefix="", delimiter="/"):
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
        return self._walk_blob_tree(res, ignore=prefix)

    def download_file(self, key, dest_path, decompress=None):
        """
        Download a file from Azure Blob Storage

        :param str key: The key to download
        :param str dest_path: Where to put the destination file
        :param str|None decompress: Compression scheme to use for decompression
        """
        obj = self.container_client.download_blob(key)
        with open(dest_path, "wb") as dest_file:
            if not decompress:
                obj.download_to_stream(dest_file)
                return
            blob = StreamingBlobIO(obj)
            if decompress == "gzip":
                source_file = gzip.GzipFile(fileobj=blob, mode="rb")
            elif decompress == "bzip2":
                source_file = bz2.BZ2File(blob, "rb")
            with source_file:
                shutil.copyfileobj(source_file, dest_file)

    def remote_open(self, key):
        """
        Open a remote Azure Blob Storage object and return a readable stream

        :param str key: The key identifying the object to open
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """
        try:
            obj = self.container_client.download_blob(key)
            return StreamingBlobIO(obj)
        except ResourceNotFoundError:
            return None

    def upload_fileobj(self, fileobj, key):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        """
        self.container_client.upload_blob(
            name=key,
            data=fileobj,
            overwrite=True,
        )

    def create_multipart_upload(self, key):
        """No-op method because Azure has no concept of multipart uploads

        Instead of multipart upload, blob blocks are staged and then committed.
        However this does not require anything to be created up front.
        This method therefore does nothing.
        """
        pass

    def _upload_part(self, mpu, key, body, part_number):
        """
        Upload a single block of this block blob.

        :param mpu: The multipart upload handle (not used with Azure).
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part handle
        :rtype: dict[str, None|str]
        """
        # Block IDs must be the same length for all bocks in the blob
        # and no greater than 64 characters. Given there is a limit of
        # 50000 blocks per blob we zero-pad the part_number to five
        # places.
        block_id = str(part_number).zfill(5)
        blob_client = self.container_client.get_blob_client(key)
        blob_client.stage_block(block_id, body)
        return {"PartNumber": block_id}

    def _complete_multipart_upload(self, mpu, key, parts):
        """
        Finish a "multipart upload" by committing all blocks in the blob.

        :param mpu:  The multipart upload handle (not used)
        :param str key: The key to use in the cloud service
        :param parts: The list of block IDs for the blocks which compose this blob
        """
        blob_client = self.container_client.get_blob_client(key)
        block_list = [part["PartNumber"] for part in parts]
        blob_client.commit_block_list(block_list)

    def _abort_multipart_upload(self, mpu, key):
        """
        Abort the upload of a block blob

        The objective of this method is to clean up any dangling resources - in
        this case those resources are uncommitted blocks.

        :param mpu:  The multipart upload handle
        :param str key: The key to use in the cloud service
        """
        # Ideally we would clean up uncommitted blocks at this point
        # however there is no way of doing that.
        # Uncommitted blocks will be discarded after 7 days or when
        # the blob is committed (if they're not included in the commit).
        # We therefore create an empty blob (thereby discarding all uploaded
        # blocks for that blob) and then delete it.
        blob_client = self.container_client.get_blob_client(key)
        blob_client.commit_block_list([])
        blob_client.delete_blob()
