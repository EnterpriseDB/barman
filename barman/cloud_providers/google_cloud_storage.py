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

from barman.clients.cloud_compression import decompress_to_file
from barman.cloud import CloudInterface, DecompressingStreamingIO, DEFAULT_DELIMITER

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from google.cloud import storage
    from google.api_core.exceptions import GoogleAPIError, Conflict
except ImportError:
    raise SystemExit("Missing required python module: google-cloud-storage")


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

    # Since there is only on chunk  min size is teh same as max archive size
    MIN_CHUNK_SIZE = 1 << 40

    # https://cloud.google.com/storage/docs/json_api/v1/objects/insert
    # Google json api  permit a maximum of 5TB per file
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    def __init__(self, url, jobs=1, tags=None):
        """
        Create a new Google cloud Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 1.
        :param List[tuple] tags: List of tags as k,v tuples to be added to all
          uploaded objects
        """
        self.bucket_name, self.path = self._parse_url(url)
        super(GoogleCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
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

    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths
        There is no multiple delete in JSON API, so we loop over each object and delete them all.
        :param List[str] paths:
        """
        failures = {}
        for path in list(set(paths)):
            try:
                blob = self.container_client.blob(path)
                blob.delete()
            except GoogleAPIError as e:
                failures[path] = [str(e.__class__), e.__str__()]

        if failures:
            logging.error(failures)
            raise RuntimeError("Could not delete all keys")
