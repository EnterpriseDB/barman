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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import logging
import shutil
from io import RawIOBase

from barman.clients.cloud_compression import decompress_to_file
from barman.cloud import (
    CloudInterface,
    CloudProviderError,
    DecompressingStreamingIO,
    DEFAULT_DELIMITER,
)


try:
    # Python 3.x
    from urllib.parse import urlencode, urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse
    from urllib import urlencode

try:
    import boto3
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
        """
        super(S3CloudInterface, self).__init__(
            url=url,
            jobs=jobs,
            tags=tags,
        )
        self.profile_name = profile_name
        self.encryption = encryption
        self.endpoint_url = endpoint_url

        # Extract information from the destination URL
        parsed_url = urlparse(url)
        # If netloc is not present, the s3 url is badly formatted.
        if parsed_url.netloc == "" or parsed_url.scheme != "s3":
            raise ValueError("Invalid s3 URL address: %s" % url)
        self.bucket_name = parsed_url.netloc
        self.bucket_exists = None
        self.path = parsed_url.path.lstrip("/")

        # Build a session, so we can extract the correct resource
        self._reinit_session()

    def _reinit_session(self):
        """
        Create a new session
        """
        session = boto3.Session(profile_name=self.profile_name)
        self.s3 = session.resource("s3", endpoint_url=self.endpoint_url)

    @property
    def _extra_upload_args(self):
        """
        Return a dict containing ExtraArgs to be passed to certain boto3 calls

        Because some boto3 calls accept `ExtraArgs: {}` and others do not, we
        return a nexted dict which can be expanded with `**` in the boto3 call.
        """
        additional_args = {}
        if self.encryption:
            additional_args["ServerSideEncryption"] = self.encryption
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

        res = self.s3.meta.client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter
        )

        # List "folders"
        keys = res.get("CommonPrefixes")
        if keys is not None:
            for k in keys:
                yield k.get("Prefix")

        # List "files"
        objects = res.get("Contents")
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
            Fileobj=fileobj, Bucket=self.bucket_name, Key=key, ExtraArgs=extra_args
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

    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        # Explicitly check if we are being asked to delete nothing at all and if
        # so return without error.
        if len(paths) == 0:
            return

        # S3 bulk deletion is limited to batches of 1000 keys
        batch_size = 1000
        try:
            # If xrange exists then we are on python 2 so we need to use it
            range_fun = xrange
        except NameError:
            # Otherwise just use range
            range_fun = range
        errors = False
        for i in range_fun(0, len(paths), batch_size):
            resp = self.s3.meta.client.delete_objects(
                Bucket=self.bucket_name,
                Delete={
                    "Objects": [{"Key": path} for path in paths[i : i + batch_size]],
                    "Quiet": True,
                },
            )
            if "Errors" in resp:
                errors = True
                for error_dict in resp["Errors"]:
                    logging.error(
                        'Deletion of object %s failed with error code: "%s", message: "%s"'
                        % (error_dict["Key"], error_dict["Code"], error_dict["Message"])
                    )
        if errors:
            raise CloudProviderError(
                "Error from cloud provider while deleting objects - "
                "please check the Barman logs"
            )
