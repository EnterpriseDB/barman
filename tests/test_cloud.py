# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
#
# Client Utilities for Barman, Backup and Recovery Manager for PostgreSQL
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

import bz2
import datetime
import gzip
import os
import shutil
import sys
from argparse import Namespace
from io import BytesIO
from tarfile import TarFile, TarInfo
from tarfile import open as open_tar
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from azure.identity import AzureCliCredential, ManagedIdentityCredential
from azure.storage.blob import PartialBatchErrorException

import mock
from mock.mock import MagicMock
import pytest
import snappy

from barman.exceptions import BackupPreconditionException
from barman.infofile import BackupInfo

if sys.version_info.major > 2:
    from unittest.mock import patch as unittest_patch
from unittest import TestCase
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, EndpointConnectionError

from barman.annotations import KeepManager
from barman.cloud import (
    CloudBackupCatalog,
    CloudBackupSnapshot,
    CloudBackupUploader,
    CloudBackupUploaderBarman,
    CloudProviderError,
    CloudTarUploader,
    CloudUploadingError,
    FileUploadStatistics,
    DEFAULT_DELIMITER,
)
from barman.cloud_providers import (
    CloudProviderOptionUnsupported,
    CloudProviderUnsupported,
    get_cloud_interface,
)
from barman.cloud_providers.aws_s3 import S3CloudInterface
from barman.cloud_providers.azure_blob_storage import AzureCloudInterface
from barman.cloud_providers.google_cloud_storage import GoogleCloudInterface

from google.api_core.exceptions import GoogleAPIError, Conflict

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


def _tar_helper(content, content_filename):
    """Helper to create an in-memory tar file with a single file."""
    tar_fileobj = BytesIO()
    tf = TarFile.open(mode="w|", fileobj=tar_fileobj)
    ti = TarInfo(name=content_filename)
    content_as_bytes = content.encode("utf-8")
    ti.size = len(content_as_bytes)
    tf.addfile(ti, BytesIO(content_as_bytes))
    tf.close()
    tar_fileobj.seek(0)
    return tar_fileobj


def _compression_helper(src, compression):
    """
    Helper to compress a file-like object.
    Similar to barman.clients.cloud_compression.compress however we tolerate
    duplication here so as to avoid including code-under-test in the test.
    """
    if compression == "snappy":
        dest = BytesIO()
        snappy.stream_compress(src, dest)
    elif compression == "gzip":
        dest = BytesIO()
        with gzip.GzipFile(fileobj=dest, mode="wb") as gz:
            shutil.copyfileobj(src, gz)
    elif compression == "bzip2" or compression == "bz2":
        dest = BytesIO(bz2.compress(src.read()))
    elif compression is None:
        dest = BytesIO()
        dest.write(src.read())
    dest.seek(0)
    return dest


class TestCloudInterface(object):
    """
    Tests of the asynchronous upload infrastructure in CloudInterface.
    S3CloudInterface is used as we cannot instantiate a CloudInterface directly
    however we do not verify any backend specific functionality of S3CloudInterface,
    only the asynchronous infrastructure is tested.
    """

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_minimal(self, boto_mock):
        """
        Minimal build of the CloudInterface class
        """
        cloud_interface = S3CloudInterface(
            url="s3://bucket/path/to/dir", encryption=None
        )

        # Asynchronous uploading infrastructure is not initialized when
        # a new instance is created
        assert cloud_interface.queue is None
        assert cloud_interface.result_queue is None
        assert cloud_interface.errors_queue is None
        assert len(cloud_interface.parts_db) == 0
        assert len(cloud_interface.worker_processes) == 0

    @mock.patch("barman.cloud.multiprocessing")
    def test_ensure_async(self, mp):
        jobs_count = 30
        interface = S3CloudInterface(
            url="s3://bucket/path/to/dir", encryption=None, jobs=jobs_count
        )

        # Test that the asynchronous uploading infrastructure is getting
        # created
        interface._ensure_async()
        assert interface.queue is not None
        assert interface.result_queue is not None
        assert interface.errors_queue is not None
        assert len(interface.worker_processes) == jobs_count
        assert mp.JoinableQueue.call_count == 1
        assert mp.Queue.call_count == 3
        assert mp.Process.call_count == jobs_count
        mp.reset_mock()

        # Now that the infrastructure is ready, a new _ensure_async must
        # be useless
        interface._ensure_async()
        assert not mp.JoinableQueue.called
        assert not mp.Queue.called
        assert not mp.Process.called

    def test_retrieve_results(self):
        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.queue = Queue()
        interface.done_queue = Queue()
        interface.result_queue = Queue()
        interface.errors_queue = Queue()

        # With an empty queue, the parts DB is empty
        interface._retrieve_results()
        assert len(interface.parts_db) == 0

        # Preset the upload statistics, to avoid a random start_date
        for name in ["test/file", "test/another_file"]:
            interface.upload_stats[name] = FileUploadStatistics(
                status="uploading",
                start_time=datetime.datetime(2016, 3, 30, 17, 1, 0),
            )

        # Fill the result queue with mock results, and assert that after
        # the refresh the result queue is empty and the parts_db full with
        # ordered results
        interface.result_queue.put(
            {
                "key": "test/file",
                "part_number": 2,
                "end_time": datetime.datetime(2016, 3, 30, 17, 2, 20),
                "part": {"ETag": "becb2f30c11b6a2b5c069f3c8a5b798c", "PartNumber": "2"},
            }
        )
        interface.result_queue.put(
            {
                "key": "test/file",
                "part_number": 1,
                "end_time": datetime.datetime(2016, 3, 30, 17, 1, 20),
                "part": {"ETag": "27960aa8b7b851eb0277f0f3f5d15d68", "PartNumber": "1"},
            }
        )
        interface.result_queue.put(
            {
                "key": "test/file",
                "part_number": 3,
                "end_time": datetime.datetime(2016, 3, 30, 17, 3, 20),
                "part": {"ETag": "724a0685c99b457d4ddd93814c2d3e2b", "PartNumber": "3"},
            }
        )
        interface.result_queue.put(
            {
                "key": "test/another_file",
                "part_number": 1,
                "end_time": datetime.datetime(2016, 3, 30, 17, 5, 20),
                "part": {"ETag": "89d4f0341d9091aa21ddf67d3b32c34a", "PartNumber": "1"},
            }
        )
        interface._retrieve_results()
        assert interface.result_queue.empty()
        assert interface.parts_db == {
            "test/file": [
                {"ETag": "27960aa8b7b851eb0277f0f3f5d15d68", "PartNumber": "1"},
                {"ETag": "becb2f30c11b6a2b5c069f3c8a5b798c", "PartNumber": "2"},
                {"ETag": "724a0685c99b457d4ddd93814c2d3e2b", "PartNumber": "3"},
            ],
            "test/another_file": [
                {"ETag": "89d4f0341d9091aa21ddf67d3b32c34a", "PartNumber": "1"}
            ],
        }
        assert interface.upload_stats == {
            "test/another_file": {
                "start_time": datetime.datetime(2016, 3, 30, 17, 1, 0),
                "status": "uploading",
                "parts": {
                    1: {
                        "end_time": datetime.datetime(2016, 3, 30, 17, 5, 20),
                        "part_number": 1,
                    },
                },
            },
            "test/file": {
                "start_time": datetime.datetime(2016, 3, 30, 17, 1, 0),
                "status": "uploading",
                "parts": {
                    1: {
                        "end_time": datetime.datetime(2016, 3, 30, 17, 1, 20),
                        "part_number": 1,
                    },
                    2: {
                        "end_time": datetime.datetime(2016, 3, 30, 17, 2, 20),
                        "part_number": 2,
                    },
                    3: {
                        "end_time": datetime.datetime(2016, 3, 30, 17, 3, 20),
                        "part_number": 3,
                    },
                },
            },
        }

    @mock.patch("barman.cloud.CloudInterface._worker_process_execute_job")
    def test_worker_process_main(self, worker_process_execute_job_mock):
        job_collection = [
            {"job_id": 1, "job_type": "upload_part"},
            {"job_id": 2, "job_type": "upload_part"},
            {"job_id": 3, "job_type": "upload_part"},
            None,
        ]

        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.queue = mock.MagicMock()
        interface.errors_queue = Queue()
        interface.queue.get.side_effect = job_collection
        interface._worker_process_main(0)

        # Jobs are been grabbed from queue, and the queue itself has been
        # notified of tasks being done
        assert interface.queue.get.call_count == 4
        # worker_process_execute_job is executed only 3 times, because it's
        # not called for the process stop marker
        assert worker_process_execute_job_mock.call_count == 3
        assert interface.queue.task_done.call_count == 4
        assert interface.errors_queue.empty()

        # If during an execution a job an exception is raised, the worker
        # process must put the error in the appropriate queue.
        def execute_mock(job, process_number):
            if job["job_id"] == 2:
                raise Boto3Error("Something is gone wrong")

        interface.queue.reset_mock()
        worker_process_execute_job_mock.reset_mock()
        worker_process_execute_job_mock.side_effect = execute_mock
        interface.queue.get.side_effect = job_collection
        interface._worker_process_main(0)
        assert interface.queue.get.call_count == 4
        # worker_process_execute_job is executed only 3 times, because it's
        # not called for the process stop marker
        assert worker_process_execute_job_mock.call_count == 3
        assert interface.queue.task_done.call_count == 4
        assert interface.errors_queue.get() == "Something is gone wrong"
        assert interface.errors_queue.empty()

    @mock.patch("barman.cloud.os.unlink")
    @mock.patch("barman.cloud.open")
    @mock.patch(
        "barman.cloud_providers.aws_s3.S3CloudInterface._complete_multipart_upload"
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface._upload_part")
    @mock.patch("datetime.datetime")
    def test_worker_process_execute_job(
        self,
        datetime_mock,
        upload_part_mock,
        complete_multipart_upload_mock,
        open_mock,
        unlink_mock,
    ):
        # Unknown job type, no boto functions are being called and
        # an exception is being raised
        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.result_queue = Queue()
        interface.done_queue = Queue()
        with pytest.raises(ValueError):
            interface._worker_process_execute_job({"job_type": "error"}, 1)
        assert upload_part_mock.call_count == 0
        assert complete_multipart_upload_mock.call_count == 0
        assert interface.result_queue.empty()

        # upload_part job, a file with the passed name is opened, uploaded
        # and them deleted
        part_result = {"ETag": "89d4f0341d9091aa21ddf67d3b32c34a", "PartNumber": "10"}
        upload_part_mock.return_value = part_result
        interface._worker_process_execute_job(
            {
                "job_type": "upload_part",
                "upload_metadata": {"UploadId": "upload_id"},
                "part_number": 10,
                "key": "this/key",
                "body": "body",
            },
            0,
        )
        upload_part_mock.assert_called_once_with(
            {"UploadId": "upload_id"},
            "this/key",
            open_mock.return_value.__enter__.return_value,
            10,
        )
        assert not interface.result_queue.empty()
        assert interface.result_queue.get() == {
            "end_time": datetime_mock.now.return_value,
            "key": "this/key",
            "part": part_result,
            "part_number": 10,
        }
        assert unlink_mock.call_count == 1

        # complete_multipart_upload, an S3 call to create a key in the bucket
        # with the right parts is called
        interface._worker_process_execute_job(
            {
                "job_type": "complete_multipart_upload",
                "upload_metadata": {"UploadId": "upload_id"},
                "key": "this/key",
                "parts_metadata": ["parts", "list"],
            },
            0,
        )
        complete_multipart_upload_mock.assert_called_once_with(
            {"UploadId": "upload_id"}, "this/key", ["parts", "list"]
        )
        assert not interface.done_queue.empty()
        assert interface.done_queue.get() == {
            "end_time": datetime_mock.now.return_value,
            "key": "this/key",
            "status": "done",
        }

    def test_handle_async_errors(self):
        # If we the upload process has already raised an error, we immediately
        # exit without doing anything
        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.error = "test"
        interface.errors_queue = None  # If get called raises AttributeError
        interface._handle_async_errors()

        # There is no error and the process haven't already errored out
        interface.error = None
        interface.errors_queue = Queue()
        interface._handle_async_errors()
        assert interface.error is None

        # There is an error in the queue
        interface.error = None
        interface.errors_queue.put("Test error")
        with pytest.raises(CloudUploadingError):
            interface._handle_async_errors()

    @mock.patch("barman.cloud.NamedTemporaryFile")
    @mock.patch("barman.cloud.CloudInterface._handle_async_errors")
    @mock.patch("barman.cloud.CloudInterface._ensure_async")
    def test_async_upload_part(
        self, ensure_async_mock, handle_async_errors_mock, temp_file_mock
    ):
        temp_name = "tmp_file"
        temp_stream = temp_file_mock.return_value.__enter__.return_value
        temp_stream.name = temp_name

        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.queue = Queue()
        interface.async_upload_part(
            {"UploadId": "upload_id"}, "test/key", BytesIO(b"test"), 1
        )
        ensure_async_mock.assert_called_once_with()
        handle_async_errors_mock.assert_called_once_with()
        assert not interface.queue.empty()
        assert interface.queue.get() == {
            "job_type": "upload_part",
            "upload_metadata": {"UploadId": "upload_id"},
            "key": "test/key",
            "body": temp_name,
            "part_number": 1,
        }

    @mock.patch("barman.cloud.CloudInterface._retrieve_results")
    @mock.patch("barman.cloud.CloudInterface._handle_async_errors")
    @mock.patch("barman.cloud.CloudInterface._ensure_async")
    def test_async_complete_multipart_upload(
        self, ensure_async_mock, handle_async_errors_mock, retrieve_results_mock
    ):
        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.queue = mock.MagicMock()
        interface.parts_db = {"key": ["part", "list"]}

        def retrieve_results_effect():
            interface.parts_db["key"].append("complete")

        retrieve_results_mock.side_effect = retrieve_results_effect

        interface.async_complete_multipart_upload({"UploadId": "upload_id"}, "key", 3)
        ensure_async_mock.assert_called_once_with()
        handle_async_errors_mock.assert_called_once_with()
        retrieve_results_mock.assert_called_once_with()

        interface.queue.put.assert_called_once_with(
            {
                "job_type": "complete_multipart_upload",
                "upload_metadata": {"UploadId": "upload_id"},
                "key": "key",
                "parts_metadata": ["part", "list", "complete"],
            }
        )


class TestS3CloudInterface(object):
    """
    Tests which verify backend-specific behaviour of S3CloudInterface.
    """

    @mock.patch("barman.cloud_providers.aws_s3.Config")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_minimal(self, boto_mock, config_mock):
        # GIVEN an s3 bucket url
        bucket_url = "s3://bucket/path/to/dir"

        # WHEN an S3CloudInterface with minimal arguments is created
        cloud_interface = S3CloudInterface(url=bucket_url, encryption=None)

        # THEN the cloud interface bucket_name is set correctly
        assert cloud_interface.bucket_name == "bucket"
        # AND the cloud interface path is set correctly
        assert cloud_interface.path == "path/to/dir"
        # AND no profile name is passed to the boto3 Session
        boto_mock.Session.assert_called_once_with(profile_name=None)
        # AND a Config is created with empty arguments
        config_mock.assert_called_once_with()
        # AND the boto3 resource is created with no specified endpoint_url
        # and the created Config object
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with(
            "s3",
            endpoint_url=None,
            config=config_mock.return_value,
        )
        # AND the s3 property of the cloud interface is set to the boto3
        # resource
        assert cloud_interface.s3 == session_mock.resource.return_value

    @mock.patch("barman.cloud_providers.aws_s3.Config")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_minimal_read_timeout(self, boto_mock, config_mock):
        # GIVEN an s3 bucket url
        bucket_url = "s3://bucket/path/to/dir"

        # WHEN an S3CloudInterface with minimal arguments is created with
        # a specified read_timeout
        cloud_interface = S3CloudInterface(
            url=bucket_url, encryption=None, read_timeout=30
        )

        # THEN the cloud interface read_timeout property is set to the specified
        # value
        assert cloud_interface.read_timeout == 30
        # AND a Config is created with the specified read_timeout
        config_mock.assert_called_once_with(read_timeout=30)
        # AND the boto3 resource is created with no specified endpoint_url
        # and the created Config object
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with(
            "s3",
            endpoint_url=None,
            config=config_mock.return_value,
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_invalid_uploader_minimal(self, boto_mock):
        """
        Minimal build of the CloudInterface class
        """
        # Check that the creation of the cloud interface class fails in case of
        # wrongly formatted/invalid s3 uri
        with pytest.raises(ValueError) as excinfo:
            S3CloudInterface("/bucket/path/to/dir", encryption=None)
        assert str(excinfo.value) == "Invalid s3 URL address: /bucket/path/to/dir"

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_connectivity(self, boto_mock):
        """
        test the  test_connectivity method
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        assert cloud_interface.test_connectivity() is True
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        client_mock = s3_mock.meta.client
        client_mock.head_bucket.assert_called_once_with(Bucket="bucket")

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_connectivity_failure(self, boto_mock):
        """
        test the test_connectivity method in case of failure
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        client_mock = s3_mock.meta.client
        # Raise the exception for the "I'm unable to reach amazon" event
        client_mock.head_bucket.side_effect = EndpointConnectionError(
            endpoint_url="bucket"
        )
        assert cloud_interface.test_connectivity() is False

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_setup_bucket(self, boto_mock):
        """
        Test if a bucket already exists
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        cloud_interface.setup_bucket()
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Expect a call on the head_bucket method of the s3 client.
        s3_client.head_bucket.assert_called_once_with(
            Bucket=cloud_interface.bucket_name
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_setup_bucket_create(self, boto_mock):
        """
        Test auto-creation of a bucket if it not exists
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Simulate a 404 error from amazon for 'bucket not found'
        s3_client.head_bucket.side_effect = ClientError(
            error_response={"Error": {"Code": "404"}}, operation_name="load"
        )
        cloud_interface.setup_bucket()
        bucket_mock = s3_mock.Bucket
        # Expect a call for bucket obj creation
        bucket_mock.assert_called_once_with(cloud_interface.bucket_name)
        # Expect the create() method of the bucket object to be called
        bucket_mock.return_value.create.assert_called_once()

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_upload_fileobj(self, boto_mock):
        """
        Tests synchronous file upload with boto3
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)

        s3_client.upload_fileobj.assert_called_once_with(
            Fileobj=mock_fileobj, Bucket="bucket", Key=mock_key, ExtraArgs={}
        )

    @pytest.mark.parametrize(
        ("encryption_args", "expected_extra_args"),
        [
            (
                {"encryption": "AES256", "sse_kms_key_id": None},
                {"ServerSideEncryption": "AES256"},
            ),
            (
                {"encryption": "aws:kms", "sse_kms_key_id": None},
                {"ServerSideEncryption": "aws:kms"},
            ),
            (
                {"encryption": "aws:kms", "sse_kms_key_id": "somekeyid"},
                {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "somekeyid"},
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_upload_fileobj_with_encryption(
        self, boto_mock, encryption_args, expected_extra_args
    ):
        """
        Tests the ServerSideEncryption and SSEKMSKeyId arguments are provided to boto3
        when uploading a file if encryption args are set on the S3CloudInterface.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", **encryption_args)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)

        s3_client.upload_fileobj.assert_called_once_with(
            Fileobj=mock_fileobj,
            Bucket="bucket",
            Key=mock_key,
            ExtraArgs=expected_extra_args,
        )

    @pytest.mark.parametrize(
        "cloud_interface_tags, override_tags, expected_tagging",
        [
            # Cloud interface tags are used if no override tags
            (
                [("foo", "bar"), ("baz $%", "qux -/")],
                None,
                "foo=bar&baz+%24%25=qux+-%2F",
            ),
            # Override tags are used in place of cloud interface tags
            (
                [("foo", "bar")],
                [("$+ a", "///"), ("()", "[]")],
                "%24%2B+a=%2F%2F%2F&%28%29=%5B%5D",
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_upload_fileobj_with_tags(
        self, boto_mock, cloud_interface_tags, override_tags, expected_tagging
    ):
        """
        Tests the Tagging argument is provided to boto3 when uploading
        a file if tags are provided when creating S3CloudInterface.
        """
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir",
            # Tags must be urlencoded so include quotable characters
            tags=cloud_interface_tags,
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"
        cloud_interface.upload_fileobj(
            mock_fileobj, mock_key, override_tags=override_tags
        )

        s3_client.upload_fileobj.assert_called_once_with(
            Fileobj=mock_fileobj,
            Bucket="bucket",
            Key=mock_key,
            ExtraArgs={
                "Tagging": expected_tagging,
            },
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_create_multipart_upload(self, boto_mock):
        """
        Tests creation of a multipart upload with boto3
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_key = "path/to/dir"
        cloud_interface.create_multipart_upload(mock_key)

        s3_client.create_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key=mock_key,
        )

    @pytest.mark.parametrize(
        ("encryption_args", "expected_extra_args"),
        [
            (
                {"encryption": "AES256", "sse_kms_key_id": None},
                {"ServerSideEncryption": "AES256"},
            ),
            (
                {"encryption": "aws:kms", "sse_kms_key_id": None},
                {"ServerSideEncryption": "aws:kms"},
            ),
            (
                {"encryption": "aws:kms", "sse_kms_key_id": "somekeyid"},
                {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "somekeyid"},
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_create_multipart_upload_with_encryption(
        self, boto_mock, encryption_args, expected_extra_args
    ):
        """
        Tests the ServerSideEncryption and SSEKMSKeyId arguments are provided to boto3
        when creating a multipart upload if encryption args are set on the
        S3CloudInterface
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", **encryption_args)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_key = "path/to/dir"
        cloud_interface.create_multipart_upload(mock_key)

        s3_client.create_multipart_upload.assert_called_once_with(
            Bucket="bucket", Key=mock_key, **expected_extra_args
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_create_multipart_upload_with_tags(self, boto_mock):
        """
        Tests the Tagging argument is provided to boto3 when creating
        a multipart upload if the S3CloudInterface is created with tags
        """
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir", tags=[("foo", "bar"), ("baz +%", "qux %/")]
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_key = "path/to/dir"
        cloud_interface.create_multipart_upload(mock_key)

        s3_client.create_multipart_upload.assert_called_once_with(
            Bucket="bucket", Key=mock_key, Tagging="foo=bar&baz+%2B%25=qux+%25%2F"
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_upload_part(self, boto_mock):
        """
        Tests upload of a single part of a boto3 multipart request
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_body = mock.MagicMock()
        mock_key = "path/to/dir"
        mock_metadata = {"UploadId": "asdf"}
        cloud_interface._upload_part(mock_metadata, mock_key, mock_body, 1)

        s3_client.upload_part.assert_called_once_with(
            Body=mock_body,
            Bucket="bucket",
            Key=mock_key,
            UploadId=mock_metadata["UploadId"],
            PartNumber=1,
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_complete_multipart_upload(self, boto_mock):
        """
        Tests completion of a boto3 multipart request
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_parts = [{"PartNumber": 1}]
        mock_key = "path/to/dir"
        mock_metadata = {"UploadId": "asdf"}
        cloud_interface._complete_multipart_upload(mock_metadata, mock_key, mock_parts)

        s3_client.complete_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key=mock_key,
            UploadId=mock_metadata["UploadId"],
            MultipartUpload={"Parts": [{"PartNumber": 1}]},
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_abort_multipart_upload(self, boto_mock):
        """
        Tests upload of a single part of a boto3 multipart request
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_key = "path/to/dir"
        mock_metadata = {"UploadId": "asdf"}
        cloud_interface._abort_multipart_upload(mock_metadata, mock_key)

        s3_client.abort_multipart_upload.assert_called_once_with(
            Bucket="bucket",
            Key=mock_key,
            UploadId=mock_metadata["UploadId"],
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects(self, boto_mock):
        """
        Tests the successful deletion of a list of objects
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = ["path/to/object/1", "path/to/object/2"]
        cloud_interface.delete_objects(mock_keys)

        s3_client.delete_objects.assert_called_once_with(
            Bucket="bucket",
            Delete={
                "Quiet": True,
                "Objects": [
                    {"Key": "path/to/object/1"},
                    {"Key": "path/to/object/2"},
                ],
            },
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_with_empty_list(self, boto_mock):
        """
        Tests the successful deletion of an empty list of objects
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = []
        cloud_interface.delete_objects(mock_keys)

        # boto3 does not accept an empty list of Objects in its delete_objects
        # method so we verify it was not called
        s3_client.delete_objects.assert_not_called()

    @pytest.mark.parametrize(
        ("total_objects", "requested_batch_size", "expected_batch_size"),
        (
            # A batch size of 0 should be treated as 1
            (10, 0, 1),
            # Batch sizes less than the maximum batch size should be honoured
            (10, 1, 1),
            (100, 10, 10),
            # A batch size which exceeds the maximum batch size of 1000 should
            # be limited to the maximum batch size
            (2000, 1001, 1000),
            # A batch size of None should be treated as the maximum batch size
            (2000, None, 1000),
        ),
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_multiple_batches(
        self, boto_mock, total_objects, requested_batch_size, expected_batch_size
    ):
        """
        Tests that deletions are split into multiple requests according to the
        requested batch size and the maximum allowed batch size for the cloud
        provider (1000 for AWS S3).
        """
        # GIVEN an S3CloudInterface with the requested delete_batch_size
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir",
            encryption=None,
            delete_batch_size=requested_batch_size,
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        # AND a list of object keys to delete
        mock_keys = ["path/to/object/%s" % i for i in range(total_objects)]

        # WHEN the objects are deleted via the cloud interface
        cloud_interface.delete_objects(mock_keys)

        # THEN the total number of requests is equivalent to the expected number of
        # batches
        total_requests = int(round(total_objects / expected_batch_size))
        assert len(s3_client.delete_objects.call_args_list) == total_requests

        # AND each batch contains the expected object keys
        for i in range(0, total_requests):
            req_index = i * expected_batch_size
            assert s3_client.delete_objects.call_args_list[i] == mock.call(
                Bucket="bucket",
                Delete={
                    "Quiet": True,
                    "Objects": [
                        {"Key": key}
                        for key in mock_keys[
                            req_index : req_index + expected_batch_size
                        ]
                    ],
                },
            )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_partial_failure(self, boto_mock, caplog):
        """
        Tests that an exception is raised if there are any failures in the response
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        s3_client.delete_objects.return_value = {
            "Errors": [
                {
                    "Key": "path/to/object/1",
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            ]
        }

        with pytest.raises(CloudProviderError) as exc:
            cloud_interface.delete_objects(mock_keys)

        assert str(exc.value) == (
            "Error from cloud provider while deleting objects - please "
            "check the command output."
        )

        assert (
            "Deletion of object path/to/object/1 failed with error code: "
            '"AccessDenied", message: "Access Denied"'
        ) in caplog.text

    @pytest.mark.skipif(sys.version_info < (3, 0), reason="Requires Python 3 or higher")
    @pytest.mark.parametrize("compression", (None, "bzip2", "gzip", "snappy"))
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_download_file(self, boto_mock, compression, tmpdir):
        """Verifies that cloud_interface.download_file decompresses correctly."""
        dest_path = os.path.join(str(tmpdir), "downloaded_file")
        # GIVEN A single file containing a string
        content = "this is an arbitrary string"
        # WHICH is compressed with the specified compression
        mock_s3_fileobj = _compression_helper(
            BytesIO(content.encode("utf-8")), compression
        )
        # AND is returned by a cloud interface
        object_key = "/arbitrary/object/key"
        cloud_interface = S3CloudInterface(
            "s3://bucket/%s" % object_key, encryption=None
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_mock.Object.return_value.get.return_value = {
            "Body": mock_s3_fileobj,
        }

        # WHEN the file is downloaded from the cloud interface
        cloud_interface.download_file(object_key, dest_path, compression)

        # THEN the data is automatically decompressed and therefore the content
        # of the downloaded file matches the original content
        with open(dest_path, "r") as f:
            assert f.read() == content

    @pytest.mark.parametrize(
        ("compression", "file_ext"),
        ((None, ""), ("bzip2", ".bz2"), ("gzip", ".gz"), ("snappy", ".snappy")),
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_extract_tar(self, boto_mock, compression, file_ext, tmpdir):
        """Verifies that cloud_interface.extract_tar decompresses correctly."""
        # GIVEN A tar file containing a single file containing a string
        content = "this is an arbitrary string"
        content_filename = "an_arbitrary_filename"
        tar_fileobj = _tar_helper(
            content="this is an arbitrary string",
            content_filename="an_arbitrary_filename",
        )
        # WHICH is compressed with the specified compression
        mock_s3_fileobj = _compression_helper(tar_fileobj, compression)
        object_key = "/arbitrary/object/key.tar" + file_ext
        # AND is returned by a cloud interface
        cloud_interface = S3CloudInterface(
            "s3://bucket/%s" % object_key, encryption=None
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_mock.Object.return_value.get.return_value = {
            "Body": mock_s3_fileobj,
        }

        # WHEN the tar is extracted via the cloud interface
        cloud_interface.extract_tar(object_key, str(tmpdir))

        # THEN the content of the archive is 0automatically decompressed and therefore
        # the content of the downloaded file matches the original content
        with open(os.path.join(str(tmpdir), content_filename), "r") as f:
            assert f.read() == content


class TestAzureCloudInterface(object):
    """
    Tests which verify backend-specific behaviour of AzureCloudInterface.
    """

    @pytest.fixture
    def mock_account_url(self):
        return "storageaccount.blob.core.windows.net"

    @pytest.fixture
    def mock_object_path(self):
        return "path/to/object"

    @pytest.fixture
    def mock_storage_url(self, mock_account_url, mock_object_path):
        return "https://%s/%s/%s" % (mock_account_url, "container", mock_object_path)

    @pytest.fixture
    def mock_fileobj(self):
        """Returns a mock fileobj with length 42."""
        mock_fileobj = mock.MagicMock()
        mock_fileobj.tell.return_value = 42
        return mock_fileobj

    @pytest.fixture
    def default_azure_client_args(self):
        return {
            "max_block_size": 2 << 20,
            "max_single_put_size": 4 << 20,
        }

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "AZURE_STORAGE_SAS_TOKEN": "sas_token",
            "AZURE_STORAGE_KEY": "storage_key",
        },
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_minimal(
        self, container_client_mock, mock_storage_url, mock_object_path
    ):
        """Connection string auth takes precedence over SAS token or shared token"""
        container_name = "container"
        cloud_interface = AzureCloudInterface(url=mock_storage_url)

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "AZURE_STORAGE_SAS_TOKEN": "sas_token",
            "AZURE_STORAGE_KEY": "storage_key",
        },
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.requests.Session")
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_with_specified_credential(
        self,
        container_client_mock,
        mock_session,
        mock_account_url,
        mock_object_path,
        mock_storage_url,
        default_azure_client_args,
    ):
        """Specified credential option takes precedences over environment"""
        container_name = "container"
        credential = AzureCliCredential()
        cloud_interface = AzureCloudInterface(
            url=mock_storage_url,
            credential=credential,
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.assert_called_once_with(
            account_url=mock_account_url,
            credential=credential,
            container_name=container_name,
            session=mock_session.return_value,
            **default_azure_client_args
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.requests.Session")
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_sas_token_auth(
        self,
        container_client_mock,
        mock_session,
        mock_account_url,
        mock_storage_url,
        mock_object_path,
        default_azure_client_args,
    ):
        """SAS token takes precedence over shared token"""
        container_name = "container"
        cloud_interface = AzureCloudInterface(
            mock_storage_url,
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.assert_called_once_with(
            account_url=mock_account_url,
            credential=os.environ["AZURE_STORAGE_SAS_TOKEN"],
            container_name=container_name,
            session=mock_session.return_value,
            **default_azure_client_args
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.requests.Session")
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_shared_token_auth(
        self,
        container_client_mock,
        mock_session,
        mock_account_url,
        mock_storage_url,
        mock_object_path,
        default_azure_client_args,
    ):
        """Shared token is used if SAS token and connection string aren't set"""
        container_name = "container"
        cloud_interface = AzureCloudInterface(url=mock_storage_url)

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.assert_called_once_with(
            account_url=mock_account_url,
            credential=os.environ["AZURE_STORAGE_KEY"],
            container_name=container_name,
            session=mock_session.return_value,
            **default_azure_client_args
        )

    @mock.patch("azure.identity.DefaultAzureCredential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.requests.Session")
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_default_credential_auth(
        self,
        container_client_mock,
        mock_session,
        default_azure_credential,
        mock_account_url,
        mock_storage_url,
        mock_object_path,
        default_azure_client_args,
    ):
        """Uses DefaultAzureCredential if no other auth provided"""
        container_name = "container"
        cloud_interface = AzureCloudInterface(
            url=mock_storage_url,
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.assert_called_once_with(
            account_url=mock_account_url,
            credential=default_azure_credential.return_value,
            container_name=container_name,
            session=mock_session.return_value,
            **default_azure_client_args
        )

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
        },
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_emulated_storage(self, container_client_mock, mock_object_path):
        """Connection string auth and emulated storage URL are valid"""
        container_name = "container"
        account_url = "https://127.0.0.1/devstoreaccount1"
        cloud_interface = AzureCloudInterface(
            url="%s/%s/%s" % (account_url, container_name, mock_object_path)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == mock_object_path
        container_client_mock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )

    # Test emulated storage fails if no URL
    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    def test_emulated_storage_no_connection_string(self, mock_object_path):
        """Emulated storage URL with no connection string fails"""
        container_name = "container"
        account_url = "https://127.0.0.1/devstoreaccount1"
        with pytest.raises(ValueError) as exc:
            AzureCloudInterface(
                url="%s/%s/%s" % (account_url, container_name, mock_object_path)
            )
        assert (
            str(exc.value)
            == "A connection string must be provided when using emulated storage"
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    def test_uploader_malformed_urls(self):
        url = "https://not.the.azure.domain/container"
        with pytest.raises(ValueError) as exc:
            AzureCloudInterface(url=url)
        assert str(exc.value) == "emulated storage URL %s is malformed" % url

        url = "https://storageaccount.blob.core.windows.net"
        with pytest.raises(ValueError) as exc:
            AzureCloudInterface(url=url)
        assert str(exc.value) == "azure blob storage URL %s is malformed" % url

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_connectivity(self, container_client_mock):
        """
        Test the test_connectivity method
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        assert cloud_interface.test_connectivity() is True
        # Bucket existence checking is carried out by checking we can successfully
        # iterate the bucket contents
        container_client = container_client_mock.from_connection_string.return_value
        container_client.list_blobs.assert_called_once_with()
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.assert_called_once_with()
        # Also test that an empty bucket passes connectivity test
        blobs_iterator.next.side_effect = StopIteration()
        assert cloud_interface.test_connectivity() is True

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_connectivity_failure(self, container_client_mock):
        """
        Test the test_connectivity method in case of failure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.side_effect = ServiceRequestError("error")
        assert cloud_interface.test_connectivity() is False

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_setup_bucket(self, container_client_mock):
        """
        Test if a bucket already exists
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        cloud_interface.setup_bucket()
        container_client = container_client_mock.from_connection_string.return_value
        container_client.list_blobs.assert_called_once_with()
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_setup_bucket_create(self, container_client_mock):
        """
        Test auto-creation of a bucket if it does not exist
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.side_effect = ResourceNotFoundError()
        cloud_interface.setup_bucket()
        container_client.list_blobs.assert_called_once_with()
        blobs_iterator.next.assert_called_once_with()
        container_client.create_container.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_fileobj(self, container_client_mock, mock_fileobj):
        """Test container client upload_blob is called with expected args"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call
        container_client.upload_blob.assert_called_once_with(
            name=mock_key,
            data=mock_fileobj,
            overwrite=True,
            max_concurrency=8,
            length=mock_fileobj.tell.return_value,
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_fileobj_with_encryption_scope(
        self, container_client_mock, mock_fileobj
    ):
        """Test encryption scope is passed to upload_blob"""
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = container_client_mock.from_connection_string.return_value
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call along
        # with the encryption_scope
        container_client.upload_blob.assert_called_once_with(
            name=mock_key,
            data=mock_fileobj,
            overwrite=True,
            length=mock_fileobj.tell.return_value,
            max_concurrency=8,
            encryption_scope=encryption_scope,
        )

    @pytest.mark.parametrize(
        "cloud_interface_tags, override_tags, expected_tags",
        [
            # Cloud interface tags are used if no override tags
            (
                [("foo", "bar"), ("baz $%", "qux -/")],
                None,
                {"foo": "bar", "baz $%": "qux -/"},
            ),
            # Override tags are used in place of cloud interface tags
            (
                [("foo", "bar")],
                [("$+ a", "///"), ("()", "[]")],
                {"$+ a": "///", "()": "[]"},
            ),
        ],
    )
    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_fileobj_with_tags(
        self,
        container_client_mock,
        cloud_interface_tags,
        override_tags,
        expected_tags,
        mock_fileobj,
    ):
        """
        Tests the tags argument is provided to the container client when uploading
        a file if tags are provided when creating AzureCloudInterface.
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            tags=cloud_interface_tags,
        )
        container_client = container_client_mock.from_connection_string.return_value
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(
            mock_fileobj, mock_key, override_tags=override_tags
        )
        # The key and fileobj are passed on to the upload_blob call along
        # with the encryption_scope
        container_client.upload_blob.assert_called_once_with(
            name=mock_key,
            data=mock_fileobj,
            overwrite=True,
            length=mock_fileobj.tell.return_value,
            max_concurrency=8,
            tags=expected_tags,
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_part(self, container_client_mock):
        """
        Tests the upload of a single block in Azure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_body = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface._upload_part({}, mock_key, mock_body, 1)

        # A blob client is created for the key and stage_block is called with
        # the mock_body and a block_id generated from the part number
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.stage_block.assert_called_once_with("00001", mock_body)

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_part_with_encryption_scope(self, container_client_mock):
        """
        Tests that the encryption scope is passed to the blob client when
        uploading a single block
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_body = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface._upload_part({}, mock_key, mock_body, 1)

        # A blob client is created for the key and stage_block is called with
        # the mock_body and a block_id generated from the part number and the
        # encryption scope
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.stage_block.assert_called_once_with(
            "00001", mock_body, encryption_scope=encryption_scope
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_complete_multipart_upload(self, container_client_mock):
        """Tests completion of a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_parts = [{"PartNumber": "00001"}]
        mock_key = "path/to/blob"
        cloud_interface._complete_multipart_upload({}, mock_key, mock_parts)

        # A blob client is created for the key and commit_block_list is called
        # with the supplied list of part numbers
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(["00001"])

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_complete_multipart_upload_with_encryption_scope(
        self, container_client_mock
    ):
        """
        Tests the completion of a block blob upload in Azure Blob Storage and that
        the encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_parts = [{"PartNumber": "00001"}]
        mock_key = "path/to/blob"
        cloud_interface._complete_multipart_upload({}, mock_key, mock_parts)

        # A blob client is created for the key and commit_block_list is called
        # with the supplied list of part numbers and the encryption scope
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(
            ["00001"], encryption_scope=encryption_scope
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_complete_multipart_upload_with_tags(self, container_client_mock):
        """
        Tests that the tags argument is provided to the container client when
        completing a multipart upload if the AzureCloudInterface is created with
        tags
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            tags=[("foo", "bar"), ("baz", "qux")],
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_parts = [{"PartNumber": "00001"}]
        mock_key = "path/to/blob"
        cloud_interface._complete_multipart_upload({}, mock_key, mock_parts)

        # A blob client is created for the key and commit_block_list is called
        # with the supplied list of part numbers and the encryption scope
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(
            ["00001"], tags={"foo": "bar", "baz": "qux"}
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_abort_multipart_upload(self, container_client_mock):
        """Test aborting a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_key = "path/to/blob"
        cloud_interface._abort_multipart_upload({}, mock_key)

        # A blob client is created for the key and commit_block_list is called
        # with an empty list, followed by delete_blob with no args
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with([])
        blob_client_mock.delete_blob.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_abort_multipart_upload_with_encryption_scope(self, container_client_mock):
        """
        Test aborting a block blob upload in Azure Blob Storage and verify that the
        encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = container_client_mock.from_connection_string.return_value
        blob_client_mock = container_client.get_blob_client.return_value

        mock_key = "path/to/blob"
        cloud_interface._abort_multipart_upload({}, mock_key)

        # A blob client is created for the key and commit_block_list is called
        # with an empty list and the encryption scope, followed by delete_blob
        # with no args
        container_client.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(
            [], encryption_scope=encryption_scope
        )
        blob_client_mock.delete_blob.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects(self, container_client_mock):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        mock_keys = ["path/to/object/1", "path/to/object/2"]
        cloud_interface.delete_objects(mock_keys)

        container_client.delete_blobs.assert_called_once_with(*mock_keys)

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_with_empty_list(self, container_client_mock):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        mock_keys = []
        cloud_interface.delete_objects(mock_keys)

        # All cloud interface implementations should short-circuit and avoid calling
        # the cloud provider SDK when given an empty list.
        container_client.delete_blobs.assert_not_called()

    @pytest.mark.parametrize(
        ("total_objects", "requested_batch_size", "expected_batch_size"),
        (
            # A batch size of 0 should be treated as 1
            (10, 0, 1),
            # Batch sizes less than the maximum batch size should be honoured
            (10, 1, 1),
            (100, 10, 10),
            # A batch size which exceeds the maximum batch size of 256 should
            # be limited to the maximum batch size
            (1024, 1001, 256),
            # A batch size of None should be treated as the maximum batch size
            (1024, None, 256),
        ),
    )
    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_multiple_batches(
        self,
        container_client_mock,
        total_objects,
        requested_batch_size,
        expected_batch_size,
    ):
        """
        Tests that deletions are split into multiple requests according to the
        requested batch size and the maximum allowed batch size for the cloud
        provider (256 for Azure Blob Storage).
        """
        # GIVEN an AzureCloudInterface with the requested delete_batch_size
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            delete_batch_size=requested_batch_size,
        )
        container_client = container_client_mock.from_connection_string.return_value

        # AND a list of object keys to delete
        mock_keys = ["path/to/object/%s" % i for i in range(total_objects)]

        # WHEN the objects are deleted via the cloud interface
        cloud_interface.delete_objects(mock_keys)

        # THEN the total number of requests is equivalent to the expected number of
        # batches
        total_requests = int(round(total_objects / expected_batch_size))
        assert len(container_client.delete_blobs.call_args_list) == total_requests

        # AND each batch contains the expected object keys
        for i in range(0, total_requests):
            req_index = i * expected_batch_size
            assert container_client.delete_blobs.call_args_list[i] == mock.call(
                *mock_keys[req_index : req_index + expected_batch_size]
            )

    def _create_mock_HttpResponse(self, status_code, url):
        """Helper function for partial failure tests."""
        htr = mock.Mock()
        htr.status_code = status_code
        htr.request.url = url
        return htr

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_partial_failure(self, container_client_mock, caplog):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        container_client.delete_blobs.return_value = iter(
            [
                self._create_mock_HttpResponse(403, "path/to/object/1"),
                self._create_mock_HttpResponse(202, "path/to/object/2"),
            ]
        )

        with pytest.raises(CloudProviderError) as exc:
            cloud_interface.delete_objects(mock_keys)

        assert str(exc.value) == (
            "Error from cloud provider while deleting objects - please "
            "check the command output."
        )

        assert (
            'Deletion of object path/to/object/1 failed with error code: "403"'
        ) in caplog.text

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_partial_failure_exception(
        self, container_client_mock, caplog
    ):
        """
        Test that partial failures raised via PartialBatchErrorException are handled.
        This isn't explicitly described in the Azure documentation but is something
        which happens in practice so we must deal with it.
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        parts = iter(
            [
                self._create_mock_HttpResponse(403, "path/to/object/1"),
                self._create_mock_HttpResponse(202, "path/to/object/2"),
            ]
        )
        partial_batch_error_exception = PartialBatchErrorException(
            "something went wrong", None, parts
        )
        container_client.delete_blobs.side_effect = partial_batch_error_exception

        with pytest.raises(CloudProviderError) as exc:
            cloud_interface.delete_objects(mock_keys)

        assert str(exc.value) == (
            "Error from cloud provider while deleting objects - please "
            "check the command output."
        )

        assert (
            'Deletion of object path/to/object/1 failed with error code: "403"'
        ) in caplog.text

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_404_not_failure(self, container_client_mock, caplog):
        """
        Test that 404 responses in partial failures do not create an error.
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        parts = iter(
            [
                self._create_mock_HttpResponse(404, "path/to/object/1"),
                self._create_mock_HttpResponse(202, "path/to/object/2"),
            ]
        )
        partial_batch_error_exception = PartialBatchErrorException(
            "something went wrong", None, parts
        )
        container_client.delete_blobs.side_effect = partial_batch_error_exception

        cloud_interface.delete_objects(mock_keys)

        assert (
            "Deletion of object path/to/object/1 failed because it could not be found"
        ) in caplog.text

    @pytest.mark.skipif(sys.version_info < (3, 0), reason="Requires Python 3 or higher")
    @pytest.mark.parametrize("compression", (None, "bzip2", "gzip", "snappy"))
    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_download_file(self, container_client_mock, compression, tmpdir):
        """Verifies that cloud_interface.download_file decompresses correctly."""
        dest_path = os.path.join(str(tmpdir), "downloaded_file")
        # GIVEN A single file containing a string
        content = "this is an arbitrary string"
        # WHICH is compressed with the specified compression
        mock_fileobj = _compression_helper(
            BytesIO(content.encode("utf-8")), compression
        )
        # AND is returned by a cloud interface
        object_key = "/arbitrary/object/key"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        # WHEN the file is downloaded from the cloud interface
        if compression is None:
            # Just verify the Azure download_to_stream method was called because
            # that is a shortcut taken when there is no compression
            cloud_interface.download_file(object_key, dest_path, compression)
            azure_resp = container_client.download_blob.return_value
            azure_resp.download_to_stream.assert_called_once()
        else:
            # The response from container_client.download_blob isn't a file-like
            # object - it provides a chunks() method which returns an iterable of
            # bytes, so that is what we create here
            chunks = iter([mock_fileobj.read()])
            try:
                chunk_iter_fun = chunks.__next__
            except AttributeError:
                # If there was no __next__ then we must be python2 so use next
                chunk_iter_fun = chunks.next
            container_client.download_blob.return_value.chunks.return_value.next = (
                chunk_iter_fun
            )
            cloud_interface.download_file(object_key, dest_path, compression)
            # THEN the data is automatically decompressed and therefore the content
            # of the downloaded file matches the original content
            with open(dest_path, "r") as f:
                assert f.read() == content

    @pytest.mark.parametrize(
        ("compression", "file_ext"),
        ((None, ""), ("bzip2", ".bz2"), ("gzip", ".gz"), ("snappy", ".snappy")),
    )
    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_extract_tar(self, container_client_mock, compression, file_ext, tmpdir):
        """Verifies that cloud_interface.download_file decompresses correctly."""
        # GIVEN A tar file containing a single file containing a string
        content = "this is an arbitrary string"
        content_filename = "an_arbitrary_filename"
        tar_fileobj = _tar_helper(
            content="this is an arbitrary string",
            content_filename="an_arbitrary_filename",
        )
        # WHICH is compressed with the specified compression
        mock_fileobj = _compression_helper(tar_fileobj, compression)
        object_key = "/arbitrary/object/key.tar" + file_ext
        # AND is returned by a cloud interface
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = container_client_mock.from_connection_string.return_value

        # The response from container_client.download_blob isn't a file-like
        # object - it provides a chunks() method which returns an iterable of
        # bytes, so that is what we create here
        chunks = iter([mock_fileobj.read()])
        try:
            chunk_iter_fun = chunks.__next__
        except AttributeError:
            # If there was no __next__ then we must be python2 so use next
            chunk_iter_fun = chunks.next
        container_client.download_blob.return_value.chunks.return_value.next = (
            chunk_iter_fun
        )

        # WHEN the tar is extracted via the cloud interface
        cloud_interface.extract_tar(object_key, str(tmpdir))

        # THEN the content of the archive is 0automatically decompressed and therefore
        # the content of the downloaded file matches the original content
        with open(os.path.join(str(tmpdir), content_filename), "r") as f:
            assert f.read() == content


class TestGoogleCloudInterface(TestCase):
    """
    Tests which verify backend-specific behaviour of GoogleCloudInterface.
    """

    @pytest.mark.skipif(
        sys.version_info < (3, 5), reason="requires python3.6 or higher"
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_uploader_default_credential_auth(self, gcs_client_mock):
        """Uses DefaultCredential if no other auth provided"""
        tests = {
            "https_url": {
                "url": "https://console.cloud.google.com/storage/browser/some-bucket/useful/path",
                "expected-path": "useful/path",
                "expected-bucket-name": "some-bucket",
            },
            "gs_url": {
                "url": "gs://some-bucket/useful/path",
                "expected-path": "useful/path",
                "expected-bucket-name": "some-bucket",
            },
        }

        for test_name, test in tests.items():
            with self.subTest(test_name):
                cloud_interface = GoogleCloudInterface(test["url"])
                assert cloud_interface.bucket_name == test["expected-bucket-name"]
                assert cloud_interface.path == test["expected-path"]
        self.assertEqual(gcs_client_mock.call_count, 2)

    @pytest.mark.skipif(
        sys.version_info < (3, 5), reason="requires python3.6 or higher"
    )
    def test_uploader_malformed_urls(
        self,
    ):
        error_string = (
            "Google cloud storage URL {} is malformed. Expected format are "
            "'https://console.cloud.google.com/storage/browser/bucket-name/some/path' "
            "or 'gs://bucket-name/some/path'"
        )
        tests = {
            "wrong domain": {
                "url": "https://unexpected.domain/storage/browser/container",
                "error": ValueError,
                "message": error_string.format(
                    "https://unexpected.domain/storage/browser/container"
                ),
            },
            "wrong base path": {
                "url": "https://console.cloud.google.com/storage/container",
                "error": ValueError,
                "message": error_string.format(
                    "https://console.cloud.google.com/storage/container"
                ),
            },
            "missing bucket": {
                "url": "https://console.cloud.google.com/storage/browser",
                "error": ValueError,
                "message": error_string.format(
                    "https://console.cloud.google.com/storage/browser"
                ),
            },
            "missing bucket bis": {
                "url": "https://console.cloud.google.com/storage/browser/",
                "error": ValueError,
                "message": "Google cloud storage URL https://console.cloud.google.com/storage/browser/ is malformed. "
                "Bucket name not found",
            },
            "missing bucket ter": {
                "url": "gs://",
                "error": ValueError,
                "message": "Google cloud storage URL gs:// is malformed. Bucket name not found",
            },
        }
        for test_name, test in tests.items():
            with self.subTest(test_name):
                with pytest.raises(test["error"]) as exc:
                    GoogleCloudInterface(url=test["url"])
                assert str(exc.value) == test["message"]

    @mock.patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "credentials_path"})
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_connectivity(self, gcs_client_mock):
        """
        Test the test_connectivity method
        """
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test"
        )
        assert cloud_interface.test_connectivity() is True
        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.exists.assert_called_once_with()

    @mock.patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "credentials_path"})
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_connectivity_failure(self, gcs_client_mock):
        """
        Test the test_connectivity method in case of failure
        """
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/bucket/path/some/blob"
        )
        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.exists.side_effect = GoogleAPIError("error")
        assert cloud_interface.test_connectivity() is False

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_setup_bucket(self, gcs_client_mock):
        """
        Test if a bucket already exists
        """
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test/path/to/dir"
        )
        cloud_interface.setup_bucket()
        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.exists.assert_called_once_with()

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_setup_bucket_create(self, gcs_client_mock):
        """
        Test auto-creation of a bucket if it not exists
        """
        container_client_mock = mock.Mock()
        container_client_mock.exists.return_value = False

        service_client_mock = gcs_client_mock.return_value
        service_client_mock.bucket.return_value = container_client_mock

        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-testss/test/path/to/my/"
        )
        cloud_interface.setup_bucket()
        container_client_mock.exists.assert_called_once_with()
        service_client_mock.create_bucket.assert_called_once_with(container_client_mock)

    @mock.patch("barman.cloud_providers.google_cloud_storage.logging")
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_setup_bucket_create_conflict_error(self, gcs_client_mock, logging_mock):
        """
        Test auto-creation of a bucket if it not exists but exist error when creating bucket.
        This doesn't seem logical, but it can happen when quickly deleting a bucket and object, recreating it
        and testing existence just after.
        Encountered in barman-testing suite 080 if I recall well.
        """
        container_client_mock = mock.Mock()
        container_client_mock.exists.return_value = False

        service_client_mock = gcs_client_mock.return_value
        service_client_mock.bucket.return_value = container_client_mock
        service_client_mock.create_bucket.side_effect = Conflict("Bucket already exist")
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-testss/test/path/to/my/"
        )
        cloud_interface.setup_bucket()
        container_client_mock.exists.assert_called_once_with()

        service_client_mock.create_bucket.assert_called_once_with(container_client_mock)
        logging_mock.warning.assert_called()

    @pytest.mark.skipif(
        sys.version_info < (3, 6), reason="requires python3.6 or higher"
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_list_bucket(self, gcs_client_mock):
        test_cases = {
            "default_delimiter": {
                "prefix": "test/path/to",
                "delimiter": None,
                "blob_files": ["path/to/some-file", "path/to/some-other-file"],
                "blob_dirs": ["path/to/dir/", "path/to/dir2/"],
                "expected": [
                    "path/to/some-file",
                    "path/to/some-other-file",
                    "path/to/dir/",
                    "path/to/dir2/",
                ],
            },
            "no_delimiter": {
                "prefix": "test/path/to",
                "delimiter": "",
                "blob_files": [
                    "path/to/some-file",
                    "path/to/some-other-file",
                    "path/to/dir/f1",
                    "path/to/dir2/f2",
                ],
                "blob_dirs": [],
                "expected": [
                    "path/to/some-file",
                    "path/to/some-other-file",
                    "path/to/dir/f1",
                    "path/to/dir2/f2",
                ],
            },
        }
        for test_name, test_case in test_cases.items():
            with self.subTest(msg=test_name, delimiter=test_case["delimiter"]):
                # Simulate blobs client response object
                blobs = MagicMock()
                blobs.__iter__.return_value = list(
                    map(
                        lambda file: type("", (), {"name": file}),
                        test_case["blob_files"],
                    )
                )
                blobs.prefixes = test_case["blob_dirs"]

                service_client_mock = gcs_client_mock.return_value
                service_client_mock.list_blobs.return_value = blobs
                # set delimiter value
                delimiter = (
                    test_case["delimiter"]
                    if test_case["delimiter"]
                    else DEFAULT_DELIMITER
                )
                # Create object and call list_bucket
                cloud_interface = GoogleCloudInterface(
                    "https://console.cloud.google.com/storage/browser/barman-tests/path/to/somewhere"
                )
                content = cloud_interface.list_bucket(
                    test_case["prefix"], delimiter=delimiter
                )
                assert content == test_case["expected"]

    @pytest.mark.skipif(
        sys.version_info < (3, 6), reason="requires python3.6 or higher"
    )
    def test_upload_fileobj_with(self):
        """
        Tests the tags argument is provided to the container client when uploading
        a file if tags are provided when creating AzureCloudInterface.
        """
        test_cases = {
            "No tag": {
                "cloud_interface_tags": None,
                "override_tags": None,
                "expected_tags": None,
            },
            "Cloud interface tags are used if no override tags": {
                "cloud_interface_tags": [("foo", "bar"), ("baz $%", "qux -/")],
                "override_tags": None,
                "expected_tags": {"foo": "bar", "baz $%": "qux -/"},
            },
            "Override tags are used in place of cloud interface tags": {
                "cloud_interface_tags": [("foo", "bar")],
                "override_tags": [("$+ a", "///"), ("()", "[]")],
                "expected_tags": {"$+ a": "///", "()": "[]"},
            },
        }
        for test_name, test in test_cases.items():
            with self.subTest(name=test_name):
                with unittest_patch(
                    "barman.cloud_providers.google_cloud_storage.storage.Client"
                ) as gcs_client_mock:
                    mock_fileobj = mock.MagicMock()
                    mock_blob = mock.MagicMock()

                    service_client_mock = gcs_client_mock.return_value
                    container_client_mock = service_client_mock.bucket.return_value
                    container_client_mock.blob.return_value = mock_blob
                    # Init metadata to none for no tag case
                    mock_blob.metadata = None
                    cloud_interface = GoogleCloudInterface(
                        "https://console.cloud.google.com/storage/browser/barman-test/test/path/to/my/",
                        tags=test["cloud_interface_tags"],
                    )
                    mock_key = "path/to/blob"
                    cloud_interface.upload_fileobj(
                        mock_fileobj, mock_key, override_tags=test["override_tags"]
                    )
                    # Validate behavior
                    assert mock_blob.metadata == test["expected_tags"]
                    container_client_mock.blob.assert_called_once_with(mock_key)
                    mock_blob.upload_from_file.assert_called_once_with(mock_fileobj)

    @pytest.mark.skipif(
        sys.version_info < (3, 6), reason="requires python3.6 or higher"
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_upload_fileobj_with_encryption(self, gcs_client_mock):
        """
        Tests the kms_key_name is provided to the GCS client when uploading a file if
        kms_key_name is set on the GoogleCloudInterface.
        """
        # GIVEN a GCS cloud interface created with the kms_key_name argument
        kms_key_name = "somekeyname"
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test/path/to/my/",
            kms_key_name=kms_key_name,
        )

        # AND a mock container client
        mock_fileobj = mock.MagicMock()
        mock_blob = mock.MagicMock()
        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.blob.return_value = mock_blob

        # WHEN upload_fileobj is called  on the cloud interface
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, "path/to/blob")

        # THEN the blob was created with the expected kms_key_name
        container_client_mock.blob.assert_called_once_with(
            mock_key, kms_key_name=kms_key_name
        )
        # AND the blob was uploaded
        mock_blob.upload_from_file.assert_called_once_with(mock_fileobj)

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_upload_part(self, gcs_client_mock):
        """
        Tests the upload of a single block in Google
        At that time there is no real multipart and file are sent entirely in one  bloc
        """
        mock_key = "path/to/blob"
        mock_body = mock.MagicMock()
        mock_blob = mock.MagicMock()

        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.blob.return_value = mock_blob

        # Create Object and call upload_fileobj
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test/path/to/my/"
        )
        cloud_interface._upload_part({}, mock_key, mock_body, 1)

        # Validate behavior
        container_client_mock.blob.assert_called_once_with(mock_key)
        mock_blob.upload_from_file.assert_called_once_with(mock_body)

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_delete_objects(self, gcs_client_mock):
        mock_blob1 = mock.MagicMock()
        mock_blob2 = mock.MagicMock()

        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value

        container_client_mock.blob.side_effect = [mock_blob1, mock_blob2]
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/"
        )
        mock_keys = ["path/to/object/1", "path/to/object/2"]
        cloud_interface.delete_objects(mock_keys)

        mock_blob1.delete.assert_called_once()
        mock_blob2.delete.assert_called_once()
        self.assertEqual(2, container_client_mock.blob.call_count)
        mock_calls = list(map(lambda x: mock.call(x), mock_keys))
        container_client_mock.blob.assert_has_calls(mock_calls, any_order=True)

    @mock.patch("barman.cloud_providers.google_cloud_storage.logging")
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_delete_objects_with_error(self, gcs_client_mock, logging_mock):
        mock_blob1 = mock.MagicMock()
        mock_blob1.delete.side_effect = GoogleAPIError("Failed delete blob1")
        mock_blob2 = mock.MagicMock()
        print("blob1", mock_blob1)
        print("blob2", mock_blob2)

        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value

        container_client_mock.blob.side_effect = {
            "path/to/object/1": mock_blob1,
            "path/to/object/2": mock_blob2,
        }.get
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/"
        )
        mock_keys = ["path/to/object/1", "path/to/object/2"]
        with pytest.raises(CloudProviderError):
            cloud_interface.delete_objects(mock_keys)

        logging_mock.error.assert_called_with(
            {
                "path/to/object/1": [
                    "<class 'google.api_core.exceptions.GoogleAPIError'>",
                    "Failed delete blob1",
                ]
            }
        )
        mock_blob1.delete.assert_called_once()
        mock_blob2.delete.assert_called_once()
        print(container_client_mock.blob.call_count)
        self.assertEqual(2, container_client_mock.blob.call_count)
        mock_calls = list(map(lambda x: mock.call(x), mock_keys))
        container_client_mock.blob.assert_has_calls(mock_calls, any_order=True)

    @pytest.mark.skipif(
        sys.version_info < (3, 6), reason="Requires Python 3.6 or higher"
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.open")
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage")
    def test_download_file(self, gcs_storage_mock, open_mock):
        test_cases = {
            "no_compression": {
                "compression": None,
            },
            "bzip2_compression": {
                "compression": "bzip2",
            },
            "gzip_compression": {
                "compression": "gzip",
            },
            "snappy_compression": {
                "compression": "snappy",
            },
        }
        for test_name, test_case in test_cases.items():
            with self.subTest(msg=test_name, compression=test_case["compression"]):
                with unittest_patch(
                    "barman.cloud_providers.google_cloud_storage.decompress_to_file"
                ) as decompress_to_file_mock:
                    opened_dest_file = open_mock().__enter__.return_value
                    storage_client_mock = gcs_storage_mock.Client()
                    blob_mock = gcs_storage_mock.Blob()
                    blob_mock.exists.return_value = True

                    """Verifies that cloud_interface.download_file decompresses correctly."""
                    # AND is returned by a cloud interface
                    object_key = "/arbitrary/object/key"
                    cloud_interface = GoogleCloudInterface(
                        "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/"
                    )

                    # WHEN the file is downloaded from the cloud interface
                    if test_case["compression"] is None:
                        # Just verify the download_blob_to_file method was called because
                        cloud_interface.download_file(
                            object_key, "/some/fake/path", None
                        )
                        storage_client_mock.download_blob_to_file.assert_called_once()
                        storage_client_mock.download_blob_to_file.assert_called_with(
                            blob_mock, opened_dest_file
                        )
                    else:
                        cloud_interface.download_file(
                            object_key, "/some/fake/path", test_case["compression"]
                        )
                        assert decompress_to_file_mock.call_count
                        decompress_to_file_mock.assert_called_with(
                            blob_mock.open().__enter__(),
                            opened_dest_file,
                            test_case["compression"],
                        )


class TestGoogleCloudInterfaceParametrized(object):
    """
    Tests which verify backend-specific behaviour of GoogleCloudInterface
    and use parametrized tests (these do not work with subclasses of TestCase).
    """

    @pytest.mark.parametrize(
        ("total_objects", "requested_batch_size", "expected_batch_size"),
        (
            # A batch size of 0 should be treated as 1
            (10, 0, 1),
            # Batch sizes less than the maximum batch size should be honoured
            (10, 1, 1),
            (50, 10, 10),
            # A batch size which exceeds the maximum batch size of 100 should
            # be limited to the maximum batch size
            (1000, 101, 100),
            # A batch size of None should be treated as the maximum batch size
            (1000, None, 100),
        ),
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_delete_objects_multiple_batches(
        self, gcs_client_mock, total_objects, requested_batch_size, expected_batch_size
    ):
        """
        Tests that deletions are split into multiple requests according to the
        requested batch size and the maximum allowed batch size for the cloud
        provider (100 for Google Cloud Storage).
        """
        # GIVEN a list of object keys to delete
        mock_keys = ["path/to/object/%s" % i for i in range(total_objects)]
        mock_blobs = [mock.MagicMock() for _ in mock_keys]

        # AND a GoogleCloudInterface with the requested delete_batch_size value
        service_client_mock = gcs_client_mock.return_value
        container_client_mock = service_client_mock.bucket.return_value
        container_client_mock.blob.side_effect = mock_blobs
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/",
            delete_batch_size=requested_batch_size,
        )

        # WHEN the objects are deleted via the cloud interface
        cloud_interface.delete_objects(mock_keys)

        # THEN delete was called on each blob
        for mock_blob in mock_blobs:
            mock_blob.delete.assert_called_once()

        # AND the batch context manager was called the expected number of times
        number_of_batches = int(round(total_objects / expected_batch_size))
        assert service_client_mock.batch.call_count == number_of_batches


class TestGetCloudInterface(object):
    """
    Verify get_cloud_interface creates the required CloudInterface
    """

    @pytest.fixture()
    def mock_config_aws(self):
        return Namespace(
            endpoint_url=None, profile=None, source_url="test-url", read_timeout=None
        )

    @pytest.fixture()
    def mock_config_azure(self):
        return Namespace(azure_credential=None, source_url="test-url")

    @pytest.fixture()
    def mock_config_gcs(self):
        return Namespace(source_url="test-url")

    def test_unsupported_provider(self, mock_config_aws):
        """Verify an exception is raised for unsupported cloud providers"""
        mock_config_aws.cloud_provider = "aws-infinidash"
        with pytest.raises(CloudProviderUnsupported) as exc:
            get_cloud_interface(mock_config_aws)
        assert "Unsupported cloud provider: aws-infinidash" == str(exc.value)

    @pytest.mark.parametrize(
        "extra_args",
        [
            {},
            {"jobs": 2},
            {"tags": [("foo", "bar"), ("baz", "qux")]},
            {"encryption": "aws:kms", "sse_kms_key_id": "somekeyid"},
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_aws_s3(self, mock_s3_cloud_interface, mock_config_aws, extra_args):
        """Verify --cloud-provider=aws-s3 creates an S3CloudInterface"""
        mock_config_aws.cloud_provider = "aws-s3"
        for k, v in extra_args.items():
            setattr(mock_config_aws, k, v)
        get_cloud_interface(mock_config_aws)
        mock_s3_cloud_interface.assert_called_once_with(
            url="test-url",
            profile_name=None,
            endpoint_url=None,
            read_timeout=None,
            **extra_args
        )

    @pytest.mark.parametrize(
        ("extra_args", "expected_error"),
        [
            (
                {"encryption": None, "sse_kms_key_id": "somekeyid"},
                'Encryption type must be "aws:kms" if SSE KMS Key ID is specified',
            ),
            (
                {"encryption": "AES256", "sse_kms_key_id": "somekeyid"},
                'Encryption type must be "aws:kms" if SSE KMS Key ID is specified',
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_aws_s3_invalid_config(
        self, mock_s3_cloud_interface, mock_config_aws, extra_args, expected_error
    ):
        """Verify disallowed parameter combinations with aws-s3 provider."""
        # GIVEN a config with cloud provider aws-s3
        mock_config_aws.cloud_provider = "aws-s3"
        # AND a set of forbiddden options
        for k, v in extra_args.items():
            setattr(mock_config_aws, k, v)

        # WHEN get_cloud_interface is called with this config
        # THEN an exception is raised
        with pytest.raises(CloudProviderOptionUnsupported) as exc:
            get_cloud_interface(mock_config_aws)

        # AND the exception has the expected message
        assert expected_error == str(exc.value)

    @pytest.mark.parametrize(
        "extra_args",
        [
            {},
            {"jobs": 2},
            {"tags": [("foo", "bar"), ("baz", "qux")]},
        ],
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.AzureCloudInterface")
    def test_azure_blob_storage(
        self, mock_azure_cloud_interface, mock_config_azure, extra_args
    ):
        """Verify --cloud-provider=azure-blob-storage creates an AzureCloudInterface"""
        mock_config_azure.cloud_provider = "azure-blob-storage"
        for k, v in extra_args.items():
            setattr(mock_config_azure, k, v)
        get_cloud_interface(mock_config_azure)
        mock_azure_cloud_interface.assert_called_once_with(url="test-url", **extra_args)

    def test_azure_blob_storage_unsupported_credential(self, mock_config_azure):
        """Verify unsupported Azure credentials raise an exception"""
        mock_config_azure.cloud_provider = "azure-blob-storage"
        mock_config_azure.azure_credential = "qbasic-credential"
        with pytest.raises(CloudProviderOptionUnsupported) as exc:
            get_cloud_interface(mock_config_azure)
        assert "Unsupported credential: qbasic-credential" == str(exc.value)

    @pytest.mark.parametrize(
        "credential_arg,expected_credential",
        [
            ("azure-cli", AzureCliCredential),
            ("managed-identity", ManagedIdentityCredential),
        ],
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.AzureCloudInterface")
    def test_azure_blob_storage_supported_credential(
        self,
        mock_azure_cloud_interface,
        mock_config_azure,
        credential_arg,
        expected_credential,
    ):
        """Verify provided credentials result in the correct credential type"""
        mock_config_azure.cloud_provider = "azure-blob-storage"
        mock_config_azure.azure_credential = credential_arg
        get_cloud_interface(mock_config_azure)
        mock_azure_cloud_interface.assert_called_once()
        assert isinstance(
            mock_azure_cloud_interface.call_args_list[0][1]["credential"],
            expected_credential,
        )

    @pytest.mark.parametrize(
        "extra_args",
        [
            {},
            {"jobs": 2},
            {"tags": [("foo", "bar"), ("baz", "qux")]},
            {"kms_key_name": "somekeyname"},
        ],
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.GoogleCloudInterface")
    def test_google_cloud_storage(
        self, mock_gcs_cloud_interface, mock_config_gcs, extra_args
    ):
        """Verify --cloud-provider=google-cloud-storage creates a GoogleCloudInterface"""
        mock_config_gcs.cloud_provider = "google-cloud-storage"
        for k, v in extra_args.items():
            setattr(mock_config_gcs, k, v)
        get_cloud_interface(mock_config_gcs)
        # No matter what, jobs parameter will be set to 1
        extra_args["jobs"] = 1
        mock_gcs_cloud_interface.assert_called_once_with(url="test-url", **extra_args)

    @pytest.mark.parametrize(
        ("extra_args", "expected_error"),
        [
            (
                {
                    "snapshot_instance": "someinstancename",
                    "kms_key_name": "somekeyname",
                },
                "KMS key cannot be specified for snapshot backups",
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.GoogleCloudInterface")
    def test_google_cloud_storage_invalid_config(
        self, _mock_gcs_cloud_interface, mock_config_gcs, extra_args, expected_error
    ):
        """Verify --cloud-provider=google-cloud-storage creates a GoogleCloudInterface"""
        # GIVEN a config with cloud provider google-cloud-storage
        mock_config_gcs.cloud_provider = "google-cloud-storage"
        # AND a set of forbidden options
        for k, v in extra_args.items():
            setattr(mock_config_gcs, k, v)

        # WHEN get_cloud_interface is called with this config
        # THEN an exception is raised
        with pytest.raises(CloudProviderOptionUnsupported) as exc:
            get_cloud_interface(mock_config_gcs)

        # AND the exception has the expected message
        assert expected_error == str(exc.value)


class TestCloudBackupCatalog(object):
    """
    Tests which verify we can list backups stored in a cloud provider
    """

    def get_backup_info_file_object(self):
        """Minimal backup info"""
        return BytesIO(
            b"""
backup_label=None
end_time=2014-12-22 09:25:27.410470+01:00
"""
        )

    def raise_exception(self):
        raise Exception("something went wrong reading backup.info")

    def mock_remote_open(self, _):
        """
        Helper function which alternates between successful and unsuccessful
        remote_open responses.
        """
        try:
            if self.remote_open_should_succeed:
                return self.get_backup_info_file_object()
            else:
                raise Exception("something went wrong reading backup.info")
        finally:
            self.remote_open_should_succeed = not self.remote_open_should_succeed

    def test_can_list_single_backup(self):
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/base/20210723T133818/",
        ]
        mock_cloud_interface.remote_open.return_value = (
            self.get_backup_info_file_object()
        )
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        backups = catalog.get_backup_list()
        assert len(backups) == 1
        assert "20210723T133818" in backups

    def test_backups_can_be_listed_if_one_is_unreadable(self):
        self.remote_open_should_succeed = True
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/base/20210723T133818/",
            "mt-backups/test-server/base/20210723T154445/",
            "mt-backups/test-server/base/20210723T154554/",
        ]
        mock_cloud_interface.remote_open.side_effect = self.mock_remote_open
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        backups = catalog.get_backup_list()
        assert len(backups) == 2
        assert "20210723T133818" in backups
        assert "20210723T154445" not in backups
        assert "20210723T154554" in backups

    def test_unreadable_backup_ids_are_stored(self):
        """Test we can retrieve IDs of backups which could not be read"""
        self.remote_open_should_succeed = False
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/base/20210723T133818/",
        ]
        mock_cloud_interface.remote_open.side_effect = self.mock_remote_open
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        catalog.get_backup_list()
        assert len(catalog.unreadable_backups) == 1
        assert "20210723T133818" in catalog.unreadable_backups

    def test_can_remove_a_backup_from_cache(self):
        """Test we can remove a backup from the cached list"""
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/base/20210723T133818/",
            "mt-backups/test-server/base/20210723T154445/",
        ]
        mock_cloud_interface.remote_open.side_effect = (
            lambda x: self.get_backup_info_file_object()
        )
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        backups = catalog.get_backup_list()
        assert len(backups) == 2
        assert "20210723T133818" in backups
        assert "20210723T154445" in backups
        catalog.remove_backup_from_cache("20210723T154445")
        backups = catalog.get_backup_list()
        assert len(backups) == 1
        assert "20210723T133818" in backups
        assert "20210723T154445" not in backups

    def _verify_wal_is_in_catalog(self, wal_name, wal_path):
        """Create a catalog from the specified wal_path and verify it is listed"""
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [wal_path]
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        wals = catalog.get_wal_paths()
        assert len(wals) == 1
        assert wal_name in wals
        assert wals[wal_name] == wal_path

    @pytest.mark.parametrize(
        ("expected_wal", "wal_path", "suffix"),
        [
            spec
            for spec_group in [
                [
                    # Regular WAL files
                    (
                        "000000010000000000000075",
                        "mt-backups/test-server/wals/0000000100000000/000000010000000000000075",
                        suffix,
                    ),
                    # Backup labels
                    (
                        "000000010000000000000075.00000028.backup",
                        "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.00000028.backup",
                        suffix,
                    ),
                    # Partial WALs
                    (
                        "000000010000000000000075.partial",
                        "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.partial",
                        suffix,
                    ),
                    # History files
                    (
                        "00000001.history",
                        "mt-backups/test-server/wals/0000000100000000/00000001.history",
                        suffix,
                    ),
                ]
                for suffix in ("", ".gz", ".bz2", ".snappy")
            ]
            for spec in spec_group
        ],
    )
    def test_can_list_wals(self, expected_wal, wal_path, suffix):
        """Test the various different WAL files are listed correctly"""
        self._verify_wal_is_in_catalog(
            expected_wal,
            wal_path + suffix,
        )

    def test_ignores_unsupported_compression(self):
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.something",
        ]
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        wals = catalog.get_wal_paths()
        assert len(wals) == 0

    def test_can_remove_a_wal_from_cache(self):
        """Test we can remove a WAL from the cached list"""
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.gz",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000076.gz",
        ]
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        wals = catalog.get_wal_paths()
        assert len(wals) == 2
        assert "000000010000000000000075" in wals
        assert "000000010000000000000076" in wals
        catalog.remove_wal_from_cache("000000010000000000000075")
        wals = catalog.get_wal_paths()
        assert len(wals) == 1
        assert "000000010000000000000075" not in wals
        assert "000000010000000000000076" in wals

    def _get_backup_files(
        self, backup_id, list_bucket_response=[], tablespaces=[], allow_missing=False
    ):
        """
        Helper which creates the necessary mocks for get_backup_files and calls it,
        returning the result.

        This allows tests to pass in a mock response for CloudInterface.list_bucket
        along with any additional tablespaces. Missing file scenarios can be created
        by including tablespaces but not including files for the tablespace in
        list_bucket_response.
        """
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = list_bucket_response
        mock_cloud_interface.path = "mt-backups"
        # Create mock backup info which includes tablespaces
        mock_backup_info = mock.MagicMock(name="backup_info", snapshots_info=None)
        mock_backup_info.backup_id = backup_id
        mock_backup_info.status = "DONE"
        mock_tablespaces = []
        for tablespace in tablespaces:
            mock_tablespace = mock.MagicMock(name="tablespace_%s" % tablespace)
            mock_tablespace.oid = tablespace
            mock_tablespaces.append(mock_tablespace)
        mock_backup_info.tablespaces = mock_tablespaces
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        return catalog.get_backup_files(mock_backup_info, allow_missing=allow_missing)

    def test_can_get_backup_files(self):
        """Test we can get backup file metadata successfully."""
        # GIVEN a backup with one tablespace
        backup_files = self._get_backup_files(
            "20210723T133818",
            # AND the cloud provider returns data.tar with one additional file and
            # the tablespace archive
            list_bucket_response=[
                "mt-backups/test-server/base/20210723T133818/",
                "mt-backups/test-server/base/20210723T133818/data.tar",
                "mt-backups/test-server/base/20210723T133818/data_0000.tar",
                "mt-backups/test-server/base/20210723T133818/16388.tar",
            ],
            tablespaces=[16388],
        )
        # THEN a BackupFileInfo is returned with a path to the data.tar file
        assert (
            backup_files[None].path
            == "mt-backups/test-server/base/20210723T133818/data.tar"
        )
        # AND it has one additional file
        assert len(backup_files[None].additional_files) == 1
        # AND the additional file has a path to data_0000.tar
        assert (
            backup_files[None].additional_files[0].path
            == "mt-backups/test-server/base/20210723T133818/data_0000.tar"
        )
        # AND a BackupFileInfo is returned with a path to the tablespace archive
        assert (
            backup_files[16388].path
            == "mt-backups/test-server/base/20210723T133818/16388.tar"
        )
        # AND it has no additional files
        assert len(backup_files[16388].additional_files) == 0

    def test_get_backup_files_fails_if_missing(self):
        """Test we fail if any backup files are missing."""
        with pytest.raises(SystemExit) as exc:
            # GIVEN a backup with one tablespace
            self._get_backup_files(
                "20210723T133818",
                # AND the cloud provider returns data.tar with one additional file but
                # omits the tablespace archive
                list_bucket_response=[
                    "mt-backups/test-server/base/20210723T133818/",
                    "mt-backups/test-server/base/20210723T133818/data.tar",
                    "mt-backups/test-server/base/20210723T133818/data_0000.tar",
                ],
                tablespaces=[16388],
            )

        # THEN attempting to get files for the backup fails with a SystemExit
        assert exc.value.code == 1

    def test_get_backup_succeeds_with_allow_missing(self):
        """
        Test we can get backup file metadata successfully even if backup files are
        missing if allow_missing=True is used.
        """
        # GIVEN a backup with one tablespace
        backup_files = self._get_backup_files(
            "20210723T133818",
            # AND the cloud provider returns data.tar with one additional file but
            # omits the tablespace archive
            list_bucket_response=[
                "mt-backups/test-server/base/20210723T133818/",
                "mt-backups/test-server/base/20210723T133818/data.tar",
                "mt-backups/test-server/base/20210723T133818/data_0000.tar",
            ],
            tablespaces=[16388],
            # AND allow_missing=True is passed to CloudBackupCatalog
            allow_missing=True,
        )
        # THEN a BackupFileInfo is returned with a path to the data.tar file
        assert (
            backup_files[None].path
            == "mt-backups/test-server/base/20210723T133818/data.tar"
        )
        # AND it has one additional file
        assert len(backup_files[None].additional_files) == 1
        # AND the additional file has a path to data_0000.tar
        assert (
            backup_files[None].additional_files[0].path
            == "mt-backups/test-server/base/20210723T133818/data_0000.tar"
        )
        # AND a BackupFileInfo is returned for the tablespace which has a path of None
        assert backup_files[16388].path is None
        # AND it has no additional files
        assert len(backup_files[16388].additional_files) == 0

    def test_get_backup_succeeds_with_missing_main_file(self):
        """
        Test that additional files are still returned even if the main file is missing
        when allow_missing=True is used.
        """
        # GIVEN a backup with one tablespace
        backup_files = self._get_backup_files(
            "20210723T133818",
            # AND the cloud provider returns data_0000.tar but not the main data.tar
            list_bucket_response=[
                "mt-backups/test-server/base/20210723T133818/",
                "mt-backups/test-server/base/20210723T133818/data_0000.tar",
            ],
            # AND allow_missing=True is passed to CloudBackupCatalog
            allow_missing=True,
        )
        # THEN a BackupFileInfo is returned for data.tar with an empty path
        assert backup_files[None].path is None
        # AND it has one additional file
        assert len(backup_files[None].additional_files) == 1
        # AND the additional file has a path to data_0000.tar
        assert (
            backup_files[None].additional_files[0].path
            == "mt-backups/test-server/base/20210723T133818/data_0000.tar"
        )

    @pytest.fixture
    @mock.patch("barman.cloud.CloudInterface")
    def in_memory_cloud_interface(self, cloud_interface_mock):
        """Create a minimal in-memory CloudInterface implementation"""
        in_memory_object_store = {}

        def upload_fileobj(fileobj, key):
            in_memory_object_store[key] = fileobj.read()

        def remote_open(key):
            try:
                return BytesIO(in_memory_object_store[key])
            except KeyError:
                return None

        def delete_objects(object_list):
            for key in object_list:
                try:
                    del in_memory_object_store[key]
                except KeyError:
                    pass

        def list_bucket(prefix, delimiter="/"):
            for key in in_memory_object_store.keys():
                if len(delimiter) > 0:
                    tokens = key.split(delimiter)
                    if len(tokens) > 1:
                        for i in range(1, len(tokens)):
                            yield delimiter.join(tokens[:i]) + "/"
                yield key

        cloud_interface_mock.upload_fileobj.side_effect = upload_fileobj
        cloud_interface_mock.remote_open.side_effect = remote_open
        cloud_interface_mock.delete_objects.side_effect = delete_objects
        cloud_interface_mock.list_bucket.side_effect = list_bucket

        return cloud_interface_mock

    def test_cloud_backup_catalog_has_keep_manager_capability(
        self, in_memory_cloud_interface
    ):
        """
        Verifies that KeepManagerMixinCloud methods are available in CloudBackupCatalog
        and that they work as expected.

        We deliberately do not test the functionality at a more granular level as
        KeepManagerMixin has its own tests and CloudBackupCatalog adds no extra
        functionality.
        """
        test_backup_id = "20210723T095432"

        in_memory_cloud_interface.path = ""

        # With a catalog using our minimal in-memory CloudInterface
        catalog = CloudBackupCatalog(in_memory_cloud_interface, "test-server")
        # Initially a backup has no annotations and therefore shouldn't be kept
        assert catalog.should_keep_backup(test_backup_id, use_cache=False) is False
        # The target is None because there is no keep annotation
        assert catalog.get_keep_target(test_backup_id, use_cache=False) is None
        # Releasing the keep is a no-op because there is no keep
        catalog.release_keep(test_backup_id)
        # We can add a new keep
        catalog.keep_backup(test_backup_id, KeepManager.TARGET_STANDALONE)
        # Now we have added a keep, the backup manager knows the backup should be kept
        assert catalog.should_keep_backup(test_backup_id) is True
        # We can also see the keep with the cache optimization
        assert catalog.should_keep_backup(test_backup_id, use_cache=True) is True
        # We can also see the recovery target
        assert catalog.get_keep_target(test_backup_id) == KeepManager.TARGET_STANDALONE
        # We can also see the recovery target with the cache optimization
        assert (
            catalog.get_keep_target(test_backup_id, use_cache=True)
            == KeepManager.TARGET_STANDALONE
        )
        # We can release the keep
        catalog.release_keep(test_backup_id)
        # Having released the keep, the backup manager tells us it shouldn't be kept
        assert catalog.should_keep_backup(test_backup_id) is False
        # And the recovery target is None again
        assert catalog.get_keep_target(test_backup_id) is None

    @pytest.fixture
    def catalog_with_named_backup(self, in_memory_cloud_interface):
        backup_infos = {
            "20221107T120000": BytesIO(
                b"""backup_label=None
end_time=2022-11-07 12:05:00
backup_name=named backup
"""
            ),
            "20221109T120000": BytesIO(
                b"""backup_label=None
end_time=2022-11-09 12:05:00
"""
            ),
        }
        in_memory_cloud_interface.path = ""
        for id, backup_info in backup_infos.items():
            in_memory_cloud_interface.upload_fileobj(
                backup_info, "test-server/base/%s/backup.info" % id
            )
        return CloudBackupCatalog(in_memory_cloud_interface, "test-server")

    @pytest.mark.parametrize(
        ("backup_id", "expected_backup_id"),
        (
            # Backup names should resolve to the ID of the backup which has that name
            ("named backup", "20221107T120000"),
            # The backup ID should resolve to itself
            ("20221109T120000", "20221109T120000"),
        ),
    )
    def test_parse_backup_id(
        self, backup_id, expected_backup_id, catalog_with_named_backup
    ):
        # GIVEN a cloud object store with two backups
        # WHEN parse_backup_id is called with a matching backup ID or name
        # THEN the returned backup ID should match the expected backup ID
        assert (
            catalog_with_named_backup.parse_backup_id(backup_id) == expected_backup_id
        )

    def test_parse_backup_id_no_match(self, catalog_with_named_backup):
        # GIVEN a cloud object store with two backups
        # WHEN parse_backup_id is called with a name which does not match
        backup_name = "non-matching name"

        # THEN a ValueError is raised
        with pytest.raises(ValueError) as exc:
            catalog_with_named_backup.parse_backup_id(backup_name)

        # AND the exception message describes the problem
        assert "Unknown backup '%s' for server 'test-server'" % backup_name in str(
            exc.value
        )


class TestCloudTarUploader(object):
    """Tests CloudTarUploader creates valid tar files."""

    @pytest.mark.parametrize(
        "compression",
        # The CloudTarUploader expects the short form compression args set by the
        # cloud_backup argument parser
        (None, "bz2", "gz", "snappy"),
    )
    @mock.patch("barman.cloud.CloudInterface")
    def test_add(self, mock_cloud_interface, compression, tmpdir):
        """
        Verifies that when files are added to the CloudTarUploader tar file
        the bytes passed to async_upload_part represent a valid tar file.
        """
        # GIVEN a cloud interface
        mock_cloud_interface.MIN_CHUNK_SIZE = 5 << 20
        # AND a source directory containing one file
        src_file = "arbitrary_file_name"
        content = "arbitrary strong representing file content"
        key = "arbitrary/path/in/the/cloud"
        with open(os.path.join(str(tmpdir), src_file), "w") as f:
            f.write(content)
        # AND a CloudTarUploader using the configured compression
        uploader = CloudTarUploader(mock_cloud_interface, key, compression=compression)

        # WHEN the file is added to the tar uploader
        uploader.tar.add(
            os.path.join(str(tmpdir), src_file), arcname=src_file, recursive=False
        )
        # AND the uploader is closed, forcing the data to be flushed to the cloud
        uploader.tar.close()
        uploader.close()

        # THEN async_upload_part is called
        mock_cloud_interface.async_upload_part.assert_called_once()
        # AND the body argument of the async_upload_part call contains the source
        # file with the specified compression
        uploaded_tar = mock_cloud_interface.async_upload_part.call_args_list[0][1][
            "body"
        ]
        with open(uploaded_tar.name, "rb") as uploaded_data:
            tar_fileobj = uploaded_data
            if compression is None:
                tar_mode = "r|"
            elif compression == "snappy":
                tar_mode = "r|"
                # We must manually decompress the snappy bytes before extracting
                tar_fileobj = BytesIO()
                snappy.stream_decompress(uploaded_data, tar_fileobj)
                tar_fileobj.seek(0)
            else:
                tar_mode = "r|%s" % compression
            with open_tar(fileobj=tar_fileobj, mode=tar_mode) as tf:
                dest_path = str(tmpdir.mkdir("result"))
                tf.extractall(path=dest_path)
                with open(os.path.join(dest_path, src_file), "r") as result:
                    assert result.read() == content


class TestCloudBackupUploader(object):
    """Tests for the CloudBackupUploader class."""

    server_name = "test_server"

    @mock.patch("barman.cloud.os.stat")
    @mock.patch("barman.cloud.CloudUploadController")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup(
        self,
        mock_backup_info,
        mock_backup_strategy,
        mock_cloud_upload_controller,
        _mock_os_stat,
    ):
        """Test the happy path for backups."""
        # GIVEN a CloudBackupUploader
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2)
        mock_postgres = MagicMock(server_major_version=150000)
        mock_backup_info.return_value.backup_label = "backup_label"
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            99999,
            mock_postgres,
        )

        # AND the backup_info file returns a single config file outside of pgdata
        mock_backup_info.return_value.get_external_config_files.return_value = [
            mock.Mock(
                file_type="ident_file",
                path="/path/to/pg_ident.conf",
            )
        ]

        # AND the backup strategy sets metadata with tablespaces
        def mock_start_backup(backup_info):
            backup_info.pgdata = "/path/to/pgdata"
            backup_info.tablespaces = [
                MagicMock(location="/tbs1", oid=1234),
                MagicMock(location="/path/to/pgdata/tbs2", oid=1235),
            ]

        mock_backup_strategy.return_value.start_backup.side_effect = mock_start_backup

        # WHEN backup is called
        uploader.backup()

        # THEN the expected directories were uploaded
        uploaded_directory_src = [
            call[1]["src"]
            for call in mock_cloud_upload_controller.return_value.upload_directory.call_args_list
        ]
        assert uploaded_directory_src == [
            "/tbs1",
            "/path/to/pgdata/tbs2",
            "/path/to/pgdata",
        ]
        # AND the external config file was uploaded
        uploaded_file_src = [
            call[1]["src"]
            for call in mock_cloud_upload_controller.return_value.add_file.call_args_list
        ]
        assert "/path/to/pg_ident.conf" in uploaded_file_src
        # AND the backup was coordinated with PostgreSQL
        mock_backup_strategy.return_value.start_backup.assert_called_once_with(
            mock_backup_info.return_value
        )
        mock_backup_strategy.return_value.stop_backup.assert_called_once_with(
            mock_backup_info.return_value
        )

    @pytest.mark.parametrize("backup_should_fail", (False, True))
    @mock.patch("barman.cloud.CloudBackupUploader._create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_with_name(
        self,
        mock_backup_info,
        _mock_backup_strategy,
        _mock_backup_data_files,
        _mock_create_upload_controller,
        backup_should_fail,
    ):
        """Verifies backup name is added to backup info if it is set."""
        # GIVEN a CloudBackupUploader with a specified backup_name
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=999999, MIN_CHUNK_SIZE=2)
        mock_postgres = MagicMock()
        mock_backup_info.return_value.backup_label = None
        backup_name = "nyy lbhe onfr"
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            99999,
            mock_postgres,
            backup_name=backup_name,
        )
        uploader.copy_start_time = datetime.datetime.now()

        # WHEN backup is called and it either succeeds or fails
        if backup_should_fail:
            _mock_backup_data_files.side_effect = Exception("failed!")
            with pytest.raises(SystemExit):
                uploader.backup()
        else:
            uploader.backup()

        # THEN the backup_name was set on the backup info
        mock_backup_info.return_value.set_attribute.assert_called_with(
            "backup_name", backup_name
        )

    @pytest.mark.parametrize("backup_should_fail", (False, True))
    @mock.patch("barman.cloud.CloudBackupUploader._create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_with_no_name(
        self,
        mock_backup_info,
        _mock_backup_strategy,
        _mock_backup_data_files,
        _mock_create_upload_controller,
        backup_should_fail,
    ):
        """Verifies backup name is added to backup info if it is set."""
        # GIVEN a CloudBackupUploader with no specified backup_name
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=999999, MIN_CHUNK_SIZE=2)
        mock_postgres = MagicMock()
        mock_backup_info.return_value.backup_label = None
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            99999,
            mock_postgres,
        )
        uploader.copy_start_time = datetime.datetime.now()

        # WHEN backup is called and it either succeeds or fails
        if backup_should_fail:
            _mock_backup_data_files.side_effect = Exception("failed!")
            with pytest.raises(SystemExit):
                uploader.backup()
        else:
            uploader.backup()

        # THEN the backup_name was not set on the backup info
        backup_info_attrs_set = [
            arg[0][0]
            for arg in mock_backup_info.return_value.set_attribute.call_args_list
        ]
        assert not any([attr == "backup_name" for attr in backup_info_attrs_set])


class TestCloudBackupUploaderBarman(object):
    """
    Test the behaviour of CloudBackupUploaderBarman.
    """

    server_name = "test_server"

    @mock.patch("barman.cloud.open")
    @mock.patch("barman.cloud.CloudUploadController")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup(
        self,
        mock_backup_info,
        mock_backup_strategy,
        mock_cloud_upload_controller,
        _mock_open,
    ):
        """Test the happy path for backups."""
        # GIVEN a CloudBackupUploaderBarman
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2)
        backup_id = "backup_id"
        backup_dir = "/path/to/{}/{}".format(self.server_name, backup_id)
        uploader = CloudBackupUploaderBarman(
            self.server_name,
            mock_cloud_interface,
            99999,
            backup_dir,
            backup_id,
        )
        # AND the backup.info has tablespace information
        mock_backup_info.return_value.pgdata = "/path/to/pgdata"
        mock_backup_info.return_value.tablespaces = [
            MagicMock(location="/tbs1", oid=1234),
            MagicMock(location="/path/to/pgdata/tbs2", oid=1235),
        ]

        # WHEN backup is called
        uploader.backup()

        # THEN the expected directories were uploaded
        uploaded_directory_src = [
            call[1]["src"]
            for call in mock_cloud_upload_controller.return_value.upload_directory.call_args_list
        ]
        assert uploaded_directory_src == [
            "/path/to/test_server/backup_id/1234",
            "/path/to/test_server/backup_id/1235",
            "/path/to/test_server/backup_id/data",
        ]
        # AND the backup strategy was not called
        mock_backup_strategy.return_value.start_backup.assert_not_called()
        mock_backup_strategy.return_value.stop_backup.assert_not_called()


class TestCloudBackupSnapshot(object):
    """
    Test the behaviour of barman cloud snapshot backups.
    """

    server_name = "test_server"
    instance_name = "test_instance"
    zone = "test_zone"
    disks = ["disk0", "disk1"]

    @pytest.fixture
    def cloud_interface(self):
        yield mock.Mock(path="path/to/objects")

    @pytest.fixture
    def snapshot_interface(self):
        yield mock.Mock()

    @pytest.fixture
    def mock_postgres(self):
        yield mock.Mock()

    @pytest.mark.parametrize(
        (
            "instance_exists",
            "missing_disks",
            "unmounted_disks",
            "expected_error_msg",
        ),
        [
            (
                False,
                [],
                [],
                "Cannot find compute instance {snapshot_instance}",
            ),
            (
                True,
                ["disk1", "disk2"],
                [],
                "Cannot find disks attached to compute instance {snapshot_instance}: disk1, disk2",
            ),
            (
                True,
                [],
                ["disk1", "disk2"],
                "Cannot find disks mounted on compute instance {snapshot_instance}: disk1, disk2",
            ),
        ],
    )
    @mock.patch("barman.cloud.SnapshotBackupExecutor.find_missing_and_unmounted_disks")
    def test_backup_precondition_failure(
        self,
        mock_find_missing_and_unmounted_disks,
        cloud_interface,
        snapshot_interface,
        mock_postgres,
        instance_exists,
        missing_disks,
        unmounted_disks,
        expected_error_msg,
    ):
        """Verify that the backup fails when preconditions are not met."""
        # GIVEN a CloudBackupSnapshot
        snapshot_backup = CloudBackupSnapshot(
            self.server_name,
            cloud_interface,
            snapshot_interface,
            mock_postgres,
            self.instance_name,
            self.disks,
        )
        # AND the compute instance has the specified state
        snapshot_interface.instance_exists.return_value = instance_exists
        # AND the specified disks are missing or unmounted
        mock_find_missing_and_unmounted_disks.return_value = (
            missing_disks,
            unmounted_disks,
        )

        # WHEN backup is called
        # THEN a BackupPrecondition exception is raised
        with pytest.raises(BackupPreconditionException) as exc:
            snapshot_backup.backup()

        # AND the exception has the expected message
        assert str(exc.value) == expected_error_msg.format(
            **{"snapshot_instance": self.instance_name}
        )

    @mock.patch("barman.cloud.CloudBackup._get_backup_info")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    def test_backup(
        self,
        mock_concurrent_backup_strategy,
        mock_get_backup_info,
        cloud_interface,
        snapshot_interface,
        mock_postgres,
    ):
        """Verify the expected behaviour when a snapshot backup is performed."""
        # GIVEN a CloudBackupSnapshot
        snapshot_backup = CloudBackupSnapshot(
            self.server_name,
            cloud_interface,
            snapshot_interface,
            mock_postgres,
            self.instance_name,
            self.disks[:1],
        )
        # AND the instance exists
        snapshot_interface.instance_exists.return_value = True
        # AND the expected disks are attached and mounted
        mock_volume_metadata = mock.Mock()

        def mock_resolve_mounted_volume(_self):
            mock_volume_metadata.mount_point = "/opt/disk0"
            mock_volume_metadata.mount_options = "rw,noatime"

        mock_volume_metadata.resolve_mounted_volume.side_effect = (
            mock_resolve_mounted_volume
        )
        snapshot_interface.get_attached_volumes.return_value = {
            "disk0": mock_volume_metadata
        }
        # AND a backup strategy which sets a given label
        backup_label = "test_backup_label"
        # AND a known backup_info
        backup_id = "20380119T031408"
        backup_info = BackupInfo(backup_id=backup_id, server_name=self.server_name)
        mock_get_backup_info.return_value = backup_info
        # AND a mock upload_fileobj function which saves the uploaded ubject for later
        # comparison
        uploaded_fileobjs = {}

        def mock_upload_fileobj(value, key):
            value.seek(0)
            uploaded_fileobjs[key] = value.read().decode()

        cloud_interface.upload_fileobj.side_effect = mock_upload_fileobj

        def mock_start_backup(backup_info):
            backup_info.backup_label = backup_label

        mock_concurrent_backup_strategy.return_value.start_backup.side_effect = (
            mock_start_backup
        )

        # AND a mock take_snapshot_backup function which sets snapshot_info
        def mock_take_snapshot_backup(backup_info, _instance_name, disks):
            backup_info.snapshots_info = mock.Mock(
                snapshots=[
                    mock.Mock(
                        identifier="snapshot0",
                        device="/dev/dev0",
                        mount_point=disks["disk0"].mount_point,
                        mount_options=disks["disk0"].mount_options,
                    )
                ]
            )

        snapshot_interface.take_snapshot_backup.side_effect = mock_take_snapshot_backup

        # WHEN backup is called
        snapshot_backup.backup()

        # THEN take_snapshot_backup is called with the expected args
        snapshot_interface.take_snapshot_backup.assert_called_once_with(
            backup_info,
            self.instance_name,
            {"disk0": mock_volume_metadata},
        )
        # AND the backup label was uploaded
        backup_label_key = "{}/{}/base/{}/backup_label".format(
            cloud_interface.path, self.server_name, backup_id
        )
        assert uploaded_fileobjs[backup_label_key] == backup_label

        # AND the backup info contains mount options
        snapshot0_info = backup_info.snapshots_info.snapshots[0]
        assert snapshot0_info.mount_options == "rw,noatime"
        assert snapshot0_info.mount_point == "/opt/disk0"

        # AND the backup info was uploaded
        backup_info_key = "{}/{}/base/{}/backup.info".format(
            cloud_interface.path, self.server_name, backup_id
        )
        assert backup_info_key in uploaded_fileobjs
        with BytesIO() as backup_info_file:
            backup_info.save(file_object=backup_info_file)
            backup_info_file.seek(0)
            assert (
                uploaded_fileobjs[backup_info_key] == backup_info_file.read().decode()
            )
