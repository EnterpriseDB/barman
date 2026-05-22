# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2025
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
import logging
import os
import shutil
import sys
import threading
from argparse import Namespace
from functools import partial
from io import BytesIO
from tarfile import TarFile, TarInfo
from tarfile import open as open_tar
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from unittest import TestCase

import botocore
import mock
import pytest
import snappy
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from azure.identity import (
    AzureCliCredential,
    DefaultAzureCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import PartialBatchErrorException
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, EndpointConnectionError
from google.api_core.exceptions import Conflict, GoogleAPIError
from mock.mock import MagicMock

from barman.annotations import KeepManager
from barman.clients.cloud_cli import NetworkErrorExit, OperationErrorExit
from barman.cloud import (
    DEFAULT_DELIMITER,
    CloudBackupCatalog,
    CloudBackupSnapshot,
    CloudBackupUploader,
    CloudBackupUploaderBarman,
    CloudProviderError,
    CloudTarUploader,
    CloudUploadController,
    CloudUploadingError,
    CloudWalDownloader,
    FileUploadStatistics,
)
from barman.cloud_providers import (
    CloudProviderOptionUnsupported,
    CloudProviderUnsupported,
    ObjectKeyAlreadyExists,
    get_cloud_interface,
    get_cloud_interface_from_server_config,
    recognize_cloud_provider,
    validate_azure_blob_storage_url,
    validate_google_cloud_url,
    validate_s3_url,
)
from barman.cloud_providers.aws_s3 import S3CloudInterface
from barman.cloud_providers.azure_blob_storage import AzureCloudInterface
from barman.cloud_providers.google_cloud_storage import GoogleCloudInterface
from barman.exceptions import (
    BackupPreconditionException,
    BarmanException,
    ConfigurationException,
)
from barman.infofile import BackupInfo, WalFileInfo

if sys.version_info.major > 2:
    from unittest.mock import patch as unittest_patch


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
    elif compression == "lz4":
        import lz4.frame

        dest = BytesIO(lz4.frame.compress(src.read()))
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
        manager = mp.Manager.return_value
        assert mp.Manager.call_count == 1
        assert manager.JoinableQueue.call_count == 1
        assert manager.Queue.call_count == 3
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

    @mock.patch("barman.cloud.CloudInterface._handle_async_errors")
    @mock.patch("barman.cloud.CloudInterface._ensure_async")
    def test_async_upload_part(self, ensure_async_mock, handle_async_errors_mock):
        tmp_file = NamedTemporaryFile(
            delete=False, prefix="barman-upload-", suffix=".part"
        )
        interface = S3CloudInterface(url="s3://bucket/path/to/dir", encryption=None)
        interface.queue = Queue()
        interface.async_upload_part({"UploadId": "upload_id"}, "test/key", tmp_file, 1)
        ensure_async_mock.assert_called_once_with()
        handle_async_errors_mock.assert_called_once_with()
        assert not interface.queue.empty()
        assert interface.queue.get() == {
            "job_type": "upload_part",
            "upload_metadata": {"UploadId": "upload_id"},
            "key": "test/key",
            "body": tmp_file.name,
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

    @pytest.mark.parametrize(
        "test_connectivity, bucket_exists, expected_error, exit_code, err_msg",
        [
            (False, None, NetworkErrorExit, 2, ""),
            (True, None, OperationErrorExit, 1, "Bucket bucket does not exist"),
            (True, True, None, 0, ""),
        ],
    )
    @mock.patch("barman.cloud.CloudInterface")
    def test_verify_cloud_connectivity_and_bucket_existence(
        self,
        mock_cloud_interface,
        test_connectivity,
        bucket_exists,
        expected_error,
        exit_code,
        err_msg,
        caplog,
    ):
        interface = S3CloudInterface(url="s3://bucket/path/to/dir")
        interface.test_connectivity = mock.MagicMock()
        interface.test_connectivity.return_value = test_connectivity
        interface.bucket_exists = bucket_exists

        if expected_error:
            with pytest.raises(expected_error) as exc:
                interface.verify_cloud_connectivity_and_bucket_existence()
            assert exc.value.code == exit_code
        else:
            interface.verify_cloud_connectivity_and_bucket_existence()
        assert err_msg in caplog.text
        interface.test_connectivity.assert_called_once_with()


class TestS3CloudInterface(object):
    """
    Tests which verify backend-specific behaviour of S3CloudInterface.
    """

    @pytest.fixture
    def client_error_factory(self):
        """A factory fixture that creates botocore.exceptions.ClientError objects."""

        def _make_client_error(code, message):
            error_response = {
                "Error": {
                    "Code": code,  # The specific error code
                    "Message": message,
                },
                "ResponseMetadata": {
                    "RequestId": "18688D37F129A2F5",
                    "HTTPStatusCode": 400,
                },
            }
            operation_name = "DeleteObjects"
            return botocore.exceptions.ClientError(
                error_response=error_response, operation_name=operation_name
            )

        return _make_client_error

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
            region_name=None,
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
            region_name=None,
            config=config_mock.return_value,
        )

    @mock.patch("barman.cloud_providers.aws_s3.Config")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_region(self, boto_mock, config_mock):
        # GIVEN an s3 bucket url
        bucket_url = "s3://bucket/path/to/dir"

        # WHEN an S3CloudInterface is created with a specified region
        cloud_interface = S3CloudInterface(
            url=bucket_url, encryption=None, region="us-west-2"
        )

        # THEN the cloud interface region property is set to the specified value
        assert cloud_interface.region == "us-west-2"
        # AND the boto3 resource is created with the specified region_name
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with(
            "s3",
            endpoint_url=None,
            region_name="us-west-2",
            config=config_mock.return_value,
        )

    @pytest.mark.parametrize(
        "addressing_style, endpoint_url, expected_config_call",
        [
            # Test with None (default behavior)
            (None, None, mock.call()),
            # Test with 'auto' addressing style
            ("auto", None, mock.call(s3={"addressing_style": "auto"})),
            # Test with 'virtual' addressing style
            ("virtual", None, mock.call(s3={"addressing_style": "virtual"})),
            # Test with 'path' addressing style (original test case)
            ("path", None, mock.call(s3={"addressing_style": "path"})),
            # Test with virtual addressing + S3-compatible endpoint (primary use case)
            (
                "virtual",
                "https://minio.example.com",
                mock.call(s3={"addressing_style": "virtual"}),
            ),
            # Test with path addressing + S3-compatible endpoint
            (
                "path",
                "https://s3-compatible.example.com",
                mock.call(s3={"addressing_style": "path"}),
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.Config")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_addressing_style(
        self,
        boto_mock,
        config_mock,
        addressing_style,
        endpoint_url,
        expected_config_call,
    ):
        """
        Test S3CloudInterface creation with various addressing_style configurations.

        This test verifies that the addressing_style parameter is correctly passed to
        boto3's Config object when creating an S3CloudInterface. The addressing_style
        parameter allows users to override boto3's default behavior, which is particularly
        important for S3-compatible storage providers (e.g., MinIO) that may require
        virtual-hosted-style addressing even when using non-amazonaws.com endpoints.

        Test scenarios:
        - Default behavior (addressing_style=None)
        - All three valid addressing styles: 'auto', 'virtual', 'path'
        - Combinations with custom endpoint_url for S3-compatible storage
        """
        # GIVEN an s3 bucket url
        bucket_url = "s3://bucket/path/to/dir"

        # WHEN an S3CloudInterface is created with the specified addressing_style
        # and endpoint_url
        cloud_interface = S3CloudInterface(
            url=bucket_url,
            encryption=None,
            addressing_style=addressing_style,
            endpoint_url=endpoint_url,
        )

        # THEN the cloud interface addressing_style property is set correctly
        assert cloud_interface.addressing_style == addressing_style
        # AND the endpoint_url property is set correctly
        assert cloud_interface.endpoint_url == endpoint_url
        # AND a Config is created with the expected arguments
        config_mock.assert_called_once_with(
            **expected_config_call.kwargs if addressing_style else {}
        )
        # AND the boto3 resource is created with the specified endpoint_url
        # and the created Config object
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with(
            "s3",
            endpoint_url=endpoint_url,
            region_name=None,
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
            Fileobj=mock_fileobj,
            Bucket="bucket",
            Key=mock_key,
            ExtraArgs={},
            Config=cloud_interface.config,
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
            Config=cloud_interface.config,
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
            Config=cloud_interface.config,
        )

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface._put_object")
    def test_upload_fileobj_fail_if_exists(self, mock_put_object):
        """
        Test :meth:`upload_fileobj` with ``fail_if_exists=True`` defers to
        :meth:`_put_object`.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        mock_fileobj = mock.MagicMock()
        cloud_interface.upload_fileobj(
            fileobj=mock_fileobj,
            key="path/to/dir",
            override_tags=None,
            fail_if_exists=True,
        )
        mock_put_object.assert_called_once_with(
            fileobj=mock_fileobj,
            key="path/to/dir",
            override_tags=None,
            fail_if_exists=True,
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_put_object(self, boto_mock):
        """
        Tests synchronous file upload with boto3 using _put_object
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"
        cloud_interface._put_object(mock_fileobj, mock_key)

        s3_client.put_object.assert_called_once_with(
            Body=mock_fileobj, Bucket="bucket", Key=mock_key
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
    def test_put_object_with_encryption(
        self, boto_mock, encryption_args, expected_extra_args
    ):
        """
        Tests the ServerSideEncryption and SSEKMSKeyId arguments are provided to boto3
        when uploading a file if encryption args are set on the S3CloudInterface
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", **encryption_args)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"
        cloud_interface._put_object(mock_fileobj, mock_key)

        s3_client.put_object.assert_called_once_with(
            Body=mock_fileobj, Bucket="bucket", Key=mock_key, **expected_extra_args
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
    def test_put_object_with_tags(
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
        cloud_interface._put_object(mock_fileobj, mock_key, override_tags=override_tags)

        s3_client.put_object.assert_called_once_with(
            Body=mock_fileobj, Bucket="bucket", Key=mock_key, Tagging=expected_tagging
        )

    @pytest.mark.parametrize("object_exists", [True, False])
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface.check_object_existence")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_put_object_fail_if_exists_old_boto_version(
        self, boto_mock, mock_check, object_exists
    ):
        """
        Test _put_object with fail_if_exists=True using boto3 versions
        older than 1.35.2, which do not support the IfNoneMatch parameter
        on put_object.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        boto_mock.__version__ = "1.29.0"

        mock_check.return_value = object_exists

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"

        if object_exists:
            with pytest.raises(ObjectKeyAlreadyExists) as excinfo:
                cloud_interface._put_object(mock_fileobj, mock_key, fail_if_exists=True)
                mock_check.assert_called_once_with(mock_key)
            assert str(excinfo.value) == (
                "Object %s already exists in bucket %s"
                % (mock_key, cloud_interface.bucket_name)
            )
        else:
            cloud_interface._put_object(mock_fileobj, mock_key, fail_if_exists=True)
            mock_check.assert_called_once_with(mock_key)
            s3_client.put_object.assert_called_once_with(
                Body=mock_fileobj, Bucket="bucket", Key=mock_key
            )

    @pytest.mark.parametrize("object_exists", [True, False])
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface.check_object_existence")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_put_object_fail_if_exists_new_boto_version(
        self, boto_mock, mock_check, object_exists
    ):
        """
        Test _put_object with fail_if_exists=True using boto3 versions
        1.35.2 and newer, which support the IfNoneMatch parameter
        on put_object.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        boto_mock.__version__ = "1.35.2"

        s3_client.put_object.side_effect = (
            ClientError(
                error_response={
                    "Error": {
                        "Code": "PreconditionFailed",
                        "Message": "At least one of the pre-conditions you specified did not hold",
                    }
                },
                operation_name="PutObject",
            )
            if object_exists
            else None
        )

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"

        if object_exists:
            with pytest.raises(ObjectKeyAlreadyExists) as excinfo:
                cloud_interface._put_object(mock_fileobj, mock_key, fail_if_exists=True)
                mock_check.assert_not_called()
                s3_client.put_object.assert_called_once_with(
                    Body=mock_fileobj, Bucket="bucket", Key=mock_key, IfNoneMatch="*"
                )
            assert str(excinfo.value) == (
                "Object %s already exists in bucket %s"
                % (mock_key, cloud_interface.bucket_name)
            )
        else:
            cloud_interface._put_object(mock_fileobj, mock_key, fail_if_exists=True)
            s3_client.put_object.assert_called_once_with(
                Body=mock_fileobj, Bucket="bucket", Key=mock_key, IfNoneMatch="*"
            )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_put_object_fails_with_conditional_failire(self, mock_boto):
        """
        Test that _put_object retries sending the file again when a
        ConditionalRequestConflict error code is received from S3.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        session_mock = mock_boto.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/dir"

        # Simulate ConditionalRequestConflict error on first call, success on second call
        s3_client.put_object.side_effect = [
            ClientError(
                error_response={
                    "Error": {
                        "Code": "ConditionalRequestConflict",
                        "Message": "Some error message",
                    }
                },
                operation_name="PutObject",
            ),
            None,
        ]

        cloud_interface._put_object(mock_fileobj, mock_key)

        s3_client.put_object.assert_has_calls(
            [mock.call(Body=mock_fileobj, Bucket="bucket", Key=mock_key)] * 2
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

        # Test AccessDenied error
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
            "Bulk deletion of object path/to/object/1 failed with error code: "
            '"AccessDenied", message: "Access Denied"'
        ) in caplog.text

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test__delete_object_partial_failure(
        self, boto_mock, client_error_factory, caplog
    ):
        """
        Test partial failure scenarios when deleting objects from S3 using
        `S3CloudInterface`.
        This test verifies that the `_delete_object` method of `S3CloudInterface`
        correctly handles and logs errors when deletion fails due to various exceptions,
        such as AWS client errors and generic exceptions. It ensures that the
        appropriate exceptions are raised and that error messages are logged for each
        failed deletion attempt.

        Parameters
        ----------
        self : object
            The test class instance.
        boto_mock : unittest.mock.Mock
            Mocked boto3 session and resource.
        client_error_factory : Callable
            Factory function to create AWS client errors.
        caplog : pytest.LogCaptureFixture
            Pytest fixture to capture log output.

        Raises
        ------
        `CloudProviderError`
            If deletion of an object fails due to an AWS client error or a generic
            exception.

        Asserts
        -------
        - The `delete_object` method is called for each key.
        - The correct calls are made to `delete_object` with expected parameters.
        - Error messages are logged for each failed deletion attempt.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        # Test AccessDenied error
        s3_client.delete_object.side_effect = client_error_factory(
            code="AnyOtherError", message="Any other error message"
        )

        for key in mock_keys:
            with pytest.raises(CloudProviderError):
                cloud_interface._delete_object(key)

        s3_client.delete_object.call_count == 2
        s3_client.delete_object.assert_has_calls(
            [
                mock.call(Bucket="bucket", Key="path/to/object/1"),
                mock.call(Bucket="bucket", Key="path/to/object/2"),
            ]
        )
        assert (
            "Deletion of object path/to/object/1 failed with error code: "
            '"AnyOtherError", message: "Any other error message'
        ) in caplog.text
        assert (
            "Deletion of object path/to/object/2 failed with error code: "
            '"AnyOtherError", message: "Any other error message'
        ) in caplog.text
        s3_client.delete_object.reset_mock()
        # Test AccessDenied error
        s3_client.delete_object.side_effect = Exception

        for key in mock_keys:
            with pytest.raises(CloudProviderError):
                cloud_interface._delete_object(key)

        s3_client.delete_object.call_count == 2
        s3_client.delete_object.assert_has_calls(
            [
                mock.call(Bucket="bucket", Key="path/to/object/1"),
                mock.call(Bucket="bucket", Key="path/to/object/2"),
            ]
        )

        assert ("Deletion of object path/to/object/1 failed with error:") in caplog.text
        assert ("Deletion of object path/to/object/2 failed with error:") in caplog.text

    @pytest.mark.parametrize(
        ("code", "message"),
        [
            (
                "MissingContentMD5",
                "Missing required header for this request: Content-Md5.",
            ),
            ("InvalidRequest", "Content-MD5 is missing"),
            ("InvalidRequest", "Missing required header for this request: Content-MD5"),
            (
                "BadDigest",
                "The Content-MD5 you specified did not match what we received",
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface._delete_object")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_botocore_exceptions_ClientError(
        self, boto_mock, mock_delete_obj, client_error_factory, caplog, code, message
    ):
        """
        Test `S3CloudInterface.delete_objects` handling of `botocore.exceptions.ClientError`.
        This test verifies that when a bulk delete operation fails with a specific
        `ClientError` (e.g., 'MissingContentMD5'), the interface falls back to deleting
        objects individually and logs the appropriate message. It also checks that for
        other errors, the exception is propagated and no individual deletions are
        attempted.

        Parameters
        ----------
        boto_mock : unittest.mock.Mock
            Mocked boto3 session and resource.
        mock_delete_obj : unittest.mock.Mock
            Mock for the individual object deletion method.
        client_error_factory : Callable
            Factory to generate botocore.exceptions.ClientError instances.
        caplog : _pytest.logging.LogCaptureFixture
            Pytest fixture to capture log output.

        Raises
        ------
        botocore.exceptions.ClientError
            If the error code is not 'MissingContentMD5', the exception is propagated.

        Asserts
        -------
        - The fallback to individual deletion occurs for 'MissingContentMD5'.
        - The appropriate log message is present.
        - Individual deletions are called for 'MissingContentMD5' error.
        - No individual deletions are called for other errors.
        - The correct exception message is raised for other errors.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = ["path/to/object/1", "path/to/object/2"]

        s3_client.delete_objects.side_effect = client_error_factory(
            code,
            message,
        )

        cloud_interface.delete_objects(mock_keys)

        assert "Bulk delete failed with 'MissingContentMD5'. Falling back to deleting "
        "files individually." in caplog.text

        mock_delete_obj.call_count == 2
        mock_delete_obj.assert_has_calls(
            [mock.call("path/to/object/1"), mock.call("path/to/object/2")]
        )

        mock_delete_obj.reset_mock()
        s3_client.delete_objects.side_effect = client_error_factory(
            code="AnyOtherError", message="Any other error message"
        )

        with pytest.raises(botocore.exceptions.ClientError) as exc:
            cloud_interface.delete_objects(mock_keys)

        assert str(exc.value) == (
            "An error occurred (AnyOtherError) when calling the DeleteObjects "
            "operation: Any other error message"
        )

        mock_delete_obj.assert_not_called()

    @pytest.mark.parametrize("prefix", ["/", "", "/something/prefix"])
    def test_delete_under_prefix_raise_ValueError(self, prefix, caplog):
        """
        Test that attempting to delete all objects under a given prefix raises a
        `ValueError`.
        This test verifies that the `delete_under_prefix` method of `S3CloudInterface`
        raises a `ValueError` when called with a specific prefix, and that the exception
        message matches the expected format.

        Parameters
        ----------
        prefix : str
            The prefix under which deletion is attempted.
        caplog : pytest.LogCaptureFixture
            Pytest fixture for capturing log messages.

        Raises
        ------
        `ValueError`
            If deletion under the specified prefix is not allowed.
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)

        with pytest.raises(ValueError) as exc:
            cloud_interface.delete_under_prefix(prefix)

        assert str(exc.value) == (
            "Deleting all objects under prefix %s is not allowed" % prefix
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_with_s3_bucket_delete(self, boto_mock, caplog):
        """
        Test the `delete_under_prefix` method of `S3CloudInterface` for object deletion
        under a given prefix.

        This test covers two scenarios:
        1. When deletion of an object fails with HTTP status code 400,
           it asserts that a `CloudProviderError` is raised and the appropriate error
           message is logged.
        2. When all objects are deleted successfully (HTTP status code 200),
           it asserts that no error messages are logged.

        Parameters
        ----------
        boto_mock : unittest.mock.Mock
            Mocked boto3 session and resource for simulating S3 interactions.
        caplog : _pytest.logging.LogCaptureFixture
            Pytest fixture for capturing log output.

        Raises
        ------
        `CloudProviderError`
            If any object deletion under the prefix fails with an error code.
        """
        prefix = "/prefix/"
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        bucket = s3_mock.Bucket.return_value

        # Mock the objects under the prefix
        mock_obj1 = mock.MagicMock()
        mock_obj1.key = "prefix/object1"
        mock_obj2 = mock.MagicMock()
        mock_obj2.key = "prefix/object2"
        mock_obj3 = mock.MagicMock()
        mock_obj3.key = "prefix/object3"

        bucket.objects.filter.return_value = [mock_obj1, mock_obj2, mock_obj3]

        # Mock the s3 client for delete_objects calls
        s3_client = s3_mock.meta.client

        # --- Case 1: one object fails with error ---
        s3_client.delete_objects.return_value = {
            "Errors": [
                {
                    "Key": "prefix/object2",
                    "Code": "AccessDenied",
                    "Message": "Access Denied",
                }
            ]
        }

        with pytest.raises(CloudProviderError):
            cloud_interface.delete_under_prefix(prefix)

        assert (
            'Deletion of object prefix/object2 failed with error code: "AccessDenied"'
        ) in caplog.text

        # --- Case 2: all objects succeed ---
        caplog.clear()
        s3_client.delete_objects.return_value = {}  # No `Errors` key means success

        cloud_interface.delete_under_prefix(prefix)

        # no error logs expected
        assert "failed with error code" not in caplog.text

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface._delete_object")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_botocore_exceptions_ClientError(
        self, boto_mock, mock_delete_obj, client_error_factory, caplog
    ):
        """
        Test the `delete_under_prefix` method of `S3CloudInterface` for handling
        `botocore.exceptions.ClientError`.

        This test covers two scenarios:

        1. When a `ClientError` with code "MissingContentMD5" is raised during object
           deletion, the method should fallback to calling `_delete_object` for the
           affected key.
        2. When a `ClientError` with any other error code is raised, the exception
           should be propagated.

        Mocks:
            - `barman.cloud_providers.aws_s3.boto3`: Mocks AWS S3 interactions.
            - `barman.cloud_providers.aws_s3.S3CloudInterface._delete_object`: Mocks the
              fallback deletion method.

        Parameters
        ----------
        boto_mock : MagicMock
            Mocked boto3 module.
        mock_delete_obj : MagicMock
            Mocked `_delete_object` method.
        client_error_factory : Callable
            Factory function to create `ClientError` exceptions.
        caplog : pytest.LogCaptureFixture
            Pytest fixture for capturing log output.

        Asserts
        -------
        - `_delete_object` is called once with the correct key when "MissingContentMD5"
          error occurs.
        - For other error codes, the `ClientError` is raised and `_delete_object` is not
          called.
        """
        # Create cloud interface
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir")
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        bucket = s3_mock.Bucket.return_value

        # --- Case 1: MissingContentMD5 (fallback) ---
        mock_obj = mock.MagicMock()
        mock_obj.key = "some-key"

        # Create a mock filter result that is iterable
        mock_filter_result = [mock_obj]
        bucket.objects.filter.return_value = mock_filter_result

        # Make the first call to list() succeed, but delete_objects raise ClientError
        s3_mock.meta.client.delete_objects.side_effect = client_error_factory(
            code="MissingContentMD5",
            message="Missing required header for this request: Content-Md5.",
        )

        cloud_interface.delete_under_prefix("/prefix1/")

        mock_delete_obj.assert_called_once_with("some-key")

        # --- Case 2: other ClientError (propagate) ---
        caplog.clear()
        mock_delete_obj.reset_mock()

        mock_obj2 = mock.MagicMock()
        mock_obj2.key = "some-key"
        mock_filter_result2 = [mock_obj2]
        bucket.objects.filter.return_value = mock_filter_result2

        s3_mock.meta.client.delete_objects.side_effect = client_error_factory(
            code="AnyOtherError", message="Any other error message"
        )

        with pytest.raises(botocore.exceptions.ClientError) as exc:
            cloud_interface.delete_under_prefix("/prefix1/")

        assert "AnyOtherError" in str(exc.value)
        mock_delete_obj.assert_not_called()

    @pytest.mark.skipif(sys.version_info < (3, 0), reason="Requires Python 3 or higher")
    @pytest.mark.parametrize("compression", (None, "bzip2", "gzip", "snappy", "lz4"))
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
        (
            (None, ""),
            ("bzip2", ".bz2"),
            ("gzip", ".gz"),
            ("snappy", ".snappy"),
            ("lz4", ".lz4"),
        ),
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

    @pytest.mark.parametrize(
        # mock_page_data is a list of tuples of (CommonPrefixes, Contents) values
        # where CommonPrefixes and Contents are lists of the prefixes and keys to
        # be returned when `get` is called on that page.
        ("mock_page_data", "expected_values"),
        (
            # If common prefixes and contents are empty then we expect no items
            # to be returned
            ((([], []),), []),
            # If there are only common prefixes then we expect to see only those
            # prefixes
            (
                [
                    (["/a/common/prefix/", "/another/common/prefix/"], []),
                ],
                ["/a/common/prefix/", "/another/common/prefix/"],
            ),
            # If there are only objects then we expect to see only those objects
            (
                [
                    ([], ["/an/object", "/another/object"]),
                ],
                ["/an/object", "/another/object"],
            ),
            # If there are both prefixes and objects then we expect to see the
            # prefixes, then the objects
            (
                [
                    (
                        ["/a/common/prefix/", "/another/common/prefix/"],
                        ["/an/object", "/another/object"],
                    ),
                ],
                [
                    "/a/common/prefix/",
                    "/another/common/prefix/",
                    "/an/object",
                    "/another/object",
                ],
            ),
            # If there are multiple pages then we expect to see the prefixes then
            # objects for each page
            (
                [
                    (
                        ["/a/common/prefix/", "/another/common/prefix/"],
                        ["/an/object", "/another/object"],
                    ),
                    (
                        ["/a/common/prefix2/", "/another/common/prefix2/"],
                        ["/an/object2", "/another/object2"],
                    ),
                ],
                [
                    "/a/common/prefix/",
                    "/another/common/prefix/",
                    "/an/object",
                    "/another/object",
                    "/a/common/prefix2/",
                    "/another/common/prefix2/",
                    "/an/object2",
                    "/another/object2",
                ],
            ),
        ),
    )
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_list_bucket(self, boto_mock, mock_page_data, expected_values):
        """
        Verify that list_bucket returns bucket content in the expected format.
        """
        # GIVEN a mock s3 bucket with the specified contents
        mock_pages = []

        def get_fun(common_prefixes, contents, msg):
            if msg == "CommonPrefixes":
                return [{"Prefix": p} for p in common_prefixes]
            elif msg == "Contents":
                return [{"Key": o} for o in contents]

        for common_prefixes, contents in mock_page_data:
            mock_page = mock.Mock()
            mock_page.get = partial(get_fun, common_prefixes, contents)
            mock_pages.append(mock_page)

        s3_mock = boto_mock.Session.return_value.resource.return_value
        paginator_mock = s3_mock.meta.client.get_paginator.return_value
        paginator_mock.paginate.return_value = mock_pages

        # AND a cloud interface which uses this bucket
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN list_bucket is called
        # THEN the expected values are returned
        assert [
            key for key in cloud_interface.list_bucket(prefix="", delimiter="/")
        ] == expected_values

        # AND the paginator was called with the expected arguments
        paginator_mock.paginate.assert_called_once_with(
            Bucket="bucket", Prefix="", Delimiter="/"
        )

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface.list_bucket")
    def test_get_prefixes(self, mock_list_bucket):
        """
        Verify get_prefixes only returns prefixes
        """
        # GIVEN a cloud interface which returns a number of prefixes and objects
        mock_list_bucket.return_value = [
            "wals/0000000100000000/",
            "wals/0000000200000001/",
            "wals/00000001.history",
        ]
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN get_prefixes is called for a given prefix
        prefixes = cloud_interface.get_prefixes("wals")

        # THEN the common prefixes are returned and the objects are not
        assert [p for p in prefixes] == [
            "wals/0000000100000000/",
            "wals/0000000200000001/",
        ]

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix(self, boto_mock):
        """Verify delete_under_prefix succeeds."""
        # GIVEN a mock s3 bucket which responds successfully to all deletions
        s3_mock = boto_mock.Session.return_value.resource.return_value
        bucket_mock = s3_mock.Bucket.return_value
        mock_responses = [
            {"ResponseMetadata": {"HTTPStatusCode": 200}},
            {"ResponseMetadata": {"HTTPStatusCode": 200}},
            {"ResponseMetadata": {"HTTPStatusCode": 200}},
        ]
        bucket_mock.objects.filter.return_value.delete.return_value = mock_responses

        # AND an S3CloudInterface to that bucket
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN delete_under_prefix is called with a given prefix
        prefix = "wals/0000000100000001/"
        cloud_interface.delete_under_prefix(prefix)

        # AND the bucket was called with the expected name
        s3_mock.Bucket.assert_called_once_with(cloud_interface.bucket_name)

        # AND the objects were filtered with the prefix
        bucket_mock.objects.filter.assert_called_once_with(Prefix=prefix)

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_errors(self, boto_mock):
        """Verify delete_under_prefix fails if any responses have errors."""
        # GIVEN a mock s3 bucket
        s3_mock = boto_mock.Session.return_value.resource.return_value
        bucket_mock = s3_mock.Bucket.return_value

        # Mock the objects under the prefix
        mock_obj1 = mock.MagicMock()
        mock_obj1.key = "wals/0000000100000001/wal1"
        mock_obj2 = mock.MagicMock()
        mock_obj2.key = "wals/0000000100000001/wal2"
        mock_obj3 = mock.MagicMock()
        mock_obj3.key = "wals/0000000100000001/wal3"

        bucket_mock.objects.filter.return_value = [mock_obj1, mock_obj2, mock_obj3]

        # Mock the s3 client for delete_objects calls
        s3_client = s3_mock.meta.client

        # Mock delete_objects to return an error for one object
        s3_client.delete_objects.return_value = {
            "Errors": [
                {
                    "Key": "wals/0000000100000001/wal2",
                    "Code": "InternalError",
                    "Message": "We encountered an internal error. Please try again.",
                }
            ]
        }

        # AND an S3CloudInterface to that bucket
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN delete_under_prefix is called
        # THEN a CloudProviderError is raised
        with pytest.raises(CloudProviderError):
            cloud_interface.delete_under_prefix("wals/0000000100000001/")

    @pytest.mark.parametrize(
        "prefix",
        (
            # An empty prefix should not be deleted
            "",
            # A prefix which is just "/" should not be deleted
            "/",
            # A prefix without a trailing slash should not be deleted
            "wals/0000000100000000",
        ),
    )
    def test_delete_under_prefix_failures(self, prefix):
        """
        Verify delete_under_prefix will not delete prefixes which would lead to
        deletion of more than intended.
        """
        # GIVEN an S3CloudInterface
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN delete_under_prefix is called with a given prefix
        # THEN a ValueError is raised
        with pytest.raises(ValueError):
            cloud_interface.delete_under_prefix(prefix)

    @pytest.mark.parametrize("object_exists", [False, True])
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_existence(self, boto_mock, object_exists):
        """
        Test ``S3CloudInterface._check_object_existence`` method.
        Verifies that the method correctly checks for the existence of an object
        in an S3 bucket using the ``head_object`` method of the S3 client.
        """
        # Mock the S3 client to raise a ClientError if the object does not exist
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        if not object_exists:
            s3_client.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )

        # GIVEN an S3CloudInterface
        cloud_interface = S3CloudInterface("s3://bucket/test", encryption=None)

        # WHEN _check_object_existence is called with a given object path
        result = cloud_interface.check_object_existence("path/to/object")

        # THEN head_object is called with the expected parameters
        s3_client.head_object.assert_called_once_with(
            Bucket="bucket", Key="path/to/object"
        )
        # AND the return value is as expected
        assert result is object_exists


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
            **default_azure_client_args,
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
            **default_azure_client_args,
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
            **default_azure_client_args,
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
            **default_azure_client_args,
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
        container_client.list_blobs.assert_called_once_with(
            name_starts_with="path/to/blob",
        )
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
        container_client.list_blobs.assert_called_once_with(
            name_starts_with="path/to/blob",
        )
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
        container_client.list_blobs.assert_called_once_with(
            name_starts_with="path/to/blob",
        )
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
    @pytest.mark.parametrize("compression", (None, "bzip2", "gzip", "snappy", "lz4"))
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
        (
            (None, ""),
            ("bzip2", ".bz2"),
            ("gzip", ".gz"),
            ("snappy", ".snappy"),
            ("lz4", ".lz4"),
        ),
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

    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_get_prefixes(self, _container_client_mock):
        """Verify that get_prefixes raises a NotImplementedError"""
        # GIVEN an AzureCloudInterface instance
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        # WHEN get_prefixes is called
        # THEN a NotImplementedError is raised
        with pytest.raises(NotImplementedError):
            cloud_interface.get_prefixes("prefix/")

    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_under_prefix(self, _container_client_mock):
        """Verify that delete_under_prefix raises a NotImplementedError"""
        # GIVEN an AzureCloudInterface instance
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        # WHEN delete_under_prefix is called
        # THEN a NotImplementedError is raised
        with pytest.raises(NotImplementedError):
            cloud_interface.delete_under_prefix("prefix/")


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

    @mock.patch.dict(
        os.environ, {"GOOGLE_CLOUD_UNIVERSE_DOMAIN": "custom.universe.domain"}
    )
    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_universe_domain_from_environment(self, gcs_client_mock):
        """
        Test that the universe domain is properly loaded from GOOGLE_CLOUD_UNIVERSE_DOMAIN
        environment variable and passed to the storage client
        """
        # GIVEN a GoogleCloudInterface instance is created
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test"
        )

        # THEN the storage.Client should be called with the universe_domain in client_options
        gcs_client_mock.assert_called_once_with(
            client_options={"universe_domain": "custom.universe.domain"}
        )

        # AND the cloud interface should be properly initialized
        assert cloud_interface.client == gcs_client_mock.return_value
        assert (
            cloud_interface.container_client
            == gcs_client_mock.return_value.bucket.return_value
        )

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage.Client")
    def test_no_universe_domain_environment(self, gcs_client_mock):
        """
        Test that when GOOGLE_CLOUD_UNIVERSE_DOMAIN is not set, the storage client
        is created without client_options
        """
        # GIVEN a GoogleCloudInterface instance is created without universe domain env var
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/test"
        )

        # THEN the storage.Client should be called with client_options=None
        gcs_client_mock.assert_called_once_with(client_options=None)

        # AND the cloud interface should be properly initialized
        assert cloud_interface.client == gcs_client_mock.return_value
        assert (
            cloud_interface.container_client
            == gcs_client_mock.return_value.bucket.return_value
        )

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

    @mock.patch("barman.cloud_providers.google_cloud_storage._logger")
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

    @mock.patch("barman.cloud_providers.google_cloud_storage._logger")
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

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage")
    def test_get_prefixes(self, _gcs_storage_mock):
        """Verify that get_prefixes raises a NotImplementedError"""
        # GIVEN a GoogleCloudInterface instance
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/"
        )
        # WHEN get_prefixes is called
        # THEN a NotImplementedError is raised
        with pytest.raises(NotImplementedError):
            cloud_interface.get_prefixes("prefix/")

    @mock.patch("barman.cloud_providers.google_cloud_storage.storage")
    def test_delete_under_prefix(self, _gcs_storage_mock):
        """Verify that delete_under_prefix raises a NotImplementedError"""
        # GIVEN a GoogleCloudInterface instance
        cloud_interface = GoogleCloudInterface(
            "https://console.cloud.google.com/storage/browser/barman-test/path/to/object/"
        )
        # WHEN delete_under_prefix is called
        # THEN a NotImplementedError is raised
        with pytest.raises(NotImplementedError):
            cloud_interface.delete_under_prefix("prefix/")


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
            endpoint_url=None,
            aws_profile=None,
            source_url="test-url",
            read_timeout=None,
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
            **extra_args,
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
            ("default", DefaultAzureCredential),
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

    @pytest.mark.parametrize(
        "cloud_provider",
        [
            "aws-s3",
            "azure-blob-storage",
            "google-cloud-storage",
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    @mock.patch("barman.cloud_providers.azure_blob_storage.AzureCloudInterface")
    @mock.patch("barman.cloud_providers.google_cloud_storage.GoogleCloudInterface")
    def test_get_cloud_interface_from_server_config(
        self,
        mock_gcs_cloud_interface,
        mock_azure_cloud_interface,
        mock_s3_cloud_interface,
        cloud_provider,
    ):
        """Test creating a CloudInterface from a server config"""
        url = "http://some-bucket/some/path"
        mock_config = MagicMock(
            url=url,
            parallel_jobs=8,
            aws_profile="some-profile",
            aws_region="us-east-1",
            aws_encryption="AES256",
            aws_sse_kms_key_id=None,
            aws_read_timeout=60,
            cloud_delete_batch_size=20,
        )
        ret = get_cloud_interface_from_server_config(mock_config, cloud_provider, url)
        if cloud_provider == "aws-s3":
            mock_s3_cloud_interface.assert_called_once_with(
                url=url,
                jobs=8,
                profile_name="some-profile",
                region="us-east-1",
                encryption="AES256",
                sse_kms_key_id=None,
                read_timeout=60,
                delete_batch_size=20,
            )
            assert ret == mock_s3_cloud_interface.return_value
        elif cloud_provider == "azure-blob-storage":
            mock_azure_cloud_interface.assert_called_once_with(
                url=url,
                jobs=8,
                delete_batch_size=20,
            )
            assert ret == mock_azure_cloud_interface.return_value
        elif cloud_provider == "google-cloud-storage":
            mock_gcs_cloud_interface.assert_called_once_with(
                url=url,
                jobs=8,
                delete_batch_size=20,
            )
            assert ret == mock_gcs_cloud_interface.return_value

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_get_cloud_interface_from_server_config_with_aws_sse_kms_key_id(
        self, mock_s3_cloud_interface
    ):
        """Test that aws_sse_kms_key_id is passed to S3CloudInterface when aws_encryption is aws:kms"""
        url = "http://some-bucket/some/path"
        mock_config = MagicMock(
            url=url,
            parallel_jobs=8,
            aws_profile="some-profile",
            aws_region="us-east-1",
            aws_encryption="aws:kms",
            aws_sse_kms_key_id="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            aws_read_timeout=60,
            cloud_delete_batch_size=20,
        )
        ret = get_cloud_interface_from_server_config(mock_config, "aws-s3", url)
        mock_s3_cloud_interface.assert_called_once_with(
            url=url,
            jobs=8,
            profile_name="some-profile",
            region="us-east-1",
            encryption="aws:kms",
            sse_kms_key_id="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            read_timeout=60,
            delete_batch_size=20,
        )
        assert ret == mock_s3_cloud_interface.return_value

    @pytest.mark.parametrize(
        ("aws_encryption", "expected_error"),
        [
            (
                None,
                'aws_encryption must be "aws:kms" if aws_sse_kms_key_id is specified',
            ),
            (
                "AES256",
                'aws_encryption must be "aws:kms" if aws_sse_kms_key_id is specified',
            ),
        ],
    )
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_get_cloud_interface_from_server_config_invalid_aws_sse_kms_key_id(
        self, mock_s3_cloud_interface, aws_encryption, expected_error
    ):
        """Test that ConfigurationException is raised when aws_sse_kms_key_id is set without aws:kms encryption"""
        url = "http://some-bucket/some/path"
        mock_config = MagicMock(
            url=url,
            parallel_jobs=8,
            aws_profile="some-profile",
            aws_region="us-east-1",
            aws_encryption=aws_encryption,
            aws_sse_kms_key_id="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            aws_read_timeout=60,
            cloud_delete_batch_size=20,
        )
        with pytest.raises(ConfigurationException) as exc:
            get_cloud_interface_from_server_config(mock_config, "aws-s3", url)
        assert expected_error == str(exc.value)


class TestCloudBackupCatalog(object):
    """
    Tests which verify we can list backups stored in a cloud provider
    """

    def get_backup_info_file_object(self):
        """Minimal backup info"""
        return BytesIO(b"""
            backup_label=None
            end_time=2014-12-22 09:25:27.410470+01:00
            """)

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
                for suffix in ("", ".gz", ".bz2", ".snappy", ".lz4")
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
            "20221107T120000": BytesIO(b"""backup_label=None
                end_time=2022-11-07 12:05:00
                backup_name=named backup
                """),
            "20221109T120000": BytesIO(b"""backup_label=None
                end_time=2022-11-09 12:05:00
                """),
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

    def test__get_backup_id_using_shortcut(self):
        """
        Verify that CloudBackupCatalog._get_backup_id_using_shortcut resolves:
        - first/oldest to the earliest backup ID
        - last/latest to the most recent backup ID
        - last-failed to the most recent FAILED backup ID
        - non-string or unknown shortcuts return None
        """
        dummy_cloud_interface = MagicMock()
        catalog = CloudBackupCatalog(dummy_cloud_interface, "test-server")

        # Set up a backups dictionary with three backups
        backups = {
            "20210723T133818": MagicMock(),
            "20210723T154445": MagicMock(),
            "20210723T154554": MagicMock(),
        }
        backups["20210723T133818"].status = BackupInfo.DONE
        backups["20210723T154445"].status = BackupInfo.FAILED
        backups["20210723T154554"].status = BackupInfo.DONE

        # Override get_backup_list() so we simulate these available backups
        catalog.get_backup_list = lambda: backups

        # Verify the 'first' and 'oldest' shortcuts return the earliest backup
        assert catalog._get_backup_id_using_shortcut("first") == "20210723T133818"
        assert catalog._get_backup_id_using_shortcut("oldest") == "20210723T133818"

        # Verify the 'last' and 'latest' shortcuts return the latest backup
        assert catalog._get_backup_id_using_shortcut("last") == "20210723T154554"
        assert catalog._get_backup_id_using_shortcut("latest") == "20210723T154554"

        # Verify the 'last-failed' shortcut returns the most recent backup with status FAILED
        assert catalog._get_backup_id_using_shortcut("last-failed") == "20210723T154445"

        # Non-string input should return None
        assert catalog._get_backup_id_using_shortcut(123) is None

        # An unrecognized shortcut should return None
        assert catalog._get_backup_id_using_shortcut("name") is None

    def test_get_wal_prefixes(self):
        """Verify the retrieval of common WAL prefixes."""
        # GIVEN a mock cloud interface
        mock_cloud_interface = mock.Mock(path="namespace")
        # AND a catalog which uses that interface
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        # WHEN get_wal_prefixes is called for the catalog
        catalog.get_wal_prefixes()
        # THEN the get_prefixes method of the cloud interface is called with the
        # wal_prefix of the catalog
        mock_cloud_interface.get_prefixes.assert_called_once_with(catalog.wal_prefix)

    @pytest.mark.parametrize(
        ("wal_paths", "expected_result"),
        (
            # Backup names should resolve to the ID of the backup which has that name
            ({}, {}),
            # The backup ID should resolve to itself
            (
                {
                    "0000000100000003000000BA": "server_name/wals/0000000100000003/0000000100000003000000BA.gz",
                    "0000000200000003000000BA": "server_name/wals/0000000200000003/0000000200000003000000BA.gz",
                    "0000000200000003000000FF": "server_name/wals/0000000200000003/0000000200000003000000FF.gz",
                    "0000000300000003000001AB": "server_name/wals/0000000300000003/0000000300000003000001AB.gz",
                    "0000000400000003000000CD": "server_name/wals/0000000400000003/0000000400000003000000CD.gz",
                    "0000000400000003000000FF": "server_name/wals/0000000400000003/0000000400000003000000FF.gz",
                    "0000000500000003000001DE": "server_name/wals/0000000500000003/0000000500000003000001DE.gz",
                },
                {
                    "00000005": WalFileInfo(
                        compression=None,
                        name="0000000500000003000001DE",
                        size=None,
                        time=None,
                    )
                },
            ),
        ),
    )
    @mock.patch("barman.cloud.CloudBackupCatalog.get_wal_paths")
    def test_get_latest_archived_wals_info(
        self, mock_get_wal_paths, wal_paths, expected_result
    ):
        mock_cloud_interface = mock.Mock(path="namespace")
        catalog = CloudBackupCatalog(mock_cloud_interface, "server_name")
        mock_get_wal_paths.return_value = wal_paths

        timelines = catalog.get_latest_archived_wals_info()

        assert timelines.keys() == expected_result.keys()

        if timelines:
            assert timelines["00000005"].name == expected_result["00000005"].name
            assert timelines["00000005"].size == expected_result["00000005"].size
            assert timelines["00000005"].time == expected_result["00000005"].time
            assert (
                timelines["00000005"].compression
                == expected_result["00000005"].compression
            )


class TestCloudTarUploader(object):
    """Tests CloudTarUploader creates valid tar files."""

    @pytest.mark.parametrize(
        "compression",
        # The CloudTarUploader expects the short form compression args set by the
        # cloud_backup argument parser
        (None, "bz2", "gz", "snappy", "lz4"),
    )
    @mock.patch("barman.cloud.CloudInterface")
    def test_add(self, mock_cloud_interface, compression, tmpdir):
        """
        Verifies that when files are added to the CloudTarUploader tar file
        the bytes passed to async_upload_part represent a valid tar file.
        """
        # GIVEN a cloud interface
        # AND a source directory containing one file
        src_file = "arbitrary_file_name"
        content = "arbitrary strong representing file content"
        key = "arbitrary/path/in/the/cloud"
        with open(os.path.join(str(tmpdir), src_file), "w") as f:
            f.write(content)
        # AND a CloudTarUploader using the configured compression
        chunk_size = 5 << 20
        uploader = CloudTarUploader(
            mock_cloud_interface, key, chunk_size=chunk_size, compression=compression
        )

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
            elif compression == "lz4":
                tar_mode = "r|"
                # We must manually decompress the lz4 bytes before extracting
                import lz4.frame

                tar_fileobj = BytesIO()
                tar_fileobj.write(lz4.frame.decompress(uploaded_data.read()))
                tar_fileobj.seek(0)
            else:
                tar_mode = "r|%s" % compression
            with open_tar(fileobj=tar_fileobj, mode=tar_mode) as tf:
                dest_path = str(tmpdir.mkdir("result"))
                tf.extractall(path=dest_path)
                with open(os.path.join(dest_path, src_file), "r") as result:
                    assert result.read() == content
        # AND the supplied chunk_size was set
        assert uploader.chunk_size == chunk_size

    @mock.patch("barman.cloud.CloudInterface")
    def test_buffer_method(self, mock_cloud_interface):
        """
        Verifies that the _buffer method returns a NamedTemporaryFile with the
        expected properties. This test ensures Python 3.14 compatibility after
        the change from staticmethod(partial(...)) to instance method.
        """
        # GIVEN a CloudTarUploader instance
        uploader = CloudTarUploader(
            mock_cloud_interface,
            key="test/key",
            chunk_size=1024,
            compression=None,
        )

        # WHEN _buffer is called
        buffer_file = uploader._buffer()

        # THEN it returns a NamedTemporaryFile
        assert isinstance(buffer_file, _TemporaryFileWrapper)

        # AND the file has the expected prefix and suffix
        assert buffer_file.name.find("barman-upload-") != -1
        assert buffer_file.name.endswith(".part")

        # AND the file exists (delete=False was set)
        assert os.path.exists(buffer_file.name)

        # Cleanup
        buffer_file.close()
        os.unlink(buffer_file.name)

    @pytest.mark.parametrize(
        (
            "max_bandwidth",
            "time_of_last_upload",
            "size_of_last_upload",
            "expected_wait_time",
        ),
        (
            # 10GB/s bandwidth limit, last uploaded 5GB 0.25s ago: wait for 0.25s
            (
                10 << 30,
                datetime.datetime(2023, 10, 9, 16, 43, 59, 750000),
                5 << 30,
                0.25,
            ),
            # 100MB/s bandwidth limit, last uploaded 200MB 1.5s ago: wait for 0.5s
            (
                100 << 20,
                datetime.datetime(2023, 10, 9, 16, 43, 58, 500000),
                200 << 20,
                0.5,
            ),
        ),
    )
    @mock.patch("barman.cloud.time")
    def test_throttle_upload(
        self,
        mock_time,
        max_bandwidth,
        time_of_last_upload,
        size_of_last_upload,
        expected_wait_time,
        caplog,
    ):
        """
        Verifies that throttle will wait for the correct length of time for the
        size and time of the last upload and the specified max_bandwidth.
        """
        # GIVEN two fixed points in time at a half second interval
        mock_times = [
            datetime.datetime(2023, 10, 9, 16, 44, 0, 0),
            datetime.datetime(2023, 10, 9, 16, 44, 0, 500000),
        ]
        # AND a CloudTarUploader with the specified max_bandwidth
        uploader = CloudTarUploader(
            None,
            None,
            None,
            None,
            max_bandwidth,
        )
        # AND the CloudTarUploader last uploaded at the specified time
        uploader.time_of_last_upload = time_of_last_upload
        # AND the CloudTarUploader last uploaded a part of the specified size
        uploader.size_of_last_upload = size_of_last_upload
        # AND with info lvel logging
        caplog.set_level(logging.INFO)

        # WHEN _throttle_upload is called with a given part size
        with mock.patch("barman.cloud.datetime.datetime") as mock_datetime:
            mock_datetime.now.side_effect = mock_times
            part_size = 10 << 30
            uploader._throttle_upload(part_size)

        # THEN time.sleep was called with the expected wait time
        mock_time.sleep.assert_called_once_with(expected_wait_time)
        # AND the expected log messages occurred
        assert (
            f"Uploaded {size_of_last_upload} bytes "
            f"{(mock_times[0] - time_of_last_upload).total_seconds()} seconds ago "
            f"which exceeds limit of {max_bandwidth} bytes/s"
        ) in caplog.text
        assert (
            f"Throttling upload by waiting for {expected_wait_time} seconds"
        ) in caplog.text
        # AND size_of_last_upload is set to the new part size
        assert uploader.size_of_last_upload == part_size
        # AND time_of_last_upload is set to the most recent datetime returned
        assert uploader.time_of_last_upload == mock_times[-1]

    @pytest.mark.parametrize(
        (
            "max_bandwidth",
            "time_of_last_upload",
            "size_of_last_upload",
        ),
        (
            # No time or size of last upload so we do not wait
            (10 << 30, None, None),
            # No time of last upload so we do not wait
            (10 << 20, None, 5 << 20),
            # No size of last upload so we do not wait
            (10 << 30, datetime.datetime(2023, 10, 9, 16, 43, 59, 75000), None),
            # 100MB/s bandwidth limit, last uploaded 100MB 1.5s ago: do not wait
            (
                100 << 20,
                datetime.datetime(2023, 10, 9, 16, 43, 58, 500000),
                100 << 20,
            ),
            # 100MB/s bandwidth limit, last uploaded 10MB 0.9s ago: do not wait
            (
                100 << 20,
                datetime.datetime(2023, 10, 9, 16, 43, 59, 100000),
                10 << 20,
            ),
        ),
    )
    @mock.patch("barman.cloud.time")
    def test_throttle_upload_no_wait(
        self,
        mock_time,
        max_bandwidth,
        time_of_last_upload,
        size_of_last_upload,
        caplog,
    ):
        """
        Verifies that throttle will wait for the correct length of time for the
        size and time of the last upload and the specified max_bandwidth.
        """
        # GIVEN one fixed datetime
        mock_next_time = datetime.datetime(2023, 10, 9, 16, 44, 0, 0)
        # AND a CloudTarUploader with the specified max_bandwidth
        uploader = CloudTarUploader(
            None,
            None,
            None,
            None,
            max_bandwidth,
        )
        # AND the CloudTarUploader last uploaded at the specified time
        uploader.time_of_last_upload = time_of_last_upload
        # AND the CloudTarUploader last uploaded a part of the specified size
        uploader.size_of_last_upload = size_of_last_upload

        # WHEN _throttle_upload is called with a given part size
        with mock.patch("barman.cloud.datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_next_time
            part_size = 10 << 30
            uploader._throttle_upload(part_size)

        # THEN time.sleep was not called
        mock_time.sleep.assert_not_called()
        # AND the log output is empty
        assert caplog.text == ""

        # AND size_of_last_upload is set to the new part size
        assert uploader.size_of_last_upload == part_size
        # AND time_of_last_upload is set to the latest returned datetime
        assert uploader.time_of_last_upload == mock_next_time

    @pytest.mark.parametrize("max_bandwidth", (10 << 20, None))
    @mock.patch("barman.cloud.CloudInterface")
    def test_flush_max_bandwidth(self, mock_cloud_interface, max_bandwidth):
        """Verifies behaviour of flush w.r.t max_bandwidth."""
        # GIVEN a CloudTarUploader with a given max_bandwidth
        uploader = CloudTarUploader(
            mock_cloud_interface,
            None,
            None,
            None,
            max_bandwidth,
        )
        uploader.buffer = MagicMock()
        # WHEN flush is called
        with mock.patch(
            "barman.cloud.CloudTarUploader._throttle_upload"
        ) as mock_throttle:
            uploader.flush()
            # THEN _throttle was not called for a None max_bandwidth
            if max_bandwidth is None:
                mock_throttle.assert_not_called()
            # AND _throttle was called when max_bandwidth was set
            else:
                mock_throttle.assert_called_once()


class TestCloudUploadController(object):
    """Tests for the CloudUploadController class."""

    @pytest.mark.parametrize(
        ("max_archive_size_arg", "max_archive_size_property"),
        ((100, 1000), (100, 1000)),
    )
    @mock.patch("barman.cloud.CloudInterface")
    def test_init_max_archive_size(
        self, mock_cloud_interface, max_archive_size_arg, max_archive_size_property
    ):
        """Test creation of CloudUploadController with max_archive_size values."""
        # GIVEN a mock cloud interface with the specified MAX_ARCHIVE_SIZE value
        # and an arbitrary MIN_CHUNK_SIZE value
        mock_cloud_interface.MAX_ARCHIVE_SIZE = max_archive_size_property
        mock_cloud_interface.MIN_CHUNK_SIZE = 5 << 20

        # WHEN a CloudUploadController is created with the requested max_archive_size
        controller = CloudUploadController(
            mock_cloud_interface, "prefix", max_archive_size_arg, None
        )

        # THEN the max_archive_size is set to the lower of requested max_archive_size
        # and the cloud interface MAX_ARCHIVE_SIZE
        assert controller.max_archive_size == min(
            max_archive_size_arg, max_archive_size_property
        )

    @pytest.mark.parametrize(
        (
            "min_chunk_size_arg",
            "min_chunk_size_property",
            "max_archive_size",
            "expected_chunk_size",
        ),
        (
            # When the supplied min_chunk_size is larger than
            # CloudInterface.MIN_CHUNK_SIZE and larger than the chunk size calculated
            # from max_archive_size and CloudInterface.MAX_CHUNKS_PER_FILE then we
            # expect CloudUploadController.chunk_size to be min_chunk_size.
            (10 << 20, 5 << 20, 1 << 30, 10 << 20),
            # When CloudInterface.MIN_CHUNK_SIZE is larger than the supplied
            # min_chunk_size and larger than the chunk size calculated from
            # max_archive_size and CloudInterface.MAX_CHUNKS_PER_FILE then we
            # expect CloudUploadController.chunk_size to be
            # CloudInterface.MIN_CHUNK_SIZE.
            (5 << 20, 10 << 20, 1 << 30, 10 << 20),
            # When the chunk size calculated from max_archive_size and
            # CloudInterface.MAX_CHUNKS_PER_FILE is larger than the supplied
            # min_chunk_size and CloudInterface.MIN_CHUNK_SIZE then we
            # expect CloudUploadController.chunk_size to be the calculated
            # value.
            (5 << 10, 5 << 10, 1 << 30, 214748),
        ),
    )
    @mock.patch("barman.cloud.CloudInterface")
    def test_init_min_chunk_size(
        self,
        mock_cloud_interface,
        min_chunk_size_arg,
        min_chunk_size_property,
        max_archive_size,
        expected_chunk_size,
    ):
        """Test creation of CloudUploadController with max_archive_size values."""
        # GIVEN a CloudInterface with a specified MIN_CHUNK_SIZE and MAX_ARCHIVE_SIZE
        # and a fixed MAX_CHUNKS_PER_FILE value of 10000
        mock_cloud_interface.MIN_CHUNK_SIZE = min_chunk_size_property
        mock_cloud_interface.MAX_ARCHIVE_SIZE = max_archive_size
        mock_cloud_interface.MAX_CHUNKS_PER_FILE = 10000

        # WHEN a CloudUploadController is created with the requested min_chunk_size
        controller = CloudUploadController(
            mock_cloud_interface,
            "prefix",
            max_archive_size,
            None,
            min_chunk_size=min_chunk_size_arg,
        )

        # THEN the chunk_size is set to the expected value
        assert controller.chunk_size == expected_chunk_size


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
        # GIVEN a CloudBackupUploademock_backup_info.r
        mock_cloud_interface = MagicMock(
            MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2, path="/"
        )
        mock_postgres = MagicMock(server_major_version=150000)
        mock_backup_info.return_value.backup_label = "backup_label"
        mock_backup_info.return_value.backup_id = "backup_id"
        expected_max_archive_size = 99999
        expected_min_chunk_size = 111
        expected_max_bandwidth = 222
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            expected_max_archive_size,
            mock_postgres,
            min_chunk_size=expected_min_chunk_size,
            max_bandwidth=expected_max_bandwidth,
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
        # AND both max_archive_size and min_chunk_size were set on the uploader
        assert uploader.max_archive_size == expected_max_archive_size
        assert uploader.min_chunk_size == expected_min_chunk_size
        # AND max_bandwidth was set on the uploader
        assert uploader.max_bandwidth == expected_max_bandwidth
        # AND The upload controller was created with the expected args
        mock_cloud_upload_controller.assert_called_once_with(
            mock_cloud_interface,
            "/{}/base/backup_id".format(self.server_name),
            expected_max_archive_size,
            None,
            expected_min_chunk_size,
            expected_max_bandwidth,
        )

    @pytest.mark.parametrize("backup_should_fail", (False, True))
    @mock.patch("barman.cloud.CloudBackupUploader.create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_with_name(
        self,
        mock_backup_info,
        _mock_backup_strategy,
        _mock_backup_data_files,
        _mockcreate_upload_controller,
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
    @mock.patch("barman.cloud.CloudBackupUploader.create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_with_no_name(
        self,
        mock_backup_info,
        _mock_backup_strategy,
        _mock_backup_data_files,
        _mockcreate_upload_controller,
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
        mock_cloud_interface = MagicMock(
            MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2, path="/"
        )
        backup_id = "backup_id"
        backup_dir = "/path/to/{}/{}".format(self.server_name, backup_id)
        backup_info_path = "/path/to/backup_info"
        expected_max_archive_size = 99999
        expected_min_chunk_size = 111
        expected_max_bandwidth = 222
        uploader = CloudBackupUploaderBarman(
            self.server_name,
            mock_cloud_interface,
            expected_max_archive_size,
            backup_dir,
            backup_id,
            backup_info_path,
            min_chunk_size=expected_min_chunk_size,
            max_bandwidth=expected_max_bandwidth,
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
        # AND both max_archive_size and min_chunk_size were set on the uploader
        assert uploader.max_archive_size == expected_max_archive_size
        assert uploader.min_chunk_size == expected_min_chunk_size
        # AND max_bandwidth was set on the uploader
        assert uploader.max_bandwidth == expected_max_bandwidth
        # AND The upload controller was created with the expected args
        mock_cloud_upload_controller.assert_called_once_with(
            mock_cloud_interface,
            "/{}/base/{}".format(self.server_name, backup_id),
            expected_max_archive_size,
            None,
            expected_min_chunk_size,
            expected_max_bandwidth,
        )


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


class TestS3ObjectLock(object):
    """
    Tests for Object Lock functionality in S3CloudInterface.
    """

    @pytest.fixture
    def s3_cloud_interface(self):
        """Fixture providing a basic S3CloudInterface for testing."""
        with mock.patch("barman.cloud_providers.aws_s3.boto3"):
            interface = S3CloudInterface(
                url="s3://test-bucket/path/to/dir",
                encryption=None,
                jobs=2,
            )
            yield interface

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    @mock.patch("barman.cloud_providers.aws_s3.datetime")
    def test_check_object_lock_with_retention(
        self, mock_datetime, boto_mock, s3_cloud_interface
    ):
        """Test _check_object_lock detects time-based retention locks."""
        # Mock the head_object response with retention
        retain_date = datetime.datetime(2025, 12, 31, 23, 59, 59)
        s3_cloud_interface.s3.meta.client.head_object.return_value = {
            "ObjectLockRetainUntilDate": retain_date,
        }

        # Mock datetime.now to return a time before the retention date
        mock_now = datetime.datetime(2025, 6, 1, 0, 0, 0)
        mock_datetime.now.return_value = mock_now

        is_locked, reason = s3_cloud_interface._check_object_lock("test-key")

        assert is_locked is True
        assert "locked and can't be deleted until" in reason
        assert str(retain_date) in reason
        s3_cloud_interface.s3.meta.client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key"
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_with_legal_hold(self, boto_mock, s3_cloud_interface):
        """Test _check_object_lock detects legal hold locks."""
        # Mock the head_object response with legal hold
        s3_cloud_interface.s3.meta.client.head_object.return_value = {
            "ObjectLockLegalHoldStatus": "ON",
        }

        is_locked, reason = s3_cloud_interface._check_object_lock("test-key")

        assert is_locked is True
        assert "legal hold" in reason
        s3_cloud_interface.s3.meta.client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key"
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_not_locked(self, boto_mock, s3_cloud_interface):
        """Test _check_object_lock returns False for unlocked objects."""
        # Mock the head_object response with no locks
        s3_cloud_interface.s3.meta.client.head_object.return_value = {}

        is_locked, reason = s3_cloud_interface._check_object_lock("test-key")

        assert is_locked is False
        assert reason is None
        s3_cloud_interface.s3.meta.client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key"
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_access_denied(self, boto_mock, s3_cloud_interface):
        """Test _check_object_lock raises CloudProviderError on access denied."""
        # Mock ClientError with AccessDenied
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        s3_cloud_interface.s3.meta.client.head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        with pytest.raises(CloudProviderError) as exc_info:
            s3_cloud_interface._check_object_lock("test-key")

        assert "Access denied" in str(exc_info.value)

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_forbidden(self, boto_mock, s3_cloud_interface):
        """Test _check_object_lock raises CloudProviderError on Forbidden."""
        # Mock ClientError with Forbidden
        error_response = {"Error": {"Code": "Forbidden", "Message": "Forbidden"}}
        s3_cloud_interface.s3.meta.client.head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        with pytest.raises(CloudProviderError) as exc_info:
            s3_cloud_interface._check_object_lock("test-key")

        assert "Access denied" in str(exc_info.value)

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_other_client_error(self, boto_mock, s3_cloud_interface):
        """Test _check_object_lock raises CloudProviderError on other client errors."""
        # Mock ClientError with a different error code
        error_response = {
            "Error": {"Code": "InternalError", "Message": "Internal Server Error"}
        }
        s3_cloud_interface.s3.meta.client.head_object.side_effect = ClientError(
            error_response, "HeadObject"
        )

        with pytest.raises(CloudProviderError) as exc_info:
            s3_cloud_interface._check_object_lock("test-key")

        assert "Error checking Object Lock" in str(exc_info.value)

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_filter_locked_objects_all_unlocked(self, boto_mock, s3_cloud_interface):
        """Test _filter_locked_objects with all unlocked objects."""
        # Mock _check_object_lock to return unlocked for all objects
        s3_cloud_interface._check_object_lock = mock.Mock(return_value=(False, None))

        objects = ["key1", "key2", "key3"]
        result = s3_cloud_interface._filter_locked_objects(objects, atomic=True)

        assert result == objects
        assert s3_cloud_interface._check_object_lock.call_count == 3

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_filter_locked_objects_atomic_fails_on_locked(
        self, boto_mock, s3_cloud_interface
    ):
        """Test _filter_locked_objects with atomic=True raises on locked object."""

        # Mock _check_object_lock to return locked for second object
        def check_lock_side_effect(key):
            if key == "key2":
                return (True, "Object is locked")
            return (False, None)

        s3_cloud_interface._check_object_lock = mock.Mock(
            side_effect=check_lock_side_effect
        )

        objects = ["key1", "key2", "key3"]

        with pytest.raises(CloudProviderError) as exc_info:
            s3_cloud_interface._filter_locked_objects(objects, atomic=True)

        assert "Deletion aborted" in str(exc_info.value)
        assert "key2 is locked" in str(exc_info.value)
        # Should fail on first locked object, so only 2 checks
        assert s3_cloud_interface._check_object_lock.call_count == 2

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_filter_locked_objects_non_atomic_skips_locked(
        self, boto_mock, s3_cloud_interface, caplog
    ):
        """Test _filter_locked_objects with atomic=False skips locked objects."""

        # Mock _check_object_lock to return locked for second object
        def check_lock_side_effect(key):
            if key == "key2":
                return (True, "Object is locked")
            return (False, None)

        s3_cloud_interface._check_object_lock = mock.Mock(
            side_effect=check_lock_side_effect
        )

        objects = ["key1", "key2", "key3"]

        result = s3_cloud_interface._filter_locked_objects(objects, atomic=False)

        assert result == ["key1", "key3"]
        assert s3_cloud_interface._check_object_lock.call_count == 3
        # Check that warning was logged
        assert "Skipping locked object key2" in caplog.text

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_batch_with_check_locks_enabled(
        self, boto_mock, s3_cloud_interface
    ):
        """Test _delete_objects_batch with check_locks=True filters locked objects."""
        # Mock _filter_locked_objects
        s3_cloud_interface._filter_locked_objects = mock.Mock(
            return_value=["key1", "key3"]
        )

        # Mock successful delete_objects response
        s3_cloud_interface.s3.meta.client.delete_objects.return_value = {}

        paths = ["key1", "key2", "key3"]
        s3_cloud_interface._delete_objects_batch(paths, check_locks=True, atomic=True)

        # Verify _filter_locked_objects was called with correct parameters
        s3_cloud_interface._filter_locked_objects.assert_called_once_with(paths, True)

        # Verify delete_objects was called with only unlocked paths
        s3_cloud_interface.s3.meta.client.delete_objects.assert_called_once()
        call_args = s3_cloud_interface.s3.meta.client.delete_objects.call_args
        assert len(call_args[1]["Delete"]["Objects"]) == 2
        assert call_args[1]["Delete"]["Objects"][0]["Key"] == "key1"
        assert call_args[1]["Delete"]["Objects"][1]["Key"] == "key3"

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_batch_without_check_locks(
        self, boto_mock, s3_cloud_interface
    ):
        """Test _delete_objects_batch with check_locks=False skips filtering."""
        # Mock successful delete_objects response
        s3_cloud_interface.s3.meta.client.delete_objects.return_value = {}

        paths = ["key1", "key2", "key3"]
        s3_cloud_interface._delete_objects_batch(paths, check_locks=False)

        # Verify delete_objects was called with all paths
        s3_cloud_interface.s3.meta.client.delete_objects.assert_called_once()
        call_args = s3_cloud_interface.s3.meta.client.delete_objects.call_args
        assert len(call_args[1]["Delete"]["Objects"]) == 3

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_batch_all_locked_no_deletion(
        self, boto_mock, s3_cloud_interface, caplog
    ):
        """Test _delete_objects_batch when all objects are locked."""
        # Mock _filter_locked_objects to return empty list
        s3_cloud_interface._filter_locked_objects = mock.Mock(return_value=[])
        caplog.set_level(logging.DEBUG)

        paths = ["key1", "key2", "key3"]
        s3_cloud_interface._delete_objects_batch(paths, check_locks=True, atomic=False)

        # Verify no deletion was attempted
        s3_cloud_interface.s3.meta.client.delete_objects.assert_not_called()
        assert "No unlocked objects to delete" in caplog.text

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_with_check_locks_enabled(
        self, boto_mock, s3_cloud_interface
    ):
        """Test delete_under_prefix with check_locks=True filters locked objects."""
        # Create mock objects
        obj1 = mock.Mock()
        obj1.key = "prefix/key1"
        obj2 = mock.Mock()
        obj2.key = "prefix/key2"
        obj3 = mock.Mock()
        obj3.key = "prefix/key3"

        # Mock bucket.objects.filter to return mock objects
        s3_cloud_interface.s3.Bucket.return_value.objects.filter.return_value = [
            obj1,
            obj2,
            obj3,
        ]

        # Mock _filter_locked_objects to skip obj2
        s3_cloud_interface._filter_locked_objects = mock.Mock(return_value=[obj1, obj3])

        # Mock successful delete_objects response
        s3_cloud_interface.s3.meta.client.delete_objects.return_value = {}

        s3_cloud_interface.delete_under_prefix(
            "prefix/", check_locks=True, atomic=False
        )

        # Verify _filter_locked_objects was called
        s3_cloud_interface._filter_locked_objects.assert_called_once_with(
            [obj1, obj2, obj3], False
        )

        # Verify delete_objects was called with only unlocked keys
        s3_cloud_interface.s3.meta.client.delete_objects.assert_called_once()
        call_args = s3_cloud_interface.s3.meta.client.delete_objects.call_args
        assert len(call_args[1]["Delete"]["Objects"]) == 2
        assert call_args[1]["Delete"]["Objects"][0]["Key"] == "prefix/key1"
        assert call_args[1]["Delete"]["Objects"][1]["Key"] == "prefix/key3"

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_without_check_locks(
        self, boto_mock, s3_cloud_interface
    ):
        """Test delete_under_prefix with check_locks=False skips filtering."""
        # Create mock objects
        obj1 = mock.Mock()
        obj1.key = "prefix/key1"
        obj2 = mock.Mock()
        obj2.key = "prefix/key2"

        # Mock bucket.objects.filter to return mock objects
        s3_cloud_interface.s3.Bucket.return_value.objects.filter.return_value = [
            obj1,
            obj2,
        ]

        # Mock successful delete_objects response
        s3_cloud_interface.s3.meta.client.delete_objects.return_value = {}

        s3_cloud_interface.delete_under_prefix("prefix/", check_locks=False)

        # Verify delete_objects was called with all keys
        s3_cloud_interface.s3.meta.client.delete_objects.assert_called_once()
        call_args = s3_cloud_interface.s3.meta.client.delete_objects.call_args
        assert len(call_args[1]["Delete"]["Objects"]) == 2

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_under_prefix_all_locked_no_deletion(
        self, boto_mock, s3_cloud_interface, caplog
    ):
        """Test delete_under_prefix when all objects are locked."""
        # Create mock objects
        obj1 = mock.Mock()
        obj1.key = "prefix/key1"
        caplog.set_level(logging.DEBUG)

        # Mock bucket.objects.filter
        s3_cloud_interface.s3.Bucket.return_value.objects.filter.return_value = [obj1]

        # Mock _filter_locked_objects to return empty list
        s3_cloud_interface._filter_locked_objects = mock.Mock(return_value=[])

        s3_cloud_interface.delete_under_prefix(
            "prefix/", check_locks=True, atomic=False
        )

        # Verify no deletion was attempted
        s3_cloud_interface.s3.meta.client.delete_objects.assert_not_called()
        assert "No unlocked objects to delete under prefix prefix/" in caplog.text

    @mock.patch("barman.cloud_providers.aws_s3.datetime")
    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_check_object_lock_with_expired_retention(
        self, boto_mock, mock_datetime, s3_cloud_interface
    ):
        """Test _check_object_lock returns False when retention date has expired."""
        # Mock the head_object response with an expired retention date
        retain_date = datetime.datetime(
            2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        current_date = datetime.datetime(
            2025, 12, 8, 12, 0, 0, tzinfo=datetime.timezone.utc
        )

        s3_cloud_interface.s3.meta.client.head_object.return_value = {
            "ObjectLockRetainUntilDate": retain_date,
        }

        # Mock datetime.now to return a time after the retention date
        mock_datetime.now.return_value = current_date

        is_locked, reason = s3_cloud_interface._check_object_lock("test-key")

        assert is_locked is False
        assert reason is None
        s3_cloud_interface.s3.meta.client.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key"
        )
        # Verify datetime.now was called with the correct timezone
        mock_datetime.now.assert_called_once_with(retain_date.tzinfo)


@pytest.mark.parametrize(
    ("url", "is_valid"),
    (
        ("s3://my-bucket/my-object", True),
        ("s3://another-bucket/path/to/object", True),
        ("http://my-bucket/my-object", False),
        ("s3:/my-bucket/my-object", False),
        ("s3://", False),
        ("s3://my-bucket", True),
    ),
)
def test_validate_s3_url(url, is_valid):
    """Test the ``validate_s3_url`` function."""
    assert validate_s3_url(url) == is_valid


@pytest.mark.parametrize(
    ("url", "is_valid"),
    (
        ("https://console.cloud.google.com/storage/browser/my-bucket/my-object", True),
        ("gs://my-bucket/my-object", True),
        ("http://my-bucket/my-object", False),
        ("gss://my-bucket/my-object", False),
        ("gs:/my-bucket/my-object", False),
        ("gs://", False),
        ("gs://my-bucket", True),
    ),
)
def test_validate_google_cloud_url(url, is_valid):
    """Test the ``validate_google_cloud_url`` function."""
    assert validate_google_cloud_url(url) == is_valid


@pytest.mark.parametrize(
    ("url", "is_valid"),
    (
        ("https://myaccount.blob.core.windows.net/mycontainer/myblob", True),
        ("https://anotheraccount.blob.core.windows.net/container/blob", True),
        ("https://myaccount.blob.core.windows.com/mycontainer/myblob", False),
        ("https://myaccount.windows.core.net/", False),
        ("https://myaccount.azure.com/container/blob", False),
    ),
)
def test_validate_azure_blob_storage_url(url, is_valid):
    """Test the ``validate_azure_blob_storage_url`` function."""
    assert validate_azure_blob_storage_url(url) == is_valid


@pytest.mark.parametrize(
    ("url", "expected_provider"),
    (
        # AWS S3 URLs
        ("s3://my-bucket/my-object", "aws-s3"),
        ("s3://bucket-name", "aws-s3"),
        # Google Cloud Storage URLs
        ("gs://my-bucket/my-object", "google-cloud-storage"),
        ("gs://bucket-name", "google-cloud-storage"),
        # Azure Blob Storage URLs
        (
            "https://myaccount.blob.core.windows.net/mycontainer/myblob",
            "azure-blob-storage",
        ),
        (
            "https://anotheraccount.blob.core.windows.net/container/blob",
            "azure-blob-storage",
        ),
        # Invalid URLs
        ("http://example.com/bucket", None),
        ("ftp://my-bucket/my-object", None),
        ("s3:/my-bucket/my-object", None),
        ("gs:/my-bucket/my-object", None),
        ("https://myaccount.blob.core.windows.com/mycontainer/myblob", None),
        ("", None),
    ),
)
def test_recognize_cloud_provider(url, expected_provider):
    """Test the ``recognize_cloud_provider`` function."""
    assert recognize_cloud_provider(url) == expected_provider


class TestCloudWalDownloader:
    """
    Tests for the :class:`CloudWalDownloader` class.
    """

    @pytest.mark.parametrize("custom_spool_dir", (None, "/custom/spool/dir"))
    def test__init__(self, custom_spool_dir):
        """Test that the CloudWalDownloader is initialized with the expected args."""
        mock_cloud_interface = MagicMock()
        server_name = "test_server"
        downloader = CloudWalDownloader(
            mock_cloud_interface, server_name, custom_spool_dir
        )
        assert downloader.cloud_interface == mock_cloud_interface
        assert downloader.server_name == server_name
        if custom_spool_dir:
            assert downloader.spool_dir == custom_spool_dir
        else:
            assert downloader.spool_dir == CloudWalDownloader.DEFAULT_SPOOL_DIR

    @mock.patch(
        # No-op, no compression logic is tested here
        "barman.cloud.CloudWalDownloader._remove_compression_suffix",
        new=lambda x: x,
    )
    @mock.patch("barman.cloud.CloudWalDownloader._download_single_wal")
    @mock.patch("barman.cloud.CloudWalDownloader._get_wals_to_download")
    def test_download_wal_no_parallel(
        self, mock_get_wals_to_download, mock_download_single_wal
    ):
        """
        Test successful download of a single WAL file.
        """
        # Prepare mocks
        # Simulate get_wals_to_download returning a single WAL path
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        requested_wal_name = "000000010000000100000001"
        cloud_wal_path = source_dir + requested_wal_name
        mock_get_wals_to_download.return_value = [cloud_wal_path]

        # GIVEN a CloudWalDownloader
        downloader = CloudWalDownloader(mock.Mock(), "test_server")

        # WHEN download_wal is called
        wal_dest = "/restore/path/000000010000000100000001"
        downloader.download_wal(
            requested_wal_name, wal_dest, no_partial=False, parallel=0
        )

        # THEN _download_single_wal is called with the expected args
        mock_download_single_wal.assert_called_once_with(cloud_wal_path, wal_dest)

    @mock.patch(
        # No-op, no compression logic is tested here
        "barman.cloud.CloudWalDownloader._remove_compression_suffix",
        new=lambda self, x: x,
    )
    @mock.patch(
        # No-op, we dont test the spool logic here
        "barman.cloud.CloudWalDownloader._try_to_deliver_from_spool",
        new=lambda x, y, z: False,
    )
    @mock.patch(
        # No-op, we dont test the spool logic here
        "barman.cloud.CloudWalDownloader._ensure_spool_dir_exists",
        new=lambda self: None,
    )
    @mock.patch("barman.cloud.threading.Thread")
    @mock.patch("barman.cloud.CloudWalDownloader._download_single_wal")
    @mock.patch("barman.cloud.CloudWalDownloader._get_wals_to_download")
    def test_download_wal_parallel(
        self, mock_get_wals_to_download, mock_download_single_wal, mock_thread_class
    ):
        """
        Test that download_wal starts parallel downloads for additional WAL files when
        ``parallel > 1``. It should spawn a thread for each additional WAL.
        """
        # Prepare mocks
        # Simulate get_wals_to_download returning multiple WAL paths (requested + 2 more)
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        requested_wal_name = "000000010000000100000001"
        extra_wal_1 = "000000010000000100000002"
        extra_wal_2 = "000000010000000100000003"
        mock_get_wals_to_download.return_value = [
            source_dir + requested_wal_name,
            source_dir + extra_wal_1,
            source_dir + extra_wal_2,
        ]

        # GIVEN a CloudWalDownloader
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/path/to/spool")

        # WHEN download_wal is called with parallel=3
        wal_dest = "/restore/path/000000010000000100000001"
        downloader.download_wal(
            requested_wal_name, wal_dest, no_partial=False, parallel=3
        )

        # THEN a thread is started for each additional WAL file
        # We also assert each thread has been started and joined
        mock_thread_class.assert_has_calls(
            [
                mock.call(
                    target=downloader._download_single_wal,
                    args=(
                        source_dir + extra_wal_1,
                        "/path/to/spool/{}".format(extra_wal_1),
                    ),
                ),
                mock.call().start(),
                mock.call(
                    target=downloader._download_single_wal,
                    args=(
                        source_dir + extra_wal_2,
                        "/path/to/spool/{}".format(extra_wal_2),
                    ),
                ),
                mock.call().start(),
                mock.call().join(),
                mock.call().join(),
            ]
        )

        # AND _download_single_wal is called in the main thread for the requested WAL
        mock_download_single_wal.assert_called_once_with(
            source_dir + requested_wal_name, wal_dest
        )

    @mock.patch(
        # No-op, we dont test the spool logic here
        "barman.cloud.CloudWalDownloader._try_to_deliver_from_spool",
        new=lambda x, y, z: False,
    )
    @mock.patch(
        # No-op, we dont test the spool logic here
        "barman.cloud.CloudWalDownloader._ensure_spool_dir_exists",
        new=lambda self: None,
    )
    @mock.patch("barman.cloud.threading.Thread")
    @mock.patch("barman.cloud.CloudWalDownloader._remove_compression_suffix")
    @mock.patch("barman.cloud.CloudWalDownloader._download_single_wal")
    @mock.patch("barman.cloud.CloudWalDownloader._get_wals_to_download")
    def test_download_wal_parallel_partial_and_compressed(
        self,
        mock_get_wals_to_download,
        mock_download_single_wal,
        mock_remove_compression_suffix,
        mock_thread_class,
    ):
        """
        Test that download_wal starts parallel downloads for additional WAL files when
        ``parallel > 1``. In this case, the requested WALs contains a compressed and
        a partial suffix, which should be removed before saving the file locally.
        """
        # Prepare mocks
        # Simulate get_wals_to_download returning multiple WAL paths (requested + 2 more)
        # Where one is standard, one compressed, one partial and one is a history file
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        requested_wal_name = "000000010000000100000001"
        extra_wal_1 = "000000010000000100000002.gz"
        extra_wal_2 = "000000010000000100000003.partial"
        mock_get_wals_to_download.return_value = [
            source_dir + requested_wal_name,
            source_dir + extra_wal_1,
            source_dir + extra_wal_2,
        ]
        # Mock _remove_compression_suffix the compression of the only compressed WAL file
        mock_remove_compression_suffix.side_effect = lambda x: x.replace(".gz", "")

        # GIVEN a CloudWalDownloader
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/path/to/spool")

        # WHEN download_wal is called with parallel=4
        wal_dest = "/restore/path/000000010000000100000001"
        downloader.download_wal(
            requested_wal_name, wal_dest, no_partial=False, parallel=3
        )

        # THEN a thread is started for each additional WAL file
        # The WALs destination should have the compression and partial suffixes removed
        mock_thread_class.assert_has_calls(
            [
                mock.call(
                    target=downloader._download_single_wal,
                    args=(
                        source_dir + extra_wal_1,
                        "/path/to/spool/000000010000000100000002",
                    ),
                ),
                mock.call().start(),
                mock.call(
                    target=downloader._download_single_wal,
                    args=(
                        source_dir + extra_wal_2,
                        "/path/to/spool/000000010000000100000003",
                    ),
                ),
                mock.call().start(),
                mock.call().join(),
                mock.call().join(),
            ]
        )

        # AND _download_single_wal is called in the main thread for the requested WAL
        mock_download_single_wal.assert_called_once_with(
            source_dir + requested_wal_name, wal_dest
        )

    @mock.patch("barman.cloud.CloudWalDownloader._get_wals_to_download")
    def test_download_wal_not_found(self, mock_get_wals_to_download):
        """
        Test that download_wal raises OperationErrorExit when WAL not found.
        """
        # GIVEN a CloudWalDownloader
        downloader = CloudWalDownloader(mock.Mock(), "test_server")

        # Parameters for the test
        requested_wal_name = "000000010000000100000001"
        wal_dest = "/restore/path/000000010000000100000001"

        # Case 1: No WALs found at all
        # WHEN download_wal is called
        # THEN OperationErrorExit is raised
        mock_get_wals_to_download.return_value = []
        with pytest.raises(OperationErrorExit):
            downloader.download_wal(
                requested_wal_name, wal_dest, no_partial=False, parallel=0
            )

        # Case 2: WALs found but not the requested one
        mock_get_wals_to_download.return_value = [
            "bucket/barman/test_server/wals/0000000100000001/000000010000000100000002"
        ]
        # WHEN download_wal is called
        # THEN OperationErrorExit is raised
        with pytest.raises(OperationErrorExit):
            downloader.download_wal(
                requested_wal_name, wal_dest, no_partial=False, parallel=2
            )

    @pytest.mark.parametrize("parallel", [0, 1])
    def test_get_wals_to_download_no_parallel(self, parallel):
        """
        Test that _get_wals_to_download returns the correct WAL when parallelism
        is disabled i.e. ``parallel=0`` or ``parallel=1``.
        """
        # Prepare mocks
        # Simulate a cloud interface that lists a single WAL file
        # This is the expected behavior when list_bucket is called with the
        # full WAL path as prefix
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_name = "000000010000000100000001"
        wal_dir = "0000000100000001"
        wal_path = "bucket/barman/test_server/wals/{}/{}".format(wal_dir, wal_name)
        mock_cloud_interface.list_bucket.return_value = [wal_path]

        # GIVEN a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with disabled parallelism
        result = downloader._get_wals_to_download(
            wal_name, no_partial=False, parallel=parallel
        )

        # THEN the expected WAL path is returned
        assert result == [wal_path]
        # AND list_bucket was called with the full WAL path as prefix
        mock_cloud_interface.list_bucket.assert_called_once_with(wal_path)

    def test_get_wals_to_download_parallel(self):
        """
        Test that _get_wals_to_download returns multiple WALs when ``parallel > 1``.
        """
        # Prepare mocks
        # Simulate a cloud interface that lists multiple WALs in the same directory
        # This is the expected behavior when list_bucket is called with the
        # WAL directory as prefix
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        requested_wal_name = "000000010000000100000002"
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        wal_paths = [
            source_dir + "000000010000000100000001",
            source_dir + "000000010000000100000002",
            source_dir + "000000010000000100000003",
            source_dir + "000000010000000100000004",
            source_dir + "000000010000000100000005",
        ]
        mock_cloud_interface.list_bucket.return_value = wal_paths

        # GIVEN a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=3
        result = downloader._get_wals_to_download(
            requested_wal_name, no_partial=False, parallel=3
        )

        # THEN the expected WAL paths are returned (requested + the following 2)
        assert result == wal_paths[1:4]
        # AND list_bucket was called with the directory as prefix
        mock_cloud_interface.list_bucket.assert_called_once_with(source_dir)

    @mock.patch("barman.cloud.CloudWalDownloader._validate_wal_path")
    def test_get_wals_to_download_skips_invalid(self, mock_validate_wal_path):
        """
        Test that _get_wals_to_download skips invalid WAL paths.
        """
        # Prepare mocks
        # Simulate a cloud interface that lists a valid WAL and an invalid file
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        requested_wal_name = "000000010000000100000002"
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        valid_wal = source_dir + requested_wal_name
        invalid_file = source_dir + "invalid_file.txt"
        mock_cloud_interface.list_bucket.return_value = [valid_wal, invalid_file]
        # Mock _validate_wal_path to return True for the valid and False for the invalid
        mock_validate_wal_path.side_effect = [True, False]

        # GIVEN a CloudWalDownloader with mixed valid and invalid WAL paths
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=2
        result = downloader._get_wals_to_download(
            requested_wal_name, no_partial=False, parallel=2
        )

        # THEN only the valid WAL path is returned
        assert result == [valid_wal]

        # Assert that _validate_wal_path was called for both paths
        mock_validate_wal_path.assert_has_calls(
            [mock.call(valid_wal, False), mock.call(invalid_file, False)]
        )

    def test_get_wals_to_download_list_bucket_out_of_order(self):
        """
        Test that _get_wals_to_download returns consistent results even when
        list_bucket returns WAL paths out of order.

        The method should always return the requested WAL followed by the next
        N - 1 WALs in sorted order, regardless of the order returned by list_bucket.
        """
        # Prepare mocks
        # Simulate a cloud interface that returns WAL paths out of order
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_dir = "0000000100000001"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        mock_cloud_interface.list_bucket.return_value = [
            source_dir + "000000010000000100000005",
            source_dir + "000000010000000100000002",
            source_dir + "000000010000000100000001",
            source_dir + "000000010000000100000004",
            source_dir + "000000010000000100000003",
        ]

        # AND a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=3
        requested_wal_name = "000000010000000100000002"
        result = downloader._get_wals_to_download(
            requested_wal_name, no_partial=False, parallel=3
        )

        # THEN the result contains the requested WAL + the next 2 WALs in sorted order
        assert result == [
            source_dir + "000000010000000100000002",
            source_dir + "000000010000000100000003",
            source_dir + "000000010000000100000004",
        ]

    def test_get_wals_to_download_empty(self):
        """
        Test that _get_wals_to_download returns empty list when no WALs found.
        """
        # GIVEN a CloudWalDownloader with no WALs in the bucket
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        mock_cloud_interface.list_bucket.return_value = []
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called
        result = downloader._get_wals_to_download(
            "000000010000000000000001", no_partial=False, parallel=1
        )

        # THEN an empty list is returned
        assert result == []

    def test_get_wals_to_download_finds_compressed_wal_with_backup_label(self):
        """
        Test that _get_wals_to_download finds the requested compressed WAL even
        when a sibling ``<wal>.<offset>.backup`` label file lives next to it in
        the bucket.

        Example scenario:
            A user requests 00000001000000030000001A. The bucket holds both
            00000001000000030000001A.gz (the actual WAL) and
            00000001000000030000001A.00000028.backup.gz (the backup label).
            S3 list_bucket returns them sorted, and the backup label sorts
            before the WAL because ``.0`` < ``.g`` in ASCII. The downloader
            must still return the real WAL.
        """
        # GIVEN a cloud bucket holding both the WAL and its sibling .backup label
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_dir = "0000000100000003"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        # sorted() order — the .backup label appears before the .gz WAL
        mock_cloud_interface.list_bucket.return_value = [
            source_dir + "00000001000000030000001A.00000028.backup.gz",
            source_dir + "00000001000000030000001A.gz",
        ]

        # AND a CloudWalDownloader
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called for the WAL
        result = downloader._get_wals_to_download(
            "00000001000000030000001A", no_partial=False, parallel=1
        )

        # THEN the actual WAL is returned, not skipped because of the label
        assert result == [source_dir + "00000001000000030000001A.gz"]

    @mock.patch(
        "barman.cloud.CloudWalDownloader._validate_wal_path",
        new=lambda self, x, no_partial: False,  # always returns false
    )
    def test_get_wals_to_download_exits_early_when_requested_wal_is_invalid(self):
        """
        Test that _get_wals_to_download returns an empty list when the requested
        WAL is invalid (a backup file in this test).

        Example scenario:
            A user requests a WAL like 00000001000000030000001A with parallel
            pre-fetching, but only 00000001000000030000001A.00000028.backup exists in
            cloud storage. The method should exit immediately without returning any
            subsequent WAL files.
        """
        # GIVEN a cloud bucket where the requested WAL only exists as a backup file
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_dir = "0000000100000003"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        mock_cloud_interface.list_bucket.return_value = [
            source_dir + "00000001000000030000001A.00000028.backup",
            source_dir + "00000001000000030000001B",
            source_dir + "00000001000000030000001C",
        ]

        # AND a CloudWalDownloader
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called for the WAL that only exists as backup
        result = downloader._get_wals_to_download(
            "00000001000000030000001A", no_partial=False, parallel=3
        )

        # THEN an empty list is returned (doesn't include the following WALs)
        assert result == []

    def test_get_wals_to_download_mixed_file_types(self):
        """
        Test that _get_wals_to_download correctly handles a directory containing a
        mix of WAL files with different compressions, backup files (including
        compressed), and partial files.

        This is an integration-style test that exercises the real
        :meth:`_validate_wal_path` and :meth:`_remove_compression_suffix` methods
        without mocks.
        """
        # Prepare mocks
        # Simulate a cloud bucket mirroring a realistic WAL directory with mixed
        # file types and compression formats
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_dir = "0000000100000002"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        mock_cloud_interface.list_bucket.return_value = [
            source_dir + "0000000100000002000000D1.gz",
            source_dir + "0000000100000002000000D2",
            source_dir + "0000000100000002000000D3.00000028.backup.zst",
            source_dir + "0000000100000002000000D3.bz2",
            source_dir + "0000000100000002000000D4.xz",
            source_dir + "0000000100000002000000D5.00000028.backup",
            source_dir + "0000000100000002000000D5.lz4",
            source_dir + "0000000100000002000000D6.zst",
            source_dir + "0000000100000002000000D7.partial",
            source_dir + "0000000100000002000000D8.partial.zst",
        ]

        # GIVEN a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=4
        result = downloader._get_wals_to_download(
            "0000000100000002000000D1", no_partial=False, parallel=4
        )

        # THEN backup files are skipped and do not consume count slots, and the
        # result contains the requested WAL + the next 3 valid WALs
        assert result == [
            source_dir + "0000000100000002000000D1.gz",
            source_dir + "0000000100000002000000D2",
            source_dir + "0000000100000002000000D3.bz2",
            source_dir + "0000000100000002000000D4.xz",
        ]

    def test_get_wals_to_download_parallel_exceeds_available(self):
        """
        Test that _get_wals_to_download returns all valid WALs when parallel
        exceeds the number of available files in the directory.
        """
        # Prepare mocks
        # Simulate a bucket with mixed file types and a high parallel value
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        wal_dir = "0000000100000002"
        source_dir = "bucket/barman/test_server/wals/{}/".format(wal_dir)
        mock_cloud_interface.list_bucket.return_value = [
            source_dir + "0000000100000002000000D1.gz",
            source_dir + "0000000100000002000000D2",
            source_dir + "0000000100000002000000D3.00000028.backup.zst",
            source_dir + "0000000100000002000000D3.bz2",
            source_dir + "0000000100000002000000D4.xz",
            source_dir + "0000000100000002000000D5.00000028.backup",
            source_dir + "0000000100000002000000D5.lz4",
            source_dir + "0000000100000002000000D6.zst",
            source_dir + "0000000100000002000000D7.partial",
            source_dir + "0000000100000002000000D8.partial.zst",
        ]

        # GIVEN a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=20 (more than available)
        result = downloader._get_wals_to_download(
            "0000000100000002000000D1", no_partial=False, parallel=20
        )

        # THEN all valid WAL files are returned (backup files excluded)
        assert result == [
            source_dir + "0000000100000002000000D1.gz",
            source_dir + "0000000100000002000000D2",
            source_dir + "0000000100000002000000D3.bz2",
            source_dir + "0000000100000002000000D4.xz",
            source_dir + "0000000100000002000000D5.lz4",
            source_dir + "0000000100000002000000D6.zst",
            source_dir + "0000000100000002000000D7.partial",
            source_dir + "0000000100000002000000D8.partial.zst",
        ]

    def test_get_wals_to_download_history_file_disables_prefetch(self):
        """
        Test that _get_wals_to_download disables prefetching when the requested
        WAL is a history file.

        History files live in the root of the "wals" directory instead of a
        hashed directory, so prefetching is not possible.
        """
        # Prepare mocks
        # Simulate a cloud interface that returns a history
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.path = "bucket/barman"
        history_file = "00000002.history"
        history_path = "bucket/barman/test_server/wals/{}".format(history_file)
        mock_cloud_interface.list_bucket.return_value = [history_path]

        # AND a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _get_wals_to_download is called with parallel=3 for a history file
        result = downloader._get_wals_to_download(
            history_file, no_partial=False, parallel=3
        )

        # THEN only the history file is returned (no prefetching)
        assert result == [history_path]

        # AND list_bucket was called with the full history file path as prefix
        # (not the directory). This indicates that prefetching is disabled
        expected_prefix = "bucket/barman/test_server/wals/{}".format(history_file)
        mock_cloud_interface.list_bucket.assert_called_once_with(expected_prefix)

    @pytest.mark.parametrize(
        ("wal_path", "no_partial", "expected_valid"),
        [
            # Valid WAL segments
            ("000000010000000000000001", False, True),
            ("000000010000000000000001", True, True),
            # Valid compressed WAL segments
            ("000000010000000000000001.gz", False, True),
            ("000000010000000000000001.bz2", True, True),
            # Partial files
            ("000000010000000000000001.partial", False, True),
            ("000000010000000000000001.partial", True, False),
            # Backup files should be skipped
            ("000000010000000000000001.00000028.backup", False, False),
            ("000000010000000000000001.00000028.backup", True, False),
            # Compressed backup files should also be skipped
            ("000000010000000000000001.00000028.backup.zst", False, False),
            ("000000010000000000000001.00000028.backup.gz", True, False),
            # Compressed partial files
            ("000000010000000000000001.partial.zst", False, True),
            ("000000010000000000000001.partial.zst", True, False),
            # History files are valid
            ("00000001.history", False, True),
            ("00000001.history", True, True),
            # Compressed history files are valid
            ("00000001.history.snappy", False, True),
            ("00000001.history.gz", True, True),
            # Invalid/unknown files
            ("invalid_file.txt", False, False),
            ("random_data", False, False),
        ],
    )
    @mock.patch("barman.cloud.CloudWalDownloader._remove_compression_suffix")
    def test_validate_wal_path(
        self, mock_remove_compression_suffix, wal_path, no_partial, expected_valid
    ):
        """
        Test that _validate_wal_path correctly identifies valid/invalid WAL paths.
        """
        # Prepare mocks
        # Mock _remove_compression_suffix to remove compression suffixes
        # from the testing WAL paths
        mock_remove_compression_suffix.side_effect = (
            lambda x: x.replace(".snappy", "")
            .replace(".gz", "")
            .replace(".bz2", "")
            .replace(".zst", "")
        )
        # GIVEN a CloudWalDownloader instance
        mock_cloud_interface = MagicMock()
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _validate_wal_path is called
        result = downloader._validate_wal_path(wal_path, no_partial)

        # THEN the expected validity is returned
        assert result == expected_valid

    @pytest.mark.parametrize(
        ("wal_path", "expected_result"),
        [
            # Uncompressed WAL - no change
            ("000000010000000000000001", "000000010000000000000001"),
            # Compressed WAL segments
            ("000000010000000000000001.gz", "000000010000000000000001"),
            ("000000010000000000000001.bz2", "000000010000000000000001"),
            ("000000010000000000000001.xz", "000000010000000000000001"),
            ("000000010000000000000001.snappy", "000000010000000000000001"),
            ("000000010000000000000001.zst", "000000010000000000000001"),
            ("000000010000000000000001.lz4", "000000010000000000000001"),
            # Full paths with compression
            (
                "/some/path/000000010000000000000001.gz",
                "/some/path/000000010000000000000001",
            ),
            (
                "bucket/barman/server/wals/0000000100000001/000000010000000100000001.bz2",
                "bucket/barman/server/wals/0000000100000001/000000010000000100000001",
            ),
            # History files with compression
            ("00000001.history.gz", "00000001.history"),
            ("00000001.history.zst", "00000001.history"),
            # Backup files with compression
            (
                "000000010000000000000001.00000028.backup.zst",
                "000000010000000000000001.00000028.backup",
            ),
            (
                "000000010000000000000001.00000028.backup.gz",
                "000000010000000000000001.00000028.backup",
            ),
            # Partial files with compression
            (
                "000000010000000000000001.partial.zst",
                "000000010000000000000001.partial",
            ),
            (
                "000000010000000000000001.partial.lz4",
                "000000010000000000000001.partial",
            ),
            # Unknown extensions - no change
            ("000000010000000000000001.unknown", "000000010000000000000001.unknown"),
        ],
    )
    def test_remove_compression_suffix(self, wal_path, expected_result):
        """
        Test that _remove_compression_suffix correctly strips compression extensions.
        """
        # GIVEN a CloudWalDownloader instance
        mock_cloud_interface = MagicMock()
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _remove_compression_suffix is called
        result = downloader._remove_compression_suffix(wal_path)

        # THEN the compression suffix is removed (or path unchanged if no suffix)
        assert result == expected_result

    @mock.patch("barman.cloud.CloudWalDownloader._identify_cloud_compression")
    def test_download_single_wal(self, mock_identify_compression):
        """
        Test that _download_single_wal calls the cloud interface correctly.
        """
        # Prepare mocks
        mock_cloud_interface = MagicMock()
        mock_identify_compression.return_value = "gzip"

        # GIVEN a CloudWalDownloader with the mocked cloud interface
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _download_single_wal is called
        cloud_wal_path = "bucket/barman/test_server/wals/0000000100000001/000000010000000100000001.gz"
        wal_dest = "/restore/path/0000000100000001/000000010000000100000001"
        downloader._download_single_wal(cloud_wal_path, wal_dest)

        # THEN the cloud interface's download_file is called with the expected args
        mock_cloud_interface.download_file.assert_called_once_with(
            cloud_wal_path, wal_dest, "gzip"
        )

    @mock.patch("barman.cloud.sys")
    def test_download_single_wal_python2_compressed_raises(self, mock_sys):
        """
        Test that _download_single_wal raises BarmanException for compressed
        WALs on Python 2.
        """
        # GIVEN Python 2 environment
        mock_sys.version_info = (2, 7, 0)
        mock_cloud_interface = MagicMock()
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _download_single_wal is called with a compressed WAL
        # THEN BarmanException is raised
        with pytest.raises(BarmanException) as exc_info:
            downloader._download_single_wal(
                "/path/000000010000000000000001.gz",
                "/restore/path/000000010000000000000001",
            )
        assert "Python 2.x" in str(exc_info.value)

    @mock.patch(
        "barman.cloud.CloudWalDownloader._identify_cloud_compression",
        new=lambda self, x: None,  # No-op
    )
    @pytest.mark.parametrize(
        "is_main_thread, should_reraise",
        [
            (True, True),  # Main thread should re-raise exceptions
            (False, False),  # Worker thread should not re-raise exceptions
        ],
    )
    @mock.patch("barman.cloud._logger")
    @mock.patch("barman.cloud.threading.current_thread")
    def test_download_single_wal_exception_main_thread_reraises(
        self,
        mock_current_thread,
        mock_logger,
        is_main_thread,
        should_reraise,
    ):
        """
        Test that _download_single_wal ALWAYS logs errors and only re-raises exceptions
        when running on the main thread.
        """
        # Prepare mocks
        # Simulate running on main thread or worker thread based on is_main_thread
        mock_current_thread.return_value = (
            threading.main_thread() if is_main_thread else threading.Thread()
        )

        # GIVEN a cloud interface that raises an exception during download
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.download_file.side_effect = Exception("Download failed")
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _download_single_wal is called on the main thread
        # THEN the exception is re-raised
        if should_reraise:
            with pytest.raises(Exception) as exc_info:
                downloader._download_single_wal(
                    "bucket/barman/test_server/wals/0000000100000001/000000010000000100000001",
                    "/restore/path/000000010000000100000001",
                )
            assert "Download failed" in str(exc_info.value)
        # WHEN _download_single_wal is called on a worker thread
        # THEN the exception is NOT re-raised but is logged
        else:
            downloader._download_single_wal(
                "bucket/barman/test_server/wals/0000000100000001/000000010000000100000001",
                "/restore/path/000000010000000100000001",
            )

        # In any case, the error should be logged
        mock_logger.error.assert_called_once_with(
            "Failure downloading WAL %s to %s: %s"
            % (
                "bucket/barman/test_server/wals/0000000100000001/000000010000000100000001",
                "/restore/path/000000010000000100000001",
                "Download failed",
            )
        )

    @pytest.mark.parametrize(
        ("wal_path", "expected_compression"),
        [
            ("000000010000000000000001.gz", "gzip"),
            ("000000010000000000000001.bz2", "bzip2"),
            ("000000010000000000000001.xz", "xz"),
            ("000000010000000000000001.snappy", "snappy"),
            ("000000010000000000000001.zst", "zstd"),
            ("000000010000000000000001.lz4", "lz4"),
            ("000000010000000000000001", None),
            ("/some/path/000000010000000000000001.gz", "gzip"),
            ("/some/path/000000010000000000000001", None),
        ],
    )
    def test_identify_cloud_compression(self, wal_path, expected_compression):
        """
        Test that _identify_cloud_compression correctly identifies compression
        based on file extension.
        """
        # GIVEN a CloudWalDownloader instance
        mock_cloud_interface = MagicMock()
        downloader = CloudWalDownloader(mock_cloud_interface, "test_server")

        # WHEN _identify_cloud_compression is called
        result = downloader._identify_cloud_compression(wal_path)

        # THEN the expected compression is returned
        assert result == expected_compression

    @mock.patch("barman.cloud.shutil.move")
    @mock.patch("barman.cloud.os.path.isfile")
    def test_try_to_deliver_from_spool_success(self, mock_isfile, mock_move):
        """
        Test that _try_to_deliver_from_spool moves WAL from spool and returns True.
        """
        # GIVEN a CloudWalDownloader with a WAL file in the spool directory
        mock_isfile.return_value = True
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/spool/dir")

        # WHEN _try_to_deliver_from_spool is called
        wal_name = "000000010000000100000001"
        destination = "/restore/path/000000010000000100000001"
        result = downloader._try_to_deliver_from_spool(wal_name, destination)

        # THEN the file is moved and True is returned
        assert result is True
        mock_isfile.assert_called_once_with("/spool/dir/000000010000000100000001")
        mock_move.assert_called_once_with(
            "/spool/dir/000000010000000100000001", destination
        )

    @mock.patch("barman.cloud.os.path.isfile")
    def test_try_to_deliver_from_spool_not_found(self, mock_isfile):
        """
        Test that _try_to_deliver_from_spool returns False when WAL not in spool.
        """
        # GIVEN a CloudWalDownloader with no WAL file in the spool directory
        mock_isfile.return_value = False
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/spool/dir")

        # WHEN _try_to_deliver_from_spool is called
        wal_name = "000000010000000100000001"
        destination = "/restore/path/000000010000000100000001"
        result = downloader._try_to_deliver_from_spool(wal_name, destination)

        # THEN False is returned and no move is attempted
        assert result is False
        mock_isfile.assert_called_once_with("/spool/dir/000000010000000100000001")

    @mock.patch("barman.cloud.shutil.move")
    @mock.patch("barman.cloud.os.path.isfile")
    def test_try_to_deliver_from_spool_move_error(self, mock_isfile, mock_move):
        """
        Test that _try_to_deliver_from_spool exits with code 2 when move fails.
        """
        # GIVEN a CloudWalDownloader where shutil.move raises an OSError
        mock_isfile.return_value = True
        mock_move.side_effect = OSError("Permission denied")
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/spool/dir")

        # WHEN _try_to_deliver_from_spool is called
        # THEN SystemExit with code 2 is raised
        wal_name = "000000010000000100000001"
        destination = "/restore/path/000000010000000100000001"
        with pytest.raises(SystemExit) as exc_info:
            downloader._try_to_deliver_from_spool(wal_name, destination)
        assert exc_info.value.code == 2

    @mock.patch("barman.cloud.os.makedirs")
    @mock.patch("barman.cloud.os.path.exists")
    def test_ensure_spool_dir(self, mock_exists, mock_makedirs):
        """
        Test that _ensure_spool_dir_exists creates directory when it doesn't exist.
        """
        # GIVEN a CloudWalDownloader where spool directory does not exist
        mock_exists.return_value = False
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/spool/dir")

        # WHEN _ensure_spool_dir_exists is called
        downloader._ensure_spool_dir_exists()

        # THEN os.makedirs is called with the spool directory
        mock_exists.assert_called_once_with("/spool/dir")
        mock_makedirs.assert_called_once_with("/spool/dir")

    @mock.patch("barman.cloud.os.makedirs")
    @mock.patch("barman.cloud.os.path.exists")
    def test_ensure_spool_dir_exists_makedirs_error(self, mock_exists, mock_makedirs):
        """
        Test that _ensure_spool_dir_exists exits with code 2 when makedirs fails.
        """
        # GIVEN a CloudWalDownloader where os.makedirs raises an OSError
        mock_exists.return_value = False
        mock_makedirs.side_effect = OSError("Permission denied")
        downloader = CloudWalDownloader(mock.Mock(), "test_server", "/spool/dir")

        # WHEN _ensure_spool_dir_exists is called
        # THEN SystemExit with code 2 is raised
        with pytest.raises(SystemExit) as exc_info:
            downloader._ensure_spool_dir_exists()
        assert exc_info.value.code == 2


class TestCoordinateBackupStartedUpload(object):
    """Tests that coordinate_backup uploads backup.info with STARTED status."""

    server_name = "test_server"

    @mock.patch("barman.cloud.CloudBackupUploader.create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_info_uploaded_with_started_before_data(
        self,
        mock_backup_info,
        mock_backup_strategy,
        _mock_backup_data_files,
        _mock_create_upload_controller,
    ):
        """
        Verify that backup.info is uploaded with status=STARTED immediately
        after _start_backup(), before any data files are copied.
        """
        # GIVEN a CloudBackupUploader
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2)
        mock_postgres = MagicMock(server_major_version=150000)
        mock_backup_info.return_value.backup_label = None
        mock_backup_info.return_value.backup_id = "20250101T120000"
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            99999,
            mock_postgres,
        )

        # Configure BackupInfo class-level constants to real strings so that
        # cloud.py code such as ``BackupInfo.STARTED`` resolves correctly when
        # the class is patched.
        mock_backup_info.STARTED = BackupInfo.STARTED
        mock_backup_info.DONE = BackupInfo.DONE

        upload_calls = []

        # Wire set_attribute to actually update the mock's attribute so that
        # status changes made via set_attribute() are observable.
        def fake_set_attribute(attr, value):
            setattr(mock_backup_info.return_value, attr, value)

        mock_backup_info.return_value.set_attribute.side_effect = fake_set_attribute

        # Track each upload_fileobj call with the status at the time of the call
        def record_upload(fileobj, key):
            if "backup.info" in key:
                upload_calls.append(mock_backup_info.return_value.status)

        mock_cloud_interface.upload_fileobj.side_effect = record_upload

        # WHEN backup is called
        uploader.backup()

        # THEN backup.info was uploaded at least twice
        assert len(upload_calls) >= 2, (
            "Expected backup.info to be uploaded at least twice "
            "(once with STARTED, once with final status)"
        )
        # AND the first upload was with status STARTED
        assert upload_calls[0] == BackupInfo.STARTED, (
            "Expected first backup.info upload to have status STARTED, "
            "got %s" % upload_calls[0]
        )

    @mock.patch("barman.cloud.CloudBackupUploader.create_upload_controller")
    @mock.patch("barman.cloud.CloudBackupUploader._backup_data_files")
    @mock.patch("barman.cloud.ConcurrentBackupStrategy")
    @mock.patch("barman.cloud.BackupInfo")
    def test_backup_info_uploaded_with_failed_on_error(
        self,
        mock_backup_info,
        mock_backup_strategy,
        mock_backup_data_files,
        _mock_create_upload_controller,
    ):
        """
        Verify that backup.info is uploaded with status=FAILED in the finally
        block when an error occurs after the initial STARTED upload.
        """
        # GIVEN a CloudBackupUploader where _backup_data_files raises an error
        mock_cloud_interface = MagicMock(MAX_ARCHIVE_SIZE=99999, MIN_CHUNK_SIZE=2)
        mock_postgres = MagicMock(server_major_version=150000)
        mock_backup_info.return_value.backup_label = None
        mock_backup_info.return_value.backup_id = "20250101T120000"
        uploader = CloudBackupUploader(
            self.server_name,
            mock_cloud_interface,
            99999,
            mock_postgres,
        )

        # Configure the patched BackupInfo class to use real status string constants
        # so that cloud.py code such as ``BackupInfo.FAILED`` resolves to 'FAILED'
        # rather than a MagicMock attribute.
        mock_backup_info.STARTED = BackupInfo.STARTED
        mock_backup_info.FAILED = BackupInfo.FAILED
        mock_backup_info.DONE = BackupInfo.DONE

        upload_calls = []

        def mock_start_backup(backup_info):
            backup_info.status = BackupInfo.STARTED

        mock_backup_strategy.return_value.start_backup.side_effect = mock_start_backup
        mock_backup_data_files.side_effect = Exception("upload error")

        # Make set_attribute actually update the mock's attribute so that
        # handle_backup_errors(…, FAILED) is reflected when the finally block
        # calls _upload_backup_info().
        def mock_set_attribute(name, value):
            setattr(mock_backup_info.return_value, name, value)

        mock_backup_info.return_value.set_attribute.side_effect = mock_set_attribute

        def record_upload(fileobj, key):
            if "backup.info" in key:
                upload_calls.append(mock_backup_info.return_value.status)

        mock_cloud_interface.upload_fileobj.side_effect = record_upload

        # WHEN backup is called, it raises SystemExit due to the error
        with pytest.raises(SystemExit):
            uploader.backup()

        # THEN backup.info was uploaded at least twice
        assert len(upload_calls) >= 2
        # AND the first upload had status STARTED
        assert upload_calls[0] == BackupInfo.STARTED
        # AND the final upload had status FAILED
        assert upload_calls[-1] == BackupInfo.FAILED


class TestGetBackupIdUsingShortcutSkipsStarted(object):
    """Tests that _get_backup_id_using_shortcut skips STARTED backups."""

    def _make_catalog(self, backups_by_id):
        """Return a CloudBackupCatalog with a mocked get_backup_list."""
        catalog = CloudBackupCatalog(MagicMock(), "test-server")
        catalog.get_backup_list = lambda: backups_by_id
        return catalog

    def _backup(self, status):
        b = MagicMock()
        b.status = status
        return b

    def test_last_skips_started_backup(self):
        """last/latest must skip in-progress backups and return latest DONE."""
        backups = {
            "20250101T100000": self._backup(BackupInfo.DONE),
            "20250101T110000": self._backup(BackupInfo.DONE),
            "20250101T120000": self._backup(BackupInfo.STARTED),
        }
        catalog = self._make_catalog(backups)
        assert catalog._get_backup_id_using_shortcut("last") == "20250101T110000"
        assert catalog._get_backup_id_using_shortcut("latest") == "20250101T110000"

    def test_first_skips_started_backup(self):
        """first/oldest must skip in-progress backups and return oldest DONE."""
        backups = {
            "20250101T100000": self._backup(BackupInfo.STARTED),
            "20250101T110000": self._backup(BackupInfo.DONE),
            "20250101T120000": self._backup(BackupInfo.DONE),
        }
        catalog = self._make_catalog(backups)
        assert catalog._get_backup_id_using_shortcut("first") == "20250101T110000"
        assert catalog._get_backup_id_using_shortcut("oldest") == "20250101T110000"

    def test_last_returns_none_when_all_started(self):
        """last/latest returns None if all backups are STARTED."""
        backups = {
            "20250101T100000": self._backup(BackupInfo.STARTED),
            "20250101T110000": self._backup(BackupInfo.STARTED),
        }
        catalog = self._make_catalog(backups)
        assert catalog._get_backup_id_using_shortcut("last") is None
        assert catalog._get_backup_id_using_shortcut("latest") is None

    def test_first_returns_none_when_all_started(self):
        """first/oldest returns None if all backups are STARTED."""
        backups = {
            "20250101T100000": self._backup(BackupInfo.STARTED),
        }
        catalog = self._make_catalog(backups)
        assert catalog._get_backup_id_using_shortcut("first") is None
        assert catalog._get_backup_id_using_shortcut("oldest") is None

    def test_last_failed_not_affected_by_started_filter(self):
        """last-failed continues to return the most recent FAILED backup."""
        backups = {
            "20250101T100000": self._backup(BackupInfo.FAILED),
            "20250101T110000": self._backup(BackupInfo.STARTED),
        }
        catalog = self._make_catalog(backups)
        assert catalog._get_backup_id_using_shortcut("last-failed") == "20250101T100000"
