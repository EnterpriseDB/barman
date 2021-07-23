# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

import datetime
import os
from io import BytesIO
from azure.core.exceptions import ServiceRequestError

import mock
from mock.mock import MagicMock
import pytest
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, EndpointConnectionError

from barman.cloud import CloudBackupCatalog, CloudUploadingError, FileUploadStatistics
from barman.cloud_providers.aws_s3 import S3CloudInterface
from barman.cloud_providers.azure_blob_storage import AzureCloudInterface

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


class TestCloudInterface(object):
    """
    Tests of the asychronous upload infrastructure in CloudInterface.
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

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_uploader_minimal(self, boto_mock):
        cloud_interface = S3CloudInterface(
            url="s3://bucket/path/to/dir", encryption=None
        )

        assert cloud_interface.bucket_name == "bucket"
        assert cloud_interface.path == "path/to/dir"
        boto_mock.Session.assert_called_once_with(profile_name=None)
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with("s3", endpoint_url=None)
        assert cloud_interface.s3 == session_mock.resource.return_value

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
        # Expect the create() metod of the bucket object to be called
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

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_upload_fileobj_with_encryption(self, boto_mock):
        """
        Tests the ServerSideEncryption argument is provided to boto3 when uploading
        a file if encryption is set on the S3CloudInterface.
        """
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir", encryption="aws:kms"
        )
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
            ExtraArgs={"ServerSideEncryption": "aws:kms"},
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

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_create_multipart_upload_with_encryption(self, boto_mock):
        """
        Tests the ServerSideEncryption argument is provided to boto3 when creating
        a multipart upload if encryption is set on the S3CloudInterface
        """
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir", encryption="aws:kms"
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_key = "path/to/dir"
        cloud_interface.create_multipart_upload(mock_key)

        s3_client.create_multipart_upload.assert_called_once_with(
            Bucket="bucket", Key=mock_key, ServerSideEncryption="aws:kms"
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


class TestAzureCloudInterface(object):
    """
    Tests which verify backend-specific behaviour of AzureCloudInterface.
    """

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "AZURE_STORAGE_SAS_TOKEN": "sas_token",
            "AZURE_STORAGE_KEY": "storage_key",
        },
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_uploader_minimal(self, blob_service_mock):
        """Connection string auth takes precedence over SAS token or shared token"""
        container_name = "container"
        account_url = "https://storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        blob_service_mock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )
        get_container_client_mock = (
            blob_service_mock.from_connection_string.return_value.get_container_client
        )
        get_container_client_mock.assert_called_once_with(container_name)
        assert (
            cloud_interface.container_client == get_container_client_mock.return_value
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_uploader_sas_token_auth(self, blob_service_mock):
        """SAS token takes precedence over shared token"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        blob_service_mock.assert_called_once_with(
            account_url=account_url,
            credential=os.environ["AZURE_STORAGE_SAS_TOKEN"],
            container_name=container_name,
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_uploader_shared_token_auth(self, blob_service_mock):
        """Shared token is used if SAS token and connection string aren't set"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        blob_service_mock.assert_called_once_with(
            account_url=account_url,
            credential=os.environ["AZURE_STORAGE_KEY"],
            container_name=container_name,
        )

    @mock.patch("azure.identity.DefaultAzureCredential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_uploader_default_credential_auth(
        self, blob_service_mock, default_azure_credential
    ):
        """Uses DefaultAzureCredential if no other auth provided"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        blob_service_mock.assert_called_once_with(
            account_url=account_url,
            credential=default_azure_credential.return_value,
            container_name=container_name,
        )

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
        },
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_emulated_storage(self, blob_service_mock):
        """Connection string auth and emulated storage URL are valid"""
        container_name = "container"
        account_url = "https://127.0.0.1/devstoreaccount1"
        cloud_interface = AzureCloudInterface(
            url="%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        blob_service_mock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )

    # Test emulated storage fails if no URL
    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_emulated_storage_no_connection_string(self, blob_service_mock):
        """Emulated storage URL with no connection string fails"""
        container_name = "container"
        account_url = "https://127.0.0.1/devstoreaccount1"
        with pytest.raises(ValueError) as exc:
            AzureCloudInterface(url="%s/%s/path/to/dir" % (account_url, container_name))
        assert (
            str(exc.value)
            == "A connection string must be provided when using emulated storage"
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_uploader_malformed_urls(self, blob_service_mock):
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
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_connectivity(self, blob_service_mock):
        """
        Test the test_connectivity method
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        assert cloud_interface.test_connectivity() is True
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        container_client_mock.exists.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_connectivity_failure(self, blob_service_mock):
        """
        Test the test_connectivity method in case of failure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        container_client_mock.exists.side_effect = ServiceRequestError("error")
        assert cloud_interface.test_connectivity() is False

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_setup_bucket(self, blob_service_mock):
        """
        Test if a bucket already exists
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        cloud_interface.setup_bucket()
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        container_client_mock.exists.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_setup_bucket_create(self, blob_service_mock):
        """
        Test auto-creation of a bucket if it not exists
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        container_client_mock.exists.return_value = False
        cloud_interface.setup_bucket()
        container_client_mock.exists.assert_called_once_with()
        container_client_mock.create_container.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_upload_fileobj(self, blob_service_mock):
        """Test container client upload_blob is called with expected args"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call
        container_client_mock.upload_blob.assert_called_once_with(
            name=mock_key, data=mock_fileobj, overwrite=True
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_upload_fileobj_with_encryption_scope(self, blob_service_mock):
        """Test encrption scope is passed to upload_blob"""
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call along
        # with the encryption_scope
        container_client_mock.upload_blob.assert_called_once_with(
            name=mock_key,
            data=mock_fileobj,
            overwrite=True,
            encryption_scope=encryption_scope,
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_upload_part(self, blob_service_mock):
        """
        Tests the upload of a single block in Azure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_body = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface._upload_part({}, mock_key, mock_body, 1)

        # A blob client is created for the key and stage_block is called with
        # the mock_body and a block_id generated from the part number
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.stage_block.assert_called_once_with("00001", mock_body)

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_upload_part_with_encryption_scope(self, blob_service_mock):
        """
        Tests that the encryption scope is passed to the blob client when
        uploading a single block
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_body = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface._upload_part({}, mock_key, mock_body, 1)

        # A blob client is created for the key and stage_block is called with
        # the mock_body and a block_id generated from the part number and the
        # encryption scope
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.stage_block.assert_called_once_with(
            "00001", mock_body, encryption_scope=encryption_scope
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_complete_multipart_upload(self, blob_service_mock):
        """Tests completion of a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_parts = [{"PartNumber": "00001"}]
        mock_key = "path/to/blob"
        cloud_interface._complete_multipart_upload({}, mock_key, mock_parts)

        # A blob client is created for the key and commit_block_list is called
        # with the supplied list of part numbers
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(["00001"])

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_complete_multipart_upload_with_encryption_scope(self, blob_service_mock):
        """
        Tests the completion of a block blob upload in Azure Blob Storage and that
        the encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_parts = [{"PartNumber": "00001"}]
        mock_key = "path/to/blob"
        cloud_interface._complete_multipart_upload({}, mock_key, mock_parts)

        # A blob client is created for the key and commit_block_list is called
        # with the supplied list of part numbers and the encryption scope
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(
            ["00001"], encryption_scope=encryption_scope
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_abort_multipart_upload(self, blob_service_mock):
        """Test aborting a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_key = "path/to/blob"
        cloud_interface._abort_multipart_upload({}, mock_key)

        # A blob client is created for the key and commit_block_list is called
        # with an empty list, followed by delete_blob with no args
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with([])
        blob_client_mock.delete_blob.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.BlobServiceClient")
    def test_abort_multipart_upload_with_encryption_scope(self, blob_service_mock):
        """
        Test aborting a block blob upload in Azure Blob Storage and verify that the
        encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        blob_service_client_mock = blob_service_mock.from_connection_string.return_value
        container_client_mock = (
            blob_service_client_mock.get_container_client.return_value
        )
        blob_client_mock = container_client_mock.get_blob_client.return_value

        mock_key = "path/to/blob"
        cloud_interface._abort_multipart_upload({}, mock_key)

        # A blob client is created for the key and commit_block_list is called
        # with an empty list and the encryption scope, followed by delete_blob
        # with no args
        container_client_mock.get_blob_client.assert_called_once_with(mock_key)
        blob_client_mock.commit_block_list.assert_called_once_with(
            [], encryption_scope=encryption_scope
        )
        blob_client_mock.delete_blob.assert_called_once_with()


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
