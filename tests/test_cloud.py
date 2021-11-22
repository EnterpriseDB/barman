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
from argparse import Namespace
from io import BytesIO
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from azure.identity import AzureCliCredential, ManagedIdentityCredential
from azure.storage.blob import PartialBatchErrorException

import mock
from mock.mock import MagicMock
import pytest
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, EndpointConnectionError

from barman.annotations import KeepManager
from barman.cloud import (
    CloudBackupCatalog,
    CloudProviderError,
    CloudUploadingError,
    FileUploadStatistics,
)
from barman.cloud_providers import (
    CloudProviderOptionUnsupported,
    CloudProviderUnsupported,
    get_cloud_interface,
)
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

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    def test_delete_objects_multiple_batches(self, boto_mock):
        """
        Tests that deletions of more than 1000 objects are split into multiple requests
        (necessary due to s3/boto3 limitations)
        """
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client

        mock_keys = ["path/to/object/%s" % i for i in range(1001)]
        cloud_interface.delete_objects(mock_keys)

        assert s3_client.delete_objects.call_args_list[0] == mock.call(
            Bucket="bucket",
            Delete={
                "Quiet": True,
                "Objects": [{"Key": key} for key in mock_keys[:1000]],
            },
        )
        assert s3_client.delete_objects.call_args_list[1] == mock.call(
            Bucket="bucket",
            Delete={"Quiet": True, "Objects": [{"Key": mock_keys[1000]}]},
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
            "check the Barman logs"
        )

        assert (
            "Deletion of object path/to/object/1 failed with error code: "
            '"AccessDenied", message: "Access Denied"'
        ) in caplog.text


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
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_minimal(self, ContainerClientMock):
        """Connection string auth takes precedence over SAS token or shared token"""
        container_name = "container"
        account_url = "https://storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.from_connection_string.assert_called_once_with(
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
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_with_specified_credential(self, ContainerClientMock):
        """Specified credential option takes precedences over environment"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        credential = AzureCliCredential()
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name),
            credential=credential,
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.assert_called_once_with(
            account_url=account_url,
            credential=credential,
            container_name=container_name,
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_sas_token_auth(self, ContainerClientMock):
        """SAS token takes precedence over shared token"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.assert_called_once_with(
            account_url=account_url,
            credential=os.environ["AZURE_STORAGE_SAS_TOKEN"],
            container_name=container_name,
        )

    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_KEY": "storage_key"},
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_shared_token_auth(self, ContainerClientMock):
        """Shared token is used if SAS token and connection string aren't set"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.assert_called_once_with(
            account_url=account_url,
            credential=os.environ["AZURE_STORAGE_KEY"],
            container_name=container_name,
        )

    @mock.patch("azure.identity.DefaultAzureCredential")
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_uploader_default_credential_auth(
        self, ContainerClientMock, default_azure_credential
    ):
        """Uses DefaultAzureCredential if no other auth provided"""
        container_name = "container"
        account_url = "storageaccount.blob.core.windows.net"
        cloud_interface = AzureCloudInterface(
            url="https://%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.assert_called_once_with(
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
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_emulated_storage(self, ContainerClientMock):
        """Connection string auth and emulated storage URL are valid"""
        container_name = "container"
        account_url = "https://127.0.0.1/devstoreaccount1"
        cloud_interface = AzureCloudInterface(
            url="%s/%s/path/to/dir" % (account_url, container_name)
        )

        assert cloud_interface.bucket_name == "container"
        assert cloud_interface.path == "path/to/dir"
        ContainerClientMock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )

    # Test emulated storage fails if no URL
    @mock.patch.dict(
        os.environ,
        {"AZURE_STORAGE_SAS_TOKEN": "sas_token", "AZURE_STORAGE_KEY": "storage_key"},
    )
    def test_emulated_storage_no_connection_string(self):
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
    def test_connectivity(self, ContainerClientMock):
        """
        Test the test_connectivity method
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        assert cloud_interface.test_connectivity() is True
        # Bucket existence checking is carried out by checking we can successfully
        # iterate the bucket contents
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_connectivity_failure(self, ContainerClientMock):
        """
        Test the test_connectivity method in case of failure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.side_effect = ServiceRequestError("error")
        assert cloud_interface.test_connectivity() is False

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_setup_bucket(self, ContainerClientMock):
        """
        Test if a bucket already exists
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        cloud_interface.setup_bucket()
        container_client = ContainerClientMock.from_connection_string.return_value
        container_client.list_blobs.assert_called_once_with()
        blobs_iterator = container_client.list_blobs.return_value
        blobs_iterator.next.assert_called_once_with()

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_setup_bucket_create(self, ContainerClientMock):
        """
        Test auto-creation of a bucket if it does not exist
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_upload_fileobj(self, ContainerClientMock):
        """Test container client upload_blob is called with expected args"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call
        container_client.upload_blob.assert_called_once_with(
            name=mock_key, data=mock_fileobj, overwrite=True
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_fileobj_with_encryption_scope(self, ContainerClientMock):
        """Test encrption scope is passed to upload_blob"""
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = ContainerClientMock.from_connection_string.return_value
        mock_fileobj = mock.MagicMock()
        mock_key = "path/to/blob"
        cloud_interface.upload_fileobj(mock_fileobj, mock_key)
        # The key and fileobj are passed on to the upload_blob call along
        # with the encryption_scope
        container_client.upload_blob.assert_called_once_with(
            name=mock_key,
            data=mock_fileobj,
            overwrite=True,
            encryption_scope=encryption_scope,
        )

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_upload_part(self, ContainerClientMock):
        """
        Tests the upload of a single block in Azure
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_upload_part_with_encryption_scope(self, ContainerClientMock):
        """
        Tests that the encryption scope is passed to the blob client when
        uploading a single block
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_complete_multipart_upload(self, ContainerClientMock):
        """Tests completion of a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_complete_multipart_upload_with_encryption_scope(self, ContainerClientMock):
        """
        Tests the completion of a block blob upload in Azure Blob Storage and that
        the encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_abort_multipart_upload(self, ContainerClientMock):
        """Test aborting a block blob upload in Azure Blob Storage"""
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_abort_multipart_upload_with_encryption_scope(self, ContainerClientMock):
        """
        Test aborting a block blob upload in Azure Blob Storage and verify that the
        encryption scope is passed to the blob client
        """
        encryption_scope = "test_encryption_scope"
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob",
            encryption_scope=encryption_scope,
        )
        container_client = ContainerClientMock.from_connection_string.return_value
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
    def test_delete_objects(self, ContainerClientMock):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value

        mock_keys = ["path/to/object/1", "path/to/object/2"]
        cloud_interface.delete_objects(mock_keys)

        container_client.delete_blobs.assert_called_once_with(*mock_keys)

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_with_empty_list(self, ContainerClientMock):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value

        mock_keys = []
        cloud_interface.delete_objects(mock_keys)

        # The Azure SDK is happy to accept an empty list here so verify that we
        # simply passed it on
        container_client.delete_blobs.assert_called_once_with()

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
    def test_delete_objects_partial_failure(self, ContainerClientMock, caplog):
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value

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
            "check the Barman logs"
        )

        assert (
            'Deletion of object path/to/object/1 failed with error code: "403"'
        ) in caplog.text

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_partial_failure_exception(
        self, ContainerClientMock, caplog
    ):
        """
        Test that partial failures raised via PartialBatchErrorException are handled.
        This isn't explicitly described in the Azure documentation but is something
        which happens in practice so we must deal with it.
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value

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
            "check the Barman logs"
        )

        assert (
            'Deletion of object path/to/object/1 failed with error code: "403"'
        ) in caplog.text

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    def test_delete_objects_404_not_failure(self, ContainerClientMock, caplog):
        """
        Test that 404 responses in partial failures do not create an error.
        """
        cloud_interface = AzureCloudInterface(
            "https://storageaccount.blob.core.windows.net/container/path/to/blob"
        )
        container_client = ContainerClientMock.from_connection_string.return_value

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


class TestGetCloudInterface(object):
    """
    Verify get_cloud_interface creates the required CloudInterface
    """

    @pytest.fixture()
    def mock_config_aws(self):
        return Namespace(endpoint_url=None, profile=None, source_url="test-url")

    @pytest.fixture()
    def mock_config_azure(self):
        return Namespace(credential=None, source_url="test-url")

    def test_unsupported_provider(self, mock_config_aws):
        """Verify an exception is raised for unsupported cloud providers"""
        mock_config_aws.cloud_provider = "aws-infinidash"
        with pytest.raises(CloudProviderUnsupported) as exc:
            get_cloud_interface(mock_config_aws)
        assert "Unsupported cloud provider: aws-infinidash" == str(exc.value)

    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_aws_s3(self, mock_s3_cloud_interface, mock_config_aws):
        """Verify --cloud-provider=aws-s3 creates an S3CloudInterface"""
        mock_config_aws.cloud_provider = "aws-s3"
        get_cloud_interface(mock_config_aws)
        mock_s3_cloud_interface.assert_called_once()

    @mock.patch("barman.cloud_providers.azure_blob_storage.AzureCloudInterface")
    def test_azure_blob_storage(self, mock_azure_cloud_interface, mock_config_azure):
        """Verify --cloud-provider=azure-blob-storage creates an AzureCloudInterface"""
        mock_config_azure.cloud_provider = "azure-blob-storage"
        get_cloud_interface(mock_config_azure)
        mock_azure_cloud_interface.assert_called_once()

    def test_azure_blob_storage_unsupported_credential(self, mock_config_azure):
        """Verify unsupported Azure credentials raise an exception"""
        mock_config_azure.cloud_provider = "azure-blob-storage"
        mock_config_azure.credential = "qbasic-credential"
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
        mock_config_azure.credential = credential_arg
        get_cloud_interface(mock_config_azure)
        mock_azure_cloud_interface.assert_called_once()
        assert isinstance(
            mock_azure_cloud_interface.call_args_list[0][1]["credential"],
            expected_credential,
        )


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

    def test_can_list_single_wal(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075",
        )

    def test_can_list_compressed_wal(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.gz",
        )

    def test_ignores_unsupported_compression(self):
        mock_cloud_interface = MagicMock()
        mock_cloud_interface.list_bucket.return_value = [
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.something",
        ]
        catalog = CloudBackupCatalog(mock_cloud_interface, "test-server")
        wals = catalog.get_wal_paths()
        assert len(wals) == 0

    def test_can_list_backup_labels(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075.00000028.backup",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.00000028.backup",
        )

    def test_can_list_compressed_backup_labels(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075.00000028.backup",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.00000028.backup.gz",
        )

    def test_can_list_partial_wals(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075.partial",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.partial",
        )

    def test_can_list_compressed_partial_wals(self):
        self._verify_wal_is_in_catalog(
            "000000010000000000000075.partial",
            "mt-backups/test-server/wals/0000000100000000/000000010000000000000075.partial.gz",
        )

    def test_can_list_history_wals(self):
        self._verify_wal_is_in_catalog(
            "00000001.history",
            "mt-backups/test-server/wals/0000000100000000/00000001.history",
        )

    def test_can_list_compressed_history_wals(self):
        self._verify_wal_is_in_catalog(
            "00000001.history",
            "mt-backups/test-server/wals/0000000100000000/00000001.history.gz",
        )

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
        mock_backup_info = mock.MagicMock(name="backup_info")
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

        def list_bucket(prefix, delimiter=""):
            return in_memory_object_store.keys()

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
