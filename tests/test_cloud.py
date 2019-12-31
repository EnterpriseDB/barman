# Copyright (C) 2013-2019 2ndQuadrant Limited
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
from io import BytesIO

import mock
import pytest
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError, EndpointConnectionError

from barman.cloud import (CloudInterface, CloudUploadingError,
                          FileUploadStatistics)

try:
    from queue import Queue
except ImportError:
    from Queue import Queue


class TestCloudInterface(object):
    @mock.patch('barman.cloud.boto3')
    def test_uploader_minimal(self, boto_mock):
        """
        Minimal build of the CloudInterface class
        """
        cloud_interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
        assert cloud_interface.bucket_name == 'bucket'
        assert cloud_interface.path == 'path/to/dir'
        boto_mock.Session.assert_called_once_with(profile_name=None)
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with('s3')
        assert cloud_interface.s3 == session_mock.resource.return_value

        # Asynchronous uploading infrastructure is not initialized when
        # a new instance is created
        assert cloud_interface.queue is None
        assert cloud_interface.result_queue is None
        assert cloud_interface.errors_queue is None
        assert len(cloud_interface.parts_db) == 0
        assert len(cloud_interface.worker_processes) == 0

    @mock.patch('barman.cloud.multiprocessing')
    def test_ensure_async(self, mp):
        jobs_count = 30
        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None,
            jobs=jobs_count)

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
        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
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
                status='uploading',
                start_time=datetime.datetime(2016, 3, 30, 17, 1, 0),
            )

        # Fill the result queue with mock results, and assert that after
        # the refresh the result queue is empty and the parts_db full with
        # ordered results
        interface.result_queue.put({
            "key": "test/file",
            "part_number": 2,
            "end_time": datetime.datetime(2016, 3, 30, 17, 2, 20),
            "part": {
                "ETag": "becb2f30c11b6a2b5c069f3c8a5b798c",
                "PartNumber": "2"
            }
        })
        interface.result_queue.put({
            "key": "test/file",
            "part_number": 1,
            "end_time": datetime.datetime(2016, 3, 30, 17, 1, 20),
            "part": {
                "ETag": "27960aa8b7b851eb0277f0f3f5d15d68",
                "PartNumber": "1"
            }
        })
        interface.result_queue.put({
            "key": "test/file",
            "part_number": 3,
            "end_time": datetime.datetime(2016, 3, 30, 17, 3, 20),
            "part": {
                "ETag": "724a0685c99b457d4ddd93814c2d3e2b",
                "PartNumber": "3"
            }
        })
        interface.result_queue.put({
            "key": "test/another_file",
            "part_number": 1,
            "end_time": datetime.datetime(2016, 3, 30, 17, 5, 20),
            "part": {
                "ETag": "89d4f0341d9091aa21ddf67d3b32c34a",
                "PartNumber": "1"
            }
        })
        interface._retrieve_results()
        assert interface.result_queue.empty()
        assert interface.parts_db == {
            "test/file": [
                {
                    "ETag": "27960aa8b7b851eb0277f0f3f5d15d68",
                    "PartNumber": "1"
                },
                {
                    "ETag": "becb2f30c11b6a2b5c069f3c8a5b798c",
                    "PartNumber": "2"
                },
                {
                    "ETag": "724a0685c99b457d4ddd93814c2d3e2b",
                    "PartNumber": "3"
                }
            ],
            "test/another_file": [
                {
                    "ETag": "89d4f0341d9091aa21ddf67d3b32c34a",
                    "PartNumber": "1"
                }
            ]
        }
        assert interface.upload_stats == {
            'test/another_file': {
                'start_time': datetime.datetime(2016, 3, 30, 17, 1, 0),
                'status': 'uploading',
                'parts': {
                    1: {
                        'end_time': datetime.datetime(2016, 3, 30, 17, 5, 20),
                        'part_number': 1,
                    },
                },
            },
            'test/file': {
                'start_time': datetime.datetime(2016, 3, 30, 17, 1, 0),
                'status': 'uploading',
                'parts': {
                    1: {
                        'end_time': datetime.datetime(2016, 3, 30, 17, 1, 20),
                        'part_number': 1,
                    },
                    2: {
                        'end_time': datetime.datetime(2016, 3, 30, 17, 2, 20),
                        'part_number': 2,
                    },
                    3: {
                        'end_time': datetime.datetime(2016, 3, 30, 17, 3, 20),
                        'part_number': 3,
                    },
                },
            },
        }

    @mock.patch('barman.cloud.CloudInterface.worker_process_execute_job')
    def test_worker_process_main(self, worker_process_execute_job_mock):
        job_collection = [
            {"job_id": 1, "job_type": "upload_part"},
            {"job_id": 2, "job_type": "upload_part"},
            {"job_id": 3, "job_type": "upload_part"},
            None,
        ]

        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
        interface.queue = mock.MagicMock()
        interface.errors_queue = Queue()
        interface.queue.get.side_effect = job_collection
        interface.worker_process_main(0)

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
        interface.worker_process_main(0)
        assert interface.queue.get.call_count == 4
        # worker_process_execute_job is executed only 3 times, because it's
        # not called for the process stop marker
        assert worker_process_execute_job_mock.call_count == 3
        assert interface.queue.task_done.call_count == 4
        assert interface.errors_queue.get() == "Something is gone wrong"
        assert interface.errors_queue.empty()

    @mock.patch('barman.cloud.os.unlink')
    @mock.patch('barman.cloud.open')
    @mock.patch('barman.cloud.CloudInterface.complete_multipart_upload')
    @mock.patch('barman.cloud.CloudInterface.upload_part')
    @mock.patch('datetime.datetime')
    def test_worker_process_execute_job(self,
                                        datetime_mock,
                                        upload_part_mock,
                                        complete_multipart_upload_mock,
                                        open_mock,
                                        unlink_mock):
        # Unknown job type, no boto functions are being called and
        # an exception is being raised
        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
        interface.result_queue = Queue()
        interface.done_queue = Queue()
        with pytest.raises(ValueError):
            interface.worker_process_execute_job({"job_type": "error"}, 1)
        assert upload_part_mock.call_count == 0
        assert complete_multipart_upload_mock.call_count == 0
        assert interface.result_queue.empty()

        # upload_part job, a file with the passed name is opened, uploaded
        # and them deleted
        part_result = {
            "ETag": "89d4f0341d9091aa21ddf67d3b32c34a",
            "PartNumber": "10"
        }
        upload_part_mock.return_value = part_result
        interface.worker_process_execute_job({
            "job_type": "upload_part",
            "mpu": "mpu",
            "part_number": 10,
            "key": "this/key",
            "body": "body"}, 0)
        upload_part_mock.assert_called_once_with(
            "mpu",
            "this/key",
            open_mock.return_value.__enter__.return_value,
            10)
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
        interface.worker_process_execute_job({
            "job_type": "complete_multipart_upload",
            "mpu": "mpu",
            "key": "this/key",
            "parts": ["parts", "list"]}, 0)
        complete_multipart_upload_mock.assert_called_once_with(
            "mpu", "this/key", ["parts", "list"])
        assert not interface.done_queue.empty()
        assert interface.done_queue.get() == {
            "end_time": datetime_mock.now.return_value,
            "key": "this/key",
            "status": "done",
        }

    def test_handle_async_errors(self):
        # If we the upload process has already raised an error, we immediately
        # exit without doing anything
        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
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

    @mock.patch('barman.cloud.NamedTemporaryFile')
    @mock.patch('barman.cloud.CloudInterface._handle_async_errors')
    @mock.patch('barman.cloud.CloudInterface._ensure_async')
    def test_async_upload_part(self,
                               ensure_async_mock,
                               handle_async_errors_mock,
                               temp_file_mock):
        temp_name = "tmp_file"
        temp_stream = temp_file_mock.return_value.__enter__.return_value
        temp_stream.name = temp_name

        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
        interface.queue = Queue()
        interface.async_upload_part(
            'mpu',
            'test/key',
            BytesIO(b'test'),
            1)
        ensure_async_mock.assert_called_once_with()
        handle_async_errors_mock.assert_called_once_with()
        assert not interface.queue.empty()
        assert interface.queue.get() == {
            "job_type": "upload_part",
            "mpu": "mpu",
            "key": "test/key",
            "body": temp_name,
            "part_number": 1,
        }

    @mock.patch('barman.cloud.CloudInterface._retrieve_results')
    @mock.patch('barman.cloud.CloudInterface._handle_async_errors')
    @mock.patch('barman.cloud.CloudInterface._ensure_async')
    def test_async_complete_multipart_upload(self,
                                             ensure_async_mock,
                                             handle_async_errors_mock,
                                             retrieve_results_mock):
        interface = CloudInterface(
            url='s3://bucket/path/to/dir',
            encryption=None)
        interface.queue = mock.MagicMock()
        interface.parts_db = {
            "key": ["part", "list"]
        }

        def retrieve_results_effect():
            interface.parts_db["key"].append("complete")
        retrieve_results_mock.side_effect = retrieve_results_effect

        interface.async_complete_multipart_upload('mpu', 'key', 3)
        ensure_async_mock.assert_called_once_with()
        handle_async_errors_mock.assert_called_once_with()
        retrieve_results_mock.assert_called_once_with()

        interface.queue.put.assert_called_once_with({
            "job_type": "complete_multipart_upload",
            "mpu": "mpu",
            "key": "key",
            "parts": ["part", "list", "complete"],
        })

    @mock.patch('barman.cloud.boto3')
    def test_invalid_uploader_minimal(self, boto_mock):
        """
        Minimal build of the CloudInterface class
        """
        # Check that the creation of the cloud interface class fails in case of
        # wrongly formatted/invalid s3 uri
        with pytest.raises(ValueError) as excinfo:
            CloudInterface(
                '/bucket/path/to/dir',
                encryption=None)
        assert str(excinfo.value) == \
            'Invalid s3 URL address: /bucket/path/to/dir'

    @mock.patch('barman.cloud.boto3')
    def test_connectivity(self, boto_mock):
        """
        test the  test_connectivity method
        """
        cloud_interface = CloudInterface(
            's3://bucket/path/to/dir',
            encryption=None)
        assert cloud_interface.test_connectivity() is True
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        client_mock = s3_mock.meta.client
        client_mock.head_bucket.assert_called_once_with(Bucket='bucket')

    @mock.patch('barman.cloud.boto3')
    def test_connectivity_failure(self, boto_mock):
        """
        test the test_connectivity method in case of failure
        """
        cloud_interface = CloudInterface(
            's3://bucket/path/to/dir',
            encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        client_mock = s3_mock.meta.client
        # Raise the exception for the "I'm unable to reach amazon" event
        client_mock.head_bucket.side_effect = EndpointConnectionError(
            endpoint_url='bucket'
        )
        assert cloud_interface.test_connectivity() is False

    @mock.patch('barman.cloud.boto3')
    def test_setup_bucket(self, boto_mock):
        """
        Test if a bucket already exists
        """
        cloud_interface = CloudInterface(
            's3://bucket/path/to/dir',
            encryption=None)
        cloud_interface.setup_bucket()
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Expect a call on the head_bucket method of the s3 client.
        s3_client.head_bucket.assert_called_once_with(
            Bucket=cloud_interface.bucket_name
        )

    @mock.patch('barman.cloud.boto3')
    def test_setup_bucket_create(self, boto_mock):
        """
        Test auto-creation of a bucket if it not exists
        """
        cloud_interface = CloudInterface(
            's3://bucket/path/to/dir',
            encryption=None)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Simulate a 404 error from amazon for 'bucket not found'
        s3_client.head_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': '404'}},
            operation_name='load'
        )
        cloud_interface.setup_bucket()
        bucket_mock = s3_mock.Bucket
        # Expect a call for bucket obj creation
        bucket_mock.assert_called_once_with(cloud_interface.bucket_name)
        # Expect the create() metod of the bucket object to be called
        bucket_mock.return_value.create.assert_called_once()
