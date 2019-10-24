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

import mock
import pytest
from botocore.exceptions import EndpointConnectionError, ClientError

from barman.cloud import CloudInterface


class TestCloudInterface(object):
    @mock.patch('barman.cloud.boto3')
    def test_uploader_minimal(self, boto_mock):
        """
        Minimal build of the CloudInterface class
        """
        cloud_interface = CloudInterface(
            destination_url='s3://bucket/path/to/dir',
            encryption=None)
        assert cloud_interface.bucket_name == 'bucket'
        assert cloud_interface.path == '/path/to/dir'
        boto_mock.Session.assert_called_once_with(profile_name=None)
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with('s3')
        assert cloud_interface.s3 == session_mock.resource.return_value

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
        bucket_mock = s3_mock.Bucket
        bucket_mock.assert_called_once_with('bucket')
        bucket_mock.return_value.load.assert_called_once()

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
        bucket_mock = s3_mock.Bucket
        # Raise the exception for the "I'm unable to reach amazon" event
        bucket_mock.return_value.load.side_effect = EndpointConnectionError(
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
