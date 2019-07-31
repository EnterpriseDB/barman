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

import bz2
import gzip
import logging
import os

import mock
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from barman.clients import cloud_walarchive
from barman.clients.cloud_walarchive import S3WalUploader


class TestMain(object):
    """
    Test the main method
    """
    @mock.patch('barman.clients.cloud_walarchive.S3WalUploader')
    def test_ok(self, uploader_mock):
        uploader_object_mock = uploader_mock.return_value
        # Plain success
        cloud_walarchive.main(
            ['s3://test-bucket/testfolder',
             'test-server',
             '/tmp/000000080000ABFF000000C1'])

        uploader_mock.assert_called_once_with(
            compression=None,
            destination_url='s3://test-bucket/testfolder',
            encryption=None,
            profile_name=None,
            server_name='test-server')
        uploader_object_mock.setup_bucket.assert_called_once_with()
        uploader_object_mock.upload_wal.assert_called_once_with(
            '/tmp/000000080000ABFF000000C1')

        # Plain success with profile
        uploader_mock.reset_mock()
        cloud_walarchive.main(
            ['--profile', 'test_profile',
             's3://test-bucket/testfolder',
             'test-server',
             '/tmp/000000080000ABFF000000C1'])

        uploader_mock.assert_called_once_with(
            compression=None,
            destination_url='s3://test-bucket/testfolder',
            encryption=None,
            profile_name='test_profile',
            server_name='test-server')
        uploader_object_mock.setup_bucket.assert_called_once_with()
        uploader_object_mock.upload_wal.assert_called_once_with(
            '/tmp/000000080000ABFF000000C1')

        # Successful connectivity test
        uploader_mock.reset_mock()
        uploader_object_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                ['-t', 's3://test-bucket/testfolder',
                 'test-server',
                 '/tmp/000000080000ABFF000000C1']
            )
        assert excinfo.value.code == 0
        uploader_mock.assert_called_once_with(
            compression=None,
            destination_url='s3://test-bucket/testfolder',
            encryption=None,
            profile_name=None,
            server_name='test-server')
        uploader_object_mock.test_connectivity.assert_called_once_with()

        # Failing connectivity test
        uploader_mock.reset_mock()
        uploader_object_mock.test_connectivity.return_value = False
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                ['-t', 's3://test-bucket/testfolder',
                 'test-server',
                 '/tmp/000000080000ABFF000000C1']
            )
        assert excinfo.value.code == 1
        uploader_mock.assert_called_once_with(
            compression=None,
            destination_url='s3://test-bucket/testfolder',
            encryption=None,
            profile_name=None,
            server_name='test-server')
        uploader_object_mock.test_connectivity.assert_called_once_with()

    @mock.patch('barman.clients.cloud_walarchive.S3WalUploader')
    def test_ko(self, uploader_mock, caplog):
        """
        Run with exception thrown
        """
        uploader_mock.return_value.upload_wal.side_effect = Exception('test')

        with pytest.raises(SystemExit) as e:
            cloud_walarchive.main(
                ['s3://test-bucket/testfolder/',
                 'test-server',
                 '/tmp/000000080000ABFF000000C1'])
            assert (
                'Barman cloud WAL archiver exception:', logging.ERROR, 'err'
            ) in caplog.record_tuples
            assert e.value.code == 1


# noinspection PyProtectedMember
class TestWalUploader(object):
    """
    Test the S3WalUploader class
    """
    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_uploader_minimal(self, boto_mock):
        """
        Minimal build of the S3WalUploader class
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        assert uploader.bucket_name == 'bucket'
        assert uploader.path == '/path/to/dir'
        boto_mock.Session.assert_called_once_with(profile_name=None)
        session_mock = boto_mock.Session.return_value
        session_mock.resource.assert_called_once_with('s3')
        assert uploader.s3 == session_mock.resource.return_value

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_invalid_uploader_minimal(self, boto_mock):
        """
        Minimal build of the S3WalUploader class
        """
        # Check that the creation of the uploader fails in case of
        # wrongly formatted/invalid s3 uri
        with pytest.raises(ValueError) as excinfo:
            S3WalUploader(
                '/bucket/path/to/dir', 'test-server'
            )
        assert str(excinfo.value) == \
            'Invalid s3 URL address: /bucket/path/to/dir'

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_connectivity(self, boto_mock):
        """
        test the  test_connectivity method
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        assert uploader.test_connectivity() is True
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        bucket_mock = s3_mock.Bucket
        bucket_mock.assert_called_once_with('bucket')
        bucket_mock.return_value.load.assert_called_once()

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_connectivity_failure(self, boto_mock):
        """
        test the test_connectivity method in case of failure
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        bucket_mock = s3_mock.Bucket
        # Raise the exception for the "I'm unable to reach amazon" event
        bucket_mock.return_value.load.side_effect = EndpointConnectionError(
            endpoint_url='bucket'
        )
        assert uploader.test_connectivity() is False

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    @mock.patch('barman.clients.cloud_walarchive.S3WalUploader.'
                'retrieve_file_obj')
    def test_upload_wal(self, rfo_mock, boto_mock):
        """
        Test the upload of a WAL
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        source = '/wal_dir/000000080000ABFF000000C1'
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        uploader.upload_wal(source)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_object_mock = s3_mock.Object
        # Check the call for the creation of the destination path
        s3_object_mock.assert_called_once_with(
            uploader.bucket_name,
            os.path.join(
                uploader.path,
                uploader.server_name,
                'wals',
                os.path.basename(source),
            )[1:]
        )
        # Check the call for the put method
        s3_object_mock.return_value.put.assert_called_once_with(
            Body=rfo_mock.return_value)

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    @mock.patch('barman.clients.cloud_walarchive.S3WalUploader.'
                'retrieve_file_obj')
    def test_encrypted_upload_wal(self, rfo_mock, boto_mock):
        """
        Test the upload of a WAL
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server', encryption='AES256'
        )
        source = '/wal_dir/000000080000ABFF000000C1'
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        uploader.upload_wal(source)
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_object_mock = s3_mock.Object
        # Check the call for the creation of the destination path
        s3_object_mock.assert_called_once_with(
            uploader.bucket_name,
            os.path.join(
                uploader.path,
                uploader.server_name,
                'wals',
                os.path.basename(source),
            )[1:]
        )
        # Check the call for the put method
        s3_object_mock.return_value.put.assert_called_once_with(
            Body=rfo_mock.return_value,
            ServerSideEncryption='AES256'
        )

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_setup_bucket(self, boto_mock):
        """
        Test if a bucket already exists
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        uploader.setup_bucket()
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Expect a call on the head_bucket method of the s3 client.
        s3_client.head_bucket.assert_called_once_with(
            Bucket=uploader.bucket_name
        )

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_setup_bucket_create(self, boto_mock):
        """
        Test auto-creation of a bucket if it not exists
        """
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        session_mock = boto_mock.Session.return_value
        s3_mock = session_mock.resource.return_value
        s3_client = s3_mock.meta.client
        # Simulate a 404 error from amazon for 'bucket not found'
        s3_client.head_bucket.side_effect = ClientError(
            error_response={'Error': {'Code': '404'}},
            operation_name='load'
        )
        uploader.setup_bucket()
        bucket_mock = s3_mock.Bucket
        # Expect a call for bucket obj creation
        bucket_mock.assert_called_once_with(uploader.bucket_name)
        # Expect the create() metod of the bucket object to be called
        bucket_mock.return_value.create.assert_called_once()

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_retrieve_normal_file_obj(self, boto_mock, tmpdir):
        """
        Test the retrieve_file_obj method with an uncompressed file
        """
        # Setup the WAL file
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something'.encode('utf-8'), ensure=True)
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the file received
        assert open_file
        # Check content
        assert open_file.read() == 'something'.encode('utf-8')

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_retrieve_gzip_file_obj(self, boto_mock, tmpdir):
        """
        Test the retrieve_file_obj method with a gzip file
        """
        # Setup the WAL
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something'.encode('utf-8'), ensure=True)
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server', compression='gzip'
        )
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the in memory file received
        assert open_file
        # Decompress on the fly to check content
        assert gzip.GzipFile(fileobj=open_file).read() == \
            'something'.encode('utf-8')

    @mock.patch('barman.clients.cloud_walarchive.boto3')
    def test_retrieve_bz2_file_obj(self, boto_mock, tmpdir):
        """
        Test the retrieve_file_obj method with a bz2 file
        """
        # Setup the WAL
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something'.encode('utf-8'), ensure=True)
        # Create a simple S3WalUploader obj
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server', compression='bzip2'
        )
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the in memory file received
        assert open_file
        # Decompress on the fly to check content
        assert bz2.decompress(open_file.read()) == 'something'.encode('utf-8')

    def test_retrieve_normal_file_name(self):
        """
        Test the retrieve_wal_name method with an uncompressed file
        """
        # Create a fake source name
        source = 'wal_dir/000000080000ABFF000000C1'
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server'
        )
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == '000000080000ABFF000000C1'

    def test_retrieve_gzip_file_name(self):
        """
        Test the retrieve_wal_name method with gzip compression
        """
        # Create a fake source name
        source = 'wal_dir/000000080000ABFF000000C1'
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server', compression='gzip'
        )
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == '000000080000ABFF000000C1.gz'

    def test_retrieve_bz2_file_name(self):
        """
        Test the retrieve_wal_name method with bz2 compression
        """
        # Create a fake source name
        source = 'wal_dir/000000080000ABFF000000C1'
        uploader = S3WalUploader(
            's3://bucket/path/to/dir', 'test-server', compression='bzip2'
        )
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == '000000080000ABFF000000C1.bz2'
