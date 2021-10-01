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

import bz2
import gzip
import logging
import os

import mock
import pytest

from barman.clients import cloud_walarchive
from barman.clients.cloud_walarchive import CloudWalUploader
from barman.cloud_providers.aws_s3 import S3CloudInterface
from barman.cloud_providers.azure_blob_storage import AzureCloudInterface
from barman.exceptions import BarmanException
from barman.xlog import hash_dir


EXAMPLE_WAL_PATH = "wal_dir/000000080000ABFF000000C1"


class TestMain(object):
    """
    Test the main method and its interactions with CloudInterface
    """

    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    def test_ok(self, cloud_interface_mock, uploader_mock):
        uploader_object_mock = uploader_mock.return_value
        cloud_object_interface_mock = cloud_interface_mock.return_value

        # Plain success
        cloud_walarchive.main(
            [
                "s3://test-bucket/testfolder",
                "test-server",
                "/tmp/000000080000ABFF000000C1",
            ]
        )

        uploader_mock.assert_called_once_with(
            cloud_interface=cloud_object_interface_mock,
            server_name="test-server",
            compression=None,
        )
        cloud_object_interface_mock.setup_bucket.assert_called_once_with()
        uploader_object_mock.upload_wal.assert_called_once_with(
            "/tmp/000000080000ABFF000000C1"
        )

        # Plain success with profile
        uploader_mock.reset_mock()
        cloud_interface_mock.reset_mock()
        cloud_walarchive.main(
            [
                "--profile",
                "test_profile",
                "s3://test-bucket/testfolder",
                "test-server",
                "/tmp/000000080000ABFF000000C1",
            ]
        )

        uploader_mock.assert_called_once_with(
            cloud_interface=cloud_object_interface_mock,
            server_name="test-server",
            compression=None,
        )
        cloud_object_interface_mock.setup_bucket.assert_called_once_with()
        uploader_object_mock.upload_wal.assert_called_once_with(
            "/tmp/000000080000ABFF000000C1"
        )

        # Invalid filename upload
        uploader_mock.reset_mock()
        cloud_interface_mock.reset_mock()
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                [
                    "--profile",
                    "test_profile",
                    "s3://test-bucket/testfolder",
                    "test-server",
                    "/tmp/000000080000ABFF000000C1-INVALID",
                ]
            )
        assert excinfo.value.code == 1

        # Successful connectivity test
        uploader_mock.reset_mock()
        cloud_interface_mock.reset_mock()
        uploader_object_mock.test_connectivity.return_value = True
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                [
                    "-t",
                    "s3://test-bucket/testfolder",
                    "test-server",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert excinfo.value.code == 0
        uploader_mock.assert_called_once_with(
            cloud_interface=cloud_object_interface_mock,
            server_name="test-server",
            compression=None,
        )
        cloud_object_interface_mock.test_connectivity.assert_called_once_with()

        # Failing connectivity test
        uploader_mock.reset_mock()
        cloud_interface_mock.reset_mock()
        cloud_object_interface_mock.test_connectivity.return_value = False
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                [
                    "-t",
                    "s3://test-bucket/testfolder",
                    "test-server",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
        assert excinfo.value.code == 1
        uploader_mock.assert_called_once_with(
            cloud_interface=cloud_object_interface_mock,
            server_name="test-server",
            compression=None,
        )
        cloud_object_interface_mock.test_connectivity.assert_called_once_with()

    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_ko(self, uploader_mock, caplog):
        """
        Run with exception thrown
        """
        uploader_mock.return_value.upload_wal.side_effect = Exception("test")

        with pytest.raises(SystemExit) as e:
            cloud_walarchive.main(
                [
                    "s3://test-bucket/testfolder/",
                    "test-server",
                    "/tmp/000000080000ABFF000000C1",
                ]
            )
            assert (
                "Barman cloud WAL archiver exception:",
                logging.ERROR,
                "err",
            ) in caplog.record_tuples
            assert e.value.code == 1


# noinspection PyProtectedMember
class TestWalUploader(object):
    """
    Test the CloudWalUploader class
    """

    def test_retrieve_normal_file_obj(self, tmpdir):
        """
        Test the retrieve_file_obj method with an uncompressed file
        """
        # Setup the WAL file
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something".encode("utf-8"), ensure=True)
        # Create a simple CloudWalUploader obj
        uploader = CloudWalUploader(mock.MagicMock(), "test-server")
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the file received
        assert open_file
        # Check content
        assert open_file.read() == "something".encode("utf-8")

    def test_retrieve_gzip_file_obj(self, tmpdir):
        """
        Test the retrieve_file_obj method with a gzip file
        """
        # Setup the WAL
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something".encode("utf-8"), ensure=True)
        # Create a simple CloudWalUploader obj
        uploader = CloudWalUploader(mock.MagicMock(), "test-server", compression="gzip")
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the in memory file received
        assert open_file
        # Decompress on the fly to check content
        assert gzip.GzipFile(fileobj=open_file).read() == "something".encode("utf-8")

    def test_retrieve_bz2_file_obj(self, tmpdir):
        """
        Test the retrieve_file_obj method with a bz2 file
        """
        # Setup the WAL
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something".encode("utf-8"), ensure=True)
        # Create a simple CloudWalUploader obj
        uploader = CloudWalUploader(
            mock.MagicMock(), "test-server", compression="bzip2"
        )
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the in memory file received
        assert open_file
        # Decompress on the fly to check content
        assert bz2.decompress(open_file.read()) == "something".encode("utf-8")

    def test_retrieve_normal_file_name(self):
        """
        Test the retrieve_wal_name method with an uncompressed file
        """
        # Create a fake source name
        source = "wal_dir/000000080000ABFF000000C1"
        uploader = CloudWalUploader(mock.MagicMock(), "test-server")
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == "000000080000ABFF000000C1"

    def test_retrieve_gzip_file_name(self):
        """
        Test the retrieve_wal_name method with gzip compression
        """
        # Create a fake source name
        source = "wal_dir/000000080000ABFF000000C1"
        uploader = CloudWalUploader(mock.MagicMock(), "test-server", compression="gzip")
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == "000000080000ABFF000000C1.gz"

    def test_retrieve_bz2_file_name(self):
        """
        Test the retrieve_wal_name method with bz2 compression
        """
        # Create a fake source name
        source = "wal_dir/000000080000ABFF000000C1"
        uploader = CloudWalUploader(
            mock.MagicMock(), "test-server", compression="bzip2"
        )
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == "000000080000ABFF000000C1.bz2"


class TestWalUploaderS3(object):
    """
    Test the CloudWalUploader class with S3CloudInterface
    """

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader.retrieve_file_obj")
    def test_upload_wal(self, rfo_mock, boto_mock):
        """
        Test the upload of a WAL
        """
        # Create a simple S3WalUploader obj
        cloud_interface = S3CloudInterface("s3://bucket/path/to/dir", encryption=None)
        uploader = CloudWalUploader(cloud_interface, "test-server")
        source = "/wal_dir/000000080000ABFF000000C1"
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        uploader.upload_wal(source)

        session_mock = boto_mock.Session.return_value
        s3_client_mock = session_mock.resource.return_value.meta.client
        # Check the call for the creation of the destination key
        s3_client_mock.upload_fileobj.assert_called_once_with(
            Fileobj=rfo_mock.return_value,
            Bucket=cloud_interface.bucket_name,
            Key=os.path.join(
                cloud_interface.path,
                uploader.server_name,
                "wals",
                hash_dir(source),
                os.path.basename(source),
            ),
            ExtraArgs={},
        )

    @mock.patch("barman.cloud_providers.aws_s3.boto3")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader.retrieve_file_obj")
    def test_encrypted_upload_wal(self, rfo_mock, boto_mock):
        """
        Test the upload of a WAL
        """
        # Create a simple CloudWalUploader obj
        cloud_interface = S3CloudInterface(
            "s3://bucket/path/to/dir", encryption="AES256"
        )
        uploader = CloudWalUploader(cloud_interface, "test-server")
        source = "/wal_dir/000000080000ABFF000000C1"
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        uploader.upload_wal(source)
        session_mock = boto_mock.Session.return_value
        s3_client_mock = session_mock.resource.return_value.meta.client
        # Check the call for the creation of the destination key
        s3_client_mock.upload_fileobj.assert_called_once_with(
            Fileobj=rfo_mock.return_value,
            Bucket=cloud_interface.bucket_name,
            Key=os.path.join(
                cloud_interface.path,
                uploader.server_name,
                "wals",
                hash_dir(source),
                os.path.basename(source),
            ),
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )


class TestWalUploaderAzure(object):
    """
    Test the CloudWalUploader class with AzureCloudInterface
    """

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.cloud_providers.azure_blob_storage.ContainerClient")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader.retrieve_file_obj")
    def test_upload_wal(self, rfo_mock, ContainerClientMock):
        """
        Test the upload of a WAL
        """
        # Create a simple S3WalUploader obj
        container_name = "container"
        cloud_interface = AzureCloudInterface(
            url="https://account.blob.core.windows.net/container/path/to/dir"
        )
        uploader = CloudWalUploader(cloud_interface, "test-server")
        source = "/wal_dir/000000080000ABFF000000C1"
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        uploader.upload_wal(source)

        ContainerClientMock.from_connection_string.assert_called_once_with(
            conn_str=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            container_name=container_name,
        )
        container_client = ContainerClientMock.from_connection_string.return_value

        # Check the call for the creation of the destination key
        container_client.upload_blob.assert_called_once_with(
            data=rfo_mock.return_value,
            name=os.path.join(
                cloud_interface.path,
                uploader.server_name,
                "wals",
                hash_dir(source),
                os.path.basename(source),
            ),
            overwrite=True,
        )


class TestWalUploaderHookScript(object):
    """
    Test that we get the intended behaviour when called as a hook script
    """

    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_uses_wal_path_argument_when_not_running_as_hook(
        self, uploader_mock, cloud_interface_mock
    ):
        uploader = uploader_mock.return_value
        cloud_walarchive.main(["cloud_storage_url", "test_server", EXAMPLE_WAL_PATH])
        cloud_interface_mock.assert_called_once()
        uploader.upload_wal.assert_called_once_with(EXAMPLE_WAL_PATH)

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "archive_script",
            "BARMAN_PHASE": "pre",
            "BARMAN_FILE": EXAMPLE_WAL_PATH,
        },
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_uses_barman_file_env_when_running_as_hook(
        self, uploader_mock, cloud_interface_mock
    ):
        uploader = uploader_mock.return_value
        cloud_walarchive.main(["cloud_storage_url", "test_server"])
        cloud_interface_mock.assert_called_once()
        uploader.upload_wal.assert_called_once_with(EXAMPLE_WAL_PATH)

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "archive_retry_script",
            "BARMAN_PHASE": "pre",
            "BARMAN_FILE": EXAMPLE_WAL_PATH,
        },
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_uses_barman_file_env_when_running_as_retry_hook(
        self, uploader_mock, cloud_interface_mock
    ):
        uploader = uploader_mock.return_value
        cloud_walarchive.main(["cloud_storage_url", "test_server"])
        cloud_interface_mock.assert_called_once()
        uploader.upload_wal.assert_called_once_with(EXAMPLE_WAL_PATH)

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "archive_retry_script",
            "BARMAN_PHASE": "pre",
        },
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_error_if_barman_file_not_provided(
        self, uploader_mock, cloud_interface_mock
    ):
        with pytest.raises(BarmanException) as exc:
            cloud_walarchive.main(["cloud_storage_url", "test_server"])
        assert "Expected environment variable BARMAN_FILE not set" in str(exc.value)
        uploader_mock.assert_not_called()
        cloud_interface_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "archive_retry_script",
            "BARMAN_PHASE": "post",
            "BARMAN_FILE": EXAMPLE_WAL_PATH,
        },
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_error_if_running_as_unsupported_phase(
        self, uploader_mock, cloud_interface_mock
    ):
        with pytest.raises(BarmanException) as exc:
            cloud_walarchive.main(["cloud_storage_url", "test_server"])
        assert "barman-cloud-wal-archive called as unsupported hook script" in str(
            exc.value
        )
        uploader_mock.assert_not_called()
        cloud_interface_mock.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "connection_string",
            "BARMAN_HOOK": "backup_script",
            "BARMAN_PHASE": "pre",
            "BARMAN_FILE": EXAMPLE_WAL_PATH,
        },
    )
    @mock.patch("barman.clients.cloud_walarchive.get_cloud_interface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    def test_error_if_running_as_unsupported_hook(
        self, uploader_mock, cloud_interface_mock
    ):
        with pytest.raises(BarmanException) as exc:
            cloud_walarchive.main(["cloud_storage_url", "test_server"])
        assert "barman-cloud-wal-archive called as unsupported hook script" in str(
            exc.value
        )
        uploader_mock.assert_not_called()
        cloud_interface_mock.assert_not_called()
