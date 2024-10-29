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
import gzip
import logging
import os

import mock
import pytest
import snappy

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

        # Plain success with aws_profile
        uploader_mock.reset_mock()
        cloud_interface_mock.reset_mock()
        cloud_walarchive.main(
            [
                "--aws-profile",
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
                    "--aws-profile",
                    "test_profile",
                    "s3://test-bucket/testfolder",
                    "test-server",
                    "/tmp/000000080000ABFF000000C1-INVALID",
                ]
            )
        assert excinfo.value.code == 3

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
        assert excinfo.value.code == 2
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

    @pytest.mark.parametrize(
        (
            "wal_name",
            "tags_args",
            "history_tags_args",
            "expected_tags",
            "expected_override_tags",
        ),
        [
            # With a standard WAL file, the cloud interface should be created with tags
            # and no override tags are expected
            (
                "/tmp/000000080000ABFF000000C1",
                ["--tags", "foo,bar", '"b,az",qux'],
                ["--history-tags", "historyfoo,historybar", "historybaz,historyqux"],
                [("foo", "bar"), ("b,az", "qux")],
                None,
            ),
            # With a history WAL file, the cloud interface should be created with tags
            # and override tags should be included on WAL upload
            (
                "/tmp/00000008.history",
                ["--tags", "foo,bar", "baz,qux"],
                ["--history-tags", "historyfoo,historybar", '"historyb,az",historyqux'],
                [("foo", "bar"), ("baz", "qux")],
                [("historyfoo", "historybar"), ("historyb,az", "historyqux")],
            ),
        ],
    )
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_wal_archive_tags(
        self,
        cloud_interface_mock,
        uploader_mock,
        wal_name,
        tags_args,
        history_tags_args,
        expected_tags,
        expected_override_tags,
    ):
        """Test that tags and history tags are handled."""
        uploader_object_mock = uploader_mock.return_value

        cloud_walarchive.main(
            [
                "s3://test-bucket/testfolder",
                "test-server",
                wal_name,
            ]
            + tags_args
            + history_tags_args
        )

        # Verify expected tags are passed to cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="s3://test-bucket/testfolder",
            tags=expected_tags,
            profile_name=None,
            endpoint_url=None,
            encryption=None,
            sse_kms_key_id=None,
            read_timeout=None,
        )

        # Verify expected override tags are passed to upload_wal
        override_args = (
            expected_override_tags and {"override_tags": expected_override_tags} or {}
        )
        uploader_object_mock.upload_wal.assert_called_once_with(
            wal_name, **override_args
        )

    @pytest.mark.parametrize(
        ("tags_args"),
        [
            # Newline in tag
            ["--tags", "foo,bar\nbaz,qux"],
            # Newline in history_tag
            ["--history-tags", "foo,bar\nbaz,qux"],
            # Carriage return in tag
            ["--tags", "foo,bar\r\nbaz,qux"],
            # Carriage return in history_tag
            ["--history-tags", "foo,bar\r\nbaz,qux"],
            # Too many values in tag
            ["--tags", "foo,bar,baz"],
            # Too many values in history tag
            ["--history-tags", "foo,bar,baz"],
        ],
    )
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_badly_formed_tags(
        self,
        _cloud_interface_mock,
        _uploader_mock,
        tags_args,
        caplog,
    ):
        """Test that badly formed tags are rejected."""
        with pytest.raises(SystemExit) as excinfo:
            cloud_walarchive.main(
                [
                    "s3://test-bucket/testfolder",
                    "test-server",
                    "/path/to/somewhere/000000080000ABFF000000C1",
                ]
                + tags_args
            )
        assert excinfo.value.code == 3

    @pytest.mark.parametrize(
        (
            "azure_client_args",
            "expected_cloud_interface_kwargs",
        ),
        [
            # Defaults should result in CLI defaults which match the Azure
            # defaults
            (
                [],
                {
                    "max_block_size": 4 << 20,
                    "max_concurrency": 1,
                    "max_single_put_size": 64 << 20,
                },
            ),
            # CLI args should override defaults in CLI and AzureCloudInterface
            (
                [
                    "--max-block-size",
                    "1MB",
                    "--max-concurrency",
                    "16",
                    "--max-single-put-size",
                    "8MB",
                ],
                {
                    "max_block_size": 1 << 20,
                    "max_concurrency": 16,
                    "max_single_put_size": 8 << 20,
                },
            ),
        ],
    )
    @mock.patch.dict(
        os.environ, {"AZURE_STORAGE_CONNECTION_STRING": "connection_string"}
    )
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.cloud_providers.azure_blob_storage.AzureCloudInterface")
    def test_wal_archive_azure_upload_block_args(
        self,
        cloud_interface_mock,
        _uploader_mock,
        azure_client_args,
        expected_cloud_interface_kwargs,
    ):
        """Test that azure block upload arguments are passed to the cloud interface."""
        cloud_walarchive.main(
            [
                "https://account.blob.core.windows.net/container/path/to/dir",
                "test-server",
                "000000080000ABFF000000C2",
                "--cloud-provider",
                "azure-blob-storage",
            ]
            + azure_client_args
        )

        # Verify expected kwargs are passed to cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="https://account.blob.core.windows.net/container/path/to/dir",
            encryption_scope=None,
            tags=None,
            **expected_cloud_interface_kwargs
        )

    @pytest.mark.parametrize(
        ("aws_cli_args", "expected_cloud_interface_kwargs"),
        [
            # Defaults should result in None values being passed
            (
                [],
                {
                    "encryption": None,
                    "sse_kms_key_id": None,
                },
            ),
            # If values are provided then they should be passed to the cloud interface
            (
                ["--encryption", "aws:kms", "--sse-kms-key-id", "somekeyid"],
                {
                    "encryption": "aws:kms",
                    "sse_kms_key_id": "somekeyid",
                },
            ),
        ],
    )
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.cloud_providers.aws_s3.S3CloudInterface")
    def test_aws_encryption_args(
        self,
        cloud_interface_mock,
        _uploader_mock,
        aws_cli_args,
        expected_cloud_interface_kwargs,
    ):
        """Verify that AWS encryption arguments are passed to the cloud interface."""
        # WHEN barman-cloud-wal-archive is run with the provided arguments
        cloud_walarchive.main(
            ["cloud_storage_url", "test_server", "000000080000ABFF000000C2"]
            + aws_cli_args
        )

        # THEN they are passed to the cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="cloud_storage_url",
            tags=None,
            profile_name=None,
            endpoint_url=None,
            read_timeout=None,
            **expected_cloud_interface_kwargs
        )

    @pytest.mark.parametrize(
        ("gcp_cli_args", "expected_cloud_interface_kwargs"),
        [
            # Defaults should result in None values being passed
            (
                [],
                {
                    "kms_key_name": None,
                },
            ),
            # If values are provided then they should be passed to the cloud interface
            (
                ["--kms-key-name", "somekeyname"],
                {
                    "kms_key_name": "somekeyname",
                },
            ),
        ],
    )
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader")
    @mock.patch("barman.cloud_providers.google_cloud_storage.GoogleCloudInterface")
    def test_gcp_encryption_args(
        self,
        cloud_interface_mock,
        _uploader_mock,
        gcp_cli_args,
        expected_cloud_interface_kwargs,
    ):
        """Verify that GCP encryption arguments are passed to the cloud interface."""
        # WHEN barman-cloud-wal-archive is run with the provided arguments
        cloud_walarchive.main(
            [
                "cloud_storage_url",
                "test_server",
                "000000080000ABFF000000C2",
                "--cloud-provider",
                "google-cloud-storage",
            ]
            + gcp_cli_args
        )

        # THEN they are passed to the cloud interface
        cloud_interface_mock.assert_called_once_with(
            url="cloud_storage_url",
            tags=None,
            jobs=1,
            **expected_cloud_interface_kwargs
        )


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

    def test_retrieve_snappy_file_obj(self, tmpdir):
        """
        Test the retrieve_file_obj method with a snappy file
        """
        # Setup the WAL
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something".encode("utf-8"), ensure=True)
        # Create a simple CloudWalUploader obj
        uploader = CloudWalUploader(
            mock.MagicMock(), "test-server", compression="snappy"
        )
        open_file = uploader.retrieve_file_obj(source.strpath)
        # Check the in memory file received
        assert open_file
        # Decompress on the fly to check content
        assert snappy.StreamDecompressor().decompress(
            open_file.read()
        ) == "something".encode("utf-8")

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

    def test_retrieve_snappy_file_name(self):
        """
        Test the retrieve_wal_name method with snappy compression
        """
        # Create a fake source name
        source = "wal_dir/000000080000ABFF000000C1"
        uploader = CloudWalUploader(
            mock.MagicMock(), "test-server", compression="snappy"
        )
        wal_final_name = uploader.retrieve_wal_name(source)
        # Check the file name received
        assert wal_final_name
        assert wal_final_name == "000000080000ABFF000000C1.snappy"

    @mock.patch("barman.cloud.CloudInterface")
    @mock.patch("barman.clients.cloud_walarchive.CloudWalUploader.retrieve_file_obj")
    def test_upload_wal(self, rfo_mock, cloud_interface_mock):
        """
        Test upload_wal calls CloudInterface with expected parameters
        """
        bucket_path = "gs://bucket/path/to/dir"
        server_name = "test_server"
        type(cloud_interface_mock).path = mock.PropertyMock(return_value=bucket_path)
        uploader = CloudWalUploader(cloud_interface_mock, server_name)
        source = "/wal_dir/000000080000ABFF000000C1"
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        mock_fileobj_length = 42
        rfo_mock.return_value.tell.return_value = mock_fileobj_length
        uploader.upload_wal(source)

        expected_key = os.path.join(
            bucket_path, server_name, "wals", hash_dir(source), os.path.basename(source)
        )
        cloud_interface_mock.upload_fileobj.assert_called_once_with(
            fileobj=rfo_mock(), key=expected_key, override_tags=None
        )


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
        # Create a simple CloudWalUploader obj
        container_name = "container"
        cloud_interface = AzureCloudInterface(
            url="https://account.blob.core.windows.net/container/path/to/dir"
        )
        uploader = CloudWalUploader(cloud_interface, "test-server")
        source = "/wal_dir/000000080000ABFF000000C1"
        # Simulate the file object returned by the retrieve_file_obj method
        rfo_mock.return_value.name = source
        mock_fileobj_length = 42
        rfo_mock.return_value.tell.return_value = mock_fileobj_length
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
            length=mock_fileobj_length,
            max_concurrency=8,
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
