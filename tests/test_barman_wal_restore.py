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
import os
import subprocess
from io import BytesIO

import mock
import pytest

from barman.clients import walrestore


# noinspection PyMethodMayBeStatic
class TestRemoteGetWal(object):
    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    @mock.patch("barman.clients.walrestore.shutil")
    def test_string_dest_file(self, shutil_mock, popen_mock, tmpdir):
        config = mock.Mock(
            compression=False,
            user="barman",
            barman_host="remote.barman.host",
            config=None,
            server_name="this-server",
        )
        dest_file = tmpdir.join("test-dest").strpath

        # dest_file is a str object
        walrestore.RemoteGetWal(config, "000000010000000000000001", dest_file)

        # In python2 the dest_file can be an unicode object
        if hasattr(dest_file, "decode"):
            walrestore.RemoteGetWal(
                config, "000000010000000000000001", dest_file.decode()
            )

    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    def test_connectivity_test_ok(self, popen_mock, capsys):
        popen_mock.return_value.communicate.return_value = ("Good test!", "")
        popen_mock.return_value.returncode = 0

        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "--test", "dummy_wal", "dummy_dest"])

        assert exc.value.code == 0
        out, err = capsys.readouterr()
        assert "Good test!" in out
        assert not err

    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    def test_connectivity_test_error(self, popen_mock, capsys):
        popen_mock.return_value.communicate.side_effect = subprocess.CalledProcessError(
            255, "remote barman"
        )

        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "--test", "dummy_wal", "dummy_dest"])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert (
            "ERROR: Impossible to invoke remote get-wal: "
            "Command 'remote barman' returned non-zero "
            "exit status 255"
        ) in err

    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    @mock.patch("barman.clients.walrestore.shutil")
    def test_ssh_port(self, shutil_mock, popen_mock):
        # WHEN barman-wal-restore is called with the --port option
        with pytest.raises(SystemExit):
            walrestore.main(
                [
                    "test_host",
                    "test_server",
                    "test_wal",
                    "test_wal_dest",
                    "--port",
                    "8888",
                ]
            )

        # THEN the ssh command is called with the -p option
        popen_mock.assert_called_once_with(
            [
                "ssh",
                "-p",
                "8888",
                "-q",
                "-T",
                "barman@test_host",
                "barman",
                "get-wal 'test_server' 'test_wal'",
            ],
            stdout=mock.ANY,
        )
        # Clean created file
        os.remove("test_wal_dest")

    @mock.patch("barman.clients.walrestore.RemoteGetWal")
    def test_ssh_connectivity_error(self, remote_get_wal_mock, capsys, tmpdir):
        """Verifies exit status is 2 when ssh connectivity fails."""
        mock_ssh_process = remote_get_wal_mock.return_value
        mock_ssh_process.returncode = 255

        dest_path = tmpdir.join("dummy_dest").strpath
        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "dummy_wal", dest_path])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert ("ERROR: Connection problem with ssh\n") in err

    @mock.patch("barman.clients.walrestore.RemoteGetWal")
    def test_ssh_exit_code_is_passed_through(self, remote_get_wal_mock, capsys, tmpdir):
        """Verifies non-255 SSH exit codes are passed through."""
        mock_ssh_process = remote_get_wal_mock.return_value
        mock_ssh_process.returncode = 1

        dest_path = tmpdir.join("dummy_dest").strpath
        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "dummy_wal", dest_path])

        assert exc.value.code == 1
        out, err = capsys.readouterr()
        assert not out
        assert ("ERROR: Remote 'barman get-wal' command has failed!\n") in err

    @mock.patch("barman.clients.walrestore.os.path.isdir")
    def test_exit_code_if_wal_dest_is_dir(self, isdir_mock, capsys):
        """Verifies exit status 3 when destination is a directory."""
        isdir_mock.return_value = True

        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "dummy_wal", "dummy_dest"])

        assert exc.value.code == 3
        out, err = capsys.readouterr()
        assert not out
        assert ("ERROR: WAL_DEST cannot be a directory: dummy_dest\n") in err

    @mock.patch("barman.clients.walrestore.open")
    @mock.patch("barman.clients.walrestore.os.path.isdir")
    def test_exit_code_if_wal_dest_not_writable(self, isdir_mock, open_mock, capsys):
        """Verifies exit status 3 when destination is not writable."""
        isdir_mock.return_value = False
        open_mock.side_effect = EnvironmentError("error")

        with pytest.raises(SystemExit) as exc:
            walrestore.main(["a.host", "a-server", "dummy_wal", "dummy_dest"])

        assert exc.value.code == 3
        out, err = capsys.readouterr()
        assert not out
        assert (
            "ERROR: Cannot open 'dummy_dest' (WAL_DEST) for writing: error\n"
        ) in err

    @mock.patch("barman.clients.walrestore.get_server_config_minimal")
    @mock.patch("barman.clients.walrestore.CompressionManager")
    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    @mock.patch("barman.clients.walrestore.build_ssh_command")
    def test_fetch_uncompressed_file(
        self,
        mock_build_command,
        mock_popen,
        mock_comp_manager,
        _mock_get_server_config_minimal,
    ):
        """
        Test fetching an uncompressed WAL file. The result of ``get-wal``
        is simply written to the destination file.
        """
        # GIVEN the following parameters
        config = mock.Mock()
        wal_name = "000000010000000000000001"
        dest_file = mock.Mock(wraps=BytesIO())

        # Make sure no compression is detected on the WAL file
        mock_comp_manager.return_value.identify_compression.return_value = None

        # WHEN RemoteGetWal is called
        walrestore.RemoteGetWal(config, wal_name, dest_file)

        # THEN the ssh command is built and called, and the output is written to dest_file
        mock_build_command.assert_called_once_with(config, wal_name)
        mock_popen.assert_called_once_with(
            mock_build_command.return_value, stdout=dest_file
        )

        # Make sure the file is rewound and closed
        dest_file.seek.assert_called_once_with(0)
        dest_file.close.assert_called_once()

    @mock.patch("barman.clients.walrestore.get_server_config_minimal")
    @mock.patch(
        "barman.clients.walrestore.BytesIO", return_value=mock.Mock(read=BytesIO())
    )
    @mock.patch("barman.clients.walrestore.CompressionManager")
    @mock.patch("barman.clients.walrestore.shutil")
    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    @mock.patch("barman.clients.walrestore.build_ssh_command")
    def test_fetch_compressed_file(
        self,
        mock_build_command,
        mock_popen,
        mock_shutil,
        mock_comp_manager,
        mock_bytesio,
        _mock_get_server_config_minimal,
    ):
        """
        Test fetching a compressed WAL file. The result of ``get-wal`` is
        decompressed and written to the destination file.
        """
        # GIVEN the following parameters
        config = mock.Mock()
        wal_name = "000000010000000000000001"
        dest_file = mock.Mock(wraps=BytesIO())

        # A compression is to be detected on the WAL file
        mock_comp_manager.return_value.identify_compression.return_value = "gzip"

        # WHEN RemoteGetWal is called
        walrestore.RemoteGetWal(config, wal_name, dest_file)

        # THEN the ssh command is built and called, and the output is written to dest_file
        mock_build_command.assert_called_once_with(config, wal_name)
        mock_popen.assert_called_once_with(
            mock_build_command.return_value, stdout=dest_file
        )
        # Make sure the file is rewound
        dest_file.seek.assert_called_with(0)

        # AND THEN the file is decompressed using the appropariate compressor object
        compressor = mock_comp_manager.return_value.get_compressor.return_value
        # Decompression consists of
        # 1) Getting the decompressed file-like object
        compressor.decompress_in_mem.assert_called_once_with(dest_file)
        dec_fileobj = compressor.decompress_in_mem.return_value
        # 2) Copying the decompressed content to a BytesIO object
        mock_bytesio.assert_called_with(dec_fileobj.read.return_value)
        # 3) Erasing and rewinding the destination file
        dest_file.truncate.assert_called_once_with(0)
        dest_file.seek.assert_called_with(0)
        # 4) Copying the decompressed content to the destination file
        mock_shutil.copyfileobj.assert_called_once_with(
            mock_bytesio.return_value, dest_file
        )

        # Make sure the destination file is closed at the end
        dest_file.close.assert_called_with()

    @mock.patch("barman.clients.walrestore.get_server_config_minimal")
    @mock.patch("barman.clients.walrestore.CompressionManager")
    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    @mock.patch("barman.clients.walrestore.build_ssh_command")
    @mock.patch("barman.clients.walrestore.open", return_value=BytesIO())
    def test_dest_file_is_string(
        self,
        mock_open,
        mock_build_command,
        mock_popen,
        mock_comp_manager,
        _mock_get_server_config_minimal,
    ):
        """
        Simple test to ensure that the destination file can also be a string.
        In this case, the file is opened in write-binary mode at the beginning.
        """
        # GIVEN the following parameters
        config = mock.Mock()
        wal_name = "000000010000000000000001"
        dest_file = "/path/to/destination/file"

        # Make sure no compression is detected on the WAL file
        mock_comp_manager.return_value.identify_compression.return_value = None

        # WHEN RemoteGetWal is called
        walrestore.RemoteGetWal(config, wal_name, dest_file)

        # THEN make sure the file was opened at the beginning
        mock_open.assert_called_once_with(dest_file, "wb+")

    @pytest.mark.parametrize("returncode", [0, 1, 255])
    def test_returncode(self, returncode):
        """
        Test the :attr:`returncode` property of :class:`RemoteGetWal`.
        It should return the return code of the underlying SSH Popen process.
        """
        # GIVEN a RemoteGetWal instance
        # NOTE: we mock the __init__ method to avoid unnecessary
        # as we don't need to initialize a complete instance to test this
        walrestore.RemoteGetWal.__init__ = lambda self, *args, **kwargs: None
        walrestore.RemoteGetWal.ssh_process = mock.Mock()
        walrestore.RemoteGetWal.ssh_process.returncode = returncode

        # WHEN we create an instance of RemoteGetWal
        remote_get_wal = walrestore.RemoteGetWal(
            config=mock.Mock(),
            wal_name="000000010000000000000001",
            dest_file=mock.Mock(),
        )

        # THEN its returncode should be the same as its ssh_process returncode
        assert remote_get_wal.returncode == returncode


@mock.patch("barman.clients.walrestore.WorkerProcess")
def test_spawn_additional_process(mock_worker_process):
    """
    Test that :func:`spawn_additional_process` spawn the correct processes
    based on the provided additional files.
    """
    # GIVEN the following parameters
    config = mock.Mock(spool_dir="/var/tmp/walrestore")
    additional_files = [
        "000000010000000000000001",
        "000000010000000000000002",
        "000000010000000000000003",
    ]

    # WHEN spawn_additional_process is called
    ret = walrestore.spawn_additional_process(config, additional_files)

    # THEN it should spawn a WorkerProcess for each additional file
    for wal_name in additional_files:
        spool_file_name = os.path.join(config.spool_dir, wal_name)
        mock_worker_process.assert_any_call(
            target=walrestore.RemoteGetWal,
            name="RemoteGetWal-%s" % wal_name,
            args=(config, wal_name, spool_file_name),
            kwargs={"is_worker_process": True},
            spool_file_name=spool_file_name,
        )

    # Ensure all processes were started
    mock_worker_process.return_value.start.call_count == len(additional_files)

    # Ensure the return value is a list of all worker processes spawned
    assert ret == [mock_worker_process.return_value for _ in additional_files]


@mock.patch("barman.clients.walrestore.execute_peek")
@mock.patch("barman.clients.walrestore.os.path.exists", return_value=True)
@pytest.mark.parametrize("parallel", [None, 3, 10])
def test_peek_additional_files(_mock_path_exists, mock_execute_peek, parallel):
    """
    Test that :func:`peek_additional_files` works correctly. It should return a list of
    additional WAL files to be fetched, excluding the first one (the main WAL file).
    If `parallel` is None, it should return an empty list.
    """
    # GIVEN the following parameters
    config = mock.Mock(
        parallel=parallel,
        wal_name="000000010000000000000001",
        spool_dir="/var/tmp/walrestore",
    )
    # Mock the execute_peek function to return a proper list of WAL names
    wal_names = (
        [f"00000001000000000000000{i}" for i in range(1, parallel + 1)]
        if parallel
        else []
    )
    mock_execute_peek.return_value = wal_names.copy()

    # WHEN peek_additional_files is called
    ret = walrestore.peek_additional_files(config)

    if not parallel:
        # THEN it should return an empty list
        assert ret == []
    else:
        # THEN it should call execute_peek with the provided config
        mock_execute_peek.assert_called_once_with(config)
        # AND return the list of additional files (excluding the first one)
        assert ret == wal_names[1:]


@mock.patch("barman.clients.walrestore.os.mkdir")
@mock.patch("barman.clients.walrestore.os.path.exists", return_value=False)
@mock.patch("barman.clients.walrestore.execute_peek")
def test_peek_additional_files_spool_dir_is_created(
    mock_execute_peek, mock_path_exists, mock_mkdir
):
    """
    Test that :func:`peek_additional_files` creates the spool directory
    if it does not exist.
    """
    # GIVEN the following parameters
    config = mock.Mock(
        parallel=3,
        wal_name="000000010000000000000001",
        spool_dir="/var/tmp/walrestore",
    )
    # Mock the execute_peek function to return a proper list of WAL names
    mock_execute_peek.return_value = [
        f"00000001000000000000000{i}" for i in range(1, config.parallel + 1)
    ]

    # WHEN peek_additional_files is called
    walrestore.peek_additional_files(config)

    # THEN it should make sure the spool directory exists at the beginning
    mock_mkdir.assert_called_once_with(config.spool_dir)


@pytest.mark.parametrize(
    "config, wal_name, peek, expected_command",
    [
        (
            mock.Mock(
                barman_host="my.barman.host",
                server_name="test_server",
                user="barman",
                port=None,
                config=None,
                test=None,
                compression=None,
                keep_compression=False,
                partial=False,
            ),
            "000000010000000000000001",
            None,
            [
                "ssh",
                "-q",
                "-T",
                "barman@my.barman.host",
                "barman",
                "get-wal 'test_server' '000000010000000000000001'",
            ],
        ),
        (
            mock.Mock(
                barman_host="my.barman.host",
                server_name="test_server",
                user="barman",
                port="22",
                config="/etc/barman.conf",
                test=True,
                compression=None,
                keep_compression=False,
                partial=False,
            ),
            "000000010000000000000001",
            None,
            [
                "ssh",
                "-p",
                "22",
                "-q",
                "-T",
                "barman@my.barman.host",
                "barman",
                "--config /etc/barman.conf",
                "get-wal --test 'test_server' '000000010000000000000001'",
            ],
        ),
        (
            mock.Mock(
                barman_host="my.barman.host",
                server_name="test_server",
                user="barman",
                port=None,
                config=None,
                test=None,
                compression="gzip",
                keep_compression=False,
                partial=False,
            ),
            "000000010000000000000001",
            None,
            [
                "ssh",
                "-q",
                "-T",
                "barman@my.barman.host",
                "barman",
                "get-wal --gzip 'test_server' '000000010000000000000001'",
            ],
        ),
        (
            mock.Mock(
                barman_host="my.barman.host",
                server_name="test_server",
                user="barman",
                port=None,
                config=None,
                test=None,
                compression=None,
                keep_compression=True,
                partial=False,
            ),
            "000000010000000000000001",
            None,
            [
                "ssh",
                "-q",
                "-T",
                "barman@my.barman.host",
                "barman",
                "get-wal --keep-compression 'test_server' '000000010000000000000001'",
            ],
        ),
        (
            mock.Mock(
                barman_host="my.barman.host",
                server_name="test_server",
                user="barman",
                port=None,
                config=None,
                test=None,
                compression=None,
                keep_compression=False,
                partial=True,
            ),
            "000000010000000000000001",
            10,
            [
                "ssh",
                "-q",
                "-T",
                "barman@my.barman.host",
                "barman",
                "get-wal --peek '10' --partial 'test_server' '000000010000000000000001'",
            ],
        ),
    ],
)
def test_build_ssh_command(config, wal_name, peek, expected_command):
    """Test the build_ssh_command function with different configurations"""
    command = walrestore.build_ssh_command(config, wal_name, peek)
    assert command == expected_command


@mock.patch("barman.clients.walrestore.subprocess.Popen")
@mock.patch("barman.clients.walrestore.build_ssh_command")
def test_execute_peek(mock_build_ssh_command, mock_popen):
    """
    Test that :func:`execute_peek` works correctly. It should use the ssh ``get-wal``
    command to peek at the WAL files and return a list of their names.
    """
    # GIVEN a configuration object
    config = mock.Mock(wal_name="000000010000000000000001", parallel=3)
    # Mock the popen call to return fake WAL names
    mock_popen.return_value.communicate.return_value = (
        b"000000010000000000000001\n000000010000000000000002\n000000010000000000000003",
        None,
    )

    # WHEN execute_peek is called
    ret = walrestore.execute_peek(config)

    # THEN it should build the ssh command
    mock_build_ssh_command.assert_called_once_with(
        config, config.wal_name, config.parallel
    )
    # AND call subprocess.Popen with the built command
    mock_popen.assert_called_once_with(
        mock_build_ssh_command.return_value, stdout=subprocess.PIPE
    )
    mock_popen.return_value.communicate.assert_called_once()

    # AND return a list of WAL names correctly
    assert ret == [
        "000000010000000000000001",
        "000000010000000000000002",
        "000000010000000000000003",
    ]


@mock.patch("barman.clients.walrestore.subprocess.Popen")
@mock.patch("barman.clients.walrestore.build_ssh_command")
@mock.patch("barman.clients.walrestore.exit_with_error")
def test_execute_peek_failed(mock_exit_with_error, _mock_build_ssh_command, mock_popen):
    # GIVEN a configuration object
    config = mock.Mock(wal_name="000000010000000000000001", parallel=3)
    # Mock the popen call to raise an exception
    exception = subprocess.CalledProcessError(1, "get-wal command failed!")
    mock_popen.return_value.communicate.side_effect = exception

    # WHEN execute_peek is called
    walrestore.execute_peek(config)

    # THEN it should exit with an error
    mock_exit_with_error.assert_called_once_with(
        "Impossible to invoke remote get-wal --peek: %s" % exception
    )


@mock.patch("barman.clients.walrestore.os.path.exists")
@mock.patch("barman.clients.walrestore.shutil.move")
def test_try_deliver_from_spool(mock_move, mock_path_exists):
    """
    Test that :func:`try_deliver_from_spool` correctly delivers a WAL file
    from the spool directory if it exists.
    """
    # GIVEN the following parameters
    config = mock.Mock(
        wal_name="000000010000000000000001", spool_dir="/var/tmp/walrestore"
    )
    dest_file = "/path/to/destination/file"

    # Mock the existence of the spool file
    with mock.patch("barman.clients.walrestore.os.path.exists", return_value=True):
        spool_file = os.path.join(config.spool_dir, config.wal_name)
        with pytest.raises(SystemExit):
            # WHEN try_deliver_from_spool is called
            walrestore.try_deliver_from_spool(config, dest_file)
            # THEN it should move the spool file to the destination
            mock_move.assert_called_once_with(spool_file, dest_file)
