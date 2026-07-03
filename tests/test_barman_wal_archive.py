# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2026
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

import hashlib
import random
import re
import subprocess
import tarfile
from contextlib import closing
from io import BytesIO

import mock
import pytest

from barman.clients import walarchive


def pipe_helper():
    """
    Create two BytesIO objects (input_mock, output_mock) to simulate a pipe.

    When the input_mock is closed, the content is copied in output_mock,
    ready to be used.

    :rtype: tuple[BytesIO, BytesIO]
    """
    input_mock = BytesIO()
    output_mock = BytesIO()

    # Save the content of input_mock into the output_mock before closing it
    def save_before_close(orig_close=input_mock.close):
        output_mock.write(input_mock.getvalue())
        output_mock.seek(0)
        orig_close()

    input_mock.close = save_before_close
    return input_mock, output_mock


# noinspection PyMethodMayBeStatic
class TestMain(object):
    @pytest.mark.parametrize(
        ["hash_algorithm", "SUMS_FILE", "flag"],
        [("sha256", "SHA256SUMS", ""), ("md5", "MD5SUMS", "--md5")],
    )
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._update_metadata")
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._get_wals_to_prefetch")
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._is_already_archived")
    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_ok(
        self,
        popen_mock,
        mock_is_archived,
        mock_get_prefetch,
        mock_update_metadata,
        hash_algorithm,
        SUMS_FILE,
        flag,
        tmpdir,
    ):
        # Mock the parent class methods to bypass cache/prefetch logic
        mock_is_archived.return_value = False
        mock_get_prefetch.return_value = []

        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        source_hash = source.computehash(hash_algorithm)

        # Prepare the fake Pipe
        input_mock, output_mock = pipe_helper()
        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0

        args_list = [
            "-c",
            "/etc/bwa.conf",
            "-U",
            "user",
            "a.host",
            "a-server",
            source.strpath,
        ]

        if flag:
            args_list.append(flag)

        walarchive.main(args_list)
        popen_mock.assert_called_once_with(
            [
                "ssh",
                "-q",
                "-T",
                "user@a.host",
                "barman",
                "--config='/etc/bwa.conf'",
                "put-wal",
                "a-server",
            ],
            stdin=subprocess.PIPE,
        )

        # Verify the tar content
        tar = tarfile.open(mode="r|", fileobj=output_mock)
        first = tar.next()
        with closing(tar.extractfile(first)) as fp:
            first_content = fp.read().decode()
        assert first.name == "000000080000ABFF000000C1"
        assert first_content == "something"
        second = tar.next()
        with closing(tar.extractfile(second)) as fp:
            second_content = fp.read().decode()
        assert second.name == SUMS_FILE
        assert second_content == "%s *000000080000ABFF000000C1\n" % source_hash
        assert tar.next() is None

    @mock.patch("barman.wal_archiver.ParallelWalArchiver._update_metadata")
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._get_wals_to_prefetch")
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._is_already_archived")
    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_ssh_port(
        self,
        popen_mock,
        mock_is_archived,
        mock_get_prefetch,
        mock_update_metadata,
        tmpdir,
    ):
        # Mock the parent class methods to bypass cache/prefetch logic
        mock_is_archived.return_value = False
        mock_get_prefetch.return_value = []

        # GIVEN a WAL file on disk
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        # AND a fake pipe
        input_mock, _output_mock = pipe_helper()
        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0

        # WHEN barman-wal-archive is called with a custom port option
        walarchive.main(
            [
                "-U",
                "user",
                "--port",
                "8888",
                "test_host",
                "test_server",
                source.strpath,
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
                "user@test_host",
                "barman",
                "put-wal",
                "test_server",
            ],
            stdin=subprocess.PIPE,
        )

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_cache_hit_exits_success(self, archiver_mock, tmpdir):
        """
        main() should exit successfully as soon as the WAL is found in the
        prefetch cache, without inspecting returncode (which stays None
        because no remote process is spawned in that case).
        """
        # GIVEN a WAL file that was already found in the prefetch cache
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        archiver_mock.return_value.found_in_cache = True
        archiver_mock.return_value.returncode = None

        # WHEN barman-wal-archive is executed
        # THEN it returns normally, without raising SystemExit
        walarchive.main(["a.host", "a-server", source.strpath])

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_default_prefetch_cache_dir(self, archiver_mock, tmpdir):
        """
        main() should use the default prefetch cache dir, templated with
        the server name and a hash of the Barman destination, when
        --prefetch-cache-dir is not specified.
        """
        # GIVEN a WAL file on disk
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        archiver_mock.return_value.found_in_cache = True

        # WHEN barman-wal-archive is executed without --prefetch-cache-dir
        walarchive.main(["a.host", "a-server", source.strpath])

        # THEN RemoteWalArchiver is created with the default cache dir,
        # hashing the destination (host, port and config file) it was
        # invoked with
        expected_hash = hashlib.sha256(b"a.host:None:None").hexdigest()[:16]
        archiver_mock.assert_called_once_with(
            "a-server",
            walarchive.PREFETCH_CACHE_DIR.format(
                server_name="a-server", computed_hash=expected_hash
            ),
            mock.ANY,
        )

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_default_prefetch_cache_dir_differs_per_destination(
        self, archiver_mock, tmpdir
    ):
        """
        main() should compute a different default prefetch cache dir for
        each distinct Barman destination (host, port and config file), even
        when the server name is the same, so that separate destinations do
        not collide on the same cache.
        """
        # GIVEN a WAL file on disk
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        archiver_mock.return_value.found_in_cache = True

        # WHEN barman-wal-archive is executed twice for the same server name
        # but against different Barman destinations
        walarchive.main(["a.host", "a-server", source.strpath])
        first_cache_dir = archiver_mock.call_args[0][1]

        archiver_mock.reset_mock()
        walarchive.main(["--port", "2222", "a.host", "a-server", source.strpath])
        second_cache_dir = archiver_mock.call_args[0][1]

        # THEN the two invocations use different cache directories
        assert first_cache_dir != second_cache_dir

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_custom_prefetch_cache_dir(self, archiver_mock, tmpdir):
        """
        main() should use the user-provided --prefetch-cache-dir instead
        of the default when it is specified.
        """
        # GIVEN a WAL file on disk
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)
        archiver_mock.return_value.found_in_cache = True

        # WHEN barman-wal-archive is executed with a custom --prefetch-cache-dir
        walarchive.main(
            [
                "--prefetch-cache-dir",
                "/custom/cache/dir",
                "a.host",
                "a-server",
                source.strpath,
            ]
        )

        # THEN RemoteWalArchiver is created with the custom cache dir
        archiver_mock.assert_called_once_with("a-server", "/custom/cache/dir", mock.ANY)

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_error_dir(self, archiver_mock, tmpdir, capsys):
        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", tmpdir.strpath])

        assert exc.value.code == 2
        assert not archiver_mock.called
        out, err = capsys.readouterr()
        assert not out
        assert "WAL_PATH cannot be a directory" in err

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_error_io(self, archiver_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        archiver_mock.return_value.archive.side_effect = EnvironmentError

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", source.strpath])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert "Error executing ssh" in err

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_error_ssh(self, archiver_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        archiver_mock.return_value.found_in_cache = False
        archiver_mock.return_value.main_wal_returncode = 255

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", source.strpath])

        assert exc.value.code == 3
        out, err = capsys.readouterr()
        assert not out
        assert "Connection problem with ssh" in err

    @mock.patch("barman.clients.walarchive.RemoteWalArchiver")
    def test_error_barman(self, archiver_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        archiver_mock.return_value.found_in_cache = False
        archiver_mock.return_value.main_wal_returncode = 1

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", source.strpath])

        assert exc.value.code == 1
        out, err = capsys.readouterr()
        assert not out
        assert "Remote 'barman put-wal' command has failed" in err

    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_connectivity_test_returns_subprocess_output(self, popen_mock, capsys):
        popen_mock.return_value.communicate.return_value = (
            b"Tested subprocess return code percolation",
            b"",
        )
        popen_mock.return_value.returncode = 255

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", "--test", "dummy_wal"])

        assert exc.value.code == 255
        out, err = capsys.readouterr()
        assert "Tested subprocess return code percolation" in out
        assert not err

    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_connectivity_test_error(self, popen_mock, capsys):
        popen_mock.return_value.communicate.side_effect = subprocess.CalledProcessError(
            255, "remote barman"
        )

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", "--test", "dummy_wal"])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert (
            "ERROR: Impossible to invoke remote put-wal: "
            "Command 'remote barman' returned non-zero "
            "exit status 255"
        ) in err


# noinspection PyMethodMayBeStatic
class TestRemoteWalArchiver(object):
    """Tests for :class:`RemoteWalArchiver`."""

    def _make_config(self, wal_path="/pg_wal/000000010000000000000001"):
        """Return a mock config object."""
        return mock.Mock(
            server_name="test-server",
            wal_path=wal_path,
            user="barman",
            barman_host="barman.example.com",
            config=None,
            port=None,
            md5=False,
            compression=None,
            compression_level=None,
        )

    def test_init_sets_attributes(self):
        """
        __init__ should set the server_name, cache_dir, config, found_in_cache, and main_wal_returncode attributes.
        """
        # GIVEN a config
        config = self._make_config()

        # WHEN creating a RemoteWalArchiver
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)

        # THEN attributes are set correctly
        assert archiver.server_name == "test-server"
        assert archiver.cache_dir == "/tmp/cache"
        assert archiver.config is config
        assert archiver.found_in_cache is False
        assert archiver.main_wal_returncode is None

    @pytest.mark.parametrize("already_archived", [True, False])
    @mock.patch("barman.wal_archiver.ParallelWalArchiver._is_already_archived")
    def test_is_already_archived_tracks_found_in_cache(
        self, mock_super_is_archived, already_archived
    ):
        """
        _is_already_archived should store the parent class result in
        found_in_cache and return it unchanged.
        """
        # GIVEN a RemoteWalArchiver and a parent class result
        mock_super_is_archived.return_value = already_archived
        config = self._make_config()
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)

        # WHEN checking whether a WAL file was already archived
        result = archiver._is_already_archived("/pg_wal/000000010000000000000001")

        # THEN the parent class result is returned unchanged
        assert result is already_archived
        # AND found_in_cache reflects that result
        assert archiver.found_in_cache is already_archived

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_archive_single_wal_creates_remote_put_wal(self, mock_remote_put_wal):
        """
        _archive_single_wal should create a RemotePutWal and wait for it.
        """
        # GIVEN a RemoteWalArchiver
        config = self._make_config()
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)
        mock_remote_put_wal.return_value.returncode = 0

        # WHEN archiving a WAL file
        archiver._archive_single_wal("/pg_wal/000000010000000000000002")

        # THEN RemotePutWal was created with the config and path
        mock_remote_put_wal.assert_called_once_with(
            config, "/pg_wal/000000010000000000000002"
        )

        # AND wait() was called
        mock_remote_put_wal.return_value.wait.assert_called_once()

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_archive_single_wal_tracks_main_wal_returncode(self, mock_remote_put_wal):
        """
        _archive_single_wal should track the main WAL process returncode when archiving
        the WAL specified in config.wal_path.
        """
        # GIVEN a RemoteWalArchiver with a specific wal_path in config
        wal_path = "/pg_wal/000000010000000000000001"
        config = self._make_config(wal_path=wal_path)
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)
        mock_remote_put_wal.return_value.returncode = 0

        # WHEN archiving the main WAL (matching config.wal_path)
        archiver._archive_single_wal(wal_path)

        # THEN main_wal_returncode is set
        assert (
            archiver.main_wal_returncode is mock_remote_put_wal.return_value.returncode
        )

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_archive_single_wal_does_not_track_prefetch_wal(self, mock_remote_put_wal):
        """
        _archive_single_wal should NOT track prefetch WAL processes.
        """
        # GIVEN a RemoteWalArchiver
        config = self._make_config(wal_path="/pg_wal/000000010000000000000001")
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)
        mock_remote_put_wal.return_value.returncode = 0

        # WHEN archiving a prefetch WAL (NOT matching config.wal_path)
        archiver._archive_single_wal("/pg_wal/000000010000000000000002")

        # THEN main_wal_returncode is NOT set
        assert archiver.main_wal_returncode is None

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_returncode_returns_main_process_returncode(self, mock_remote_put_wal):
        """
        returncode should return the exit code of the main WAL process.
        """
        # GIVEN a RemoteWalArchiver that has archived the main WAL
        config = self._make_config(wal_path="/pg_wal/000000010000000000000001")
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)
        mock_remote_put_wal.return_value.returncode = 0

        archiver._archive_single_wal("/pg_wal/000000010000000000000001")

        # WHEN checking returncode
        result = archiver.main_wal_returncode

        # THEN it returns the process returncode
        assert result == 0

    def test_returncode_returns_none_when_no_main_process(self):
        """
        returncode should return None if no main WAL was archived yet.
        """
        # GIVEN a RemoteWalArchiver with no archival done
        config = self._make_config()
        archiver = walarchive.RemoteWalArchiver("test-server", "/tmp/cache", config)

        # WHEN checking returncode
        result = archiver.main_wal_returncode

        # THEN it returns None
        assert result is None


# noinspection PyMethodMayBeStatic
class TestRemotePutWal(object):
    @pytest.mark.parametrize(
        ("hash_algorithm", "SUMS_FILE", "flag"),
        [("md5", "MD5SUMS", True), ("sha256", "SHA256SUMS", False)],
    )
    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_str_source_file(self, popen_mock, hash_algorithm, SUMS_FILE, flag, tmpdir):
        input_mock, output_mock = pipe_helper()

        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0
        config = mock.Mock(
            user="barman",
            barman_host="remote.barman.host",
            config=None,
            server_name="this-server",
            test=False,
            port=None,
            md5=flag,
            compression=None,
            compression_level=None,
        )
        source_file = tmpdir.join("test-source/000000010000000000000001")
        source_file.write("test-content", ensure=True)
        source_path = source_file.strpath

        # In python2 the source_path can be an unicode object
        if hasattr(source_path, "decode"):
            source_path = source_path.decode()

        rpw = walarchive.RemotePutWal(config, source_path)

        popen_mock.assert_called_once_with(
            [
                "ssh",
                "-q",
                "-T",
                "barman@remote.barman.host",
                "barman",
                "put-wal",
                "this-server",
            ],
            stdin=subprocess.PIPE,
        )

        assert rpw.returncode == 0

        tar = tarfile.open(mode="r|", fileobj=output_mock)
        first = tar.next()
        with closing(tar.extractfile(first)) as fp:
            first_content = fp.read().decode()
        assert first.name == "000000010000000000000001"
        assert first_content == "test-content"
        second = tar.next()
        with closing(tar.extractfile(second)) as fp:
            second_content = fp.read().decode()
        assert second.name == SUMS_FILE
        assert (
            second_content
            == "%s *000000010000000000000001\n"
            % source_file.computehash(hash_algorithm)
        )
        assert tar.next() is None

    @pytest.mark.parametrize(
        ("compression",),
        [("gzip",), ("bzip2",), ("xz",), ("zstd",), ("lz4",), ("snappy",)],
    )
    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    @mock.patch("barman.clients.walarchive.get_internal_compressor")
    def test_compression(self, mock_get_compressor, popen_mock, compression, tmpdir):
        # Mock the popen and config objects to behave accordingly
        input_mock, output_mock = pipe_helper()
        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0
        config = mock.Mock(
            user="barman",
            barman_host="remote.barman.host",
            config=None,
            server_name="this-server",
            test=False,
            port=None,
            md5=False,
            compression=compression,
            compression_level=6,
        )
        source_file = tmpdir.join("test-source/000000010000000000000001")
        source_file.write("test-content", ensure=True)

        # Mock the compressor to be used when compressing the file
        mock_compressor = mock.Mock()
        mock_compressor.compress.side_effect = lambda src, dst: open(
            dst, mode="w+"
        ).write("compressed-content")
        mock_get_compressor.return_value = mock_compressor

        # Call remote put wal
        rpw = walarchive.RemotePutWal(config, source_file.strpath)

        # Assert it called popen correctly
        popen_mock.assert_called_once_with(
            [
                "ssh",
                "-q",
                "-T",
                "barman@remote.barman.host",
                "barman",
                "put-wal",
                "this-server",
            ],
            stdin=subprocess.PIPE,
        )
        assert rpw.returncode == 0

        # Assert it called get_internal_compressor correctly
        mock_get_compressor.assert_called_once_with(
            config.compression, config.compression_level
        )
        # Assert the compress method of the compress was called correctly
        # We accept ANY destination path parameter as it is randomly generated due to
        # it being a temporary file
        mock_compressor.compress.assert_called_once_with(source_file.strpath, mock.ANY)

        tar = tarfile.open(mode="r|", fileobj=output_mock)
        first = tar.next()
        with closing(tar.extractfile(first)) as fp:
            # Assert that the WAL (first) was compressed with the specified algorithm
            data = fp.read()
            data == b"compressed-content"

            # Assert that the checksum file (second) was created correctly
            second = tar.next()
            with closing(tar.extractfile(second)) as fp2:
                second_content = fp2.read().decode()
                assert second.name == "SHA256SUMS"
                assert (
                    second_content
                    == "%s *000000010000000000000001\n"
                    % hashlib.sha256(data).hexdigest()
                )
        assert tar.next() is None

    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_error(self, popen_mock, tmpdir):
        input_mock = BytesIO()

        popen_mock.return_value.stdin = input_mock
        config = mock.Mock(
            user="barman",
            barman_host="remote.barman.host",
            config=None,
            server_name="this-server",
            test=False,
            port=None,
            md5=False,
            compression=None,
            compression_level=None,
        )
        source_file = tmpdir.join("test-source/000000010000000000000001")
        source_file.write("test-content", ensure=True)
        source_path = source_file.strpath

        # Simulate a remote failure
        popen_mock.return_value.returncode = 5

        # In python2 the source_path can be an unicode object
        if hasattr(source_path, "decode"):
            source_path = source_path.decode()

        rwa = walarchive.RemotePutWal(config, source_path)

        popen_mock.assert_called_once_with(
            [
                "ssh",
                "-q",
                "-T",
                "barman@remote.barman.host",
                "barman",
                "put-wal",
                "this-server",
            ],
            stdin=subprocess.PIPE,
        )

        assert rwa.returncode == 5


# noinspection PyMethodMayBeStatic
class TestChecksumTarFile(object):
    @pytest.mark.parametrize(
        ["hash_algorithm", "SUMS_FILE"], [("sha256", "SHA256SUMS"), ("md5", "MD5SUMS")]
    )
    def test_tar(self, hash_algorithm, SUMS_FILE, tmpdir):
        # Prepare some content
        source = tmpdir.join("source.file")
        source.write("something", ensure=True)
        source.setmtime(source.mtime() - 100)  # Set mtime to 100 seconds ago
        source_hash = source.computehash(hash_algorithm)

        # Write the content in a tar file
        storage = tmpdir.join("storage.tar")
        with closing(
            walarchive.ChecksumTarFile.open(storage.strpath, mode="w:")
        ) as tar:
            tar.hash_algorithm = hash_algorithm
            tar.HASHSUMS_FILE = SUMS_FILE
            tar.add(source.strpath, source.basename)
            checksum = tar.members[0].data_checksum
            assert checksum == source_hash

        # Double close should not give any issue
        tar.close()

        lab = tmpdir.join("lab").ensure(dir=True)
        tar = tarfile.open(storage.strpath, mode="r:")
        tar.extractall(lab.strpath)
        tar.close()

        dest_file = lab.join(source.basename)
        sum_file = lab.join(SUMS_FILE)
        sums = {}
        for line in sum_file.readlines():
            checksum, name = re.split(r" [* ]", line.rstrip(), 1)
            sums[name] = checksum

        assert list(sums.keys()) == [source.basename]
        assert sums[source.basename] == source_hash
        assert dest_file.computehash(hash_algorithm) == source_hash
        # Verify file mtime
        # Use a round(2) comparison because float is not precise in Python 2.x
        assert round(dest_file.mtime(), 2) == round(source.mtime(), 2)

    @pytest.mark.parametrize(
        ["hash_algorithm", "size", "mode"],
        [
            ["sha256", 0, 0],
            ["sha256", 10, None],
            ["sha256", 10, 0],
            ["sha256", 10, 1],
            ["sha256", 10, -5],
            ["sha256", 16 * 1024, 0],
            ["sha256", 32 * 1024 - 1, -1],
            ["sha256", 32 * 1024 - 1, 0],
            ["sha256", 32 * 1024 - 1, 1],
            ["md5", 0, 0],
            ["md5", 10, None],
            ["md5", 10, 0],
            ["md5", 10, 1],
            ["md5", 10, -5],
            ["md5", 16 * 1024, 0],
            ["md5", 32 * 1024 - 1, -1],
            ["md5", 32 * 1024 - 1, 0],
            ["md5", 32 * 1024 - 1, 1],
        ],
    )
    def test_hashCopyfileobj(self, hash_algorithm, size, mode):
        """
        Test hashCopyfileobj different size.

        If mode is None, copy the whole data.
        If mode is <= 0, copy the data passing the exact length.
        If mode is > 0, require more bytes than available, raising an error

        :param int size: The size of random data to use for the test
        :param int|None mode: the mode of operation, see above description
        """
        src = BytesIO()
        dst = BytesIO()

        # Generate `size` random bytes
        src_string = bytearray(random.getrandbits(8) for _ in range(size))
        src.write(src_string)
        src.seek(0)

        if mode and mode > 0:
            # Require more bytes than available. Make sure to get an exception
            with pytest.raises(IOError):
                walarchive.hashCopyfileobj(
                    src, dst, size + mode, hash_algorithm=hash_algorithm
                )
        else:
            if mode is None:
                # Copy the whole file until the end
                checksum = walarchive.hashCopyfileobj(
                    src, dst, hash_algorithm=hash_algorithm
                )
            else:
                # Copy only a portion of the file
                checksum = walarchive.hashCopyfileobj(
                    src, dst, size + mode, hash_algorithm=hash_algorithm
                )
                src_string = src_string[0 : size + mode]

            # Validate the content and the checksum
            assert dst.getvalue() == src_string
            assert (
                checksum == hashlib.new(hash_algorithm, bytes(src_string)).hexdigest()
            )
