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
    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_ok(self, popen_mock, hash_algorithm, SUMS_FILE, flag, tmpdir):
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

    @mock.patch("barman.clients.walarchive.subprocess.Popen")
    def test_ssh_port(self, popen_mock, tmpdir):
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

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_error_dir(self, rpw_mock, tmpdir, capsys):
        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", tmpdir.strpath])

        assert exc.value.code == 2
        assert not rpw_mock.called
        out, err = capsys.readouterr()
        assert not out
        assert "WAL_PATH cannot be a directory" in err

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_error_io(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        rpw_mock.side_effect = EnvironmentError

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", source.strpath])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert "Error executing ssh" in err

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_error_ssh(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        rpw_mock.return_value.returncode = 255

        with pytest.raises(SystemExit) as exc:
            walarchive.main(["a.host", "a-server", source.strpath])

        assert exc.value.code == 3
        out, err = capsys.readouterr()
        assert not out
        assert "Connection problem with ssh" in err

    @mock.patch("barman.clients.walarchive.RemotePutWal")
    def test_error_barman(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join("wal_dir/000000080000ABFF000000C1")
        source.write("something", ensure=True)

        rpw_mock.return_value.returncode = 1

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
