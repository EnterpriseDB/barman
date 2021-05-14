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

    @mock.patch('barman.clients.walarchive.subprocess.Popen')
    def test_ok(self, popen_mock, tmpdir):
        # Prepare some content
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something', ensure=True)
        source_hash = source.computehash()

        # Prepare the fake Pipe
        input_mock, output_mock = pipe_helper()
        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0

        walarchive.main(['-c', '/etc/bwa.conf', '-U', 'user', 'a.host',
                         'a-server', source.strpath])

        popen_mock.assert_called_once_with(
            ['ssh', '-q', '-T', 'user@a.host',
             'barman', "--config='/etc/bwa.conf'", 'put-wal', 'a-server'],
            stdin=subprocess.PIPE)

        # Verify the tar content
        tar = tarfile.open(mode='r|', fileobj=output_mock)
        first = tar.next()
        with closing(tar.extractfile(first)) as fp:
            first_content = fp.read().decode()
        assert first.name == '000000080000ABFF000000C1'
        assert first_content == 'something'
        second = tar.next()
        with closing(tar.extractfile(second)) as fp:
            second_content = fp.read().decode()
        assert second.name == 'MD5SUMS'
        assert second_content == \
            '%s *000000080000ABFF000000C1\n' % source_hash
        assert tar.next() is None

    @mock.patch('barman.clients.walarchive.RemotePutWal')
    def test_error_dir(self, rpw_mock, tmpdir, capsys):

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', tmpdir.strpath])

        assert exc.value.code == 2
        assert not rpw_mock.called
        out, err = capsys.readouterr()
        assert not out
        assert 'WAL_PATH cannot be a directory' in err

    @mock.patch('barman.clients.walarchive.RemotePutWal')
    def test_error_io(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something', ensure=True)

        rpw_mock.side_effect = EnvironmentError

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', source.strpath])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert 'Error executing ssh' in err

    @mock.patch('barman.clients.walarchive.RemotePutWal')
    def test_error_ssh(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something', ensure=True)

        rpw_mock.return_value.returncode = 255

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', source.strpath])

        assert exc.value.code == 3
        out, err = capsys.readouterr()
        assert not out
        assert 'Connection problem with ssh' in err

    @mock.patch('barman.clients.walarchive.RemotePutWal')
    def test_error_barman(self, rpw_mock, tmpdir, capsys):
        # Prepare some content
        source = tmpdir.join('wal_dir/000000080000ABFF000000C1')
        source.write('something', ensure=True)

        rpw_mock.return_value.returncode = 1

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', source.strpath])

        assert exc.value.code == 1
        out, err = capsys.readouterr()
        assert not out
        assert "Remote 'barman put-wal' command has failed" in err

    @mock.patch('barman.clients.walarchive.subprocess.Popen')
    def test_connectivity_test_ok(self, popen_mock, capsys):

        popen_mock.return_value.communicate.return_value = ('Good test!', '')

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', '--test', 'dummy_wal'])

        assert exc.value.code == 0
        out, err = capsys.readouterr()
        assert "Good test!" in out
        assert not err

    @mock.patch('barman.clients.walarchive.subprocess.Popen')
    def test_connectivity_test_error(self, popen_mock, capsys):

        popen_mock.return_value.communicate.side_effect = subprocess.\
            CalledProcessError(255, "remote barman")

        with pytest.raises(SystemExit) as exc:
            walarchive.main(['a.host', 'a-server', '--test', 'dummy_wal'])

        assert exc.value.code == 2
        out, err = capsys.readouterr()
        assert not out
        assert ("ERROR: Impossible to invoke remote put-wal: "
                "Command 'remote barman' returned non-zero "
                "exit status 255") in err


# noinspection PyMethodMayBeStatic
class TestRemotePutWal(object):

    @mock.patch('barman.clients.walarchive.subprocess.Popen')
    def test_str_source_file(self, popen_mock, tmpdir):
        input_mock, output_mock = pipe_helper()

        popen_mock.return_value.stdin = input_mock
        popen_mock.return_value.returncode = 0
        config = mock.Mock(
            user='barman',
            barman_host='remote.barman.host',
            config=None,
            server_name='this-server',
            test=False)
        source_file = tmpdir.join('test-source/000000010000000000000001')
        source_file.write("test-content", ensure=True)
        source_path = source_file.strpath

        # In python2 the source_path can be an unicode object
        if hasattr(source_path, 'decode'):
            source_path = source_path.decode()

        rpw = walarchive.RemotePutWal(config, source_path)

        popen_mock.assert_called_once_with(
            ['ssh', '-q', '-T', 'barman@remote.barman.host',
             'barman', 'put-wal', 'this-server'], stdin=subprocess.PIPE)

        assert rpw.returncode == 0

        tar = tarfile.open(mode='r|', fileobj=output_mock)
        first = tar.next()
        with closing(tar.extractfile(first)) as fp:
            first_content = fp.read().decode()
        assert first.name == '000000010000000000000001'
        assert first_content == 'test-content'
        second = tar.next()
        with closing(tar.extractfile(second)) as fp:
            second_content = fp.read().decode()
        assert second.name == 'MD5SUMS'
        assert second_content == \
            '%s *000000010000000000000001\n' % source_file.computehash('md5')
        assert tar.next() is None

    @mock.patch('barman.clients.walarchive.subprocess.Popen')
    def test_error(self, popen_mock, tmpdir):
        input_mock = BytesIO()

        popen_mock.return_value.stdin = input_mock
        config = mock.Mock(
            user='barman',
            barman_host='remote.barman.host',
            config=None,
            server_name='this-server',
            test=False)
        source_file = tmpdir.join('test-source/000000010000000000000001')
        source_file.write("test-content", ensure=True)
        source_path = source_file.strpath

        # Simulate a remote failure
        popen_mock.return_value.returncode = 5

        # In python2 the source_path can be an unicode object
        if hasattr(source_path, 'decode'):
            source_path = source_path.decode()

        rwa = walarchive.RemotePutWal(config, source_path)

        popen_mock.assert_called_once_with(
            ['ssh', '-q', '-T', 'barman@remote.barman.host',
             'barman', 'put-wal', 'this-server'], stdin=subprocess.PIPE)

        assert rwa.returncode == 5


# noinspection PyMethodMayBeStatic
class TestChecksumTarFile(object):

    def test_tar(self, tmpdir):
        # Prepare some content
        source = tmpdir.join('source.file')
        source.write('something', ensure=True)
        source.setmtime(source.mtime() - 100)  # Set mtime to 100 seconds ago
        source_hash = source.computehash()

        # Write the content in a tar file
        storage = tmpdir.join('storage.tar')
        with closing(walarchive.ChecksumTarFile.open(
                storage.strpath, mode='w:')) as tar:
            tar.add(source.strpath, source.basename)
            checksum = tar.members[0].data_checksum
            assert checksum == source_hash

        # Double close should not give any issue
        tar.close()

        lab = tmpdir.join('lab').ensure(dir=True)
        tar = tarfile.open(storage.strpath, mode='r:')
        tar.extractall(lab.strpath)
        tar.close()

        dest_file = lab.join(source.basename)
        sum_file = lab.join('MD5SUMS')
        sums = {}
        for line in sum_file.readlines():
            checksum, name = re.split(r' [* ]', line.rstrip(), 1)
            sums[name] = checksum

        assert list(sums.keys()) == [source.basename]
        assert sums[source.basename] == source_hash
        assert dest_file.computehash() == source_hash
        # Verify file mtime
        # Use a round(2) comparison because float is not precise in Python 2.x
        assert round(dest_file.mtime(), 2) == round(source.mtime(), 2)

    @pytest.mark.parametrize(
        ['size', 'mode'],
        [
            [0, 0],
            [10, None],
            [10, 0],
            [10, 1],
            [10, -5],
            [16 * 1024, 0],
            [32 * 1024 - 1, -1],
            [32 * 1024 - 1, 0],
            [32 * 1024 - 1, 1],
        ])
    def test_md5copyfileobj(self, size, mode):
        """
        Test md5copyfileobj different size.

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
            # Require more bytes thant available. Make sure to get an exception
            with pytest.raises(IOError):
                walarchive.md5copyfileobj(src, dst, size + mode)
        else:
            if mode is None:
                # Copy the whole file until the end
                md5 = walarchive.md5copyfileobj(src, dst)
            else:
                # Copy only a portion of the file
                md5 = walarchive.md5copyfileobj(src, dst, size + mode)
                src_string = src_string[0:size + mode]

            # Validate the content and the checksum
            assert dst.getvalue() == src_string
            assert md5 == hashlib.md5(bytes(src_string)).hexdigest()
