# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2022
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
import subprocess

import mock
import pytest

from barman.clients import walrestore


# noinspection PyMethodMayBeStatic
class TestRemoteGetWal(object):
    @mock.patch("barman.clients.walrestore.subprocess.Popen")
    def test_string_dest_file(self, popen_mock, tmpdir):
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
