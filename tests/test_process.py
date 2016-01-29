# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
#
# This file is part of Barman.
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

import errno
import os

import mock

from barman.lockfile import ServerWalReceiveLock
from barman.process import ProcessInfo, ProcessManager
from testing_helpers import build_config_from_dicts


# noinspection PyMethodMayBeStatic
class TestProcessInfo(object):
    """
    ProcessInfo obj tests
    """

    def test_init(self):
        """
        Test the init method
        """
        pi = ProcessInfo(pid=12345,
                         server_name='test_server',
                         task='test_task')

        assert pi.pid == 12345
        assert pi.server_name == 'test_server'
        assert pi.task == 'test_task'


class TestProcessManager(object):
    """
    Simple class for testing the ProcessManager obj
    """

    def test_init(self, tmpdir):
        """
        Test the init method
        """
        # Build a basic configuration
        config = build_config_from_dicts({
            'barman_lock_directory': tmpdir.strpath})
        config.name = 'main'
        # Acquire a lock and initialise the ProjectManager.
        # Expect the ProjectManager to Retrieve the
        # "Running process" identified by the lock
        lock = ServerWalReceiveLock(tmpdir.strpath, 'main')
        with lock:
            pm = ProcessManager(config)

        # Test for the length of the process list
        assert len(pm.process_list) == 1
        # Test for the server identifier of the process
        assert pm.process_list[0].server_name == 'main'
        # Test for the task type
        assert pm.process_list[0].task == 'receive-wal'
        # Read the pid from the lockfile and test id against the ProcessInfo
        # contained in the process_list
        with open(lock.filename, 'r') as lockfile:
            pid = lockfile.read().strip()
            assert int(pid) == pm.process_list[0].pid

        # Test lock file parse error.
        # Skip the lock and don't raise any exception.
        with lock:
            with open(lock.filename, 'w') as lockfile:
                lockfile.write("invalid")
            pm = ProcessManager(config)
            assert len(pm.process_list) == 0

    def test_list(self, tmpdir):
        """
        Test the list method from the ProjectManager class
        """
        config = build_config_from_dicts({
            'barman_lock_directory': tmpdir.strpath})
        config.name = 'main'
        with ServerWalReceiveLock(tmpdir.strpath, 'main'):
            pm = ProcessManager(config)
            process = pm.list('receive-wal')[0]

        assert process.server_name == 'main'
        assert process.task == 'receive-wal'
        with open(os.path.join(
                tmpdir.strpath,
                '.%s-receive-wal.lock' % config.name)) as lockfile:
            pid = lockfile.read().strip()
            assert int(pid) == process.pid

    @mock.patch('os.kill')
    def test_kill(self, kill_mock, tmpdir):
        """
        Test the Kill method from the ProjectManager class.
        Mocks the os.kill used inside the the kill method
        """
        config = build_config_from_dicts({
            'barman_lock_directory': tmpdir.strpath})
        config.name = 'main'
        # Acquire a lock, simulating a running process
        with ServerWalReceiveLock(tmpdir.strpath, 'main'):
            # Build a ProcessManager and retrieve the receive-wal process
            pm = ProcessManager(config)
            pi = pm.list('receive-wal')[0]
            # Exit at the first invocation of kill (this is a failed kill)
            kill_mock.side_effect = OSError(errno.EPERM, '', '')
            kill = pm.kill(pi)
            # Expect the kill result to be false
            assert kill is False
            assert kill_mock.call_count == 1
            kill_mock.assert_called_with(pi.pid, 2)

            kill_mock.reset_mock()
            # Exit at the second invocation of kill (this is a successful kill)
            kill_mock.side_effect = [None, OSError(errno.ESRCH, '', '')]
            # Expect the kill result to be true
            kill = pm.kill(pi)
            assert kill
            assert kill_mock.call_count == 2
            kill_mock.assert_has_calls([mock.call(pi.pid, 2),
                                        mock.call(pi.pid, 0)])

            kill_mock.reset_mock()
            # Check for the retry feature. exit at the second iteration of the
            # kill cycle
            kill_mock.side_effect = [None, None, OSError(errno.ESRCH, '', '')]
            kill = pm.kill(pi)
            assert kill
            assert kill_mock.call_count == 3
            kill_mock.assert_has_calls([mock.call(pi.pid, 2),
                                        mock.call(pi.pid, 0),
                                        mock.call(pi.pid, 0)])
