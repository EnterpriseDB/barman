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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import errno
import logging
import os
import signal
import time
from glob import glob

from barman import output
from barman.lockfile import LockFileParsingError, ServerWalReceiveLock

_logger = logging.getLogger(__name__)


class ProcessInfo(object):
    """
    Barman process representation
    """

    def __init__(self, pid, server_name, task):
        """
        This object contains all the information required to identify a
        barman process

        :param int pid: Process ID
        :param string server_name: Name of the server owning the process
        :param string task: Task name (receive-wal, archive-wal...)
        """

        self.pid = pid
        self.server_name = server_name
        self.task = task


class ProcessManager(object):
    """
    Class for the management of barman processes owned by a server
    """

    # Map containing the tasks we want to retrieve (and eventually manage)
    TASKS = {
        'receive-wal': ServerWalReceiveLock
    }

    def __init__(self, config):
        """
        Build a ProcessManager for the provided server

        :param config: configuration of the server owning the process manager
        """
        self.config = config
        self.process_list = []
        # Cycle over the lock files in the lock directory for this server
        for path in glob(os.path.join(self.config.barman_lock_directory,
                                      '.%s-*.lock' % self.config.name)):
            for task, lock_class in self.TASKS.items():
                # Check the lock_name against the lock class
                lock = lock_class.build_if_matches(path)
                if lock:
                    try:
                        # Use the lock to get the owner pid
                        pid = lock.get_owner_pid()
                    except LockFileParsingError:
                        _logger.warning(
                            "Skipping the %s process for server %s: "
                            "Error reading the PID from lock file '%s'",
                            task, self.config.name, path)
                        break
                    # If there is a pid save it in the process list
                    if pid:
                        self.process_list.append(
                            ProcessInfo(pid, config.name, task))
                    # In any case, we found a match, so we must stop iterating
                    # over the task types and handle the the next path
                    break

    def list(self, task_filter=None):
        """
        Returns a list of processes owned by this server

        If no filter is provided, all the processes are returned.

        :param str task_filter: Type of process we want to retrieve
        :return list[ProcessInfo]: List of processes for the server
        """
        server_tasks = []
        for process in self.process_list:
            # Filter the processes if necessary
            if task_filter and process.task != task_filter:
                continue
            server_tasks.append(process)
        return server_tasks

    def kill(self, process_info, retries=10):
        """
        Kill a process

        Returns True if killed successfully False otherwise

        :param ProcessInfo process_info: representation of the process
            we want to kill
        :param int retries: number of times the method will check
            if the process is still alive
        :rtype: bool
        """
        # Try to kill the process
        try:
            _logger.debug("Sending SIGINT to PID %s", process_info.pid)
            os.kill(process_info.pid, signal.SIGINT)
            _logger.debug("os.kill call succeeded")
        except OSError as e:
            _logger.debug("os.kill call failed: %s", e)
            # The process doesn't exists. It has probably just terminated.
            if e.errno == errno.ESRCH:
                return True
            # Something unexpected has happened
            output.error("%s", e)
            return False
        # Check if the process have been killed. the fastest (and maybe safest)
        # way is to send a kill with 0 as signal.
        # If the method returns an OSError exceptions, the process have been
        # killed successfully, otherwise is still alive.
        for counter in range(retries):
            try:
                _logger.debug("Checking with SIG_DFL if PID %s is still alive",
                              process_info.pid)
                os.kill(process_info.pid, signal.SIG_DFL)
                _logger.debug("os.kill call succeeded")
            except OSError as e:
                _logger.debug("os.kill call failed: %s", e)
                # If the process doesn't exists, we are done.
                if e.errno == errno.ESRCH:
                    return True
                # Something unexpected has happened
                output.error("%s", e)
                return False
            time.sleep(1)
        _logger.debug("The PID %s has not been terminated after %s retries",
                      process_info.pid, retries)
        return False
