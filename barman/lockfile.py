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

"""
This module is the lock manager for Barman
"""

import errno
import fcntl
import os
import re


class LockFileException(Exception):
    """
    LockFile Exception base class
    """
    pass


class LockFileBusy(LockFileException):
    """
    Raised when a lock file is not free
    """
    pass


class LockFilePermissionDenied(LockFileException):
    """
    Raised when a lock file is not accessible
    """
    pass


class LockFileParsingError(LockFileException):
    """
    Raised when the content of the lockfile is unexpected
    """
    pass


class LockFile(object):
    """
    Ensures that there is only one process which is running against a
    specified LockFile.
    It supports the Context Manager interface, allowing the use in with
    statements.

        with LockFile('file.lock') as locked:
            if not locked:
                print "failed"
            else:
                <do something>

    You can also use exceptions on failures

        try:
            with LockFile('file.lock', True):
                <do something>
        except LockFileBusy, e, file:
            print "failed to lock %s" % file

    """

    LOCK_PATTERN = None
    """
    If defined in a subclass, it must be a compiled regular expression
    which matches the lock filename.

    It must provide named groups for the constructor parameters which produce
    the same lock name. I.e.:

    >>> ServerWalReceiveLock('/tmp', 'server-name').filename
    '/tmp/.server-name-receive-wal.lock'
    >>> ServerWalReceiveLock.LOCK_PATTERN = re.compile(
            r'\.(?P<server_name>.+)-receive-wal\.lock')
    >>> m = ServerWalReceiveLock.LOCK_PATTERN.match(
            '.server-name-receive-wal.lock')
    >>> ServerWalReceiveLock('/tmp', **(m.groupdict())).filename
    '/tmp/.server-name-receive-wal.lock'

    """

    @classmethod
    def build_if_matches(cls, path):
        """
        Factory method that creates a lock instance if the path matches
        the lock filename created by the actual class

        :param path: the full path of a LockFile
        :return:
        """
        # If LOCK_PATTERN is not defined always return None
        if not cls.LOCK_PATTERN:
            return None
        # Matches the provided path against LOCK_PATTERN
        lock_directory = os.path.abspath(os.path.dirname(path))
        lock_name = os.path.basename(path)
        match = cls.LOCK_PATTERN.match(lock_name)
        if match:
            # Build the lock object for the provided path
            return cls(lock_directory, **(match.groupdict()))
        return None

    def __init__(self, filename, raise_if_fail=True, wait=False):
        self.filename = os.path.abspath(filename)
        self.fd = None
        self.raise_if_fail = raise_if_fail
        self.wait = wait

    def acquire(self, raise_if_fail=None, wait=None):
        """
        Creates and holds on to the lock file.

        When raise_if_fail, a LockFileBusy is raised if
        the lock is held by someone else and a LockFilePermissionDenied is
        raised when the user executing barman have insufficient rights for
        the creation of a LockFile.

        Returns True if lock has been successfully acquired, False otherwise.

        :param bool raise_if_fail: If True raise an exception on failure
        :param bool wait: If True issue a blocking request
        :returns bool: whether the lock has been acquired
        """
        if self.fd:
            return True
        fd = None
        # method arguments take precedence on class parameters
        raise_if_fail = raise_if_fail \
            if raise_if_fail is not None else self.raise_if_fail
        wait = wait if wait is not None else self.wait
        try:
            # 384 is 0600 in octal, 'rw-------'
            fd = os.open(self.filename, os.O_CREAT | os.O_RDWR, 384)
            flags = fcntl.LOCK_EX
            if not wait:
                flags |= fcntl.LOCK_NB
            fcntl.flock(fd, flags)
            # Once locked, replace the content of the file
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, ("%s\n" % os.getpid()).encode('ascii'))
            # Truncate the file at the current position
            os.ftruncate(fd, os.lseek(fd, 0, os.SEEK_CUR))
            self.fd = fd
            return True
        except (OSError, IOError) as e:
            if fd:
                os.close(fd)  # let's not leak  file descriptors
            if raise_if_fail:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise LockFileBusy(self.filename)
                elif e.errno == errno.EACCES:
                    raise LockFilePermissionDenied(self.filename)
                else:
                    raise
            else:
                return False

    def release(self):
        """
        Releases the lock.

        If the lock is not held by the current process it does nothing.
        """
        if not self.fd:
            return
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
        except (OSError, IOError):
            pass
        self.fd = None

    def __del__(self):
        """
        Avoid stale lock files.
        """
        self.release()

    # Contextmanager interface

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exception_type, value, traceback):
        self.release()

    def get_owner_pid(self):
        """
        Test whether a lock is already held by a process.

        Returns the PID of the owner process or None if the lock is available.

        :rtype: int|None
        :raises LockFileParsingError: when the lock content is garbled
        :raises LockFilePermissionDenied: when the lockfile is not accessible
        """
        try:
            self.acquire(raise_if_fail=True, wait=False)
        except LockFileBusy:
            try:
                # Read the lock content and parse the PID
                # NOTE: We cannot read it in the self.acquire method to avoid
                # reading the previous locker PID
                with open(self.filename, 'r') as file_object:
                    return int(file_object.readline().strip())
            except ValueError as e:
                # This should not happen
                raise LockFileParsingError(e)
        # release the lock and return None
        self.release()
        return None


class GlobalCronLock(LockFile):
    """
    This lock protects cron from multiple executions.

    Creates a global '.cron.lock' lock file under the given lock_directory.
    """

    def __init__(self, lock_directory):
        super(GlobalCronLock, self).__init__(
            os.path.join(lock_directory, '.cron.lock'),
            raise_if_fail=True)


class ServerBackupLock(LockFile):
    """
    This lock protects a server from multiple executions of backup command

    Creates a '.<SERVER>-backup.lock' lock file under the given lock_directory
    for the named SERVER.
    """

    def __init__(self, lock_directory, server_name):
        super(ServerBackupLock, self).__init__(
            os.path.join(lock_directory, '.%s-backup.lock' % server_name),
            raise_if_fail=True)


class ServerCronLock(LockFile):
    """
    This lock protects a server from multiple executions of cron command

    Creates a '.<SERVER>-cron.lock' lock file under the given lock_directory
    for the named SERVER.
    """

    def __init__(self, lock_directory, server_name):
        super(ServerCronLock, self).__init__(
            os.path.join(lock_directory, '.%s-cron.lock' % server_name),
            raise_if_fail=True, wait=False)


class ServerXLOGDBLock(LockFile):
    """
    This lock protects a server's xlogdb access

    Creates a '.<SERVER>-xlogdb.lock' lock file under the given lock_directory
    for the named SERVER.
    """

    def __init__(self, lock_directory, server_name):
        super(ServerXLOGDBLock, self).__init__(
            os.path.join(lock_directory, '.%s-xlogdb.lock' % server_name),
            raise_if_fail=True, wait=True)


class ServerWalArchiveLock(LockFile):
    """
    This lock protects a server from multiple executions of wal-archive command

    Creates a '.<SERVER>-archive-wal.lock' lock file under
    the given lock_directory for the named SERVER.
    """

    def __init__(self, lock_directory, server_name):
        super(ServerWalArchiveLock, self).__init__(
            os.path.join(lock_directory, '.%s-archive-wal.lock' % server_name),
            raise_if_fail=True, wait=False)


class ServerWalReceiveLock(LockFile):
    """
    This lock protects a server from multiple executions of receive-wal command

    Creates a '.<SERVER>-receive-wal.lock' lock file under
    the given lock_directory for the named SERVER.
    """
    # TODO: Implement on the other LockFile subclasses
    LOCK_PATTERN = re.compile(r'\.(?P<server_name>.+)-receive-wal\.lock')

    def __init__(self, lock_directory, server_name):
        super(ServerWalReceiveLock, self).__init__(
            os.path.join(lock_directory, '.%s-receive-wal.lock' % server_name),
            raise_if_fail=True, wait=False)
