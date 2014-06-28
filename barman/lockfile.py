# Copyright (C) 2011-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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

    def __init__(self, filename, raise_if_fail=False, wait=False):
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

        Returns True if lock has been successfully acquired, False if it is not.

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
            fd = os.open(self.filename, os.O_TRUNC | os.O_CREAT | os.O_RDWR,
                         0600)
            flags = fcntl.LOCK_EX
            if not wait:
                flags |= fcntl.LOCK_NB
            fcntl.flock(fd, flags)
            os.write(fd, ("%s\n" % os.getpid()).encode('ascii'))
            self.fd = fd
            return True
        except (OSError, IOError), e:
            if fd:
                os.close(fd)   # let's not leak  file descriptors
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
