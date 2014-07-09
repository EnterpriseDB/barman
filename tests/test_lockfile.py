# Copyright (C) 2013-2014 2ndQuadrant Italia (Devise.IT S.r.L.)
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
import fcntl

from mock import patch, ANY
import pytest

from barman.lockfile import LockFile, LockFileBusy, LockFilePermissionDenied


def _prepare_fnctl_mock(fcntl_mock, exception=None):
    """
    Setup the fcntl_mock to behave like we need

    :param fcntl_mock: a 'barman.lockfile.fcntl' mock
    :param Exception|None exception: If not none set it as flock side effect
    """
    # Reset the mock
    fcntl_mock.reset_mock()
    # Setup fcntl flags
    fcntl_mock.LOCK_EX = fcntl.LOCK_EX
    fcntl_mock.LOCK_NB = fcntl.LOCK_NB
    fcntl_mock.LOCK_UN = fcntl.LOCK_UN
    if exception:
        fcntl_mock.flock.side_effect = exception


# noinspection PyMethodMayBeStatic
@patch('barman.lockfile.fcntl')
class TestLockFileBehavior(object):

    def test_raise(self, fcntl_mock, tmpdir):
        """
        Test raise_if_fail override using a call to method acquire.
        Test different method behaviour with different flags
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lock_file1")

        # set flock to raise OSError exception with errno = EAGAIN
        _prepare_fnctl_mock(fcntl_mock, OSError(errno.EAGAIN, '', ''))
        lock_file = LockFile(lock_file_path.strpath,
                             raise_if_fail=False,
                             wait=False)
        # Expect the acquire method to raise a LockFileBusy exception.
        # This is the expected behaviour if the raise_if_fail flag is set to
        # True and the OSError.errno = errno.EAGAIN
        with pytest.raises(LockFileBusy):
            lock_file.acquire(raise_if_fail=True)
        # check for the right call at flock method
        fcntl_mock.flock.assert_called_once_with(
            ANY, fcntl_mock.LOCK_EX | fcntl_mock.LOCK_NB)

        # set flock to raise OSError exception with errno = EWOULDBLOCK
        _prepare_fnctl_mock(fcntl_mock, OSError(errno.EWOULDBLOCK, '', ''))
        # Expect the acquire method to raise a LockFileBusy exception.
        # This is the expected behaviour if the raise_if_fail flag is set to
        # True and the OSError.errno = errno.EWOULDBLOCK
        with pytest.raises(LockFileBusy):
            lock_file.acquire(raise_if_fail=True)
        # Check for the call at flock method
        fcntl_mock.flock.assert_called_once_with(
            ANY, fcntl_mock.LOCK_EX | fcntl_mock.LOCK_NB)

        # set flock to raise OSError exception with errno = EACCES
        _prepare_fnctl_mock(fcntl_mock, OSError(errno.EACCES, '', ''))
        # Expect the acquire method to raise a LockFileBusy exception.
        # This is the expected behaviour if the raise_if_fail flag is set to
        # True and the OSError.errno = errno.EACCES
        with pytest.raises(LockFilePermissionDenied):
            lock_file.acquire(raise_if_fail=True)
        # Check for the call at flock method
        fcntl_mock.flock.assert_called_once_with(
            ANY, fcntl_mock.LOCK_EX | fcntl_mock.LOCK_NB)

        # it should not raise if not raise_if_fail, but return False
        _prepare_fnctl_mock(fcntl_mock, OSError(errno.EWOULDBLOCK, '', ''))
        assert not lock_file.acquire(raise_if_fail=False)

    def test_wait(self, fcntl_mock, tmpdir):
        """
        Test wait parameter override using method acquire.
        Test different method behaviour
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lock_file1")
        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        lock_file = LockFile(lock_file_path.strpath,
                             raise_if_fail=False,
                             wait=False)
        # should succeed
        assert lock_file.acquire(wait=True)
        # if the wait flag is set to true we expect a lock_ex flag, that
        # has a numeric value of 2
        fcntl_mock.flock.assert_called_once_with(ANY, fcntl.LOCK_EX)
        # release the lock
        lock_file.release()

        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        # acquire it again with wait flag set to False
        assert lock_file.acquire(wait=False)
        # with the wait flag not set flock bust be called with
        # LOCK_EX and LOCK_NB flags
        fcntl_mock.flock.assert_called_once_with(
            ANY, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_acquire(self, fcntl_mock, tmpdir):
        """
        Test for double acquisition using the same LockFile object.
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lock_file1")
        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        lock_file = LockFile(lock_file_path.strpath,
                             raise_if_fail=False,
                             wait=False)
        assert lock_file.acquire()
        # with the wait flag not set flock bust be called with
        # LOCK_EX and LOCK_NB flags
        fcntl_mock.flock.assert_called_once_with(
            ANY, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        # Try the acquisition using the same unreleased LockFile.
        # The acquire method should exit immediately without calling
        # fcntl.flock() again
        assert lock_file.acquire()
        assert not fcntl_mock.flock.called

    def test_release(self, fcntl_mock, tmpdir):
        """
        Try to release an already released LockFile object
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lock_file1")
        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        lock_file = LockFile(lock_file_path.strpath,
                             raise_if_fail=False,
                             wait=False)
        assert lock_file.acquire()

        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        # Release the lock
        lock_file.release()
        # Check that the fcntl.flock() have been called using the flag LOCK_UN
        fcntl_mock.flock.assert_called_once_with(ANY, fcntl.LOCK_UN)

        # set flock to not raise
        _prepare_fnctl_mock(fcntl_mock)
        # Try to release the lock again
        lock_file.release()
        # The release method should not have called fcntl.flock()
        assert not fcntl_mock.flock.called


# noinspection PyMethodMayBeStatic
@pytest.mark.timeout(1)
class TestLockFile(object):
    """
    This class test a raw LockFile object.

    It runs without mocking the fcntl.flock() method, so it could end up waiting
    forever if something goes wrong. To avoid it we use a timeout of one second.
    """

    def test_init_with_minimal_params(self):
        """
        Minimal params object creation
        """
        LockFile("test_lock_file")

    def test_init_with_raise(self):
        """
        Object creation with raise_if_fail param True and False
        """
        lock_file = LockFile("test_lock_file", raise_if_fail=False)
        assert not lock_file.raise_if_fail

        lock_file = LockFile("test_lock_file", raise_if_fail=True)
        assert lock_file.raise_if_fail

    def test_init_with_wait(self):
        """
        Object creation with wait parameter True and False
        """
        lock_file = LockFile("test_lock_file", wait=False)
        assert not lock_file.wait

        lock_file = LockFile("test_lock_file", wait=True)
        assert lock_file.wait

    def test_context_manager_implementation(self, tmpdir):
        """
        Check lock acquisition using the context manager into a with statement.

         * Take a lock using a LockFile into a with statement.
         * Inside the with statement, try to acquire another lock using
           a different object. Expect the second lock to fail.
         * Outside the first with statement try to acquire a third lock.
           The first with statement should have been released, so it should
           succeed.
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lockfile1")
        # First lock, must succeed
        with LockFile(lock_file_path.strpath, False, False) as result1:
            assert result1
            # Second lock, expect to fail without waiting
            with LockFile(lock_file_path.strpath, False, False) as result2:
                assert not result2
        # Third lock, this must succeed because the first lock
        # has been released
        with LockFile(lock_file_path.strpath, False, False) as result3:
            assert result3

    def test_acquire(self, tmpdir):
        """
        Test lock acquisition using direct methods.

         * Create a LockFile, and acquire the lock.
         * Create a second LockFile and try to acquire the lock. It should fail.
         * Release the first lock and try acquiring the lock with the second
           one. It should now succeed.
        """
        # Use a lock file inside the testing tempdir
        lock_file_path = tmpdir.join("test_lockfile1")
        # Acquire the first lock, should succeed
        first_lock_file = LockFile(lock_file_path.strpath,
                                   raise_if_fail=False,
                                   wait=False)
        assert first_lock_file.acquire()
        # Try to acquire the lock using a second object, must fail
        second_lock_file = LockFile(lock_file_path.strpath,
                                    raise_if_fail=False,
                                    wait=False)
        assert not second_lock_file.acquire()
        # Release the lock with the first LockFile
        first_lock_file.release()
        # The second LockFile is now able to acquire the lock
        assert second_lock_file.acquire()
        second_lock_file.release()