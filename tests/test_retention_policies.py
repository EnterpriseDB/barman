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

from datetime import datetime
import pytest
from barman.infofile import BackupInfo
from barman.retention_policies import RetentionPolicyFactory, \
    RedundancyRetentionPolicy, RecoveryWindowRetentionPolicy
from mock import Mock
from dateutil.tz import tzlocal
from barman.testing_helpers import build_test_backup_info


class TestRetentionPolicies(object):

    @staticmethod
    def build_server():
        """
        Build a server object

        :rtype: barman.server.Server
        """
        # instantiate a retention policy object using mocked parameters
        server = Mock(name='server')
        # The basebackup_directory is not used, but if unset BackupInfo will
        # yield an error
        server.config.basebackups_directory = "/some/directory"
        return server

    def test_redundancy_report(self):
        """
        Basic unit test of RedundancyRetentionPolicy

        Given a mock simulating a Backup with status DONE,
        the report method of the RedundancyRetentionPolicy class must mark
        it as valid
        """
        server = self.build_server()
        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'REDUNDANCY 2')
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy because
        # the total number of valid backups is lower than the retention policy
        # redundancy.
        assert report == {'test_backup': BackupInfo.VALID}
        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context='invalid')

    def test_recovery_window_report(self):
        """
        Basic unit test of RecoveryWindowRetentionPolicy

        Given a mock simulating a Backup with status DONE and
        the end_date not over the point of recoverability,
        the report method of the RecoveryWindowRetentionPolicy class must mark
        it as valid
        """
        server = self.build_server()
        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'RECOVERY WINDOW OF 4 WEEKS')
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy
        assert report == {'test_backup': BackupInfo.VALID}

        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context='invalid')

    def test_backup_status(self):
        """
        Basic unit test of method backup_status

        Given a mock simulating a Backup with status DONE and
        requesting the status through the backup_status method, the
        RetentionPolicy class must mark it as valid

        This method tests the validity of a backup using both
        RedundancyRetentionPolicy and RecoveryWindowRetentionPolicy
        """

        server = self.build_server()
        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'REDUNDANCY 2')
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.backup_status('test_backup')

        assert report == 'VALID'
        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = 'invalid'
        empty_report = rp.backup_status('test_backup')

        assert empty_report == BackupInfo.NONE

        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'RECOVERY WINDOW OF 4 WEEKS')
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.backup_status("test_backup")

        assert report == 'VALID'

        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = 'invalid'
        empty_report = rp.backup_status('test_backup')

        assert empty_report == BackupInfo.NONE

    def test_first_backup(self):
        server = self.build_server()
        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'RECOVERY WINDOW OF 4 WEEKS')
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.first_backup()

        assert report == 'test_backup'

        rp = RetentionPolicyFactory.create(
            server,
            'retention_policy',
            'REDUNDANCY 2')
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=rp.server,
            backup_id='test1',
            end_time=datetime.now(tzlocal()))

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.get_available_backups.return_value = {
            "test_backup": backup_info
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.first_backup()

        assert report == 'test_backup'
