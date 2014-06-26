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
from barman.infofile import BackupInfo
from barman.retention_policies import RetentionPolicyFactory, \
    RedundancyRetentionPolicy, RecoveryWindowRetentionPolicy
from mock import Mock
from dateutil.tz import tzlocal


class TestRetentionPolicies(object):

    @staticmethod
    def build_redundancy_retention():
        """
        Build RedundancyRetentionPolicy with redundancy 2

        :return RedundancyRetentionPolicy: a RedundancyRetentionPolicy instance
        """
        # instantiate a retention policy object using mocked parameters
        server = Mock(name='server')
        rp = RetentionPolicyFactory.create(server, 'retention_policy',
                                           'REDUNDANCY 2')
        return rp

    @staticmethod
    def build_recovery_window_retention():
        """
        Build RecoveryWindowRetentionPolicy with recovery window of 4 weeks

        :return RecoveryWindowRetentionPolicy: a RecoveryWindowRetentionPolicy
            instance
        """
        # instantiate a retention policy object using mocked parameters
        server = Mock(name='server')
        rp = RetentionPolicyFactory.create(server, 'retention_policy',
                                           'RECOVERY WINDOW OF 4 WEEKS')
        return rp

    def test_redundancy_report(self):
        """
        Basic unit test of RedundancyRetentionPolicy

        Given a mock simulating a Backup with status DONE,
        the report method of the RedundancyRetentionPolicy class must mark
        it as valid
        """
        rp = self.build_redundancy_retention()
        assert isinstance(rp, RedundancyRetentionPolicy)

        # use a Mock class as BackupInfo with status to DONE
        backup_info = Mock(name='backup_info')
        backup_info.status = BackupInfo.DONE

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

    def test_recovery_window_report(self):
        """
        Basic unit test of RecoveryWindowRetentionPolicy

        Given a mock simulating a Backup with status DONE and
        the end_date not over the point of recoverability,
        the report method of the RecoveryWindowRetentionPolicy class must mark
        it as valid
        """
        rp = self.build_recovery_window_retention()
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # use a Mock class as BackupInfo with status to DONE and end_time = now
        backup_info = Mock(name='backup_info')
        backup_info.status = BackupInfo.DONE
        backup_info.end_time = datetime.now(tzlocal())

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