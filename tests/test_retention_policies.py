# Copyright (C) 2013-2016 2ndQuadrant Italia Srl
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

import logging
from datetime import datetime, timedelta

import pytest
from dateutil.tz import tzlocal

from barman.infofile import BackupInfo
from barman.retention_policies import (RecoveryWindowRetentionPolicy,
                                       RedundancyRetentionPolicy,
                                       RetentionPolicyFactory)
from testing_helpers import build_mocked_server, build_test_backup_info


class TestRetentionPolicies(object):

    def test_redundancy_report(self, caplog):
        """
        Test of the management of the minimum_redundancy parameter
        into the backup_report method of the RedundancyRetentionPolicy class

        """
        server = build_mocked_server()
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
            "test_backup": backup_info,
            "test_backup2": backup_info,
            "test_backup3": backup_info,
        }
        rp.server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy because
        # the total number of valid backups is lower than the retention policy
        # redundancy.
        assert report == {'test_backup': BackupInfo.OBSOLETE,
                          'test_backup2': BackupInfo.VALID,
                          'test_backup3': BackupInfo.VALID}
        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context='invalid')
        # Set a new minimum_redundancy parameter, enforcing the usage of the
        # configuration parameter instead of the retention policy default
        rp.server.config.minimum_redundancy = 3
        # execute retention policy report
        rp.report()
        # Check for the warning inside the log
        caplog.set_level(logging.WARNING)

        log = caplog.text
        assert log.find("WARNING  Retention policy redundancy (2) "
                        "is lower than the required minimum redundancy (3). "
                        "Enforce 3.")

    def test_recovery_window_report(self, caplog):
        """
        Basic unit test of RecoveryWindowRetentionPolicy

        Given a mock simulating a Backup with status DONE and
        the end_date not over the point of recoverability,
        the report method of the RecoveryWindowRetentionPolicy class must mark
        it as valid
        """
        server = build_mocked_server()
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

        backup_source = {'test_backup3': backup_info}
        # Add a obsolete backup
        backup_info.end_time = datetime.now(tzlocal()) - timedelta(weeks=5)
        backup_source['test_backup2'] = backup_info
        # Add a second obsolete backup
        backup_info.end_time = datetime.now(tzlocal()) - timedelta(weeks=6)
        backup_source['test_backup'] = backup_info
        rp.server.get_available_backups.return_value = backup_source
        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        rp.server.config.minimum_redundancy = 1
        rp.server.config.name = "test"
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy
        assert report == {'test_backup3': 'VALID',
                          'test_backup2': 'OBSOLETE',
                          'test_backup': 'OBSOLETE'}

        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context='invalid')
        # Set a new minimum_redundancy parameter, enforcing the usage of the
        # configuration parameter instead of the retention policy default
        rp.server.config.minimum_redundancy = 4
        # execute retention policy report
        rp.report()
        # Check for the warning inside the log
        caplog.set_level(logging.WARNING)
        log = caplog.text
        warn = "WARNING  Keeping obsolete backup test_backup2 for " \
               "server test (older than %s) due to minimum redundancy " \
               "requirements (4)\n" % rp._point_of_recoverability()
        assert log.find(warn)

    def test_backup_status(self):
        """
        Basic unit test of method backup_status

        Given a mock simulating a Backup with status DONE and
        requesting the status through the backup_status method, the
        RetentionPolicy class must mark it as valid

        This method tests the validity of a backup using both
        RedundancyRetentionPolicy and RecoveryWindowRetentionPolicy
        """

        server = build_mocked_server()
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
        server = build_mocked_server()
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
