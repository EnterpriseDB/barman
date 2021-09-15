# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2021
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

import mock
import pytest
from dateutil.tz import tzlocal

from barman.annotations import KeepManager
from barman.infofile import BackupInfo
from barman.retention_policies import (
    RecoveryWindowRetentionPolicy,
    RedundancyRetentionPolicy,
    RetentionPolicyFactory,
)
from testing_helpers import build_mocked_server, build_test_backup_info


class TestRetentionPolicies(object):
    @pytest.fixture
    def server(self):
        backup_manager = mock.Mock()
        backup_manager.get_keep_target.return_value = None
        server = build_mocked_server()
        server.backup_manager = backup_manager
        yield server

    def test_redundancy_report(self, server, caplog):
        """
        Test of the management of the minimum_redundancy parameter
        into the backup_report method of the RedundancyRetentionPolicy class

        """
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=server
        )
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup2": backup_info,
            "test_backup3": backup_info,
        }
        server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy because
        # the total number of valid backups is lower than the retention policy
        # redundancy.
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.VALID,
        }
        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context="invalid")
        # Set a new minimum_redundancy parameter, enforcing the usage of the
        # configuration parameter instead of the retention policy default
        server.config.minimum_redundancy = 3
        # execute retention policy report
        rp.report()
        # Check for the warning inside the log
        caplog.set_level(logging.WARNING)

        log = caplog.text
        assert log.find(
            "WARNING  Retention policy redundancy (2) "
            "is lower than the required minimum redundancy (3). "
            "Enforce 3."
        )

    def test_recovery_window_report(self, server, caplog):
        """
        Basic unit test of RecoveryWindowRetentionPolicy

        Given a mock simulating a Backup with status DONE and
        the end_date not over the point of recoverability,
        the report method of the RecoveryWindowRetentionPolicy class must mark
        it as valid
        """
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        backup_source = {"test_backup3": backup_info}
        # Add a obsolete backup
        backup_info.end_time = datetime.now(tzlocal()) - timedelta(weeks=5)
        backup_source["test_backup2"] = backup_info
        # Add a second obsolete backup
        backup_info.end_time = datetime.now(tzlocal()) - timedelta(weeks=6)
        backup_source["test_backup"] = backup_info
        server.get_available_backups.return_value = backup_source
        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.config.minimum_redundancy = 1
        server.config.name = "test"
        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy
        assert report == {
            "test_backup3": "VALID",
            "test_backup2": "OBSOLETE",
            "test_backup": "OBSOLETE",
        }

        # Expect a ValueError if passed context is invalid
        with pytest.raises(ValueError):
            rp.report(context="invalid")
        # Set a new minimum_redundancy parameter, enforcing the usage of the
        # configuration parameter instead of the retention policy default
        server.config.minimum_redundancy = 4
        # execute retention policy report
        rp.report()
        # Check for the warning inside the log
        caplog.set_level(logging.WARNING)
        log = caplog.text
        warn = (
            "WARNING  Keeping obsolete backup test_backup2 for "
            "server test (older than %s) due to minimum redundancy "
            "requirements (4)\n" % rp._point_of_recoverability()
        )
        assert log.find(warn)

    def test_backup_status(self, server):
        """
        Basic unit test of method backup_status

        Given a mock simulating a Backup with status DONE and
        requesting the status through the backup_status method, the
        RetentionPolicy class must mark it as valid

        This method tests the validity of a backup using both
        RedundancyRetentionPolicy and RecoveryWindowRetentionPolicy
        """

        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=server
        )
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {"test_backup": backup_info}
        server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.backup_status("test_backup")

        assert report == "VALID"
        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = "invalid"
        empty_report = rp.backup_status("test_backup")

        assert empty_report == BackupInfo.NONE

        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {"test_backup": backup_info}
        server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.backup_status("test_backup")

        assert report == "VALID"

        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = "invalid"
        empty_report = rp.backup_status("test_backup")

        assert empty_report == BackupInfo.NONE

    def test_first_backup(self, server):
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test0", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {"test_backup": backup_info}
        server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.first_backup()

        assert report == "test_backup"

        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=server
        )
        assert isinstance(rp, RedundancyRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {"test_backup": backup_info}
        server.config.minimum_redundancy = 1

        # execute retention policy report
        report = rp.first_backup()

        assert report == "test_backup"


class TestRedundancyRetentionPolicyWithKeepAnnotation(object):
    """
    Tests redundancy retention policy correctly handles backups tagged with the
    keep annotation.
    """

    @pytest.fixture
    def mock_server(self):
        server = build_mocked_server()
        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup2": backup_info,
            "test_backup3": backup_info,
        }
        server.config.minimum_redundancy = 1
        yield server

    @pytest.fixture
    def mock_backup_manager(self):
        backup_manager = mock.Mock()

        def get_keep_target(backup_id):
            try:
                return self.keep_targets[backup_id]
            except KeyError:
                pass

        backup_manager.get_keep_target.side_effect = get_keep_target
        yield backup_manager

    def test_keep_standalone_within_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:standalone backup within policy is reported as
        KEEP_STANDALONE.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=mock_server
        )
        self.keep_targets = {"test_backup3": KeepManager.TARGET_STANDALONE}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.KEEP_STANDALONE,
        }

    def test_keep_full_within_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:full backup within policy is reported as KEEP_FULL.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=mock_server
        )
        self.keep_targets = {"test_backup3": KeepManager.TARGET_FULL}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.KEEP_FULL,
        }

    def test_keep_standalone_out_of_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:standalone backup out-of-policy is reported as
        KEEP_STANDALONE.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=mock_server
        )
        self.keep_targets = {"test_backup": KeepManager.TARGET_STANDALONE}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_STANDALONE,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.VALID,
        }

    def test_keep_full_out_of_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:full backup out-of-policy is reported as KEEP_FULL.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=mock_server
        )
        self.keep_targets = {"test_backup": KeepManager.TARGET_FULL}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_FULL,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.VALID,
        }

    def test_keep_unknown_recovery_target(self, mock_server, mock_backup_manager):
        """Verify backups with an unrecognized keep target default to KEEP_FULL"""
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=mock_server
        )
        self.keep_targets = {"test_backup": "unsupported_recovery_target"}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_FULL,
            "test_backup2": BackupInfo.VALID,
            "test_backup3": BackupInfo.VALID,
        }


class TestRecoveryWindowRetentionPolicyWithKeepAnnotation(object):
    """
    Tests recovery window retention policy correctly handles backups tagged with the
    keep annotation.
    """

    @pytest.fixture
    def mock_server(self):
        server = build_mocked_server()
        # Build a BackupInfo object with status to DONE
        backup_source = {
            "test_backup4": build_test_backup_info(
                server=server, backup_id="test1", end_time=datetime.now(tzlocal())
            )
        }
        # Add an out-of-policy backup
        backup_source["test_backup3"] = build_test_backup_info(
            server=server,
            backup_id="test1",
            end_time=datetime.now(tzlocal()) - timedelta(weeks=5),
        )
        # Add an alder out-of-policy backup
        backup_source["test_backup2"] = build_test_backup_info(
            server=server,
            backup_id="test1",
            end_time=datetime.now(tzlocal()) - timedelta(weeks=6),
        )
        # Add yet another out-of-policy backup
        backup_source["test_backup"] = build_test_backup_info(
            server=server,
            backup_id="test1",
            end_time=datetime.now(tzlocal()) - timedelta(weeks=7),
        )
        server.get_available_backups.return_value = backup_source
        # Set a minimum redundancy of 3 so we have two valid backups, one potentially
        # obsolete, and one obsolete. The reason we have two valid backups is because
        # even though the second backup is outside of the recovery window, the backup
        # is required in order to be able to recover to points in time before the most
        # recent backup.
        server.config.minimum_redundancy = 3
        yield server

    @pytest.fixture
    def mock_backup_manager(self):
        backup_manager = mock.Mock()

        def get_keep_target(backup_id):
            try:
                return self.keep_targets[backup_id]
            except KeyError:
                pass

        backup_manager.get_keep_target.side_effect = get_keep_target
        yield backup_manager

    def test_keep_standalone_within_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:standalone backup within policy is reported as
        KEEP_STANDALONE.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup4": KeepManager.TARGET_STANDALONE}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.POTENTIALLY_OBSOLETE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.KEEP_STANDALONE,
        }

    def test_keep_full_within_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:full backup within policy is reported as KEEP_FULL.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup4": KeepManager.TARGET_FULL}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.POTENTIALLY_OBSOLETE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.KEEP_FULL,
        }

    def test_keep_standalone_out_of_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:standalone backup out-of-policy is reported as
        KEEP_STANDALONE.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup": KeepManager.TARGET_STANDALONE}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_STANDALONE,
            "test_backup2": BackupInfo.POTENTIALLY_OBSOLETE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.VALID,
        }

    def test_keep_full_out_of_policy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:full backup out-of-policy is reported as KEEP_FULL.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup": KeepManager.TARGET_FULL}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_FULL,
            "test_backup2": BackupInfo.POTENTIALLY_OBSOLETE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.VALID,
        }

    def test_keep_standalone_minimum_redundancy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:standalone backup which would normally be flagged as
        POTENTIALLY_OBSOLETE due to not meeting the minimum redundancy (3 in this
        case) is reported as KEEP_STANDALONE.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup2": KeepManager.TARGET_STANDALONE}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.KEEP_STANDALONE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.VALID,
        }

    def test_keep_full_minimum_redundancy(self, mock_server, mock_backup_manager):
        """
        Test that a keep:full backup which would normally be flagged as
        POTENTIALLY_OBSOLETE due to not meeting the minimum redundancy (3 in this
        case) is reported as KEEP_FULL.
        """
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup2": KeepManager.TARGET_FULL}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.OBSOLETE,
            "test_backup2": BackupInfo.KEEP_FULL,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.VALID,
        }

    def test_keep_unknown_recovery_target(self, mock_server, mock_backup_manager):
        """Verify backups with an unrecognized keep target default to KEEP_FULL"""
        mock_server.backup_manager = mock_backup_manager
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=mock_server
        )
        self.keep_targets = {"test_backup": "unsupported_recovery_target"}

        report = rp.report()
        assert report == {
            "test_backup": BackupInfo.KEEP_FULL,
            "test_backup2": BackupInfo.POTENTIALLY_OBSOLETE,
            "test_backup3": BackupInfo.VALID,
            "test_backup4": BackupInfo.VALID,
        }
