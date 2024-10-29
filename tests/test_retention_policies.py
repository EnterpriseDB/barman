# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2013-2023
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

import itertools
import logging
import re
from datetime import datetime, timedelta

import mock
import pytest
from dateutil.tz import tzlocal
from testing_helpers import build_mocked_server, build_test_backup_info

from barman.annotations import KeepManager
from barman.infofile import BackupInfo
from barman.retention_policies import (
    RecoveryWindowRetentionPolicy,
    RedundancyRetentionPolicy,
    RetentionPolicyFactory,
)


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
        backup_source = {
            "test_backup3": build_test_backup_info(
                server=server,
                backup_id="test_backup3",
                end_time=datetime.now(tzlocal()),
            )
        }
        # Add a obsolete backup
        backup_source["test_backup2"] = build_test_backup_info(
            server=server,
            backup_id="test_backup2",
            end_time=datetime.now(tzlocal()) - timedelta(weeks=5),
        )
        # Add a second obsolete backup
        backup_source["test_backup"] = build_test_backup_info(
            server=server,
            backup_id="test_backup",
            end_time=datetime.now(tzlocal()) - timedelta(weeks=6),
        )
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
            "test_backup2": "VALID",
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
            r"WARNING  .*Keeping obsolete backup test_backup for "
            r"server test \(older than .*\) due to minimum redundancy "
            r"requirements \(4\)\n"
        )
        assert re.search(warn, log)

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
            server=server,
            backup_id="test_backup",
            end_time=datetime.now(tzlocal()),
            parent_backup_id=None,
            children_backup_ids=["test_backup_child"],
        )

        # Build a CHILD BackupInfo object with status to DONE
        child_backup_info = build_test_backup_info(
            server=server,
            backup_id="test_backup_child",
            end_time=datetime.now(tzlocal()) + timedelta(days=1),
            parent_backup_id="test_backup",
            children_backup_ids=None,
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup_child": child_backup_info,
        }
        server.config.minimum_redundancy = 1

        # execute retention policy report on parent
        report_parent = rp.backup_status("test_backup")

        assert report_parent == "VALID"

        # execute retention policy report on child
        report_child = rp.backup_status("test_backup_child")

        assert report_child == "VALID"
        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = "invalid"
        empty_report_parent = rp.backup_status("test_backup")

        assert empty_report_parent == BackupInfo.NONE

        empty_report_child = rp.backup_status("test_backup_child")

        assert empty_report_child == BackupInfo.NONE

        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup_child": child_backup_info,
        }
        server.config.minimum_redundancy = 1

        # execute retention policy report on parent
        report_parent = rp.backup_status("test_backup")

        assert report_parent == "VALID"

        # execute retention policy report on child
        report_child = rp.backup_status("test_backup_child")

        assert report_child == "VALID"

        # Force context of retention policy for testing purposes.
        # Expect the method to return a BackupInfo.NONE value
        rp.context = "invalid"
        empty_report_parent = rp.backup_status("test_backup")

        assert empty_report_parent == BackupInfo.NONE

        empty_report_child = rp.backup_status("test_backup_child")

        assert empty_report_child == BackupInfo.NONE

    def test_first_backup(self, server):
        """
        Basic unit test of method first_backup

        This method tests the retrieval of the first backup using both
        RedundancyRetentionPolicy and RecoveryWindowRetentionPolicy
        """
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        # Build a BackupInfo object with status to DONE
        backup_info = build_test_backup_info(
            server=server,
            backup_id="test0",
            end_time=datetime.now(tzlocal()) - timedelta(days=1),
        )
        # Build another BackupInfo object with status to DONE taken one day after
        backup_info2 = build_test_backup_info(
            server=server, backup_id="test1", end_time=datetime.now(tzlocal())
        )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup2": backup_info2,
        }
        server.config.minimum_redundancy = 1
        # execute retention policy report
        report = rp.first_backup()

        assert report == "test_backup"

        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=server
        )
        assert isinstance(rp, RedundancyRetentionPolicy)

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = {
            "test_backup": backup_info,
            "test_backup2": backup_info2,
        }
        server.config.minimum_redundancy = 1

        # execute retention policy report
        report = rp.first_backup()

        assert report == "test_backup"

    @pytest.mark.parametrize(
        ("retention_policy", "retention_status"),
        itertools.product(
            ("RECOVERY WINDOW OF 4 WEEKS", "REDUNDANCY 2"),
            (
                BackupInfo.OBSOLETE,
                BackupInfo.VALID,
                BackupInfo.POTENTIALLY_OBSOLETE,
                BackupInfo.KEEP_FULL,
                BackupInfo.KEEP_STANDALONE,
                BackupInfo.NONE,
            ),
        ),
    )
    @mock.patch("barman.retention_policies._logger.debug")
    @mock.patch("barman.infofile.LocalBackupInfo.walk_backups_tree")
    def test__propagate_retention_status_to_children(
        self,
        mock_walk_backups_tree,
        mock_logger,
        retention_policy,
        retention_status,
        server,
        tmpdir,
    ):
        """
        Unit test of method _propagate_retention_status_to_children
        """

        # Use this to Build a chain of incrementals BackupInfo objects in
        # post-order up to the root.
        chain = {
            "b3": """parent_backup_id=b2
                   children_backup_ids=None
                   status=DONE""",
            "b6": """parent_backup_id=b2
                   children_backup_ids=None
                   status=DONE""",
            "b2": """parent_backup_id=root
                   children_backup_ids=b3,b6
                   status=DONE""",
            "b5": """parent_backup_id=b4
                   children_backup_ids=None
                   status=DONE""",
            "b4": """parent_backup_id=root
                   children_backup_ids=b5
                   status=DONE""",
            "root": """parent_backup_id=None
                     children_backup_ids=b2,b4
                     status=DONE""",
        }
        backup_chain = {}
        for bkp in chain:
            infofile = tmpdir.mkdir(bkp).join("backup.info")
            infofile.write(chain[bkp])
            b_info = build_test_backup_info(
                backup_id=bkp,
                server=server,
            )
            backup_chain[bkp] = b_info

        root = backup_chain["root"]
        mock_walk_backups_tree.return_value = iter(list(backup_chain.values())[:-1])

        rp = RetentionPolicyFactory.create(
            "retention_policy", retention_policy, server=server
        )

        report = {}
        rp._propagate_retention_status_to_children(root, report, retention_status)

        mock_walk_backups_tree.assert_called_once()

        assert mock_logger.call_count == 5
        # For full backups with status KEEP, we propagate VALID status to children
        if retention_status in (BackupInfo.KEEP_FULL, BackupInfo.KEEP_STANDALONE):
            retention_status = BackupInfo.VALID
        for backup_id in report:
            mock_logger.assert_any_call(
                "Propagating %s retention status of backup root to %s."
                % (retention_status, backup_id)
            )

        for backup in report:
            assert report[backup] == retention_status

    def test_redundancy_report_with_incrementals(self, server, caplog):
        """
        Test of the management of the minimum_redundancy parameter
        into the backup_report method of the RedundancyRetentionPolicy class

        """
        rp = RetentionPolicyFactory.create(
            "retention_policy", "REDUNDANCY 2", server=server
        )
        assert isinstance(rp, RedundancyRetentionPolicy)

        backups_data = {
            "20240628T000000": {
                "parent_backup_id": None,
                "children_backup_ids": ["20240628T120000"],
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=6, days=1),
            },
            "20240628T120000": {
                "parent_backup_id": "20240628T000000",
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=6),
            },
            "20240629T000000": {
                "parent_backup_id": None,
                "children_backup_ids": ["20240629T120000"],
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=5, days=1),
            },
            "20240629T120000": {
                "parent_backup_id": "20240629T000000",
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=5),
            },
            "20240630T000000": {
                "parent_backup_id": None,
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()),
            },
        }

        available_backups = {}
        for bkp_id, info in backups_data.items():
            available_backups[bkp_id] = build_test_backup_info(
                backup_id=bkp_id,
                server=server,
                parent_backup_id=info["parent_backup_id"],
                children_backup_ids=info["children_backup_ids"],
                end_time=info["end_time"],
            )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = available_backups

        server.config.minimum_redundancy = 1

        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy because
        # the total number of valid backups is lower than the retention policy
        # redundancy.
        assert report == {
            "20240630T000000": "VALID",
            "20240629T000000": "VALID",
            "20240629T120000": "VALID",
            "20240628T000000": "OBSOLETE",
            "20240628T120000": "OBSOLETE",
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

    def test_recovery_window_report_with_incrementals(self, server, caplog):
        """
        Test of the management of the minimum_redundancy parameter
        into the backup_report method of the RecoveryWindowRetentionPolicy class

        """
        rp = RetentionPolicyFactory.create(
            "retention_policy", "RECOVERY WINDOW OF 4 WEEKS", server=server
        )
        assert isinstance(rp, RecoveryWindowRetentionPolicy)

        backups_data = {
            "20240628T000000": {
                "parent_backup_id": None,
                "children_backup_ids": ["20240628T120000"],
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=6, days=1),
            },
            "20240628T120000": {
                "parent_backup_id": "20240628T000000",
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=6),
            },
            "20240629T000000": {
                "parent_backup_id": None,
                "children_backup_ids": ["20240629T120000"],
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=5, days=1),
            },
            "20240629T120000": {
                "parent_backup_id": "20240629T000000",
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()) - timedelta(weeks=5),
            },
            "20240630T000000": {
                "parent_backup_id": None,
                "children_backup_ids": None,
                "end_time": datetime.now(tzlocal()),
            },
        }

        available_backups = {}
        for bkp_id, info in backups_data.items():
            available_backups[bkp_id] = build_test_backup_info(
                backup_id=bkp_id,
                server=server,
                parent_backup_id=info["parent_backup_id"],
                children_backup_ids=info["children_backup_ids"],
                end_time=info["end_time"],
            )

        # instruct the get_available_backups method to return a map with
        # our mock as result and minimum_redundancy = 1
        server.get_available_backups.return_value = available_backups

        server.config.minimum_redundancy = 1
        server.config.name = "test"

        # execute retention policy report
        report = rp.report()
        # check that our mock is valid for the retention policy because
        # the total number of valid backups is lower than the retention policy
        # redundancy.
        assert report == {
            "20240630T000000": "VALID",
            "20240629T000000": "VALID",
            "20240629T120000": "VALID",
            "20240628T000000": "OBSOLETE",
            "20240628T120000": "OBSOLETE",
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
            r"WARNING  .*Keeping obsolete backup 20240628T000000 for "
            r"server test \(older than .*\) due to minimum redundancy "
            r"requirements \(4\)\n"
        )
        assert re.search(warn, log)


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
