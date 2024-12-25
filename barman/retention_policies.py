# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2011-2025
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
This module defines backup retention policies. A backup retention
policy in Barman is a user-defined policy for determining how long
backups and archived logs (WAL segments) need to be retained for media
recovery.
You can define a retention policy in terms of backup redundancy
or a recovery window.
Barman retains the periodical backups required to satisfy
the current retention policy, and any archived WAL files required for complete
recovery of those backups.
"""

import logging
import re
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta

from dateutil import tz

from barman.annotations import KeepManager
from barman.exceptions import InvalidRetentionPolicy
from barman.infofile import BackupInfo
from barman.utils import with_metaclass

_logger = logging.getLogger(__name__)


class RetentionPolicy(with_metaclass(ABCMeta, object)):
    """Abstract base class for retention policies"""

    def __init__(self, mode, unit, value, context, server):
        """Constructor of the retention policy base class"""
        self.mode = mode
        self.unit = unit
        self.value = int(value)
        self.context = context
        self.server = server
        self._first_backup = None
        self._first_wal = None

    def report(self, source=None, context=None):
        """Report obsolete/valid objects according to the retention policy"""
        if context is None:
            context = self.context
        # Overrides the list of available backups
        if source is None:
            source = self.server.available_backups
        if context == "BASE":
            return self._backup_report(source)
        elif context == "WAL":
            return self._wal_report()
        else:
            raise ValueError("Invalid context %s", context)

    def backup_status(self, backup_id):
        """Report the status of a backup according to the retention policy"""
        source = self.server.available_backups
        if self.context == "BASE":
            return self._backup_report(source)[backup_id]
        else:
            return BackupInfo.NONE

    def first_backup(self):
        """Returns the first valid backup according to retention policies"""
        if not self._first_backup:
            self.report(context="BASE")
        return self._first_backup

    def first_wal(self):
        """Returns the first valid WAL according to retention policies"""
        if not self._first_wal:
            self.report(context="WAL")
        return self._first_wal

    @abstractmethod
    def __str__(self):
        """String representation"""
        pass

    @abstractmethod
    def debug(self):
        """Debug information"""
        pass

    @abstractmethod
    def _backup_report(self, source):
        """Report obsolete/valid backups according to the retention policy"""
        pass

    @abstractmethod
    def _wal_report(self):
        """Report obsolete/valid WALs according to the retention policy"""
        pass

    @classmethod
    def create(cls, server, option, value):
        """
        If given option and value from the configuration file match,
        creates the retention policy object for the given server
        """
        # using @abstractclassmethod from python3 would be better here
        raise NotImplementedError(
            "The class %s must override the create() class method", cls.__name__
        )

    def to_json(self):
        """
        Output representation of the obj for JSON serialization
        """
        return self.__str__()

    def _propagate_retention_status_to_children(self, backup_info, report, ret_status):
        """
        Propagate retention status to all backups in the tree.

        .. note::
            This has a side-effect. It modifies or add data to *report* dict.

        :param barman.infofile.BackupInfo backup_info: The object we want to
            propagate the RETENTION STATUS from.
        :param dict[str, str] report: The report data structure to be modified.
            Each key is the ID of a backup, and its value is the retention status
            of that backup.
        :param str ret_status:  The status of the backup according to retention
            policies
        """
        backup_tree = backup_info.walk_backups_tree(return_self=False)
        for backup in backup_tree:
            report[backup.backup_id] = ret_status
            _logger.debug(
                "Propagating %s retention status of backup %s to %s."
                % (ret_status, backup_info.backup_id, backup.backup_id)
            )


class RedundancyRetentionPolicy(RetentionPolicy):
    """
    Retention policy based on redundancy, the setting that determines
    many periodical backups to keep. A redundancy-based retention policy
    is contrasted with retention policy that uses a recovery window.
    """

    _re = re.compile(r"^\s*redundancy\s+(\d+)\s*$", re.IGNORECASE)

    def __init__(self, context, value, server):
        super(RedundancyRetentionPolicy, self).__init__(
            "redundancy", "b", value, "BASE", server
        )
        assert value >= 0

    def __str__(self):
        return "REDUNDANCY %s" % self.value

    def debug(self):
        return "Redundancy: %s (%s)" % (self.value, self.context)

    def _backup_report(self, source):
        """Report obsolete/valid backups according to the retention policy"""
        report = dict()
        backups = source
        # Normalise the redundancy value (according to minimum redundancy)
        redundancy = self.value
        if redundancy < self.server.minimum_redundancy:
            _logger.warning(
                "Retention policy redundancy (%s) is lower than "
                "the required minimum redundancy (%s). Enforce %s.",
                redundancy,
                self.server.minimum_redundancy,
                self.server.minimum_redundancy,
            )
            redundancy = self.server.minimum_redundancy

        # Map the latest 'redundancy' DONE backups as VALID
        # The remaining DONE backups are classified as OBSOLETE
        # Non DONE backups are classified as NONE
        # NOTE: reverse key orders (simulate reverse chronology)
        i = 0
        for bid in sorted(backups.keys(), reverse=True):
            if backups[bid].is_incremental:
                _logger.debug(
                    "Ignoring incremental backup %s. The retention status will"
                    " be propagated from %s."
                    % (backups[bid], backups[bid].parent_backup_id)
                )
                continue
            if backups[bid].status == BackupInfo.DONE:
                keep_target = self.server.get_keep_target(bid)
                if keep_target == KeepManager.TARGET_STANDALONE:
                    report[bid] = BackupInfo.KEEP_STANDALONE
                elif keep_target:
                    # Any other recovery target is treated as KEEP_FULL for safety
                    report[bid] = BackupInfo.KEEP_FULL
                elif i < redundancy:
                    report[bid] = BackupInfo.VALID
                    self._first_backup = bid
                else:
                    report[bid] = BackupInfo.OBSOLETE
                i = i + 1
            else:
                report[bid] = BackupInfo.NONE

            if backups[bid].has_children:
                status = report[bid]
                # If the root backup retention status is KEEP:STANDALONE and the backup
                # is still VALID for retention policy, the incremental backups will have
                # the VALID retention status. But if this backup falls outside the
                # retention policy, it will be kept but the incremental backups will get
                # the status OBSOLETE.
                if status == BackupInfo.KEEP_STANDALONE:
                    status = BackupInfo.VALID
                    if i > redundancy:
                        status = BackupInfo.OBSOLETE
                # If the root backup retention status is KEEP:FULL, the incremental
                # backups will have the VALID retention status.
                elif status == BackupInfo.KEEP_FULL:
                    status = BackupInfo.VALID
                self._propagate_retention_status_to_children(
                    backup_info=backups[bid],
                    report=report,
                    ret_status=status,
                )
        return report

    def _wal_report(self):
        """Report obsolete/valid WALs according to the retention policy"""
        pass

    @classmethod
    def create(cls, server, context, optval):
        # Detect Redundancy retention type
        mtch = cls._re.match(optval)
        if not mtch:
            return None
        value = int(mtch.groups()[0])
        return cls(context, value, server)


class RecoveryWindowRetentionPolicy(RetentionPolicy):
    """
    Retention policy based on recovery window. The DBA specifies a period of
    time and Barman ensures retention of backups and archived WAL files
    required for point-in-time recovery to any time during the recovery window.
    The interval always ends with the current time and extends back in time
    for the number of days specified by the user.
    For example, if the retention policy is set for a recovery window of
    seven days, and the current time is 9:30 AM on Friday, Barman retains
    the backups required to allow point-in-time recovery back to 9:30 AM
    on the previous Friday.
    """

    _re = re.compile(
        r"""
        ^\s*
        recovery\s+window\s+of\s+   # recovery window of
        (\d+)\s+(day|month|week)s?  # N (day|month|week) with optional 's'
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )
    _kw = {"d": "DAYS", "m": "MONTHS", "w": "WEEKS"}

    def __init__(self, context, value, unit, server):
        super(RecoveryWindowRetentionPolicy, self).__init__(
            "window", unit, value, context, server
        )
        assert value >= 0
        assert unit == "d" or unit == "m" or unit == "w"
        assert context == "WAL" or context == "BASE"
        # Calculates the time delta
        if unit == "d":
            self.timedelta = timedelta(days=self.value)
        elif unit == "w":
            self.timedelta = timedelta(weeks=self.value)
        elif unit == "m":
            self.timedelta = timedelta(days=(31 * self.value))

    def __str__(self):
        return "RECOVERY WINDOW OF %s %s" % (self.value, self._kw[self.unit])

    def debug(self):
        return "Recovery Window: %s %s: %s (%s)" % (
            self.value,
            self.unit,
            self.context,
            self._point_of_recoverability(),
        )

    def _point_of_recoverability(self):
        """
        Based on the current time and the window, calculate the point
        of recoverability, which will be then used to define the first
        backup or the first WAL
        """
        return datetime.now(tz.tzlocal()) - self.timedelta

    def _backup_report(self, source):
        """Report obsolete/valid backups according to the retention policy"""
        report = dict()
        backups = source
        # Map as VALID all DONE backups having end time lower than
        # the point of recoverability. The older ones
        # are classified as OBSOLETE.
        # Non DONE backups are classified as NONE
        found = False
        valid = 0
        # NOTE: reverse key orders (simulate reverse chronology)
        for bid in sorted(backups.keys(), reverse=True):
            if backups[bid].is_incremental:
                _logger.debug(
                    "Ignoring incremental backup %s. The retention status will"
                    " be propagated from %s."
                    % (backups[bid], backups[bid].parent_backup_id)
                )
                continue
            # We are interested in DONE backups only
            if backups[bid].status == BackupInfo.DONE:
                keep_target = self.server.get_keep_target(bid)
                if keep_target == KeepManager.TARGET_STANDALONE:
                    keep_target = BackupInfo.KEEP_STANDALONE
                elif keep_target:
                    # Any other recovery target is treated as KEEP_FULL for safety
                    keep_target = BackupInfo.KEEP_FULL
                # By found, we mean "found the first backup outside the recovery
                # window" if that is the case then this bid is potentially obsolete.
                if found:
                    # Check minimum redundancy requirements
                    if valid < self.server.minimum_redundancy:
                        if keep_target:
                            _logger.info(
                                "Keeping obsolete backup %s for server %s "
                                "(older than %s) "
                                "due to keep status: %s",
                                bid,
                                self.server.name,
                                self._point_of_recoverability,
                                keep_target,
                            )
                            report[bid] = keep_target
                        else:
                            _logger.warning(
                                "Keeping obsolete backup %s for server %s "
                                "(older than %s) "
                                "due to minimum redundancy requirements (%s)",
                                bid,
                                self.server.name,
                                self._point_of_recoverability(),
                                self.server.minimum_redundancy,
                            )
                            # We mark the backup as potentially obsolete
                            # as we must respect minimum redundancy requirements
                            report[bid] = BackupInfo.POTENTIALLY_OBSOLETE
                        self._first_backup = bid
                        valid = valid + 1
                    else:
                        if keep_target:
                            _logger.info(
                                "Keeping obsolete backup %s for server %s "
                                "(older than %s) "
                                "due to keep status: %s",
                                bid,
                                self.server.name,
                                self._point_of_recoverability,
                                keep_target,
                            )
                            report[bid] = keep_target
                        else:
                            # We mark this backup as obsolete
                            # (older than the first valid one)
                            _logger.info(
                                "Reporting backup %s for server %s as OBSOLETE "
                                "(older than %s)",
                                bid,
                                self.server.name,
                                self._point_of_recoverability(),
                            )
                            report[bid] = BackupInfo.OBSOLETE
                else:
                    _logger.debug(
                        "Reporting backup %s for server %s as VALID (newer than %s)",
                        bid,
                        self.server.name,
                        self._point_of_recoverability(),
                    )
                    # Backup within the recovery window
                    report[bid] = keep_target or BackupInfo.VALID
                    self._first_backup = bid
                    valid = valid + 1
                    # TODO: Currently we use the backup local end time
                    # We need to make this more accurate
                    if backups[bid].end_time < self._point_of_recoverability():
                        found = True
            else:
                report[bid] = BackupInfo.NONE

            if backups[bid].has_children:
                status = report[bid]
                # If the root backup retention status is KEEP:STANDALONE and the backup
                # is still VALID for retention policy, the incremental backups will have
                # the VALID retention status. But if this backup falls outside the
                # retention policy, it will be kept but the incremental backups will get
                # the status OBSOLETE.
                if status == BackupInfo.KEEP_STANDALONE:
                    status = BackupInfo.VALID
                    if found:
                        status = BackupInfo.OBSOLETE
                # If the root backup retention status is KEEP:FULL, the incremental
                # backups will have the VALID retention status.
                elif status == BackupInfo.KEEP_FULL:
                    status = BackupInfo.VALID
                self._propagate_retention_status_to_children(
                    backup_info=backups[bid],
                    report=report,
                    ret_status=status,
                )
        return report

    def _wal_report(self):
        """Report obsolete/valid WALs according to the retention policy"""
        pass

    @classmethod
    def create(cls, server, context, optval):
        # Detect Recovery Window retention type
        match = cls._re.match(optval)
        if not match:
            return None
        value = int(match.groups()[0])
        unit = match.groups()[1][0].lower()
        return cls(context, value, unit, server)


class SimpleWALRetentionPolicy(RetentionPolicy):
    """Simple retention policy for WAL files (identical to the main one)"""

    _re = re.compile(r"^\s*main\s*$", re.IGNORECASE)

    def __init__(self, context, policy, server):
        super(SimpleWALRetentionPolicy, self).__init__(
            "simple-wal", policy.unit, policy.value, context, server
        )
        # The referred policy must be of type 'BASE'
        assert self.context == "WAL" and policy.context == "BASE"
        self.policy = policy

    def __str__(self):
        return "MAIN"

    def debug(self):
        return "Simple WAL Retention Policy (%s)" % self.policy

    def _backup_report(self, source):
        """Report obsolete/valid backups according to the retention policy"""
        pass

    def _wal_report(self):
        """Report obsolete/valid backups according to the retention policy"""
        self.policy.report(context="WAL")

    def first_wal(self):
        """Returns the first valid WAL according to retention policies"""
        return self.policy.first_wal()

    @classmethod
    def create(cls, server, context, optval):
        # Detect Redundancy retention type
        match = cls._re.match(optval)
        if not match:
            return None
        return cls(context, server.retention_policy, server)


class ServerMetadata(object):
    """
    Static retention metadata for a barman-managed server

    This will return the same values regardless of any changes in the state of
    the barman-managed server and associated backups.
    """

    def __init__(self, server_name, backup_info_list, keep_manager, minimum_redundancy):
        self.name = server_name
        self.minimum_redundancy = minimum_redundancy
        self.retention_policy = None
        self.backup_info_list = backup_info_list
        self.keep_manager = keep_manager

    @property
    def available_backups(self):
        return self.backup_info_list

    def get_keep_target(self, backup_id):
        return self.keep_manager.get_keep_target(backup_id)


class ServerMetadataLive(ServerMetadata):
    """
    Live retention metadata for a barman-managed server

    This will always return the current values for the barman.Server passed in
    at construction time.
    """

    def __init__(self, server, keep_manager):
        self.server = server
        self.keep_manager = keep_manager

    @property
    def name(self):
        return self.server.config.name

    @property
    def minimum_redundancy(self):
        return self.server.config.minimum_redundancy

    @property
    def retention_policy(self):
        return self.server.config.retention_policy

    @property
    def available_backups(self):
        return self.server.get_available_backups(BackupInfo.STATUS_NOT_EMPTY)

    def get_keep_target(self, backup_id):
        return self.keep_manager.get_keep_target(backup_id)


class RetentionPolicyFactory(object):
    """Factory for retention policy objects"""

    # Available retention policy types
    policy_classes = [
        RedundancyRetentionPolicy,
        RecoveryWindowRetentionPolicy,
        SimpleWALRetentionPolicy,
    ]

    @classmethod
    def create(
        cls,
        option,
        value,
        server=None,
        server_name=None,
        catalog=None,
        minimum_redundancy=0,
    ):
        """
        Based on the given option and value from the configuration
        file, creates the appropriate retention policy object
        for the given server

        Either server *or* server_name and backup_info_list must be provided.
        If server (a `barman.Server`) is provided then the returned
        RetentionPolicy will update as the state of the `barman.Server` changes.
        If server_name and backup_info_list are provided then the RetentionPolicy
        will be a snapshot based on the backup_info_list passed at construction
        time.
        """
        if option == "wal_retention_policy":
            context = "WAL"
        elif option == "retention_policy":
            context = "BASE"
        else:
            raise InvalidRetentionPolicy(
                "Unknown option for retention policy: %s" % option
            )

        if server:
            server_metadata = ServerMetadataLive(
                server, keep_manager=server.backup_manager
            )
        else:
            server_metadata = ServerMetadata(
                server_name,
                catalog.get_backup_list(),
                keep_manager=catalog,
                minimum_redundancy=minimum_redundancy,
            )
        # Look for the matching rule
        for policy_class in cls.policy_classes:
            policy = policy_class.create(server_metadata, context, value)
            if policy:
                return policy

        raise InvalidRetentionPolicy("Cannot parse option %s: %s" % (option, value))
