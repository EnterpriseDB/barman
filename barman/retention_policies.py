# Copyright (C) 2011, 2012 2ndQuadrant Italia (Devise.IT S.r.L.)
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

''' This module defines backup retention policies. A backup retention
policy in Barman is a user-defined policy for determining how long
backups and archived logs (WAL segments) need to be retained for media recovery.
You can define a retention policy in terms of backup redundancy or a recovery window.
Barman retains the periodical backups required to satisfy the current retention policy,
and any archived WAL files required for complete recovery of those backups.'''

from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from barman.backup import BackupInfo
import re
import logging

_logger = logging.getLogger(__name__)

class RetentionPolicy(object):
    '''Abstract base class for retention policies'''
    __metaclass__ = ABCMeta

    def __init__(self, mode, unit, value, context, server):
        '''Constructor of the retention policy base class'''
        self.mode = mode
        self.unit = unit
        self.value = int(value)
        self.context = context
        self.server = server
        self._first_backup = None
            
    def report(self):
        '''Report obsolete/valid objects according to the retention policy'''
        if self.context == 'BASE':
            return self._backup_report()
        elif self.context == 'WAL':
            return self._wal_report()

    def backup_status(self, backup_id):
        '''Report the status of a backup according to the retention policy'''
        if self.context == 'BASE':
            return self._backup_report()[backup_id]
        else:
            return BackupInfo.NONE

    def first_backup(self):
        '''Returns the first valid backup according to retention policies'''
        if not self._first_backup:
            self.report()
        return self._first_backup

    @abstractmethod
    def __str__(self):
        '''String representation'''
        pass

    @abstractmethod
    def debug(self):
        '''Debug information'''
        pass

    @abstractmethod
    def _backup_report(self):
        '''Report obsolete/valid backups according to the retention policy'''
        pass
        
    @abstractmethod
    def _wal_report(self):
        '''Report obsolete/valid WALs according to the retention policy'''
        pass
        
    @abstractmethod
    def first_wal(self):
        '''Returns the first valid WAL according to retention policies'''
        pass
    
class RedundancyRetentionPolicy(RetentionPolicy):
    '''Retention policy based on redundancy, the setting that determines
    many periodical backups to keep. A redundancy-based retention polic
    yis contrasted with retention policy that uses a recovery window.'''
    
    _re = re.compile('^\s*redundancy\s+(\d+)\s*$', re.IGNORECASE)

    def __init__(self, context, value, server):
        RetentionPolicy.__init__(self, 'redundancy', 'r', value, 'BASE', server)
        assert (value >= 0)
        
    def __str__(self):
        return "REDUNDANCY %s" % (self.value)

    def debug(self):
        return "Redundancy: %s (%s)" % (self.value, self.context)

    def _backup_report(self):
        '''Report obsolete/valid backups according to the retention policy'''
        report = dict()
        backups = self.server.get_available_backups(BackupInfo.STATUS_NOT_EMPTY)
        # Normalise the redundancy value (according to minimum redundancy)
        redundancy = self.value
        if redundancy < self.server.config.minimum_redundancy:
            _logger.warning("Retention policy redundancy (%s) is lower than the required minimum redundancy (%s). Enforce %s."
                % (redundancy, self.server.config.minimum_redundancy, self.server.config.minimum_redundancy))
            redundancy = self.server.config.minimum_redundancy

        # Map the latest 'redundancy' DONE backups as VALID
        # The remaining DONE backups are classified as OBSOLETE
        # Non DONE backups are classified as NONE
        # NOTE: reverse key orders (simulate reverse chronology)
        i = 0
        for bid in reversed(sorted(backups.iterkeys())):
            if backups[bid].status == BackupInfo.DONE:
                if i < redundancy:
                    report[bid] = BackupInfo.VALID
                    self._first_backup = bid
                else:
                    report[bid] = BackupInfo.OBSOLETE
                i = i + 1
            else:
                report[bid] = BackupInfo.NONE
        return report

    def _wal_report(self):
        '''Report obsolete/valid WALs according to the retention policy'''
        pass

    def first_wal(self):
        '''Returns the first valid WAL according to retention policies'''
        return "TODO"

    @staticmethod
    def create(server, context, optval):
        # Detect Redundancy retention type
        rm = RedundancyRetentionPolicy._re.match(optval)
        if not rm:
            return None
        value = int(rm.groups()[0])
        return RedundancyRetentionPolicy(context, value, server)

class RecoveryWindowRetentionPolicy(RetentionPolicy):
    '''Retention policy based on recovery window. The DBA specifies a period of
    time and Barman ensures retention of backups and archived WAL files required
    for point-in-time recovery to any time during the recovery window.
    The interval always ends with the current time and extends back in time
    for the number of days specified by the user.
    For example, if the retention policy is set for a recovery window of seven days,
    and the current time is 9:30 AM on Friday, Barman retains the backups required
    to allow point-in-time recovery back to 9:30 AM on the previous Friday.'''

    _re = re.compile('^\s*recovery\s+window\s+of\s+(\d+)\s+(day|month|week)s?\s*$', re.IGNORECASE)
    _kw = {'d':'DAYS', 'm':'MONTHS', 'w':'WEEKS'}
    
    def __init__(self, context, value, unit, server):
        RetentionPolicy.__init__(self, 'window', unit, value, context, server)
        assert (value >= 0)
        assert (unit == 'd' or unit == 'm' or unit == 'w')
        assert (context == 'WAL' or context == 'BASE')
        # Calculates the time delta
        if (unit == 'd'):
            self.timedelta = timedelta(days=(self.value))
        elif (unit == 'w'):
            self.timedelta = timedelta(weeks=(self.value))
        elif (unit == 'm'):
            self.timedelta = timedelta(days=(31 * self.value))
        
    def __str__(self):
        return "RECOVERY WINDOW OF %s %s" % (self.value, self._kw[self.unit])

    def debug(self):
        return "Recovery Window: %s %s: %s (%s)" % (self.value, self.unit, self.context, self._point_of_recoverability())

    def _point_of_recoverability(self):
        '''Based on the current time and the window, calculate the point
        of recoverability, which will be then used to define the first
        backup or the first WAL'''
        return datetime.now() - self.timedelta
    
    def _backup_report(self):
        '''Report obsolete/valid backups according to the retention policy'''
        report = dict()
        backups = self.server.get_available_backups(BackupInfo.STATUS_NOT_EMPTY)
        # Map as VALID all DONE backups having end time lower than
        # the point of recoverability. The older ones
        # are classified as OBSOLETE.
        # Non DONE backups are classified as NONE
        found = False
        valid = 0
        # NOTE: reverse key orders (simulate reverse chronology)
        for bid in reversed(sorted(backups.iterkeys())):
            # We are interested in DONE backups only
            if backups[bid].status == BackupInfo.DONE:
                if found:
                    # Check minimum redundancy requirements
                    if valid < self.server.config.minimum_redundancy:
                        _logger.warning("Keeping obsolete backup %s for server %s (older than %s) due to minimum redundancy requirements (%s)"
                            % (bid, self.server.config.name, self._point_of_recoverability(),
                               self.server.config.minimum_redundancy))
                        # We mark the backup as potentially obsolete
                        # as we must respect minimum redundancy requirements
                        report[bid] = BackupInfo.POTENTIALLY_OBSOLETE
                        self._first_backup = bid
                        valid = valid + 1
                    else:
                        # We mark this backup as obsolete (older than the first valid one)
                        _logger.info("Reporting backup %s for server %s as OBSOLETE (older than %s)"
                            % (bid, self.server.config.name, self._point_of_recoverability()))
                        report[bid] = BackupInfo.OBSOLETE
                else:
                    # Backup within the recovery window
                    report[bid] = BackupInfo.VALID
                    self._first_backup = bid
                    valid = valid + 1
                    # TODO: Currently we use the backup local end time
                    # We need to make this more accurate
                    if backups[bid].end_time < self._point_of_recoverability():
                        found = True
            else:
                report[bid] = BackupInfo.NONE
        return report

    def _wal_report(self):
        '''Report obsolete/valid WALs according to the retention policy'''
        pass

    def first_wal(self):
        '''Returns the first valid WAL according to retention policies'''
        return "TODO"
    
    @staticmethod
    def create(server, context, optval):
        # Detect Recovery Window retention type
        rm = RecoveryWindowRetentionPolicy._re.match(optval)
        if not rm:
            return  None
        value = int(rm.groups()[0])
        unit = rm.groups()[1][0].lower()
        return RecoveryWindowRetentionPolicy(context, value, unit, server)
        

class SimpleWALRetentionPolicy(RetentionPolicy):
    '''Simple retention policy for WAL files (identical to the main one)'''
    _re = re.compile('^\s*main\s*$', re.IGNORECASE)
    
    def __init__(self, policy, server):
        RetentionPolicy.__init__(self, 'simple-wal', policy.unit, policy.value, 'WAL', server)
        # The referred policy must be of type 'BASE'
        assert (self.context == 'WAL' and policy.context=='BASE')
        self.policy = policy
        
    def __str__(self):
        return "main"

    def debug(self):
        return "Simple WAL Retention Policy (%s)" % (self.policy)

    def _backup_report(self):
        '''Report obsolete/valid backups according to the retention policy'''
        pass

    def _wal_report(self):
        '''Report obsolete/valid backups according to the retention policy'''
        self.policy._wal_report()

    def first_wal(self):
        '''Returns the first valid WAL according to retention policies'''
        return self.policy.first_wal()
    
    @staticmethod
    def create(server, context, optval):
        # Detect Redundancy retention type
        rm = SimpleWALRetentionPolicy._re.match(optval)
        if not rm:
            return None
        return SimpleWALRetentionPolicy(server.config.retention_policy, server)


class RetentionPolicyFactory(object):
    '''Factory for retention policy objects'''
    
    # Available retention policy types
    policy_classes = [RedundancyRetentionPolicy, RecoveryWindowRetentionPolicy, SimpleWALRetentionPolicy]
    
    @staticmethod    
    def create(server, option, value):
        '''Based on the given option and value from the configuration
        file, creates the appropriate retention policy object for the given server'''
        if option == 'wal_retention_policy':
            context = 'WAL'
        elif option == 'retention_policy':
            context = 'BASE'
        else:
            raise Exception('Unknown option for retention policy: %s' % (option))

        # Look for the matching rule
        for policy_class in RetentionPolicyFactory.policy_classes:
            policy = policy_class.create(server, context, value)
            if policy:
                return policy
        raise Exception('Cannot parse option %s: %s' % (option, value))

