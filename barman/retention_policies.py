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
from barman.server import Server
from barman.backup import BackupInfo, BackupManager
import re

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
            
    @abstractmethod
    def __str__(self):
        '''String representation'''
        pass

    @abstractmethod
    def first_backup(self):
        '''Returns the first valid backup according to retention policies'''
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
        return "Redundancy: %s (%s)" % (self.value, self.context)

    def first_backup(self):
        '''Returns the first valid backup according to retention policies'''
        return "TODO"
    
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

    _re = re.compile('^\s*recovery\s+window\s+of\s+(\d+)\s+(days|months|weeks)\s*$', re.IGNORECASE)
    
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
        return "Recovery Window: %s %s: %s (%s)" % (self.value, self.unit, self.context, self._point_of_recoverability())

    def _point_of_recoverability(self):
        '''Based on the current time and the window, calculate the point
        of recoverability, which will be then used to define the first
        backup or the first WAL'''
        return datetime.now() - self.timedelta
    
    def first_backup(self):
        '''Returns the first valid backup according to retention policies'''
        return "TODO"
    
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
        return "Simple WAL Retention Policy (%s)" % (self.policy)

    def first_backup(self):
        '''Returns the first valid backup according to retention policies'''
        return self.policy.first_backup()

    def first_wal(self):
        '''Returns the first valid WAL according to retention policies'''
        return self.policy.first_wal()
    
    @staticmethod
    def create(server, context, optval):
        # Detect Redundancy retention type
        rm = SimpleWALRetentionPolicy._re.match(optval)
        if not rm:
            return None
        # Same as the main retention policy
        # Retrieves the policy through the server
        # TODO
        pass


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

