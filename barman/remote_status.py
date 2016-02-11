# Copyright (C) 2011-2016 2ndQuadrant Italia Srl
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
Remote Status module

A Remote Status class implements a standard interface for
retrieving and caching the results of a remote component
(such as Postgres server, WAL archiver, etc.). It follows
the Mixin pattern.
"""

from abc import ABCMeta, abstractmethod

from barman.utils import with_metaclass


class RemoteStatusMixin(with_metaclass(ABCMeta, object)):
    """
    Abstract base class that implements remote status capabilities
    following the Mixin pattern.
    """

    def __init__(self, *args, **kwargs):
        """
        Base constructor (Mixin pattern)
        """
        self._remote_status = None
        super(RemoteStatusMixin, self).__init__(*args, **kwargs)

    @abstractmethod
    def fetch_remote_status(self):
        """
        Retrieve status information from the remote component

        The implementation of this method must not raise any exception in case
        of errors, but should set the missing values to None in the resulting
        dictionary.

        :rtype: dict[str, None|str]
        """

    def get_remote_status(self):
        """
        Get the status of the remote component

        This method does not raise any exception in case of errors,
        but set the missing values to None in the resulting dictionary.

        :rtype: dict[str, None|str]
        """
        if self._remote_status is None:
            self._remote_status = self.fetch_remote_status()
        return self._remote_status

    def reset_remote_status(self):
        """
        Reset the cached result
        """
        self._remote_status = None
