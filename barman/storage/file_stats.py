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

from datetime import datetime

try:
    from datetime import timezone

    utc = timezone.utc
except ImportError:
    # python 2.7 compatibility
    try:
        from dateutil import tz

        utc = tz.UTC

    except AttributeError:
        # This is to manage special case for minimal env with python-dateutil==1.5
        utc = None


class FileStats:
    def __init__(self, size, last_modified):
        """
        Arbitrary timezone set to UTC. There is probably possible improvement here.
        :param size: file size in bytes
        :type size: int
        :param last_modified: Time of last modification in seconds
        :type last_modified: int
        """
        self.size = size
        if utc:
            self.last_modified = datetime.fromtimestamp(last_modified, tz=utc)
        else:
            self.last_modified = datetime.utcfromtimestamp(last_modified)

    def get_size(self):
        """ """
        return self.size

    def get_last_modified(self, datetime_format="%Y-%m-%d %H:%M:%S"):
        """
        :param datetime_format: Format to apply on datetime object
        :type datetime_format: str
        """
        return self.last_modified.strftime(datetime_format)
